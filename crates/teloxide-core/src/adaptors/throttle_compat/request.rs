use std::{
    future::{Future, IntoFuture},
    num::NonZeroU32,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
};

use crate::{
    adaptors::throttle_compat::CompatState,
    errors::AsResponseParameters,
    outbound::{
        OutboundAcquireError, OutboundCompletion, OutboundMetadata, OutboundPayload,
        OutboundPermit, OutboundPriority, OutboundQueue, OutboundScope,
    },
    requests::{HasPayload, Output, Payload, Request},
    types::Seconds,
};

/// Request returned by [`ThrottleCompat`](super::ThrottleCompat) methods.
///
/// Like the legacy `ThrottlingRequest`, the inner request is shared
/// through an `Arc`: an outer `send_ref()` (and a retried owned request)
/// re-sends the same payload without consuming it and without cloning the
/// inner request itself (an arbitrary `R::clone()` may carry side
/// effects, and the legacy layer never calls it).
#[must_use = "Requests are lazy and do nothing unless sent"]
#[derive(Clone)]
pub struct CompatRequest<R: HasPayload> {
    pub(super) request: Arc<R>,
    pub(super) queue: OutboundQueue,
    pub(super) state: Arc<CompatState>,
}

/// The boxed send future of [`CompatRequest`].
type BoxedSend<R> = Pin<Box<dyn Future<Output = Result<Output<R>, <R as Request>::Err>> + Send>>;

/// Future returned by [`CompatRequest`].
pub struct CompatSend<R: Request>(BoxedSend<R>);

/// How the inner request is shared between the wrapper and the send
/// future, mirroring the legacy `ShareableRequest` exactly: an owned
/// `send()` first tries to unwrap the `Arc` — if another wrapper clone
/// exists, the request is SHARED and even `retry = false` runs the inner
/// `send_ref()`; an outer `send_ref()` always shares the `Arc`.
enum ShareableRequest<R> {
    Shared(Arc<R>),
    // `Option` is used to `take` ownership on the final owned execution.
    Owned(Option<R>),
}

impl<R: HasPayload> ShareableRequest<R> {
    fn payload_ref(&self) -> &R::Payload {
        match self {
            Self::Shared(shared) => shared.payload_ref(),
            Self::Owned(owned) => owned
                .as_ref()
                .expect("the owned request is only taken in the final iteration")
                .payload_ref(),
        }
    }
}

impl<R: HasPayload + Clone> HasPayload for CompatRequest<R> {
    type Payload = R::Payload;

    fn payload_mut(&mut self) -> &mut Self::Payload {
        Arc::make_mut(&mut self.request).payload_mut()
    }

    fn payload_ref(&self) -> &Self::Payload {
        self.request.payload_ref()
    }
}

impl<R> Request for CompatRequest<R>
where
    R: Request + Clone + Send + Sync + 'static,
    R::Err: AsResponseParameters,
    R::Payload: Payload<Output: Send> + OutboundPayload,
{
    type Err = R::Err;
    type Send = CompatSend<R>;
    type SendRef = CompatSend<R>;

    fn send(self) -> Self::Send {
        // The legacy `Arc::try_unwrap`: an owned send only stays owned
        // when no wrapper clone shares the request.
        let request = match Arc::try_unwrap(self.request) {
            Ok(owned) => ShareableRequest::Owned(Some(owned)),
            Err(shared) => ShareableRequest::Shared(shared),
        };
        CompatSend(Box::pin(compat_send(request, self.queue, self.state)))
    }

    fn send_ref(&self) -> Self::SendRef {
        // Only the `Arc` is cloned — never the inner request itself.
        CompatSend(Box::pin(compat_send(
            ShareableRequest::Shared(Arc::clone(&self.request)),
            self.queue.clone(),
            self.state.clone(),
        )))
    }
}

impl<R> IntoFuture for CompatRequest<R>
where
    R: Request + Clone + Send + Sync + 'static,
    R::Err: AsResponseParameters,
    R::Payload: Payload<Output: Send> + OutboundPayload,
{
    type Output = Result<Output<Self>, <Self as Request>::Err>;
    type IntoFuture = <Self as Request>::Send;

    fn into_future(self) -> Self::IntoFuture {
        self.send()
    }
}

impl<R: Request> Future for CompatSend<R> {
    type Output = Result<Output<R>, R::Err>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

/// The scheduler metadata of one throttled request: the payload's scope
/// and class, the legacy-compatible NORMAL priority and weight 1 (the
/// legacy worker counts API calls, not messages).
fn compat_metadata<R>(request: &ShareableRequest<R>) -> OutboundMetadata
where
    R: Request,
    R::Payload: OutboundPayload,
{
    let hint = request.payload_ref().outbound_hint();
    OutboundMetadata {
        scope: hint.scope,
        class: hint.class,
        priority: OutboundPriority::NORMAL,
        weight: NonZeroU32::new(1).unwrap(),
    }
}

/// Executes one throttled request through the outbound queue.
///
/// The capacity semaphore plays the role of the legacy bounded channel: a
/// request that finds the backlog full parks on it (tokio's FIFO waitlist)
/// instead of erroring, and a slot freed by a grant, a cancellation or the
/// actor's death automatically wakes the next waiter — no lost wakeups, no
/// dead waiters blocking the queue. The permit is held while the job is
/// pending and released when the grant arrives, exactly like the legacy
/// worker popping a request from its queue before sending it. The outcome
/// is classified like the legacy worker's request loop (see [`finish`]).
async fn compat_send<R>(
    mut request: ShareableRequest<R>,
    queue: OutboundQueue,
    state: Arc<CompatState>,
) -> Result<Output<R>, R::Err>
where
    R: Request + Clone + Send + Sync + 'static,
    R::Err: AsResponseParameters,
    R::Payload: Payload<Output: Send> + OutboundPayload,
{
    let retry = state.retry;
    let metadata = compat_metadata(&request);
    loop {
        // FIFO admission: one permit per backlog slot. A SINGLE
        // `acquire_owned` future is used: registering in the semaphore's
        // FIFO waitlist is atomic, so a permit released between a failed
        // `try_acquire` and the waitlist registration can never be taken
        // by a newer request.
        let slot = state
            .pending_slots
            .clone()
            .acquire_owned()
            .await
            .expect("the pending-slots semaphore is never closed");
        // The legacy worker fires the callback when its queue REACHES the
        // capacity (the N-th pending request), NOT when the N-th request
        // is granted: the check runs before the rate limits are applied.
        // The compatibility layer therefore fires it on the ENQUEUE
        // ACCEPTANCE — the moment the actor put the job into the backlog —
        // and not on the grant: a last pending request that is cancelled
        // before its grant must not erase the full-backlog event that
        // already happened, and a request granted at t=0 must report the
        // full backlog of t=0, not of its own grant instant.
        let was_last_slot = state.pending_slots.available_permits() == 0;
        // Phase 1: enqueue acceptance. The job enters the scheduler
        // backlog; `QueueFull` can only follow a cancel the actor has not
        // processed yet (the semaphore permits == the queue's own backlog
        // bound, held while the jobs are pending): the slot stays with
        // THIS request, so drop it and retry until the actor drains the
        // cancel — dropping the slot would hand it to the next waiter and
        // invert the FIFO order.
        let grant = loop {
            match queue.handle().enqueue(metadata.clone()).await {
                Ok(grant) => break grant,
                Err(OutboundAcquireError::QueueFull) => {
                    tokio::task::yield_now().await;
                }
                Err(_) => {
                    // The actor is shut down (or an impossible
                    // configuration slipped through): send directly, like
                    // the legacy worker dying before draining its queue.
                    // The slot is released BEFORE the direct send, so the
                    // other parked requests are woken immediately — a
                    // slow or hanging direct send must not hold them.
                    log::error!("ThrottleCompat: outbound queue unavailable, sending directly");
                    drop(slot);
                    return match &mut request {
                        ShareableRequest::Shared(shared) => shared.send_ref().await,
                        ShareableRequest::Owned(owned) => owned.take().unwrap().await,
                    };
                }
            }
        };
        if was_last_slot {
            // The backlog is full NOW (before any grant): fire the
            // callback at the legacy timing. The legacy worker does not
            // run its checks while frozen, so the callback stays silent
            // during a freeze — but the messages are already in its
            // bounded channel, and it WILL report the full backlog after
            // the thaw even if every pending request was cancelled before
            // it. The event is therefore deferred and emitted exactly
            // once at the thaw boundary; the monitor is still started and
            // sleeps until the freeze ends.
            if !super::freeze_active(&state) {
                super::notify_queue_full(&state);
            } else {
                state.deferred_full.store(true, Ordering::SeqCst);
            }
            super::ensure_saturation_monitor(&state);
        }
        // Phase 2: the grant. The slot is held while the job is pending
        // and handed to the next waiter on the grant, exactly like the
        // legacy worker popping a request from its queue before sending
        // it.
        let permit = match grant.await {
            Ok(permit) => permit,
            Err(_) => {
                // The actor died between the acceptance and the
                // grant: send directly (see above). A full-backlog event
                // deferred during a freeze is cleared: the legacy worker
                // is dead and would never run the callback again.
                state.deferred_full.store(false, Ordering::SeqCst);
                log::error!("ThrottleCompat: outbound queue unavailable, sending directly");
                drop(slot);
                return match &mut request {
                    ShareableRequest::Shared(shared) => shared.send_ref().await,
                    ShareableRequest::Owned(owned) => owned.take().unwrap().await,
                };
            }
        };
        drop(slot);

        // The inner path follows the legacy table exactly: `retry = true`
        // always uses `send_ref` (even for an owned request), an outer
        // `send_ref` (shared) uses `send_ref`, and only a truly owned
        // `send` with retries disabled uses `send` — and, like the legacy
        // `owned.take().unwrap().await`, through `IntoFuture`, NOT
        // through `Request::send` (a custom requester may distinguish the
        // two). The owned branch consumes the request and is therefore
        // the final iteration.
        let outcome = match (retry, &mut request) {
            (true, request) => {
                let request = match request {
                    ShareableRequest::Shared(shared) => &**shared,
                    ShareableRequest::Owned(owned) => {
                        owned.as_ref().expect("an owned request is not taken while retries are on")
                    }
                };
                request.send_ref().await
            }
            (false, ShareableRequest::Shared(shared)) => shared.send_ref().await,
            (false, ShareableRequest::Owned(owned)) => owned.take().unwrap().await,
        };
        // The outcome is classified EXACTLY ONCE: the
        // `AsResponseParameters` trait does not require `retry_after` to
        // be pure, and a stateful implementation would make repeated
        // calls diverge (a retry could turn into a plain failure). The
        // single result drives the completion, the retry deadline, the
        // retry decision and the compat-side freeze deadline below.
        let retry_after = outcome.as_ref().err().and_then(AsResponseParameters::retry_after);
        // ONE observation anchor: the scheduler penalty, the local retry
        // sleep and the compat-side freeze deadline are all derived from
        // this single timestamp. With three independent `Instant::now()`
        // calls a preempted request could produce a window in which the
        // monitor considers the freeze over while the scheduler still
        // holds the global penalty.
        let observed_at = tokio::time::Instant::now();
        if let Some(seconds) = retry_after {
            // The freeze deadline is recorded even when the request will
            // not be retried (the legacy worker freezes regardless of the
            // retry flag).
            super::record_freeze_at(&state, observed_at, seconds.duration());
        }
        let retry_until = retry_after.map(|seconds| observed_at + seconds.duration());
        let result: Result<Output<R>, R::Err> =
            finish::<R>(permit, outcome, retry_after, observed_at);
        match result {
            Ok(output) => return Ok(output),
            Err(error) => {
                if retry && retry_after.is_some() {
                    // The legacy worker sleeps until the freeze expires
                    // OUTSIDE the queue and only then re-sends: a request
                    // that arrived during the freeze keeps its place
                    // ahead of the retry, and the retry occupies no
                    // pending slot while it sleeps. The penalty deadline
                    // is anchored at the same observed moment (the
                    // completion command carries the observation
                    // timestamp), so the re-enqueue is granted right
                    // after the freeze.
                    if let Some(until) = retry_until {
                        tokio::time::sleep_until(until).await;
                    }
                    continue;
                }
                return Err(error);
            }
        }
    }
}

/// Completes the permit according to the PRE-CLASSIFIED outcome and
/// returns the outcome unchanged:
///
/// - `retry_after` — registers the GLOBAL penalty (the legacy worker freezes
///   everything);
/// - otherwise `Ok` — success;
/// - otherwise — failure.
///
/// The completion is NON-BLOCKING: the legacy request loop returns its
/// result right after the inner request finished (the worker is only told
/// about a `RetryAfter` freeze, and even that without waiting for the
/// worker to apply it), so the compatibility layer must not stall a
/// granted request on an actor round-trip. Ordering is preserved anyway:
/// the completion is sent synchronously into the actor's lifecycle
/// channel, and the actor drains lifecycle commands (applying the penalty)
/// before it considers any later enqueue from the same caller.
///
/// `retry_after` is classified by the caller exactly once and passed in:
/// [`AsResponseParameters::retry_after`] is not required to be pure, so
/// the completion must not re-examine the error. `observed_at` is the
/// single observation anchor shared with the scheduler penalty.
fn finish<R>(
    permit: OutboundPermit,
    outcome: Result<Output<R>, R::Err>,
    retry_after: Option<Seconds>,
    observed_at: tokio::time::Instant,
) -> Result<Output<R>, R::Err>
where
    R: Request,
{
    match retry_after {
        Some(seconds) => permit.complete_observed_at(
            OutboundCompletion::RetryAfter {
                scope: OutboundScope::Global,
                duration: seconds.duration(),
            },
            observed_at,
        ),
        None => match &outcome {
            Ok(_) => permit.complete_observed_at(OutboundCompletion::Success, observed_at),
            Err(_) => permit.complete_observed_at(OutboundCompletion::Failed, observed_at),
        },
    }
    outcome
}
