//! The outbound queue actor, handle and completion-aware permit.
//!
//! Commit 2: a thin Tokio actor over the pure [`SchedulerState`]. The actor
//! owns the mutable scheduling state, processes commands (enqueue, cancel,
//! complete, penalize, limits, snapshot, shutdown), runs the admission loop
//! on every wake-up and sleeps until the next scheduler deadline — never
//! polling, never scanning all jobs per tick.
//!
//! Backpressure: the enqueue ingress is a bounded channel of capacity
//! [`OutboundSettings::queue_capacity`]; an acquire that cannot be admitted
//! fails fast with [`OutboundQueueError::QueueFull`] instead of growing
//! memory. Inside the scheduler the backlog is bounded the same way (a
//! latest-wins replacement does not grow the backlog and is admitted even
//! at capacity).
//!
//! Lifecycle commands (cancel, complete, penalize, limits, snapshot,
//! shutdown) flow through a separate unbounded channel: `Drop`-based
//! completions are synchronous and can never await a bounded send, and a
//! saturated ingress must not delay them. The actor exits when every
//! external enqueue sender is dropped — it mints permits with its own
//! lifecycle sender, which never keeps the ingress alive.
//!
//! The actor's clock: the scheduler is a pure state machine over
//! `std::time::Instant`. The actor derives its `now` from the Tokio clock,
//! so `#[tokio::test(start_paused = true)]` tests drive the scheduler
//! deterministically.

use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};

use super::{
    scheduler::SchedulerState,
    types::{
        EnqueueError, Grant, JobId, OutboundAcquireError, OutboundCompletion, OutboundEnqueueMode,
        OutboundLaneKey, OutboundLimits, OutboundMeta, OutboundMetadata, OutboundQueueError,
        OutboundScope, OutboundSetLimitsError, OutboundSettings, OutboundSnapshot,
        SchedulerConfigError, SchedulerWakeup,
    },
};

/// A running outbound queue.
///
/// Clone-friendly facade over the [`OutboundQueueHandle`]; the actor task is
/// spawned by [`OutboundQueue::new_spawn`] or driven by the caller via the
/// future returned from [`OutboundQueue::new`].
#[derive(Clone)]
pub struct OutboundQueue {
    handle: OutboundQueueHandle,
}

impl OutboundQueue {
    /// Creates the queue and returns the handle plus the actor future.
    /// The caller decides how to drive the future (spawn it, run it in a
    /// background task, or await it).
    pub fn new(
        settings: OutboundSettings,
    ) -> Result<(Self, impl Future<Output = ()>), SchedulerConfigError> {
        let (handle, actor) = OutboundQueueHandle::new(settings)?;
        Ok((Self { handle }, actor.run()))
    }

    /// Creates the queue and spawns the actor on the current Tokio runtime.
    pub fn new_spawn(settings: OutboundSettings) -> Result<Self, SchedulerConfigError> {
        let (queue, actor) = Self::new(settings)?;
        tokio::spawn(actor);
        Ok(queue)
    }

    pub fn handle(&self) -> &OutboundQueueHandle {
        &self.handle
    }

    pub fn into_handle(self) -> OutboundQueueHandle {
        self.handle
    }
}

/// Command sent to the actor.
enum OutboundCommand {
    Enqueue {
        metadata: OutboundMetadata,
        lane: Option<OutboundLaneKey>,
        mode: OutboundEnqueueMode,
        /// Resolves with the job id (or the enqueue error).
        response: oneshot::Sender<Result<JobId, OutboundQueueError>>,
        /// Resolves when the job is granted (or superseded, or closed).
        /// Created by the acquire future itself, so dropping the future in
        /// ANY state drops this receiver: a grant delivered afterwards
        /// fails to send and the job is completed as cancelled instead of
        /// leaking an in-flight slot.
        granted: oneshot::Sender<AcquireResult>,
    },
    Cancel {
        job_id: JobId,
    },
    Complete {
        job_id: JobId,
        outcome: OutboundCompletion,
        /// Resolved once the completion is applied; used by
        /// [`OutboundPermit::complete_and_await`] as a per-request barrier.
        ack: Option<oneshot::Sender<()>>,
    },
    Penalize {
        scope: OutboundScope,
        duration: Duration,
    },
    GetLimits {
        response: oneshot::Sender<OutboundLimits>,
    },
    SetLimits {
        limits: OutboundLimits,
        response: oneshot::Sender<Result<(), SchedulerConfigError>>,
    },
    GetSnapshot {
        response: oneshot::Sender<OutboundSnapshot>,
    },
    Shutdown,
}

/// How a waiter resolves once its job is enqueued.
enum AcquireResult {
    /// The job was granted; the permit is owned by whoever holds this
    /// message. If the acquire future dies before polling it, the buffered
    /// permit is dropped and its `Drop` reports `CancelledAfterGrant` —
    /// an in-flight slot or lane lock can never leak.
    Granted(OutboundPermit),
    /// The job was superseded by a latest-wins replacement.
    Superseded,
    /// The actor shut down before the job was granted.
    Closed,
}

/// Clone-friendly client handle of the outbound queue.
///
/// The handle owns two channels: a **bounded enqueue channel** (capacity
/// `OutboundSettings::queue_capacity`) for new requests and an unbounded
/// lifecycle channel for cancellation, completion, penalties, settings and
/// shutdown. The actor exits when every external enqueue sender is dropped
/// (the lifecycle channel may stay open — the actor mints permits with its
/// own lifecycle sender, but that never keeps the ingress alive).
#[derive(Clone)]
pub struct OutboundQueueHandle {
    enqueue: mpsc::Sender<OutboundCommand>,
    lifecycle: mpsc::UnboundedSender<OutboundCommand>,
    next_lane_id: Arc<AtomicU64>,
}

impl OutboundQueueHandle {
    fn new(settings: OutboundSettings) -> Result<(Self, OutboundActor), SchedulerConfigError> {
        if settings.queue_capacity == 0 {
            // tokio's bounded channels require a positive buffer; a zero
            // capacity queue could admit nothing anyway.
            return Err(SchedulerConfigError::ZeroQueueCapacity);
        }
        let scheduler = SchedulerState::new(settings.limits, settings.aging)?;
        // The enqueue ingress is bounded by the queue capacity: an acquire
        // that cannot be admitted fails fast with `QueueFull` instead of
        // growing the channel without bound. Lifecycle commands (including
        // `Drop`-based completions) flow through an unbounded channel so a
        // synchronous `Drop` can never block.
        let (enqueue, enqueue_rx) = mpsc::channel(settings.queue_capacity);
        let (lifecycle, lifecycle_rx) = mpsc::unbounded_channel();
        let handle = Self {
            enqueue,
            lifecycle: lifecycle.clone(),
            next_lane_id: Arc::new(AtomicU64::new(0)),
        };
        let actor = OutboundActor {
            scheduler,
            waiters: HashMap::new(),
            enqueue: enqueue_rx,
            lifecycle: lifecycle_rx,
            // The actor mints permits with this lifecycle sender. It is
            // deliberately not an enqueue sender: dropping every external
            // handle closes the ingress and ends the actor task.
            lifecycle_tx: lifecycle,
            queue_capacity: settings.queue_capacity,
            base: Instant::now(),
            started_at: tokio::time::Instant::now(),
        };
        Ok((handle, actor))
    }

    /// Acquires a permit for an independent request (no ordering lane).
    ///
    /// The acquire is enqueued immediately; the returned future resolves
    /// with the permit once the scheduler grants it, or with an error if
    /// the queue is full or closed, or the job was superseded. Dropping the
    /// future before it resolves cancels the pending job.
    pub fn acquire(&self, metadata: OutboundMetadata) -> OutboundAcquire {
        self.acquire_inner(metadata, None, OutboundEnqueueMode::Fifo)
    }

    /// Acquires a permit for a latest-wins slot: while the job is still
    /// pending, a later acquire with the same `user_key` (and compatible
    /// metadata) replaces it, and the replaced waiter resolves with
    /// [`OutboundAcquireError::Superseded`]. The replacement inherits the
    /// queue position and scheduling age of the superseded job.
    pub fn acquire_latest_wins(
        &self,
        metadata: OutboundMetadata,
        user_key: u64,
    ) -> OutboundAcquire {
        self.acquire_inner(metadata, None, OutboundEnqueueMode::ReplacePending { user_key })
    }

    /// Creates a strictly FIFO ordering lane. At most one request of the
    /// lane is in flight at a time, and requests of the lane are granted in
    /// enqueue order regardless of priority.
    pub fn serial_lane(&self) -> OutboundLane {
        let key = OutboundLaneKey(self.next_lane_id.fetch_add(1, Ordering::Relaxed));
        OutboundLane { handle: self.clone(), key }
    }

    fn acquire_inner(
        &self,
        metadata: OutboundMetadata,
        lane: Option<OutboundLaneKey>,
        mode: OutboundEnqueueMode,
    ) -> OutboundAcquire {
        let (response, response_rx) = oneshot::channel();
        let (granted, granted_rx) = oneshot::channel();
        let command = OutboundCommand::Enqueue { metadata, lane, mode, response, granted };
        match self.enqueue.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                // The ingress is at capacity: fail fast, the actor never
                // saw the command. The response receiver is alive and
                // resolves the acquire with `QueueFull`.
                if let OutboundCommand::Enqueue { response, .. } = command {
                    let _ = response.send(Err(OutboundQueueError::QueueFull));
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // The actor is gone; the acquire resolves with `Closed`
                // through the dropped response receiver.
            }
        }
        OutboundAcquire {
            handle: self.clone(),
            granted: granted_rx,
            state: AcquireState::Enqueuing(response_rx),
        }
    }

    /// The current rate limits.
    pub async fn limits(&self) -> Option<OutboundLimits> {
        let (response, response_rx) = oneshot::channel();
        self.lifecycle.send(OutboundCommand::GetLimits { response }).ok()?;
        response_rx.await.ok()
    }

    /// Replaces the rate windows. The command wakes the actor immediately,
    /// so newly admissible candidates are granted without waiting for the
    /// next deadline.
    pub async fn set_limits(&self, limits: OutboundLimits) -> Result<(), OutboundSetLimitsError> {
        let (response, response_rx) = oneshot::channel();
        self.lifecycle
            .send(OutboundCommand::SetLimits { limits, response })
            .map_err(|_| OutboundSetLimitsError::Closed)?;
        match response_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(OutboundSetLimitsError::Invalid(error)),
            Err(_) => Err(OutboundSetLimitsError::Closed),
        }
    }

    /// A point-in-time snapshot of the queue state.
    pub async fn snapshot(&self) -> Option<OutboundSnapshot> {
        let (response, response_rx) = oneshot::channel();
        self.lifecycle.send(OutboundCommand::GetSnapshot { response }).ok()?;
        response_rx.await.ok()
    }

    /// Reports a `RetryAfter`-style penalty for a scope without an in-flight
    /// permit (e.g. after a retry sequence exhausted). The scope is blocked
    /// for the given duration.
    pub fn penalize(&self, scope: OutboundScope, duration: Duration) {
        let _ = self.lifecycle.send(OutboundCommand::Penalize { scope, duration });
    }

    /// Shuts the queue down: all pending waiters resolve with
    /// [`OutboundAcquireError::Closed`] and the actor task ends. Permits
    /// already granted stay valid until dropped; their completion messages
    /// are best-effort.
    pub fn shutdown(&self) {
        let _ = self.lifecycle.send(OutboundCommand::Shutdown);
    }

    fn cancel(&self, job_id: JobId) {
        let _ = self.lifecycle.send(OutboundCommand::Cancel { job_id });
    }
}

/// A strictly FIFO ordering lane obtained from
/// [`OutboundQueueHandle::serial_lane`].
#[derive(Clone)]
pub struct OutboundLane {
    handle: OutboundQueueHandle,
    key: OutboundLaneKey,
}

impl OutboundLane {
    /// Acquires a permit for a request of this lane. Requests of the lane
    /// are granted strictly in enqueue order; the next request starts only
    /// after the previous one completed (or was cancelled after grant).
    pub fn acquire(&self, metadata: OutboundMetadata) -> OutboundAcquire {
        self.handle.acquire_inner(metadata, Some(self.key), OutboundEnqueueMode::Fifo)
    }

    /// Latest-wins variant for a lane slot: replaces the pending job of the
    /// same `user_key` in this lane.
    pub fn acquire_latest_wins(
        &self,
        metadata: OutboundMetadata,
        user_key: u64,
    ) -> OutboundAcquire {
        self.handle.acquire_inner(
            metadata,
            Some(self.key),
            OutboundEnqueueMode::ReplacePending { user_key },
        )
    }
}

/// The pending state of an [`OutboundAcquire`].
enum AcquireState {
    /// The enqueue command was sent; waiting for the actor's response.
    Enqueuing(oneshot::Receiver<Result<JobId, OutboundQueueError>>),
    /// The job is enqueued; waiting for the grant (or supersede, or close).
    Waiting { job_id: JobId },
    /// The future resolved; polling it again is a panic.
    Done,
}

/// A future that resolves with a permit once the scheduler grants the job.
///
/// The grant receiver is owned by the future from the moment of creation,
/// not nested inside the enqueue reply: dropping the future in ANY state
/// drops the receiver, so a grant delivered afterwards is detected by the
/// actor (its waiter send fails) and the job is completed as cancelled
/// instead of leaking an in-flight slot or a lane lock.
pub struct OutboundAcquire {
    handle: OutboundQueueHandle,
    granted: oneshot::Receiver<AcquireResult>,
    state: AcquireState,
}

impl Future for OutboundAcquire {
    type Output = Result<OutboundPermit, OutboundAcquireError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            // Take the state out so that the response channel can be polled
            // without holding a borrow across the state transition.
            let state = std::mem::replace(&mut this.state, AcquireState::Done);
            this.state = match state {
                AcquireState::Enqueuing(mut response) => match response.poll_unpin(cx) {
                    Poll::Ready(Ok(Ok(job_id))) => AcquireState::Waiting { job_id },
                    Poll::Ready(Ok(Err(error))) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Err(_)) => {
                        // The actor died before answering.
                        return Poll::Ready(Err(OutboundAcquireError::Closed));
                    }
                    Poll::Pending => {
                        this.state = AcquireState::Enqueuing(response);
                        return Poll::Pending;
                    }
                },
                AcquireState::Waiting { job_id } => match this.granted.poll_unpin(cx) {
                    Poll::Ready(Ok(AcquireResult::Granted(permit))) => {
                        return Poll::Ready(Ok(permit));
                    }
                    Poll::Ready(Ok(AcquireResult::Superseded)) => {
                        return Poll::Ready(Err(OutboundAcquireError::Superseded));
                    }
                    Poll::Ready(Ok(AcquireResult::Closed)) | Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(OutboundAcquireError::Closed));
                    }
                    Poll::Pending => {
                        this.state = AcquireState::Waiting { job_id };
                        return Poll::Pending;
                    }
                },
                AcquireState::Done => panic!("polled an outbound acquire after completion"),
            };
        }
    }
}

impl Drop for OutboundAcquire {
    fn drop(&mut self) {
        // After a successful resolution the state is `Done`; while it is
        // `Waiting` the job is still pending and can be cancelled. In the
        // `Enqueuing` state the job id is not known yet, but the dropped
        // `granted` receiver is observed by the actor: a pending job is
        // cancelled via the failed enqueue reply, a granted one is
        // completed as cancelled via the failed waiter send.
        if let AcquireState::Waiting { job_id } = self.state {
            self.handle.cancel(job_id);
        }
    }
}

/// A granted outbound slot. The caller holds it while the request runs and
/// reports the outcome with [`OutboundPermit::complete`].
///
/// Dropping the permit without an explicit completion reports
/// [`OutboundCompletion::CancelledAfterGrant`] to the scheduler (best
/// effort): the rate budget is not refunded, but the ordering lane (if any)
/// is released.
/// The permit owns a lifecycle sender (not the full handle): minting it
/// never keeps the bounded enqueue ingress alive, so the actor can exit
/// when every external handle is dropped even while permits are in flight.
pub struct OutboundPermit {
    job_id: JobId,
    lifecycle: mpsc::UnboundedSender<OutboundCommand>,
    completed: bool,
}

impl OutboundPermit {
    fn new(job_id: JobId, lifecycle: mpsc::UnboundedSender<OutboundCommand>) -> Self {
        Self { job_id, lifecycle, completed: false }
    }

    /// Reports how the request ended and releases the permit.
    ///
    /// A `RetryAfter` completion penalizes the reported scope for the
    /// reported duration (a chat-scoped request may report a global flood
    /// penalty). The rate budget consumed by the grant is never refunded.
    pub fn complete(mut self, outcome: OutboundCompletion) {
        self.completed = true;
        let _ = self.lifecycle.send(OutboundCommand::Complete {
            job_id: self.job_id,
            outcome,
            ack: None,
        });
    }

    /// Completes the permit and waits until the actor has applied the
    /// outcome. This is the per-request barrier of the adaptor: when this
    /// future resolves, a `RetryAfter` penalty is already registered, so a
    /// subsequent acquire from the same caller is guaranteed to observe it
    /// (no acquire can overtake the completion that precedes it).
    ///
    /// If the actor is gone the wait is skipped: there is nothing to apply
    /// the outcome to.
    pub async fn complete_and_await(mut self, outcome: OutboundCompletion) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.completed = true;
        let sent = self.lifecycle.send(OutboundCommand::Complete {
            job_id: self.job_id,
            outcome,
            ack: Some(ack_tx),
        });
        if sent.is_err() {
            // The actor (and its channel) is gone: the outcome cannot be
            // applied anywhere.
            return;
        }
        // The actor may die after receiving the command but before
        // resolving the ack: the receiver closes and the wait ends.
        let _ = ack_rx.await;
    }
}

impl Drop for OutboundPermit {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.lifecycle.send(OutboundCommand::Complete {
                job_id: self.job_id,
                outcome: OutboundCompletion::CancelledAfterGrant,
                ack: None,
            });
        }
    }
}

/// The actor: sole owner of the mutable scheduling state.
pub(crate) struct OutboundActor {
    scheduler: SchedulerState,
    /// Waiters of enqueued jobs: resolved on grant, supersede or shutdown.
    waiters: HashMap<JobId, oneshot::Sender<AcquireResult>>,
    enqueue: mpsc::Receiver<OutboundCommand>,
    lifecycle: mpsc::UnboundedReceiver<OutboundCommand>,
    /// Used to mint permits at grant time; see
    /// [`OutboundActor::deliver_grants`]. Deliberately a lifecycle sender,
    /// never an enqueue sender: the actor must not keep the ingress open.
    lifecycle_tx: mpsc::UnboundedSender<OutboundCommand>,
    queue_capacity: usize,
    /// `std` clock anchor: the scheduler sees `base + tokio_elapsed`, so
    /// paused Tokio time drives the scheduler deterministically in tests.
    base: Instant,
    started_at: tokio::time::Instant,
}

/// Maximum number of lifecycle commands drained per actor loop iteration.
/// The drain gives already-arrived completions/cancellations priority over
/// new enqueues; the bound keeps a busy lifecycle producer from starving
/// the enqueue ingress.
const LIFECYCLE_DRAIN_BATCH: usize = 64;

impl OutboundActor {
    fn now(&self) -> Instant {
        self.base + (tokio::time::Instant::now() - self.started_at)
    }

    fn to_tokio(&self, at: Instant) -> tokio::time::Instant {
        self.started_at + (at - self.base)
    }

    fn handle_command(&mut self, command: OutboundCommand) {
        match command {
            OutboundCommand::Enqueue { metadata, lane, mode, response, granted } => {
                let meta = OutboundMeta {
                    scope: metadata.scope,
                    lane,
                    class: metadata.class,
                    priority: metadata.priority,
                    weight: metadata.weight,
                };
                match self.scheduler.enqueue(meta, mode, self.queue_capacity, None, self.now()) {
                    Ok(outcome) => {
                        if let Some(superseded) = outcome.superseded {
                            if let Some(waiter) = self.waiters.remove(&superseded) {
                                let _ = waiter.send(AcquireResult::Superseded);
                            }
                        }
                        if response.send(Ok(outcome.job)).is_err() {
                            // The acquire future was dropped before the reply
                            // was delivered. The job is still pending here
                            // (enqueue and admission run sequentially in one
                            // loop iteration), so cancel it: nobody can ever
                            // own the permit, and leaving the job would leak
                            // an in-flight slot and a lane block.
                            self.scheduler.cancel(outcome.job, self.now());
                            return;
                        }
                        self.waiters.insert(outcome.job, granted);
                    }
                    Err(EnqueueError::QueueFull) => {
                        let _ = response.send(Err(OutboundQueueError::QueueFull));
                    }
                    Err(EnqueueError::IncompatibleCoalesceMetadata) => {
                        let _ =
                            response.send(Err(OutboundQueueError::IncompatibleCoalesceMetadata));
                    }
                    Err(EnqueueError::WeightExceedsWindow { scope, weight, capacity }) => {
                        let _ = response.send(Err(OutboundQueueError::WeightExceedsWindow {
                            scope,
                            weight,
                            capacity,
                        }));
                    }
                }
            }
            OutboundCommand::Cancel { job_id } => {
                self.waiters.remove(&job_id);
                self.scheduler.cancel(job_id, self.now());
            }
            OutboundCommand::Complete { job_id, outcome, ack } => {
                self.scheduler.complete(job_id, outcome, self.now());
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
            }
            OutboundCommand::Penalize { scope, duration } => {
                if !duration.is_zero() {
                    self.scheduler.penalize(scope, self.now() + duration);
                }
            }
            OutboundCommand::GetLimits { response } => {
                let limits = OutboundLimits {
                    global: self.scheduler.global_limits().to_vec(),
                    chat: self.scheduler.chat_limits().to_vec(),
                };
                let _ = response.send(limits);
            }
            OutboundCommand::SetLimits { limits, response } => {
                let _ = response.send(self.scheduler.set_limits(limits, self.now()));
            }
            OutboundCommand::GetSnapshot { response } => {
                let _ = response.send(self.scheduler.snapshot());
            }
            OutboundCommand::Shutdown => {}
        }
    }

    /// Hands each grant to its waiter as a ready-made permit.
    ///
    /// The permit is the ownership token of the in-flight slot: if the
    /// waiter is gone (the acquire future was dropped in any state, whether
    /// the grant was already buffered in its channel or the waiter send
    /// fails here), the permit is dropped and its `Drop` reports
    /// `CancelledAfterGrant` — the lane is released and the rate budget
    /// stays consumed. A permit can therefore never be lost: it either
    /// reaches the caller or completes the job itself.
    fn deliver_grants(&mut self, grants: Vec<Grant>) {
        for grant in grants {
            let permit = OutboundPermit::new(grant.job, self.lifecycle_tx.clone());
            if let Some(waiter) = self.waiters.remove(&grant.job) {
                let _ = waiter.send(AcquireResult::Granted(permit));
            }
        }
    }

    fn shutdown_waiters(&mut self) {
        for waiter in self.waiters.drain() {
            let _ = waiter.1.send(AcquireResult::Closed);
        }
    }

    /// Runs one admission pass and returns the next scheduler deadline.
    fn run_admission(&mut self) -> SchedulerWakeup {
        let now = self.now();
        let grants = self.scheduler.grant_ready(now);
        self.deliver_grants(grants);
        self.scheduler.next_deadline(now)
    }

    fn timer_deadline(&self, wakeup: SchedulerWakeup) -> tokio::time::Instant {
        match wakeup {
            SchedulerWakeup::At(at) => self.to_tokio(at),
            // The scheduler contract says `Immediate` means "call
            // `grant_ready` again right away"; it should never be returned
            // after a grant pass, but if the invariant is ever breached an
            // immediate wake-up is a visible busy loop, not a silent
            // year-long deadlock.
            SchedulerWakeup::Immediate => tokio::time::Instant::now(),
            // Nothing time-based will change: park the timer far in the
            // future and rely on commands to wake the actor.
            SchedulerWakeup::ExternalEvent => {
                tokio::time::Instant::now() + Duration::from_secs(365 * 24 * 3600)
            }
        }
    }

    /// The actor loop: process commands, run admission, sleep until the
    /// next scheduler deadline. Panics inside the loop are contained so
    /// that the task ends cleanly and waiters resolve with `Closed` instead
    /// of hanging.
    async fn run(mut self) {
        let _ = AssertUnwindSafe(async {
            // `tokio::time::Sleep` is `!Unpin`, so it must be pinned in
            // place: `tokio::pin!` keeps the timer stable across the loop
            // iterations and lets `tokio::select!` poll it by reborrow.
            let timer = tokio::time::sleep(Duration::from_secs(0));
            tokio::pin!(timer);
            loop {
                // Bounded lifecycle drain. Lifecycle commands ride their
                // own unbounded channel so that completions, cancellations
                // and shutdown can never be blocked by a saturated enqueue
                // ingress. Draining the already-arrived lifecycle commands
                // BEFORE the `select!` below makes a completion (and its
                // RetryAfter penalty) that was sent before a subsequent
                // acquire from the same caller apply first: both sends are
                // synchronous, so by the time the caller yields both are in
                // their channels, and the drain sees the completion before
                // the acquire is even considered.
                //
                // The drain is BOUNDED: the `select!` below polls the
                // channels fairly, so a busy lifecycle producer (the public
                // handle exposes `penalize`/`snapshot`/`limits` with no
                // admission) can delay but never starve the enqueue
                // ingress.
                let mut drained = 0;
                while drained < LIFECYCLE_DRAIN_BATCH {
                    match self.lifecycle.try_recv() {
                        Ok(OutboundCommand::Shutdown) => {
                            self.shutdown_waiters();
                            return;
                        }
                        Ok(command) => {
                            self.handle_command(command);
                            drained += 1;
                        }
                        Err(_) => break,
                    }
                }

                // Admission AFTER the drain and BEFORE waiting for the
                // next event: drained commands change scheduler state
                // (a completion frees its lane, a cancel wakes the next
                // lane head or releases a reservation, `set_limits` can
                // make pending jobs admissible). Without this pass the
                // actor would go to sleep with ready candidates behind it:
                // e.g. two serial lanes with in-flight heads and pending
                // tails, both completions arriving in one batch — the
                // first is handled by the `select!`, the second by the
                // drain, and the second lane's tail would never be granted
                // until some unrelated event woke the actor.
                let wakeup = self.run_admission();
                timer.as_mut().reset(self.timer_deadline(wakeup));

                tokio::select! {
                    command = self.lifecycle.recv() => {
                        match command {
                            Some(OutboundCommand::Shutdown) => {
                                self.shutdown_waiters();
                                return;
                            }
                            Some(command) => self.handle_command(command),
                            None => {
                                // Defensive: the actor holds its own
                                // lifecycle sender, so every sender can
                                // never be gone.
                                self.shutdown_waiters();
                                return;
                            }
                        }
                    }
                    command = self.enqueue.recv() => {
                        match command {
                            Some(command) => self.handle_command(command),
                            None => {
                                // Every external enqueue sender is gone:
                                // the caller cannot submit new work any
                                // more. Drain the waiters and end the task
                                // (permits already granted stay valid; their
                                // completions are best-effort).
                                self.shutdown_waiters();
                                return;
                            }
                        }
                    }
                    _ = timer.as_mut() => {}
                }
            }
        })
        .catch_unwind()
        .await;
        // The actor ended (cleanly or via a contained panic): its state is
        // dropped, so the command channel closes and every pending waiter
        // resolves with `Closed` from the handle side.
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::outbound::types::{
        AgingPolicy, OutboundChatKey, OutboundClass, OutboundPriority, WindowLimit,
    };

    fn settings() -> OutboundSettings {
        OutboundSettings {
            limits: OutboundLimits { global: Vec::new(), chat: Vec::new() },
            queue_capacity: 1024,
            aging: AgingPolicy { quantum: Duration::from_secs(1), max_boost: u8::MAX },
        }
    }

    fn settings_with(global: Vec<WindowLimit>, chat: Vec<WindowLimit>) -> OutboundSettings {
        let mut settings = settings();
        settings.limits = OutboundLimits { global, chat };
        settings
    }

    fn metadata(priority: OutboundPriority) -> OutboundMetadata {
        OutboundMetadata {
            scope: OutboundScope::Global,
            class: OutboundClass::new(0),
            priority,
            weight: NonZeroU32::new(1).unwrap(),
        }
    }

    fn chat_metadata(chat: i64, priority: OutboundPriority) -> OutboundMetadata {
        OutboundMetadata {
            scope: OutboundScope::Chat(OutboundChatKey::new(chat)),
            class: OutboundClass::new(0),
            priority,
            weight: NonZeroU32::new(1).unwrap(),
        }
    }

    fn global_metadata(priority: OutboundPriority, weight: u32) -> OutboundMetadata {
        OutboundMetadata {
            scope: OutboundScope::Global,
            class: OutboundClass::new(0),
            priority,
            weight: NonZeroU32::new(weight).unwrap(),
        }
    }

    /// One window of `capacity` units per `window` seconds.
    fn window(capacity: u32, window: Duration) -> WindowLimit {
        WindowLimit { capacity, window }
    }

    #[tokio::test(start_paused = true)]
    async fn higher_priority_is_granted_before_lower_priority() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();
        // Fill the window first so both acquires are pending when it frees:
        // only then does priority decide between them.
        let holder = handle.acquire(metadata(OutboundPriority::CRITICAL)).await.unwrap();
        let mut normal = handle.acquire(metadata(OutboundPriority::NORMAL));
        let mut critical = handle.acquire(metadata(OutboundPriority::CRITICAL));

        holder.complete(OutboundCompletion::Success);
        // Let the actor process the enqueues at t0: otherwise the batch is
        // handled only after the advance, with the window already expired.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(61)).await;

        // Only one of the two can pass the window; the higher priority must
        // win despite being enqueued second.
        let (winner, permit) = tokio::select! {
            permit = &mut normal => ("normal", permit),
            permit = &mut critical => ("critical", permit),
        };
        assert_eq!(winner, "critical");
        permit.unwrap().complete(OutboundCompletion::Success);

        // The rolling window (capacity 1 / 60 s) frees only when time
        // passes; the blocked lower-priority job is then granted.
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), normal).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn same_priority_jobs_are_granted_in_fifo_order() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();
        let mut first = handle.acquire(metadata(OutboundPriority::NORMAL));
        let mut second = handle.acquire(metadata(OutboundPriority::NORMAL));

        let (winner, permit) = tokio::select! {
            permit = &mut first => ("first", permit),
            permit = &mut second => ("second", permit),
        };
        assert_eq!(winner, "first");
        permit.unwrap().complete(OutboundCompletion::Success);

        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_penalizes_only_the_reported_chat_scope() {
        let queue = OutboundQueue::new_spawn(settings_with(
            Vec::new(),
            vec![window(1, Duration::from_secs(60))],
        ))
        .unwrap();
        let handle = queue.handle();

        // Chat 1 consumes its window and then reports a chat-scoped penalty.
        let permit = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL)).await.unwrap();
        permit.complete(OutboundCompletion::RetryAfter {
            scope: OutboundScope::Chat(OutboundChatKey::new(1)),
            duration: Duration::from_secs(60),
        });

        // A new chat-1 acquire is blocked by the penalty, chat 2 is not.
        let blocked = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        let other = handle.acquire(chat_metadata(2, OutboundPriority::NORMAL));
        let permit_other =
            tokio::time::timeout(Duration::from_secs(1), other).await.unwrap().unwrap();
        permit_other.complete(OutboundCompletion::Success);

        // After the penalty expires the blocked chat-1 acquire proceeds.
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_of_a_chat_request_can_penalize_the_global_scope() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        // A chat-scoped request reports a *global* flood penalty.
        let permit = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL)).await.unwrap();
        permit.complete(OutboundCompletion::RetryAfter {
            scope: OutboundScope::Global,
            duration: Duration::from_secs(60),
        });

        // Even a global-scope request is now blocked.
        let blocked = handle.acquire(metadata(OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn aging_grants_a_background_job_under_continuous_critical_load() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        // Keep the window occupied from the very start, so the background
        // job cannot be granted at once and has to age while criticals keep
        // arriving.
        let holder = handle.acquire(metadata(OutboundPriority::CRITICAL)).await.unwrap();

        let background = handle.acquire(metadata(OutboundPriority::BACKGROUND));
        tokio::pin!(background);

        // Feed a fresh critical request every simulated second. The
        // background job must still be granted within a bounded time.
        let mut grant = None;
        for _ in 0..300 {
            tokio::time::advance(Duration::from_secs(1)).await;
            if let Poll::Ready(output) = futures::poll!(background.as_mut()) {
                grant = Some(output);
                break;
            }
            let critical = handle.acquire(metadata(OutboundPriority::CRITICAL));
            tokio::pin!(critical);
            if let Poll::Ready(output) = futures::poll!(critical.as_mut()) {
                output.unwrap().complete(OutboundCompletion::Success);
            }
            // If the critical was parked (window reserved for the aged
            // background job), dropping it cancels it.
        }
        holder.complete(OutboundCompletion::Success);
        let grant = grant.expect("the background job starved under critical load");
        grant.unwrap().complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_an_acquire_before_grant_cancels_the_job_without_consuming_budget() {
        // A capacity-2 window: if the cancelled job leaked a consumption,
        // the window would stay full and the third acquire would block.
        let settings = settings_with(vec![window(2, Duration::from_secs(60))], Vec::new());
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();

        let first = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        // The second acquire can only be pending; dropping it must cancel it.
        let second = handle.acquire(metadata(OutboundPriority::NORMAL));
        drop(second);
        tokio::task::yield_now().await;

        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.in_flight, 1);

        // The cancelled job must not have consumed budget: with only the
        // first permit in the window, the third acquire is granted at once.
        let third = handle.acquire(metadata(OutboundPriority::NORMAL));
        let permit = tokio::time::timeout(Duration::from_secs(1), third).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
        first.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_an_acquire_before_the_enqueue_is_processed_leaves_no_job_behind() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();

        // Fire-and-forget: the acquire is dropped while the enqueue command
        // is still in flight, possibly before the actor processed it.
        drop(handle.acquire(metadata(OutboundPriority::NORMAL)));
        tokio::task::yield_now().await;

        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.in_flight, 0);

        // The queue keeps working normally afterwards.
        let permit = handle.acquire(metadata(OutboundPriority::NORMAL));
        let permit = tokio::time::timeout(Duration::from_secs(1), permit).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_an_acknowledged_acquire_before_polling_releases_the_job() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        // Occupy the window so the second acquire stays pending after its
        // enqueue is acknowledged.
        let holder = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        let mut acquire = handle.acquire(metadata(OutboundPriority::NORMAL));
        tokio::task::yield_now().await;
        // Poll until the enqueue is acknowledged and the future waits for
        // the grant (still pending: the window is full).
        assert!(futures::poll!(&mut acquire).is_pending());

        // Free the window: the actor grants the job and buffers the permit
        // in the acquire's channel. The snapshot proves the grant happened
        // (pending 0, in flight 1) before the acquire is dropped WITHOUT a
        // final poll — the buffered permit must report
        // `CancelledAfterGrant` on its own: no in-flight slot may leak.
        holder.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.in_flight, 1);
        drop(acquire);
        tokio::task::yield_now().await;

        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.in_flight, 0);

        // The window keeps working normally (the cancelled permit's budget
        // was consumed at grant and expires with time).
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = handle.acquire(metadata(OutboundPriority::NORMAL));
        let permit = tokio::time::timeout(Duration::from_secs(1), permit).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_permit_releases_the_lane_as_cancelled() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let lane = handle.serial_lane();

        let first = lane.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        let second = lane.acquire(metadata(OutboundPriority::NORMAL));
        tokio::pin!(second);
        tokio::task::yield_now().await;
        assert!(futures::poll!(second.as_mut()).is_pending());

        // Dropping the permit reports `CancelledAfterGrant`: the lane is
        // released, so the second job of the lane proceeds.
        drop(first);
        let permit = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn drained_completions_wake_all_serial_lanes() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let lane_a = handle.serial_lane();
        let lane_b = handle.serial_lane();

        // Каждая lane: in-flight head + pending tail.
        let a1 = lane_a.acquire(chat_metadata(1, OutboundPriority::NORMAL)).await.unwrap();
        let b1 = lane_b.acquire(chat_metadata(2, OutboundPriority::NORMAL)).await.unwrap();
        let a2 = lane_a.acquire(chat_metadata(1, OutboundPriority::NORMAL));
        let b2 = lane_b.acquire(chat_metadata(2, OutboundPriority::NORMAL));
        tokio::pin!(a2);
        tokio::pin!(b2);
        assert!(futures::poll!(a2.as_mut()).is_pending());
        assert!(futures::poll!(b2.as_mut()).is_pending());

        // Барьер: оба хвоста уже обработаны actor-ом и лежат в pending
        // (иначе complete'ы могли бы обработаться раньше enqueue, и тест
        // не детерминированно попадал бы в drained batch).
        for _ in 0..100 {
            tokio::task::yield_now().await;
            let snapshot = handle.snapshot().await.unwrap();
            if snapshot.pending == 2 {
                break;
            }
        }
        assert_eq!(handle.snapshot().await.unwrap().pending, 2);

        // Оба completion'а отправляются синхронно, до следующего yield:
        // один обрабатывается `select!`, второй — следующим lifecycle
        // drain. Admission после drain обязан выдать ОБА хвоста, иначе
        // вторая lane навсегда уснёт (регрессия: drained lifecycle batch
        // без сопровождающего admission pass).
        a1.complete(OutboundCompletion::Success);
        b1.complete(OutboundCompletion::Success);

        // Никаких команд между получениями хвостов: завершение первого
        // permit'а само бы разбудило actor и замаскировало регрессию
        // (admission после этого события выдал бы и второй хвост).
        let a2_permit = tokio::time::timeout(Duration::from_secs(1), a2).await.unwrap().unwrap();
        let wait_b2 = tokio::time::timeout(Duration::from_secs(1), b2);
        tokio::pin!(wait_b2);
        // Прокручиваем время после создания deadline, чтобы регрессия
        // упала на timeout, а не висела (paused clock не тикает сам).
        tokio::time::advance(Duration::from_secs(2)).await;
        let b2_permit = wait_b2.await.unwrap().unwrap();
        a2_permit.complete(OutboundCompletion::Success);
        b2_permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn drained_set_limits_wakes_newly_admissible_jobs() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(2, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        // used = 2 (два permit'а weight 1); pending weight 2 не влезает.
        let h1 = handle.acquire(global_metadata(OutboundPriority::NORMAL, 1)).await.unwrap();
        let h2 = handle.acquire(global_metadata(OutboundPriority::NORMAL, 1)).await.unwrap();
        let pending = handle.acquire(global_metadata(OutboundPriority::NORMAL, 2));
        tokio::pin!(pending);
        assert!(futures::poll!(pending.as_mut()).is_pending());
        for _ in 0..100 {
            tokio::task::yield_now().await;
            let snapshot = handle.snapshot().await.unwrap();
            if snapshot.pending == 1 {
                break;
            }
        }
        assert_eq!(handle.snapshot().await.unwrap().pending, 1);

        // Синхронная пара: complete уходит в `select!`, set_limits — в
        // следующий lifecycle drain. После h1.complete used=1, и даже
        // с ним pending (weight 2) не влезает в окно capacity 2; только
        // drained set_limits(capacity 4) делает job допустимым — admission
        // после drain обязан выдать его (регрессия: drained batch без
        // сопровождающего admission pass усыпила бы actor).
        h1.complete(OutboundCompletion::Success);
        let set = handle.set_limits(OutboundLimits {
            global: vec![window(4, Duration::from_secs(60))],
            chat: Vec::new(),
        });
        tokio::pin!(set);
        let _ = futures::poll!(set.as_mut()); // команда отправлена, ответ не получен

        let wait = tokio::time::timeout(Duration::from_secs(1), pending);
        tokio::pin!(wait);
        tokio::time::advance(Duration::from_secs(2)).await;
        let permit = wait.await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
        tokio::time::timeout(Duration::from_secs(1), set).await.unwrap().unwrap();
        h2.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn drained_cancellation_wakes_a_parked_reservation_job() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(10, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        // used = 9; heavy (weight 10) становится owner резервации окна,
        // light (weight 1) паркуется за ней.
        for _ in 0..9 {
            let permit =
                handle.acquire(global_metadata(OutboundPriority::NORMAL, 1)).await.unwrap();
            permit.complete(OutboundCompletion::Success);
        }
        // Барьер: все завершения обработаны (иначе heavy был бы выдан как
        // permit, а не стал бы блокированным owner резервации).
        for _ in 0..100 {
            tokio::task::yield_now().await;
            let snapshot = handle.snapshot().await.unwrap();
            if snapshot.in_flight == 0 {
                break;
            }
        }
        assert_eq!(handle.snapshot().await.unwrap().in_flight, 0);
        let mut heavy = Box::pin(handle.acquire(global_metadata(OutboundPriority::NORMAL, 10)));
        let light = handle.acquire(global_metadata(OutboundPriority::NORMAL, 1));
        tokio::pin!(light);
        tokio::task::yield_now().await;
        assert!(futures::poll!(heavy.as_mut()).is_pending());
        assert!(futures::poll!(light.as_mut()).is_pending());
        for _ in 0..100 {
            tokio::task::yield_now().await;
            let snapshot = handle.snapshot().await.unwrap();
            if snapshot.pending == 2 {
                break;
            }
        }
        assert_eq!(handle.snapshot().await.unwrap().pending, 2);

        // Синхронная пара: no-op penalize уходит в `select!`, cancel
        // владельца резервации (drop acquire-будущего) — в следующий
        // lifecycle drain. Admission после drain обязан немедленно выдать
        // parked light.
        handle.penalize(OutboundScope::Global, Duration::ZERO);
        drop(heavy);

        let wait = tokio::time::timeout(Duration::from_secs(1), light);
        tokio::pin!(wait);
        tokio::time::advance(Duration::from_secs(2)).await;
        let permit = wait.await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_permit_keeps_the_consumed_window_budget() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        // A granted permit consumes the window; dropping it without an
        // explicit completion must NOT refund the budget (spec: rate
        // reservation is never rolled back).
        let permit = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        drop(permit);
        tokio::task::yield_now().await;

        let blocked = handle.acquire(metadata(OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        // Only time frees the window.
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn lane_is_strictly_fifo_across_priorities() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();
        let lane = handle.serial_lane();

        // The lane must not reorder: the low-priority head is granted before
        // the high-priority tail.
        let mut first = lane.acquire(metadata(OutboundPriority::BACKGROUND));
        let mut second = lane.acquire(metadata(OutboundPriority::CRITICAL));

        let (winner, permit) = tokio::select! {
            permit = &mut first => ("first", permit),
            permit = &mut second => ("second", permit),
        };
        assert_eq!(winner, "first");
        permit.unwrap().complete(OutboundCompletion::Success);

        // Completion releases the lane, so the tail is granted next.
        let permit = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn queue_capacity_backpressure_rejects_new_acquires() {
        let mut settings = settings_with(vec![window(1, Duration::from_secs(60))], Vec::new());
        settings.queue_capacity = 2;
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();

        // One in flight, two pending: the backlog is at capacity.
        let first = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        let second = handle.acquire(metadata(OutboundPriority::NORMAL));
        let third = handle.acquire(metadata(OutboundPriority::NORMAL));

        let error = match handle.acquire(metadata(OutboundPriority::NORMAL)).await {
            Ok(_permit) => panic!("acquire unexpectedly succeeded beyond the queue capacity"),
            Err(error) => error,
        };
        assert_eq!(error, OutboundAcquireError::QueueFull);

        drop(second);
        drop(third);
        first.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_resolves_pending_acquires_with_closed() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        let first = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        let pending = handle.acquire(metadata(OutboundPriority::NORMAL));

        handle.shutdown();

        let error = match tokio::time::timeout(Duration::from_secs(5), pending).await {
            Ok(Ok(_permit)) => panic!("pending acquire resolved with a permit after shutdown"),
            Ok(Err(error)) => error,
            Err(_) => panic!("pending acquire hung after shutdown"),
        };
        assert_eq!(error, OutboundAcquireError::Closed);

        // Acquires after the shutdown fail fast with `Closed` as well.
        let error = match handle.acquire(metadata(OutboundPriority::NORMAL)).await {
            Ok(_permit) => panic!("acquire unexpectedly succeeded after shutdown"),
            Err(error) => error,
        };
        assert_eq!(error, OutboundAcquireError::Closed);

        // Completion of an already granted permit is best-effort.
        first.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_reports_pending_and_in_flight_counts() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        let first = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        let second = handle.acquire(metadata(OutboundPriority::NORMAL));
        tokio::task::yield_now().await;

        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 1);
        assert_eq!(snapshot.in_flight, 1);

        first.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let second = tokio::time::timeout(Duration::from_secs(1), second).await.unwrap().unwrap();
        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.in_flight, 1);
        second.complete(OutboundCompletion::Success);

        let snapshot = handle.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.in_flight, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn latest_wins_supersedes_the_replaced_waiter_and_inherits_its_position() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        // Consume the window so the latest-wins jobs stay pending.
        let in_flight = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();

        let replaced = handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7);
        let replacement = handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7);
        let fifo = handle.acquire(metadata(OutboundPriority::NORMAL));

        // The replaced waiter resolves explicitly, not with a fake permit.
        let error = match tokio::time::timeout(Duration::from_secs(1), replaced).await {
            Ok(Ok(_permit)) => panic!("replaced acquire resolved with a permit"),
            Ok(Err(error)) => error,
            Err(_) => panic!("replaced acquire hung"),
        };
        assert_eq!(error, OutboundAcquireError::Superseded);

        // The replacement inherited the queue position of the superseded
        // job: when the window frees, it is granted before the later FIFO
        // job.
        in_flight.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let mut replacement = replacement;
        let mut fifo = fifo;
        let winner = tokio::select! {
            _ = &mut replacement => "replacement",
            _ = &mut fifo => "fifo",
        };
        assert_eq!(winner, "replacement");
    }

    #[tokio::test(start_paused = true)]
    async fn latest_wins_replacement_is_admitted_at_queue_capacity() {
        let mut settings = settings_with(vec![window(1, Duration::from_secs(60))], Vec::new());
        settings.queue_capacity = 1;
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();

        // One in-flight permit, one pending latest-wins job: the backlog is
        // at capacity. A replacement of the pending job does not grow the
        // backlog and must be admitted, not rejected with `QueueFull`.
        let in_flight = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        let replaced = handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7);
        // The enqueue ingress has the capacity of one command: let the
        // actor dequeue `replaced` before the replacement is submitted.
        tokio::task::yield_now().await;
        let replacement = handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7);

        let error = match tokio::time::timeout(Duration::from_secs(1), replaced).await {
            Ok(Ok(_permit)) => panic!("replaced acquire resolved with a permit"),
            Ok(Err(error)) => error,
            Err(_) => panic!("replaced acquire hung"),
        };
        assert_eq!(error, OutboundAcquireError::Superseded);

        in_flight.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit =
            tokio::time::timeout(Duration::from_secs(1), replacement).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_latest_wins_key_at_queue_capacity_is_rejected() {
        let mut settings = settings_with(vec![window(1, Duration::from_secs(60))], Vec::new());
        settings.queue_capacity = 1;
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();

        let in_flight = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        let pending = handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7);
        tokio::pin!(pending);
        tokio::task::yield_now().await;
        assert!(futures::poll!(pending.as_mut()).is_pending());

        // A *new* latest-wins key would grow the backlog at capacity.
        let error = match handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 8).await {
            Ok(_permit) => panic!("acquire unexpectedly succeeded beyond the queue capacity"),
            Err(error) => error,
        };
        assert_eq!(error, OutboundAcquireError::QueueFull);

        in_flight.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn latest_wins_never_touches_an_in_flight_job() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        let in_flight =
            handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7).await.unwrap();
        // Latest-wins acquires with the same user key while the first job
        // is in flight: the in-flight permit is never replaced, only the
        // pending job is.
        let superseded = handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7);
        let pending = handle.acquire_latest_wins(metadata(OutboundPriority::NORMAL), 7);
        let error = match tokio::time::timeout(Duration::from_secs(1), superseded).await {
            Ok(Ok(_permit)) => panic!("superseded acquire resolved with a permit"),
            Ok(Err(error)) => error,
            Err(_) => panic!("superseded acquire hung"),
        };
        assert_eq!(error, OutboundAcquireError::Superseded);
        tokio::pin!(pending);
        assert!(futures::poll!(pending.as_mut()).is_pending());

        // The in-flight permit still completes normally; once the window
        // frees, the latest pending job is granted.
        in_flight.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), pending).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn independent_chats_do_not_block_each_other() {
        let queue = OutboundQueue::new_spawn(settings_with(
            Vec::new(),
            vec![window(1, Duration::from_secs(60))],
        ))
        .unwrap();
        let handle = queue.handle();

        let first_chat = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL)).await.unwrap();
        // Chat 2 is not blocked by chat 1's window.
        let second_chat = handle.acquire(chat_metadata(2, OutboundPriority::NORMAL)).await.unwrap();
        // A second chat-1 request is blocked by chat 1's window.
        let blocked = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        second_chat.complete(OutboundCompletion::Success);
        first_chat.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_ingress_is_bounded_by_the_channel_capacity() {
        let mut settings = settings();
        settings.queue_capacity = 4;
        let queue = OutboundQueue::new_spawn(settings).unwrap();
        let handle = queue.handle();

        // Не yield-им: актор не обработает команды, пока тест крутится на
        // single-thread runtime. Канал ёмкости 4 принимает 4 команды,
        // пятая падает fast-fail с `QueueFull`.
        let acquires: Vec<OutboundAcquire> =
            (0..4).map(|_| handle.acquire(metadata(OutboundPriority::NORMAL))).collect();
        let overflow = handle.acquire(metadata(OutboundPriority::NORMAL));
        let error = match tokio::time::timeout(Duration::from_secs(1), overflow).await {
            Ok(Ok(_permit)) => panic!("acquire unexpectedly succeeded beyond the ingress capacity"),
            Ok(Err(error)) => error,
            Err(_) => panic!("overflow acquire hung"),
        };
        assert_eq!(error, OutboundAcquireError::QueueFull);
        drop(acquires);
    }

    #[tokio::test]
    async fn dropping_the_last_external_handle_ends_the_actor_task() {
        // Реальное время: bounded timeout, а не paused clock.
        let (queue, actor) = OutboundQueue::new(settings()).unwrap();
        let acquire = queue.handle().acquire(metadata(OutboundPriority::NORMAL));
        drop(queue);
        drop(acquire);
        // Все внешние enqueue sender-ы упали: актор завершается сам.
        tokio::time::timeout(Duration::from_secs(5), actor)
            .await
            .expect("the actor task did not terminate after the last handle was dropped");
    }

    #[tokio::test(start_paused = true)]
    async fn set_limits_with_unchanged_values_keeps_the_consumed_budget() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        let permit = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        // Тот же набор лимитов: история должна сохраниться, burst невозможен.
        let limits = handle.limits().await.unwrap();
        handle.set_limits(limits).await.unwrap();

        let blocked = handle.acquire(metadata(OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        permit.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn set_limits_below_pending_weight_is_rejected() {
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(10, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        let heavy = OutboundMetadata {
            weight: NonZeroU32::new(10).unwrap(),
            ..metadata(OutboundPriority::NORMAL)
        };
        let holder = handle.acquire(heavy.clone()).await.unwrap();
        let pending = handle.acquire(heavy);
        tokio::task::yield_now().await;

        let error = handle
            .set_limits(OutboundLimits {
                global: vec![window(1, Duration::from_secs(60))],
                chat: Vec::new(),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            OutboundSetLimitsError::Invalid(
                SchedulerConfigError::PendingWeightExceedsWindow { .. }
            )
        ));

        // Старые лимиты остаются в силе, очередь продолжает работать.
        let limits = handle.limits().await.unwrap();
        assert_eq!(limits.global, vec![window(10, Duration::from_secs(60))]);
        drop(pending);
        holder.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn set_limits_preserves_history_of_unequal_global_windows() {
        // Два окна разной длительности: короткое уже prune-нуло событие,
        // длинное — нет. Ledger обязан браться из самого длинного окна,
        // иначе тот же набор лимитов сбросит budget длинного окна.
        let queue = OutboundQueue::new_spawn(settings_with(
            vec![window(1, Duration::from_secs(1)), window(1, Duration::from_secs(60))],
            Vec::new(),
        ))
        .unwrap();
        let handle = queue.handle();

        let permit = handle.acquire(metadata(OutboundPriority::NORMAL)).await.unwrap();
        // Короткое окно (1s) истекло, длинное (60s) ещё держит событие.
        tokio::time::advance(Duration::from_secs(2)).await;
        let limits = handle.limits().await.unwrap();
        handle.set_limits(limits).await.unwrap();

        let blocked = handle.acquire(metadata(OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        permit.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn set_limits_preserves_history_of_unequal_chat_windows() {
        let queue = OutboundQueue::new_spawn(settings_with(
            Vec::new(),
            vec![window(1, Duration::from_secs(1)), window(1, Duration::from_secs(60))],
        ))
        .unwrap();
        let handle = queue.handle();

        let permit = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL)).await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        let limits = handle.limits().await.unwrap();
        handle.set_limits(limits).await.unwrap();

        let blocked = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        permit.complete(OutboundCompletion::Success);
        tokio::time::advance(Duration::from_secs(61)).await;
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test]
    async fn zero_queue_capacity_is_rejected() {
        let mut settings = settings();
        settings.queue_capacity = 0;
        let error = match OutboundQueue::new_spawn(settings) {
            Ok(_queue) => panic!("zero queue capacity unexpectedly accepted"),
            Err(error) => error,
        };
        assert_eq!(error, SchedulerConfigError::ZeroQueueCapacity);
    }

    #[tokio::test(start_paused = true)]
    async fn set_limits_rejects_invalid_configuration() {
        let queue = OutboundQueue::new_spawn(settings()).unwrap();
        let handle = queue.handle();

        let error = handle
            .set_limits(OutboundLimits {
                global: vec![window(0, Duration::from_secs(60))],
                chat: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(
            error,
            OutboundSetLimitsError::Invalid(SchedulerConfigError::ZeroWindowCapacity)
        );

        // The previous limits stay in effect and the queue keeps working.
        let permit = handle.acquire(metadata(OutboundPriority::NORMAL));
        let permit = tokio::time::timeout(Duration::from_secs(1), permit).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn set_limits_wakes_up_blocked_candidates() {
        let queue = OutboundQueue::new_spawn(settings_with(
            Vec::new(),
            vec![window(1, Duration::from_secs(60))],
        ))
        .unwrap();
        let handle = queue.handle();

        let first = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL)).await.unwrap();
        let blocked = handle.acquire(chat_metadata(1, OutboundPriority::NORMAL));
        tokio::pin!(blocked);
        tokio::task::yield_now().await;
        assert!(futures::poll!(blocked.as_mut()).is_pending());

        // Removing the windows must wake the actor and grant the blocked
        // candidate immediately (no waiting for the window to expire).
        handle.set_limits(OutboundLimits { global: Vec::new(), chat: Vec::new() }).await.unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
        permit.complete(OutboundCompletion::Success);
        first.complete(OutboundCompletion::Success);
    }
}
