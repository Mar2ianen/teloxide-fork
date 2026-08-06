//! Compatibility `Throttle` built on top of the outbound scheduler.
//!
//! Commit 5 of the outbound scheduler migration: the legacy
//! [`Throttle`](super::throttle::Throttle) worker is kept and this module
//! implements the same public contract over the
//! [`OutboundQueue`](crate::outbound::OutboundQueue) instead, so the two
//! engines can be compared head-to-head on paused time (see `tests`).
//!
//! Reproduced legacy semantics:
//!
//! - the throttled method allowlist matches the legacy `requester_impl` exactly
//!   (25 message-send methods; everything else passes through untouched) — it
//!   is a compatibility predicate, NOT derived from the `class` taxonomy
//!   (`copy_message(s)`/`forward_message(s)` are `OTHER`);
//! - every throttled request is accounted with weight 1: the legacy worker
//!   counts API calls, not messages, so a media group of ten items costs one
//!   unit (the scheduler's generated batch weights would make it inadmissible
//!   against `messages_per_sec_chat = 1`);
//! - per-chat FIFO: all throttled requests share one priority, so the
//!   scheduler's (priority, sequence) arbitration plus the shared per-chat
//!   windows reproduce the legacy "request order in chats is not changed";
//! - shared/owned semantics: `CompatRequest` holds the inner request as
//!   `Arc<R>` like the legacy wrapper. Cloning the WRAPPER makes an owned
//!   `send()` shared — `send()` runs `Arc::try_unwrap` and falls back to inner
//!   `send_ref()` whenever another clone exists, even with `retry = false`.
//!   `send_ref()` clones only the `Arc` (a side-effecting `R::clone` is never
//!   invoked), and `payload_mut()` goes through `Arc::make_mut`. A TRULY owned
//!   execution runs the inner request through `IntoFuture::into_future`
//!   (`owned.take().unwrap().await`), NOT through `Request::send` — in the
//!   regular `retry = false` path and in the direct-send fallback alike,
//!   because the legacy worker only ever calls `.await` on the taken request;
//! - a `RetryAfter` outcome registers a GLOBAL penalty: the legacy worker
//!   freezes everything until the backoff expires, so the compatibility layer
//!   completes the permit with [`crate::outbound::OutboundScope::Global`]; the
//!   request then sleeps until the penalty expires and only THEN re-queues —
//!   like the legacy worker, which sleeps outside its queue, so requests that
//!   arrive during the freeze keep their place ahead of the retry. The penalty
//!   deadline is anchored at the moment the error was OBSERVED — ONE shared
//!   timestamp drives the scheduler penalty, the local retry sleep and the
//!   compat-side freeze deadline (`OutboundPermit::complete_observed_at`), so a
//!   completion processed late by the actor cannot extend a freeze whose
//!   deadline already passed — exactly like the legacy worker receiving an
//!   expired absolute `until`. The outcome is classified exactly once
//!   (`AsResponseParameters::retry_after` is not required to be pure);
//! - the backlog is bounded by `messages_per_sec_overall` (the legacy channel
//!   capacity): a capacity semaphore parks requests in FIFO order when the
//!   backlog is full, `on_queue_full` fires at most once per 4 seconds (both
//!   when the last slot is taken and when a request has to wait), and a slot
//!   freed by a grant, a cancellation or the actor's death automatically wakes
//!   the next waiter — no lost wakeups, no dead waiters. The notification is
//!   anchored at the ENQUEUE ACCEPTANCE of the last slot — the moment the actor
//!   put the job into the backlog, BEFORE the rate-limit wait and the grant
//!   (the two-phase `OutboundQueueHandle::enqueue`/`OutboundGrant` acquire
//!   exposes the admission point): a last pending request cancelled before its
//!   grant cannot erase the full-backlog event that already happened, and a
//!   request granted at t=0 reports the backlog of t=0. A request going
//!   straight into a direct send because the actor died before the acceptance
//!   never reports a full backlog, and the callback is silent while a global
//!   freeze is active — exactly like the legacy worker, which does not run its
//!   queue checks while frozen; a full-backlog event deferred during the freeze
//!   is emitted EXACTLY ONCE at the thaw boundary (the legacy worker still
//!   reads the messages out of its bounded channel after the thaw, even if
//!   every pending request was cancelled before it) and cleared when the actor
//!   dies (a dead worker never runs the callback again). A saturation monitor
//!   re-fires the callback while the backlog stays full (the legacy worker
//!   re-checks on every iteration), sleeps past the freeze deadline before
//!   re-checking, and exits when a slot frees up, resetting its flag BEFORE
//!   releasing the observed permit so a fresh saturation wave spawns a new
//!   monitor. The slot is held while the job is pending and released on grant;
//!   a `QueueFull` rejection (only possible behind an unprocessed cancel) keeps
//!   the slot and retries, preserving the FIFO order, and the direct-send
//!   fallback on the actor's death releases the slot BEFORE the direct request
//!   runs. The cancellation identity is a CLIENT TOKEN minted before the
//!   enqueue is sent: dropping the enqueue/grant future sends `Cancel { token
//!   }`, and the actor applies it whether the enqueue was already processed
//!   (token mapped to the job) or still in flight (the cancel is remembered and
//!   applied on acceptance) — a dropped future can never leave a ghost job
//!   pending. The completion is NON-BLOCKING
//!   ([`crate::outbound::OutboundPermit::complete`], not the adaptor's
//!   per-request barrier): the legacy request loop returns its result right
//!   after the inner request finished, and the ordering is preserved because
//!   the completion lands synchronously in the actor's lifecycle channel, which
//!   is drained before any later enqueue;
//! - `limits()`/`set_limits()` keep the legacy async API; the queue actor is
//!   the single source of truth (there is no client-side mirror to diverge,
//!   even if a `set_limits` future is cancelled mid-flight), the scheduler's
//!   `set_limits` carries the rate history over, and `limits()` panics if the
//!   actor is gone — exactly like the legacy worker dying.
//!
//! Documented temporary incompatibilities:
//!
//! - `Settings::check_slow_mode` is ignored (the legacy worker asks `get_chat`
//!   to skip a freeze caused by slow mode);
//! - channel usernames are canonicalized (the legacy hashed the raw spelling,
//!   so `@Foo` and `foo` were different identities);
//! - zero-capacity limits: the legacy worker accepts `messages_per_min_chat =
//!   0` (requests to such chats wait forever) and `set_limits` with zeroes
//!   simply pauses the traffic. The scheduler rejects zero-capacity windows at
//!   construction and in `set_limits`, so `ThrottleCompat::set_limits` with a
//!   zero limit is rejected and logged while the previous limits stay in
//!   effect. This MUST be resolved (either zero-capacity support in the
//!   scheduler or a compat-side "blocked until next set_limits" gate) before
//!   the public `Throttle` switches to this layer.
mod request;
mod requester_impl;

#[cfg(test)]
mod tests;

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{sync::Semaphore, time::Instant};

use crate::{
    errors::AsResponseParameters,
    outbound::{OutboundLimits, OutboundQueue, OutboundSettings, WindowChatKind, WindowLimit},
    requests::Requester,
};

use super::throttle::{Limits, Settings};

pub use request::{CompatRequest, CompatSend};

/// The callback type of [`Settings::on_queue_full`].
type BoxedFnMut<I, O> = Box<dyn FnMut(I) -> O + Send>;
type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Minimum delay between two `on_queue_full` invocations, mirroring the
/// legacy worker's `QUEUE_FULL_DELAY`.
const QUEUE_FULL_DELAY: Duration = Duration::from_secs(4);

/// The saturation monitor's re-check period: one rate-limit window plus a
/// millisecond of headroom, so the legacy `elapsed() > QUEUE_FULL_DELAY`
/// boundary is always crossed when the monitor wakes.
const SATURATION_CHECK_PERIOD: Duration = Duration::from_millis(4001);

/// A `Throttle`-compatible wrapper over the outbound scheduler.
///
/// Same public contract as [`Throttle`](super::throttle::Throttle):
/// [`Limits`], [`Settings`], `limits()`/`set_limits()`, `inner()`,
/// `into_inner()` and the throttled method allowlist. The worker future
/// returned by `new`/`with_settings` is the outbound actor future.
#[derive(Clone)]
pub struct ThrottleCompat<B> {
    bot: B,
    queue: OutboundQueue,
    state: Arc<CompatState>,
}

/// Same `Debug` contract as the legacy [`Throttle`](super::throttle::Throttle)
/// (`#[derive(Debug)]`): the callback is not printable and is skipped.
impl<B: std::fmt::Debug> std::fmt::Debug for ThrottleCompat<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThrottleCompat")
            .field("bot", &self.bot)
            .field("queue", &self.queue)
            .field("state", &self.state)
            .finish()
    }
}

impl std::fmt::Debug for CompatState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompatState")
            .field("retry", &self.retry)
            .field("available_slots", &self.pending_slots.available_permits())
            .field("queue_capacity", &self.queue_capacity)
            .field("on_queue_full", &"<callback>")
            .field("last_queue_full", &self.last_queue_full)
            .field("saturation_monitor_active", &self.monitor_active)
            .field("freeze_until", &self.freeze_until)
            .field("deferred_full", &self.deferred_full)
            .finish()
    }
}

/// State shared between the wrapper and every in-flight request.
struct CompatState {
    /// Whether a `RetryAfter` outcome re-sends the request.
    retry: bool,
    /// Backlog bound in permits — the role of the legacy bounded channel
    /// (capacity `messages_per_sec_overall`). A request holds one permit
    /// while its job is pending and releases it on grant, cancellation or
    /// the actor's death, so the next waiter is woken automatically,
    /// cancellation-safe and in FIFO order.
    pending_slots: Arc<Semaphore>,
    /// The value passed to `on_queue_full` when it fires.
    queue_capacity: usize,
    on_queue_full: Mutex<BoxedFnMut<usize, BoxedFuture>>,
    /// Last `on_queue_full` invocation, for the 4-second rate limit.
    last_queue_full: Mutex<Instant>,
    /// Whether the saturation monitor task is running (see
    /// [`ensure_saturation_monitor`]).
    monitor_active: AtomicBool,
    /// The latest known GLOBAL freeze deadline (from `RetryAfter`
    /// outcomes). While a freeze is active the legacy worker does not run
    /// its queue checks, so `on_queue_full` must stay silent and the
    /// saturation monitor must sleep until the freeze ends.
    freeze_until: Mutex<Option<tokio::time::Instant>>,
    /// A full-backlog event that happened while a global freeze was
    /// active (the last slot was accepted during the freeze). The legacy
    /// worker does not run its queue checks while frozen, but the
    /// messages are already IN its bounded channel: after the thaw it
    /// reads them, sees `queue.len() == capacity()` and fires the
    /// callback — even if every pending request was cancelled before the
    /// thaw. The deferred event is emitted exactly once at the thaw
    /// boundary, and cleared when the actor dies (a dead legacy worker
    /// would never run the callback again).
    deferred_full: AtomicBool,
    /// The outbound queue, kept for the saturation monitor's actor
    /// liveness probe (a dead actor must clear the deferred event).
    queue: OutboundQueue,
}

/// Maps the legacy [`Limits`] to scheduler windows.
///
/// The per-chat minute limit is split by chat kind, reproducing the legacy
/// `messages_per_min_chat` vs `messages_per_min_channel_or_supergroup`
/// distinction; the per-second limits become 1-second windows.
fn to_outbound_limits(limits: Limits) -> OutboundLimits {
    OutboundLimits {
        global: vec![WindowLimit::new(limits.messages_per_sec_overall, Duration::from_secs(1))],
        chat: vec![
            WindowLimit::new(limits.messages_per_sec_chat, Duration::from_secs(1)),
            WindowLimit::for_chat_kind(
                limits.messages_per_min_chat,
                Duration::from_secs(60),
                WindowChatKind::NonChannel,
            ),
            WindowLimit::for_chat_kind(
                limits.messages_per_min_channel_or_supergroup,
                Duration::from_secs(60),
                WindowChatKind::ChannelOrSupergroup,
            ),
        ],
    }
}

/// Maps scheduler windows back to the legacy [`Limits`].
///
/// The inverse of [`to_outbound_limits`]: the 1-second global window is
/// `messages_per_sec_overall`, the 1-second chat window is
/// `messages_per_sec_chat`, and the two kind-specific minute windows are
/// `messages_per_min_chat` and `messages_per_min_channel_or_supergroup`.
/// Windows of other shapes (set by a non-compat caller) are ignored
/// defensively.
fn to_legacy_limits(outbound: OutboundLimits) -> Limits {
    let mut limits = Limits::default();
    for window in outbound.global {
        if window.window == Duration::from_secs(1) {
            limits.messages_per_sec_overall = window.capacity;
        }
    }
    for window in outbound.chat {
        match window.kind {
            _ if window.window == Duration::from_secs(1) => {
                limits.messages_per_sec_chat = window.capacity;
            }
            WindowChatKind::NonChannel if window.window == Duration::from_secs(60) => {
                limits.messages_per_min_chat = window.capacity;
            }
            WindowChatKind::ChannelOrSupergroup if window.window == Duration::from_secs(60) => {
                limits.messages_per_min_channel_or_supergroup = window.capacity;
            }
            _ => {}
        }
    }
    limits
}

impl<B> ThrottleCompat<B> {
    /// Creates the wrapper alongside the outbound actor future.
    ///
    /// Note: requests will only be sent while the returned future is
    /// polled/spawned/awaited (same contract as [`Throttle::new`]).
    pub fn new(bot: B, limits: Limits) -> (Self, impl Future<Output = ()>)
    where
        B: Requester + Clone,
        B::Err: AsResponseParameters,
    {
        let settings = Settings { limits, ..<_>::default() };
        Self::with_settings(bot, settings)
    }

    /// Creates the wrapper with custom [`Settings`].
    pub fn with_settings(bot: B, settings: Settings) -> (Self, impl Future<Output = ()>)
    where
        B: Requester + Clone,
        B::Err: AsResponseParameters,
    {
        let Settings { limits, on_queue_full, retry, check_slow_mode } = settings;
        if check_slow_mode {
            log::warn!("ThrottleCompat: `check_slow_mode` is not supported yet and is ignored");
        }
        let queue_capacity = limits.messages_per_sec_overall as usize;
        let queue_settings = OutboundSettings {
            limits: to_outbound_limits(limits),
            queue_capacity,
            // The default aging policy preserves the FIFO order of equal
            // priorities (aging is monotonic in waiting time), which is
            // what the legacy worker guarantees.
            ..<_>::default()
        };
        let (queue, actor) = OutboundQueue::new(queue_settings)
            .unwrap_or_else(|error| panic!("invalid throttle limits: {error:?}"));
        let state = Arc::new(CompatState {
            retry,
            pending_slots: Arc::new(Semaphore::new(queue_capacity)),
            queue_capacity,
            on_queue_full: Mutex::new(on_queue_full),
            // Initialized just PAST the rate-limit window so the FIRST
            // overflow fires immediately: the legacy worker initializes to
            // `now - QUEUE_FULL_DELAY` and fires when `elapsed() > DELAY`,
            // which under real clocks is always true; under a paused test
            // clock the elapsed time would be exactly the delay, so a
            // millisecond of headroom reproduces the legacy behavior.
            last_queue_full: Mutex::new(
                Instant::now()
                    .checked_sub(QUEUE_FULL_DELAY + Duration::from_millis(1))
                    .unwrap_or_else(Instant::now),
            ),
            monitor_active: AtomicBool::new(false),
            freeze_until: Mutex::new(None),
            deferred_full: AtomicBool::new(false),
            queue: queue.clone(),
        });
        let this = Self { bot, queue, state };
        (this, actor)
    }

    /// Creates the wrapper and spawns the actor with `tokio::spawn`.
    pub fn new_spawn(bot: B, limits: Limits) -> Self
    where
        B: Requester + Clone + Send + Sync + 'static,
        B::Err: AsResponseParameters,
    {
        let (this, actor) = Self::new(bot, limits);
        tokio::spawn(actor);
        this
    }

    /// Creates the wrapper with custom [`Settings`] and spawns the actor.
    pub fn spawn_with_settings(bot: B, settings: Settings) -> Self
    where
        B: Requester + Clone + Send + Sync + 'static,
        B::Err: AsResponseParameters,
    {
        let (this, actor) = Self::with_settings(bot, settings);
        tokio::spawn(actor);
        this
    }

    /// Allows to access inner bot.
    pub fn inner(&self) -> &B {
        &self.bot
    }

    /// Unwraps inner bot.
    pub fn into_inner(self) -> B {
        self.bot
    }

    /// Returns currently used [`Limits`].
    ///
    /// The limits are read from the queue actor — the scheduler is the
    /// single source of truth. There is no client-side mirror, so the
    /// value always matches what the queue enforces, even if a
    /// `set_limits` future was cancelled mid-flight. Like the legacy
    /// [`Throttle::limits`], this panics if the worker is gone (a silent
    /// default could hand the caller a completely wrong state).
    ///
    /// [`Throttle::limits`]: super::throttle::Throttle::limits
    pub async fn limits(&self) -> Limits {
        const WORKER_DIED: &str = "worker died before last `Throttle` instance";
        let outbound = self.queue.handle().limits().await.expect(WORKER_DIED);
        to_legacy_limits(outbound)
    }

    /// Sets new limits.
    ///
    /// The scheduler's `set_limits` carries the already debited history
    /// over, so changing limits does not reset the rate budget (same
    /// semantics as the legacy worker, which keeps its history). If the
    /// queue rejects the new limits (e.g. a zero capacity, see the module
    /// docs), the previous limits stay in effect and the error is logged.
    ///
    /// There is deliberately no client-side mirror: [`ThrottleCompat::limits`]
    /// always reads the actor, so cancelling this future after the actor
    /// committed the update cannot leave the two views diverged.
    pub async fn set_limits(&self, new: Limits) {
        match self.queue.handle().set_limits(to_outbound_limits(new)).await {
            Ok(()) => {}
            Err(error) => log::error!("ThrottleCompat: set_limits rejected: {error:?}"),
        }
    }
}

/// Fires `on_queue_full` at most once per [`QUEUE_FULL_DELAY`], passing
/// the backlog bound as the pending count (the legacy worker fires when
/// `queue.len() == capacity` and passes the length).
fn notify_queue_full(state: &Arc<CompatState>) {
    let mut last = state.last_queue_full.lock().unwrap();
    // The legacy worker fires when `elapsed() > QUEUE_FULL_DELAY`; the
    // boundary is kept identical.
    if last.elapsed() <= QUEUE_FULL_DELAY {
        return;
    }
    *last = Instant::now();
    let pending = state.queue_capacity;
    let future = (state.on_queue_full.lock().unwrap())(pending);
    tokio::spawn(future);
}

/// Whether a GLOBAL freeze is active right now (the legacy worker
/// freezes everything and does not run its queue checks while frozen).
fn freeze_active(state: &CompatState) -> bool {
    state.freeze_until.lock().unwrap().is_some_and(|until| tokio::time::Instant::now() < until)
}

/// Records a GLOBAL freeze deadline, extending an existing one only when
/// the new deadline is later (max semantics, like the scheduler's
/// penalties). Called on every `RetryAfter` outcome — even when the
/// request will not be retried, because the legacy worker freezes
/// regardless of the retry flag. The deadline is anchored at the
/// `observed_at` moment passed in by the caller: the scheduler penalty,
/// the local retry sleep and this compat-side freeze must all share ONE
/// observation timestamp, otherwise a late-processed completion could
/// produce a window in which the monitor thinks the freeze is over while
/// the scheduler still holds the global penalty.
fn record_freeze_at(state: &CompatState, observed_at: tokio::time::Instant, duration: Duration) {
    let until = observed_at + duration;
    let mut freeze = state.freeze_until.lock().unwrap();
    if freeze.is_none_or(|old| until > old) {
        *freeze = Some(until);
    }
}

/// Starts the saturation monitor if one is not already running.
///
/// The legacy worker re-checks `queue.len() == capacity()` on EVERY
/// iteration and re-fires the callback once the 4-second rate limit has
/// expired, so a backlog that stays full for a long time produces several
/// notifications even without new requests. The monitor reproduces that:
/// while the semaphore stays empty it re-fires the callback (rate-limited)
/// and exits as soon as a slot frees up.
fn ensure_saturation_monitor(state: &Arc<CompatState>) {
    if state.monitor_active.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::spawn(saturation_monitor(Arc::clone(state)));
}

/// The saturation monitor task: see [`ensure_saturation_monitor`].
///
/// While a global freeze is active the monitor sleeps until the freeze
/// ends (the legacy worker does not run its checks while frozen) and only
/// then re-checks the saturation.
async fn saturation_monitor(state: Arc<CompatState>) {
    loop {
        let now = tokio::time::Instant::now();
        let freeze_until = *state.freeze_until.lock().unwrap();
        let wake = match freeze_until {
            Some(until) if until > now => until + Duration::from_millis(1),
            _ => now + SATURATION_CHECK_PERIOD,
        };
        tokio::time::sleep_until(wake).await;
        if freeze_active(&state) {
            // The freeze was extended while the monitor slept: wait for
            // the new deadline.
            continue;
        }
        // The thaw boundary: a full-backlog event deferred during the
        // freeze is emitted exactly once, even if every pending request
        // was cancelled before the thaw (the legacy worker still reads
        // the messages out of its bounded channel). A dead actor clears
        // it: the legacy callback would never run again.
        if state.deferred_full.swap(false, Ordering::SeqCst) {
            if state.queue.handle().limits().await.is_none() {
                state.monitor_active.store(false, Ordering::Release);
                return;
            }
            notify_queue_full(&state);
        }
        match state.pending_slots.clone().try_acquire_owned() {
            Ok(permit) => {
                // The flag is cleared BEFORE the permit is released: a
                // waiter that takes the freed slot (the last one) and
                // calls `ensure_saturation_monitor` must observe `false`
                // and spawn the next monitor — otherwise a re-saturated
                // backlog would be left without notifications.
                state.monitor_active.store(false, Ordering::Release);
                drop(permit);
                return;
            }
            Err(_) => notify_queue_full(&state),
        }
    }
}
