//! Types of the deterministic outbound scheduling model.
//!
//! Commit 1 kept these internal; Commit 2 lands the actor, the handle and
//! the completion-aware permit on top of the pure `SchedulerState`, so the
//! caller-facing types become public. The naming and shape are still
//! expected to be refined during the architectural review of each commit.

use std::{
    num::NonZeroU32,
    time::{Duration, Instant},
};

/// Numeric base priority of an outbound request; a higher value means a
/// higher priority. The named levels are just constants: callers may use
/// any value in the full `u8` range.
///
/// Selection is strictly priority-ordered; [`AgingPolicy`] raises the
/// effective priority of long-waiting jobs so that no job can starve
/// indefinitely (the guarantee requires `max_boost` to span the whole
/// range, see [`AgingPolicy`]). Priority orders the heads of different
/// ordering lanes but never reorders a single lane.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboundPriority(u8);

impl OutboundPriority {
    pub const LOWEST: Self = Self(0);
    pub const BACKGROUND: Self = Self(32);
    pub const NORMAL: Self = Self(128);
    pub const INTERACTIVE: Self = Self(192);
    pub const CRITICAL: Self = Self(224);
    pub const HIGHEST: Self = Self(u8::MAX);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Where a request applies and which rate limits it consumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutboundScope {
    Global,
    Chat(OutboundChatKey),
}

/// Chat identifier used for per-chat windows and penalties.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OutboundChatKey(u64);

impl OutboundChatKey {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Draft ordering-lane identifier: at most one in-flight request per lane,
/// and the lane is served strictly in enqueue order. Allocated by the
/// queue handle; callers never construct one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct OutboundLaneKey(pub(crate) u64);

/// Draft request class (message send, preview, chat action, ...). Part of
/// the request metadata so that latest-wins slots can never be spoofed with
/// different semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OutboundClass(u64);

impl OutboundClass {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Draft opaque identity of a job inside the scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct JobId(pub(crate) u64);

/// Full request metadata known at enqueue time, including the ordering
/// lane. Built by the actor from the caller's [`OutboundMetadata`] and the
/// lane (if any) the acquire was issued through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutboundMeta {
    pub(crate) scope: OutboundScope,
    pub(crate) lane: Option<OutboundLaneKey>,
    pub(crate) class: OutboundClass,
    pub(crate) priority: OutboundPriority,
    pub(crate) weight: NonZeroU32,
}

/// Caller-provided metadata of an acquire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboundMetadata {
    pub scope: OutboundScope,
    pub class: OutboundClass,
    pub priority: OutboundPriority,
    /// Accounting weight: the number of window capacity units the request
    /// consumes when granted. Must fit every window that applies to the
    /// scope, otherwise the acquire fails with
    /// [`OutboundQueueError::WeightExceedsWindow`].
    pub weight: NonZeroU32,
}

/// Stable user-provided correlation id carried by an acquire.
///
/// Draft placeholder: `OutboundMetadata` does not carry it yet — it will
/// return together with the observability commit (snapshot/observer
/// events), until then the id would be silently discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OutboundCorrelationId(u64);

impl OutboundCorrelationId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// How a new job relates to already pending jobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutboundEnqueueMode {
    /// Plain FIFO queueing.
    Fifo,
    /// Replace the pending job of the latest-wins slot identified by
    /// `user_key`. The slot itself is derived by the scheduler from the
    /// request metadata (scope, lane, class), so a replacement can never
    /// silently change what the inherited position and budget accounting
    /// mean. Only a job that has not been granted yet is replaced; in-flight
    /// requests are never cancelled by the scheduler. The replacement
    /// inherits the queue position and the scheduling age of the superseded
    /// job.
    ReplacePending { user_key: u64 },
}

/// The outcome of an enqueue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EnqueueOutcome {
    /// The newly created job.
    pub(crate) job: JobId,
    /// The pending job replaced by this enqueue, if any. The actor must
    /// complete its waiter with `Superseded`: it must neither silently
    /// disappear nor receive a fake permit.
    pub(crate) superseded: Option<JobId>,
}

/// Why an enqueue was rejected. The scheduler state is left unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EnqueueError {
    /// The pending backlog is at capacity and this enqueue would grow it
    /// (a latest-wins replacement does not grow the backlog and is admitted
    /// even at capacity).
    QueueFull,
    /// The latest-wins slot exists with a different accounting weight.
    /// Changing the weight of one semantic slot is almost always a
    /// classification error, so it is rejected instead of being silently
    /// turned into a second pending job.
    IncompatibleCoalesceMetadata,
    /// The request weight never fits at least one window that applies to
    /// its scope (the weight exceeds the window capacity). Such a job
    /// could never be granted, so it is rejected at enqueue time instead
    /// of waiting forever.
    WeightExceedsWindow { scope: OutboundScope, weight: NonZeroU32, capacity: u32 },
}

/// Why a queue could not be constructed, or why a settings update was
/// rejected. The settings are invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchedulerConfigError {
    /// A window with zero capacity can never admit any request.
    ZeroWindowCapacity,
    /// A window with zero duration never expires its history.
    ZeroWindowDuration,
    /// An aging quantum of zero would divide by zero.
    ZeroAgingQuantum,
    /// `max_boost` cannot lift a [`OutboundPriority::LOWEST`] job to
    /// [`OutboundPriority::HIGHEST`], so the anti-starvation guarantee
    /// cannot hold.
    AgingCannotReachHighest { max_boost: u8 },
    /// A zero queue capacity cannot bound an ingress channel (tokio's
    /// bounded channels require a positive buffer).
    ZeroQueueCapacity,
    /// A pending job would not fit any new window: lowering the limits
    /// below the weight of a pending job would make it ungrantable. The
    /// update is rejected as a whole and the previous limits stay in
    /// effect.
    PendingWeightExceedsWindow { scope: OutboundScope, weight: u32, capacity: u32 },
}

/// One granted job handed to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Grant {
    pub(crate) job: JobId,
}

/// How a granted request ended.
///
/// This is the public completion contract of an
/// [`crate::outbound::OutboundPermit`]; the actor converts the `RetryAfter`
/// duration into an absolute penalty deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutboundCompletion {
    Success,
    /// The request hit a `RetryAfter` limit. The penalty scope is explicit:
    /// a chat-scoped request can report a global flood penalty. The
    /// scheduler penalizes the reported scope for `duration` and never
    /// retries on its own.
    RetryAfter {
        scope: OutboundScope,
        duration: Duration,
    },
    Failed,
    /// The granted permit was dropped without an explicit completion.
    CancelledAfterGrant,
}

/// Fairness configuration for priority aging.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgingPolicy {
    /// One full quantum of waiting raises the effective priority by one
    /// level.
    pub quantum: Duration,
    /// Maximum number of levels the effective priority can be raised.
    ///
    /// The anti-starvation guarantee holds only when aging can lift a
    /// [`OutboundPriority::LOWEST`] job all the way to
    /// [`OutboundPriority::HIGHEST`]; construction enforces
    /// `max_boost >= HIGHEST - LOWEST`. With the guarantee in place a job
    /// is granted within `max_boost * quantum` plus the drain of the
    /// higher-priority backlog that arrived before its boost matured.
    pub max_boost: u8,
}

/// Sliding-window rate limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowLimit {
    pub capacity: u32,
    pub window: Duration,
}

/// Global and per-chat window limits. Each entry is one window of a
/// window set: a request must pass every window of the set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundLimits {
    pub global: Vec<WindowLimit>,
    pub chat: Vec<WindowLimit>,
}

/// Construction settings of an outbound queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundSettings {
    /// Rate windows.
    pub limits: OutboundLimits,
    /// Maximum number of pending (not yet granted) jobs. Enqueues beyond
    /// the capacity fail fast with [`OutboundQueueError::QueueFull`] and
    /// do not grow the backlog.
    pub queue_capacity: usize,
    /// Priority aging policy. `max_boost` must span the whole priority
    /// range, otherwise construction fails.
    pub aging: AgingPolicy,
}

/// Actor-level errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutboundQueueError {
    /// The queue actor is shut down or dead; the operation was not
    /// performed.
    Closed,
    /// The backlog is at capacity; the acquire was rejected without
    /// growing the queue.
    QueueFull,
    /// The acquire weight never fits an applicable window.
    WeightExceedsWindow { scope: OutboundScope, weight: NonZeroU32, capacity: u32 },
    /// A latest-wins acquire changed the accounting weight of an existing
    /// slot; the slot was left untouched.
    IncompatibleCoalesceMetadata,
}

/// Errors of an acquire future.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutboundAcquireError {
    /// The queue actor is shut down or dead.
    Closed,
    /// The backlog was at capacity when the acquire was enqueued.
    QueueFull,
    /// The acquire weight never fits an applicable window.
    WeightExceedsWindow { scope: OutboundScope, weight: NonZeroU32, capacity: u32 },
    /// A latest-wins acquire changed the accounting weight of an existing
    /// slot; the slot was left untouched.
    IncompatibleCoalesceMetadata,
    /// The pending job was replaced by a latest-wins acquire of the same
    /// slot; it is neither granted nor silently dropped.
    Superseded,
}

/// Errors of [`OutboundQueueHandle::set_limits`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutboundSetLimitsError {
    /// The queue actor is shut down or dead; the update was not performed.
    Closed,
    /// The limits are invalid; the previous limits stay in effect.
    Invalid(SchedulerConfigError),
}

impl From<OutboundQueueError> for OutboundAcquireError {
    fn from(error: OutboundQueueError) -> Self {
        match error {
            OutboundQueueError::Closed => Self::Closed,
            OutboundQueueError::QueueFull => Self::QueueFull,
            OutboundQueueError::WeightExceedsWindow { scope, weight, capacity } => {
                Self::WeightExceedsWindow { scope, weight, capacity }
            }
            OutboundQueueError::IncompatibleCoalesceMetadata => Self::IncompatibleCoalesceMetadata,
        }
    }
}

/// A point-in-time view of the queue state, for diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboundSnapshot {
    /// Pending jobs (enqueued, not yet granted).
    pub pending: usize,
    /// Granted jobs whose permits are still in flight.
    pub in_flight: usize,
    /// Candidates that failed admission and are waiting for a deadline.
    pub blocked: usize,
    /// Jobs waiting for their `not_before` moment.
    pub delayed: usize,
    /// Candidate-heap entries (may include lazy stale entries).
    pub candidates: usize,
    /// Windows held back for blocked top-aged candidates.
    pub reservations: usize,
}

/// What the actor should do after a failed `grant_ready`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SchedulerWakeup {
    /// `grant_ready` should be called again right away. A normal result
    /// right after `grant_ready` is never `Immediate`: that would mean the
    /// grant loop left an admissible job behind.
    Immediate,
    /// The earliest moment at which something may become grantable.
    At(Instant),
    /// Nothing time-based will change; wait for an external event (an
    /// enqueue, a completion or a penalty).
    ExternalEvent,
}
