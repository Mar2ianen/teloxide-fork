//! Draft types of the deterministic outbound scheduling model.
//!
//! These are internal drafts for Commit 1: no stable API, no public exports.
//! The naming and shape are expected to be refined during the architectural
//! review before the actor (Commit 2) is built on top.

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
pub(crate) struct OutboundPriority(pub(crate) u8);

impl OutboundPriority {
    pub(crate) const LOWEST: Self = Self(0);
    pub(crate) const BACKGROUND: Self = Self(32);
    pub(crate) const NORMAL: Self = Self(128);
    pub(crate) const INTERACTIVE: Self = Self(192);
    pub(crate) const CRITICAL: Self = Self(224);
    pub(crate) const HIGHEST: Self = Self(u8::MAX);

    pub(crate) const fn new(value: u8) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> u8 {
        self.0
    }
}

/// Where a request applies and which rate limits it consumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OutboundScope {
    Global,
    Chat(OutboundChatKey),
}

/// Draft chat identifier used for per-chat windows and penalties.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OutboundChatKey(pub(crate) u64);

/// Draft ordering-lane identifier: at most one in-flight request per lane,
/// and the lane is served strictly in enqueue order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct OutboundLaneKey(pub(crate) u64);

/// Draft request class (message send, preview, chat action, ...). Part of
/// the request metadata so that latest-wins slots can never be spoofed with
/// different semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OutboundClass(pub(crate) u64);

/// Draft opaque identity of a job inside the scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct JobId(pub(crate) u64);

/// Request metadata known at enqueue time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutboundMeta {
    pub(crate) scope: OutboundScope,
    pub(crate) lane: Option<OutboundLaneKey>,
    pub(crate) class: OutboundClass,
    pub(crate) priority: OutboundPriority,
    pub(crate) weight: NonZeroU32,
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
    /// The pending job replaced by this enqueue, if any. The caller must
    /// complete its waiter with `Superseded`: it must neither silently
    /// disappear nor receive a fake permit.
    pub(crate) superseded: Option<JobId>,
}

/// Why an enqueue was rejected. The scheduler state is left unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EnqueueError {
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

/// Why a scheduler could not be constructed. The configuration is invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SchedulerConfigError {
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
}

/// One granted job handed to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Grant {
    pub(crate) job: JobId,
}

/// How a granted request ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutboundCompletion {
    Success,
    /// The request hit a `RetryAfter` limit. The penalty scope is explicit:
    /// a chat-scoped request can report a global flood penalty. The
    /// scheduler penalizes the reported scope until `until` and never
    /// retries on its own.
    RetryAfter {
        scope: OutboundScope,
        until: Instant,
    },
    Failed,
    /// The granted permit was dropped without an explicit completion.
    CancelledAfterGrant,
}

/// Fairness configuration for priority aging.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AgingPolicy {
    /// One full quantum of waiting raises the effective priority by one
    /// level.
    pub(crate) quantum: Duration,
    /// Maximum number of levels the effective priority can be raised.
    ///
    /// The anti-starvation guarantee holds only when aging can lift a
    /// [`OutboundPriority::LOWEST`] job all the way to
    /// [`OutboundPriority::HIGHEST`]; `SchedulerState::new` enforces
    /// `max_boost >= HIGHEST - LOWEST`. With the guarantee in place a job
    /// is granted within `max_boost * quantum` plus the drain of the
    /// higher-priority backlog that arrived before its boost matured.
    pub(crate) max_boost: u8,
}

/// Sliding-window rate limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WindowLimit {
    pub(crate) capacity: u32,
    pub(crate) window: Duration,
}

/// Global and per-chat window limits. Each entry is one window of a
/// window set: a request must pass every window of the set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutboundLimits {
    pub(crate) global: Vec<WindowLimit>,
    pub(crate) chat: Vec<WindowLimit>,
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
