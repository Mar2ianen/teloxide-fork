//! Deterministic outbound scheduling model.
//!
//! Commit 1 delivered the pure deterministic state machine; Commit 2 adds
//! the public actor, handle and completion-aware permit on top of it.
//!
//! The scheduler owns only the shared admission/rate/order layer:
//! priorities with aging, per-chat ordering lanes, rolling windows,
//! `RetryAfter` penalties and latest-wins replacement of pending jobs. It
//! never executes requests itself and never retries them (retry remains the
//! policy of the calling layer).

mod actor;
mod adaptor;
pub(crate) mod classify;
mod observability;
mod outbox;
mod scheduler;
mod types;

pub use actor::{
    OutboundAcquire, OutboundLane, OutboundPermit, OutboundQueue, OutboundQueueHandle,
};
pub use adaptor::{class, Outbound, OutboundRequestError, ScheduledRequest};
pub use observability::{NoopOutboundObserver, OutboundEvent, OutboundEventKind, OutboundObserver};
pub use outbox::{
    ClaimedOutboxRequest, InMemoryOutboxError, InMemoryOutboxStore, NewOutboxRequest,
    OutboundOutbox, OutboxEnqueueResult, OutboxExecutionError, OutboxExecutor, OutboxFailure,
    OutboxFailureKind, OutboxId, OutboxLease, OutboxRetry, OutboxSnapshot, OutboxStatus,
    OutboxStore, OutboxWorkerError, OutboxWorkerId, OutboxWorkerSettings,
};
pub use types::{
    AgingPolicy, OutboundAcquireError, OutboundChatKey, OutboundClass, OutboundClassLimits,
    OutboundClassWindowLimit, OutboundCompletion, OutboundCorrelationId, OutboundHint,
    OutboundLaneMode, OutboundLimits, OutboundMetadata, OutboundOverrides, OutboundPayload,
    OutboundPriority, OutboundQueueError, OutboundScope, OutboundSetLimitsError, OutboundSettings,
    OutboundSnapshot, SchedulerConfigError, WindowChatKind, WindowLimit,
};
