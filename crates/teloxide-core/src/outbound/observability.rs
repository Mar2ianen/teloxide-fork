//! Optional, panic-isolated outbound lifecycle observations.

use std::time::Instant;

use super::types::{OutboundCompletion, OutboundCorrelationId};

/// Lifecycle phase emitted by an outbound queue observer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboundEventKind {
    Enqueued,
    Granted,
    Started,
    Completed { outcome: OutboundCompletion },
}

/// A point-in-time outbound lifecycle event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundEvent {
    pub kind: OutboundEventKind,
    pub correlation_id: Option<OutboundCorrelationId>,
    pub at: Instant,
}

/// Receives outbound lifecycle events.
///
/// Observer callbacks run inside the queue actor and are isolated with
/// `catch_unwind`; a faulty observer cannot take down admission processing.
pub trait OutboundObserver: Send + Sync + 'static {
    fn observe(&self, event: OutboundEvent);
}

/// An observer that intentionally discards all events.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopOutboundObserver;

impl OutboundObserver for NoopOutboundObserver {
    fn observe(&self, _event: OutboundEvent) {}
}
