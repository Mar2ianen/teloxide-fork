//! Optional, panic-isolated outbound lifecycle observations.

use std::time::Instant;

/// Bounded hand-off capacity. A slow observer drops events rather than
/// applying backpressure to the scheduler actor.
pub(crate) const OBSERVER_CHANNEL_CAPACITY: usize = 256;

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
/// Callbacks run on a dedicated consumer thread behind a bounded channel;
/// they never run in the queue actor. A slow observer drops new events when
/// the channel is full, and a panic is isolated from both the actor and the
/// consumer thread.
pub trait OutboundObserver: Send + Sync + 'static {
    fn observe(&self, event: OutboundEvent);
}

/// An observer that intentionally discards all events.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopOutboundObserver;

impl OutboundObserver for NoopOutboundObserver {
    fn observe(&self, _event: OutboundEvent) {}
}
