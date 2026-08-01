use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use teloxide_core::types::ChatId;

use super::{DraftRevision, DrafterMode, DrafterOperation};

/// Lifecycle event emitted by a drafter without payload or user text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrafterEvent {
    pub kind: DrafterEventKind,
    pub mode: DrafterMode,
    pub chat_id: ChatId,
    pub segment: u64,
    pub revision: Option<DraftRevision>,
    pub operation: Option<DrafterOperation>,
}

/// Events exposed by the scheduler for tracing, metrics and tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrafterEventKind {
    Spawn,
    Update,
    Coalesced,
    PreviewStart,
    PreviewSuccess,
    PreviewError,
    PreviewTimeout,
    RetryAfter,
    Refresh,
    FlushStart,
    FlushComplete,
    SegmentCommit,
    SegmentCommitError,
    SegmentRotate,
    FinishStart,
    FinalSuccess,
    FinalError,
    AbortError,
    CleanupError,
    Abort,
    WorkerStop,
}

/// Receives scheduler lifecycle events.
pub trait DrafterObserver: Send + Sync + 'static {
    fn record(&self, event: DrafterEvent);
}

/// Observer used by constructors that do not need instrumentation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDrafterObserver;

impl DrafterObserver for NoopDrafterObserver {
    fn record(&self, _event: DrafterEvent) {}
}

/// Emits payload-free lifecycle events through the crate's `tracing` feature.
#[cfg(feature = "tracing")]
#[derive(Clone, Copy, Debug, Default)]
pub struct TracingDrafterObserver;

#[cfg(feature = "tracing")]
impl DrafterObserver for TracingDrafterObserver {
    fn record(&self, event: DrafterEvent) {
        tracing::debug!(
            target: "teloxide::drafter",
            event = ?event.kind,
            mode = ?event.mode,
            chat_id = event.chat_id.0,
            segment = event.segment,
            revision = event.revision.map(DraftRevision::get),
            operation = ?event.operation,
            "drafter lifecycle"
        );
    }
}

pub(crate) fn default_observer() -> Arc<dyn DrafterObserver> {
    #[cfg(feature = "tracing")]
    {
        Arc::new(TracingDrafterObserver)
    }

    #[cfg(not(feature = "tracing"))]
    Arc::new(NoopDrafterObserver)
}

/// Thread-safe event counters suitable for a bot-local metrics registry.
#[derive(Clone, Default)]
pub struct DrafterMetricsCollector {
    counters: Arc<MetricsCounters>,
}

#[derive(Default)]
struct MetricsCounters {
    received_updates: AtomicU64,
    sent_previews: AtomicU64,
    coalesced_updates: AtomicU64,
    refresh_requests: AtomicU64,
    retry_count: AtomicU64,
    rate_limit_count: AtomicU64,
    segment_count: AtomicU64,
    preview_failures: AtomicU64,
    final_failures: AtomicU64,
    cleanup_failures: AtomicU64,
}

/// Snapshot of the counters maintained by [`DrafterMetricsCollector`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DrafterMetricsSnapshot {
    pub received_updates: u64,
    pub sent_previews: u64,
    pub coalesced_updates: u64,
    pub refresh_requests: u64,
    pub retry_count: u64,
    pub rate_limit_count: u64,
    pub segment_count: u64,
    pub preview_failures: u64,
    pub final_failures: u64,
    pub cleanup_failures: u64,
}

impl DrafterMetricsCollector {
    #[must_use]
    pub fn snapshot(&self) -> DrafterMetricsSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        DrafterMetricsSnapshot {
            received_updates: load(&self.counters.received_updates),
            sent_previews: load(&self.counters.sent_previews),
            coalesced_updates: load(&self.counters.coalesced_updates),
            refresh_requests: load(&self.counters.refresh_requests),
            retry_count: load(&self.counters.retry_count),
            rate_limit_count: load(&self.counters.rate_limit_count),
            segment_count: load(&self.counters.segment_count),
            preview_failures: load(&self.counters.preview_failures),
            final_failures: load(&self.counters.final_failures),
            cleanup_failures: load(&self.counters.cleanup_failures),
        }
    }
}

impl DrafterObserver for DrafterMetricsCollector {
    fn record(&self, event: DrafterEvent) {
        let counter = match event.kind {
            DrafterEventKind::Update => Some(&self.counters.received_updates),
            DrafterEventKind::PreviewSuccess => Some(&self.counters.sent_previews),
            DrafterEventKind::Coalesced => Some(&self.counters.coalesced_updates),
            DrafterEventKind::Refresh => Some(&self.counters.refresh_requests),
            DrafterEventKind::RetryAfter => Some(&self.counters.retry_count),
            DrafterEventKind::SegmentCommit => Some(&self.counters.segment_count),
            DrafterEventKind::PreviewError | DrafterEventKind::PreviewTimeout => {
                Some(&self.counters.preview_failures)
            }
            DrafterEventKind::FinalSuccess
            | DrafterEventKind::SegmentCommitError
            | DrafterEventKind::AbortError => None,
            DrafterEventKind::FinalError => Some(&self.counters.final_failures),
            DrafterEventKind::CleanupError => Some(&self.counters.cleanup_failures),
            DrafterEventKind::Spawn
            | DrafterEventKind::PreviewStart
            | DrafterEventKind::FlushStart
            | DrafterEventKind::FlushComplete
            | DrafterEventKind::SegmentRotate
            | DrafterEventKind::FinishStart
            | DrafterEventKind::Abort
            | DrafterEventKind::WorkerStop => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }

        if matches!(event.kind, DrafterEventKind::RetryAfter) {
            self.counters.rate_limit_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}
