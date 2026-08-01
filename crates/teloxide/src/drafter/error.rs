use std::{fmt, time::Duration};

/// A monotonically increasing source revision.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct DraftRevision(pub u64);

impl DraftRevision {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Error returned synchronously by a producer after its lifecycle has closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DraftPushError {
    Closed,
    ClosedForTransition,
}

impl fmt::Display for DraftPushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("drafter lifecycle is closed"),
            Self::ClosedForTransition => f.write_str("drafter is transitioning between segments"),
        }
    }
}

impl std::error::Error for DraftPushError {}

/// Errors found before a worker is spawned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DraftConfigError {
    ZeroDuration(&'static str),
    RetryRange,
    RequestTimeoutNotBelowRefresh,
    RefreshIntervalTooLong,
}

impl fmt::Display for DraftConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDuration(name) => write!(f, "{name} must be greater than zero"),
            Self::RetryRange => f.write_str("retry_initial must not exceed retry_max"),
            Self::RequestTimeoutNotBelowRefresh => {
                f.write_str("request_timeout must be below refresh_interval for expiring backends")
            }
            Self::RefreshIntervalTooLong => {
                f.write_str("refresh_interval must be below Telegram's 30 second draft TTL")
            }
        }
    }
}

impl std::error::Error for DraftConfigError {}

/// Operations that a backend can classify for the scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrafterOperation {
    Preview,
    PreviewFirstSend,
    PreviewEdit,
    Refresh,
    SegmentCommit,
    Final,
    Cleanup,
}

/// A backend's classification of a failed operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrafterErrorClass {
    RetryAfter {
        delay: Duration,
        scope: super::DrafterRateLimitScope,
    },
    Transient {
        retry_safe: bool,
    },
    /// The payload was rejected before it could have an external side effect.
    /// Segment commits keep the worker alive so the caller can submit a
    /// corrected payload.
    InvalidPayload,
    Permanent,
    Ambiguous,
}

/// Certainty about whether an external side effect was applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryCertainty {
    /// The request was rejected locally and never reached the external API.
    NotAttempted,
    /// The remote API explicitly rejected the request.
    Rejected,
    /// The request may have been applied, but confirmation was lost.
    Unknown,
}

/// Retry classification together with the delivery certainty of the failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrafterErrorDisposition {
    pub class: DrafterErrorClass,
    pub delivery: DeliveryCertainty,
}

/// Error returned by `flush` when the worker cannot deliver the target.
#[derive(Debug)]
pub enum DraftFlushError {
    WorkerStopped,
    PreviewDisabled,
}

impl fmt::Display for DraftFlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerStopped => f.write_str("drafter worker stopped"),
            Self::PreviewDisabled => f.write_str("preview delivery is disabled"),
        }
    }
}

impl std::error::Error for DraftFlushError {}

/// Error returned by `commit_segment`.
#[derive(Debug)]
pub enum DraftCommitError<E> {
    WorkerStoppedBeforeCommand,
    WorkerStoppedAfterCommand { delivery: DeliveryCertainty },
    Backend { source: E, class: DrafterErrorClass, delivery: DeliveryCertainty },
}

impl<E: fmt::Display> fmt::Display for DraftCommitError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerStoppedBeforeCommand => {
                f.write_str("drafter worker stopped before segment commit command")
            }
            Self::WorkerStoppedAfterCommand { delivery } => {
                write!(f, "drafter worker stopped after segment commit command ({delivery:?})")
            }
            Self::Backend { source, class, delivery } => {
                write!(f, "segment commit failed ({class:?}, {delivery:?}): {source}")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for DraftCommitError<E> {}

/// Error returned by `finish`.
#[derive(Debug)]
pub enum DraftFinishError<E> {
    WorkerStoppedBeforeCommand,
    WorkerStoppedAfterCommand { delivery: DeliveryCertainty },
    Backend { source: E, class: DrafterErrorClass, delivery: DeliveryCertainty },
}

impl<E: fmt::Display> fmt::Display for DraftFinishError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerStoppedBeforeCommand => {
                f.write_str("drafter worker stopped before final command")
            }
            Self::WorkerStoppedAfterCommand { delivery } => {
                write!(f, "drafter worker stopped after final command ({delivery:?})")
            }
            Self::Backend { source, class, delivery } => {
                write!(f, "final delivery failed ({class:?}, {delivery:?}): {source}")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for DraftFinishError<E> {}

/// Error returned by `abort`.
#[derive(Debug)]
pub enum DraftAbortError<E> {
    WorkerStopped,
    Backend(E),
}

impl<E: fmt::Display> fmt::Display for DraftAbortError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerStopped => f.write_str("drafter worker stopped"),
            Self::Backend(error) => write!(f, "abort cleanup failed: {error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for DraftAbortError<E> {}

/// Error returned if a backend is incompatible with the configured schedule.
#[derive(Debug)]
pub enum DraftStartError {
    InvalidConfig(DraftConfigError),
    UnsupportedTarget(&'static str),
}

impl fmt::Display for DraftStartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(error) => write!(f, "invalid drafter config: {error}"),
            Self::UnsupportedTarget(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for DraftStartError {}
