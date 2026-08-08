//! Asynchronous, latest-wins delivery of temporary previews and permanent
//! results.
//!
//! A [`DraftSink`] is intentionally synchronous: it only updates local state
//! and wakes a Tokio worker. Network requests, throttling and lifecycle
//! transitions belong to the owning [`Drafter`].
//!
//! Standard Telegram backends are available through [`TelegramDrafter`]. Pass
//! one [`InProcessRateLimiter`] instance to all drafters that share a bot
//! token, or create one [`DrafterOutboundLimiter`] per Drafter over a shared
//! [`teloxide_core::outbound::OutboundQueue`]. The `*_with_observer`
//! constructors expose lifecycle events without recording preview payloads or
//! user text; enabling the `tracing` feature installs the payload-free tracing
//! observer by default.
//!
//! Distributed rate limiting, persistence/recovery, automatic native-to-edit
//! fallback, rich pagination and automatic semantic segment splitting are
//! intentionally outside this local runtime's MVP boundary.

mod backend;
mod config;
mod error;
mod limiter;
mod machine;
mod observer;
mod outbound;
mod source;
mod telegram;

pub use backend::{
    CleanupFailure, DraftId, DrafterBackend, DrafterCapabilities, DrafterMode, PreviewAck,
};
pub use config::{DraftConfig, DraftSchedule};
pub use error::{
    DeliveryCertainty, DraftAbortError, DraftCommitError, DraftConfigError, DraftFinishError,
    DraftFlushError, DraftPushError, DraftRevision, DraftStartError, DrafterErrorClass,
    DrafterErrorDisposition, DrafterOperation,
};
pub use limiter::{
    DrafterAcquireError, DrafterPermit, DrafterPermitCompletion, DrafterPriority,
    DrafterRateLimitKey, DrafterRateLimitScope, DrafterRateLimiter, DrafterRequestClass,
    InProcessRateLimiter,
};
pub use machine::{DraftSink, Drafter};
#[cfg(feature = "tracing")]
pub use observer::TracingDrafterObserver;
pub use observer::{
    DrafterEvent, DrafterEventKind, DrafterMetricsCollector, DrafterMetricsSnapshot,
    DrafterObserver, NoopDrafterObserver,
};
pub use outbound::DrafterOutboundLimiter;
pub use source::{
    AccumulatorSource, DraftAccumulator, PreviewSnapshot, PreviewSource, ReplacePreview,
};
pub use telegram::{
    EditAbortPolicy, EditInPlaceBackend, NativeRichBackend, NativeRichDrafterBackend,
    NativeTextBackend, NativeTextDrafterBackend, RichEditInPlaceBackend, SnapshotDrafter,
    StatusCleanup, StatusEditThenSendFinalBackend, StatusTextDrafterBackend, StatusThenRichBackend,
    StatusThenTextBackend, TelegramDraftOptions, TelegramDrafter, TelegramDrafterPolicy,
    TelegramEditOptions, TelegramSendOptions,
};
