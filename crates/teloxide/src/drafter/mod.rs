//! Asynchronous, latest-wins delivery of temporary previews and permanent
//! results.
//!
//! A [`DraftSink`] is intentionally synchronous: it only updates local state
//! and wakes a Tokio worker. Network requests, throttling and lifecycle
//! transitions belong to the owning [`Drafter`].

mod backend;
mod config;
mod error;
mod limiter;
mod machine;
mod source;
mod telegram;

pub use backend::{DraftId, DrafterBackend, DrafterCapabilities, DrafterMode, PreviewAck};
pub use config::{DraftConfig, DraftSchedule};
pub use error::{
    DraftAbortError, DraftCommitError, DraftConfigError, DraftFinishError, DraftFlushError,
    DraftPushError, DraftRevision, DraftStartError, DrafterErrorClass, DrafterOperation,
};
pub use limiter::{
    DrafterPermit, DrafterPriority, DrafterRateLimitKey, DrafterRateLimitScope, DrafterRateLimiter,
    InProcessRateLimiter,
};
pub use machine::{DraftSink, Drafter};
pub use source::{
    AccumulatorSource, DraftAccumulator, PreviewSnapshot, PreviewSource, ReplacePreview,
};
pub use telegram::{
    EditAbortPolicy, EditInPlaceBackend, NativeRichBackend, NativeRichDrafterBackend,
    NativeTextBackend, NativeTextDrafterBackend, RichEditInPlaceBackend, SnapshotDrafter,
    StatusCleanup, StatusEditThenSendFinalBackend, StatusTextDrafterBackend, StatusThenRichBackend,
    StatusThenTextBackend, TelegramDrafter, TelegramDrafterPolicy,
};
