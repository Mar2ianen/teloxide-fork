use std::{
    future::Future,
    num::NonZeroI32,
    sync::atomic::{AtomicI32, Ordering},
};

use teloxide_core::types::{ChatId, MessageId};

use super::{DrafterErrorClass, DrafterOperation, DrafterRateLimitKey};

/// Successful acknowledgement of a preview request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreviewAck;

/// Capabilities declared by a delivery backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrafterCapabilities {
    pub mode: DrafterMode,
    pub expires_without_refresh: bool,
    pub supports_draft_thinking: bool,
    pub supports_rich_preview: bool,
}

/// Delivery mode used by a backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrafterMode {
    NativeDraft,
    EditInPlace,
    StatusEditThenSendFinal,
}

/// Backend contract. The worker serializes all calls to one backend.
pub trait DrafterBackend: Send + 'static {
    type Preview: Send + 'static;
    type Final: Send + 'static;
    type SegmentOutput: Send + 'static;
    type Output: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> DrafterCapabilities;

    fn rate_limit_key(&self) -> DrafterRateLimitKey {
        DrafterRateLimitKey { chat_id: ChatId(0) }
    }

    /// Native draft identifier, when the backend owns one.
    fn draft_id(&self) -> Option<DraftId> {
        None
    }

    /// Current preview message identifier for message-based backends.
    fn preview_message_id(&self) -> Option<MessageId> {
        None
    }

    fn update(
        &mut self,
        preview: Self::Preview,
    ) -> impl Future<Output = Result<PreviewAck, Self::Error>> + Send;

    fn commit_segment(
        &mut self,
        final_payload: &Self::Final,
    ) -> impl Future<Output = Result<Self::SegmentOutput, Self::Error>> + Send;

    fn finish(
        &mut self,
        final_payload: &Self::Final,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;

    fn abort(self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn classify_error(
        &self,
        _operation: DrafterOperation,
        _error: &Self::Error,
    ) -> DrafterErrorClass {
        DrafterErrorClass::Transient { retry_safe: true }
    }

    /// Takes a best-effort cleanup error observed after a successful delivery.
    ///
    /// Cleanup must not turn an already confirmed final delivery into a retry,
    /// but schedulers can still expose the failure through observability.
    fn take_cleanup_error(&mut self) -> Option<Self::Error> {
        None
    }
}

/// A non-zero identifier shared by all updates in one native-draft segment.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DraftId(NonZeroI32);

impl DraftId {
    #[must_use]
    pub const fn new(value: i32) -> Option<Self> {
        match NonZeroI32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> i32 {
        self.0.get()
    }

    #[must_use]
    pub fn next() -> Self {
        static NEXT_DRAFT_ID: AtomicI32 = AtomicI32::new(1);
        loop {
            let value = NEXT_DRAFT_ID.fetch_add(1, Ordering::Relaxed);
            if let Some(id) = Self::new(value) {
                return id;
            }
        }
    }
}
