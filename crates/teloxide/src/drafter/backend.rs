use std::{
    future::Future,
    num::NonZeroI32,
    sync::atomic::{AtomicI32, Ordering},
};

use teloxide_core::types::{ChatId, MessageId};

use super::{
    DrafterErrorDisposition, DrafterOperation, DrafterRateLimitKey, DrafterRequestClass,
    DrafterRequestContext,
};

/// Successful acknowledgement of a preview request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreviewAck;

/// Cleanup failure together with the preview message it targeted.
///
/// The message identifier is kept separately from the backend's active
/// preview state because a failed delete may have succeeded remotely.
#[derive(Debug)]
pub struct CleanupFailure<E> {
    pub message_id: MessageId,
    pub error: E,
}

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

    fn abort(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Runs best-effort cleanup after a segment or final delivery has already
    /// succeeded. Scheduler-aware backends must keep this cleanup separate
    /// from [`Self::commit_segment`] and [`Self::finish`], so cleanup admission
    /// cannot change the primary delivery result.
    fn cleanup_after_delivery(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Whether successful delivery currently has an external cleanup request
    /// to run through [`Self::cleanup_after_delivery`].
    fn cleanup_after_delivery_possible(&self) -> bool {
        false
    }

    /// Classifies the first real request made by an operation. Scheduler-aware
    /// custom backends must override this when their first request differs from
    /// the default state-based Telegram backend classification.
    fn first_request_class(&self, operation: DrafterOperation) -> DrafterRequestClass {
        if matches!(operation, DrafterOperation::Cleanup) {
            DrafterRequestClass::Mutation
        } else if matches!(operation, DrafterOperation::SegmentCommit | DrafterOperation::Final)
            && self.capabilities().mode == DrafterMode::StatusEditThenSendFinal
        {
            DrafterRequestClass::Send
        } else if self.preview_message_id().is_some() {
            DrafterRequestClass::Mutation
        } else {
            DrafterRequestClass::Send
        }
    }

    /// Whether this backend error means that one admitted request exceeded its
    /// per-request timeout. The worker uses this to preserve its existing
    /// timeout result shape for scheduler-aware backends.
    fn is_request_timeout(&self, _error: &Self::Error) -> bool {
        false
    }

    /// Whether this backend can identify a preview update that performs no
    /// external request before admission. This is an optimization for
    /// scheduler-aware backends; the default keeps the existing behavior.
    fn may_skip_preview(&self) -> bool {
        false
    }

    /// Returns whether this particular preview is a local no-op. Called only
    /// when [`Self::may_skip_preview`] returns `true`.
    fn preview_is_noop(&self, _preview: &Self::Preview) -> bool {
        false
    }

    /// Whether this backend consumes a request context and schedules every
    /// underlying Bot API request separately.
    ///
    /// The default is `false`: custom backends retain the legacy
    /// operation-level permit contract until they opt into per-request
    /// accounting explicitly. A backend that opts in and can issue a real
    /// request from [`Self::abort`] must also return `true` from
    /// [`Self::abort_request_possible`] while that request is possible.
    fn supports_request_scheduler(&self) -> bool {
        false
    }

    /// Installs the context for the next backend operation. Standard Telegram
    /// backends consume it at the operation boundary and schedule every typed
    /// request. Custom backends must override this together with
    /// [`Self::supports_request_scheduler`] to use per-request accounting.
    fn set_request_context(&mut self, _context: Option<DrafterRequestContext>) {}

    /// Whether `abort` can issue a real cleanup request for the current
    /// backend state. A scheduler-enabled custom backend must override this
    /// whenever `abort` can issue a request; the conservative default avoids
    /// granting a phantom permit for a no-op cleanup.
    fn abort_request_possible(&self) -> bool {
        false
    }

    fn classify_error(
        &self,
        operation: DrafterOperation,
        error: &Self::Error,
    ) -> DrafterErrorDisposition;

    /// Takes a best-effort cleanup failure observed after a successful
    /// delivery.
    ///
    /// Cleanup must not turn an already confirmed final delivery into a retry,
    /// but schedulers can still expose the failure through observability.
    fn take_cleanup_failure(&mut self) -> Option<CleanupFailure<Self::Error>> {
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
