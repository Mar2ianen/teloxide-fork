//! Telegram Bot API delivery backends for the generic drafter runtime.

use std::time::Duration;

use teloxide_core::{
    errors::{ApiError, RequestError},
    payloads::setters::*,
    requests::Requester,
    types::{ChatId, InputRichMessage, Message, MessageId, ReplyParameters, ThreadId, UserId},
    Bot,
};

use super::{
    DraftConfig, DraftId, DraftSink, DraftStartError, Drafter, DrafterBackend, DrafterCapabilities,
    DrafterErrorClass, DrafterMode, DrafterOperation, DrafterRateLimitKey, InProcessRateLimiter,
    PreviewAck, ReplacePreview,
};

fn classify_request_error(operation: DrafterOperation, error: &RequestError) -> DrafterErrorClass {
    match error {
        RequestError::RetryAfter(seconds) => DrafterErrorClass::RetryAfter {
            delay: Duration::from_secs(seconds.seconds() as u64),
            scope: super::DrafterRateLimitScope::Global,
        },
        RequestError::Network(_) | RequestError::Io(_) | RequestError::InvalidJson { .. } => {
            DrafterErrorClass::Transient {
                retry_safe: !matches!(
                    operation,
                    DrafterOperation::PreviewFirstSend | DrafterOperation::Final
                ),
            }
        }
        RequestError::Validation(_) | RequestError::Api(_) | RequestError::MigrateToChatId(_) => {
            DrafterErrorClass::Permanent
        }
    }
}

fn is_message_not_modified(error: &RequestError) -> bool {
    matches!(error, RequestError::Api(ApiError::MessageNotModified))
}

async fn send_text<R>(
    bot: &R,
    chat_id: ChatId,
    text: String,
    reply_parameters: Option<ReplyParameters>,
    message_thread_id: Option<ThreadId>,
) -> Result<Message, RequestError>
where
    R: Requester<Err = RequestError>,
    R::SendMessage: Send,
{
    let mut request = bot.send_message(chat_id, text);
    if let Some(thread_id) = message_thread_id {
        request = request.message_thread_id(thread_id);
    }
    if let Some(reply_parameters) = reply_parameters {
        request = request.reply_parameters(reply_parameters);
    }
    request.await
}

async fn send_rich<R>(
    bot: &R,
    chat_id: ChatId,
    rich_message: InputRichMessage,
    reply_parameters: Option<ReplyParameters>,
    message_thread_id: Option<ThreadId>,
) -> Result<Message, RequestError>
where
    R: Requester<Err = RequestError>,
    R::SendRichMessage: Send,
{
    let mut request = bot.send_rich_message(chat_id, rich_message);
    if let Some(thread_id) = message_thread_id {
        request = request.message_thread_id(thread_id);
    }
    if let Some(reply_parameters) = reply_parameters {
        request = request.reply_parameters(reply_parameters);
    }
    request.await
}

/// Native plain-text draft backend. Its target is a `UserId`, which prevents
/// accidentally issuing a native draft request for a group at construction.
pub struct NativeTextBackend<R> {
    bot: R,
    chat_id: UserId,
    draft_id: DraftId,
    message_thread_id: Option<ThreadId>,
    reply_parameters: Option<ReplyParameters>,
}

impl<R> NativeTextBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: UserId) -> Self {
        Self {
            bot,
            chat_id,
            draft_id: DraftId::next(),
            message_thread_id: None,
            reply_parameters: None,
        }
    }

    #[must_use]
    pub fn with_draft_id(mut self, draft_id: DraftId) -> Self {
        self.draft_id = draft_id;
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.reply_parameters = Some(reply_parameters);
        self
    }
}

impl<R> DrafterBackend for NativeTextBackend<R>
where
    R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
    R::SendMessageDraft: Send,
    R::SendMessage: Send,
{
    type Preview = String;
    type Final = String;
    type SegmentOutput = Message;
    type Output = Message;
    type Error = RequestError;

    fn capabilities(&self) -> DrafterCapabilities {
        DrafterCapabilities {
            mode: DrafterMode::NativeDraft,
            expires_without_refresh: true,
            supports_draft_thinking: true,
            supports_rich_preview: false,
        }
    }

    fn rate_limit_key(&self) -> DrafterRateLimitKey {
        DrafterRateLimitKey { chat_id: self.chat_id.into() }
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        self.bot
            .send_message_draft(self.chat_id, self.draft_id.get())
            .text(preview)
            .await
            .map(|_| PreviewAck)
    }

    async fn commit_segment(&mut self, final_payload: String) -> Result<Message, RequestError> {
        let result = send_text(
            &self.bot,
            self.chat_id.into(),
            final_payload,
            self.reply_parameters.clone(),
            self.message_thread_id,
        )
        .await;
        if result.is_ok() {
            self.draft_id = DraftId::next();
        }
        result
    }

    async fn finish(self, final_payload: String) -> Result<Message, RequestError> {
        send_text(
            &self.bot,
            self.chat_id.into(),
            final_payload,
            self.reply_parameters,
            self.message_thread_id,
        )
        .await
    }

    async fn abort(self) -> Result<(), RequestError> {
        Ok(())
    }

    fn classify_error(
        &self,
        operation: DrafterOperation,
        error: &RequestError,
    ) -> DrafterErrorClass {
        classify_request_error(operation, error)
    }
}

/// Native rich-message draft backend.
pub struct NativeRichBackend<R> {
    bot: R,
    chat_id: UserId,
    draft_id: DraftId,
    message_thread_id: Option<ThreadId>,
    reply_parameters: Option<ReplyParameters>,
}

impl<R> NativeRichBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: UserId) -> Self {
        Self {
            bot,
            chat_id,
            draft_id: DraftId::next(),
            message_thread_id: None,
            reply_parameters: None,
        }
    }

    #[must_use]
    pub fn with_draft_id(mut self, draft_id: DraftId) -> Self {
        self.draft_id = draft_id;
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.reply_parameters = Some(reply_parameters);
        self
    }
}

impl<R> DrafterBackend for NativeRichBackend<R>
where
    R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
    R::SendRichMessageDraft: Send,
    R::SendRichMessage: Send,
{
    type Preview = InputRichMessage;
    type Final = InputRichMessage;
    type SegmentOutput = Message;
    type Output = Message;
    type Error = RequestError;

    fn capabilities(&self) -> DrafterCapabilities {
        DrafterCapabilities {
            mode: DrafterMode::NativeDraft,
            expires_without_refresh: true,
            supports_draft_thinking: true,
            supports_rich_preview: true,
        }
    }

    fn rate_limit_key(&self) -> DrafterRateLimitKey {
        DrafterRateLimitKey { chat_id: self.chat_id.into() }
    }

    async fn update(&mut self, preview: InputRichMessage) -> Result<PreviewAck, RequestError> {
        let mut request =
            self.bot.send_rich_message_draft(self.chat_id, self.draft_id.get(), preview);
        if let Some(thread_id) = self.message_thread_id {
            request = request.message_thread_id(thread_id);
        }
        request.await.map(|_| PreviewAck)
    }

    async fn commit_segment(
        &mut self,
        final_payload: InputRichMessage,
    ) -> Result<Message, RequestError> {
        let result = send_rich(
            &self.bot,
            self.chat_id.into(),
            final_payload,
            self.reply_parameters.clone(),
            self.message_thread_id,
        )
        .await;
        if result.is_ok() {
            self.draft_id = DraftId::next();
        }
        result
    }

    async fn finish(self, final_payload: InputRichMessage) -> Result<Message, RequestError> {
        send_rich(
            &self.bot,
            self.chat_id.into(),
            final_payload,
            self.reply_parameters,
            self.message_thread_id,
        )
        .await
    }

    async fn abort(self) -> Result<(), RequestError> {
        Ok(())
    }

    fn classify_error(
        &self,
        operation: DrafterOperation,
        error: &RequestError,
    ) -> DrafterErrorClass {
        classify_request_error(operation, error)
    }
}

/// Plain status preview followed by a separate rich permanent message.
pub struct StatusThenRichBackend<R> {
    bot: R,
    chat_id: ChatId,
    preview_message_id: Option<MessageId>,
    reply_parameters: Option<ReplyParameters>,
    message_thread_id: Option<ThreadId>,
    cleanup: StatusCleanup,
}

/// Whether the status message is removed after final delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusCleanup {
    Keep,
    DeleteAfterFinalSuccess,
}

impl<R> StatusThenRichBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: ChatId) -> Self {
        Self {
            bot,
            chat_id,
            preview_message_id: None,
            reply_parameters: None,
            message_thread_id: None,
            cleanup: StatusCleanup::DeleteAfterFinalSuccess,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn cleanup(mut self, cleanup: StatusCleanup) -> Self {
        self.cleanup = cleanup;
        self
    }
}

impl<R> DrafterBackend for StatusThenRichBackend<R>
where
    R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
    R::SendMessage: Send,
    R::EditMessageText: Send,
    R::SendRichMessage: Send,
    R::DeleteMessage: Send,
{
    type Preview = String;
    type Final = InputRichMessage;
    type SegmentOutput = Message;
    type Output = Message;
    type Error = RequestError;

    fn capabilities(&self) -> DrafterCapabilities {
        DrafterCapabilities {
            mode: DrafterMode::StatusEditThenSendFinal,
            expires_without_refresh: false,
            supports_draft_thinking: false,
            supports_rich_preview: false,
        }
    }

    fn rate_limit_key(&self) -> DrafterRateLimitKey {
        DrafterRateLimitKey { chat_id: self.chat_id }
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        let message_id = if let Some(message_id) = self.preview_message_id {
            self.bot.edit_message_text(self.chat_id, message_id, preview).await
        } else {
            let message = send_text(
                &self.bot,
                self.chat_id,
                preview,
                self.reply_parameters.clone(),
                self.message_thread_id,
            )
            .await?;
            self.preview_message_id = Some(message.id);
            Ok(message)
        };
        match message_id {
            Ok(_) => Ok(PreviewAck),
            Err(error) if is_message_not_modified(&error) => Ok(PreviewAck),
            Err(error) => Err(error),
        }
    }

    async fn commit_segment(
        &mut self,
        final_payload: InputRichMessage,
    ) -> Result<Message, RequestError> {
        let result = send_rich(
            &self.bot,
            self.chat_id,
            final_payload,
            self.reply_parameters.clone(),
            self.message_thread_id,
        )
        .await;
        if result.is_ok() {
            self.cleanup_preview().await;
        }
        result
    }

    async fn finish(self, final_payload: InputRichMessage) -> Result<Message, RequestError> {
        let StatusThenRichBackend {
            bot,
            chat_id,
            preview_message_id,
            reply_parameters,
            message_thread_id,
            cleanup,
        } = self;
        let result =
            send_rich(&bot, chat_id, final_payload, reply_parameters, message_thread_id).await;
        if result.is_ok() && cleanup == StatusCleanup::DeleteAfterFinalSuccess {
            if let Some(message_id) = preview_message_id {
                let _ = bot.delete_message(chat_id, message_id).await;
            }
        }
        result
    }

    async fn abort(self) -> Result<(), RequestError> {
        if self.cleanup != StatusCleanup::DeleteAfterFinalSuccess {
            return Ok(());
        }
        if let Some(message_id) = self.preview_message_id {
            let _ = self.bot.delete_message(self.chat_id, message_id).await;
        }
        Ok(())
    }

    fn classify_error(
        &self,
        operation: DrafterOperation,
        error: &RequestError,
    ) -> DrafterErrorClass {
        let operation = if self.preview_message_id.is_none()
            && matches!(operation, DrafterOperation::Preview)
        {
            DrafterOperation::PreviewFirstSend
        } else {
            operation
        };
        classify_request_error(operation, error)
    }
}

impl<R> StatusThenRichBackend<R>
where
    R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
    R::SendMessage: Send,
    R::EditMessageText: Send,
    R::DeleteMessage: Send,
{
    async fn cleanup_preview(&mut self) {
        if self.cleanup == StatusCleanup::Keep {
            return;
        }
        if let Some(message_id) = self.preview_message_id.take() {
            let _ = self.bot.delete_message(self.chat_id, message_id).await;
        }
    }
}

/// Plain status preview followed by a separate plain permanent message.
pub struct StatusThenTextBackend<R> {
    bot: R,
    chat_id: ChatId,
    preview_message_id: Option<MessageId>,
    reply_parameters: Option<ReplyParameters>,
    message_thread_id: Option<ThreadId>,
    cleanup: StatusCleanup,
}

impl<R> StatusThenTextBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: ChatId) -> Self {
        Self {
            bot,
            chat_id,
            preview_message_id: None,
            reply_parameters: None,
            message_thread_id: None,
            cleanup: StatusCleanup::DeleteAfterFinalSuccess,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn cleanup(mut self, cleanup: StatusCleanup) -> Self {
        self.cleanup = cleanup;
        self
    }
}

impl<R> DrafterBackend for StatusThenTextBackend<R>
where
    R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
    R::SendMessage: Send,
    R::EditMessageText: Send,
    R::DeleteMessage: Send,
{
    type Preview = String;
    type Final = String;
    type SegmentOutput = Message;
    type Output = Message;
    type Error = RequestError;

    fn capabilities(&self) -> DrafterCapabilities {
        DrafterCapabilities {
            mode: DrafterMode::StatusEditThenSendFinal,
            expires_without_refresh: false,
            supports_draft_thinking: false,
            supports_rich_preview: false,
        }
    }

    fn rate_limit_key(&self) -> DrafterRateLimitKey {
        DrafterRateLimitKey { chat_id: self.chat_id }
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        if let Some(message_id) = self.preview_message_id {
            self.bot
                .edit_message_text(self.chat_id, message_id, preview)
                .await
                .map(|_| PreviewAck)
                .or_else(
                    |error| {
                        if is_message_not_modified(&error) {
                            Ok(PreviewAck)
                        } else {
                            Err(error)
                        }
                    },
                )
        } else {
            let message = send_text(
                &self.bot,
                self.chat_id,
                preview,
                self.reply_parameters.clone(),
                self.message_thread_id,
            )
            .await?;
            self.preview_message_id = Some(message.id);
            Ok(PreviewAck)
        }
    }

    async fn commit_segment(&mut self, final_payload: String) -> Result<Message, RequestError> {
        let result = send_text(
            &self.bot,
            self.chat_id,
            final_payload,
            self.reply_parameters.clone(),
            self.message_thread_id,
        )
        .await;
        if result.is_ok() {
            self.cleanup_preview().await;
        }
        result
    }

    async fn finish(self, final_payload: String) -> Result<Message, RequestError> {
        let StatusThenTextBackend {
            bot,
            chat_id,
            preview_message_id,
            reply_parameters,
            message_thread_id,
            cleanup,
        } = self;
        let result =
            send_text(&bot, chat_id, final_payload, reply_parameters, message_thread_id).await;
        if result.is_ok() && cleanup == StatusCleanup::DeleteAfterFinalSuccess {
            if let Some(message_id) = preview_message_id {
                let _ = bot.delete_message(chat_id, message_id).await;
            }
        }
        result
    }

    async fn abort(self) -> Result<(), RequestError> {
        if self.cleanup == StatusCleanup::DeleteAfterFinalSuccess {
            if let Some(message_id) = self.preview_message_id {
                let _ = self.bot.delete_message(self.chat_id, message_id).await;
            }
        }
        Ok(())
    }

    fn classify_error(
        &self,
        operation: DrafterOperation,
        error: &RequestError,
    ) -> DrafterErrorClass {
        let operation = if self.preview_message_id.is_none()
            && matches!(operation, DrafterOperation::Preview)
        {
            DrafterOperation::PreviewFirstSend
        } else {
            operation
        };
        classify_request_error(operation, error)
    }
}

impl<R> StatusThenTextBackend<R>
where
    R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
    R::DeleteMessage: Send,
{
    async fn cleanup_preview(&mut self) {
        if self.cleanup == StatusCleanup::Keep {
            return;
        }
        if let Some(message_id) = self.preview_message_id.take() {
            let _ = self.bot.delete_message(self.chat_id, message_id).await;
        }
    }
}

/// Edit-in-place backend for plain text preview and plain final text.
pub struct EditInPlaceBackend<R> {
    bot: R,
    chat_id: ChatId,
    message_id: Option<MessageId>,
    last_message: Option<Message>,
    last_fingerprint: Option<u64>,
    reply_parameters: Option<ReplyParameters>,
    message_thread_id: Option<ThreadId>,
    abort_policy: EditAbortPolicy,
}

/// Policy for the preview message when an edit-in-place drafter is aborted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EditAbortPolicy {
    KeepPreview,
    DeletePreviewBestEffort,
}

impl<R> EditInPlaceBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: ChatId) -> Self {
        Self {
            bot,
            chat_id,
            message_id: None,
            last_message: None,
            last_fingerprint: None,
            reply_parameters: None,
            message_thread_id: None,
            abort_policy: EditAbortPolicy::KeepPreview,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn abort_policy(mut self, policy: EditAbortPolicy) -> Self {
        self.abort_policy = policy;
        self
    }
}

fn fingerprint(text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

impl<R> DrafterBackend for EditInPlaceBackend<R>
where
    R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
    R::SendMessage: Send,
    R::EditMessageText: Send,
    R::DeleteMessage: Send,
{
    type Preview = String;
    type Final = String;
    type SegmentOutput = Message;
    type Output = Message;
    type Error = RequestError;

    fn capabilities(&self) -> DrafterCapabilities {
        DrafterCapabilities {
            mode: DrafterMode::EditInPlace,
            expires_without_refresh: false,
            supports_draft_thinking: false,
            supports_rich_preview: false,
        }
    }

    fn rate_limit_key(&self) -> DrafterRateLimitKey {
        DrafterRateLimitKey { chat_id: self.chat_id }
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        let current_fingerprint = fingerprint(&preview);
        if self.last_fingerprint == Some(current_fingerprint) {
            return Ok(PreviewAck);
        }
        let result = if let Some(message_id) = self.message_id {
            self.bot.edit_message_text(self.chat_id, message_id, preview).await
        } else {
            let message = send_text(
                &self.bot,
                self.chat_id,
                preview,
                self.reply_parameters.clone(),
                self.message_thread_id,
            )
            .await?;
            self.message_id = Some(message.id);
            self.last_message = Some(message.clone());
            Ok(message)
        };
        match result {
            Ok(message) => {
                self.last_message = Some(message);
                self.last_fingerprint = Some(current_fingerprint);
                Ok(PreviewAck)
            }
            Err(error) if is_message_not_modified(&error) => {
                self.last_fingerprint = Some(current_fingerprint);
                Ok(PreviewAck)
            }
            Err(error) => Err(error),
        }
    }

    async fn commit_segment(&mut self, final_payload: String) -> Result<Message, RequestError> {
        let result = if let Some(message_id) = self.message_id {
            self.bot.edit_message_text(self.chat_id, message_id, final_payload).await
        } else {
            send_text(
                &self.bot,
                self.chat_id,
                final_payload,
                self.reply_parameters.clone(),
                self.message_thread_id,
            )
            .await
        };
        match result {
            Ok(message) => {
                self.message_id = None;
                self.last_message = None;
                self.last_fingerprint = None;
                Ok(message)
            }
            Err(error) if is_message_not_modified(&error) => {
                let Some(message) = self.last_message.take() else {
                    return Err(error);
                };
                self.message_id = None;
                self.last_fingerprint = None;
                Ok(message)
            }
            Err(error) => Err(error),
        }
    }

    async fn finish(self, final_payload: String) -> Result<Message, RequestError> {
        if let Some(message_id) = self.message_id {
            match self.bot.edit_message_text(self.chat_id, message_id, final_payload).await {
                Ok(message) => Ok(message),
                Err(error) if is_message_not_modified(&error) => {
                    self.last_message.clone().ok_or(error)
                }
                Err(error) => Err(error),
            }
        } else {
            send_text(
                &self.bot,
                self.chat_id,
                final_payload,
                self.reply_parameters,
                self.message_thread_id,
            )
            .await
        }
    }

    async fn abort(self) -> Result<(), RequestError> {
        if self.abort_policy == EditAbortPolicy::DeletePreviewBestEffort {
            if let Some(message_id) = self.message_id {
                let _ = self.bot.delete_message(self.chat_id, message_id).await;
            }
        }
        Ok(())
    }

    fn classify_error(
        &self,
        operation: DrafterOperation,
        error: &RequestError,
    ) -> DrafterErrorClass {
        let operation =
            if self.message_id.is_none() && matches!(operation, DrafterOperation::Preview) {
                DrafterOperation::PreviewFirstSend
            } else {
                operation
            };
        classify_request_error(operation, error)
    }
}

/// Edit-in-place backend that keeps a plain preview and replaces it with a
/// rich final message. Rich editing is currently exposed for the concrete
/// `Bot`, whose multipart request factory is available as an inherent method.
pub struct RichEditInPlaceBackend {
    bot: Bot,
    chat_id: ChatId,
    message_id: Option<MessageId>,
    last_message: Option<Message>,
    last_fingerprint: Option<u64>,
    reply_parameters: Option<ReplyParameters>,
    message_thread_id: Option<ThreadId>,
    abort_policy: EditAbortPolicy,
}

impl RichEditInPlaceBackend {
    #[must_use]
    pub fn new(bot: Bot, chat_id: ChatId) -> Self {
        Self {
            bot,
            chat_id,
            message_id: None,
            last_message: None,
            last_fingerprint: None,
            reply_parameters: None,
            message_thread_id: None,
            abort_policy: EditAbortPolicy::KeepPreview,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn abort_policy(mut self, policy: EditAbortPolicy) -> Self {
        self.abort_policy = policy;
        self
    }
}

impl DrafterBackend for RichEditInPlaceBackend {
    type Preview = String;
    type Final = InputRichMessage;
    type SegmentOutput = Message;
    type Output = Message;
    type Error = RequestError;

    fn capabilities(&self) -> DrafterCapabilities {
        DrafterCapabilities {
            mode: DrafterMode::EditInPlace,
            expires_without_refresh: false,
            supports_draft_thinking: false,
            supports_rich_preview: false,
        }
    }

    fn rate_limit_key(&self) -> DrafterRateLimitKey {
        DrafterRateLimitKey { chat_id: self.chat_id }
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        let current_fingerprint = fingerprint(&preview);
        if self.last_fingerprint == Some(current_fingerprint) {
            return Ok(PreviewAck);
        }
        let result = if let Some(message_id) = self.message_id {
            self.bot.edit_message_text(self.chat_id, message_id, preview).await
        } else {
            let message = send_text(
                &self.bot,
                self.chat_id,
                preview,
                self.reply_parameters.clone(),
                self.message_thread_id,
            )
            .await?;
            self.message_id = Some(message.id);
            Ok(message)
        };
        match result {
            Ok(message) => {
                self.last_message = Some(message);
                self.last_fingerprint = Some(current_fingerprint);
                Ok(PreviewAck)
            }
            Err(error) if is_message_not_modified(&error) => {
                self.last_fingerprint = Some(current_fingerprint);
                Ok(PreviewAck)
            }
            Err(error) => Err(error),
        }
    }

    async fn commit_segment(
        &mut self,
        final_payload: InputRichMessage,
    ) -> Result<Message, RequestError> {
        let result = if let Some(message_id) = self.message_id {
            match self.bot.edit_message_rich_text(self.chat_id, message_id, final_payload).await {
                Ok(message) => Ok(message),
                Err(error) if is_message_not_modified(&error) => {
                    self.last_message.clone().ok_or(error)
                }
                Err(error) => Err(error),
            }
        } else {
            send_rich(
                &self.bot,
                self.chat_id,
                final_payload,
                self.reply_parameters.clone(),
                self.message_thread_id,
            )
            .await
        };
        if result.is_ok() {
            self.message_id = None;
            self.last_message = None;
            self.last_fingerprint = None;
        }
        result
    }

    async fn finish(self, final_payload: InputRichMessage) -> Result<Message, RequestError> {
        if let Some(message_id) = self.message_id {
            match self.bot.edit_message_rich_text(self.chat_id, message_id, final_payload).await {
                Ok(message) => Ok(message),
                Err(error) if is_message_not_modified(&error) => {
                    self.last_message.clone().ok_or(error)
                }
                Err(error) => Err(error),
            }
        } else {
            send_rich(
                &self.bot,
                self.chat_id,
                final_payload,
                self.reply_parameters,
                self.message_thread_id,
            )
            .await
        }
    }

    async fn abort(self) -> Result<(), RequestError> {
        if self.abort_policy == EditAbortPolicy::DeletePreviewBestEffort {
            if let Some(message_id) = self.message_id {
                let _ = self.bot.delete_message(self.chat_id, message_id).await;
            }
        }
        Ok(())
    }

    fn classify_error(
        &self,
        operation: DrafterOperation,
        error: &RequestError,
    ) -> DrafterErrorClass {
        let operation =
            if self.message_id.is_none() && matches!(operation, DrafterOperation::Preview) {
                DrafterOperation::PreviewFirstSend
            } else {
                operation
            };
        classify_request_error(operation, error)
    }
}

pub type NativeTextDrafterBackend<R> = NativeTextBackend<R>;
pub type NativeRichDrafterBackend<R> = NativeRichBackend<R>;
pub type StatusEditThenSendFinalBackend<R> = StatusThenRichBackend<R>;
pub type StatusTextDrafterBackend<R> = StatusThenTextBackend<R>;

pub type SnapshotDrafter<P, B, L> = (Drafter<ReplacePreview<P>, B, L>, DraftSink<P>);

/// Policy helper for applications that choose a backend from a known chat
/// kind before constructing the worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramDrafterPolicy {
    NativeInPrivateStatusInChats,
    NativeOnly,
    EditInPlaceOnly,
    StatusThenFinalOnly,
}

impl TelegramDrafterPolicy {
    #[must_use]
    pub const fn mode_for(self, is_private_chat: bool) -> DrafterMode {
        match self {
            Self::NativeInPrivateStatusInChats if is_private_chat => DrafterMode::NativeDraft,
            Self::NativeInPrivateStatusInChats => DrafterMode::StatusEditThenSendFinal,
            Self::NativeOnly => DrafterMode::NativeDraft,
            Self::EditInPlaceOnly => DrafterMode::EditInPlace,
            Self::StatusThenFinalOnly => DrafterMode::StatusEditThenSendFinal,
        }
    }
}

/// Small constructor facade for the standard Telegram backends.
pub struct TelegramDrafter;

impl TelegramDrafter {
    pub fn native_text<R, L>(
        bot: R,
        chat_id: UserId,
        config: DraftConfig,
        limiter: L,
    ) -> Result<SnapshotDrafter<String, NativeTextBackend<R>, L>, DraftStartError>
    where
        R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
        R::SendMessageDraft: Send,
        R::SendMessage: Send,
        L: super::DrafterRateLimiter,
    {
        Drafter::snapshots(NativeTextBackend::new(bot, chat_id), limiter, config)
    }

    pub fn native_rich<R, L>(
        bot: R,
        chat_id: UserId,
        config: DraftConfig,
        limiter: L,
    ) -> Result<SnapshotDrafter<InputRichMessage, NativeRichBackend<R>, L>, DraftStartError>
    where
        R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
        R::SendRichMessageDraft: Send,
        R::SendRichMessage: Send,
        L: super::DrafterRateLimiter,
    {
        Drafter::snapshots(NativeRichBackend::new(bot, chat_id), limiter, config)
    }

    pub fn edit_in_place<R, L>(
        bot: R,
        chat_id: ChatId,
        config: DraftConfig,
        limiter: L,
    ) -> Result<SnapshotDrafter<String, EditInPlaceBackend<R>, L>, DraftStartError>
    where
        R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
        R::SendMessage: Send,
        R::EditMessageText: Send,
        R::DeleteMessage: Send,
        L: super::DrafterRateLimiter,
    {
        Drafter::snapshots(EditInPlaceBackend::new(bot, chat_id), limiter, config)
    }

    pub fn edit_in_place_rich<L>(
        bot: Bot,
        chat_id: ChatId,
        config: DraftConfig,
        limiter: L,
    ) -> Result<SnapshotDrafter<String, RichEditInPlaceBackend, L>, DraftStartError>
    where
        L: super::DrafterRateLimiter,
    {
        Drafter::snapshots(RichEditInPlaceBackend::new(bot, chat_id), limiter, config)
    }

    pub fn status_then_rich<R, L>(
        bot: R,
        chat_id: ChatId,
        config: DraftConfig,
        limiter: L,
    ) -> Result<SnapshotDrafter<String, StatusThenRichBackend<R>, L>, DraftStartError>
    where
        R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
        R::SendMessage: Send,
        R::EditMessageText: Send,
        R::SendRichMessage: Send,
        R::DeleteMessage: Send,
        L: super::DrafterRateLimiter,
    {
        Drafter::snapshots(StatusThenRichBackend::new(bot, chat_id), limiter, config)
    }

    pub fn status_then_text<R, L>(
        bot: R,
        chat_id: ChatId,
        config: DraftConfig,
        limiter: L,
    ) -> Result<SnapshotDrafter<String, StatusThenTextBackend<R>, L>, DraftStartError>
    where
        R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
        R::SendMessage: Send,
        R::EditMessageText: Send,
        R::DeleteMessage: Send,
        L: super::DrafterRateLimiter,
    {
        Drafter::snapshots(StatusThenTextBackend::new(bot, chat_id), limiter, config)
    }

    pub fn native_text_with_defaults<R>(
        bot: R,
        chat_id: UserId,
        config: DraftConfig,
    ) -> Result<SnapshotDrafter<String, NativeTextBackend<R>, InProcessRateLimiter>, DraftStartError>
    where
        R: Requester<Err = RequestError> + Clone + Send + Sync + 'static,
        R::SendMessageDraft: Send,
        R::SendMessage: Send,
    {
        Self::native_text(bot, chat_id, config, InProcessRateLimiter::default())
    }
}
