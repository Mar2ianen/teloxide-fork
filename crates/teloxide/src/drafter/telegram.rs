//! Telegram Bot API delivery backends for the generic drafter runtime.

use std::time::Duration;

use teloxide_core::{
    errors::{ApiError, RequestError},
    payloads::{
        EditMessageTextSetters, SendMessageDraftSetters, SendMessageSetters,
        SendRichMessageDraftSetters, SendRichMessageSetters,
    },
    requests::Requester,
    types::{
        BusinessConnectionId, CallbackQueryId, ChatId, EffectId, InlineKeyboardMarkup,
        InputRichMessage, LinkPreviewOptions, Message, MessageEntity, MessageId, ParseMode,
        ReplyMarkup, ReplyParameters, SuggestedPostParameters, ThreadId, TopicId, UserId,
    },
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

struct CleanupFailure<E> {
    message_id: MessageId,
    error: E,
}

/// Options for permanent text and rich-message requests.
///
/// Status backends copy only the preview-safe subset into their temporary
/// status message. Final-only fields such as buttons, message effects, paid
/// broadcasts and suggested-post parameters are never applied to that
/// temporary request.
#[derive(Clone, Debug, Default)]
pub struct TelegramSendOptions {
    pub business_connection_id: Option<BusinessConnectionId>,
    pub message_thread_id: Option<ThreadId>,
    pub direct_messages_topic_id: Option<TopicId>,
    pub receiver_user_id: Option<UserId>,
    pub callback_query_id: Option<CallbackQueryId>,
    pub parse_mode: Option<ParseMode>,
    pub entities: Option<Vec<MessageEntity>>,
    pub link_preview_options: Option<LinkPreviewOptions>,
    pub disable_notification: Option<bool>,
    pub protect_content: Option<bool>,
    pub allow_paid_broadcast: Option<bool>,
    pub message_effect_id: Option<EffectId>,
    pub suggested_post_parameters: Option<SuggestedPostParameters>,
    pub reply_parameters: Option<ReplyParameters>,
    pub reply_markup: Option<ReplyMarkup>,
}

impl TelegramSendOptions {
    fn preview_safe(&self) -> Self {
        let mut options = self.clone();
        options.allow_paid_broadcast = None;
        options.message_effect_id = None;
        options.suggested_post_parameters = None;
        options.reply_markup = None;
        options
    }

    #[must_use]
    pub fn message_thread_id(mut self, value: ThreadId) -> Self {
        self.message_thread_id = Some(value);
        self
    }

    #[must_use]
    pub fn reply_parameters(mut self, value: ReplyParameters) -> Self {
        self.reply_parameters = Some(value);
        self
    }

    #[must_use]
    pub fn business_connection_id(mut self, value: BusinessConnectionId) -> Self {
        self.business_connection_id = Some(value);
        self
    }

    #[must_use]
    pub fn direct_messages_topic_id(mut self, value: TopicId) -> Self {
        self.direct_messages_topic_id = Some(value);
        self
    }

    #[must_use]
    pub fn parse_mode(mut self, value: ParseMode) -> Self {
        self.parse_mode = Some(value);
        self
    }

    #[must_use]
    pub fn entities(mut self, value: Vec<MessageEntity>) -> Self {
        self.entities = Some(value);
        self
    }

    #[must_use]
    pub fn link_preview_options(mut self, value: LinkPreviewOptions) -> Self {
        self.link_preview_options = Some(value);
        self
    }

    #[must_use]
    pub fn disable_notification(mut self, value: bool) -> Self {
        self.disable_notification = Some(value);
        self
    }

    #[must_use]
    pub fn protect_content(mut self, value: bool) -> Self {
        self.protect_content = Some(value);
        self
    }

    #[must_use]
    pub fn allow_paid_broadcast(mut self, value: bool) -> Self {
        self.allow_paid_broadcast = Some(value);
        self
    }

    #[must_use]
    pub fn message_effect_id(mut self, value: EffectId) -> Self {
        self.message_effect_id = Some(value);
        self
    }

    #[must_use]
    pub fn suggested_post_parameters(mut self, value: SuggestedPostParameters) -> Self {
        self.suggested_post_parameters = Some(value);
        self
    }

    #[must_use]
    pub fn reply_markup(mut self, value: ReplyMarkup) -> Self {
        self.reply_markup = Some(value);
        self
    }
}

/// Options accepted by the native draft methods.
#[derive(Clone, Debug, Default)]
pub struct TelegramDraftOptions {
    pub message_thread_id: Option<ThreadId>,
    pub parse_mode: Option<ParseMode>,
    pub entities: Option<Vec<MessageEntity>>,
}

impl TelegramDraftOptions {
    #[must_use]
    pub fn message_thread_id(mut self, value: ThreadId) -> Self {
        self.message_thread_id = Some(value);
        self
    }

    #[must_use]
    pub fn parse_mode(mut self, value: ParseMode) -> Self {
        self.parse_mode = Some(value);
        self
    }

    #[must_use]
    pub fn entities(mut self, value: Vec<MessageEntity>) -> Self {
        self.entities = Some(value);
        self
    }
}

/// Options accepted by edit-message requests.
#[derive(Clone, Debug, Default)]
pub struct TelegramEditOptions {
    pub business_connection_id: Option<BusinessConnectionId>,
    pub parse_mode: Option<ParseMode>,
    pub entities: Option<Vec<MessageEntity>>,
    pub link_preview_options: Option<LinkPreviewOptions>,
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

impl TelegramEditOptions {
    #[must_use]
    pub fn business_connection_id(mut self, value: BusinessConnectionId) -> Self {
        self.business_connection_id = Some(value);
        self
    }

    #[must_use]
    pub fn parse_mode(mut self, value: ParseMode) -> Self {
        self.parse_mode = Some(value);
        self
    }

    #[must_use]
    pub fn entities(mut self, value: Vec<MessageEntity>) -> Self {
        self.entities = Some(value);
        self
    }

    #[must_use]
    pub fn link_preview_options(mut self, value: LinkPreviewOptions) -> Self {
        self.link_preview_options = Some(value);
        self
    }

    #[must_use]
    pub fn reply_markup(mut self, value: InlineKeyboardMarkup) -> Self {
        self.reply_markup = Some(value);
        self
    }
}

fn apply_text_send_options<T>(mut request: T, options: &TelegramSendOptions) -> T
where
    T: SendMessageSetters,
{
    if let Some(value) = options.business_connection_id.clone() {
        request = request.business_connection_id(value);
    }
    if let Some(value) = options.message_thread_id {
        request = request.message_thread_id(value);
    }
    if let Some(value) = options.direct_messages_topic_id {
        request = request.direct_messages_topic_id(value);
    }
    if let Some(value) = options.receiver_user_id {
        request = request.receiver_user_id(value);
    }
    if let Some(value) = options.callback_query_id.clone() {
        request = request.callback_query_id(value);
    }
    if let Some(value) = options.parse_mode {
        request = request.parse_mode(value);
    }
    if let Some(value) = options.entities.clone() {
        request = request.entities(value);
    }
    if let Some(value) = options.link_preview_options.clone() {
        request = request.link_preview_options(value);
    }
    if let Some(value) = options.disable_notification {
        request = request.disable_notification(value);
    }
    if let Some(value) = options.protect_content {
        request = request.protect_content(value);
    }
    if let Some(value) = options.allow_paid_broadcast {
        request = request.allow_paid_broadcast(value);
    }
    if let Some(value) = options.message_effect_id.clone() {
        request = request.message_effect_id(value);
    }
    if let Some(value) = options.suggested_post_parameters.clone() {
        request = request.suggested_post_parameters(value);
    }
    if let Some(value) = options.reply_parameters.clone() {
        request = request.reply_parameters(value);
    }
    if let Some(value) = options.reply_markup.clone() {
        request = request.reply_markup(value);
    }
    request
}

fn apply_rich_send_options<T>(mut request: T, options: &TelegramSendOptions) -> T
where
    T: SendRichMessageSetters,
{
    if let Some(value) = options.business_connection_id.clone() {
        request = request.business_connection_id(value);
    }
    if let Some(value) = options.message_thread_id {
        request = request.message_thread_id(value);
    }
    if let Some(value) = options.direct_messages_topic_id {
        request = request.direct_messages_topic_id(value);
    }
    if let Some(value) = options.disable_notification {
        request = request.disable_notification(value);
    }
    if let Some(value) = options.protect_content {
        request = request.protect_content(value);
    }
    if let Some(value) = options.allow_paid_broadcast {
        request = request.allow_paid_broadcast(value);
    }
    if let Some(value) = options.message_effect_id.clone() {
        request = request.message_effect_id(value);
    }
    if let Some(value) = options.suggested_post_parameters.clone() {
        request = request.suggested_post_parameters(value);
    }
    if let Some(value) = options.reply_parameters.clone() {
        request = request.reply_parameters(value);
    }
    if let Some(value) = options.reply_markup.clone() {
        request = request.reply_markup(value);
    }
    request
}

fn apply_draft_options<T>(mut request: T, options: &TelegramDraftOptions) -> T
where
    T: SendMessageDraftSetters,
{
    if let Some(value) = options.message_thread_id {
        request = request.message_thread_id(value);
    }
    if let Some(value) = options.parse_mode {
        request = request.parse_mode(value);
    }
    if let Some(value) = options.entities.clone() {
        request = request.entities(value);
    }
    request
}

fn apply_rich_draft_options<T>(mut request: T, options: &TelegramDraftOptions) -> T
where
    T: SendRichMessageDraftSetters,
{
    if let Some(value) = options.message_thread_id {
        request = request.message_thread_id(value);
    }
    request
}

fn apply_edit_options<T>(mut request: T, options: &TelegramEditOptions) -> T
where
    T: EditMessageTextSetters,
{
    if let Some(value) = options.business_connection_id.clone() {
        request = request.business_connection_id(value);
    }
    if let Some(value) = options.parse_mode {
        request = request.parse_mode(value);
    }
    if let Some(value) = options.entities.clone() {
        request = request.entities(value);
    }
    if let Some(value) = options.link_preview_options.clone() {
        request = request.link_preview_options(value);
    }
    if let Some(value) = options.reply_markup.clone() {
        request = request.reply_markup(value);
    }
    request
}

async fn send_text<R>(
    bot: &R,
    chat_id: ChatId,
    text: String,
    options: &TelegramSendOptions,
) -> Result<Message, RequestError>
where
    R: Requester<Err = RequestError>,
    R::SendMessage: Send,
{
    apply_text_send_options(bot.send_message(chat_id, text), options).await
}

async fn send_rich<R>(
    bot: &R,
    chat_id: ChatId,
    rich_message: InputRichMessage,
    options: &TelegramSendOptions,
) -> Result<Message, RequestError>
where
    R: Requester<Err = RequestError>,
    R::SendRichMessage: Send,
{
    apply_rich_send_options(bot.send_rich_message(chat_id, rich_message), options).await
}

/// Native plain-text draft backend. Its target is a `UserId`, which prevents
/// accidentally issuing a native draft request for a group at construction.
pub struct NativeTextBackend<R> {
    bot: R,
    chat_id: UserId,
    draft_id: DraftId,
    send_options: TelegramSendOptions,
    draft_options: TelegramDraftOptions,
}

impl<R> NativeTextBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: UserId) -> Self {
        Self {
            bot,
            chat_id,
            draft_id: DraftId::next(),
            send_options: TelegramSendOptions::default(),
            draft_options: TelegramDraftOptions::default(),
        }
    }

    #[must_use]
    pub fn with_draft_id(mut self, draft_id: DraftId) -> Self {
        self.draft_id = draft_id;
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.send_options.message_thread_id = Some(thread_id);
        self.draft_options.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.send_options.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn send_options(mut self, options: TelegramSendOptions) -> Self {
        self.send_options = options;
        self
    }

    #[must_use]
    pub fn draft_options(mut self, options: TelegramDraftOptions) -> Self {
        self.draft_options = options;
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

    fn draft_id(&self) -> Option<DraftId> {
        Some(self.draft_id)
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        apply_draft_options(
            self.bot.send_message_draft(self.chat_id, self.draft_id.get()).text(preview),
            &self.draft_options,
        )
        .await
        .map(|_| PreviewAck)
    }

    async fn commit_segment(&mut self, final_payload: &String) -> Result<Message, RequestError> {
        let result =
            send_text(&self.bot, self.chat_id.into(), final_payload.clone(), &self.send_options)
                .await;
        if result.is_ok() {
            self.draft_id = DraftId::next();
        }
        result
    }

    async fn finish(&mut self, final_payload: &String) -> Result<Message, RequestError> {
        send_text(&self.bot, self.chat_id.into(), final_payload.clone(), &self.send_options).await
    }

    async fn abort(&mut self) -> Result<(), RequestError> {
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
    send_options: TelegramSendOptions,
    draft_options: TelegramDraftOptions,
}

impl<R> NativeRichBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: UserId) -> Self {
        Self {
            bot,
            chat_id,
            draft_id: DraftId::next(),
            send_options: TelegramSendOptions::default(),
            draft_options: TelegramDraftOptions::default(),
        }
    }

    #[must_use]
    pub fn with_draft_id(mut self, draft_id: DraftId) -> Self {
        self.draft_id = draft_id;
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.send_options.message_thread_id = Some(thread_id);
        self.draft_options.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.send_options.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn send_options(mut self, options: TelegramSendOptions) -> Self {
        self.send_options = options;
        self
    }

    #[must_use]
    pub fn draft_options(mut self, options: TelegramDraftOptions) -> Self {
        self.draft_options = options;
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

    fn draft_id(&self) -> Option<DraftId> {
        Some(self.draft_id)
    }

    async fn update(&mut self, preview: InputRichMessage) -> Result<PreviewAck, RequestError> {
        apply_rich_draft_options(
            self.bot.send_rich_message_draft(self.chat_id, self.draft_id.get(), preview),
            &self.draft_options,
        )
        .await
        .map(|_| PreviewAck)
    }

    async fn commit_segment(
        &mut self,
        final_payload: &InputRichMessage,
    ) -> Result<Message, RequestError> {
        let result =
            send_rich(&self.bot, self.chat_id.into(), final_payload.clone(), &self.send_options)
                .await;
        if result.is_ok() {
            self.draft_id = DraftId::next();
        }
        result
    }

    async fn finish(&mut self, final_payload: &InputRichMessage) -> Result<Message, RequestError> {
        send_rich(&self.bot, self.chat_id.into(), final_payload.clone(), &self.send_options).await
    }

    async fn abort(&mut self) -> Result<(), RequestError> {
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
    preview_send_options: TelegramSendOptions,
    final_send_options: TelegramSendOptions,
    edit_options: TelegramEditOptions,
    cleanup: StatusCleanup,
    cleanup_error: Option<CleanupFailure<RequestError>>,
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
            preview_send_options: TelegramSendOptions::default(),
            final_send_options: TelegramSendOptions::default(),
            edit_options: TelegramEditOptions::default(),
            cleanup: StatusCleanup::DeleteAfterFinalSuccess,
            cleanup_error: None,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.preview_send_options.reply_parameters = Some(reply_parameters.clone());
        self.final_send_options.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.preview_send_options.message_thread_id = Some(thread_id);
        self.final_send_options.message_thread_id = Some(thread_id);
        self
    }

    /// Sets options for the permanent rich message.
    #[must_use]
    pub fn send_options(mut self, options: TelegramSendOptions) -> Self {
        self.preview_send_options = options.preview_safe();
        self.final_send_options = options;
        self
    }

    /// Sets options for the temporary status message only.
    #[must_use]
    pub fn preview_send_options(mut self, options: TelegramSendOptions) -> Self {
        self.preview_send_options = options.preview_safe();
        self
    }

    /// Sets options for the permanent rich message only.
    #[must_use]
    pub fn final_send_options(mut self, options: TelegramSendOptions) -> Self {
        self.final_send_options = options;
        self
    }

    #[must_use]
    pub fn edit_options(mut self, options: TelegramEditOptions) -> Self {
        self.edit_options = options;
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

    fn preview_message_id(&self) -> Option<MessageId> {
        self.preview_message_id
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        let message_id = if let Some(message_id) = self.preview_message_id {
            apply_edit_options(
                self.bot.edit_message_text(self.chat_id, message_id, preview),
                &self.edit_options,
            )
            .await
        } else {
            let message =
                send_text(&self.bot, self.chat_id, preview, &self.preview_send_options).await?;
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
        final_payload: &InputRichMessage,
    ) -> Result<Message, RequestError> {
        let result =
            send_rich(&self.bot, self.chat_id, final_payload.clone(), &self.final_send_options)
                .await;
        if result.is_ok() {
            self.cleanup_preview().await;
        }
        result
    }

    async fn finish(&mut self, final_payload: &InputRichMessage) -> Result<Message, RequestError> {
        let result =
            send_rich(&self.bot, self.chat_id, final_payload.clone(), &self.final_send_options)
                .await;
        if result.is_ok() && self.cleanup == StatusCleanup::DeleteAfterFinalSuccess {
            if let Some(message_id) = self.preview_message_id {
                if let Err(error) = self.bot.delete_message(self.chat_id, message_id).await {
                    self.cleanup_error = Some(CleanupFailure { message_id, error });
                } else {
                    self.preview_message_id = None;
                }
            }
        }
        result
    }

    async fn abort(&mut self) -> Result<(), RequestError> {
        if self.cleanup != StatusCleanup::DeleteAfterFinalSuccess {
            return Ok(());
        }
        if let Some(message_id) = self.preview_message_id {
            self.bot.delete_message(self.chat_id, message_id).await?;
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

    fn take_cleanup_error(&mut self) -> Option<Self::Error> {
        self.cleanup_error.take().map(|failure| {
            debug_assert_eq!(self.preview_message_id, Some(failure.message_id));
            failure.error
        })
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
        if let Some(message_id) = self.preview_message_id {
            if let Err(error) = self.bot.delete_message(self.chat_id, message_id).await {
                self.cleanup_error = Some(CleanupFailure { message_id, error });
            } else {
                self.preview_message_id = None;
            }
        }
    }
}

/// Plain status preview followed by a separate plain permanent message.
pub struct StatusThenTextBackend<R> {
    bot: R,
    chat_id: ChatId,
    preview_message_id: Option<MessageId>,
    preview_send_options: TelegramSendOptions,
    final_send_options: TelegramSendOptions,
    edit_options: TelegramEditOptions,
    cleanup: StatusCleanup,
    cleanup_error: Option<CleanupFailure<RequestError>>,
}

impl<R> StatusThenTextBackend<R> {
    #[must_use]
    pub fn new(bot: R, chat_id: ChatId) -> Self {
        Self {
            bot,
            chat_id,
            preview_message_id: None,
            preview_send_options: TelegramSendOptions::default(),
            final_send_options: TelegramSendOptions::default(),
            edit_options: TelegramEditOptions::default(),
            cleanup: StatusCleanup::DeleteAfterFinalSuccess,
            cleanup_error: None,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.preview_send_options.reply_parameters = Some(reply_parameters.clone());
        self.final_send_options.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.preview_send_options.message_thread_id = Some(thread_id);
        self.final_send_options.message_thread_id = Some(thread_id);
        self
    }

    /// Sets options for the permanent text message.
    #[must_use]
    pub fn send_options(mut self, options: TelegramSendOptions) -> Self {
        self.preview_send_options = options.preview_safe();
        self.final_send_options = options;
        self
    }

    /// Sets options for the temporary status message only.
    #[must_use]
    pub fn preview_send_options(mut self, options: TelegramSendOptions) -> Self {
        self.preview_send_options = options.preview_safe();
        self
    }

    /// Sets options for the permanent text message only.
    #[must_use]
    pub fn final_send_options(mut self, options: TelegramSendOptions) -> Self {
        self.final_send_options = options;
        self
    }

    #[must_use]
    pub fn edit_options(mut self, options: TelegramEditOptions) -> Self {
        self.edit_options = options;
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

    fn preview_message_id(&self) -> Option<MessageId> {
        self.preview_message_id
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        if let Some(message_id) = self.preview_message_id {
            apply_edit_options(
                self.bot.edit_message_text(self.chat_id, message_id, preview),
                &self.edit_options,
            )
            .await
            .map(|_| PreviewAck)
            .or_else(|error| {
                if is_message_not_modified(&error) {
                    Ok(PreviewAck)
                } else {
                    Err(error)
                }
            })
        } else {
            let message =
                send_text(&self.bot, self.chat_id, preview, &self.preview_send_options).await?;
            self.preview_message_id = Some(message.id);
            Ok(PreviewAck)
        }
    }

    async fn commit_segment(&mut self, final_payload: &String) -> Result<Message, RequestError> {
        let result =
            send_text(&self.bot, self.chat_id, final_payload.clone(), &self.final_send_options)
                .await;
        if result.is_ok() {
            self.cleanup_preview().await;
        }
        result
    }

    async fn finish(&mut self, final_payload: &String) -> Result<Message, RequestError> {
        let result =
            send_text(&self.bot, self.chat_id, final_payload.clone(), &self.final_send_options)
                .await;
        if result.is_ok() && self.cleanup == StatusCleanup::DeleteAfterFinalSuccess {
            if let Some(message_id) = self.preview_message_id {
                if let Err(error) = self.bot.delete_message(self.chat_id, message_id).await {
                    self.cleanup_error = Some(CleanupFailure { message_id, error });
                } else {
                    self.preview_message_id = None;
                }
            }
        }
        result
    }

    async fn abort(&mut self) -> Result<(), RequestError> {
        if self.cleanup == StatusCleanup::DeleteAfterFinalSuccess {
            if let Some(message_id) = self.preview_message_id {
                self.bot.delete_message(self.chat_id, message_id).await?;
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

    fn take_cleanup_error(&mut self) -> Option<Self::Error> {
        self.cleanup_error.take().map(|failure| {
            debug_assert_eq!(self.preview_message_id, Some(failure.message_id));
            failure.error
        })
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
        if let Some(message_id) = self.preview_message_id {
            if let Err(error) = self.bot.delete_message(self.chat_id, message_id).await {
                self.cleanup_error = Some(CleanupFailure { message_id, error });
            } else {
                self.preview_message_id = None;
            }
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
    send_options: TelegramSendOptions,
    edit_options: TelegramEditOptions,
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
            send_options: TelegramSendOptions::default(),
            edit_options: TelegramEditOptions::default(),
            abort_policy: EditAbortPolicy::KeepPreview,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.send_options.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.send_options.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn send_options(mut self, options: TelegramSendOptions) -> Self {
        self.send_options = options;
        self
    }

    #[must_use]
    pub fn edit_options(mut self, options: TelegramEditOptions) -> Self {
        self.edit_options = options;
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

    fn preview_message_id(&self) -> Option<MessageId> {
        self.message_id
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        let current_fingerprint = fingerprint(&preview);
        if self.last_fingerprint == Some(current_fingerprint) {
            return Ok(PreviewAck);
        }
        let result = if let Some(message_id) = self.message_id {
            apply_edit_options(
                self.bot.edit_message_text(self.chat_id, message_id, preview),
                &self.edit_options,
            )
            .await
        } else {
            let message = send_text(&self.bot, self.chat_id, preview, &self.send_options).await?;
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

    async fn commit_segment(&mut self, final_payload: &String) -> Result<Message, RequestError> {
        let result = if let Some(message_id) = self.message_id {
            apply_edit_options(
                self.bot.edit_message_text(self.chat_id, message_id, final_payload.clone()),
                &self.edit_options,
            )
            .await
        } else {
            send_text(&self.bot, self.chat_id, final_payload.clone(), &self.send_options).await
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

    async fn finish(&mut self, final_payload: &String) -> Result<Message, RequestError> {
        if let Some(message_id) = self.message_id {
            match apply_edit_options(
                self.bot.edit_message_text(self.chat_id, message_id, final_payload.clone()),
                &self.edit_options,
            )
            .await
            {
                Ok(message) => Ok(message),
                Err(error) if is_message_not_modified(&error) => {
                    self.last_message.clone().ok_or(error)
                }
                Err(error) => Err(error),
            }
        } else {
            send_text(&self.bot, self.chat_id, final_payload.clone(), &self.send_options).await
        }
    }

    async fn abort(&mut self) -> Result<(), RequestError> {
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
    send_options: TelegramSendOptions,
    edit_options: TelegramEditOptions,
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
            send_options: TelegramSendOptions::default(),
            edit_options: TelegramEditOptions::default(),
            abort_policy: EditAbortPolicy::KeepPreview,
        }
    }

    #[must_use]
    pub fn reply_parameters(mut self, reply_parameters: ReplyParameters) -> Self {
        self.send_options.reply_parameters = Some(reply_parameters);
        self
    }

    #[must_use]
    pub fn message_thread_id(mut self, thread_id: ThreadId) -> Self {
        self.send_options.message_thread_id = Some(thread_id);
        self
    }

    #[must_use]
    pub fn send_options(mut self, options: TelegramSendOptions) -> Self {
        self.send_options = options;
        self
    }

    #[must_use]
    pub fn edit_options(mut self, options: TelegramEditOptions) -> Self {
        self.edit_options = options;
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

    fn preview_message_id(&self) -> Option<MessageId> {
        self.message_id
    }

    async fn update(&mut self, preview: String) -> Result<PreviewAck, RequestError> {
        let current_fingerprint = fingerprint(&preview);
        if self.last_fingerprint == Some(current_fingerprint) {
            return Ok(PreviewAck);
        }
        let result = if let Some(message_id) = self.message_id {
            apply_edit_options(
                self.bot.edit_message_text(self.chat_id, message_id, preview),
                &self.edit_options,
            )
            .await
        } else {
            let message = send_text(&self.bot, self.chat_id, preview, &self.send_options).await?;
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
        final_payload: &InputRichMessage,
    ) -> Result<Message, RequestError> {
        let result = if let Some(message_id) = self.message_id {
            match apply_edit_options(
                self.bot.edit_message_rich_text(self.chat_id, message_id, final_payload.clone()),
                &self.edit_options,
            )
            .await
            {
                Ok(message) => Ok(message),
                Err(error) if is_message_not_modified(&error) => {
                    self.last_message.clone().ok_or(error)
                }
                Err(error) => Err(error),
            }
        } else {
            send_rich(&self.bot, self.chat_id, final_payload.clone(), &self.send_options).await
        };
        if result.is_ok() {
            self.message_id = None;
            self.last_message = None;
            self.last_fingerprint = None;
        }
        result
    }

    async fn finish(&mut self, final_payload: &InputRichMessage) -> Result<Message, RequestError> {
        if let Some(message_id) = self.message_id {
            match apply_edit_options(
                self.bot.edit_message_rich_text(self.chat_id, message_id, final_payload.clone()),
                &self.edit_options,
            )
            .await
            {
                Ok(message) => Ok(message),
                Err(error) if is_message_not_modified(&error) => {
                    self.last_message.clone().ok_or(error)
                }
                Err(error) => Err(error),
            }
        } else {
            send_rich(&self.bot, self.chat_id, final_payload.clone(), &self.send_options).await
        }
    }

    async fn abort(&mut self) -> Result<(), RequestError> {
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

    /// Creates a native text drafter with a limiter owned only by this drafter.
    ///
    /// Prefer [`Self::native_text`] with one bot-scoped limiter shared by all
    /// drafters when several workers use the same bot token.
    pub fn native_text_with_isolated_limiter<R>(
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

#[cfg(test)]
mod tests {
    use teloxide_core::{
        requests::Requester,
        types::{InlineKeyboardButton, MessageId, ThreadId},
    };

    use super::*;

    #[test]
    fn text_send_options_are_applied_to_typed_request() {
        let options = TelegramSendOptions::default()
            .message_thread_id(ThreadId(MessageId(9)))
            .disable_notification(true)
            .protect_content(true)
            .parse_mode(ParseMode::Html)
            .reply_parameters(ReplyParameters::new(MessageId(4)));
        let request =
            apply_text_send_options(Bot::new("token").send_message(ChatId(1), "preview"), &options);

        assert_eq!(request.message_thread_id, Some(ThreadId(MessageId(9))));
        assert_eq!(request.disable_notification, Some(true));
        assert_eq!(request.protect_content, Some(true));
        assert_eq!(request.parse_mode, Some(ParseMode::Html));
        assert_eq!(request.reply_parameters, Some(ReplyParameters::new(MessageId(4))));
    }

    #[test]
    fn status_preview_options_drop_final_only_fields() {
        let options = TelegramSendOptions::default()
            .allow_paid_broadcast(true)
            .message_effect_id(EffectId("effect".to_owned()))
            .suggested_post_parameters(SuggestedPostParameters { price: None, send_date: None })
            .reply_markup(ReplyMarkup::inline_kb(std::iter::empty::<Vec<InlineKeyboardButton>>()));
        let preview_options = options.preview_safe();

        assert_eq!(preview_options.allow_paid_broadcast, None);
        assert_eq!(preview_options.message_effect_id, None);
        assert_eq!(preview_options.suggested_post_parameters, None);
        assert_eq!(preview_options.reply_markup, None);
    }

    #[test]
    fn status_reply_parameters_are_shared_by_preview_and_final() {
        let reply_parameters = ReplyParameters::new(MessageId(7));
        let text_backend = StatusThenTextBackend::new(Bot::new("token"), ChatId(1))
            .reply_parameters(reply_parameters.clone());
        let rich_backend = StatusThenRichBackend::new(Bot::new("token"), ChatId(1))
            .reply_parameters(reply_parameters.clone());

        assert_eq!(
            text_backend.preview_send_options.reply_parameters,
            Some(reply_parameters.clone())
        );
        assert_eq!(
            text_backend.final_send_options.reply_parameters,
            Some(reply_parameters.clone())
        );
        assert_eq!(
            rich_backend.preview_send_options.reply_parameters,
            Some(reply_parameters.clone())
        );
        assert_eq!(rich_backend.final_send_options.reply_parameters, Some(reply_parameters));
    }

    #[test]
    fn draft_and_edit_options_are_applied_to_typed_requests() {
        let draft_options = TelegramDraftOptions::default()
            .message_thread_id(ThreadId(MessageId(3)))
            .parse_mode(ParseMode::Html);
        let draft = apply_draft_options(
            Bot::new("token").send_message_draft(UserId(1), 7).text("preview"),
            &draft_options,
        );
        assert_eq!(draft.message_thread_id, Some(ThreadId(MessageId(3))));
        assert_eq!(draft.parse_mode, Some(ParseMode::Html));

        let edit_options = TelegramEditOptions::default().parse_mode(ParseMode::Html);
        let edit = apply_edit_options(
            Bot::new("token").edit_message_text(ChatId(1), MessageId(2), "preview"),
            &edit_options,
        );
        assert_eq!(edit.parse_mode, Some(ParseMode::Html));
    }
}
