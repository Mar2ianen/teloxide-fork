use std::fmt;

/// A segment in a [`RequestFieldPath`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum RequestFieldPathSegment {
    /// A named request field.
    Field(&'static str),
    /// An item in a request collection.
    Index(usize),
}

/// A structured path to a request field.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct RequestFieldPath {
    segments: Vec<RequestFieldPathSegment>,
}

impl RequestFieldPath {
    /// Creates an empty path.
    #[must_use]
    pub const fn new() -> Self {
        Self { segments: Vec::new() }
    }

    /// Creates a path containing one field segment.
    #[must_use]
    pub fn field(name: &'static str) -> Self {
        let mut path = Self::new();
        path.push_field(name);
        path
    }

    /// Appends a field segment.
    pub fn push_field(&mut self, name: &'static str) {
        self.segments.push(RequestFieldPathSegment::Field(name));
    }

    /// Appends an index segment.
    pub fn push_index(&mut self, index: usize) {
        self.segments.push(RequestFieldPathSegment::Index(index));
    }

    /// Removes the last segment.
    pub fn pop(&mut self) {
        self.segments.pop();
    }

    /// Returns the path segments.
    #[must_use]
    pub fn segments(&self) -> &[RequestFieldPathSegment] {
        &self.segments
    }
}

impl fmt::Display for RequestFieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, segment) in self.segments.iter().enumerate() {
            match segment {
                RequestFieldPathSegment::Field(name) => {
                    if index != 0 {
                        f.write_str(".")?;
                    }
                    f.write_str(name)?;
                }
                RequestFieldPathSegment::Index(value) => write!(f, "[{value}]")?,
            }
        }

        if self.segments.is_empty() {
            f.write_str("request")?;
        }
        Ok(())
    }
}

/// A reason why a value is invalid before it is sent to Telegram.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum InvalidValueReason {
    /// The value must not be zero.
    MustBeNonZero,
}

impl fmt::Display for InvalidValueReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MustBeNonZero => f.write_str("must be non-zero"),
        }
    }
}

/// The rich-message sending context used by context-sensitive validation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RichMessageContext {
    /// `sendRichMessage` or another regular message send method.
    Send,
    /// `editMessageText` for a regular message.
    Edit,
    /// `editMessageText` for an inline message.
    EditInline,
    /// `sendRichMessageDraft`.
    Draft,
    /// An inline query result.
    InlineResult,
    /// An `answerGuestQuery` result.
    GuestResult,
    /// An `answerWebAppQuery` or `savePreparedInlineMessage` result.
    PreparedMessage,
}

impl fmt::Display for RichMessageContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Send => "rich message send",
            Self::Edit => "rich message edit",
            Self::EditInline => "inline rich message edit",
            Self::Draft => "rich message draft",
            Self::InlineResult => "inline result",
            Self::GuestResult => "guest result",
            Self::PreparedMessage => "prepared message",
        };
        f.write_str(name)
    }
}

/// A statically known reason why a request cannot be sent in its current form.
#[derive(Clone, Debug, Eq, Hash, PartialEq, thiserror::Error)]
pub enum RequestValidationError {
    /// A scalar value violates a method or Bot API constraint.
    #[error("invalid value at {path}: {reason}")]
    InvalidValue {
        /// Path to the invalid value.
        path: RequestFieldPath,
        /// The known violated constraint.
        reason: InvalidValueReason,
    },
    /// A valid object is not allowed by the selected method context.
    #[error("{path} is not supported in {context}")]
    UnsupportedInContext {
        /// Path to the unsupported object.
        path: RequestFieldPath,
        /// The method context that rejected it.
        context: RichMessageContext,
    },
    /// A new file upload is forbidden by the selected method.
    #[error("direct file upload is not allowed at {path}")]
    DirectUploadNotAllowed {
        /// Path to the forbidden file source.
        path: RequestFieldPath,
    },
}

/// Validates a request or another payload before serialization and dispatch.
///
/// Implementations must only check constraints that can be determined from the
/// value itself. A successful result does not guarantee that Telegram accepts
/// the request; permissions and server-side capabilities remain Telegram's
/// responsibility. A validation error means that no HTTP request was sent.
pub trait Validate {
    /// Checks static request constraints without serializing or consuming
    /// `self`.
    fn validate(&self) -> Result<(), RequestValidationError>;
}

impl<T> Validate for T
where
    T: crate::requests::Payload + ?Sized,
{
    fn validate(&self) -> Result<(), RequestValidationError> {
        crate::requests::Payload::validate(self)
    }
}

/// Validates a value whose validity depends on a selected request context.
pub trait ValidateWith<C> {
    /// Checks static constraints for `self` in `context`.
    fn validate_with(&self, context: &C) -> Result<(), RequestValidationError>;
}

/// Validates the non-rich `sendMessageDraft` payload.
pub(crate) fn validate_send_message_draft(
    payload: &crate::payloads::SendMessageDraft,
) -> Result<(), RequestValidationError> {
    if payload.draft_id == 0 {
        return Err(RequestValidationError::InvalidValue {
            path: RequestFieldPath::field("draft_id"),
            reason: InvalidValueReason::MustBeNonZero,
        });
    }
    Ok(())
}

impl RequestFieldPath {
    #[cfg(test)]
    fn with_field_for_test(mut self, name: &'static str) -> Self {
        self.push_field(name);
        self
    }

    #[cfg(test)]
    fn with_index_for_test(mut self, index: usize) -> Self {
        self.push_index(index);
        self
    }
}

fn validate_rich_message_at(
    message: &crate::types::InputRichMessage,
    context: RichMessageContext,
    path: &mut RequestFieldPath,
) -> Result<(), RequestValidationError> {
    if let Some(blocks) = message.blocks_ref() {
        path.push_field("blocks");
        validate_blocks(blocks, context, path)?;
        path.pop();
    }

    if let Some(media) = &message.media {
        path.push_field("media");
        for (index, media) in media.iter().enumerate() {
            path.push_index(index);
            path.push_field("media");
            validate_rich_media_content(&media.media, context, path)?;
            path.pop();
            path.pop();
        }
        path.pop();
    }

    Ok(())
}

fn validate_blocks(
    blocks: &[crate::types::InputRichBlock],
    context: RichMessageContext,
    path: &mut RequestFieldPath,
) -> Result<(), RequestValidationError> {
    for (index, block) in blocks.iter().enumerate() {
        path.push_index(index);
        validate_block(block, context, path)?;
        path.pop();
    }
    Ok(())
}

fn validate_block(
    block: &crate::types::InputRichBlock,
    context: RichMessageContext,
    path: &mut RequestFieldPath,
) -> Result<(), RequestValidationError> {
    use crate::types::InputRichBlock;

    match block {
        InputRichBlock::List(value) => {
            path.push_field("list");
            path.push_field("items");
            for (index, item) in value.items.iter().enumerate() {
                path.push_index(index);
                path.push_field("blocks");
                validate_blocks(&item.blocks, context, path)?;
                path.pop();
                path.pop();
            }
            path.pop();
            path.pop();
        }
        InputRichBlock::Blockquote(value) => {
            path.push_field("blockquote");
            path.push_field("blocks");
            validate_blocks(&value.blocks, context, path)?;
            path.pop();
            path.pop();
        }
        InputRichBlock::Collage(value) => {
            path.push_field("collage");
            path.push_field("blocks");
            validate_blocks(&value.blocks, context, path)?;
            path.pop();
            path.pop();
        }
        InputRichBlock::Slideshow(value) => {
            path.push_field("slideshow");
            path.push_field("blocks");
            validate_blocks(&value.blocks, context, path)?;
            path.pop();
            path.pop();
        }
        InputRichBlock::Details(value) => {
            path.push_field("details");
            path.push_field("blocks");
            validate_blocks(&value.blocks, context, path)?;
            path.pop();
            path.pop();
        }
        InputRichBlock::Animation(value) => {
            path.push_field("animation");
            validate_animation(&value.animation, context, path)?;
            path.pop();
        }
        InputRichBlock::Audio(value) => {
            path.push_field("audio");
            validate_audio(&value.audio, context, path)?;
            path.pop();
        }
        InputRichBlock::Photo(value) => {
            path.push_field("photo");
            validate_photo(&value.photo, context, path)?;
            path.pop();
        }
        InputRichBlock::Video(value) => {
            path.push_field("video");
            validate_video(&value.video, context, path)?;
            path.pop();
        }
        InputRichBlock::VoiceNote(value) => {
            path.push_field("voice_note");
            validate_voice_note(&value.voice_note, context, path)?;
            path.pop();
        }
        InputRichBlock::Thinking(_) if context != RichMessageContext::Draft => {
            return Err(RequestValidationError::UnsupportedInContext {
                path: path.clone(),
                context,
            });
        }
        InputRichBlock::Paragraph(_)
        | InputRichBlock::Heading(_)
        | InputRichBlock::Pre(_)
        | InputRichBlock::Footer(_)
        | InputRichBlock::Divider(_)
        | InputRichBlock::MathematicalExpression(_)
        | InputRichBlock::Anchor(_)
        | InputRichBlock::Pullquote(_)
        | InputRichBlock::Table(_)
        | InputRichBlock::Map(_)
        | InputRichBlock::Thinking(_) => {}
    }

    Ok(())
}

fn validate_rich_media_content(
    media: &crate::types::InputRichMessageMediaContent,
    context: RichMessageContext,
    path: &RequestFieldPath,
) -> Result<(), RequestValidationError> {
    use crate::types::InputRichMessageMediaContent;

    match media {
        InputRichMessageMediaContent::Animation(value) => validate_animation(value, context, path),
        InputRichMessageMediaContent::Audio(value) => validate_audio(value, context, path),
        InputRichMessageMediaContent::Photo(value) => validate_photo(value, context, path),
        InputRichMessageMediaContent::Video(value) => validate_video(value, context, path),
        InputRichMessageMediaContent::VoiceNote(value) => validate_voice_note(value, context, path),
    }
}

fn validate_animation(
    media: &crate::types::InputMediaAnimation,
    context: RichMessageContext,
    path: &RequestFieldPath,
) -> Result<(), RequestValidationError> {
    let mut path = path.clone();
    path.push_field("media");
    validate_file(&media.media, context, &path)?;
    path.pop();
    validate_optional_file(media.thumbnail.as_ref(), context, &mut path, "thumbnail")
}

fn validate_audio(
    media: &crate::types::InputMediaAudio,
    context: RichMessageContext,
    path: &RequestFieldPath,
) -> Result<(), RequestValidationError> {
    let mut path = path.clone();
    path.push_field("media");
    validate_file(&media.media, context, &path)?;
    path.pop();
    validate_optional_file(media.thumbnail.as_ref(), context, &mut path, "thumbnail")
}

fn validate_photo(
    media: &crate::types::InputMediaPhoto,
    context: RichMessageContext,
    path: &RequestFieldPath,
) -> Result<(), RequestValidationError> {
    let mut path = path.clone();
    path.push_field("media");
    validate_file(&media.media, context, &path)
}

fn validate_video(
    media: &crate::types::InputMediaVideo,
    context: RichMessageContext,
    path: &RequestFieldPath,
) -> Result<(), RequestValidationError> {
    let mut path = path.clone();
    path.push_field("media");
    validate_file(&media.media, context, &path)?;
    path.pop();
    validate_optional_file(media.thumbnail.as_ref(), context, &mut path, "thumbnail")?;
    validate_optional_file(media.cover.as_ref(), context, &mut path, "cover")
}

fn validate_voice_note(
    media: &crate::types::InputMediaVoiceNote,
    context: RichMessageContext,
    path: &RequestFieldPath,
) -> Result<(), RequestValidationError> {
    let mut path = path.clone();
    path.push_field("media");
    validate_file(&media.media, context, &path)
}

fn validate_optional_file(
    file: Option<&crate::types::InputFile>,
    context: RichMessageContext,
    path: &mut RequestFieldPath,
    field: &'static str,
) -> Result<(), RequestValidationError> {
    let Some(file) = file else { return Ok(()) };
    path.push_field(field);
    let result = validate_file(file, context, path);
    path.pop();
    result
}

fn validate_file(
    file: &crate::types::InputFile,
    context: RichMessageContext,
    path: &RequestFieldPath,
) -> Result<(), RequestValidationError> {
    if matches!(context, RichMessageContext::Draft | RichMessageContext::EditInline)
        && file.needs_attach()
    {
        return Err(RequestValidationError::DirectUploadNotAllowed { path: path.clone() });
    }
    Ok(())
}

impl ValidateWith<RichMessageContext> for crate::types::InputRichMessage {
    fn validate_with(&self, context: &RichMessageContext) -> Result<(), RequestValidationError> {
        let mut path = RequestFieldPath::field("rich_message");
        validate_rich_message_at(self, *context, &mut path)
    }
}

pub(crate) fn validate_send_rich_message(
    payload: &crate::payloads::SendRichMessage,
) -> Result<(), RequestValidationError> {
    let mut path = RequestFieldPath::field("rich_message");
    validate_rich_message_at(&payload.rich_message, RichMessageContext::Send, &mut path)
}

pub(crate) fn validate_send_rich_message_draft(
    payload: &crate::payloads::SendRichMessageDraft,
) -> Result<(), RequestValidationError> {
    if payload.draft_id == 0 {
        return Err(RequestValidationError::InvalidValue {
            path: RequestFieldPath::field("draft_id"),
            reason: InvalidValueReason::MustBeNonZero,
        });
    }

    let mut path = RequestFieldPath::field("rich_message");
    validate_rich_message_at(&payload.rich_message, RichMessageContext::Draft, &mut path)
}

pub(crate) fn validate_edit_message_text(
    payload: &crate::payloads::EditMessageText,
) -> Result<(), RequestValidationError> {
    let Some(rich_message) = &payload.rich_message else { return Ok(()) };
    let mut path = RequestFieldPath::field("rich_message");
    validate_rich_message_at(rich_message, RichMessageContext::Edit, &mut path)
}

pub(crate) fn validate_edit_message_text_inline(
    payload: &crate::payloads::EditMessageTextInline,
) -> Result<(), RequestValidationError> {
    let Some(rich_message) = &payload.rich_message else { return Ok(()) };
    let mut path = RequestFieldPath::field("rich_message");
    validate_rich_message_at(rich_message, RichMessageContext::EditInline, &mut path)
}

fn validate_inline_result_at(
    result: &crate::types::InlineQueryResult,
    context: RichMessageContext,
    path: &mut RequestFieldPath,
) -> Result<(), RequestValidationError> {
    let Some(content) = result.input_message_content_ref() else { return Ok(()) };
    path.push_field("input_message_content");
    if let crate::types::InputMessageContent::Rich(content) = content {
        path.push_field("rich_message");
        validate_rich_message_at(&content.rich_message, context, path)?;
        path.pop();
    }
    path.pop();
    Ok(())
}

fn validate_result_payload(
    result: &crate::types::InlineQueryResult,
    context: RichMessageContext,
    root: &'static str,
) -> Result<(), RequestValidationError> {
    let mut path = RequestFieldPath::field(root);
    validate_inline_result_at(result, context, &mut path)
}

pub(crate) fn validate_answer_inline_query(
    payload: &crate::payloads::AnswerInlineQuery,
) -> Result<(), RequestValidationError> {
    let mut path = RequestFieldPath::field("results");
    for (index, result) in payload.results.iter().enumerate() {
        path.push_index(index);
        validate_inline_result_at(result, RichMessageContext::InlineResult, &mut path)?;
        path.pop();
    }
    Ok(())
}

pub(crate) fn validate_answer_guest_query(
    payload: &crate::payloads::AnswerGuestQuery,
) -> Result<(), RequestValidationError> {
    validate_result_payload(&payload.result, RichMessageContext::GuestResult, "result")
}

pub(crate) fn validate_answer_web_app_query(
    payload: &crate::payloads::AnswerWebAppQuery,
) -> Result<(), RequestValidationError> {
    validate_result_payload(&payload.result, RichMessageContext::PreparedMessage, "result")
}

pub(crate) fn validate_save_prepared_inline_message(
    payload: &crate::payloads::SavePreparedInlineMessage,
) -> Result<(), RequestValidationError> {
    validate_result_payload(&payload.result, RichMessageContext::PreparedMessage, "result")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payloads::{
            AnswerGuestQuery, AnswerInlineQuery, AnswerWebAppQuery, EditMessageText,
            EditMessageTextInline, SavePreparedInlineMessage, SendMessageDraft, SendRichMessage,
            SendRichMessageDraft,
        },
        requests::{Payload, Request, Requester},
        types::{
            FileId, InlineQueryId, InlineQueryResult, InlineQueryResultArticle, InputFile,
            InputMediaPhoto, InputMediaVideo, InputMediaVoiceNote, InputMessageContent,
            InputRichBlock, InputRichBlockBlockQuotation, InputRichBlockDetails,
            InputRichBlockList, InputRichBlockListItem, InputRichBlockPhoto,
            InputRichBlockThinking, InputRichBlockVideo, InputRichBlockVoiceNote, InputRichMessage,
            InputRichMessageContent, InputRichMessageMedia, InputRichMessageMediaContent, RichText,
            UserId,
        },
        Bot,
    };

    #[test]
    fn field_path_formats_nested_fields_and_indexes() {
        let mut path = RequestFieldPath::field("rich_message");
        path.push_field("blocks");
        path.push_index(2);
        path.push_field("details");
        path.push_field("blocks");
        path.push_index(1);

        assert_eq!(path.to_string(), "rich_message.blocks[2].details.blocks[1]");
    }

    #[test]
    fn send_message_draft_rejects_zero_draft_id() {
        let payload = SendMessageDraft::new(UserId(1), 0);

        assert_eq!(
            Payload::validate(&payload),
            Err(RequestValidationError::InvalidValue {
                path: RequestFieldPath::field("draft_id"),
                reason: InvalidValueReason::MustBeNonZero,
            })
        );
        assert_eq!(
            Payload::validate(&payload).unwrap_err().to_string(),
            "invalid value at draft_id: must be non-zero"
        );
    }

    #[test]
    fn send_message_draft_accepts_non_zero_draft_id() {
        let payload = SendMessageDraft::new(UserId(1), -1);
        assert_eq!(Validate::validate(&payload), Ok(()));
    }

    #[tokio::test]
    async fn invalid_payload_is_rejected_before_http_dispatch() {
        let result = Bot::new("token").send_message_draft(UserId(1), 0).send().await;

        assert!(matches!(
            result,
            Err(crate::RequestError::Validation(RequestValidationError::InvalidValue { .. }))
        ));
    }

    #[tokio::test]
    async fn valid_payload_reaches_the_transport() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut request);
            std::io::Write::write_all(
                &mut stream,
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"ok\":true,\"result\":true}",
            )
            .unwrap();
        });

        let bot = Bot::new("token").set_api_url(format!("http://{address}/").parse().unwrap());
        let result = bot.send_message_draft(UserId(1), 1).send().await;

        assert!(result.is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn invalid_rich_payload_is_rejected_before_dispatch() {
        let result =
            Bot::new("token").send_rich_message(UserId(1), thinking_message()).send().await;

        assert!(matches!(
            result,
            Err(crate::RequestError::Validation(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::Send,
                ..
            }))
        ));
    }

    fn thinking_message() -> InputRichMessage {
        InputRichMessage::blocks([InputRichBlock::Thinking(InputRichBlockThinking {
            text: RichText::from("thinking"),
        })])
    }

    #[test]
    fn thinking_is_only_valid_in_draft_context() {
        let message = thinking_message();
        assert_eq!(message.validate_with(&RichMessageContext::Draft), Ok(()));

        for context in [
            RichMessageContext::Send,
            RichMessageContext::Edit,
            RichMessageContext::EditInline,
            RichMessageContext::InlineResult,
        ] {
            let error = message.validate_with(&context).unwrap_err();
            assert_eq!(
                error,
                RequestValidationError::UnsupportedInContext {
                    path: RequestFieldPath::field("rich_message")
                        .with_field_for_test("blocks")
                        .with_index_for_test(0),
                    context,
                }
            );
        }
    }

    #[test]
    fn thinking_in_details_preserves_the_nested_path() {
        let message = InputRichMessage::blocks([InputRichBlock::Details(InputRichBlockDetails {
            summary: RichText::from("summary"),
            blocks: vec![InputRichBlock::Thinking(InputRichBlockThinking {
                text: RichText::from("thinking"),
            })],
            is_open: None,
        })]);

        let error = message.validate_with(&RichMessageContext::Send).unwrap_err();
        assert_eq!(
            error,
            RequestValidationError::UnsupportedInContext {
                path: RequestFieldPath::field("rich_message")
                    .with_field_for_test("blocks")
                    .with_index_for_test(0)
                    .with_field_for_test("details")
                    .with_field_for_test("blocks")
                    .with_index_for_test(0),
                context: RichMessageContext::Send,
            }
        );
        assert_eq!(
            error.to_string(),
            "rich_message.blocks[0].details.blocks[0] is not supported in rich message send"
        );
    }

    #[test]
    fn thinking_in_list_and_blockquote_is_checked_recursively() {
        let list = InputRichMessage::blocks([InputRichBlock::List(InputRichBlockList {
            items: vec![InputRichBlockListItem {
                blocks: vec![InputRichBlock::Thinking(InputRichBlockThinking {
                    text: RichText::from("thinking"),
                })],
                has_checkbox: None,
                is_checked: None,
                value: None,
                type_field: None,
            }],
        })]);
        let list_error = list.validate_with(&RichMessageContext::Send).unwrap_err();
        assert_eq!(
            list_error.to_string(),
            "rich_message.blocks[0].list.items[0].blocks[0] is not supported in rich message send"
        );

        let quote =
            InputRichMessage::blocks([InputRichBlock::Blockquote(InputRichBlockBlockQuotation {
                blocks: vec![InputRichBlock::Thinking(InputRichBlockThinking {
                    text: RichText::from("thinking"),
                })],
                credit: None,
            })]);
        let quote_error = quote.validate_with(&RichMessageContext::Send).unwrap_err();
        assert_eq!(
            quote_error.to_string(),
            "rich_message.blocks[0].blockquote.blocks[0] is not supported in rich message send"
        );
    }

    #[test]
    fn draft_rejects_every_direct_upload_source_but_accepts_reusable_files() {
        let sources = [
            InputFile::memory("memory"),
            InputFile::file("/tmp/file"),
            InputFile::read(tokio::io::empty()),
        ];
        for file in sources {
            let message = InputRichMessage::blocks([InputRichBlock::Photo(InputRichBlockPhoto {
                photo: InputMediaPhoto::new(file),
                caption: None,
            })]);
            assert!(matches!(
                message.validate_with(&RichMessageContext::Draft),
                Err(RequestValidationError::DirectUploadNotAllowed { path })
                    if path.to_string() == "rich_message.blocks[0].photo.media"
            ));
        }

        let file_id = InputRichMessage::blocks([InputRichBlock::Photo(InputRichBlockPhoto {
            photo: InputMediaPhoto::new(InputFile::file_id(FileId("file-id".to_owned()))),
            caption: None,
        })]);
        assert_eq!(file_id.validate_with(&RichMessageContext::Draft), Ok(()));

        let url = InputRichMessage::blocks([InputRichBlock::Photo(InputRichBlockPhoto {
            photo: InputMediaPhoto::new(InputFile::url(
                "https://example.com/photo.jpg".parse().unwrap(),
            )),
            caption: None,
        })]);
        assert_eq!(url.validate_with(&RichMessageContext::Draft), Ok(()));
    }

    #[test]
    fn draft_rejects_nested_video_thumbnail_and_cover_with_exact_paths() {
        let thumbnail = InputRichMessage::blocks([InputRichBlock::Video(InputRichBlockVideo {
            video: InputMediaVideo::new(InputFile::file_id(FileId("video".to_owned())))
                .thumbnail(InputFile::memory("thumbnail")),
            caption: None,
        })]);
        assert!(matches!(
            thumbnail.validate_with(&RichMessageContext::Draft),
            Err(RequestValidationError::DirectUploadNotAllowed { path })
                if path.to_string() == "rich_message.blocks[0].video.thumbnail"
        ));

        let cover = InputRichMessage::blocks([InputRichBlock::Video(InputRichBlockVideo {
            video: InputMediaVideo::new(InputFile::file_id(FileId("video".to_owned())))
                .cover(InputFile::memory("cover")),
            caption: None,
        })]);
        assert!(matches!(
            cover.validate_with(&RichMessageContext::Draft),
            Err(RequestValidationError::DirectUploadNotAllowed { path })
                if path.to_string() == "rich_message.blocks[0].video.cover"
        ));
    }

    #[test]
    fn draft_rejects_top_level_media_and_voice_note_uploads() {
        let top_level = InputRichMessage::html("<img src=\"tg://photo?id=photo\">").media([
            InputRichMessageMedia::new(
                "photo",
                InputRichMessageMediaContent::Photo(InputMediaPhoto::new(InputFile::memory(
                    "photo",
                ))),
            ),
        ]);
        assert!(matches!(
            top_level.validate_with(&RichMessageContext::Draft),
            Err(RequestValidationError::DirectUploadNotAllowed { path })
                if path.to_string() == "rich_message.media[0].media.media"
        ));

        let voice =
            InputRichMessage::blocks([InputRichBlock::VoiceNote(InputRichBlockVoiceNote {
                voice_note: InputMediaVoiceNote::new(InputFile::memory("voice")),
                caption: None,
            })]);
        assert!(matches!(
            voice.validate_with(&RichMessageContext::Draft),
            Err(RequestValidationError::DirectUploadNotAllowed { path })
                if path.to_string() == "rich_message.blocks[0].voice_note.media"
        ));
    }

    #[test]
    fn validation_does_not_initialize_attachment_ids() {
        let file = InputFile::memory("photo");
        let message = InputRichMessage::blocks([InputRichBlock::Photo(InputRichBlockPhoto {
            photo: InputMediaPhoto::new(file.clone()),
            caption: None,
        })]);
        assert!(!file.attachment_id_initialized());
        let _ = message.validate_with(&RichMessageContext::Draft);
        assert!(!file.attachment_id_initialized());
    }

    #[test]
    fn rich_payload_hooks_use_their_declared_contexts() {
        let thinking = thinking_message();
        assert!(matches!(
            Payload::validate(&SendRichMessage::new(UserId(1), thinking.clone())),
            Err(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::Send,
                ..
            })
        ));
        assert_eq!(Payload::validate(&SendRichMessageDraft::new(UserId(1), 1, thinking)), Ok(()));
        assert!(matches!(
            Payload::validate(&EditMessageText::rich(
                UserId(1),
                crate::types::MessageId(1),
                thinking_message(),
            )),
            Err(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::Edit,
                ..
            })
        ));
        assert!(matches!(
            Payload::validate(&EditMessageTextInline::rich("inline", thinking_message())),
            Err(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::EditInline,
                ..
            })
        ));
    }

    #[test]
    fn inline_result_hooks_validate_rich_content() {
        let result = InlineQueryResult::Article(InlineQueryResultArticle::new(
            "id",
            "title",
            InputMessageContent::Rich(InputRichMessageContent::new(thinking_message())),
        ));
        assert!(matches!(
            Payload::validate(&AnswerInlineQuery::new(
                InlineQueryId("query".to_owned()),
                [result.clone()],
            )),
            Err(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::InlineResult,
                ..
            })
        ));
        assert!(matches!(
            Payload::validate(&AnswerGuestQuery::new("guest", result.clone())),
            Err(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::GuestResult,
                ..
            })
        ));
        assert!(matches!(
            Payload::validate(&AnswerWebAppQuery::new("web-app", result.clone())),
            Err(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::PreparedMessage,
                ..
            })
        ));
        assert!(matches!(
            Payload::validate(&SavePreparedInlineMessage::new(UserId(1), result)),
            Err(RequestValidationError::UnsupportedInContext {
                context: RichMessageContext::PreparedMessage,
                ..
            })
        ));
    }
}
