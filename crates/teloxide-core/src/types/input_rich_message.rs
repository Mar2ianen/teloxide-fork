use serde::Serialize;
use serde_json::Value;

use crate::types::{
    InputFile, InputFileLike, InputMediaAnimation, InputMediaAudio, InputMediaPhoto,
    InputMediaVideo, MessageEntity, ParseMode,
};

/// Describes a rich message to be sent.
///
/// Exactly one of [`html`](Self::html), [`markdown`](Self::markdown), or
/// [`blocks`](Self::blocks) should be present.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct InputRichMessage {
    /// Content described as a list of blocks.
    ///
    /// This is a temporary raw representation; a typed rich-block AST can
    /// replace it without changing the request methods.
    ///
    /// Local uploads embedded directly in raw blocks are not traversed. Use
    /// file IDs/URLs in raw blocks, or put uploads in [`Self::media`].
    pub blocks: Option<Vec<Value>>,
    /// Content using Telegram rich-message HTML formatting.
    pub html: Option<String>,
    /// Content using Telegram rich-message Markdown formatting.
    pub markdown: Option<String>,
    /// Media referenced by `tg://photo`, `tg://video`, or `tg://audio` links.
    pub media: Option<Vec<InputRichMessageMedia>>,
    /// Show the rich message right-to-left.
    pub is_rtl: Option<bool>,
    /// Skip automatic entity detection.
    pub skip_entity_detection: Option<bool>,
}

impl InputRichMessage {
    /// Creates a rich message from HTML.
    pub fn html(content: impl Into<String>) -> Self {
        Self {
            blocks: None,
            html: Some(content.into()),
            markdown: None,
            media: None,
            is_rtl: None,
            skip_entity_detection: None,
        }
    }

    /// Creates a rich message from Markdown.
    pub fn markdown(content: impl Into<String>) -> Self {
        Self {
            blocks: None,
            html: None,
            markdown: Some(content.into()),
            media: None,
            is_rtl: None,
            skip_entity_detection: None,
        }
    }

    /// Creates a rich message from raw block JSON values.
    ///
    /// Raw blocks currently support only remote/file-id media. `attach://`
    /// references created outside [`Self::media`] have no matching multipart
    /// part and are therefore unsupported.
    pub fn blocks(blocks: impl IntoIterator<Item = Value>) -> Self {
        Self {
            blocks: Some(blocks.into_iter().collect()),
            html: None,
            markdown: None,
            media: None,
            is_rtl: None,
            skip_entity_detection: None,
        }
    }

    /// Sets media referenced by rich-message links.
    pub fn media(mut self, media: impl IntoIterator<Item = InputRichMessageMedia>) -> Self {
        self.media = Some(media.into_iter().collect());
        self
    }

    /// Sets right-to-left display.
    pub const fn rtl(mut self, is_rtl: bool) -> Self {
        self.is_rtl = Some(is_rtl);
        self
    }

    /// Controls automatic entity detection.
    pub const fn skip_entity_detection(mut self, skip: bool) -> Self {
        self.skip_entity_detection = Some(skip);
        self
    }

    fn files(&self) -> impl Iterator<Item = &InputFile> {
        self.media.iter().flatten().flat_map(InputRichMessageMedia::files)
    }

    fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        self.media.iter_mut().flatten().flat_map(InputRichMessageMedia::files_mut)
    }
}

impl InputFileLike for InputRichMessage {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        self.files().for_each(|file| file.copy_into(into));
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.files_mut().for_each(|file| file.move_into(into));
    }
}

/// A media element embedded in an outgoing rich message.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct InputRichMessageMedia {
    /// Identifier used by a `tg://photo`, `tg://video`, or `tg://audio` link.
    pub id: String,
    /// The referenced media.
    pub media: InputRichMessageMediaContent,
}

impl InputRichMessageMedia {
    pub fn new(id: impl Into<String>, media: InputRichMessageMediaContent) -> Self {
        Self { id: id.into(), media }
    }

    fn files(&self) -> impl Iterator<Item = &InputFile> {
        self.media.files()
    }

    fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        self.media.files_mut()
    }
}

/// Media kinds supported inside rich messages.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputRichMessageMediaContent {
    Animation(InputMediaAnimation),
    Audio(InputMediaAudio),
    Photo(InputMediaPhoto),
    Video(InputMediaVideo),
    VoiceNote(InputMediaVoiceNote),
}

impl InputRichMessageMediaContent {
    fn files(&self) -> impl Iterator<Item = &InputFile> {
        let mut files = Vec::new();
        match self {
            Self::Photo(media) => files.push(&media.media),
            Self::Audio(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::Animation(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::Video(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
                files.extend(media.cover.iter());
            }
            Self::VoiceNote(media) => files.push(&media.media),
        }
        files.into_iter()
    }

    fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        let mut files = Vec::new();
        match self {
            Self::Photo(media) => files.push(&mut media.media),
            Self::Audio(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::Animation(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::Video(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
                files.extend(media.cover.iter_mut());
            }
            Self::VoiceNote(media) => files.push(&mut media.media),
        }
        files.into_iter()
    }
}

/// A voice-note media element used in a rich message.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct InputMediaVoiceNote {
    pub media: InputFile,
    pub caption: Option<String>,
    pub parse_mode: Option<ParseMode>,
    pub caption_entities: Option<Vec<MessageEntity>>,
    pub duration: Option<u16>,
}

impl InputMediaVoiceNote {
    pub const fn new(media: InputFile) -> Self {
        Self { media, caption: None, parse_mode: None, caption_entities: None, duration: None }
    }

    pub fn caption(mut self, caption: impl Into<String>) -> Self {
        self.caption = Some(caption.into());
        self
    }

    pub const fn parse_mode(mut self, parse_mode: ParseMode) -> Self {
        self.parse_mode = Some(parse_mode);
        self
    }

    pub fn caption_entities(mut self, entities: impl IntoIterator<Item = MessageEntity>) -> Self {
        self.caption_entities = Some(entities.into_iter().collect());
        self
    }

    pub const fn duration(mut self, duration: u16) -> Self {
        self.duration = Some(duration);
        self
    }
}

/// Result of a chat join request query.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ChatJoinRequestQueryResult {
    Approve,
    Decline,
    Queue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileId;

    #[test]
    fn rich_html_serializes_without_unused_variants() {
        let value = serde_json::to_value(
            InputRichMessage::html("<b>hello</b>").rtl(true).skip_entity_detection(true),
        )
        .unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "html": "<b>hello</b>",
                "is_rtl": true,
                "skip_entity_detection": true
            })
        );
    }

    #[test]
    fn voice_note_media_uses_official_type_tag() {
        let media = InputRichMessageMedia::new(
            "voice",
            InputRichMessageMediaContent::VoiceNote(InputMediaVoiceNote::new(InputFile::file_id(
                FileId("voice-file".to_owned()),
            ))),
        );
        let value = serde_json::to_value(media).unwrap();
        assert_eq!(value["id"], "voice");
        assert_eq!(value["media"]["type"], "voice_note");
        assert_eq!(value["media"]["media"], "voice-file");
    }
}
