use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::types::{Animation, Audio, Location, PhotoSize, User, Video, Voice};

/// A rich-formatted message returned by Telegram.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichMessage {
    pub blocks: Vec<RichBlock>,
    pub is_rtl: Option<bool>,
}

/// Rich-formatted inline text.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum RichText {
    Text(String),
    List(Vec<Self>),
    Object(RichTextObject),
}

impl From<String> for RichText {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for RichText {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<Vec<Self>> for RichText {
    fn from(value: Vec<Self>) -> Self {
        Self::List(value)
    }
}

/// A typed rich-text object.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RichTextObject {
    Bold(RichTextBold),
    Italic(RichTextItalic),
    Underline(RichTextUnderline),
    Strikethrough(RichTextStrikethrough),
    Spoiler(RichTextSpoiler),
    DateTime(RichTextDateTime),
    TextMention(RichTextTextMention),
    Subscript(RichTextSubscript),
    Superscript(RichTextSuperscript),
    Marked(RichTextMarked),
    Code(RichTextCode),
    CustomEmoji(RichTextCustomEmoji),
    MathematicalExpression(RichTextMathematicalExpression),
    Url(RichTextUrl),
    EmailAddress(RichTextEmailAddress),
    PhoneNumber(RichTextPhoneNumber),
    BankCardNumber(RichTextBankCardNumber),
    Mention(RichTextMention),
    Hashtag(RichTextHashtag),
    Cashtag(RichTextCashtag),
    BotCommand(RichTextBotCommand),
    Anchor(RichTextAnchor),
    AnchorLink(RichTextAnchorLink),
    Reference(RichTextReference),
    ReferenceLink(RichTextReferenceLink),
}

macro_rules! rich_text_from {
    ($type:ident, $variant:ident) => {
        impl From<$type> for RichText {
            fn from(value: $type) -> Self {
                Self::Object(RichTextObject::$variant(value))
            }
        }
    };
}

macro_rules! rich_text_wrapper {
    ($($type:ident => $variant:ident),+ $(,)?) => {
        $(
            #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
            #[cfg_attr(test, derive(schemars::JsonSchema))]
            pub struct $type {
                pub text: Box<RichText>,
            }

            impl $type {
                pub fn new(text: impl Into<RichText>) -> Self {
                    Self { text: Box::new(text.into()) }
                }
            }

            rich_text_from!($type, $variant);
        )+
    };
}

rich_text_wrapper! {
    RichTextBold => Bold,
    RichTextItalic => Italic,
    RichTextUnderline => Underline,
    RichTextStrikethrough => Strikethrough,
    RichTextSpoiler => Spoiler,
    RichTextSubscript => Subscript,
    RichTextSuperscript => Superscript,
    RichTextMarked => Marked,
    RichTextCode => Code,
}

macro_rules! rich_text_link {
    ($type:ident, $variant:ident, $field:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[cfg_attr(test, derive(schemars::JsonSchema))]
        pub struct $type {
            pub text: Box<RichText>,
            pub $field: String,
        }

        rich_text_from!($type, $variant);
    };
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichTextDateTime {
    pub text: Box<RichText>,
    pub unix_time: i64,
    pub date_time_format: String,
}
rich_text_from!(RichTextDateTime, DateTime);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichTextTextMention {
    pub text: Box<RichText>,
    pub user: User,
}
rich_text_from!(RichTextTextMention, TextMention);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichTextCustomEmoji {
    pub custom_emoji_id: String,
    pub alternative_text: String,
}
rich_text_from!(RichTextCustomEmoji, CustomEmoji);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichTextMathematicalExpression {
    pub expression: String,
}
rich_text_from!(RichTextMathematicalExpression, MathematicalExpression);

rich_text_link!(RichTextUrl, Url, url);
rich_text_link!(RichTextEmailAddress, EmailAddress, email_address);
rich_text_link!(RichTextPhoneNumber, PhoneNumber, phone_number);
rich_text_link!(RichTextBankCardNumber, BankCardNumber, bank_card_number);
rich_text_link!(RichTextMention, Mention, username);
rich_text_link!(RichTextHashtag, Hashtag, hashtag);
rich_text_link!(RichTextCashtag, Cashtag, cashtag);
rich_text_link!(RichTextBotCommand, BotCommand, bot_command);
rich_text_link!(RichTextAnchorLink, AnchorLink, anchor_name);
rich_text_link!(RichTextReference, Reference, name);
rich_text_link!(RichTextReferenceLink, ReferenceLink, reference_name);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichTextAnchor {
    pub name: String,
}
rich_text_from!(RichTextAnchor, Anchor);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockCaption {
    pub text: RichText,
    pub credit: Option<RichText>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockTableCell {
    pub text: Option<RichText>,
    pub is_header: Option<bool>,
    pub colspan: Option<u32>,
    pub rowspan: Option<u32>,
    pub align: String,
    pub valign: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockListItem {
    pub label: String,
    pub blocks: Vec<RichBlock>,
    pub has_checkbox: Option<bool>,
    pub is_checked: Option<bool>,
    pub value: Option<i64>,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
}

/// A rich block returned by Telegram.
///
/// Unknown block variants are preserved instead of failing deserialization of
/// the containing update.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub enum RichBlock {
    /// A block whose type is supported by this version of teloxide.
    Known(Box<RichBlockKind>),
    /// A future Telegram block type preserved as its original JSON value.
    Unknown(Value),
}

impl<'de> Deserialize<'de> for RichBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let type_name = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| de::Error::missing_field("type"))?;

        match type_name {
            "paragraph"
            | "heading"
            | "pre"
            | "footer"
            | "divider"
            | "mathematical_expression"
            | "anchor"
            | "list"
            | "blockquote"
            | "pullquote"
            | "collage"
            | "slideshow"
            | "table"
            | "details"
            | "map"
            | "animation"
            | "audio"
            | "photo"
            | "video"
            | "voice_note"
            | "thinking" => {
                serde_json::from_value(value).map(Self::Known).map_err(de::Error::custom)
            }
            _ => Ok(Self::Unknown(value)),
        }
    }
}

impl From<RichBlockKind> for RichBlock {
    fn from(value: RichBlockKind) -> Self {
        Self::Known(Box::new(value))
    }
}

/// The currently known rich-block variants.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RichBlockKind {
    Paragraph(RichBlockParagraph),
    Heading(RichBlockSectionHeading),
    Pre(RichBlockPreformatted),
    Footer(RichBlockFooter),
    Divider(RichBlockDivider),
    MathematicalExpression(RichBlockMathematicalExpression),
    Anchor(RichBlockAnchor),
    List(RichBlockList),
    Blockquote(RichBlockBlockQuotation),
    Pullquote(RichBlockPullQuotation),
    Collage(RichBlockCollage),
    Slideshow(RichBlockSlideshow),
    Table(RichBlockTable),
    Details(RichBlockDetails),
    Map(RichBlockMap),
    Animation(RichBlockAnimation),
    Audio(RichBlockAudio),
    Photo(RichBlockPhoto),
    Video(RichBlockVideo),
    VoiceNote(RichBlockVoiceNote),
    Thinking(RichBlockThinking),
}

macro_rules! rich_text_block {
    ($($type:ident),+ $(,)?) => {
        $(
            #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
            #[cfg_attr(test, derive(schemars::JsonSchema))]
            pub struct $type {
                pub text: RichText,
            }
        )+
    };
}

rich_text_block!(RichBlockParagraph, RichBlockFooter, RichBlockThinking);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockSectionHeading {
    pub text: RichText,
    pub size: u8,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockPreformatted {
    pub text: RichText,
    pub language: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockDivider {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockMathematicalExpression {
    pub expression: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockAnchor {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockList {
    pub items: Vec<RichBlockListItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockBlockQuotation {
    pub blocks: Vec<RichBlock>,
    pub credit: Option<RichText>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockPullQuotation {
    pub text: RichText,
    pub credit: Option<RichText>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockCollage {
    pub blocks: Vec<RichBlock>,
    pub caption: Option<RichBlockCaption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockSlideshow {
    pub blocks: Vec<RichBlock>,
    pub caption: Option<RichBlockCaption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockTable {
    pub cells: Vec<Vec<RichBlockTableCell>>,
    pub is_bordered: Option<bool>,
    pub is_striped: Option<bool>,
    pub caption: Option<RichText>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockDetails {
    pub summary: RichText,
    pub blocks: Vec<RichBlock>,
    pub is_open: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockMap {
    pub location: Location,
    pub zoom: u8,
    pub width: u32,
    pub height: u32,
    pub caption: Option<RichBlockCaption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockAnimation {
    pub animation: Animation,
    pub has_spoiler: Option<bool>,
    pub caption: Option<RichBlockCaption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockAudio {
    pub audio: Audio,
    pub caption: Option<RichBlockCaption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockPhoto {
    pub photo: Vec<PhotoSize>,
    pub has_spoiler: Option<bool>,
    pub caption: Option<RichBlockCaption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockVideo {
    pub video: Video,
    pub has_spoiler: Option<bool>,
    pub caption: Option<RichBlockCaption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichBlockVoiceNote {
    pub voice_note: Voice,
    pub caption: Option<RichBlockCaption>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_block_is_preserved() {
        let value = serde_json::json!({"type": "future_block", "x": 1});
        let block: RichBlock = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(block, RichBlock::Unknown(value));
    }

    #[test]
    fn malformed_known_block_is_rejected() {
        let result = serde_json::from_value::<RichBlock>(serde_json::json!({
            "type": "paragraph"
        }));
        assert!(result.is_err());
    }

    #[test]
    fn known_block_is_typed() {
        let block: RichBlock = serde_json::from_value(serde_json::json!({
            "type": "paragraph",
            "text": "hello"
        }))
        .unwrap();
        assert!(matches!(block, RichBlock::Known(_)));
    }
}
