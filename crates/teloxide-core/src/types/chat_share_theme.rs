use crate::types::{CustomEmojiId, UniqueGift, UniqueGiftBackdropColors};
use serde::{Deserialize, Serialize};

/// This object describes an emoji. Currently, it can be one of
/// EmojiTypeEmoji EmojiTypeCustomEmoji
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EmojiType {
    Emoji(EmojiTypeEmoji),
    CustomEmoji(EmojiTypeCustomEmoji),
}

/// A regular emoji.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct EmojiTypeEmoji {
    /// The emoji
    pub emoji: String,
}

/// A custom emoji.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct EmojiTypeCustomEmoji {
    /// Identifier of the custom emoji. Use getCustomEmojiStickers to get full
    /// information about the sticker.
    pub custom_emoji_id: CustomEmojiId,
}

/// This object describes the theme of the screen for sharing a link to a user
/// or a chat. Currently, it can be one of ChatShareThemeEmoji
/// ChatShareThemeUniqueGift
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatShareTheme {
    Emoji(ChatShareThemeEmoji),
    UniqueGift(Box<ChatShareThemeUniqueGift>),
}

/// The theme with emoji placed on a background.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ChatShareThemeEmoji {
    /// The emoji; 0-6 regular or custom emoji
    pub emojis: Vec<EmojiType>,
    /// Identifier of the backdrop used as the background; omitted if the
    /// default background is used. Use getChatShareThemeBackdrops to get
    /// details about the backdrop.
    pub background_backdrop_id: Option<i64>,
}

/// The theme based on a unique gift owned by the user or the chat.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ChatShareThemeUniqueGift {
    /// The gift
    pub gift: UniqueGift,
}

/// This object contains a list of backdrops, which can be used as the
/// background of a theme of the screen for sharing a link to a user or a chat.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ChatShareThemeBackdrops {
    /// The list of backdrops
    pub backdrops: Vec<ChatShareThemeBackdrop>,
}

/// This object describes a backdrop, which can be used as the background of a
/// theme of the screen for sharing a link to a user or a chat.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ChatShareThemeBackdrop {
    /// Unique identifier of the backdrop
    pub id: i64,
    /// Name of the backdrop
    pub name: String,
    /// Colors of the backdrop
    pub colors: UniqueGiftBackdropColors,
}
