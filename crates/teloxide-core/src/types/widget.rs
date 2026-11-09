use crate::types::{Chat, ChatType, MessageEntity, MessageId, ParseMode, TonWalletCard, User};
use serde::{Deserialize, Serialize};

/// This object describes an HTML widget.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Widget {
    /// Unique identifier of the shared widget, which can be used to send the
    /// widget again using the method sendSharedWidget ; omitted if the widget
    /// isn't shared
    pub id: Option<String>,
    /// Width of the widget in logical pixels; omitted if the widget must occupy
    /// the whole width of the message bubble
    pub width: Option<i64>,
    /// Height of the widget in logical pixels; omitted if the height of the
    /// widget isn't fixed
    pub height: Option<i64>,
    /// HTML code of the widget
    pub html: String,
    /// Public data of the widget, which is available wherever the widget is
    /// shown. All messages with a shared widget have the same public data.
    pub public_data: Option<String>,
    /// Data of the widget, which is specific to the message
    pub chat_data: Option<String>,
}

/// This object describes the type of a query from a widget. Currently, it can
/// be one of WidgetQueryTypeData WidgetQueryTypeSignData
/// WidgetQueryTypeSentTransaction
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WidgetQueryType {
    Data(WidgetQueryTypeData),
    SignData(WidgetQueryTypeSignData),
    SentTransaction(WidgetQueryTypeSentTransaction),
}

/// The query contains data for the bot.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WidgetQueryTypeData {
    /// TON wallet card of the user who sent the query, if it was sent to the
    /// bot along with the query
    pub wallet_card: Option<TonWalletCard>,
}

/// The query contains data signed by a TON wallet of the user who sent the
/// query. The data is signed for the domain bot<bot_id> , where <bot_id> is the
/// identifier of the bot.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WidgetQueryTypeSignData {
    /// The wallet that signed the data
    pub wallet: WidgetQueryWallet,
    /// JSON-serialized signed data
    pub sign_data: String,
}

/// The query notifies the bot about a transaction sent from a TON wallet of the
/// user who sent the query.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WidgetQueryTypeSentTransaction {
    /// The wallet from which the transaction was sent
    pub wallet: WidgetQueryWallet,
    /// Value of msg_hash of the transaction
    pub msg_hash: String,
}

/// This object describes a TON wallet of the user, which is used in a query
/// from a widget.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WidgetQueryWallet {
    /// Address of the wallet
    pub address: String,
    /// Balance of the wallet, in the smallest units of the cryptocurrency, as a
    /// decimal number
    pub amount: String,
    /// Hex-encoded public key of the wallet; omitted if unknown
    pub public_key: Option<String>,
    /// Base64-encoded serialized StateInit of the wallet contract; omitted if
    /// and only if public_key is omitted
    pub state_init: Option<String>,
}

/// This object represents an incoming query from a widget in a message sent by
/// the bot or via the bot. Use answerWidgetQuery to answer the query. Each user
/// can send at most 5 queries per 5 seconds to the bot from all its widgets.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct WidgetQuery {
    /// Unique identifier for this query
    pub id: String,
    /// Sender
    pub from: User,
    /// Chat with the message containing the widget; omitted if the widget is in
    /// an inline message
    pub chat: Option<Chat>,
    /// Identifier of the message containing the widget; omitted if the widget
    /// is in an inline message
    #[serde(default, with = "crate::types::option_msg_id_as_int")]
    #[cfg_attr(test, schemars(with = "Option<i32>"))]
    pub message_id: Option<MessageId>,
    /// Identifier of the inline message containing the widget
    pub inline_message_id: Option<String>,
    /// Global identifier, uniquely corresponding to the chat to which the
    /// message with the widget was sent
    pub chat_instance: String,
    /// For widgets in inline messages, type of the chat to which the message
    /// was sent. Can be either “sender” for a private chat with the inline
    /// query sender, “private”, “group”, “supergroup”, or “channel”.
    pub chat_type: Option<ChatType>,
    /// Type of the query
    pub r#type: WidgetQueryType,
    /// Data of the query, 1-64 bytes
    pub data: String,
    /// Unique identifier of the shared widget, which sent the query; omitted if
    /// the widget isn't shared
    pub widget_id: Option<String>,
}

/// This object represents an incoming query from a forwarded message with a
/// shared widget created by the bot. Use answerWidgetQuery to answer the query.
/// Each user can send at most 5 queries per 5 seconds to the bot from all its
/// widgets.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct SharedWidgetQuery {
    /// Unique identifier for this query
    pub id: String,
    /// Sender
    pub from: User,
    /// Type of the query
    pub r#type: WidgetQueryType,
    /// Data of the query, 1-64 bytes
    pub data: String,
    /// Unique identifier of the shared widget, which sent the query
    pub widget_id: String,
}

/// Represents the content of a message with an HTML widget to be sent as the
/// result of an inline query. Currently, widgets can't be sent in response to
/// inline queries in the production environment.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct InputWidgetMessageContent {
    /// HTML code of the widget, 1-32768 bytes. The code must not exceed 4096
    /// bytes when compressed by zlib with the maximum compression level.
    pub html: String,
    /// Width of the widget in logical pixels; omit if the widget must occupy
    /// the whole width of the message bubble. The width is clamped by the app
    /// to the range from 200 to the maximum width of the message bubble.
    pub width: Option<i64>,
    /// Height of the widget in logical pixels; omit if the height of the widget
    /// isn't fixed. The height is clamped by the app to the range 50-600.
    pub height: Option<i64>,
    /// Public data of the widget, 0-1024 bytes, which will be available
    /// wherever the widget is shown. If the widget is shared, then all messages
    /// with it will have the same public data.
    pub public_data: Option<String>,
    /// Data of the widget, which is specific to the message, 0-1024 bytes
    pub chat_data: Option<String>,
    /// Widget caption, 0-1024 characters after entities parsing
    pub caption: Option<String>,
    /// Mode for parsing entities in the widget caption. See formatting options
    /// for more details.
    pub parse_mode: Option<ParseMode>,
    /// List of special entities that appear in the caption, which can be
    /// specified instead of parse_mode
    pub caption_entities: Option<Vec<MessageEntity>>,
    /// Pass True if the caption must be shown above the widget
    pub show_caption_above_media: Option<bool>,
    /// Pass True to allow the widget to send queries to the bot
    pub allow_queries: Option<bool>,
    /// Pass True to create a shared widget. Each message sent from the result
    /// creates a separate shared widget, whose identifier is received by the
    /// bot only in the field widget_id of queries from the widget. To send an
    /// existing shared widget in response to an inline query, use
    /// InputSharedWidgetMessageContent instead.
    pub is_shared: Option<bool>,
}

impl InputWidgetMessageContent {
    /// Sets `width`.
    #[must_use]
    pub fn width(mut self, value: i64) -> Self {
        self.width = Some(value);
        self
    }
    /// Sets `height`.
    #[must_use]
    pub fn height(mut self, value: i64) -> Self {
        self.height = Some(value);
        self
    }
    /// Sets `public_data`.
    #[must_use]
    pub fn public_data(mut self, value: impl Into<String>) -> Self {
        self.public_data = Some(value.into());
        self
    }
    /// Sets `chat_data`.
    #[must_use]
    pub fn chat_data(mut self, value: impl Into<String>) -> Self {
        self.chat_data = Some(value.into());
        self
    }
    /// Sets `caption`.
    #[must_use]
    pub fn caption(mut self, value: impl Into<String>) -> Self {
        self.caption = Some(value.into());
        self
    }
    /// Sets `parse_mode`.
    #[must_use]
    pub fn parse_mode(mut self, value: ParseMode) -> Self {
        self.parse_mode = Some(value);
        self
    }
    /// Sets `caption_entities`.
    #[must_use]
    pub fn caption_entities(mut self, value: Vec<MessageEntity>) -> Self {
        self.caption_entities = Some(value);
        self
    }
    /// Sets `show_caption_above_media`.
    #[must_use]
    pub fn show_caption_above_media(mut self, value: bool) -> Self {
        self.show_caption_above_media = Some(value);
        self
    }
    /// Sets `allow_queries`.
    #[must_use]
    pub fn allow_queries(mut self, value: bool) -> Self {
        self.allow_queries = Some(value);
        self
    }
    /// Sets `is_shared`.
    #[must_use]
    pub fn is_shared(mut self, value: bool) -> Self {
        self.is_shared = Some(value);
        self
    }

    pub fn new(html: impl Into<String>) -> Self {
        Self {
            html: html.into(),
            width: None,
            height: None,
            public_data: None,
            chat_data: None,
            caption: None,
            parse_mode: None,
            caption_entities: None,
            show_caption_above_media: None,
            allow_queries: None,
            is_shared: None,
        }
    }
}

/// Represents the content of a message with a shared widget to be sent as the
/// result of an inline query. Currently, widgets can't be sent in response to
/// inline queries in the production environment.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct InputSharedWidgetMessageContent {
    /// Unique identifier of the shared widget created by the bot
    pub widget_id: String,
    /// Data of the widget, which is specific to the message, 0-1024 bytes
    pub chat_data: Option<String>,
    /// Widget caption, 0-1024 characters after entities parsing
    pub caption: Option<String>,
    /// Mode for parsing entities in the widget caption. See formatting options
    /// for more details.
    pub parse_mode: Option<ParseMode>,
    /// List of special entities that appear in the caption, which can be
    /// specified instead of parse_mode
    pub caption_entities: Option<Vec<MessageEntity>>,
    /// Pass True if the caption must be shown above the widget
    pub show_caption_above_media: Option<bool>,
}

impl InputSharedWidgetMessageContent {
    /// Sets `chat_data`.
    #[must_use]
    pub fn chat_data(mut self, value: impl Into<String>) -> Self {
        self.chat_data = Some(value.into());
        self
    }
    /// Sets `caption`.
    #[must_use]
    pub fn caption(mut self, value: impl Into<String>) -> Self {
        self.caption = Some(value.into());
        self
    }
    /// Sets `parse_mode`.
    #[must_use]
    pub fn parse_mode(mut self, value: ParseMode) -> Self {
        self.parse_mode = Some(value);
        self
    }
    /// Sets `caption_entities`.
    #[must_use]
    pub fn caption_entities(mut self, value: Vec<MessageEntity>) -> Self {
        self.caption_entities = Some(value);
        self
    }
    /// Sets `show_caption_above_media`.
    #[must_use]
    pub fn show_caption_above_media(mut self, value: bool) -> Self {
        self.show_caption_above_media = Some(value);
        self
    }

    pub fn new(widget_id: impl Into<String>) -> Self {
        Self {
            widget_id: widget_id.into(),
            chat_data: None,
            caption: None,
            parse_mode: None,
            caption_entities: None,
            show_caption_above_media: None,
        }
    }
}
