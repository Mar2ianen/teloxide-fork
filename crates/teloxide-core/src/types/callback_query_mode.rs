use serde::{Deserialize, Serialize};

/// Controls how callback queries from the bot's own messages are represented.
///
/// Used by [`SetMySettings`](crate::payloads::SetMySettings).
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[cfg_attr(test, schemars(with = "String"))]
#[serde(rename_all = "lowercase")]
pub enum CallbackQueryMode {
    /// Always populate `message`, using an inaccessible message when needed.
    Default,
    /// Prefer `inline_message_id` when the message is inaccessible.
    Auto,
    /// Always populate `inline_message_id` instead of `message`.
    Inline,
}
