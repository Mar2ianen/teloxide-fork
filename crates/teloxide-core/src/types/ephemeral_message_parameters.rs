use crate::types::UserId;
use serde::{Deserialize, Serialize};

/// Parameters for sending an ephemeral message to a user.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct EphemeralMessageParameters {
    /// Identifier of the user who will receive the message.
    pub receiver_user_id: Option<UserId>,
    /// Identifier of the callback query which triggered the message, if any.
    pub callback_query_id: Option<String>,
    /// Shows the ephemeral message in place of the original callback message.
    pub replace_callback_query_message: Option<bool>,
}

impl EphemeralMessageParameters {
    /// Replies only to the user who pressed the button. The chat ID is ignored.
    /// The callback query must still be answered with `answer_callback_query`.
    pub fn reply(callback_query_id: impl Into<String>) -> Self {
        Self {
            receiver_user_id: None,
            callback_query_id: Some(callback_query_id.into()),
            replace_callback_query_message: None,
        }
    }

    pub const fn new(receiver_user_id: UserId) -> Self {
        Self {
            receiver_user_id: Some(receiver_user_id),
            callback_query_id: None,
            replace_callback_query_message: None,
        }
    }

    pub fn callback_query_id(mut self, callback_query_id: impl Into<String>) -> Self {
        self.callback_query_id = Some(callback_query_id.into());
        self
    }

    pub const fn replace_callback_query_message(mut self, replace: bool) -> Self {
        self.replace_callback_query_message = Some(replace);
        self
    }
}
