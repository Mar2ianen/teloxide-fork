use serde::{Deserialize, Serialize};

/// Describes a service message about the sender of the message using a link of
/// the other user, which allows sending messages to the user without paying
/// Telegram Stars. Currently holds no information.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct PaidMessagesWaived {}
