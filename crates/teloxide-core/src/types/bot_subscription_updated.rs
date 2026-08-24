use serde::{Deserialize, Serialize};

use crate::types::User;

/// Contains information about a change to a user's payment subscription.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct BotSubscriptionUpdated {
    /// User whose subscription changed.
    pub user: User,
    /// Bot-specified invoice payload.
    pub invoice_payload: String,
    /// The new subscription state.
    pub state: BotSubscriptionState,
}

/// A payment subscription state reported by Telegram.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BotSubscriptionState {
    /// The user canceled the subscription.
    Canceled,
    /// The user enabled or re-enabled the subscription.
    Active,
    /// A subscription payment failed.
    Failed,
}
