use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A rich message returned by Telegram.
///
/// Blocks intentionally use raw JSON until teloxide grows a typed rich-text
/// and rich-block AST. This preserves the wire response without preventing a
/// later source-compatible renderer layer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RichMessage {
    /// Rich-message blocks in Telegram wire format.
    #[serde(default)]
    pub blocks: Vec<Value>,
}
