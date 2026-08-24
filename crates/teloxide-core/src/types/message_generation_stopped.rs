use crate::types::{Chat, ThreadId};
use serde::{Deserialize, Serialize};

/// An update about a user stopping message generation.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct MessageGenerationStopped {
    pub chat: Chat,
    pub message_thread_id: Option<ThreadId>,
    pub draft_id: i32,
}
