//! Scope classification helpers used by the generated [`OutboundPayload`]
//! impls.
//!
//! These functions are the single source of truth for how a payload maps
//! to an [`OutboundScope`]. They are `pub(crate)`: the generated payload
//! code calls them; the public classification surface is the
//! [`OutboundPayload`] trait.

use crate::{
    outbound::{OutboundChatKey, OutboundScope},
    types::{ChatId, Recipient, TargetMessage, UserId},
};

/// Scope of a `Recipient` payload field (`chat_id`): numeric ids keep
/// their signed id, channel usernames become a textual canonical identity.
pub(crate) fn scope_of_recipient(recipient: &Recipient) -> OutboundScope {
    match recipient {
        Recipient::Id(chat_id) => OutboundScope::Chat(OutboundChatKey::id(chat_id.0)),
        Recipient::ChannelUsername(username) => {
            OutboundScope::Chat(OutboundChatKey::username(username))
        }
    }
}

/// Scope of a numeric `ChatId` payload field.
pub(crate) fn chat_id_scope(chat_id: ChatId) -> OutboundScope {
    OutboundScope::Chat(OutboundChatKey::id(chat_id.0))
}

/// Scope of a numeric `UserId` payload field (`user_id`, `sender_chat_id`,
/// ...). Users get their own per-id scope, so per-user windows and
/// penalties apply to exactly one user.
pub(crate) fn user_id_scope(user_id: UserId) -> OutboundScope {
    OutboundScope::Chat(OutboundChatKey::id(user_id.0 as i64))
}

/// Scope of the draft-payload `chat_id: UserId` field (`send_message_draft`
/// and siblings address the draft owner, not a chat).
pub(crate) fn draft_chat_id_scope(chat_id: &UserId) -> OutboundScope {
    user_id_scope(*chat_id)
}

/// Scope of a game target: a common chat addresses the chat, an inline
/// message has no chat identity and falls back to global.
pub(crate) fn target_message_scope(target: &TargetMessage) -> OutboundScope {
    match target {
        TargetMessage::Common { chat_id, .. } => scope_of_recipient(chat_id),
        TargetMessage::Inline { .. } => OutboundScope::Global,
    }
}

/// Scope of `set_game_score.chat_id` (the schema models it as a plain
/// `u32`).
pub(crate) fn game_chat_id_scope(chat_id: u32) -> OutboundScope {
    OutboundScope::Chat(OutboundChatKey::id(chat_id as i64))
}

/// Scope of `transfer_gift.new_owner_chat_id`.
pub(crate) fn new_owner_chat_id_scope(chat_id: &ChatId) -> OutboundScope {
    chat_id_scope(*chat_id)
}

/// Canonicalizes a channel username into the identity form: strips the
/// single optional leading `@` and lower-cases (Telegram usernames are
/// case-insensitive ASCII), so `@Foo`, `foo` and `@FOO` are one chat
/// identity.
pub(crate) fn canonical_username(username: &str) -> String {
    username.strip_prefix('@').unwrap_or(username).to_ascii_lowercase()
}
