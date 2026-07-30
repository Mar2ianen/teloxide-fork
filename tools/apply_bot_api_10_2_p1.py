from pathlib import Path
import json
W=Path.cwd()

def edit(rel, fn):
    p=W/rel
    s=p.read_text()
    ns=fn(s)
    if ns==s:
        print('NO CHANGE', rel)
    p.write_text(ns)

# Export new output rich-message types.
edit('crates/teloxide-core/src/types.rs', lambda s: s.replace('pub use reply_parameters::*;\n', 'pub use reply_parameters::*;\npub use rich_message::*;\n').replace('mod reply_parameters;\n', 'mod reply_parameters;\nmod rich_message;\n'))

(W/'crates/teloxide-core/src/types/rich_message.rs').write_text(r'''use serde::{Deserialize, Serialize};
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
''')

# Message: response fields and rich media kind.
def message(s):
    s=s.replace('    SuggestedPostPaid, SuggestedPostRefunded, TextQuote, ThreadId, True, UniqueGiftInfo, User,\n', '    RichMessage, SuggestedPostPaid, SuggestedPostRefunded, TextQuote, ThreadId, True,\n    UniqueGiftInfo, User,\n')
    needle='''    /// The bot that actually sent the message on behalf of the business
    /// account. Available only for outgoing messages sent on behalf of the
    /// connected business account.
    pub sender_business_bot: Option<User>,

    #[serde(flatten)]
'''
    repl='''    /// The bot that actually sent the message on behalf of the business
    /// account. Available only for outgoing messages sent on behalf of the
    /// connected business account.
    pub sender_business_bot: Option<User>,

    /// User that received an outgoing ephemeral message.
    pub receiver_user: Option<User>,

    /// Identifier of an outgoing ephemeral message.
    pub ephemeral_message_id: Option<i32>,

    #[serde(flatten)]
'''
    s=s.replace(needle,repl)
    s=s.replace('    LivePhoto(MediaLivePhoto),\n    Poll(MediaPoll),', '    LivePhoto(MediaLivePhoto),\n    RichMessage(MediaRichMessage),\n    Poll(MediaPoll),')
    insert='''
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct MediaRichMessage {
    /// Message contains rich content.
    pub rich_message: RichMessage,
}

'''
    s=s.replace('#[serde_with::skip_serializing_none]\n#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\n#[cfg_attr(test, derive(schemars::JsonSchema))]\npub struct MediaPoll {', insert+'#[serde_with::skip_serializing_none]\n#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\n#[cfg_attr(test, derive(schemars::JsonSchema))]\npub struct MediaPoll {')
    marker='''    #[test]
    fn show_caption_above_media() {'''
    test=r'''    #[test]
    fn rich_message_response_and_ephemeral_ids_deserialize() {
        let json = r#"{
            "message_id": 42,
            "date": 0,
            "chat": {"id": 1, "type": "private", "first_name": "receiver"},
            "receiver_user": {"id": 7, "is_bot": false, "first_name": "receiver"},
            "ephemeral_message_id": 99,
            "rich_message": {"blocks": []}
        }"#;

        let message: Message = from_str(json).unwrap();
        assert_eq!(message.ephemeral_message_id, Some(99));
        assert_eq!(message.receiver_user.as_ref().map(|user| user.id.0), Some(7));
        assert!(matches!(
            message.kind,
            MessageKind::Common(MessageCommon {
                media_kind: MediaKind::RichMessage(MediaRichMessage {
                    rich_message: RichMessage { ref blocks },
                }),
                ..
            }) if blocks.is_empty()
        ));
    }

'''
    s=s.replace(marker,test+marker)
    return s
edit('crates/teloxide-core/src/types/message.rs', message)

# ReplyParameters supports normal or ephemeral target.
def reply(s):
    s=s.replace('''    #[serde(with = "crate::types::msg_id_as_int")]
    #[cfg_attr(test, schemars(with = "i32"))]
    pub message_id: MessageId,
''','''    #[serde(default, with = "crate::types::option_msg_id_as_int")]
    #[cfg_attr(test, schemars(with = "Option<i32>"))]
    pub message_id: Option<MessageId>,
    /// Identifier of an ephemeral message that will be replied to.
    pub ephemeral_message_id: Option<i32>,
''')
    s=s.replace('Self { message_id, ..Self::default() }','Self { message_id: Some(message_id), ..Self::default() }')
    s=s.replace('''    /// Setter for the `chat_id` field
''','''    /// Creates reply parameters for an ephemeral message.
    pub fn ephemeral(ephemeral_message_id: i32) -> Self {
        Self { ephemeral_message_id: Some(ephemeral_message_id), ..Self::default() }
    }

    /// Setter for the `chat_id` field
''')
    s += r'''

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_exactly_one_reply_identifier() {
        assert_eq!(
            serde_json::to_value(ReplyParameters::new(MessageId(5))).unwrap(),
            serde_json::json!({"message_id": 5})
        );
        assert_eq!(
            serde_json::to_value(ReplyParameters::ephemeral(9)).unwrap(),
            serde_json::json!({"ephemeral_message_id": 9})
        );
    }
}
'''
    return s
edit('crates/teloxide-core/src/types/reply_parameters.rs', reply)

edit('crates/teloxide-core/src/types/chat_join_request.rs', lambda s: s.replace('''    /// Chat to which the request was sent
    pub chat: Chat,
''','''    /// Chat to which the request was sent
    pub chat: Chat,
    /// Identifier of the join-request query that can be passed to query methods.
    pub query_id: Option<String>,
'''))

edit('crates/teloxide-core/src/types/user.rs', lambda s: s.replace('''    pub supports_guest_queries: bool,

    /// `true`, if the bot has forum topic mode enabled in private chats.
''','''    pub supports_guest_queries: bool,

    /// `true`, if the bot supports chat join request queries. Returned only in `getMe`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub supports_join_request_queries: bool,

    /// `true`, if the bot has forum topic mode enabled in private chats.
'''))

edit('crates/teloxide-core/src/types/chat_full_info.rs', lambda s: s.replace('''    /// The most recent pinned message (by sending date).
    pub pinned_message: Option<Box<Message>>,
''','''    /// The most recent pinned message (by sending date).
    pub pinned_message: Option<Box<Message>>,

    /// Guard bot configured for the chat, if any.
    pub guard_bot: Option<User>,
'''))

def input_rich(s):
    s=s.replace('''    /// This is a temporary raw representation; a typed rich-block AST can
    /// replace it without changing the request methods.
''','''    /// This is a temporary raw representation; a typed rich-block AST can
    /// replace it without changing the request methods.
    ///
    /// Local uploads embedded directly in raw blocks are not traversed. Use
    /// file IDs/URLs in raw blocks, or put uploads in [`Self::media`].
''')
    s=s.replace('''    /// Creates a rich message from raw block JSON values.
''','''    /// Creates a rich message from raw block JSON values.
    ///
    /// Raw blocks currently support only remote/file-id media. `attach://`
    /// references created outside [`Self::media`] have no matching multipart
    /// part and are therefore unsupported.
''')
    return s
edit('crates/teloxide-core/src/types/input_rich_message.rs', input_rich)

def edit_payload(rel, inline=False):
    def fn(s):
        if not inline:
            s=s.replace('impl_payload! {\n', 'impl_payload! {\n    @[multipart = rich_message]\n',1)
        s=s.replace('''            /// New text of the message, 1-4096 characters after entities parsing
            pub text: String [into],
''','''            /// New text of the message, 1-4096 characters after entities parsing
            #[serde(skip_serializing_if = "String::is_empty")]
            pub text: String [into],
''')
        typ='EditMessageTextInline' if inline else 'EditMessageText'
        args=('inline_message_id: impl Into<String>' if inline else 'chat_id: impl Into<Recipient>, message_id: MessageId')
        call=('Self::new(inline_message_id, String::new())' if inline else 'Self::new(chat_id, message_id, String::new())')
        s += f'''\nimpl {typ} {{\n    /// Creates a rich-only edit request.\n    pub fn rich({args}, rich_message: InputRichMessage) -> Self {{\n        let mut payload = {call};\n        payload.rich_message = Some(rich_message);\n        payload\n    }}\n}}\n'''
        return s
    edit(rel,fn)
edit_payload('crates/teloxide-core/src/payloads/edit_message_text.rs',False)
edit_payload('crates/teloxide-core/src/payloads/edit_message_text_inline.rs',True)

def payload_codegen(s):
    s=s.replace('''    if m.names.2 == "send_rich_message" {
        fields.push("rich_message");
    }
''','''    if matches!(m.names.2.as_str(), "send_rich_message" | "edit_message_text") {
        fields.push("rich_message");
    }
''')
    old='''        files.push((path, reformat(add_preamble("codegen_payloads", contents))));
'''
    new='''        let contents = match method.names.2.as_str() {
            "edit_message_text" | "edit_message_text_inline" => {
                let contents = contents.replace(
                    "            pub text: String [into],",
                    "            #[serde(skip_serializing_if = \\\"String::is_empty\\\")]\\n            pub text: String [into],",
                );
                let constructor = if method.names.2 == "edit_message_text" {
                    r#"
impl EditMessageText {
    /// Creates a rich-only edit request.
    pub fn rich(
        chat_id: impl Into<Recipient>,
        message_id: MessageId,
        rich_message: InputRichMessage,
    ) -> Self {
        let mut payload = Self::new(chat_id, message_id, String::new());
        payload.rich_message = Some(rich_message);
        payload
    }
}
"#
                } else {
                    r#"
impl EditMessageTextInline {
    /// Creates a rich-only edit request.
    pub fn rich(
        inline_message_id: impl Into<String>,
        rich_message: InputRichMessage,
    ) -> Self {
        let mut payload = Self::new(inline_message_id, String::new());
        payload.rich_message = Some(rich_message);
        payload
    }
}
"#
                };
                format!("{contents}{constructor}")
            }
            _ => contents,
        };

        files.push((path, reformat(add_preamble("codegen_payloads", contents))));
'''
    s=s.replace(old,new)
    return s
edit('crates/teloxide-core/src/payloads/codegen.rs', payload_codegen)

def bot_api(s):
    s=s.replace('type EditMessageText = JsonRequest<payloads::EditMessageText>;','type EditMessageText = MultipartRequest<payloads::EditMessageText>;')
    s += r'''

impl Bot {
    /// Edits a regular chat message using rich content only.
    pub fn edit_message_rich_text<C>(
        &self,
        chat_id: C,
        message_id: MessageId,
        rich_message: InputRichMessage,
    ) -> MultipartRequest<payloads::EditMessageText>
    where
        C: Into<Recipient>,
    {
        MultipartRequest::new(
            self.clone(),
            payloads::EditMessageText::rich(chat_id, message_id, rich_message),
        )
    }

    /// Edits an inline message using rich content only.
    ///
    /// Telegram doesn't allow uploading new files while editing inline
    /// messages, so rich media must use existing file IDs or URLs.
    pub fn edit_message_rich_text_inline<I>(
        &self,
        inline_message_id: I,
        rich_message: InputRichMessage,
    ) -> JsonRequest<payloads::EditMessageTextInline>
    where
        I: Into<String>,
    {
        JsonRequest::new(
            self.clone(),
            payloads::EditMessageTextInline::rich(inline_message_id, rich_message),
        )
    }
}
'''
    return s
edit('crates/teloxide-core/src/bot/api.rs', bot_api)

def checker(s):
    s=s.replace('''    SiblingParam { param: String },
''','''    SiblingParam { param: String },
    Requiredness { method: String, param: String },
''')
    s=s.replace('''    fn is_sibling_param_exception(&self, param: String) -> bool {
        self.exceptions.contains(&Exception::SiblingParam { param })
    }
''','''    fn is_sibling_param_exception(&self, param: String) -> bool {
        self.exceptions.contains(&Exception::SiblingParam { param })
    }

    fn is_requiredness_exception(&self, method: String, param: String) -> bool {
        self.exceptions.contains(&Exception::Requiredness { method, param })
    }
''')
    s=s.replace('''    } else if !param.required && !ignore_optional {
''','''    } else if !param.required
        && !ignore_optional
        && !exceptions.is_requiredness_exception(method_name.clone(), param.name.clone())
    {
''')
    s=s.replace('''            Exception::MethodField {
                method: "getGameHighScores".to_owned(),
                param: "inline_message_id".to_owned(),
            },
''','''            Exception::MethodField {
                method: "getGameHighScores".to_owned(),
                param: "inline_message_id".to_owned(),
            },
            Exception::Requiredness {
                method: "editMessageText".to_owned(),
                param: "text".to_owned(),
            },
''')
    return s
edit('crates/teloxide-core/src/codegen/schema_check/ron_check.rs', checker)

p=W/'crates/teloxide-core/custom_v2.json'
data=json.loads(p.read_text())
for m in data['methods']:
    if m['name']=='editMessageText':
        for a in m['arguments']:
            if a['name']=='text': a['required']=False
p.write_text(json.dumps(data, ensure_ascii=False, indent=2)+'\n')

print('applied')
