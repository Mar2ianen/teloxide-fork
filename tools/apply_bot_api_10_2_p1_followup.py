from pathlib import Path
import json
import re

root = Path.cwd()

# The master-side inline payload import won the merge conflict; retain both fixes.
p = root / "crates/teloxide-core/src/payloads/edit_message_text_inline.rs"
s = p.read_text()
s = s.replace(
    "BusinessConnectionId, InlineKeyboardMarkup, LinkPreviewOptions, MessageEntity, ParseMode, True,",
    "BusinessConnectionId, InlineKeyboardMarkup, InputRichMessage, LinkPreviewOptions, MessageEntity, ParseMode, True,",
)
p.write_text(s)

# Multipart traversal is structural, so Option must delegate for every InputFileLike value.
p = root / "crates/teloxide-core/src/types/input_file.rs"
s = p.read_text()
s = s.replace(
'''impl InputFileLike for Option<InputFile> {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        if let Some(this) = self {
            this.copy_into(into)
        }
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        if let Some(this) = self {
            this.move_into(into)
        }
    }
}
''',
'''impl<T> InputFileLike for Option<T>
where
    T: InputFileLike,
{
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        if let Some(this) = self {
            this.copy_into(into)
        }
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        if let Some(this) = self {
            this.move_into(into)
        }
    }
}
''')
p.write_text(s)

# Keep in-crate test literals exhaustive after extending public wire types.
for p in (root / "crates").rglob("*.rs"):
    s = p.read_text()
    s = re.sub(
        r'(?m)^(\s*)supports_guest_queries: ([^\n]+),\n(?!\1supports_join_request_queries:)',
        r'\1supports_guest_queries: \2,\n\1supports_join_request_queries: false,',
        s,
    )
    s = re.sub(
        r'(?m)^(\s*)sender_business_bot: ([^\n]+),\n(?!\1receiver_user:)',
        r'\1sender_business_bot: \2,\n\1receiver_user: None,\n\1ephemeral_message_id: None,',
        s,
    )
    s = re.sub(
        r'(?m)^(\s*)pinned_message: None,\n(?!\1guard_bot:)',
        r'\1pinned_message: None,\n\1guard_bot: None,',
        s,
    )
    p.write_text(s)

# Rich messages have neither captions nor media spoilers.
p = root / "crates/teloxide-core/src/types/message.rs"
s = p.read_text()
s = s.replace(
    "| MediaKind::Location(_)\n                    | MediaKind::Poll(_)",
    "| MediaKind::Location(_)\n                    | MediaKind::RichMessage(_)\n                    | MediaKind::Poll(_)",
)
p.write_text(s)

# ReplyParameters.message_id is optional in Bot API 10.2 when
# ephemeral_message_id is supplied; keep the external schema aligned.
p = root / "crates/teloxide-core/custom_v2.json"
data = json.loads(p.read_text())
for obj in data["objects"]:
    if obj.get("name") != "ReplyParameters":
        continue
    properties = obj.setdefault("properties", [])
    for field in properties:
        if field.get("name") == "message_id":
            field["required"] = False
    if not any(field.get("name") == "ephemeral_message_id" for field in properties):
        properties.insert(
            1,
            {
                "name": "ephemeral_message_id",
                "description": "Identifier of an ephemeral message that will be replied to",
                "required": False,
                "type_info": {"type": "integer", "enumeration": []},
            },
        )
p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
