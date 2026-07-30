from pathlib import Path
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
