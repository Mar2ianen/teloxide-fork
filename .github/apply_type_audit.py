from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    file = ROOT / path
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}")
    file.write_text(text.replace(old, new, 1))


# Preserve every user contained directly in these update kinds.
path = ROOT / "crates/teloxide-core/src/types/update.rs"
text = path.read_text()
text = replace_once.__globals__["Path"] if False else text
text = text.replace(
    """        let i5 = |x| R(R(x));
""",
    """        let i5 = |x| R(R(L(x)));
        let i6 = |x| R(R(R(x)));
""",
    1,
)
text = text.replace(
    """            UpdateKind::ChatJoinRequest(_)
            | UpdateKind::MessageReactionCount(_)
            | UpdateKind::BusinessConnection(_)
            | UpdateKind::ManagedBot(_)
            | UpdateKind::DeletedBusinessMessages(_)
            | UpdateKind::Error(_) => i5(empty()),
""",
    """            UpdateKind::ChatJoinRequest(request) => i1(once(&request.from)),
            UpdateKind::BusinessConnection(connection) => i1(once(&connection.user)),
            UpdateKind::ManagedBot(update) => i5([&update.user, &update.bot].into_iter()),

            UpdateKind::MessageReactionCount(_)
            | UpdateKind::DeletedBusinessMessages(_)
            | UpdateKind::Error(_) => i6(empty()),
""",
    1,
)
if "let i6" not in text:
    raise RuntimeError("failed to extend mentioned_users iterator tree")
update_tests = r'''

    #[test]
    fn mentioned_users_includes_direct_update_users() {
        let business = r#"{
            "update_id": 1,
            "business_connection": {
                "id": "business",
                "user": {"id": 10, "is_bot": false, "first_name": "owner"},
                "user_chat_id": 10,
                "date": 1,
                "is_enabled": true
            }
        }"#;
        let managed = r#"{
            "update_id": 2,
            "managed_bot": {
                "user": {"id": 20, "is_bot": false, "first_name": "owner"},
                "bot": {"id": 21, "is_bot": true, "first_name": "bot"}
            }
        }"#;
        let join = r#"{
            "update_id": 3,
            "chat_join_request": {
                "chat": {"id": -100, "title": "group", "type": "supergroup"},
                "from": {"id": 30, "is_bot": false, "first_name": "joiner"},
                "user_chat_id": 30,
                "date": 1
            }
        }"#;

        let ids = |json: &str| {
            serde_json::from_str::<Update>(json)
                .unwrap()
                .mentioned_users()
                .map(|user| user.id)
                .collect::<Vec<_>>()
        };

        assert_eq!(ids(business), [UserId(10)]);
        assert_eq!(ids(managed), [UserId(20), UserId(21)]);
        assert_eq!(ids(join), [UserId(30)]);
    }
'''
head, tail = text.rsplit("\n}", 1)
path.write_text(head + update_tests + "\n}" + tail)

# Reuse the existing request ID newtype for managed-bot keyboard requests.
replace_once(
    "crates/teloxide-core/src/types/managed_bot.rs",
    "use crate::types::{User, UserId};",
    "use crate::types::{RequestId, User, UserId};",
)
replace_once(
    "crates/teloxide-core/src/types/managed_bot.rs",
    "    pub request_id: i32,",
    "    pub request_id: RequestId,",
)
path = ROOT / "crates/teloxide-core/src/types/managed_bot.rs"
text = path.read_text()
managed_tests = r'''

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_bot_request_id_uses_newtype_without_changing_json() {
        let request = KeyboardButtonRequestManagedBot {
            request_id: RequestId(42),
            suggested_name: None,
            suggested_username: None,
        };
        let json = serde_json::to_value(request).unwrap();
        assert_eq!(json["request_id"], 42);
    }
}
'''
path.write_text(text + managed_tests)

# Builder diagnostics and must-use annotations.
replace_once(
    "crates/teloxide-core/src/types/keyboard_button.rs",
    """impl KeyboardButton {
    pub fn new<T>(text: T) -> Self
""",
    """impl KeyboardButton {
    #[must_use]
    pub fn new<T>(text: T) -> Self
""",
)
replace_once(
    "crates/teloxide-core/src/types/keyboard_button.rs",
    """    pub fn request<T>(mut self, val: T) -> Self
""",
    """    #[must_use]
    pub fn request<T>(mut self, val: T) -> Self
""",
)
replace_once(
    "crates/teloxide-core/src/types/keyboard_button.rs",
    "`request_contact`, `request_location`, `request_chat`, `request_user`, \\",
    "`request_contact`, `request_location`, `request_chat`, `request_users`, \\",
)
replace_once(
    "crates/teloxide-core/src/types/keyboard_button_request_chat.rs",
    """    /// Creates a new [`KeyboardButtonRequestChat`].
    pub fn new(request_id: RequestId, chat_is_channel: bool) -> Self {
""",
    """    /// Creates a new [`KeyboardButtonRequestChat`].
    #[must_use]
    pub fn new(request_id: RequestId, chat_is_channel: bool) -> Self {
""",
)
replace_once(
    "crates/teloxide-core/src/codegen/patch.rs",
    "// FIXME RETUNRS",
    "// FIXME: returns",
)

# Lock in the already-correct alias username behavior.
test_path = ROOT / "crates/teloxide/tests/bot_commands_alias.rs"
test_path.write_text(r'''#![cfg(feature = "macros")]

use teloxide::utils::command::{BotCommands, ParseError};

#[derive(BotCommands, Debug, PartialEq)]
#[command(rename_rule = "lowercase")]
enum Command {
    #[command(alias = "h")]
    Help,
}

#[test]
fn alias_for_another_bot_is_rejected() {
    let error = Command::parse("/h@other_bot", "this_bot").unwrap_err();
    assert_eq!(error, ParseError::WrongBotName("other_bot".to_owned()));
}
''')

subprocess.run(["git", "fetch", "origin", "next"], cwd=ROOT, check=True)
workflow = subprocess.run(
    ["git", "show", "origin/next:.github/workflows/ci.yml"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
).stdout
(ROOT / ".github/workflows/ci.yml").write_text(workflow)
Path(__file__).unlink()
