from pathlib import Path
import re


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if text.count(old) != 1:
        raise RuntimeError(f"expected exactly one match in {path}: {old[:80]!r}")
    file.write_text(text.replace(old, new, 1))


def fix_chat_id() -> None:
    path = Path("crates/teloxide-core/src/types/chat_id.rs")
    text = path.read_text()
    replacements = {
        "matches!(self.to_bare(), BareChatId::User(_))":
            "matches!(self.to_bare(), Some(BareChatId::User(_)))",
        "matches!(self.to_bare(), BareChatId::Group(_))":
            "matches!(self.to_bare(), Some(BareChatId::Group(_)))",
        "matches!(self.to_bare(), BareChatId::Channel(_))":
            "matches!(self.to_bare(), Some(BareChatId::Channel(_)))",
        "pub(crate) fn to_bare(self) -> BareChatId {":
            "pub(crate) fn to_bare(self) -> Option<BareChatId> {",
    }
    for old, new in replacements.items():
        if text.count(old) != 1:
            raise RuntimeError(f"ChatId pattern count mismatch: {old!r}")
        text = text.replace(old, new, 1)

    text = text.replace(
        """        match self.to_bare() {
            BareChatId::User(u) => Some(u),
            BareChatId::Group(_) | BareChatId::Channel(_) => None,
        }
""",
        """        match self.to_bare() {
            Some(BareChatId::User(u)) => Some(u),
            Some(BareChatId::Group(_) | BareChatId::Channel(_)) | None => None,
        }
""",
        1,
    )
    text = text.replace(
        """            id @ MIN_MARKED_CHAT_ID..=MAX_MARKED_CHAT_ID => Group(-id as _),
            id @ MIN_MARKED_CHANNEL_ID..=MAX_MARKED_CHANNEL_ID => {
                Channel((MAX_MARKED_CHANNEL_ID - id) as _)
            }
            id @ MIN_USER_ID..=MAX_USER_ID => User(UserId(id as _)),
            id => panic!("malformed chat id: {id}"),
""",
        """            id @ MIN_MARKED_CHAT_ID..=MAX_MARKED_CHAT_ID => Some(Group(-id as _)),
            id @ MIN_MARKED_CHANNEL_ID..=MAX_MARKED_CHANNEL_ID => {
                Some(Channel((MAX_MARKED_CHANNEL_ID - id) as _))
            }
            id @ MIN_USER_ID..=MAX_USER_ID => Some(User(UserId(id as _))),
            _ => None,
""",
        1,
    )
    text = text.replace(
        "assert!(matches!(ChatId(5298363099).to_bare(), BareChatId::User(UserId(5298363099))));",
        "assert!(matches!(ChatId(5298363099).to_bare(), Some(BareChatId::User(UserId(5298363099)))));",
        1,
    )
    text = text.replace(
        """            assert_eq!(User(UserId(x)), User(UserId(x)).to_bot_api().to_bare());
            assert_eq!(Group(x), Group(x).to_bot_api().to_bare());
            assert_eq!(Channel(x), Channel(x).to_bot_api().to_bare());
""",
        """            assert_eq!(Some(User(UserId(x))), User(UserId(x)).to_bot_api().to_bare());
            assert_eq!(Some(Group(x)), Group(x).to_bot_api().to_bare());
            assert_eq!(Some(Channel(x)), Channel(x).to_bot_api().to_bare());
""",
        1,
    )
    marker = "    #[test]\n    fn display() {\n"
    test = """    #[test]
    fn unknown_ranges_are_not_classified() {
        for chat_id in [ChatId(i64::MIN), ChatId(MAX_USER_ID + 1)] {
            assert_eq!(chat_id.to_bare(), None);
            assert!(!chat_id.is_user());
            assert!(!chat_id.is_group());
            assert!(!chat_id.is_channel_or_supergroup());
            assert_eq!(chat_id.as_user(), None);
        }
    }

"""
    if text.count(marker) != 1:
        raise RuntimeError("ChatId test marker mismatch")
    path.write_text(text.replace(marker, test + marker, 1))


def fix_sticker_flags() -> None:
    path = Path("crates/teloxide-core/src/types/sticker.rs")
    text = path.read_text()
    start = text.index("impl StickerFormatFlags {")
    end = text.index("\nimpl StickerFormat {", start)
    implementation = """impl StickerFormatFlags {
    /// Returns the sticker format when the two Telegram flags are consistent.
    #[must_use]
    pub fn try_format(&self) -> Option<StickerFormat> {
        match (self.is_animated, self.is_video) {
            (false, false) => Some(StickerFormat::Static),
            (true, false) => Some(StickerFormat::Animated),
            (false, true) => Some(StickerFormat::Video),
            (true, true) => None,
        }
    }

    /// Returns the sticker format without panicking on malformed Telegram data.
    ///
    /// If both flags are present, video takes precedence. Use [`Self::try_format`]
    /// when the distinction between valid and malformed flags matters.
    #[must_use]
    pub fn format(&self) -> StickerFormat {
        self.try_format().unwrap_or(StickerFormat::Video)
    }
}
"""
    text = text[:start] + implementation + text[end:]
    pattern = re.compile(
        r"    #\[test\]\n    #\[should_panic\]\n"
        r"    fn wrong_sticker_format_flags_serde\(\) \{.*?\n    \}\n",
        re.DOTALL,
    )
    replacement = """    #[test]
    fn inconsistent_sticker_format_flags_do_not_panic() {
        let json = r#"{"is_animated":true,"is_video":true}"#;
        let fmt_flags: StickerFormatFlags = serde_json::from_str(json).unwrap();

        assert_eq!(fmt_flags.try_format(), None);
        assert_eq!(fmt_flags.format(), StickerFormat::Video);
    }
"""
    text, count = pattern.subn(replacement, text, count=1)
    if count != 1:
        raise RuntimeError("sticker invalid-flags test not found")
    path.write_text(text)


def fix_update_kind() -> None:
    path = Path("crates/teloxide-core/src/types/update.rs")
    text = path.read_text()
    start = text.index("impl<'de> Deserialize<'de> for UpdateKind {")
    end = text.index("impl Serialize for UpdateKind {", start)
    implementation = r'''impl<'de> Deserialize<'de> for UpdateKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UpdateKind;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let Some(key) = map.next_key::<String>()? else {
                    return Ok(empty_error())
                };

                let this = match key.as_str() {
                    "message" => UpdateKind::Message(map.next_value()?),
                    "edited_message" => UpdateKind::EditedMessage(map.next_value()?),
                    "channel_post" => UpdateKind::ChannelPost(map.next_value()?),
                    "edited_channel_post" => UpdateKind::EditedChannelPost(map.next_value()?),
                    "guest_message" => UpdateKind::GuestMessage(map.next_value()?),
                    "business_connection" => UpdateKind::BusinessConnection(map.next_value()?),
                    "business_message" => UpdateKind::BusinessMessage(map.next_value()?),
                    "edited_business_message" => UpdateKind::EditedBusinessMessage(map.next_value()?),
                    "deleted_business_messages" => UpdateKind::DeletedBusinessMessages(map.next_value()?),
                    "managed_bot" => UpdateKind::ManagedBot(map.next_value()?),
                    "message_reaction" => UpdateKind::MessageReaction(map.next_value()?),
                    "message_reaction_count" => UpdateKind::MessageReactionCount(map.next_value()?),
                    "inline_query" => UpdateKind::InlineQuery(map.next_value()?),
                    "chosen_inline_result" => UpdateKind::ChosenInlineResult(map.next_value()?),
                    "callback_query" => UpdateKind::CallbackQuery(map.next_value()?),
                    "shipping_query" => UpdateKind::ShippingQuery(map.next_value()?),
                    "pre_checkout_query" => UpdateKind::PreCheckoutQuery(map.next_value()?),
                    "purchased_paid_media" => UpdateKind::PurchasedPaidMedia(map.next_value()?),
                    "poll" => UpdateKind::Poll(map.next_value()?),
                    "poll_answer" => UpdateKind::PollAnswer(map.next_value()?),
                    "my_chat_member" => UpdateKind::MyChatMember(map.next_value()?),
                    "chat_member" => UpdateKind::ChatMember(map.next_value()?),
                    "chat_join_request" => UpdateKind::ChatJoinRequest(map.next_value()?),
                    "chat_boost" => UpdateKind::ChatBoost(map.next_value()?),
                    "removed_chat_boost" => UpdateKind::RemovedChatBoost(map.next_value()?),
                    unknown => {
                        let mut raw = serde_json::Map::new();
                        raw.insert(unknown.to_owned(), map.next_value::<Value>()?);
                        while let Some((key, value)) = map.next_entry::<String, Value>()? {
                            raw.insert(key, value);
                        }
                        UpdateKind::Error(Value::Object(raw))
                    }
                };

                Ok(this)
            }
        }

        stacker::maybe_grow(256 * 1024, 1024 * 1024, || deserializer.deserialize_any(Visitor))
    }
}

'''
    text = text[:start] + implementation + text[end:]
    text = text.replace(
        """    /// **Note that deserialize implementation always returns an empty value**,
    /// teloxide fills in the data when doing deserialization.
""",
        """    /// The raw value is preserved for genuinely unknown update kinds. Malformed
    /// payloads of known update kinds are returned as deserialization errors.
""",
        1,
    )
    tests = r'''
    #[test]
    fn malformed_known_update_is_an_error() {
        let update = r#"{"update_id":1,"message":null}"#;
        assert!(serde_json::from_str::<Update>(update).is_err());
    }

    #[test]
    fn unknown_update_preserves_raw_value() {
        let update = r#"{"update_id":1,"future_update":{"answer":42}}"#;
        let update: Update = serde_json::from_str(update).unwrap();

        assert_eq!(
            update.kind,
            UpdateKind::Error(serde_json::json!({"future_update": {"answer": 42}}))
        );
    }
'''
    pos = text.rfind("\n}")
    if pos < 0:
        raise RuntimeError("Update test module closing brace not found")
    path.write_text(text[:pos] + tests + text[pos:])


def fix_input_file_url() -> None:
    replace_once(
        "crates/teloxide-core/src/serde_multipart/unserializers/input_file.rs",
        """            "Url" => Ok(InputFile::Url(
                reqwest::Url::parse(&value.serialize(StringUnserializer)?).unwrap(),
            )),
""",
        """            "Url" => {
                let value = value.serialize(StringUnserializer)?;
                reqwest::Url::parse(&value)
                    .map(InputFile::Url)
                    .map_err(|error| UnserializerError::Custom(error.to_string()))
            }
""",
    )
    replace_once(
        "crates/teloxide-core/src/serde_multipart/unserializers.rs",
        "use serde::Serialize;",
        "use serde::{Serialize, Serializer as _};",
    )
    replace_once(
        "crates/teloxide-core/src/serde_multipart/unserializers.rs",
        "    let value = InputFile::FileId(\"file_id\".into());\n",
        """    let invalid_url = InputFileUnserializer::NotMem
        .serialize_newtype_variant("InputFile", 0, "Url", &"not a valid URL")
        .unwrap_err();
    assert!(matches!(invalid_url, UnserializerError::Custom(_)));

    let value = InputFile::FileId("file_id".into());
""",
    )


def fix_multipart_error() -> None:
    path = Path("crates/teloxide-core/src/serde_multipart/error.rs")
    text = path.read_text()
    start = text.index("            // This should be ok")
    end = text.index("\n        }", start)
    replacement = """            error => RequestError::Io(Arc::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            ))),"""
    text = text[:start] + replacement + text[end:]
    text += """

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_serialization_error_does_not_panic() {
        let error: RequestError = Error::TopLevelNotStruct.into();
        assert!(matches!(error, RequestError::Io(_)));
    }
}
"""
    path.write_text(text)


Path(".github/robustness-fixer-error.txt").unlink(missing_ok=True)
fix_chat_id()
fix_sticker_flags()
fix_update_kind()
fix_input_file_url()
fix_multipart_error()
