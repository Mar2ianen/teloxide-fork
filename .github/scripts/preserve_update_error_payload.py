from pathlib import Path

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
                let value = map.next_value::<Value>()?;

                let mut raw = serde_json::Map::new();
                raw.insert(key.clone(), value.clone());
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
                    raw.insert(key, value);
                }

                macro_rules! decode {
                    ($ty:ty, $variant:path) => {
                        match serde_json::from_value::<$ty>(value.clone()) {
                            Ok(value) => $variant(value),
                            Err(_) => UpdateKind::Error(Value::Object(raw.clone())),
                        }
                    };
                }

                let this = match key.as_str() {
                    "message" => decode!(Message, UpdateKind::Message),
                    "edited_message" => decode!(Message, UpdateKind::EditedMessage),
                    "channel_post" => decode!(Message, UpdateKind::ChannelPost),
                    "edited_channel_post" => decode!(Message, UpdateKind::EditedChannelPost),
                    "guest_message" => decode!(Message, UpdateKind::GuestMessage),
                    "business_connection" => {
                        decode!(BusinessConnection, UpdateKind::BusinessConnection)
                    }
                    "business_message" => decode!(Message, UpdateKind::BusinessMessage),
                    "edited_business_message" => {
                        decode!(Message, UpdateKind::EditedBusinessMessage)
                    }
                    "deleted_business_messages" => decode!(
                        BusinessMessagesDeleted,
                        UpdateKind::DeletedBusinessMessages
                    ),
                    "managed_bot" => decode!(ManagedBotUpdated, UpdateKind::ManagedBot),
                    "message_reaction" => {
                        decode!(MessageReactionUpdated, UpdateKind::MessageReaction)
                    }
                    "message_reaction_count" => decode!(
                        MessageReactionCountUpdated,
                        UpdateKind::MessageReactionCount
                    ),
                    "inline_query" => decode!(InlineQuery, UpdateKind::InlineQuery),
                    "chosen_inline_result" => {
                        decode!(ChosenInlineResult, UpdateKind::ChosenInlineResult)
                    }
                    "callback_query" => decode!(CallbackQuery, UpdateKind::CallbackQuery),
                    "shipping_query" => decode!(ShippingQuery, UpdateKind::ShippingQuery),
                    "pre_checkout_query" => {
                        decode!(PreCheckoutQuery, UpdateKind::PreCheckoutQuery)
                    }
                    "purchased_paid_media" => {
                        decode!(PaidMediaPurchased, UpdateKind::PurchasedPaidMedia)
                    }
                    "poll" => decode!(Poll, UpdateKind::Poll),
                    "poll_answer" => decode!(PollAnswer, UpdateKind::PollAnswer),
                    "my_chat_member" => decode!(ChatMemberUpdated, UpdateKind::MyChatMember),
                    "chat_member" => decode!(ChatMemberUpdated, UpdateKind::ChatMember),
                    "chat_join_request" => {
                        decode!(ChatJoinRequest, UpdateKind::ChatJoinRequest)
                    }
                    "chat_boost" => decode!(ChatBoostUpdated, UpdateKind::ChatBoost),
                    "removed_chat_boost" => {
                        decode!(ChatBoostRemoved, UpdateKind::RemovedChatBoost)
                    }
                    _ => UpdateKind::Error(Value::Object(raw)),
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
    """    /// The raw value is preserved for genuinely unknown update kinds. Malformed
    /// payloads of known update kinds are returned as deserialization errors.
""",
    """    /// The raw value is preserved for genuinely unknown update kinds and for
    /// malformed payloads of known update kinds.
""",
    1,
)
text = text.replace(
    """    #[test]
    fn malformed_known_update_is_an_error() {
        let update = r#\"{\"update_id\":1,\"message\":null}\"#;
        assert!(serde_json::from_str::<Update>(update).is_err());
    }
""",
    """    #[test]
    fn malformed_known_update_preserves_raw_value() {
        let update = r#\"{\"update_id\":1,\"message\":null}\"#;
        let update: Update = serde_json::from_str(update).unwrap();

        assert_eq!(update.kind, UpdateKind::Error(serde_json::json!({\"message\": null})));
    }
""",
    1,
)
path.write_text(text)

Path(".github/robustness-test-failure.log").unlink(missing_ok=True)
Path(__file__).unlink()
