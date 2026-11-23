use proptest::prelude::*;
use serde_json::{json, Value};
use teloxide_core::{
    payloads::SetMySettings,
    types::{BotFullInfo, CallbackQueryMode, InputRichBlock, InputRichBlockExpandableBlockQuotation,
        InputRichBlockParagraph, InputRichMessage, RichText},
};

proptest! {
    /// None means "leave unchanged", Some(false) must be serialized explicitly.
    #[test]
    fn settings_optional_booleans_are_three_state(
        inline in prop::option::of(any::<bool>()),
        groups in prop::option::of(any::<bool>()),
        business in prop::option::of(any::<bool>()),
        mode in prop::option::of(0u8..3),
    ) {
        let mut settings = SetMySettings::new();
        settings.supports_inline_queries = inline;
        settings.can_join_groups = groups;
        settings.can_connect_to_business = business;
        settings.callback_query_mode = mode.map(|v| match v {
            0 => CallbackQueryMode::Default,
            1 => CallbackQueryMode::Auto,
            _ => CallbackQueryMode::Inline,
        });
        let wire = serde_json::to_value(settings).unwrap();

        prop_assert_eq!(wire.get("supports_inline_queries").cloned(), inline.map(Value::Bool));
        prop_assert_eq!(wire.get("can_join_groups").cloned(), groups.map(Value::Bool));
        prop_assert_eq!(wire.get("can_connect_to_business").cloned(), business.map(Value::Bool));
        let expected_mode = mode.map(|v| {
            json!(match v {
                0 => "default",
                1 => "auto",
                _ => "inline",
            })
        });
        prop_assert_eq!(wire.get("callback_query_mode").cloned(), expected_mode);
    }

    /// Every getMe boolean is required and preserves its value independently.
    #[test]
    fn full_info_preserves_independent_boolean_capabilities(
        flags in prop::collection::vec(any::<bool>(), 12),
    ) {
        let fields = [
            "can_join_groups", "can_read_all_group_messages",
            "supports_inline_queries", "needs_inline_query_location",
            "can_connect_to_business", "supports_guest_queries",
            "supports_join_request_queries", "has_topics_enabled",
            "allows_users_to_create_topics", "has_topic_sidebar",
            "can_manage_bots", "has_main_web_app",
        ];
        let mut wire = json!({
            "id": 456, "is_bot": true, "first_name": "Test",
            "can_join_groups": false, "can_read_all_group_messages": false,
            "supports_inline_queries": false, "needs_inline_query_location": false,
            "can_connect_to_business": false, "supports_guest_queries": false,
            "supports_join_request_queries": false, "has_topics_enabled": false,
            "allows_users_to_create_topics": false, "has_topic_sidebar": false,
            "can_manage_bots": false, "has_main_web_app": false
        });
        for (name, flag) in fields.iter().zip(flags.iter()) {
            wire[*name] = json!(flag);
        }

        let info: BotFullInfo = serde_json::from_value(wire.clone()).unwrap();
        let serialized = serde_json::to_value(info).unwrap();
        for name in fields {
            prop_assert_eq!(&serialized[name], &wire[name], "field: {}", name);
        }
    }

    /// Generated rich blocks must not add phantom variants or lose UTF-8 content.
    #[test]
    fn expandable_quote_preserves_text(text in ".{0,256}") {
        let quote = InputRichMessage::blocks([
            InputRichBlock::ExpandableBlockquote(InputRichBlockExpandableBlockQuotation {
                blocks: vec![InputRichBlock::Paragraph(InputRichBlockParagraph {
                    text: RichText::from(text.as_str()),
                })],
                credit: None,
            }),
        ]);
        let wire = serde_json::to_value(quote).unwrap();
        prop_assert_eq!(&wire["blocks"][0]["blocks"][0]["text"], &json!(text));
        prop_assert!(wire["blocks"][0].get("credit").is_none());
    }
}
