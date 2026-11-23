use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

use serde_json::{json, Value};
use teloxide_core::{
    prelude::*,
    types::{
        BotFullInfo, CallbackQueryMode, InputRichBlock, InputRichBlockExpandableBlockQuotation,
        InputRichBlockParagraph, InputRichMessage, RichBlock, RichBlockKind, Update, UpdateKind,
        UserId, WriteAccessAllowed,
    },
};

fn round_trip<T>(value: Value) -> T
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let decoded: T = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), value);
    decoded
}

#[test]
fn bot_full_info_settings_and_owner_invite_event_are_typed() {
    let full_info: BotFullInfo = serde_json::from_value(json!({
        "id": 123,
        "is_bot": true,
        "first_name": "Example",
        "username": "example_bot",
        "active_usernames": ["example_bot", "example_ai"],
        "can_join_groups": true,
        "can_read_all_group_messages": false,
        "supports_guest_queries": true,
        "supports_inline_queries": true,
        "inline_query_placeholder": "Search",
        "needs_inline_query_location": false,
        "can_connect_to_business": true,
        "has_main_web_app": false,
        "has_topics_enabled": true,
        "allows_users_to_create_topics": true,
        "has_topic_sidebar": true,
        "can_manage_bots": false,
        "supports_join_request_queries": true
    }))
    .unwrap();
    assert_eq!(full_info.user.id, UserId(123));
    assert_eq!(
        full_info
            .active_usernames
            .as_deref()
            .unwrap()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["example_bot", "example_ai"]
    );
    assert!(full_info.has_topic_sidebar);

    for (name, mode) in [
        ("default", CallbackQueryMode::Default),
        ("auto", CallbackQueryMode::Auto),
        ("inline", CallbackQueryMode::Inline),
    ] {
        assert_eq!(serde_json::to_value(mode).unwrap(), json!(name));
        assert_eq!(serde_json::from_value::<CallbackQueryMode>(json!(name)).unwrap(), mode);
    }

    let allowed: WriteAccessAllowed = round_trip(json!({
        "inviter": {"id": 1, "is_bot": false, "first_name": "Owner"}
    }));
    assert_eq!(allowed.inviter.unwrap().first_name, "Owner");
    let update: Update = serde_json::from_value(json!({
        "update_id": 1,
        "message": {
            "message_id": 1,
            "date": 0,
            "chat": {"id": 7, "type": "private", "first_name": "Invitee"},
            "write_access_allowed": {"inviter": {"id": 1, "is_bot": false, "first_name": "Owner"}}
        }
    }))
    .unwrap();
    assert!(matches!(update.kind, UpdateKind::Message(_)));
    assert_eq!(update.mentioned_users().count(), 1);
}

#[test]
fn expandable_quotes_round_trip_arbitrary_input_and_output_blocks() {
    let outgoing = InputRichMessage::blocks([InputRichBlock::ExpandableBlockquote(
        InputRichBlockExpandableBlockQuotation {
            blocks: vec![InputRichBlock::Paragraph(InputRichBlockParagraph {
                text: "details".into(),
            })],
            credit: None,
        },
    )]);
    assert_eq!(
        serde_json::to_value(outgoing).unwrap(),
        json!({"blocks":[{"type":"expandable_blockquote","blocks":[{"type":"paragraph","text":"details"}]}]})
    );

    let incoming: RichBlock = serde_json::from_value(json!({
        "type": "expandable_blockquote",
        "blocks": [{"type": "table", "cells": []}]
    }))
    .unwrap();
    assert!(
        matches!(&incoming, RichBlock::Known(kind) if matches!(kind.as_ref(), RichBlockKind::ExpandableBlockquote(_)))
    );
    assert_eq!(serde_json::to_value(incoming).unwrap()["blocks"][0]["type"], "table");
}

async fn capture_settings_request() -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let size = stream.read(&mut buffer).unwrap();
            assert!(size > 0);
            request.extend_from_slice(&buffer[..size]);
            if let Some(end) = request.windows(4).position(|x| x == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse::<usize>()
                    .unwrap();
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        let body = r#"{"ok":true,"result":true}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
        request
    });

    let bot = Bot::new("123:test").set_api_url(url.parse().unwrap());
    bot.set_my_settings()
        .supports_inline_queries(true)
        .inline_feedback_share_denominator(0)
        .has_topic_sidebar(true)
        .can_communicate_with_bots(true)
        .callback_query_mode(CallbackQueryMode::Auto)
        .await
        .unwrap();
    server.join().unwrap()
}

#[tokio::test]
async fn set_my_settings_serializes_partial_updates_to_its_endpoint() {
    let request = capture_settings_request().await;
    let end = request.windows(4).position(|x| x == b"\r\n\r\n").unwrap();
    let header = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
    assert!(header.starts_with("post /bot123:test/setmysettings http/1.1"));
    assert_eq!(
        serde_json::from_slice::<Value>(&request[end + 4..]).unwrap(),
        json!({
            "supports_inline_queries": true,
            "inline_feedback_share_denominator": 0,
            "has_topic_sidebar": true,
            "can_communicate_with_bots": true,
            "callback_query_mode": "auto"
        })
    );
}

#[test]
fn telegram_service_senders_are_not_real_chat_identity() {
    use teloxide_core::types::ChatId;

    for (sender, sender_chat, is_channel) in [
        (
            json!({
                "id": 136817688,
                "is_bot": true,
                "first_name": "Channel",
                "username": "Channel_Bot"
            }),
            json!({"id": -1001234567890_i64, "type": "channel", "title": "News"}),
            true,
        ),
        (
            json!({
                "id": 1087968824,
                "is_bot": true,
                "first_name": "Group",
                "username": "GroupAnonymousBot"
            }),
            json!({"id": -1009876543210_i64, "type": "supergroup", "title": "Group"}),
            false,
        ),
    ] {
        let update: Update = serde_json::from_value(json!({
            "update_id": 123,
            "message": {
                "message_id": 123,
                "date": 0,
                "chat": {"id": -1009876543210_i64, "type": "supergroup", "title": "Group"},
                "from": sender,
                "sender_chat": sender_chat,
                "text": "message from a chat"
            }
        }))
        .unwrap();
        let UpdateKind::Message(message) = update.kind else {
            panic!("expected a message update");
        };
        let from = message.from.as_ref().unwrap();
        assert_eq!(from.is_channel(), is_channel);
        assert_eq!(from.is_anonymous(), !is_channel);

        let actual_chat = message.sender_chat.as_ref().unwrap();
        assert_ne!(actual_chat.id, ChatId(from.id.0 as i64));
        assert!(actual_chat.id.0 < 0);
    }
}

#[test]
fn bot_full_info_optional_metadata_and_required_capabilities() {
    let minimal = json!({
        "id": 777,
        "is_bot": true,
        "first_name": "No Username",
        "can_join_groups": false,
        "can_read_all_group_messages": false,
        "supports_guest_queries": false,
        "supports_inline_queries": false,
        "needs_inline_query_location": false,
        "can_connect_to_business": false,
        "has_main_web_app": false,
        "has_topics_enabled": false,
        "allows_users_to_create_topics": false,
        "has_topic_sidebar": false,
        "can_manage_bots": false,
        "supports_join_request_queries": false
    });
    let info: BotFullInfo = serde_json::from_value(minimal.clone()).unwrap();
    assert_eq!(info.user.username, None);
    assert_eq!(info.active_usernames, None);
    assert_eq!(info.inline_query_placeholder, None);
    assert!(info.user.is_bot);

    let mut missing_required = minimal;
    missing_required.as_object_mut().unwrap().remove("has_topic_sidebar");
    assert!(serde_json::from_value::<BotFullInfo>(missing_required).is_err());
}

#[test]
fn callback_query_inline_mode_does_not_require_a_message() {
    use teloxide_core::types::CallbackQuery;

    let query: CallbackQuery = serde_json::from_value(json!({
        "id": "callback-id",
        "from": {"id": 42, "is_bot": false, "first_name": "Tester"},
        "inline_message_id": "AAED-inline-id",
        "chat_instance": "chat-instance",
        "data": "button"
    }))
    .unwrap();
    assert!(query.message.is_none());
    assert!(query.regular_message().is_none());
    assert_eq!(query.inline_message_id.as_deref(), Some("AAED-inline-id"));
    assert_eq!(query.data.as_deref(), Some("button"));
}

#[test]
fn set_my_settings_default_payload_does_not_reset_other_settings() {
    use teloxide_core::payloads::SetMySettings;

    let payload = SetMySettings::new();
    assert_eq!(serde_json::to_value(payload).unwrap(), json!({}));
}

#[test]
fn bot_full_info_capabilities_preserve_boolean_values() {
    let baseline = json!({
        "id": 777,
        "is_bot": true,
        "first_name": "Capabilities",
        "can_join_groups": false,
        "can_read_all_group_messages": false,
        "supports_guest_queries": false,
        "supports_inline_queries": false,
        "needs_inline_query_location": false,
        "can_connect_to_business": false,
        "has_main_web_app": false,
        "has_topics_enabled": false,
        "allows_users_to_create_topics": false,
        "has_topic_sidebar": false,
        "can_manage_bots": false,
        "supports_join_request_queries": false
    });
    let keys = [
        "can_join_groups",
        "can_read_all_group_messages",
        "supports_guest_queries",
        "supports_inline_queries",
        "needs_inline_query_location",
        "can_connect_to_business",
        "has_main_web_app",
        "has_topics_enabled",
        "allows_users_to_create_topics",
        "has_topic_sidebar",
        "can_manage_bots",
        "supports_join_request_queries",
    ];

    for field in keys {
        for value in [true, false] {
            let mut raw = baseline.clone();
            raw[field] = json!(value);
            let info: BotFullInfo = serde_json::from_value(raw).unwrap();
            assert_eq!(serde_json::to_value(&info).unwrap()[field], json!(value), "{field}");
        }
    }
}

#[test]
fn set_my_settings_supports_all_fifteen_optional_fields() {
    use teloxide_core::payloads::SetMySettings;

    let mut payload = SetMySettings::new();
    payload.supports_inline_queries = Some(false);
    payload.inline_query_placeholder = Some(String::new());
    payload.needs_inline_query_location = Some(true);
    payload.inline_feedback_share_denominator = Some(0);
    payload.can_join_groups = Some(false);
    payload.can_read_all_group_messages = Some(true);
    payload.can_connect_to_business = Some(false);
    payload.can_manage_bots = Some(true);
    payload.supports_guest_queries = Some(false);
    payload.supports_join_request_queries = Some(true);
    payload.has_topics_enabled = Some(false);
    payload.allows_users_to_create_topics = Some(true);
    payload.has_topic_sidebar = Some(false);
    payload.can_communicate_with_bots = Some(true);
    payload.callback_query_mode = Some(CallbackQueryMode::Inline);

    assert_eq!(
        serde_json::to_value(payload).unwrap(),
        json!({
            "supports_inline_queries": false,
            "inline_query_placeholder": "",
            "needs_inline_query_location": true,
            "inline_feedback_share_denominator": 0,
            "can_join_groups": false,
            "can_read_all_group_messages": true,
            "can_connect_to_business": false,
            "can_manage_bots": true,
            "supports_guest_queries": false,
            "supports_join_request_queries": true,
            "has_topics_enabled": false,
            "allows_users_to_create_topics": true,
            "has_topic_sidebar": false,
            "can_communicate_with_bots": true,
            "callback_query_mode": "inline"
        })
    );
}

