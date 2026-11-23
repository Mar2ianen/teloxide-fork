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
