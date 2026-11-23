//! Offline wire-level checks: no Telegram credentials or external network
//! needed.

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

use serde_json::json;
use teloxide_core::{
    prelude::*,
    self_check::{
        check_bot, check_delivery, BotDeliveryMode, ExpectedBotCapabilities, ExpectedBotDelivery,
    },
    types::{CallbackQueryMode, UpdateKind, UserId},
};

fn mock_response(
    status: &'static str,
    response: serde_json::Value,
) -> (reqwest::Url, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url: reqwest::Url = format!("http://{}", listener.local_addr().unwrap()).parse().unwrap();
    let handle = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut request = [0u8; 8192];
        let size = client.read(&mut request).unwrap();
        let request = String::from_utf8_lossy(&request[..size]).into_owned();
        let response = response.to_string();
        write!(
            client,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
        request
    });
    (url, handle)
}

#[tokio::test]
async fn self_check_uses_a_read_only_get_me_request() {
    let (url, handler) = mock_response(
        "200 OK",
        json!({
            "ok": true,
            "result": {
                "id": 123, "is_bot": true, "first_name": "Local Test",
                "can_join_groups": true, "can_read_all_group_messages": false,
                "supports_inline_queries": true, "needs_inline_query_location": false,
                "can_connect_to_business": false, "supports_guest_queries": false,
                "supports_join_request_queries": false, "has_topics_enabled": false,
                "allows_users_to_create_topics": false, "has_topic_sidebar": false,
                "can_manage_bots": false, "has_main_web_app": false
            }
        }),
    );
    let bot = Bot::new("123:offline").set_api_url(url);
    let expected = ExpectedBotCapabilities {
        id: Some(UserId(123)),
        supports_inline_queries: Some(true),
        can_read_all_group_messages: Some(true),
        ..ExpectedBotCapabilities::default()
    };
    let report = check_bot(&bot, &expected).await.unwrap();
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.info.user.username, None);

    let request = handler.join().unwrap();
    assert!(request.to_ascii_lowercase().starts_with("post /bot123:offline/getme http/1.1"));
    assert!(!request.to_ascii_lowercase().contains("setmysettings"));
}

#[tokio::test]
async fn long_polling_decodes_internal_channel_bot_update() {
    let (url, handler) = mock_response(
        "200 OK",
        json!({
            "ok": true,
            "result": [{
                "update_id": 88,
                "channel_post": {
                    "message_id": 1, "date": 0,
                    "chat": {"id": -1001234567890_i64, "type": "channel", "title": "Channel"},
                    "from": {
                        "id": 136817688, "is_bot": true,
                        "first_name": "Channel", "username": "Channel_Bot"
                    },
                    "sender_chat": {
                        "id": -1001234567890_i64, "type": "channel", "title": "Channel"
                    },
                    "text": "A channel post"
                }
            }]
        }),
    );
    let bot = Bot::new("123:offline").set_api_url(url);
    let updates = bot.get_updates().await.unwrap();
    assert_eq!(updates.len(), 1);
    let UpdateKind::ChannelPost(message) = &updates[0].kind else {
        panic!("channel post was not decoded");
    };
    assert!(message.from.as_ref().unwrap().is_channel());
    assert_ne!(message.sender_chat.as_ref().unwrap().id.0, 136817688);
    assert!(handler.join().unwrap().to_ascii_lowercase().contains("/getupdates"));
}

#[tokio::test]
async fn rate_limit_is_not_silently_treated_as_success() {
    let (url, handler) = mock_response(
        "429 Too Many Requests",
        json!({
            "ok": false, "error_code": 429,
            "description": "Too Many Requests: retry after 2",
            "parameters": {"retry_after": 2}
        }),
    );
    let bot = Bot::new("123:offline").set_api_url(url);
    let result = bot.set_my_settings().callback_query_mode(CallbackQueryMode::Auto).await;
    assert!(result.is_err());
    assert!(handler.join().unwrap().to_ascii_lowercase().contains("/setmysettings"));
}

#[tokio::test]
async fn delivery_self_check_distinguishes_polling_from_webhook() {
    let (url, handler) = mock_response(
        "200 OK",
        json!({
            "ok": true,
            "result": {
                "url": "",
                "has_custom_certificate": false,
                "pending_update_count": 8
            }
        }),
    );
    let bot = Bot::new("123:offline").set_api_url(url);
    let expected =
        ExpectedBotDelivery { mode: Some(BotDeliveryMode::Polling), max_pending_updates: Some(4) };
    let report = check_delivery(&bot, &expected).await.unwrap();
    assert_eq!(report.issues.len(), 1);
    assert!(report.info.url.is_none());
    assert!(handler.join().unwrap().to_ascii_lowercase().contains("/getwebhookinfo"));
}

#[test]
fn telegram_internal_sender_identifiers_are_disjoint() {
    use teloxide_core::types::{User, UserId};

    let telegram: User = serde_json::from_value(json!({
        "id": 777000,
        "is_bot": false,
        "first_name": "Telegram"
    }))
    .unwrap();
    assert!(telegram.is_telegram());
    assert!(!telegram.is_channel());
    assert!(!telegram.is_anonymous());

    assert!(UserId(136817688).is_channel());
    assert!(UserId(1087968824).is_anonymous());
    assert!(!UserId(777000).is_channel());
}
