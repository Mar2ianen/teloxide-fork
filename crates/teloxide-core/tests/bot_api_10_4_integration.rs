use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

use serde_json::{json, Value};
use teloxide_core::{
    outbound::{OutboundPayload, OutboundScope},
    payloads::*,
    prelude::*,
    requests::{InvalidValueReason, Payload, RequestFieldPath, RequestValidationError},
    types::*,
    RequestError,
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
fn widget_query_variants_keep_nested_discriminators_and_wallet_precision() {
    let wallet = json!({"address":"EQ-wallet", "amount":"123456789012345678901234567890", "public_key":"abcd", "state_init":"AAAA"});
    for query_type in [
        json!({"type":"data", "wallet_card":{"address":"EQ-wallet", "amount":"123456789012345678901234567890", "stickers":[]}}),
        json!({"type":"sign_data", "wallet":wallet, "sign_data":"{\"domain\":\"bot123\"}"}),
        json!({"type":"sent_transaction", "wallet":wallet, "msg_hash":"hash"}),
    ] {
        let _: WidgetQueryType = round_trip(query_type.clone());
        let query = json!({"id":"query", "from":{"id":7,"is_bot":false,"first_name":"User"}, "chat":{"id":7,"type":"private","first_name":"User"}, "message_id":4, "chat_instance":"instance", "type":query_type, "data":"click"});
        let update: Update =
            serde_json::from_value(json!({"update_id":1,"widget_query":query})).unwrap();
        assert!(matches!(update.kind, UpdateKind::WidgetQuery(_)));
        assert_eq!(update.from().unwrap().id, UserId(7));
        assert_eq!(update.chat().unwrap().id, ChatId(7));
        assert_eq!(update.mentioned_users().count(), 1);
        assert_eq!(serde_json::to_value(&update).unwrap()["widget_query"]["type"], query_type);
        let shared: Update = serde_json::from_value(json!({"update_id":2,"shared_widget_query":{"id":"shared-query", "from":{"id":7,"is_bot":false,"first_name":"User"}, "widget_id":"shared", "data":"click", "type":query_type}})).unwrap();
        assert!(matches!(shared.kind, UpdateKind::SharedWidgetQuery(_)));
        assert_eq!(shared.from().unwrap().id, UserId(7));
        assert!(shared.chat().is_none());
        assert_eq!(shared.mentioned_users().count(), 1);
    }
    let inline: WidgetQuery = serde_json::from_value(json!({"id":"q","from":{"id":7,"is_bot":false,"first_name":"User"},"inline_message_id":"inline","chat_instance":"instance","chat_type":"sender","type":{"type":"data"},"data":"x"})).unwrap();
    assert!(inline.chat.is_none());
    assert!(inline.message_id.is_none());
    assert_eq!(inline.chat_type, Some(ChatType::Sender));
    for malformed in [
        json!({}),
        json!({"type":"sign_data","sign_data":"signed"}),
        json!({"type":"sent_transaction","wallet":wallet}),
    ] {
        assert!(serde_json::from_value::<WidgetQueryType>(malformed.clone()).is_err());
        let raw = json!({"update_id":3,"shared_widget_query":{"id":"q","from":{"id":7,"is_bot":false,"first_name":"User"},"widget_id":"w","data":"x","type":malformed}});
        let update: Update = serde_json::from_value(raw.clone()).unwrap();
        assert!(matches!(update.kind, UpdateKind::Error(_)));
        assert_eq!(serde_json::to_value(update).unwrap(), raw);
    }
}

#[test]
fn wallet_card_share_themes_and_service_messages_are_typed() {
    let position = json!({"x_percentage":50.0,"y_percentage":50.0,"width_percentage":20.0,"height_percentage":20.0,"rotation_angle":0.0,"corner_radius_percentage":4.0});
    let _: AreaPosition = round_trip(position.clone());
    let _: StoryAreaPosition = serde_json::from_value(position.clone()).unwrap();
    let card: TonWalletCard = round_trip(
        json!({"address":"EQ-wallet","amount":"18446744073709551616000000000","stickers":[{"position":position,"nft":{"address":"EQ-nft","collection_address":"EQ-collection","index":"184467440737095516160","name":"NFT","description":"description","attributes":[{"trait_type":"Color","value":"Blue"}]}}],"background_nft_address":"EQ-nft"}),
    );
    let chat: ChatFullInfo = serde_json::from_value(json!({"id":7,"type":"private","first_name":"User","accent_color_id":1,"max_reaction_count":1,"accepted_gift_types":{"unlimited_gifts":true,"limited_gifts":true,"unique_gifts":true,"premium_subscription":true},"ton_wallet_card":card,"share_theme":{"type":"emoji","emojis":[{"type":"emoji","emoji":"🦀"},{"type":"custom_emoji","custom_emoji_id":"123"}],"background_backdrop_id":1234567890123i64}})).unwrap();
    assert_eq!(chat.ton_wallet_card.unwrap().amount, "18446744073709551616000000000");
    assert!(matches!(chat.share_theme, Some(ChatShareTheme::Emoji(_))));
    let _: ChatShareThemeBackdrops = round_trip(
        json!({"backdrops":[{"id":1234567890123i64,"name":"Blue","colors":{"center_color":1,"edge_color":2,"symbol_color":3,"text_color":4}}]}),
    );
    for (field, data) in [
        (
            "ton_wallet_transfer",
            json!({"transaction_id":"","peer_address":"EQ-peer","amount":"-123456789012345678901234567890"}),
        ),
        ("paid_messages_waived", json!({})),
    ] {
        let mut value =
            json!({"message_id":1,"date":0,"chat":{"id":7,"type":"private","first_name":"User"}});
        value[field] = data;
        let message: Message = serde_json::from_value(value).unwrap();
        if field == "ton_wallet_transfer" {
            assert_eq!(
                message.ton_wallet_transfer().unwrap().amount,
                "-123456789012345678901234567890"
            );
        } else {
            assert!(message.paid_messages_waived().is_some());
        }
    }
}

#[test]
fn widget_message_captions_and_inline_contents_are_preserved() {
    let message: Message = serde_json::from_value(json!({"message_id":1,"date":0,"chat":{"id":7,"type":"private","first_name":"User"},"widget":{"html":"<p>Hello</p>","id":"shared","width":300,"height":100},"caption":"Caption","caption_entities":[{"type":"bold","offset":0,"length":7}],"show_caption_above_media":true})).unwrap();
    assert_eq!(message.widget().unwrap().id.as_deref(), Some("shared"));
    assert_eq!(message.caption(), Some("Caption"));
    assert_eq!(message.caption_entities().unwrap().len(), 1);
    assert!(message.show_caption_above_media());
    assert!(!message.has_media_spoiler());
    for content in [
        json!({"html":"<p>Hello</p>","caption":"Widget","parse_mode":"HTML"}),
        json!({"widget_id":"shared","chat_data":"","caption":"Widget","parse_mode":"HTML"}),
    ] {
        let _: InputMessageContent = round_trip(content);
    }
}

#[test]
fn ton_rich_quotes_and_new_entities_round_trip() {
    let rich =
        json!({"type":"ton_address","text":{"type":"bold","text":"wallet"},"address":"EQ-wallet"});
    let text: RichText = round_trip(rich.clone());
    assert!(matches!(text, RichText::Object(RichTextObject::TonAddress(_))));
    let quote: TextQuote =
        serde_json::from_value(json!({"text":"wallet","rich_texts":[rich.clone()],"position":0}))
            .unwrap();
    assert_eq!(quote.rich_texts.as_ref().unwrap().len(), 1);
    let mut reply = ReplyParameters::new(MessageId(1));
    reply.quote_rich_texts = Some(vec![text]);
    assert_eq!(serde_json::to_value(reply).unwrap()["quote_rich_texts"], json!([rich]));
    for kind in ["ton_address", "bank_card_number"] {
        let _: MessageEntity = round_trip(json!({"type":kind,"offset":0,"length":6}));
    }
}

#[test]
fn callback_only_replies_omit_receiver_and_schedule_without_ignored_chat() {
    let ephemeral = EphemeralMessageParameters::reply("callback");
    assert_eq!(serde_json::to_value(&ephemeral).unwrap(), json!({"callback_query_id":"callback"}));
    let payload = SendMessage::new("Only you").ephemeral_message_parameters(ephemeral.clone());
    assert_eq!(
        serde_json::to_value(&payload).unwrap(),
        json!({"text":"Only you","ephemeral_message_parameters":{"callback_query_id":"callback"}})
    );
    assert_eq!(payload.outbound_hint().scope, OutboundScope::Global);
    assert_eq!(payload.chat_id(ChatId(7)).outbound_hint().scope, OutboundScope::Global);
    let normal = SendMessage::new("Hello").chat_id(ChatId(7));
    assert_ne!(normal.outbound_hint().scope, OutboundScope::Global);
    let targeted = normal.ephemeral_message_parameters(EphemeralMessageParameters::new(UserId(7)));
    assert_ne!(targeted.outbound_hint().scope, OutboundScope::Global);
}

// Capture actual requests through Bot and reqwest, with deterministic Telegram
// errors. No real bot token or Telegram account is required.
async fn capture<R: Request<Err = RequestError>>(build: impl FnOnce(Bot) -> R) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            let size = stream.read(&mut buffer).unwrap();
            assert!(size > 0);
            request.extend_from_slice(&buffer[..size]);
            if let Some(end) = request.windows(4).position(|x| x == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                if let Some(length) = headers.lines().find_map(|line| {
                    line.strip_prefix("content-length:")
                        .and_then(|s| s.trim().parse::<usize>().ok())
                }) {
                    if request.len() >= end + 4 + length {
                        break;
                    }
                } else if request.ends_with(b"0\r\n\r\n") {
                    break;
                }
            }
        }
        let body = r#"{"ok":false,"error_code":400,"description":"Bad Request: test fixture"}"#;
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
    let request = build(Bot::new("123:test").set_api_url(url.parse().unwrap()));
    assert!(matches!(request.send().await, Err(RequestError::Api(_))));
    server.join().unwrap()
}

fn json_request(request: &[u8], endpoint: &str) -> Value {
    let end = request.windows(4).position(|x| x == b"\r\n\r\n").unwrap();
    let header = String::from_utf8_lossy(&request[..end]);
    assert!(
        header
            .to_ascii_lowercase()
            .starts_with(&format!("post /bot123:test/{} http/1.1", endpoint.to_ascii_lowercase())),
        "{header}"
    );
    serde_json::from_slice(&request[end + 4..]).unwrap()
}

#[tokio::test]
async fn all_new_methods_reach_the_exact_bot_api_endpoints() {
    let request = capture(|bot| {
        bot.send_widget(ChatId(7), "<p>Hello</p>")
            .allow_queries(true)
            .is_shared(true)
            .public_data("{}")
    })
    .await;
    assert_eq!(json_request(&request, "sendWidget")["html"], "<p>Hello</p>");
    let request = capture(|bot| bot.send_shared_widget(ChatId(7), "shared").chat_data("")).await;
    assert_eq!(json_request(&request, "sendSharedWidget")["chat_data"], "");
    let request = capture(|bot| bot.answer_widget_query("query").data("")).await;
    assert_eq!(
        json_request(&request, "answerWidgetQuery"),
        json!({"widget_query_id":"query","data":""})
    );
    let request = capture(|bot| {
        bot.edit_message_widget_data(ChatId(7), MessageId(9)).public_data("").chat_data("{}")
    })
    .await;
    assert_eq!(
        json_request(&request, "editMessageWidgetData"),
        json!({"chat_id":7,"message_id":9,"public_data":"","chat_data":"{}"})
    );
    let request = capture(|bot| bot.edit_message_widget_data_inline("inline").chat_data("")).await;
    assert_eq!(
        json_request(&request, "editMessageWidgetData"),
        json!({"inline_message_id":"inline","chat_data":""})
    );
    let request = capture(|bot| bot.edit_shared_widget_data("shared", "").cache_time(5)).await;
    assert_eq!(
        json_request(&request, "editSharedWidgetData"),
        json!({"widget_id":"shared","public_data":"","cache_time":5})
    );
    let request = capture(|bot| bot.get_chat_share_theme_backdrops()).await;
    assert_eq!(json_request(&request, "getChatShareThemeBackdrops"), json!({}));
    let request = capture(|bot| bot.get_unique_gift("Gift-123")).await;
    assert_eq!(json_request(&request, "getUniqueGift"), json!({"name":"Gift-123"}));
    let request = capture(|bot| {
        bot.delete_business_account_gift(
            BusinessConnectionId("business".into()),
            OwnedGiftId("gift".into()),
        )
    })
    .await;
    assert_eq!(
        json_request(&request, "deleteBusinessAccountGift"),
        json!({"business_connection_id":"business","owned_gift_id":"gift"})
    );
    assert_eq!(EditMessageWidgetDataInline::NAME, "EditMessageWidgetDataInline");
}

#[tokio::test]
async fn callback_only_json_and_multipart_replies_keep_the_wire_shape() {
    let request = capture(|bot| {
        bot.send_message("Only you")
            .ephemeral_message_parameters(EphemeralMessageParameters::reply("callback"))
    })
    .await;
    let body = json_request(&request, "sendMessage");
    assert!(body.get("chat_id").is_none());
    assert_eq!(body["ephemeral_message_parameters"], json!({"callback_query_id":"callback"}));
    let request = capture(|bot| {
        bot.send_photo(InputFile::memory("file bytes").file_name("photo.png"))
            .ephemeral_message_parameters(EphemeralMessageParameters::reply("callback"))
    })
    .await;
    let body = String::from_utf8(request).unwrap();
    assert!(body.to_ascii_lowercase().starts_with("post /bot123:test/sendphoto http/1.1"));
    assert!(body.contains("multipart/form-data"));
    assert!(!body.contains("name=\"chat_id\""));
    assert!(!body.contains("receiver_user_id"));
    assert!(body.contains("{\"callback_query_id\":\"callback\"}"));
    assert_eq!(body.matches("attach://").count(), 1);
    assert_eq!(body.matches("filename=\"photo.png\"").count(), 1);
    assert_eq!(body.matches("file bytes").count(), 1);
}

#[tokio::test]
#[cfg(all(feature = "cache_me", feature = "trace_adaptor", feature = "erased"))]
async fn widget_and_verification_formatting_survives_the_adaptor_stack() {
    let request = capture(|bot| {
        bot.cache_me()
            .trace(teloxide_core::adaptors::trace::Settings::empty())
            .parse_mode(ParseMode::Html)
            .erase()
            .send_widget(ChatId(7), "<p>Hello</p>")
            .caption("<b>Caption</b>")
    })
    .await;
    assert_eq!(json_request(&request, "sendWidget")["parse_mode"], "HTML");
    let request = capture(|bot| {
        bot.parse_mode(ParseMode::Html)
            .erase()
            .send_shared_widget(ChatId(7), "shared")
            .caption("<b>Caption</b>")
    })
    .await;
    assert_eq!(json_request(&request, "sendSharedWidget")["parse_mode"], "HTML");
    let request = capture(|bot| {
        bot.parse_mode(ParseMode::Html)
            .erase()
            .verify_user(UserId(7))
            .custom_description("<a href=\"https://example.com\">Verified</a>")
    })
    .await;
    assert_eq!(json_request(&request, "verifyUser")["custom_description_parse_mode"], "HTML");
    let request = capture(|bot| {
        bot.parse_mode(ParseMode::Html)
            .erase()
            .verify_chat(ChatId(7))
            .custom_description("Verified")
            .custom_description_parse_mode(ParseMode::MarkdownV2)
    })
    .await;
    assert_eq!(json_request(&request, "verifyChat")["custom_description_parse_mode"], "MarkdownV2");
    // A cached media result can override its message content with a widget too.
    let result:InlineQueryResult=serde_json::from_value(json!({"type":"photo","id":"result","photo_file_id":"file","input_message_content":{"html":"<p>Hello</p>","caption":"<b>Widget</b>"}})).unwrap();
    let request = capture(|bot| {
        bot.parse_mode(ParseMode::Html)
            .erase()
            .answer_inline_query(InlineQueryId("query".into()), [result])
    })
    .await;
    let body = String::from_utf8(request).unwrap();
    assert!(body.contains("\"input_message_content\":{\"html\":\"<p>Hello</p>\""));
    assert!(body.contains("\"parse_mode\":\"HTML\""));
}

#[test]
fn every_ephemeral_send_payload_can_omit_chat_id() {
    fn check<T: serde::Serialize>(payload: T) {
        let value = serde_json::to_value(payload).unwrap();
        assert!(value.get("chat_id").is_none(), "{value}");
        assert_eq!(value["ephemeral_message_parameters"], json!({"callback_query_id":"callback"}));
    }
    let e = EphemeralMessageParameters::reply("callback");
    let file = || InputFile::file_id(FileId("file".into()));
    check(SendMessage::new("text").ephemeral_message_parameters(e.clone()));
    check(
        SendRichMessage::new(InputRichMessage::html("<b>text</b>"))
            .ephemeral_message_parameters(e.clone()),
    );
    check(SendAnimation::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendAudio::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendDocument::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendPhoto::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendSticker::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendVideo::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendVideoNote::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendVoice::new(file()).ephemeral_message_parameters(e.clone()));
    check(SendLivePhoto::new(file(), file()).ephemeral_message_parameters(e.clone()));
    check(SendContact::new("+123", "User").ephemeral_message_parameters(e.clone()));
    check(SendLocation::new(0.0, 0.0).ephemeral_message_parameters(e.clone()));
    check(SendVenue::new(0.0, 0.0, "Venue", "Address").ephemeral_message_parameters(e));
}

#[test]
fn malformed_widget_input_never_falls_back_to_another_message_shape() {
    for value in [
        json!({"html":"<p>Widget</p>","widget_id":"shared"}),
        json!({"html":"<p>Widget</p>","message_text":"text"}),
        json!({"widget_id":null,"message_text":"text"}),
        json!({"html":123}),
        json!({"html":"<p>Widget</p>","rich_message":{"html":"<b>Rich</b>"}}),
        json!({"widget_id":"shared","rich_message":{"html":"<b>Rich</b>"}}),
    ] {
        assert!(serde_json::from_value::<InputMessageContent>(value).is_err());
    }
}

#[test]
fn unique_gift_theme_and_external_widget_reply_decode() {
    let sticker = json!({"file_id":"file","file_unique_id":"unique","type":"regular","width":1,"height":1,"is_animated":false,"is_video":false,"file_size":1,"needs_repainting":false,"premium_animation":null});
    let gift = json!({"base_name":"Gift","name":"Gift-1","number":1,"model":{"name":"Model","sticker":sticker,"rarity_per_mille":1},"symbol":{"name":"Symbol","sticker":sticker,"rarity_per_mille":1},"backdrop":{"name":"Backdrop","rarity_per_mille":1,"colors":{"center_color":1,"edge_color":2,"symbol_color":3,"text_color":4}}});
    let theme: ChatShareTheme = round_trip(json!({"type":"unique_gift","gift":gift}));
    assert!(matches!(theme, ChatShareTheme::UniqueGift(_)));
    let reply:ExternalReplyInfo=serde_json::from_value(json!({"origin":{"type":"user","date":0,"sender_user":{"id":7,"is_bot":false,"first_name":"User"}},"widget":{"html":"<p>Widget</p>"}})).unwrap();
    assert!(matches!(reply.kind, ExternalReplyInfoKind::Widget(_)));
}

// Exercise the generated Payload::validate hooks, not just the common
// predicate.
macro_rules! for_each_ephemeral_send {
    ($bot:ident, $check:ident) => {{
        let file = || InputFile::memory("upload bytes").file_name("media.bin");
        $check!($bot.send_message("Hello"));
        $check!($bot.send_rich_message(InputRichMessage::html("<b>Hello</b>")));
        $check!($bot.send_animation(file()));
        $check!($bot.send_audio(file()));
        $check!($bot.send_contact("+123", "User"));
        $check!($bot.send_document(file()));
        $check!($bot.send_live_photo(file(), file()));
        $check!($bot.send_location(0.0, 0.0));
        $check!($bot.send_photo(file()));
        $check!($bot.send_sticker(file()));
        $check!($bot.send_venue(0.0, 0.0, "Venue", "Address"));
        $check!($bot.send_video(file()));
        $check!($bot.send_video_note(file()));
        $check!($bot.send_voice(file()));
    }};
}

#[test]
fn all_ephemeral_sends_accept_normal_targeted_and_callback_only_delivery() {
    use teloxide_core::requests::HasPayload;
    let bot = Bot::new("123:test");
    macro_rules! check {
        ($request:expr) => {{
            let request = $request.chat_id(ChatId(7));
            assert_eq!(Payload::validate(request.payload_ref()), Ok(()));
            let request = $request
                .chat_id(ChatId(7))
                .ephemeral_message_parameters(EphemeralMessageParameters::new(UserId(7)));
            assert_eq!(Payload::validate(request.payload_ref()), Ok(()));
            let request = $request.chat_id(ChatId(7)).ephemeral_message_parameters(
                EphemeralMessageParameters::new(UserId(7)).callback_query_id("callback"),
            );
            assert_eq!(Payload::validate(request.payload_ref()), Ok(()));
            let request = $request
                .ephemeral_message_parameters(EphemeralMessageParameters::reply("callback"));
            assert_eq!(Payload::validate(request.payload_ref()), Ok(()));
            let request = $request
                .chat_id(ChatId(7))
                .ephemeral_message_parameters(EphemeralMessageParameters::reply("callback"));
            assert_eq!(Payload::validate(request.payload_ref()), Ok(()));
        }};
    }
    for_each_ephemeral_send!(bot, check);
}

fn assert_target_rejected<R: Request<Err = RequestError>>(
    request: R,
    path: &'static str,
    reason: InvalidValueReason,
) -> impl std::future::Future<Output = ()> {
    // Bound the matrix test's async frame on default test-thread stacks.
    Box::pin(async move {
        let expected =
            RequestValidationError::InvalidValue { path: RequestFieldPath::field(path), reason };
        // Both immutable and consuming transports must validate before serialization,
        // attachment preparation or HTTP dispatch (JSON and multipart).
        let result = tokio::time::timeout(Duration::from_secs(1), request.send_ref())
            .await
            .expect("send_ref must fail preflight without waiting for HTTP");
        match result {
            Err(RequestError::Validation(error)) => assert_eq!(error, expected),
            _ => panic!("expected send_ref validation error at {path}"),
        }
        let result = tokio::time::timeout(Duration::from_secs(1), request.send())
            .await
            .expect("send must fail preflight without waiting for HTTP");
        match result {
            Err(RequestError::Validation(error)) => assert_eq!(error, expected),
            _ => panic!("expected send validation error at {path}"),
        }
    })
}

#[tokio::test]
async fn all_ephemeral_sends_reject_invalid_targets_without_contacting_http() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let bot = Bot::new("123:test")
        .set_api_url(format!("http://{}", listener.local_addr().unwrap()).parse().unwrap());
    macro_rules! check {
        ($request:expr) => {{
            assert_target_rejected(
                $request,
                "chat_id",
                InvalidValueReason::MustHaveChatOrCallbackReply,
            )
            .await;
            assert_target_rejected(
                $request.ephemeral_message_parameters(EphemeralMessageParameters::new(UserId(7))),
                "chat_id",
                InvalidValueReason::MustHaveChatOrCallbackReply,
            )
            .await;
            assert_target_rejected(
                $request.ephemeral_message_parameters(
                    EphemeralMessageParameters::new(UserId(7)).callback_query_id("callback"),
                ),
                "chat_id",
                InvalidValueReason::MustHaveChatOrCallbackReply,
            )
            .await;
            let empty = EphemeralMessageParameters {
                receiver_user_id: None,
                callback_query_id: None,
                replace_callback_query_message: None,
            };
            assert_target_rejected(
                $request.ephemeral_message_parameters(empty.clone()),
                "ephemeral_message_parameters",
                InvalidValueReason::MustHaveReceiverOrCallback,
            )
            .await;
            assert_target_rejected(
                $request.chat_id(ChatId(7)).ephemeral_message_parameters(empty),
                "ephemeral_message_parameters",
                InvalidValueReason::MustHaveReceiverOrCallback,
            )
            .await;
        }};
    }
    for_each_ephemeral_send!(bot, check);
    match listener.accept() {
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
        Ok(_) => panic!("an invalid send reached the HTTP listener"),
    }
}
