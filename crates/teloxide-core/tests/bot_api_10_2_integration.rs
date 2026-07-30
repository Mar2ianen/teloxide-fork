use teloxide_core::{
    payloads::EditMessageText,
    requests::{MultipartPayload, MultipartRequest},
    types::{
        ChatId, ChatJoinRequest, InputFile, InputMediaPhoto, InputRichMessage,
        InputRichMessageMedia, InputRichMessageMediaContent, MediaKind, Message, MessageId,
        MessageKind, UserId,
    },
    Bot,
};

#[test]
fn rich_message_response_preserves_content_and_ephemeral_identifiers() {
    let message: Message = serde_json::from_value(serde_json::json!({
        "message_id": 42,
        "date": 0,
        "chat": {"id": 1, "type": "private", "first_name": "receiver"},
        "receiver_user": {"id": 7, "is_bot": false, "first_name": "receiver"},
        "ephemeral_message_id": 99,
        "rich_message": {"blocks": []}
    }))
    .unwrap();

    assert_eq!(message.ephemeral_message_id, Some(99));
    assert_eq!(message.receiver_user.as_ref().map(|user| user.id), Some(UserId(7)));

    let MessageKind::Common(common) = message.kind else {
        panic!("expected common rich message");
    };
    let MediaKind::RichMessage(rich) = common.media_kind else {
        panic!("expected rich message media kind");
    };
    assert!(rich.rich_message.blocks.is_empty());
}

#[test]
fn rich_only_edit_collects_local_media_and_uses_multipart() {
    fn assert_multipart(_: MultipartRequest<EditMessageText>) {}

    let rich = InputRichMessage::html(r#"<img src="tg://photo?id=cover">"#).media([
        InputRichMessageMedia::new(
            "cover",
            InputRichMessageMediaContent::Photo(InputMediaPhoto::new(InputFile::memory(vec![1]))),
        ),
    ]);
    let mut payload = EditMessageText::rich(ChatId(1), MessageId(2), rich.clone());

    let mut copied = 0;
    payload.copy_files(&mut |_| copied += 1);
    assert_eq!(copied, 1);

    let mut moved = 0;
    payload.move_files(&mut |_| moved += 1);
    assert_eq!(moved, 1);

    assert_multipart(Bot::new("token").edit_message_rich_text(ChatId(1), MessageId(2), rich));
}

#[test]
fn join_request_keeps_query_id_needed_by_query_methods() {
    let request: ChatJoinRequest = serde_json::from_value(serde_json::json!({
        "chat": {"id": -100, "type": "supergroup", "title": "group"},
        "query_id": "join-query",
        "from": {"id": 7, "is_bot": false, "first_name": "joiner"},
        "user_chat_id": 7,
        "date": 0
    }))
    .unwrap();

    assert_eq!(request.query_id.as_deref(), Some("join-query"));
}
