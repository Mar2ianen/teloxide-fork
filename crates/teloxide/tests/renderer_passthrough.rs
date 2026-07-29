use teloxide::{
    types::{MessageEntity, MessageEntityKind},
    utils::render::Renderer,
};

#[test]
fn public_renderer_reports_unrepresented_entity_semantics() {
    let text = "@name";
    let entities = [MessageEntity::new(MessageEntityKind::Mention, 0, 5)];
    let renderer = Renderer::new(text, &entities);

    assert!(renderer.has_passthrough_entities());
    assert_eq!(renderer.passthrough_entities().collect::<Vec<_>>(), [&MessageEntityKind::Mention]);
    assert_eq!(renderer.as_html(), text);
    assert_eq!(renderer.as_markdown(), text);
}
