use teloxide_core::{
    payloads::{GetUpdates, GetUpdatesSetters},
    types::UpdateId,
};

#[test]
fn maximum_update_id_serializes_as_a_positive_next_offset() {
    let payload = GetUpdates::new().offset(UpdateId(u32::MAX).as_offset());
    let value = serde_json::to_value(payload).unwrap();

    assert_eq!(value, serde_json::json!({ "offset": 4_294_967_296_i64 }));
}

#[test]
fn widened_offset_remains_signed() {
    let payload = GetUpdates::new().offset(-1_i64);
    let value = serde_json::to_value(payload).unwrap();

    assert_eq!(value, serde_json::json!({ "offset": -1 }));
}
