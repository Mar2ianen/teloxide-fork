use teloxide_core::{
    payloads::{GetBusinessAccountGifts, GetBusinessAccountGiftsSetters},
    types::BusinessConnectionId,
};

#[test]
fn business_account_gift_filters_use_current_wire_names() {
    let payload = GetBusinessAccountGifts::new(BusinessConnectionId("business".to_owned()))
        .exclude_limited_upgradable(true)
        .exclude_limited_non_upgradable(true)
        .exclude_from_blockchain(true);
    let value = serde_json::to_value(payload).unwrap();
    let object = value.as_object().unwrap();

    assert_eq!(object["exclude_limited_upgradable"], true);
    assert_eq!(object["exclude_limited_non_upgradable"], true);
    assert_eq!(object["exclude_from_blockchain"], true);
    assert!(!object.contains_key("exclude_limited"));
}
