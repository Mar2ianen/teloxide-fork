#![no_main]

use libfuzzer_sys::fuzz_target;
use teloxide_core::types::{BotFullInfo, RichBlock, RichMessage};

fuzz_target!(|data: &[u8]| {
    if let Ok(info) = serde_json::from_slice::<BotFullInfo>(data) {
        let bytes = serde_json::to_vec(&info).unwrap();
        let _: BotFullInfo = serde_json::from_slice(&bytes).unwrap();
    }
    if let Ok(block) = serde_json::from_slice::<RichBlock>(data) {
        let bytes = serde_json::to_vec(&block).unwrap();
        let _: RichBlock = serde_json::from_slice(&bytes).unwrap();
    }
    let _ = serde_json::from_slice::<RichMessage>(data);
});
