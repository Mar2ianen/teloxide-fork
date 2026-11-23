#![no_main]

use libfuzzer_sys::fuzz_target;
use teloxide_core::types::{CallbackQuery, Message, Update, WriteAccessAllowed};

fuzz_target!(|data: &[u8]| {
    // All Bot API data is untrusted. Invalid JSON must produce an error,
    // never an unwind or out-of-bounds access.
    let _ = serde_json::from_slice::<Update>(data);
    let _ = serde_json::from_slice::<Message>(data);
    let _ = serde_json::from_slice::<CallbackQuery>(data);
    let _ = serde_json::from_slice::<WriteAccessAllowed>(data);
});
