//! Minimal native-draft lifecycle.
//!
//! Run with `TELOXIDE_TOKEN=... cargo run --example drafter -p teloxide`.

use teloxide::{
    drafter::{DraftConfig, InProcessRateLimiter, TelegramDrafter},
    types::UserId,
    Bot,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bot = Bot::from_env();
    let limiter = InProcessRateLimiter::default();
    let (drafter, sink) =
        TelegramDrafter::native_text(bot, UserId(123), DraftConfig::default(), limiter)?;

    sink.update("Generating…".to_owned())?;
    sink.update("The final answer is ready".to_owned())?;
    let _ = drafter.finish("The final answer is ready".to_owned()).await?;
    Ok(())
}
