//! Minimal native-draft lifecycle.
//!
//! Run with `TELOXIDE_TOKEN=... TELOXIDE_USER_ID=... \
//! cargo run -p teloxide --features drafter --example drafter`.

use teloxide::{
    drafter::{DraftConfig, InProcessRateLimiter, TelegramDrafter},
    types::UserId,
    Bot,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bot = Bot::from_env();
    let limiter = InProcessRateLimiter::default();
    let user_id = std::env::var("TELOXIDE_USER_ID")?.parse::<u64>()?;
    let (drafter, sink) =
        TelegramDrafter::native_text(bot, UserId(user_id), DraftConfig::default(), limiter)?;

    sink.update("Generating…".to_owned())?;
    drafter.flush().await?;
    sink.update("The final answer is ready".to_owned())?;
    drafter.flush().await?;
    let _ = drafter.finish("The final answer is ready".to_owned()).await?;
    Ok(())
}
