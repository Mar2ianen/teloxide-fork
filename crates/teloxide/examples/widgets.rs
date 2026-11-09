//! Send `/widget` in a private chat with the bot, then tap Refresh.
//! Widgets in groups, forwards and inline results currently require Telegram's
//! test environment. The HTML can also be checked with widget-sandbox's Node
//! harness.
use std::time::Duration;

use teloxide::{
    outbound::{Outbound, OutboundQueue, OutboundSettings},
    prelude::*,
    types::{SharedWidgetQuery, WidgetQuery, WidgetQueryType},
};

type WidgetBot = Outbound<Bot>;
type HandlerResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> HandlerResult {
    pretty_env_logger::init();
    let handler = dptree::entry()
        .branch(Update::filter_message().endpoint(send_widget))
        .branch(Update::filter_widget_query().endpoint(answer_widget))
        .branch(Update::filter_shared_widget_query().endpoint(answer_shared_widget));
    let queue = OutboundQueue::new_spawn(OutboundSettings::default())?;
    Dispatcher::builder(Bot::from_env().outbound(queue), handler)
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
    Ok(())
}

async fn send_widget(bot: WidgetBot, message: Message) -> HandlerResult {
    if message.chat.is_private() && message.text() == Some("/widget") {
        bot.send_widget(message.chat.id, include_str!("widgets/status.html"))
            .allow_queries(true)
            .caption("Native HTML widget; no external assets or network access")
            .await?;
    }
    Ok(())
}

async fn answer_widget(bot: WidgetBot, query: WidgetQuery) -> HandlerResult {
    // Capture the budget before application work; include queue wait and HTTP.
    // This local 14-second budget cannot recover time spent delivering the update.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(14);
    // Answer promptly: Telegram requires an answer within 15 seconds. Signed
    // wallet data and transaction notifications need their own application logic.
    let answer = if matches!(query.r#type, WidgetQueryType::Data(_)) && query.data == "refresh" {
        "Fresh data from the bot"
    } else {
        "Unsupported request"
    };
    tokio::time::timeout_at(deadline, bot.answer_widget_query(query.id).data(answer)).await??;
    Ok(())
}

async fn answer_shared_widget(bot: WidgetBot, query: SharedWidgetQuery) -> HandlerResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(14);
    // A forwarded shared widget has no source chat/message to edit. Its answer
    // goes only to the requesting widget; shared state has a separate edit method.
    tokio::time::timeout_at(
        deadline,
        bot.answer_widget_query(query.id).data("This is a forwarded shared widget"),
    )
    .await??;
    Ok(())
}
