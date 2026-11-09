//! Send `/widget` in a private chat with the bot, then tap Refresh.
//! Widgets in groups, forwards and inline results currently require Telegram's
//! test environment. The HTML can also be checked with widget-sandbox's Node
//! harness.
use teloxide::{
    prelude::*,
    types::{SharedWidgetQuery, WidgetQuery, WidgetQueryType},
};

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let handler = dptree::entry()
        .branch(Update::filter_message().endpoint(send_widget))
        .branch(Update::filter_widget_query().endpoint(answer_widget))
        .branch(Update::filter_shared_widget_query().endpoint(answer_shared_widget));
    Dispatcher::builder(Bot::from_env(), handler).enable_ctrlc_handler().build().dispatch().await;
}

async fn send_widget(bot: Bot, message: Message) -> ResponseResult<()> {
    if message.chat.is_private() && message.text() == Some("/widget") {
        bot.send_widget(message.chat.id, include_str!("widgets/status.html"))
            .allow_queries(true)
            .caption("Native HTML widget; no external assets or network access")
            .await?;
    }
    Ok(())
}

async fn answer_widget(bot: Bot, query: WidgetQuery) -> ResponseResult<()> {
    // Answer promptly: Telegram requires an answer within 15 seconds. Signed
    // wallet data and transaction notifications need their own application logic.
    let answer = if matches!(query.r#type, WidgetQueryType::Data(_)) && query.data == "refresh" {
        "Fresh data from the bot"
    } else {
        "Unsupported request"
    };
    bot.answer_widget_query(query.id).data(answer).await?;
    Ok(())
}

async fn answer_shared_widget(bot: Bot, query: SharedWidgetQuery) -> ResponseResult<()> {
    // A forwarded shared widget has no source chat/message to edit. Its answer
    // goes only to the requesting widget; shared state has a separate edit method.
    bot.answer_widget_query(query.id).data("This is a forwarded shared widget").await?;
    Ok(())
}
