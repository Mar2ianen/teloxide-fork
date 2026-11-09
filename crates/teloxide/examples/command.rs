use teloxide::{prelude::*, utils::command::BotCommands};

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    log::info!("Starting command bot...");

    let bot = Bot::from_env();

    Command::repl(bot, answer).await;
}

/// These commands are supported:
#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase")]
enum Command {
    /// Display this text.
    #[command(aliases = ["h", "?"])]
    Help,
    /// Handle a username.
    #[command(alias = "u")]
    Username(String),
    /// Handle a username and an age.
    #[command(parse_with = "split", alias = "ua", hide_aliases)]
    UsernameAndAge { username: String, age: u8 },
}

async fn answer(bot: Bot, msg: Message, cmd: Command) -> ResponseResult<()> {
    match cmd {
        Command::Help => {
            bot.send_message(Command::descriptions().to_string()).chat_id(msg.chat.id).await?
        }
        Command::Username(username) => {
            bot.send_message(format!("Your username is @{username}.")).chat_id(msg.chat.id).await?
        }
        Command::UsernameAndAge { username, age } => {
            bot.send_message(format!("Your username is @{username} and age is {age}."))
                .chat_id(msg.chat.id)
                .await?
        }
    };

    Ok(())
}
