#![cfg(feature = "macros")]

use teloxide::utils::command::{BotCommands, ParseError};

#[derive(BotCommands, Debug, PartialEq)]
#[command(rename_rule = "lowercase")]
enum Command {
    #[command(alias = "h")]
    Help,
}

#[test]
fn alias_for_another_bot_is_rejected() {
    let error = Command::parse("/h@other_bot", "this_bot").unwrap_err();
    match error {
        ParseError::WrongBotName(name) => assert_eq!(name, "other_bot"),
        other => panic!("expected WrongBotName, got {other:?}"),
    }
}

#[test]
fn alias_for_this_bot_is_accepted() {
    assert_eq!(Command::parse("/h@this_bot", "this_bot").unwrap(), Command::Help);
}
