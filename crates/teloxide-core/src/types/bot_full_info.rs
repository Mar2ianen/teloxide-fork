use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::types::User;

/// Full information about the bot itself, returned by [`GetMe`].
///
/// [`GetMe`]: crate::payloads::GetMe
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct BotFullInfo {
    #[serde(flatten)]
    pub user: User,

    /// `true`, if the bot can be invited to groups.
    pub can_join_groups: bool,

    /// `true`, if [privacy mode] is disabled for the bot.
    ///
    /// [privacy mode]: https://core.telegram.org/bots#privacy-mode
    pub can_read_all_group_messages: bool,

    /// `true`, if the bot supports inline queries.
    pub supports_inline_queries: bool,

    /// Active usernames of the bot, if it has any.
    pub active_usernames: Option<Vec<String>>,

    /// Placeholder shown in the input field after the bot's username for
    /// inline queries. Omitted when Telegram uses the default placeholder.
    pub inline_query_placeholder: Option<String>,

    /// `true`, if the bot requests the user's location with every inline query.
    pub needs_inline_query_location: bool,

    /// `true`, if the bot can be connected to a Telegram Business account to
    /// receive its messages.
    pub can_connect_to_business: bool,

    /// `true`, if the bot supports guest queries from chats it is not a member
    /// of.
    pub supports_guest_queries: bool,

    /// `true`, if the bot supports chat join request queries.
    pub supports_join_request_queries: bool,

    /// `true`, if the bot has forum topic mode enabled in private chats.
    pub has_topics_enabled: bool,

    /// `true`, if the bot allows users to create and delete topics in private
    /// chats.
    pub allows_users_to_create_topics: bool,

    /// `true`, if topics in private chats with the bot are shown in a sidebar.
    pub has_topic_sidebar: bool,

    /// `true`, if other bots can be created to be controlled by this bot.
    pub can_manage_bots: bool,

    /// `true`, if the bot has a main Web App.
    pub has_main_web_app: bool,
}

impl BotFullInfo {
    /// Returns the username of the bot.
    #[must_use]
    pub fn username(&self) -> &str {
        self.user.username.as_deref().expect("Bots must have usernames")
    }

    /// Returns the username if Telegram provided one; never panics.
    #[must_use]
    pub fn username_opt(&self) -> Option<&str> {
        self.user.username.as_deref()
    }

    /// Returns the mention if a username is available.
    #[must_use]
    pub fn mention_opt(&self) -> Option<String> {
        self.user.mention()
    }

    /// Returns a `t.me` URL only if a username is available.
    #[must_use]
    pub fn tme_url_opt(&self) -> Option<reqwest::Url> {
        self.user.tme_url()
    }

    /// Returns a username mention of this bot.
    #[must_use]
    pub fn mention(&self) -> String {
        format!("@{}", self.username())
    }

    /// Returns an URL that links to this bot in the form of `t.me/<...>`.
    #[must_use]
    pub fn tme_url(&self) -> reqwest::Url {
        format!("https://t.me/{}", self.username()).parse().unwrap()
    }
}

impl Deref for BotFullInfo {
    type Target = User;

    fn deref(&self) -> &Self::Target {
        &self.user
    }
}

/// Backwards-compatible name for [`BotFullInfo`].
pub type Me = BotFullInfo;

#[cfg(test)]
mod tests {
    use crate::types::{BotFullInfo, User, UserId};

    #[test]
    fn convenience_methods_work() {
        let me = BotFullInfo {
            user: User {
                id: UserId(42),
                is_bot: true,
                first_name: "First".to_owned(),
                last_name: None,
                username: Some("SomethingSomethingBot".to_owned()),
                language_code: None,
                is_premium: false,
                added_to_attachment_menu: false,
            },
            active_usernames: None,
            can_join_groups: false,
            can_read_all_group_messages: false,
            supports_inline_queries: false,
            inline_query_placeholder: None,
            needs_inline_query_location: false,
            can_connect_to_business: false,
            supports_guest_queries: false,
            supports_join_request_queries: false,
            has_topics_enabled: false,
            allows_users_to_create_topics: false,
            has_topic_sidebar: false,
            can_manage_bots: false,
            has_main_web_app: false,
        };

        assert_eq!(me.username(), "SomethingSomethingBot");
        assert_eq!(me.mention(), "@SomethingSomethingBot");
        assert_eq!(me.tme_url(), "https://t.me/SomethingSomethingBot".parse().unwrap());
    }
}
