//! Read-only runtime checks for the bot's own capabilities.
//!
//! Telegram settings are not inherently valid or invalid: a mismatch exists
//! only when the application explicitly declares what it needs. The checks
//! never call \`setMySettings\` and do not silently modify the account.
//!
//! When using \`CacheMe\`, pass its underlying requester to \`check_bot\` to
//! fetch fresh information rather than a potentially stale cached snapshot.

use crate::{
    requests::{Request, Requester},
    types::{BotFullInfo, UserId},
};

/// Capabilities expected by an application. Unspecified fields are not checked.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExpectedBotCapabilities {
    pub id: Option<UserId>,
    pub username: Option<String>,
    pub can_join_groups: Option<bool>,
    pub can_read_all_group_messages: Option<bool>,
    pub supports_inline_queries: Option<bool>,
    pub needs_inline_query_location: Option<bool>,
    pub can_connect_to_business: Option<bool>,
    pub supports_guest_queries: Option<bool>,
    pub supports_join_request_queries: Option<bool>,
    pub has_topics_enabled: Option<bool>,
    pub allows_users_to_create_topics: Option<bool>,
    pub has_topic_sidebar: Option<bool>,
    pub can_manage_bots: Option<bool>,
    pub has_main_web_app: Option<bool>,
}

/// An observable disagreement between bot identity/settings and requirements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BotSelfCheckIssue {
    NotABot,
    IdMismatch { expected: UserId, actual: UserId },
    UsernameMismatch { expected: String, actual: Option<String> },
    CapabilityMismatch { field: &'static str, expected: bool, actual: bool },
}

/// Snapshot and findings from checking a bot.
#[derive(Clone, Debug, PartialEq)]
pub struct BotSelfCheckReport {
    pub info: BotFullInfo,
    pub issues: Vec<BotSelfCheckIssue>,
}

impl BotSelfCheckReport {
    /// True when every explicitly requested capability matches.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.issues.is_empty()
    }
}

/// Check an already obtained \`getMe\` response with no network activity.
#[must_use]
pub fn inspect_bot(info: BotFullInfo, expected: &ExpectedBotCapabilities) -> BotSelfCheckReport {
    let mut issues = Vec::new();
    if !info.user.is_bot {
        issues.push(BotSelfCheckIssue::NotABot);
    }

    if let Some(wanted) = expected.id {
        if info.user.id != wanted {
            issues.push(BotSelfCheckIssue::IdMismatch {
                expected: wanted,
                actual: info.user.id,
            });
        }
    }

    if let Some(wanted) = &expected.username {
        if info.user.username.as_deref() != Some(wanted.as_str()) {
            issues.push(BotSelfCheckIssue::UsernameMismatch {
                expected: wanted.clone(),
                actual: info.user.username.clone(),
            });
        }
    }

    macro_rules! check_flag {
        ($field:ident) => {
            if let Some(wanted) = expected.$field {
                if info.$field != wanted {
                    issues.push(BotSelfCheckIssue::CapabilityMismatch {
                        field: stringify!($field),
                        expected: wanted,
                        actual: info.$field,
                    });
                }
            }
        };
    }

    check_flag!(can_join_groups);
    check_flag!(can_read_all_group_messages);
    check_flag!(supports_inline_queries);
    check_flag!(needs_inline_query_location);
    check_flag!(can_connect_to_business);
    check_flag!(supports_guest_queries);
    check_flag!(supports_join_request_queries);
    check_flag!(has_topics_enabled);
    check_flag!(allows_users_to_create_topics);
    check_flag!(has_topic_sidebar);
    check_flag!(can_manage_bots);
    check_flag!(has_main_web_app);

    BotSelfCheckReport { info, issues }
}

/// Fetch \`getMe\` and check the requirements without changing Telegram settings.
///
/// A \`CacheMe\` requester can return stale values after \`setMySettings\`.
/// Call \`check_bot(cache.inner(), ...)\` to bypass that cache when freshness
/// matters. Transport failures are returned unchanged.
pub async fn check_bot<B: Requester>(
    bot: &B,
    expected: &ExpectedBotCapabilities,
) -> Result<BotSelfCheckReport, B::Err> {
    let info = bot.get_me().send().await?;
    Ok(inspect_bot(info, expected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn info() -> BotFullInfo {
        serde_json::from_value(json!({
            "id": 42,
            "is_bot": true,
            "first_name": "Bot",
            "username": "example_bot",
            "can_join_groups": true,
            "can_read_all_group_messages": false,
            "supports_inline_queries": true,
            "needs_inline_query_location": false,
            "can_connect_to_business": false,
            "supports_guest_queries": true,
            "supports_join_request_queries": false,
            "has_topics_enabled": true,
            "allows_users_to_create_topics": false,
            "has_topic_sidebar": true,
            "can_manage_bots": false,
            "has_main_web_app": false
        }))
        .unwrap()
    }

    #[test]
    fn unspecified_capabilities_are_never_treated_as_errors() {
        assert!(inspect_bot(info(), &ExpectedBotCapabilities::default()).is_healthy());
    }

    #[test]
    fn requirements_check_id_username_and_capabilities() {
        let expected = ExpectedBotCapabilities {
            id: Some(UserId(43)),
            username: Some("another_bot".into()),
            supports_inline_queries: Some(false),
            has_topic_sidebar: Some(false),
            can_join_groups: Some(true),
            ..ExpectedBotCapabilities::default()
        };
        let result = inspect_bot(info(), &expected);
        assert_eq!(result.issues.len(), 4);
        assert!(result.issues.contains(&BotSelfCheckIssue::IdMismatch {
            expected: UserId(43),
            actual: UserId(42),
        }));
        assert!(result.issues.contains(&BotSelfCheckIssue::UsernameMismatch {
            expected: "another_bot".into(),
            actual: Some("example_bot".into()),
        }));
    }

    #[test]
    fn absent_username_is_safe_and_non_bot_detected() {
        let mut bot = info();
        bot.user.is_bot = false;
        bot.user.username = None;
        let expected = ExpectedBotCapabilities {
            username: Some("example_bot".into()),
            ..ExpectedBotCapabilities::default()
        };
        let report = inspect_bot(bot, &expected);
        assert_eq!(report.issues.len(), 2);
        assert!(report.issues.contains(&BotSelfCheckIssue::NotABot));
    }
}
