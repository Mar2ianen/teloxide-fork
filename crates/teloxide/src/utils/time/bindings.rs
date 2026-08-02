use std::collections::HashMap;

use teloxide_core::types::CustomEmojiId;
use url::Url;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomEmojiBinding {
    pub custom_emoji_id: CustomEmojiId,
    pub fallback: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidAlias {
    #[error("invalid {kind} alias `{alias}`: {reason}")]
    Invalid { alias: String, kind: &'static str, reason: &'static str },
}

#[derive(Clone, Debug, Default)]
pub struct RichTextBindings {
    links: HashMap<String, Url>,
    custom_emojis: HashMap<String, CustomEmojiBinding>,
}

impl RichTextBindings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn link(
        mut self,
        alias: impl Into<String>,
        url: impl Into<Url>,
    ) -> Result<Self, InvalidAlias> {
        self.insert_link(alias, url)?;
        Ok(self)
    }

    pub fn custom_emoji(
        mut self,
        alias: impl Into<String>,
        emoji: CustomEmojiBinding,
    ) -> Result<Self, InvalidAlias> {
        self.insert_custom_emoji(alias, emoji)?;
        Ok(self)
    }

    pub fn insert_link(
        &mut self,
        alias: impl Into<String>,
        url: impl Into<Url>,
    ) -> Result<Option<Url>, InvalidAlias> {
        let alias = alias.into();
        validate_alias(&alias, "link")?;
        Ok(self.links.insert(alias, url.into()))
    }

    pub fn insert_custom_emoji(
        &mut self,
        alias: impl Into<String>,
        emoji: CustomEmojiBinding,
    ) -> Result<Option<CustomEmojiBinding>, InvalidAlias> {
        let alias = alias.into().to_ascii_lowercase();
        validate_emoji_alias(&alias)?;
        if emoji.fallback.is_empty() {
            return Err(InvalidAlias::Invalid {
                alias,
                kind: "custom emoji",
                reason: "fallback must not be empty",
            });
        }
        Ok(self.custom_emojis.insert(alias, emoji))
    }

    pub fn link_value(&self, alias: &str) -> Option<&Url> {
        self.links.get(alias)
    }

    pub fn custom_emoji_value(&self, alias: &str) -> Option<&CustomEmojiBinding> {
        self.custom_emojis.get(&alias.to_ascii_lowercase())
    }

    pub fn links(&self) -> &HashMap<String, Url> {
        &self.links
    }

    pub fn custom_emojis(&self) -> &HashMap<String, CustomEmojiBinding> {
        &self.custom_emojis
    }
}

fn validate_alias(alias: &str, kind: &'static str) -> Result<(), InvalidAlias> {
    if alias.is_empty() {
        return Err(InvalidAlias::Invalid {
            alias: alias.to_owned(),
            kind,
            reason: "alias is empty",
        });
    }
    if alias.len() > 64 {
        return Err(InvalidAlias::Invalid {
            alias: alias.to_owned(),
            kind,
            reason: "alias is longer than 64 bytes",
        });
    }
    if !alias.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')) {
        return Err(InvalidAlias::Invalid {
            alias: alias.to_owned(),
            kind,
            reason: "only ASCII letters, digits, `_` and `-` are allowed",
        });
    }
    Ok(())
}

fn validate_emoji_alias(alias: &str) -> Result<(), InvalidAlias> {
    validate_alias(alias, "custom emoji")?;
    if !alias.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(InvalidAlias::Invalid {
            alias: alias.to_owned(),
            kind: "custom emoji",
            reason: "only lowercase ASCII letters, digits and `_` are allowed",
        });
    }
    Ok(())
}
