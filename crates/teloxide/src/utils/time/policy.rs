#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnknownLinkAliasPolicy {
    KeepLabel,
    KeepLiteralMarkdown,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnknownCustomEmojiPolicy {
    KeepLiteral,
    UseFallback(String),
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidTimePolicy {
    KeepLiteral,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RichTextPolicies {
    pub unknown_link_alias: UnknownLinkAliasPolicy,
    pub unknown_custom_emoji: UnknownCustomEmojiPolicy,
    pub invalid_time: InvalidTimePolicy,
}

impl RichTextPolicies {
    pub fn developer() -> Self {
        Self {
            unknown_link_alias: UnknownLinkAliasPolicy::Error,
            unknown_custom_emoji: UnknownCustomEmojiPolicy::Error,
            invalid_time: InvalidTimePolicy::Error,
        }
    }

    pub fn llm() -> Self {
        Self::default()
    }
}

impl Default for RichTextPolicies {
    fn default() -> Self {
        Self {
            unknown_link_alias: UnknownLinkAliasPolicy::KeepLabel,
            unknown_custom_emoji: UnknownCustomEmojiPolicy::KeepLiteral,
            invalid_time: InvalidTimePolicy::KeepLiteral,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtensionKind {
    Link,
    CustomEmoji,
    Time,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RichTextDiagnostic {
    UnknownLinkAlias { alias: String },
    UnknownCustomEmojiAlias { alias: String },
    InvalidLiteralUrl { destination: String },
    InvalidTimeToken { token: String },
    UnterminatedExtension { kind: ExtensionKind },
}

pub type MarkdownDiagnostic = RichTextDiagnostic;

pub struct RichTextRenderContext<'a> {
    pub time: &'a super::TimeContext,
    pub bindings: &'a super::RichTextBindings,
    pub policies: RichTextPolicies,
}

impl<'a> RichTextRenderContext<'a> {
    pub fn new(time: &'a super::TimeContext, bindings: &'a super::RichTextBindings) -> Self {
        Self { time, bindings, policies: RichTextPolicies::default() }
    }

    pub fn with_policies(mut self, policies: RichTextPolicies) -> Self {
        self.policies = policies;
        self
    }
}
