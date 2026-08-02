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
pub enum LiteralLinkPolicy {
    /// Allow only URI schemes accepted by the semantic Telegram frontend.
    TelegramSafeSchemes,
    /// Allow any syntactically valid URI scheme.
    AnyUri,
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
    pub literal_link: LiteralLinkPolicy,
}

impl RichTextPolicies {
    pub fn developer() -> Self {
        Self {
            unknown_link_alias: UnknownLinkAliasPolicy::Error,
            unknown_custom_emoji: UnknownCustomEmojiPolicy::Error,
            invalid_time: InvalidTimePolicy::Error,
            literal_link: LiteralLinkPolicy::TelegramSafeSchemes,
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
            literal_link: LiteralLinkPolicy::TelegramSafeSchemes,
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
    pub time_bindings: &'a super::TimeBindings,
    pub bindings: &'a super::RichTextBindings,
    pub policies: RichTextPolicies,
}

impl<'a> RichTextRenderContext<'a> {
    pub fn new(
        time: &'a super::TimeContext,
        time_bindings: &'a super::TimeBindings,
        bindings: &'a super::RichTextBindings,
    ) -> Self {
        Self { time, time_bindings, bindings, policies: RichTextPolicies::developer() }
    }

    pub fn for_llm(
        time: &'a super::TimeContext,
        time_bindings: &'a super::TimeBindings,
        bindings: &'a super::RichTextBindings,
    ) -> Self {
        Self { time, time_bindings, bindings, policies: RichTextPolicies::llm() }
    }

    pub fn for_developer(
        time: &'a super::TimeContext,
        time_bindings: &'a super::TimeBindings,
        bindings: &'a super::RichTextBindings,
    ) -> Self {
        Self::new(time, time_bindings, bindings)
    }

    pub fn with_policies(mut self, policies: RichTextPolicies) -> Self {
        self.policies = policies;
        self
    }
}
