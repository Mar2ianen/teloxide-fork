//! Semantic Telegram Rich Text rendering.
//!
//! The `time-rendering` feature exposes time normalization and the typed time
//! API. The `rich-text` feature additionally exposes the semantic Rich Text
//! pipeline: HTML, developer Markdown and LLM Markdown are parsed into the
//! same [`RichNode`] model and rendered with shared bindings and policies.

mod error;
mod model;
mod normalize;
mod token;

pub use error::{RenderError, TimeError, TimeZoneError};
pub use model::{DateTimeFormat, SignedTimeSpan, TimeBindings, TimeExpression, TimeValue};
pub use normalize::{NormalizedDateTime, TimeContext};
pub use token::DateTimeToken;

#[cfg(feature = "rich-text")]
mod bindings;
#[cfg(feature = "rich-text")]
mod markdown;
#[cfg(feature = "rich-text")]
mod policy;

#[cfg(feature = "rich-text")]
pub use bindings::{CustomEmojiBinding, InvalidAlias, RichTextBindings};
#[cfg(feature = "rich-text")]
pub use markdown::{
    HtmlFormatter, LlmMarkdownFormatter, MainMarkdownFormatter, ParsedHtml, ParsedLlmMarkdown,
    ParsedMainMarkdown, RenderedMessage, StandardMarkdownFormatter,
};
#[cfg(feature = "rich-text")]
pub use model::{classify_link_target, DateTimeNode, LinkTarget, RichNode};
#[cfg(feature = "rich-text")]
pub use policy::{
    ExtensionKind, InvalidTimePolicy, LiteralLinkPolicy, MarkdownDiagnostic, RichTextDiagnostic,
    RichTextPolicies, RichTextRenderContext, UnknownCustomEmojiPolicy, UnknownLinkAliasPolicy,
};

/// The version of the explicit developer-facing dialect.
pub const MAIN_DIALECT_VERSION: &str = "main-v1";

/// The version of the model-facing dialect.
pub const LLM_DIALECT_VERSION: &str = "llm-v1";

/// Explicit revision of the renderer implementation used in audit records.
/// Update this whenever the parser, normalizer, or transport representation
/// changes in a way that affects rendered Telegram payloads.
pub const TIME_RENDERER_VERSION: &str = "time-rendering-v2";
