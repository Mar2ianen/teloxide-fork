//! Semantic Telegram Rich Text rendering.
//!
//! The `time-rendering` feature enables this module for compatibility. The
//! `rich-text` feature is the explicit opt-in for the complete pipeline:
//! HTML, developer Markdown and LLM Markdown are parsed into the same
//! [`RichNode`] model and rendered with shared bindings and policies.
//!
//! [`StandardMarkdownFormatter`] is the opt-in path that keeps the source
//! unchanged and does not enable semantic extensions.

mod bindings;
mod error;
mod markdown;
mod model;
mod normalize;
mod policy;
mod token;

pub use bindings::{CustomEmojiBinding, InvalidAlias, RichTextBindings};
pub use error::{RenderError, TimeError, TimeZoneError};
pub use markdown::{
    HtmlFormatter, LlmMarkdownFormatter, MainMarkdownFormatter, ParsedHtml, ParsedLlmMarkdown,
    ParsedMainMarkdown, RenderedMessage, StandardMarkdownFormatter,
};
pub use model::{
    classify_link_target, DateTimeFormat, DateTimeNode, LinkTarget, RichNode, SignedTimeSpan,
    TimeBindings, TimeExpression, TimeValue,
};
pub use normalize::{NormalizedDateTime, TimeContext};
pub use policy::{
    ExtensionKind, InvalidTimePolicy, MarkdownDiagnostic, RichTextDiagnostic, RichTextPolicies,
    RichTextRenderContext, UnknownCustomEmojiPolicy, UnknownLinkAliasPolicy,
};
pub use token::DateTimeToken;

/// The version of the explicit developer-facing dialect.
pub const MAIN_DIALECT_VERSION: &str = "main-v1";

/// The version of the model-facing dialect.
pub const LLM_DIALECT_VERSION: &str = "llm-v1";

/// Explicit revision of the renderer implementation used in audit records.
/// Update this whenever the parser, normalizer, or transport representation
/// changes in a way that affects rendered Telegram payloads.
pub const TIME_RENDERER_VERSION: &str = "time-rendering-v2";
