//! Semantic Rich Text frontends and shared rendering context.
//!
//! The implementation is shared with the compatibility `utils::time` path,
//! but this module is the canonical public namespace for the general pipeline.

pub use super::time::{
    classify_link_target, CustomEmojiBinding, DateTimeNode, ExtensionKind, HtmlFormatter,
    InvalidAlias, InvalidTimePolicy, LiteralLinkPolicy, LlmMarkdownFormatter,
    MainMarkdownFormatter, MarkdownDiagnostic, ParsedHtml, ParsedLlmMarkdown, ParsedMainMarkdown,
    RenderedMessage, RichNode, RichTextBindings, RichTextDiagnostic, RichTextPolicies,
    RichTextRenderContext, StandardMarkdownFormatter, UnknownCustomEmojiPolicy,
    UnknownLinkAliasPolicy, LLM_DIALECT_VERSION, MAIN_DIALECT_VERSION,
};

/// Exact revision identifier for the complete semantic Rich Text renderer.
pub const RICH_TEXT_RENDERER_VERSION: &str = super::time::RICH_TEXT_RENDERER_VERSION;
