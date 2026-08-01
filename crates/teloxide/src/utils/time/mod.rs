//! Explicit Telegram time rendering.
//!
//! This module is enabled by the `time-rendering` feature. It keeps the
//! semantic time model and the Telegram transport representation together,
//! while leaving ordinary Markdown and HTML rendering unchanged.

mod error;
mod markdown;
mod model;
mod normalize;
mod token;

pub use error::{RenderError, TimeError, TimeZoneError};
pub use markdown::{
    LlmMarkdownFormatter, MainMarkdownFormatter, ParsedLlmMarkdown, ParsedMainMarkdown,
    RenderedMessage,
};
pub use model::{
    DateTimeFormat, DateTimeNode, RichNode, SignedTimeSpan, TimeBindings, TimeExpression, TimeValue,
};
pub use normalize::{NormalizedDateTime, TimeContext};
pub use token::DateTimeToken;

/// The version of the explicit developer-facing dialect.
pub const MAIN_DIALECT_VERSION: &str = "main-v1";

/// The version of the model-facing dialect.
pub const LLM_DIALECT_VERSION: &str = "llm-v1";

/// Explicit revision of the renderer implementation used in audit records.
/// Update this whenever the parser, normalizer, or transport representation
/// changes in a way that affects rendered Telegram payloads.
pub const TIME_RENDERER_VERSION: &str = "time-rendering-v2";
