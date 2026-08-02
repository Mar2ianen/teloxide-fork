#![cfg(feature = "time-rendering")]

use teloxide::utils::time::{
    LlmMarkdownFormatter, MainMarkdownFormatter, ParsedLlmMarkdown, ParsedMainMarkdown,
    RenderedMessage,
};

#[test]
fn legacy_time_rendering_imports_remain_available() {
    let _: LlmMarkdownFormatter = LlmMarkdownFormatter::new();
    let _: MainMarkdownFormatter = MainMarkdownFormatter::new();
    let _: Option<ParsedLlmMarkdown> = None;
    let _: Option<ParsedMainMarkdown> = None;
    let _: Option<RenderedMessage> = None;
}
