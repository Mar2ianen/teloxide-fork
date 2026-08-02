# Semantic Rich Text

`rich-text` is an opt-in frontend and renderer layer for Telegram rich
messages. It has one semantic model and three explicit input dialects:

```text
HTML                  <tg-time value="15:00" />
Developer Markdown    @time(15:00)
LLM Markdown          15:::00/
                              ↓
                         RichNode
                              ↓
                    Telegram + fallback text
```

The same `RichTextRenderContext` supplies the timezone, trusted link
bindings, custom-emoji bindings and error policies to every frontend.

```rust
use teloxide::utils::time::{
    CustomEmojiBinding, LlmMarkdownFormatter, RichTextBindings,
    RichTextRenderContext, TimeBindings, TimeContext,
};
use teloxide::types::CustomEmojiId;
use url::Url;

let time = TimeContext::from_name("Europe/Moscow")?;
let time_bindings = TimeBindings::default();
let mut bindings = RichTextBindings::new();
bindings.insert_link("source_1", Url::parse("https://example.com")?)?;
bindings.insert_custom_emoji(
    "party",
    CustomEmojiBinding {
        custom_emoji_id: CustomEmojiId("123".into()),
        fallback: "🎉".into(),
    },
)?;
let context = RichTextRenderContext::for_llm(&time, &time_bindings, &bindings);
let rendered = LlmMarkdownFormatter::new().render_with_context(
    "Релиз 15:::00/ [в источнике](source_1) :party:",
    &context,
)?;
```

`MainMarkdownFormatter` uses the strict developer policies by default.
`LlmMarkdownFormatter` keeps unknown aliases readable and reports them in
`RenderedMessage::diagnostics`. `HtmlFormatter` accepts `<tg-time>`,
`<tg-emoji>` and ordinary `<a href>` nodes. `StandardMarkdownFormatter`
explicitly leaves time markers, emoji aliases and link aliases untouched.

Link destinations are classified consistently: a URI scheme or a dot means a
literal URI/URL; otherwise the value is looked up in `RichTextBindings`. The
application, not the model, owns the final URL and custom emoji ID.

Parsed frontends expose `known_extension_end_points()`. These are parser
landmarks after completed extensions, not guarantees that a message can be
segmented there: Markdown emphasis, HTML containers and other syntax may span
an arbitrary landmark. An unfinished time marker, emoji alias, link or HTML
semantic tag is retained as pending input.

The old `MainMarkdownFormatter` and `LlmMarkdownFormatter` time-only methods
remain available as compatibility wrappers. New code should pass one explicit
`RichTextRenderContext` (including `TimeBindings`) and one captured `Timestamp`
through the complete render call. `RichTextRenderContext::for_developer` uses
strict policies; `for_llm` uses readable fallback policies.
