use jiff::Timestamp;
use teloxide_core::types::InputRichMessage;
use url::Url;

use super::{
    classify_link_target, model::parse_expression, CustomEmojiBinding, DateTimeFormat,
    DateTimeNode, InvalidTimePolicy, LinkTarget, RenderError, RichNode, RichTextBindings,
    RichTextDiagnostic, RichTextPolicies, RichTextRenderContext, TimeBindings, TimeContext,
    TimeExpression,
};

#[derive(Clone)]
pub struct MainMarkdownFormatter {
    time: TimeContext,
}

#[derive(Clone)]
pub struct LlmMarkdownFormatter {
    time: TimeContext,
}

#[derive(Clone)]
pub struct HtmlFormatter {
    time: TimeContext,
}

/// Explicit Markdown transport without time, alias or custom-emoji semantics.
#[derive(Clone, Copy, Debug, Default)]
pub struct StandardMarkdownFormatter;

#[derive(Clone, Debug)]
pub struct ParsedMainMarkdown {
    source: String,
    nodes: Vec<RichNode>,
    safe_cut_points: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct ParsedLlmMarkdown {
    source: String,
    nodes: Vec<RichNode>,
    safe_cut_points: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct ParsedHtml {
    source: String,
    nodes: Vec<RichNode>,
    safe_cut_points: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct RenderedMessage {
    /// Compiled Markdown or HTML accepted by Telegram's rich-text parser.
    /// The field name is retained for compatibility with the time renderer.
    pub markdown: String,
    /// The same compiled payload under a frontend-neutral name.
    pub compiled: String,
    pub rich_message: InputRichMessage,
    pub fallback_text: String,
    pub captured_now: Timestamp,
    pub diagnostics: Vec<RichTextDiagnostic>,
    /// Byte offsets at which an incremental producer may safely cut input.
    pub safe_cut_points: Vec<usize>,
}

impl MainMarkdownFormatter {
    pub fn new(time: TimeContext) -> Self {
        Self { time }
    }

    pub fn time(&self) -> &TimeContext {
        &self.time
    }

    pub fn parse(&self, source: &str) -> Result<ParsedMainMarkdown, RenderError> {
        let scanned = scan_main(source)?;
        Ok(ParsedMainMarkdown {
            source: source.to_owned(),
            nodes: scanned.nodes,
            safe_cut_points: scanned.safe_cut_points,
        })
    }

    pub fn render(
        &self,
        source: &str,
        bindings: &TimeBindings,
    ) -> Result<RenderedMessage, RenderError> {
        self.render_at(source, bindings, Timestamp::now())
    }

    pub fn render_with_bindings(
        &self,
        source: &str,
        bindings: &TimeBindings,
    ) -> Result<RenderedMessage, RenderError> {
        self.render(source, bindings)
    }

    pub fn render_at(
        &self,
        source: &str,
        bindings: &TimeBindings,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        self.parse(source)?.render_at(&self.time, bindings, captured_now)
    }

    /// Renders the developer Markdown frontend with shared semantic bindings.
    pub fn render_with_context(
        &self,
        source: &str,
        context: &RichTextRenderContext<'_>,
    ) -> Result<RenderedMessage, RenderError> {
        self.render_with_context_at(source, context, Timestamp::now())
    }

    pub fn render_with_context_at(
        &self,
        source: &str,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        self.parse(source)?.render_with_context_at(context, captured_now)
    }
}

impl LlmMarkdownFormatter {
    pub fn new(time: TimeContext) -> Self {
        Self { time }
    }

    pub fn time(&self) -> &TimeContext {
        &self.time
    }

    pub fn parse(&self, source: &str) -> Result<ParsedLlmMarkdown, RenderError> {
        let scanned = scan_llm(source)?;
        Ok(ParsedLlmMarkdown {
            source: source.to_owned(),
            nodes: scanned.nodes,
            safe_cut_points: scanned.safe_cut_points,
        })
    }

    pub fn render(&self, source: &str) -> Result<RenderedMessage, RenderError> {
        self.render_at(source, Timestamp::now())
    }

    pub fn render_at(
        &self,
        source: &str,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let bindings = RichTextBindings::default();
        let policies = RichTextPolicies::llm();
        let time_bindings = TimeBindings::default();
        let parsed = self.parse(source)?;
        let options = RenderOptions {
            time: &self.time,
            time_bindings: &time_bindings,
            bindings: &bindings,
            policies: &policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(source, &parsed.nodes, &parsed.safe_cut_points, &options)
    }

    /// Renders the compact LLM Markdown frontend with application bindings.
    pub fn render_with_context(
        &self,
        source: &str,
        context: &RichTextRenderContext<'_>,
    ) -> Result<RenderedMessage, RenderError> {
        self.render_with_context_at(source, context, Timestamp::now())
    }

    pub fn render_with_context_at(
        &self,
        source: &str,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        self.parse(source)?.render_with_context_at(context, captured_now)
    }
}

impl HtmlFormatter {
    pub fn new(time: TimeContext) -> Self {
        Self { time }
    }

    pub fn time(&self) -> &TimeContext {
        &self.time
    }

    pub fn parse(&self, source: &str) -> Result<ParsedHtml, RenderError> {
        let scanned = scan_html(source)?;
        Ok(ParsedHtml {
            source: source.to_owned(),
            nodes: scanned.nodes,
            safe_cut_points: scanned.safe_cut_points,
        })
    }

    pub fn render(
        &self,
        source: &str,
        context: &RichTextRenderContext<'_>,
    ) -> Result<RenderedMessage, RenderError> {
        self.render_at(source, context, Timestamp::now())
    }

    pub fn render_at(
        &self,
        source: &str,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        self.parse(source)?.render_at(context, captured_now)
    }
}

impl StandardMarkdownFormatter {
    pub fn new() -> Self {
        Self
    }

    pub fn render(&self, source: &str) -> RenderedMessage {
        let captured_now = Timestamp::now();
        RenderedMessage {
            markdown: source.to_owned(),
            compiled: source.to_owned(),
            rich_message: InputRichMessage::markdown(source),
            fallback_text: source.to_owned(),
            captured_now,
            diagnostics: Vec::new(),
            safe_cut_points: vec![0, source.len()],
        }
    }
}

impl ParsedMainMarkdown {
    pub fn safe_cut_points(&self) -> &[usize] {
        &self.safe_cut_points
    }

    pub fn render_at(
        &self,
        time: &TimeContext,
        bindings: &TimeBindings,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let rich_bindings = RichTextBindings::default();
        let policies = RichTextPolicies::developer();
        let options = RenderOptions {
            time,
            time_bindings: bindings,
            bindings: &rich_bindings,
            policies: &policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.safe_cut_points, &options)
    }

    pub fn render_with_context_at(
        &self,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let time_bindings = TimeBindings::default();
        let options = RenderOptions {
            time: context.time,
            time_bindings: &time_bindings,
            bindings: context.bindings,
            policies: &context.policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.safe_cut_points, &options)
    }
}

impl ParsedLlmMarkdown {
    pub fn safe_cut_points(&self) -> &[usize] {
        &self.safe_cut_points
    }

    pub fn render_at(
        &self,
        time: &TimeContext,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let bindings = RichTextBindings::default();
        let policies = RichTextPolicies::llm();
        let time_bindings = TimeBindings::default();
        let options = RenderOptions {
            time,
            time_bindings: &time_bindings,
            bindings: &bindings,
            policies: &policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.safe_cut_points, &options)
    }

    pub fn render_with_context_at(
        &self,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let time_bindings = TimeBindings::default();
        let options = RenderOptions {
            time: context.time,
            time_bindings: &time_bindings,
            bindings: context.bindings,
            policies: &context.policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.safe_cut_points, &options)
    }
}

impl ParsedHtml {
    pub fn safe_cut_points(&self) -> &[usize] {
        &self.safe_cut_points
    }

    pub fn render_at(
        &self,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let time_bindings = TimeBindings::default();
        let options = RenderOptions {
            time: context.time,
            time_bindings: &time_bindings,
            bindings: context.bindings,
            policies: &context.policies,
            frontend: Frontend::Html,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.safe_cut_points, &options)
    }
}

#[derive(Clone, Copy)]
enum Frontend {
    Markdown,
    Html,
}

struct RenderOptions<'a> {
    time: &'a TimeContext,
    time_bindings: &'a TimeBindings,
    bindings: &'a RichTextBindings,
    policies: &'a RichTextPolicies,
    frontend: Frontend,
    captured_now: Timestamp,
}

struct RenderedFragment {
    compiled: String,
    fallback: String,
    diagnostics: Vec<RichTextDiagnostic>,
}

fn render_nodes(
    source: &str,
    nodes: &[RichNode],
    safe_cut_points: &[usize],
    options: &RenderOptions<'_>,
) -> Result<RenderedMessage, RenderError> {
    let fragment = render_fragment(source, nodes, options)?;
    let compiled = fragment.compiled;
    let rich_message = match options.frontend {
        Frontend::Markdown => InputRichMessage::markdown(compiled.clone()),
        Frontend::Html => InputRichMessage::html(compiled.clone()),
    };
    Ok(RenderedMessage {
        markdown: compiled.clone(),
        compiled,
        rich_message,
        fallback_text: fragment.fallback,
        captured_now: options.captured_now,
        diagnostics: fragment.diagnostics,
        safe_cut_points: safe_cut_points.to_vec(),
    })
}

fn render_fragment(
    source: &str,
    nodes: &[RichNode],
    options: &RenderOptions<'_>,
) -> Result<RenderedFragment, RenderError> {
    let mut result = RenderedFragment {
        compiled: String::new(),
        fallback: String::new(),
        diagnostics: Vec::new(),
    };
    for node in nodes {
        match node {
            RichNode::Text(text) => {
                result.compiled.push_str(text);
                result.fallback.push_str(text);
            }
            RichNode::DateTime(node) => {
                let normalized = match options.time.normalize(
                    &node.expression,
                    node.format,
                    options.captured_now,
                    options.time_bindings,
                ) {
                    Ok(normalized) => normalized,
                    Err(error) => {
                        if matches!(options.policies.invalid_time, InvalidTimePolicy::Error) {
                            return Err(RenderError::from_time_error(
                                source,
                                node.source_range.start,
                                error,
                            ));
                        }
                        let literal =
                            source.get(node.source_range.clone()).unwrap_or_default().to_owned();
                        result
                            .diagnostics
                            .push(RichTextDiagnostic::InvalidTimeToken { token: literal.clone() });
                        result.compiled.push_str(&literal);
                        result.fallback.push_str(&literal);
                        continue;
                    }
                };
                match options.frontend {
                    Frontend::Markdown => {
                        result.compiled.push_str("![");
                        result.compiled.push_str(&escape_markdown_label(&normalized.fallback_text));
                        result.compiled.push_str("](tg://time?unix=");
                        result.compiled.push_str(&normalized.unix_time.to_string());
                        result.compiled.push_str("&format=");
                        result.compiled.push_str(normalized.format.wire_value());
                        result.compiled.push(')');
                    }
                    Frontend::Html => {
                        result.compiled.push_str("<tg-time unix=\"");
                        result.compiled.push_str(&normalized.unix_time.to_string());
                        result.compiled.push_str("\" format=\"");
                        result.compiled.push_str(normalized.format.wire_value());
                        result.compiled.push_str("\">");
                        result.compiled.push_str(&escape_html(&normalized.fallback_text));
                        result.compiled.push_str("</tg-time>");
                    }
                }
                result.fallback.push_str(&normalized.fallback_text);
            }
            RichNode::Link { label, target } => {
                let label = render_fragment(source, label, options)?;
                result.diagnostics.extend(label.diagnostics);
                let resolution = resolve_link(target, options.bindings, options.policies, source)?;
                if let Some(diagnostic) = resolution.diagnostic {
                    result.diagnostics.push(diagnostic);
                }
                if let Some(destination) = resolution.destination.as_deref() {
                    match options.frontend {
                        Frontend::Markdown => {
                            result.compiled.push('[');
                            result.compiled.push_str(&label.compiled);
                            result.compiled.push_str("](");
                            result.compiled.push_str(&escape_markdown_destination(destination));
                            result.compiled.push(')');
                        }
                        Frontend::Html => {
                            result.compiled.push_str("<a href=\"");
                            result.compiled.push_str(&escape_html(destination));
                            result.compiled.push_str("\">");
                            result.compiled.push_str(&label.compiled);
                            result.compiled.push_str("</a>");
                        }
                    }
                } else if matches!(
                    options.policies.unknown_link_alias,
                    super::UnknownLinkAliasPolicy::KeepLiteralMarkdown
                ) {
                    match options.frontend {
                        Frontend::Markdown => {
                            result.compiled.push('[');
                            result.compiled.push_str(&label.compiled);
                            result.compiled.push_str("](");
                            if let LinkTarget::Alias(alias) = target {
                                result.compiled.push_str(&escape_markdown_destination(alias));
                            }
                            result.compiled.push(')');
                        }
                        Frontend::Html => {
                            result.compiled.push_str(&label.compiled);
                        }
                    }
                } else {
                    result.compiled.push_str(&label.compiled);
                }
                match (options.frontend, target, resolution.destination.as_deref()) {
                    (Frontend::Markdown, LinkTarget::Literal(_), Some(destination)) => {
                        result.fallback.push('[');
                        result.fallback.push_str(&label.fallback);
                        result.fallback.push_str("](");
                        result.fallback.push_str(destination);
                        result.fallback.push(')');
                    }
                    (Frontend::Html, LinkTarget::Literal(_), Some(_)) => {
                        result.fallback.push_str(&label.fallback);
                    }
                    _ => result.fallback.push_str(&label.fallback),
                }
            }
            RichNode::CustomEmoji { alias } => {
                let binding = options.bindings.custom_emoji_value(alias);
                match binding {
                    Some(CustomEmojiBinding { custom_emoji_id, fallback }) => {
                        match options.frontend {
                            Frontend::Markdown => {
                                result.compiled.push_str("![");
                                result.compiled.push_str(&escape_markdown_label(fallback));
                                result.compiled.push_str("](tg://emoji?id=");
                                result.compiled.push_str(&custom_emoji_id.0);
                                result.compiled.push(')');
                            }
                            Frontend::Html => {
                                result.compiled.push_str("<tg-emoji emoji-id=\"");
                                result.compiled.push_str(&escape_html(&custom_emoji_id.0));
                                result.compiled.push_str("\">");
                                result.compiled.push_str(&escape_html(fallback));
                                result.compiled.push_str("</tg-emoji>");
                            }
                        }
                        result.fallback.push_str(fallback);
                    }
                    None => {
                        result.diagnostics.push(RichTextDiagnostic::UnknownCustomEmojiAlias {
                            alias: alias.clone(),
                        });
                        match &options.policies.unknown_custom_emoji {
                            super::UnknownCustomEmojiPolicy::KeepLiteral => {
                                let literal = format!(":{alias}:");
                                result.compiled.push_str(&literal);
                                result.fallback.push_str(&literal);
                            }
                            super::UnknownCustomEmojiPolicy::UseFallback(fallback) => {
                                result.compiled.push_str(fallback);
                                result.fallback.push_str(fallback);
                            }
                            super::UnknownCustomEmojiPolicy::Error => {
                                return Err(RenderError::invalid(
                                    "rich-text",
                                    source,
                                    0,
                                    format!(":{alias}:"),
                                    "unknown custom emoji alias",
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(result)
}

struct LinkResolution {
    destination: Option<String>,
    diagnostic: Option<RichTextDiagnostic>,
}

fn resolve_link(
    target: &LinkTarget,
    bindings: &RichTextBindings,
    policies: &RichTextPolicies,
    source: &str,
) -> Result<LinkResolution, RenderError> {
    match target {
        LinkTarget::Literal(value) => {
            if value.is_empty() || value.chars().any(char::is_control) {
                return invalid_literal_link(
                    policies,
                    source,
                    value,
                    "literal link destination is invalid",
                );
            }
            if has_uri_scheme(value) && Url::parse(value).is_err() {
                return invalid_literal_link(policies, source, value, "literal URI is invalid");
            }
            Ok(LinkResolution { destination: Some(value.clone()), diagnostic: None })
        }
        LinkTarget::Alias(alias) => {
            if let Some(url) = bindings.link_value(alias) {
                return Ok(LinkResolution {
                    destination: Some(url.as_str().to_owned()),
                    diagnostic: None,
                });
            }
            if matches!(policies.unknown_link_alias, super::UnknownLinkAliasPolicy::Error) {
                return Err(RenderError::invalid(
                    "rich-text",
                    source,
                    0,
                    alias,
                    "unknown link alias",
                ));
            }
            Ok(LinkResolution {
                destination: None,
                diagnostic: Some(RichTextDiagnostic::UnknownLinkAlias { alias: alias.clone() }),
            })
        }
    }
}

fn invalid_literal_link(
    policies: &RichTextPolicies,
    source: &str,
    literal: &str,
    message: &str,
) -> Result<LinkResolution, RenderError> {
    if matches!(policies.unknown_link_alias, super::UnknownLinkAliasPolicy::Error) {
        Err(RenderError::invalid("rich-text", source, 0, literal, message))
    } else {
        Ok(LinkResolution {
            destination: None,
            diagnostic: Some(RichTextDiagnostic::InvalidLiteralUrl {
                destination: literal.to_owned(),
            }),
        })
    }
}

struct ScanResult {
    nodes: Vec<RichNode>,
    safe_cut_points: Vec<usize>,
}

enum MarkerScan {
    NoMatch,
    Parsed(DateTimeNode, usize),
    MalformedIntent(RenderError),
}

fn scan_main(source: &str) -> Result<ScanResult, RenderError> {
    scan(source, "main", scan_main_marker)
}

fn scan_llm(source: &str) -> Result<ScanResult, RenderError> {
    scan(source, "llm", scan_llm_marker)
}

fn scan(
    source: &str,
    dialect: &'static str,
    marker: impl Fn(&str, usize, &'static str) -> MarkerScan + Copy,
) -> Result<ScanResult, RenderError> {
    let mut nodes = Vec::new();
    let mut safe_cut_points = vec![0];
    let mut text_start = 0;
    let mut index = 0;
    while index < source.len() {
        if source[index..].starts_with("```") {
            index = skip_delimited(source, index, "```");
            continue;
        }
        if source.as_bytes()[index] == b'`' {
            index = skip_inline_code(source, index);
            continue;
        }
        if source.as_bytes()[index] == b'\\' {
            index = skip_escaped(source, index);
            continue;
        }
        if let Some(end) = skip_uri(source, index) {
            index = end;
            continue;
        }
        if source.as_bytes()[index] == b'[' {
            if let Some((label_end, destination_end, destination)) =
                parse_markdown_link(source, index)
            {
                let label_source = &source[index + 1..label_end];
                let label = scan(label_source, dialect, marker)?;
                push_text(&mut nodes, &source[text_start..index]);
                nodes.push(RichNode::Link {
                    label: label.nodes,
                    target: classify_link_target(&destination),
                });
                index = destination_end;
                text_start = index;
                safe_cut_points.push(index);
                continue;
            }
        }
        if source.as_bytes()[index] == b':' {
            if let Some((alias, end)) = parse_custom_emoji(source, index) {
                push_text(&mut nodes, &source[text_start..index]);
                nodes.push(RichNode::CustomEmoji { alias });
                index = end;
                text_start = end;
                safe_cut_points.push(end);
                continue;
            }
        }
        match marker(source, index, dialect) {
            MarkerScan::NoMatch => {}
            MarkerScan::Parsed(node, end) => {
                push_text(&mut nodes, &source[text_start..index]);
                nodes.push(RichNode::DateTime(node));
                index = end;
                text_start = end;
                safe_cut_points.push(end);
                continue;
            }
            MarkerScan::MalformedIntent(error) => return Err(error),
        }
        index = next_char_boundary(source, index);
    }
    push_text(&mut nodes, &source[text_start..]);
    safe_cut_points.push(safe_cut_end(source));
    safe_cut_points.sort_unstable();
    safe_cut_points.dedup();
    Ok(ScanResult { nodes, safe_cut_points })
}

fn scan_main_marker(source: &str, index: usize, dialect: &'static str) -> MarkerScan {
    if !is_boundary_before(source, index) {
        return MarkerScan::NoMatch;
    }
    let directives = [
        ("@time(", DateTimeFormat::Time),
        ("@date(", DateTimeFormat::Date),
        ("@datetime(", DateTimeFormat::DateTime),
        ("@relative(", DateTimeFormat::Relative),
    ];
    let Some((literal, format)) =
        directives.iter().find(|(literal, _)| source[index..].starts_with(literal))
    else {
        return MarkerScan::NoMatch;
    };
    let content_start = index + literal.len();
    let Some(close_offset) = source[content_start..].find(')') else {
        return MarkerScan::MalformedIntent(RenderError::invalid(
            dialect,
            source,
            index,
            &source[index..],
            "directive is missing a closing `)`",
        ));
    };
    let content_end = content_start + close_offset;
    let content = &source[content_start..content_end];
    let expression = match parse_expression(content) {
        Ok(expression) => expression,
        Err(message) => {
            return MarkerScan::MalformedIntent(RenderError::invalid(
                dialect,
                source,
                index,
                &source[index..=content_end],
                message,
            ));
        }
    };
    if matches!(format, DateTimeFormat::Relative)
        && !matches!(expression, TimeExpression::Now { .. } | TimeExpression::Variable { .. })
    {
        return MarkerScan::MalformedIntent(RenderError::invalid(
            dialect,
            source,
            index,
            &source[index..=content_end],
            "@relative accepts only `now` or a typed binding",
        ));
    }
    MarkerScan::Parsed(
        DateTimeNode { expression, format: *format, source_range: index..content_end + 1 },
        content_end + 1,
    )
}

fn scan_llm_marker(source: &str, index: usize, dialect: &'static str) -> MarkerScan {
    if !is_boundary_before(source, index) {
        return MarkerScan::NoMatch;
    }
    if source[index..].starts_with("now") {
        return parse_llm_now(source, index, dialect);
    }
    if !source.as_bytes()[index].is_ascii_digit() {
        return MarkerScan::NoMatch;
    }
    parse_llm_numeric(source, index, dialect)
}

fn parse_llm_now(source: &str, index: usize, dialect: &'static str) -> MarkerScan {
    let after_now = index + 3;
    let Some(next) = source.as_bytes().get(after_now).copied() else {
        return MarkerScan::NoMatch;
    };
    if next != b'/' && next != b'+' && next != b'-' {
        return MarkerScan::NoMatch;
    }
    let end = if next == b'/' {
        after_now + 1
    } else {
        let mut cursor = after_now + 1;
        let mut pairs = 0;
        loop {
            let digits_start = cursor;
            while source.as_bytes().get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
            if cursor == digits_start {
                if cursor >= source.len() {
                    return MarkerScan::NoMatch;
                }
                return malformed_llm(
                    source,
                    index,
                    cursor,
                    dialect,
                    "relative offset needs a number",
                );
            }
            let Some(unit) = source.as_bytes().get(cursor).copied() else {
                if cursor >= source.len() {
                    return MarkerScan::NoMatch;
                }
                return malformed_llm(
                    source,
                    index,
                    cursor,
                    dialect,
                    "relative marker is missing `/`",
                );
            };
            if !matches!(unit, b's' | b'm' | b'h' | b'd' | b'w') {
                return malformed_llm(
                    source,
                    index,
                    cursor + 1,
                    dialect,
                    "unknown relative offset unit",
                );
            }
            cursor += 1;
            pairs += 1;
            if source.as_bytes().get(cursor) == Some(&b'/') {
                break;
            }
            if cursor >= source.len() {
                return MarkerScan::NoMatch;
            }
            if pairs >= 16 {
                return malformed_llm(
                    source,
                    index,
                    cursor,
                    dialect,
                    "relative offset is too long",
                );
            }
        }
        cursor + 1
    };
    let literal = &source[index..end];
    let expression = match parse_expression(&literal[..literal.len() - 1]) {
        Ok(expression) => expression,
        Err(message) => return malformed_llm(source, index, end, dialect, &message),
    };
    MarkerScan::Parsed(
        DateTimeNode { expression, format: DateTimeFormat::Time, source_range: index..end },
        end,
    )
}

fn parse_llm_numeric(source: &str, index: usize, dialect: &'static str) -> MarkerScan {
    let bytes = source.as_bytes();
    let clock_prefix = bytes.get(index..index + 5).is_some_and(|part| {
        part[0].is_ascii_digit() && part[1].is_ascii_digit() && part[2..] == *b":::"
    });
    if clock_prefix {
        let end = index + 8;
        if end > source.len() {
            return MarkerScan::NoMatch;
        }
        if bytes
            .get(index + 5..index + 7)
            .is_none_or(|part| part.len() != 2 || !part.iter().all(u8::is_ascii_digit))
            || bytes.get(index + 7) != Some(&b'/')
        {
            return malformed_llm(source, index, end, dialect, "malformed clock marker");
        }
        let literal = &source[index..end];
        return parsed_llm_time(
            source,
            index,
            end,
            format!("{}:{}", &literal[..2], &literal[5..7]),
            dialect,
        );
    }

    let date_prefix = bytes.get(index..index + 10).is_some_and(|part| {
        part[0..4].iter().all(|byte| byte.is_ascii_digit())
            && part[4] == b'-'
            && part[5..7].iter().all(|byte| byte.is_ascii_digit())
            && part[7] == b'-'
            && part[8..10].iter().all(|byte| byte.is_ascii_digit())
    });
    if !date_prefix {
        return MarkerScan::NoMatch;
    }
    let Some(separator @ (b' ' | b'T')) = bytes.get(index + 10).copied() else {
        return MarkerScan::NoMatch;
    };
    let has_clock_prefix = bytes.get(index + 11..index + 16).is_some_and(|part| {
        part[0].is_ascii_digit() && part[1].is_ascii_digit() && part[2..] == *b":::"
    });
    if !has_clock_prefix {
        return MarkerScan::NoMatch;
    }
    let end = index + 19;
    if end > source.len() {
        return MarkerScan::NoMatch;
    }
    let has_clock_shape = bytes.get(index + 16..index + 19).is_some_and(|part| {
        part[0].is_ascii_digit() && part[1].is_ascii_digit() && part[2] == b'/'
    });
    if !has_clock_shape {
        return malformed_llm(source, index, end, dialect, "malformed local datetime marker");
    }
    let literal = &source[index..end];
    let expression = format!("{}T{}:{}", &literal[..10], &literal[11..13], &literal[16..18]);
    let _ = separator;
    parsed_llm_time(source, index, end, expression, dialect)
}

fn parsed_llm_time(
    source: &str,
    index: usize,
    end: usize,
    expression_text: String,
    dialect: &'static str,
) -> MarkerScan {
    let expression = match parse_expression(&expression_text) {
        Ok(expression) => expression,
        Err(message) => return malformed_llm(source, index, end, dialect, &message),
    };
    let format = if matches!(expression, TimeExpression::Clock(_)) {
        DateTimeFormat::Time
    } else {
        DateTimeFormat::DateTime
    };
    MarkerScan::Parsed(DateTimeNode { expression, format, source_range: index..end }, end)
}

fn malformed_llm(
    source: &str,
    index: usize,
    end: usize,
    dialect: &'static str,
    message: &str,
) -> MarkerScan {
    MarkerScan::MalformedIntent(RenderError::invalid(
        dialect,
        source,
        index,
        &source[index..end.min(source.len())],
        message,
    ))
}

fn scan_html(source: &str) -> Result<ScanResult, RenderError> {
    let mut nodes = Vec::new();
    let mut safe_cut_points = vec![0];
    let mut text_start = 0;
    let mut index = 0;
    while index < source.len() {
        if source.as_bytes()[index] != b'<' {
            index = next_char_boundary(source, index);
            continue;
        }
        let Some(tag_end) = find_tag_end(source, index) else {
            index = next_char_boundary(source, index);
            continue;
        };
        let tag = &source[index..=tag_end];
        let Some(name) = html_tag_name(tag) else {
            index = tag_end + 1;
            continue;
        };
        let name = name.to_ascii_lowercase();
        if !matches!(name.as_str(), "tg-time" | "tg-emoji" | "a") {
            index = tag_end + 1;
            continue;
        }
        let attrs = parse_html_attributes(tag);
        let self_closing = tag.trim_end().ends_with("/>");
        let (node, end) = match name.as_str() {
            "tg-time" => {
                let value = attrs.get("value").or_else(|| attrs.get("datetime"));
                let Some(value) = value else {
                    return Err(RenderError::invalid(
                        "html",
                        source,
                        index,
                        tag,
                        "tg-time requires a `value` attribute",
                    ));
                };
                let expression = parse_expression(&value.replace(' ', "T"))
                    .map_err(|message| RenderError::invalid("html", source, index, tag, message))?;
                let end = if self_closing {
                    tag_end + 1
                } else {
                    let closing = "</tg-time>";
                    let Some(offset) = source[tag_end + 1..].find(closing) else {
                        return Err(RenderError::invalid(
                            "html",
                            source,
                            index,
                            tag,
                            "tg-time is missing its closing tag",
                        ));
                    };
                    tag_end + 1 + offset + closing.len()
                };
                (
                    RichNode::DateTime(DateTimeNode {
                        expression,
                        format: infer_time_format(value),
                        source_range: index..end,
                    }),
                    end,
                )
            }
            "tg-emoji" => {
                let Some(alias) = attrs.get("alias") else {
                    return Err(RenderError::invalid(
                        "html",
                        source,
                        index,
                        tag,
                        "tg-emoji requires an `alias` attribute",
                    ));
                };
                let alias = alias.to_ascii_lowercase();
                let end = if self_closing {
                    tag_end + 1
                } else {
                    let closing = "</tg-emoji>";
                    let Some(offset) = source[tag_end + 1..].find(closing) else {
                        return Err(RenderError::invalid(
                            "html",
                            source,
                            index,
                            tag,
                            "tg-emoji is missing its closing tag",
                        ));
                    };
                    tag_end + 1 + offset + closing.len()
                };
                (RichNode::CustomEmoji { alias }, end)
            }
            "a" => {
                let Some(href) = attrs.get("href") else {
                    return Err(RenderError::invalid(
                        "html",
                        source,
                        index,
                        tag,
                        "a requires an `href` attribute",
                    ));
                };
                let closing = "</a>";
                let Some(offset) = source[tag_end + 1..].find(closing) else {
                    return Err(RenderError::invalid(
                        "html",
                        source,
                        index,
                        tag,
                        "a is missing its closing tag",
                    ));
                };
                let label_start = tag_end + 1;
                let label_end = label_start + offset;
                let label = scan_html(&source[label_start..label_end])?;
                (
                    RichNode::Link { label: label.nodes, target: classify_link_target(href) },
                    label_end + closing.len(),
                )
            }
            _ => unreachable!(),
        };
        push_text(&mut nodes, &source[text_start..index]);
        nodes.push(node);
        index = end;
        text_start = end;
        safe_cut_points.push(end);
    }
    push_text(&mut nodes, &source[text_start..]);
    safe_cut_points.push(safe_cut_end(source));
    safe_cut_points.sort_unstable();
    safe_cut_points.dedup();
    Ok(ScanResult { nodes, safe_cut_points })
}

fn infer_time_format(value: &str) -> DateTimeFormat {
    if value.starts_with("now") {
        DateTimeFormat::Relative
    } else if value.len() == 5 {
        DateTimeFormat::Time
    } else if value.len() == 10 {
        DateTimeFormat::Date
    } else {
        DateTimeFormat::DateTime
    }
}

fn find_tag_end(source: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    for (offset, byte) in source[start..].bytes().enumerate() {
        match (quote, byte) {
            (Some(current), byte) if byte == current => quote = None,
            (None, b'\'' | b'"') => quote = Some(byte),
            (None, b'>') => return Some(start + offset),
            _ => {}
        }
    }
    None
}

fn html_tag_name(tag: &str) -> Option<&str> {
    let inner = tag.strip_prefix('<')?.strip_suffix('>')?.trim_start_matches('/').trim();
    let end = inner.find(|ch: char| ch.is_ascii_whitespace() || ch == '/').unwrap_or(inner.len());
    Some(&inner[..end])
}

fn parse_html_attributes(tag: &str) -> std::collections::HashMap<String, String> {
    let mut attributes = std::collections::HashMap::new();
    let inner = tag.strip_prefix('<').and_then(|value| value.strip_suffix('>')).unwrap_or_default();
    let inner = inner
        .find(|character: char| character.is_ascii_whitespace() || character == '/')
        .map_or("", |offset| &inner[offset..]);
    let mut bytes = inner.bytes().peekable();
    while let Some(byte) = bytes.peek().copied() {
        if byte.is_ascii_whitespace() || byte == b'/' {
            bytes.next();
            continue;
        }
        let mut name = String::new();
        while let Some(byte) = bytes.peek().copied() {
            if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
                name.push(byte as char);
                bytes.next();
            } else {
                break;
            }
        }
        if name.is_empty() {
            bytes.next();
            continue;
        }
        while bytes.peek().is_some_and(u8::is_ascii_whitespace) {
            bytes.next();
        }
        if bytes.next() != Some(b'=') {
            continue;
        }
        while bytes.peek().is_some_and(u8::is_ascii_whitespace) {
            bytes.next();
        }
        let Some(quote @ (b'\'' | b'"')) = bytes.next() else {
            continue;
        };
        let mut value = String::new();
        for byte in bytes.by_ref() {
            if byte == quote {
                break;
            }
            value.push(byte as char);
        }
        attributes.insert(name.to_ascii_lowercase(), value);
    }
    attributes
}

fn parse_markdown_link(source: &str, index: usize) -> Option<(usize, usize, String)> {
    let label_end = find_unescaped(source, index + 1, b']')?;
    if source.as_bytes().get(label_end + 1) != Some(&b'(') {
        return None;
    }
    let destination_start = label_end + 2;
    let destination_end = find_markdown_destination_end(source, destination_start)?;
    let mut destination = source[destination_start..destination_end].trim().to_owned();
    if destination.len() >= 2 && destination.starts_with('<') && destination.ends_with('>') {
        destination = destination[1..destination.len() - 1].to_owned();
    }
    Some((label_end, destination_end + 1, destination))
}

fn find_markdown_destination_end(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut depth = 0usize;
    let mut index = start;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index = (index + 2).min(bytes.len());
            continue;
        }
        match bytes[index] {
            b'(' => depth = depth.saturating_add(1),
            b')' if depth == 0 => return Some(index),
            b')' => depth -= 1,
            _ => {}
        }
        index += 1;
    }
    None
}

fn find_unescaped(source: &str, start: usize, needle: u8) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut index = start;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index = (index + 2).min(bytes.len());
            continue;
        }
        if bytes[index] == needle {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn parse_custom_emoji(source: &str, index: usize) -> Option<(String, usize)> {
    let bytes = source.as_bytes();
    let mut cursor = index + 1;
    let start = cursor;
    while bytes.get(cursor).is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_') {
        cursor += 1;
    }
    if cursor == start || bytes.get(cursor) != Some(&b':') {
        return None;
    }
    Some((source[start..cursor].to_ascii_lowercase(), cursor + 1))
}

/// Returns the last byte offset that is not inside an unfinished extension.
/// This is intentionally a small tail lexer: it never scans beyond the
/// suffixes that can still become one of our semantic tokens.
fn safe_cut_end(source: &str) -> usize {
    let mut candidate = source.len();

    if let Some(index) = source.rfind('[') {
        if parse_markdown_link(source, index).is_none() {
            candidate = candidate.min(index);
        }
    }

    if let Some(index) = source.rfind(':') {
        let suffix = &source[index + 1..];
        if !suffix.is_empty()
            && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            candidate = candidate.min(index);
        }
    }

    for marker in ["now", "now+", "now-"] {
        if let Some(index) = source.rfind(marker) {
            if is_boundary_before(source, index) && is_pending_now_suffix(&source[index..]) {
                candidate = candidate.min(index);
            }
        }
    }

    if let Some(marker_start) = source.rfind(":::") {
        let before = &source[..marker_start];
        let mut digits_start = before.len();
        while digits_start > 0 && before.as_bytes()[digits_start - 1].is_ascii_digit() {
            digits_start -= 1;
        }
        let after_marker = &source[marker_start + 3..];
        let complete_clock = after_marker.as_bytes().get(0..3).is_some_and(|part| {
            part[0].is_ascii_digit() && part[1].is_ascii_digit() && part[2] == b'/'
        });
        if !complete_clock
            && marker_start >= digits_start + 2
            && before[digits_start..].bytes().all(|byte| byte.is_ascii_digit())
            && is_boundary_before(source, digits_start)
        {
            candidate = candidate.min(digits_start);
        }
    }

    candidate
}

fn is_pending_now_suffix(value: &str) -> bool {
    if value == "now" {
        return true;
    }
    let Some(rest) = value.strip_prefix("now+").or_else(|| value.strip_prefix("now-")) else {
        return false;
    };
    !rest.is_empty()
        && rest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b's' | b'm' | b'h' | b'd' | b'w'))
}

fn push_text(nodes: &mut Vec<RichNode>, text: &str) {
    if text.is_empty() {
        return;
    }
    if let Some(RichNode::Text(previous)) = nodes.last_mut() {
        previous.push_str(text);
    } else {
        nodes.push(RichNode::Text(text.to_owned()));
    }
}

fn skip_delimited(source: &str, index: usize, delimiter: &str) -> usize {
    source[index + delimiter.len()..]
        .find(delimiter)
        .map_or(source.len(), |offset| index + delimiter.len() + offset + delimiter.len())
}

fn skip_inline_code(source: &str, index: usize) -> usize {
    let run = source[index..].bytes().take_while(|byte| *byte == b'`').count();
    let delimiter = &source[index..index + run];
    source[index + run..].find(delimiter).map_or(source.len(), |offset| index + run + offset + run)
}

fn skip_uri(source: &str, index: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let (start, autolink) = if source[index..].starts_with("http://")
        || source[index..].starts_with("https://")
        || source[index..].starts_with("tg://")
    {
        (index, false)
    } else if bytes.get(index) == Some(&b'<')
        && (source.get(index + 1..)?.starts_with("http://")
            || source.get(index + 1..)?.starts_with("https://")
            || source.get(index + 1..)?.starts_with("tg://"))
    {
        (index + 1, true)
    } else {
        return None;
    };
    let mut cursor = start;
    while cursor < source.len() {
        let byte = bytes[cursor];
        if byte.is_ascii_whitespace() || (!autolink && matches!(byte, b')' | b']')) {
            break;
        }
        if autolink && byte == b'>' {
            return Some(cursor + 1);
        }
        cursor += 1;
    }
    Some(cursor)
}

fn skip_escaped(source: &str, index: usize) -> usize {
    let next = next_char_boundary(source, index);
    if next < source.len() {
        next_char_boundary(source, next)
    } else {
        next
    }
}

fn next_char_boundary(source: &str, index: usize) -> usize {
    source[index..].chars().next().map_or(source.len(), |character| index + character.len_utf8())
}

fn is_boundary_before(source: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }
    source[..index]
        .chars()
        .next_back()
        .is_none_or(|character| !(character.is_ascii_alphanumeric() || character == '_'))
}

fn has_uri_scheme(value: &str) -> bool {
    let Some(colon) = value.bytes().position(|byte| byte == b':') else {
        return false;
    };
    colon > 0
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value.as_bytes()[1..colon]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-'))
}

fn escape_markdown_destination(value: &str) -> String {
    value.replace('\\', "\\\\").replace(')', "\\)")
}

fn escape_markdown_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace(']', "\\]")
}

fn escape_html(value: &str) -> String {
    value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::time::{CustomEmojiBinding, TimeValue};
    use teloxide_core::types::CustomEmojiId;

    fn context() -> TimeContext {
        TimeContext::from_name("Europe/Moscow").unwrap()
    }

    fn instant() -> Timestamp {
        "2026-08-01T10:00:00Z".parse().unwrap()
    }

    #[test]
    fn main_formatter_replaces_directive_and_keeps_fallback_clean() {
        let formatter = MainMarkdownFormatter::new(context());
        let rendered = formatter
            .render_at("Встреча @time(14:00).", &TimeBindings::default(), instant())
            .unwrap();
        assert!(rendered.markdown.contains("tg://time?unix="));
        assert_eq!(rendered.fallback_text, "Встреча 14:00.");
    }

    #[test]
    fn main_formatter_ignores_code_and_url_destination() {
        let formatter = MainMarkdownFormatter::new(context());
        let rendered = formatter
            .render_at(
                "`@time(14:00)` [@time(15:00)](https://example.test/@time(16:00))",
                &TimeBindings::default(),
                instant(),
            )
            .unwrap();
        assert_eq!(
            rendered.fallback_text,
            "`@time(14:00)` [15:00](https://example.test/@time(16:00))"
        );
    }

    #[test]
    fn llm_formatter_maps_clock_and_now() {
        let formatter = LlmMarkdownFormatter::new(context());
        let rendered =
            formatter.render_at("14:::00/ and now/ now-15m/ now+2h30m/.", instant()).unwrap();
        assert_eq!(rendered.fallback_text, "14:00 and 13:00 12:45 15:30.");
    }

    #[test]
    fn llm_formatter_maps_full_local_datetime() {
        let formatter = LlmMarkdownFormatter::new(context());
        let rendered = formatter.render_at("Release: 2026-08-03 14:::00/.", instant()).unwrap();
        assert_eq!(rendered.fallback_text, "Release: 2026-08-03 14:00.");
    }

    #[test]
    fn llm_formatter_ignores_code_url_and_escaped_marker() {
        let formatter = LlmMarkdownFormatter::new(context());
        let rendered = formatter
            .render_at(
                "`14:::00/` [14:::00/](https://example.test/14:::00/) \\::: 14:::00/",
                instant(),
            )
            .unwrap();
        assert_eq!(
            rendered.fallback_text,
            "`14:::00/` [14:00](https://example.test/14:::00/) \\::: 14:00"
        );
    }

    #[test]
    fn llm_scanner_is_bounded_and_url_aware() {
        let formatter = LlmMarkdownFormatter::new(context());
        for source in [
            "now we continue / later",
            "nowadays/path",
            "::: section",
            "https://example.org/now/latest",
            "https://example.org/archive/14:::00/path",
            "<https://example.org/14:::00/path>",
            "[link](https://example.org/now/latest)",
        ] {
            let rendered = formatter.render_at(source, instant()).unwrap();
            assert_eq!(rendered.fallback_text, source);
            assert!(!rendered.markdown.contains("tg://time"), "unexpected marker in {source}");
        }

        let rendered = formatter
            .render_at(
                "У нас 2 встречи: первая в 14:::00/\nВерсия 2, запуск в 2026-08-03 14:::00/",
                instant(),
            )
            .unwrap();
        assert_eq!(
            rendered.fallback_text,
            "У нас 2 встречи: первая в 14:00\nВерсия 2, запуск в 2026-08-03 14:00"
        );

        for source in ["2026-08-03 release", "2026-08-03 в 14:00", "2026-08-03T14:00"] {
            let rendered = formatter.render_at(source, instant()).unwrap();
            assert_eq!(rendered.fallback_text, source);
        }

        for source in ["2026-08-03 14:::00/", "2026-08-03T14:::00/"] {
            let rendered = formatter.render_at(source, instant()).unwrap();
            assert_eq!(rendered.fallback_text, "2026-08-03 14:00");
        }

        for source in ["2026-08-03 14:::xx/", "2026-08-03 14::::00/"] {
            assert!(formatter.render_at(source, instant()).is_err(), "{source}");
        }
    }

    #[test]
    fn llm_formatter_rejects_malformed_marker() {
        let formatter = LlmMarkdownFormatter::new(context());
        assert!(formatter.render_at("24:::00/", instant()).is_err());
        assert!(formatter.render_at("now+3hours/", instant()).is_err());
        assert!(formatter.render_at("14::::00/", instant()).is_err());
    }

    #[test]
    fn main_formatter_rejects_missing_binding_and_invalid_relative_value() {
        let formatter = MainMarkdownFormatter::new(context());
        assert!(formatter
            .render_at("@time($missing)", &TimeBindings::default(), instant())
            .is_err());
        assert!(formatter
            .render_at("@relative(14:00)", &TimeBindings::default(), instant())
            .is_err());
    }

    #[test]
    fn dst_gap_and_fold_use_compatible_disambiguation() {
        let context = TimeContext::from_name("America/New_York").unwrap();
        let gap = "2026-03-08T02:30".parse().unwrap();
        let gap = context
            .normalize(
                &TimeExpression::CivilDateTime(gap),
                DateTimeFormat::DateTime,
                instant(),
                &TimeBindings::default(),
            )
            .unwrap();
        assert_eq!(gap.timestamp.to_string(), "2026-03-08T07:30:00Z");

        let fold = "2026-11-01T01:30".parse().unwrap();
        let fold = context
            .normalize(
                &TimeExpression::CivilDateTime(fold),
                DateTimeFormat::DateTime,
                instant(),
                &TimeBindings::default(),
            )
            .unwrap();
        assert_eq!(fold.timestamp.to_string(), "2026-11-01T05:30:00Z");
    }

    #[test]
    fn bindings_are_typed_not_string_substitutions() {
        let formatter = MainMarkdownFormatter::new(context());
        let mut bindings = TimeBindings::new();
        bindings.insert("retry_at", TimeValue::Instant(instant()));
        let rendered = formatter.render_at("retry @time($retry_at)", &bindings, instant()).unwrap();
        assert_eq!(rendered.fallback_text, "retry 13:00");
    }

    #[test]
    fn render_at_reuses_one_captured_now() {
        let formatter = LlmMarkdownFormatter::new(context());
        let rendered = formatter.render_at("now/ and now+1h/", instant()).unwrap();
        assert_eq!(rendered.fallback_text, "13:00 and 14:00");
        assert_eq!(rendered.captured_now, instant());
    }

    #[test]
    fn all_frontends_share_link_and_emoji_bindings() {
        let mut bindings = RichTextBindings::new();
        bindings
            .insert_link("source_1", Url::parse("https://example.com/source").unwrap())
            .unwrap();
        bindings
            .insert_custom_emoji(
                "party",
                CustomEmojiBinding {
                    custom_emoji_id: CustomEmojiId("123".into()),
                    fallback: "🎉".into(),
                },
            )
            .unwrap();
        let time = context();
        let context = RichTextRenderContext::new(&time, &bindings);
        let expected = "Релиз источник 🎉";

        let markdown = LlmMarkdownFormatter::new(time.clone())
            .render_with_context_at("Релиз [источник](source_1) :party:", &context, instant())
            .unwrap();
        assert_eq!(markdown.fallback_text, expected);
        assert!(markdown.compiled.contains("tg://emoji?id=123"));

        let html = HtmlFormatter::new(time.clone())
            .render_at(
                "Релиз <a href=\"source_1\">источник</a> <tg-emoji alias=\"party\" />",
                &context,
                instant(),
            )
            .unwrap();
        assert_eq!(html.fallback_text, expected);
        assert!(html.compiled.contains("emoji-id=\"123\""));
    }

    #[test]
    fn developer_policy_is_strict_and_llm_policy_keeps_readable_fallback() {
        let time = context();
        let bindings = RichTextBindings::default();
        let developer = RichTextRenderContext::new(&time, &bindings)
            .with_policies(RichTextPolicies::developer());
        assert!(MainMarkdownFormatter::new(time.clone())
            .render_with_context_at("[missing](source_1) :party:", &developer, instant())
            .is_err());

        let llm =
            RichTextRenderContext::new(&time, &bindings).with_policies(RichTextPolicies::llm());
        let rendered = LlmMarkdownFormatter::new(time.clone())
            .render_with_context_at("[missing](source_1) :party:", &llm, instant())
            .unwrap();
        assert_eq!(rendered.fallback_text, "missing :party:");
        assert_eq!(rendered.diagnostics.len(), 2);
    }

    #[test]
    fn html_time_and_links_use_common_ir() {
        let time = context();
        let bindings = RichTextBindings::default();
        let context = RichTextRenderContext::new(&time, &bindings);
        let rendered = HtmlFormatter::new(time.clone())
            .render_at(
                "Встреча <tg-time value=\"15:00\"></tg-time> <a href=\"https://example.com\">сайт</a>",
                &context,
                instant(),
            )
            .unwrap();
        assert_eq!(rendered.fallback_text, "Встреча 15:00 сайт");
        assert!(rendered.compiled.contains("<tg-time unix=\""));
        assert!(rendered.compiled.contains("<a href=\"https://example.com\">"));
    }

    #[test]
    fn incomplete_extensions_are_pending_and_not_safe_cut_points() {
        let formatter = LlmMarkdownFormatter::new(context());
        for source in ["14:::", "14:::00", "now", "now+3h", ":party", "[источник](source_"]
        {
            let parsed = formatter.parse(source).unwrap();
            assert_eq!(parsed.safe_cut_points().last().copied(), Some(0), "{source}");
            assert_eq!(parsed.nodes.len(), 1, "{source}");
        }
        for source in ["14:::00/", "now+3h/", ":party:", "[источник](source_1)"] {
            let parsed = formatter.parse(source).unwrap();
            assert_eq!(parsed.safe_cut_points().last().copied(), Some(source.len()), "{source}");
        }
    }

    #[test]
    fn standard_formatter_does_not_enable_semantic_extensions() {
        let rendered = StandardMarkdownFormatter::new().render("15:::00/ :party: [x](source_1)");
        assert_eq!(rendered.fallback_text, "15:::00/ :party: [x](source_1)");
        assert_eq!(rendered.compiled, rendered.fallback_text);
        assert!(rendered.diagnostics.is_empty());
    }
}
