use jiff::Timestamp;
use teloxide_core::types::InputRichMessage;
use url::Url;

use super::{
    classify_link_target, model::parse_expression, CustomEmojiBinding, DateTimeFormat,
    DateTimeNode, InvalidTimePolicy, LinkTarget, LiteralLinkPolicy, RenderError, RichNode,
    RichTextBindings, RichTextDiagnostic, RichTextPolicies, RichTextRenderContext, TimeBindings,
    TimeContext, TimeExpression,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct MainMarkdownFormatter;

#[derive(Clone, Copy, Debug, Default)]
pub struct LlmMarkdownFormatter;

#[derive(Clone, Copy, Debug, Default)]
pub struct HtmlFormatter;

/// Explicit Markdown transport without time, alias or custom-emoji semantics.
#[derive(Clone, Copy, Debug, Default)]
pub struct StandardMarkdownFormatter;

#[derive(Clone, Debug)]
pub struct ParsedMainMarkdown {
    source: String,
    nodes: Vec<RichNode>,
    known_extension_end_points: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct ParsedLlmMarkdown {
    source: String,
    nodes: Vec<RichNode>,
    known_extension_end_points: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct ParsedHtml {
    source: String,
    nodes: Vec<RichNode>,
    known_extension_end_points: Vec<usize>,
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
    /// End offsets recognized by the frontend after complete extensions.
    ///
    /// These are diagnostic parser landmarks, not a segmentation guarantee:
    /// Markdown emphasis, HTML containers and other syntax may still span an
    /// arbitrary landmark.
    pub known_extension_end_points: Vec<usize>,
}

impl MainMarkdownFormatter {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, source: &str) -> Result<ParsedMainMarkdown, RenderError> {
        let scanned = scan_main(source)?;
        Ok(ParsedMainMarkdown {
            source: source.to_owned(),
            nodes: scanned.nodes,
            known_extension_end_points: scanned.known_extension_end_points,
        })
    }

    pub fn render(
        &self,
        source: &str,
        time: &TimeContext,
        bindings: &TimeBindings,
    ) -> Result<RenderedMessage, RenderError> {
        self.render_at(source, time, bindings, Timestamp::now())
    }

    pub fn render_with_bindings(
        &self,
        source: &str,
        time: &TimeContext,
        bindings: &TimeBindings,
    ) -> Result<RenderedMessage, RenderError> {
        self.render(source, time, bindings)
    }

    pub fn render_at(
        &self,
        source: &str,
        time: &TimeContext,
        bindings: &TimeBindings,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        self.parse(source)?.render_at(time, bindings, captured_now)
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
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, source: &str) -> Result<ParsedLlmMarkdown, RenderError> {
        let scanned = scan_llm(source)?;
        Ok(ParsedLlmMarkdown {
            source: source.to_owned(),
            nodes: scanned.nodes,
            known_extension_end_points: scanned.known_extension_end_points,
        })
    }

    pub fn render(&self, source: &str, time: &TimeContext) -> Result<RenderedMessage, RenderError> {
        self.render_at(source, time, Timestamp::now())
    }

    pub fn render_at(
        &self,
        source: &str,
        time: &TimeContext,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let bindings = RichTextBindings::default();
        let policies = RichTextPolicies::llm();
        let time_bindings = TimeBindings::default();
        let parsed = self.parse(source)?;
        let options = RenderOptions {
            time,
            time_bindings: &time_bindings,
            bindings: &bindings,
            policies: &policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(source, &parsed.nodes, &parsed.known_extension_end_points, &options)
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
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, source: &str) -> Result<ParsedHtml, RenderError> {
        let scanned = scan_html(source)?;
        Ok(ParsedHtml {
            source: source.to_owned(),
            nodes: scanned.nodes,
            known_extension_end_points: scanned.known_extension_end_points,
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
            known_extension_end_points: vec![0, source.len()],
        }
    }
}

impl ParsedMainMarkdown {
    pub fn known_extension_end_points(&self) -> &[usize] {
        &self.known_extension_end_points
    }

    pub fn link_aliases(&self) -> Vec<String> {
        collect_link_aliases(&self.nodes)
    }

    pub fn link_destinations(&self) -> Vec<String> {
        collect_link_destinations(&self.nodes)
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
        render_nodes(&self.source, &self.nodes, &self.known_extension_end_points, &options)
    }

    pub fn render_with_context_at(
        &self,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let options = RenderOptions {
            time: context.time,
            time_bindings: context.time_bindings,
            bindings: context.bindings,
            policies: &context.policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.known_extension_end_points, &options)
    }
}

impl ParsedLlmMarkdown {
    pub fn known_extension_end_points(&self) -> &[usize] {
        &self.known_extension_end_points
    }

    pub fn link_aliases(&self) -> Vec<String> {
        collect_link_aliases(&self.nodes)
    }

    pub fn link_destinations(&self) -> Vec<String> {
        collect_link_destinations(&self.nodes)
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
        render_nodes(&self.source, &self.nodes, &self.known_extension_end_points, &options)
    }

    pub fn render_with_context_at(
        &self,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let options = RenderOptions {
            time: context.time,
            time_bindings: context.time_bindings,
            bindings: context.bindings,
            policies: &context.policies,
            frontend: Frontend::Markdown,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.known_extension_end_points, &options)
    }
}

impl ParsedHtml {
    pub fn known_extension_end_points(&self) -> &[usize] {
        &self.known_extension_end_points
    }

    pub fn link_aliases(&self) -> Vec<String> {
        collect_link_aliases(&self.nodes)
    }

    pub fn link_destinations(&self) -> Vec<String> {
        collect_link_destinations(&self.nodes)
    }

    pub fn render_at(
        &self,
        context: &RichTextRenderContext<'_>,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        let options = RenderOptions {
            time: context.time,
            time_bindings: context.time_bindings,
            bindings: context.bindings,
            policies: &context.policies,
            frontend: Frontend::Html,
            captured_now,
        };
        render_nodes(&self.source, &self.nodes, &self.known_extension_end_points, &options)
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

fn collect_link_aliases(nodes: &[RichNode]) -> Vec<String> {
    let mut aliases = Vec::new();
    for node in nodes {
        match node {
            RichNode::Link { label, target, .. } => {
                if let LinkTarget::Alias(alias) = target {
                    if !aliases.contains(alias) {
                        aliases.push(alias.clone());
                    }
                }
                for alias in collect_link_aliases(label) {
                    if !aliases.contains(&alias) {
                        aliases.push(alias);
                    }
                }
            }
            RichNode::Text(_)
            | RichNode::DateTime(_)
            | RichNode::InvalidTime { .. }
            | RichNode::CustomEmoji { .. } => {}
        }
    }
    aliases
}

fn collect_link_destinations(nodes: &[RichNode]) -> Vec<String> {
    let mut destinations = Vec::new();
    for node in nodes {
        match node {
            RichNode::Link { label, target, .. } => {
                if let LinkTarget::Literal(destination) = target {
                    if !destinations.contains(destination) {
                        destinations.push(destination.clone());
                    }
                }
                for destination in collect_link_destinations(label) {
                    if !destinations.contains(&destination) {
                        destinations.push(destination);
                    }
                }
            }
            RichNode::Text(_)
            | RichNode::DateTime(_)
            | RichNode::InvalidTime { .. }
            | RichNode::CustomEmoji { .. } => {}
        }
    }
    destinations
}

fn render_nodes(
    source: &str,
    nodes: &[RichNode],
    known_extension_end_points: &[usize],
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
        known_extension_end_points: known_extension_end_points.to_vec(),
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
            RichNode::InvalidTime { literal, source_range } => {
                if matches!(options.policies.invalid_time, InvalidTimePolicy::Error) {
                    return Err(RenderError::invalid(
                        "llm",
                        source,
                        source_range.start,
                        literal,
                        "malformed time marker",
                    ));
                }
                result
                    .diagnostics
                    .push(RichTextDiagnostic::InvalidTimeToken { token: literal.clone() });
                push_literal_for_frontend(&mut result.compiled, literal, options.frontend);
                result.fallback.push_str(literal);
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
            RichNode::Link { label, target, source_range } => {
                let label = render_fragment(source, label, options)?;
                result.diagnostics.extend(label.diagnostics);
                let resolution =
                    resolve_link(target, options.bindings, options.policies, source, source_range)?;
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
                    (_, LinkTarget::Alias(_), Some(destination)) => {
                        result.fallback.push_str(&label.fallback);
                        result.fallback.push_str(" (");
                        result.fallback.push_str(destination);
                        result.fallback.push(')');
                    }
                    _ => result.fallback.push_str(&label.fallback),
                }
            }
            RichNode::CustomEmoji { alias, source_range } => {
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
                                push_literal_for_frontend(
                                    &mut result.compiled,
                                    &literal,
                                    options.frontend,
                                );
                                result.fallback.push_str(&literal);
                            }
                            super::UnknownCustomEmojiPolicy::UseFallback(fallback) => {
                                push_literal_for_frontend(
                                    &mut result.compiled,
                                    fallback,
                                    options.frontend,
                                );
                                result.fallback.push_str(fallback);
                            }
                            super::UnknownCustomEmojiPolicy::Error => {
                                return Err(RenderError::invalid(
                                    "rich-text",
                                    source,
                                    source_range.start,
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
    source_range: &std::ops::Range<usize>,
) -> Result<LinkResolution, RenderError> {
    match target {
        LinkTarget::Literal(value) => {
            if value.is_empty()
                || value
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
            {
                return invalid_literal_link(
                    policies,
                    source,
                    value,
                    source_range.start,
                    "literal link destination is invalid",
                );
            }
            if has_uri_scheme(value) {
                let Some(scheme) = value.split_once(':').map(|(scheme, _)| scheme) else {
                    unreachable!("has_uri_scheme guarantees a scheme separator")
                };
                if matches!(policies.literal_link, LiteralLinkPolicy::TelegramSafeSchemes)
                    && !matches!(
                        scheme.to_ascii_lowercase().as_str(),
                        "http" | "https" | "tg" | "mailto" | "ftp"
                    )
                {
                    return invalid_literal_link(
                        policies,
                        source,
                        value,
                        source_range.start,
                        "literal URI scheme is not allowed",
                    );
                }
                if Url::parse(value).is_err() {
                    return invalid_literal_link(
                        policies,
                        source,
                        value,
                        source_range.start,
                        "literal URI is invalid",
                    );
                }
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
                    source_range.start,
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
    byte_offset: usize,
    message: &str,
) -> Result<LinkResolution, RenderError> {
    if matches!(policies.unknown_link_alias, super::UnknownLinkAliasPolicy::Error) {
        Err(RenderError::invalid("rich-text", source, byte_offset, literal, message))
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
    known_extension_end_points: Vec<usize>,
}

enum MarkerScan {
    NoMatch,
    Parsed(DateTimeNode, usize),
    MalformedIntent { error: RenderError, range: std::ops::Range<usize> },
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
    scan_at(source, 0, dialect, marker)
}

fn scan_at(
    source: &str,
    base_offset: usize,
    dialect: &'static str,
    marker: impl Fn(&str, usize, &'static str) -> MarkerScan + Copy,
) -> Result<ScanResult, RenderError> {
    let mut nodes = Vec::new();
    let mut known_extension_end_points = vec![0];
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
                let mut label = scan(label_source, dialect, marker)?;
                for node in &mut label.nodes {
                    shift_node_ranges(node, base_offset + index + 1);
                }
                push_text(&mut nodes, &source[text_start..index]);
                nodes.push(RichNode::Link {
                    label: label.nodes,
                    target: classify_link_target(&destination),
                    source_range: index..destination_end,
                });
                index = destination_end;
                text_start = index;
                known_extension_end_points.push(index);
                continue;
            }
        }
        if source.as_bytes()[index] == b':' {
            if let Some((alias, end)) = parse_custom_emoji(source, index) {
                push_text(&mut nodes, &source[text_start..index]);
                nodes.push(RichNode::CustomEmoji { alias, source_range: index..end });
                index = end;
                text_start = end;
                known_extension_end_points.push(end);
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
                known_extension_end_points.push(end);
                continue;
            }
            MarkerScan::MalformedIntent { error, range } => {
                if dialect != "llm" {
                    return Err(error);
                }
                let range = range.start..range.end.min(source.len());
                let literal = source.get(range.clone()).unwrap_or_default().to_owned();
                push_text(&mut nodes, &source[text_start..index]);
                nodes.push(RichNode::InvalidTime {
                    literal,
                    source_range: base_offset + range.start..base_offset + range.end,
                });
                index = range.end;
                text_start = index;
                known_extension_end_points.push(index);
                continue;
            }
        }
        index = next_char_boundary(source, index);
    }
    push_text(&mut nodes, &source[text_start..]);
    known_extension_end_points.push(known_extension_end(source));
    known_extension_end_points.sort_unstable();
    known_extension_end_points.dedup();
    Ok(ScanResult { nodes, known_extension_end_points })
}

fn shift_node_ranges(node: &mut RichNode, base_offset: usize) {
    match node {
        RichNode::DateTime(node) => {
            node.source_range.start += base_offset;
            node.source_range.end += base_offset;
        }
        RichNode::InvalidTime { source_range, .. } | RichNode::CustomEmoji { source_range, .. } => {
            source_range.start += base_offset;
            source_range.end += base_offset;
        }
        RichNode::Link { label, source_range, .. } => {
            source_range.start += base_offset;
            source_range.end += base_offset;
            for node in label {
                shift_node_ranges(node, base_offset);
            }
        }
        RichNode::Text(_) => {}
    }
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
        return MarkerScan::MalformedIntent {
            error: RenderError::invalid(
                dialect,
                source,
                index,
                &source[index..],
                "directive is missing a closing `)`",
            ),
            range: index..source.len(),
        };
    };
    let content_end = content_start + close_offset;
    let content = &source[content_start..content_end];
    let expression = match parse_expression(content) {
        Ok(expression) => expression,
        Err(message) => {
            return MarkerScan::MalformedIntent {
                error: RenderError::invalid(
                    dialect,
                    source,
                    index,
                    &source[index..=content_end],
                    message,
                ),
                range: index..content_end + 1,
            };
        }
    };
    if matches!(format, DateTimeFormat::Relative)
        && !matches!(expression, TimeExpression::Now { .. } | TimeExpression::Variable { .. })
    {
        return MarkerScan::MalformedIntent {
            error: RenderError::invalid(
                dialect,
                source,
                index,
                &source[index..=content_end],
                "@relative accepts only `now` or a typed binding",
            ),
            range: index..content_end + 1,
        };
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
    let end = end.min(source.len());
    MarkerScan::MalformedIntent {
        error: RenderError::invalid(dialect, source, index, &source[index..end], message),
        range: index..end,
    }
}

fn scan_html(source: &str) -> Result<ScanResult, RenderError> {
    scan_html_at(source, 0)
}

fn scan_html_at(source: &str, base_offset: usize) -> Result<ScanResult, RenderError> {
    let mut nodes = Vec::new();
    let mut known_extension_end_points = vec![0];
    let mut text_start = 0;
    let mut index = 0;
    while index < source.len() {
        if source.as_bytes()[index] != b'<' {
            index = next_char_boundary(source, index);
            continue;
        }
        if is_unclosed_html_literal(source, index) {
            known_extension_end_points.push(index);
            break;
        }
        if let Some(end) = skip_html_literal_context(source, index) {
            index = end;
            continue;
        }
        let Some(tag_end) = find_tag_end(source, index) else {
            if looks_like_html_extension_prefix(&source[index..]) {
                known_extension_end_points.push(index);
                break;
            }
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
        if !self_closing && !has_html_closing_tag(source, tag_end + 1, &name) {
            known_extension_end_points.push(index);
            break;
        }
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
                let format = parse_html_time_format(attrs.get("format"), value)
                    .map_err(|message| RenderError::invalid("html", source, index, tag, message))?;
                let end = if self_closing {
                    tag_end + 1
                } else {
                    let closing = "</tg-time>";
                    let Some(offset) = find_case_insensitive(&source[tag_end + 1..], closing)
                    else {
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
                        format,
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
                    let Some(offset) = find_case_insensitive(&source[tag_end + 1..], closing)
                    else {
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
                (RichNode::CustomEmoji { alias, source_range: index..end }, end)
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
                let Some(offset) = find_case_insensitive(&source[tag_end + 1..], closing) else {
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
                let mut label = scan_html(&source[label_start..label_end])?;
                for node in &mut label.nodes {
                    shift_node_ranges(node, base_offset + label_start);
                }
                (
                    RichNode::Link {
                        label: label.nodes,
                        target: classify_link_target(href),
                        source_range: index..label_end + closing.len(),
                    },
                    label_end + closing.len(),
                )
            }
            _ => unreachable!(),
        };
        push_text(&mut nodes, &source[text_start..index]);
        nodes.push(node);
        index = end;
        text_start = end;
        known_extension_end_points.push(end);
    }
    push_text(&mut nodes, &source[text_start..]);
    known_extension_end_points.push(known_extension_end(source));
    known_extension_end_points.sort_unstable();
    known_extension_end_points.dedup();
    Ok(ScanResult { nodes, known_extension_end_points })
}

fn parse_html_time_format(
    explicit: Option<&String>,
    value: &str,
) -> Result<DateTimeFormat, &'static str> {
    if let Some(format) = explicit {
        return match format.to_ascii_lowercase().as_str() {
            "time" => Ok(DateTimeFormat::Time),
            "date" => Ok(DateTimeFormat::Date),
            "datetime" | "date-time" => Ok(DateTimeFormat::DateTime),
            "relative" => Ok(DateTimeFormat::Relative),
            _ => Err("tg-time `format` must be time, date, datetime or relative"),
        };
    }
    if value.starts_with("now") || value.len() == 5 {
        Ok(DateTimeFormat::Time)
    } else if value.len() == 10 {
        Ok(DateTimeFormat::Date)
    } else {
        Ok(DateTimeFormat::DateTime)
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

fn find_case_insensitive(source: &str, needle: &str) -> Option<usize> {
    let needle = needle.as_bytes();
    source.as_bytes().windows(needle.len()).position(|window| {
        window.iter().zip(needle).all(|(left, right)| left.to_ascii_lowercase() == *right)
    })
}

fn skip_html_literal_context(source: &str, start: usize) -> Option<usize> {
    let tag_end = find_tag_end(source, start)?;
    let tag = &source[start..=tag_end];
    if tag.starts_with("</") || tag.trim_end().ends_with("/>") {
        return None;
    }
    let name = html_tag_name(tag)?.to_ascii_lowercase();
    if !matches!(name.as_str(), "code" | "pre") {
        return None;
    }
    let closing = format!("</{name}>");
    let offset = find_case_insensitive(&source[tag_end + 1..], &closing)?;
    Some(tag_end + 1 + offset + closing.len())
}

fn is_unclosed_html_literal(source: &str, start: usize) -> bool {
    let Some(tag_end) = find_tag_end(source, start) else {
        return false;
    };
    let tag = &source[start..=tag_end];
    if tag.starts_with("</") || tag.trim_end().ends_with("/>") {
        return false;
    }
    let Some(name) = html_tag_name(tag) else {
        return false;
    };
    let name = name.to_ascii_lowercase();
    matches!(name.as_str(), "code" | "pre")
        && find_case_insensitive(&source[tag_end + 1..], &format!("</{name}>")).is_none()
}

fn has_html_closing_tag(source: &str, start: usize, name: &str) -> bool {
    find_case_insensitive(&source[start..], &format!("</{name}>")).is_some()
}

fn looks_like_html_extension_prefix(source: &str) -> bool {
    let source = source.to_ascii_lowercase();
    ["<tg-time", "<tg-emoji", "<a ", "<a>", "<a\n"].iter().any(|prefix| source.starts_with(prefix))
}

fn html_tag_name(tag: &str) -> Option<&str> {
    let inner = tag.strip_prefix('<')?.strip_suffix('>')?.trim_start_matches('/').trim();
    let end = inner.find(|ch: char| ch.is_ascii_whitespace() || ch == '/').unwrap_or(inner.len());
    Some(&inner[..end])
}

fn parse_html_attributes(tag: &str) -> std::collections::HashMap<String, String> {
    let mut attributes = std::collections::HashMap::new();
    let inner = tag.strip_prefix('<').and_then(|value| value.strip_suffix('>')).unwrap_or_default();
    let mut cursor = inner.char_indices();
    while cursor
        .next()
        .is_some_and(|(_, character)| !character.is_ascii_whitespace() && character != '/')
    {}
    loop {
        while let Some((_, character)) = cursor.clone().next() {
            if character.is_ascii_whitespace() || character == '/' {
                cursor.next();
            } else {
                break;
            }
        }
        let Some((name_start, first)) = cursor.clone().next() else {
            break;
        };
        if !first.is_ascii_alphanumeric() && first != '-' && first != '_' {
            cursor.next();
            continue;
        }
        let mut name_end = name_start;
        while let Some((offset, character)) = cursor.clone().next() {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                name_end = offset + character.len_utf8();
                cursor.next();
            } else {
                break;
            }
        }
        while cursor.clone().next().is_some_and(|(_, character)| character.is_ascii_whitespace()) {
            cursor.next();
        }
        if cursor.next().is_none_or(|(_, character)| character != '=') {
            continue;
        }
        while cursor.clone().next().is_some_and(|(_, character)| character.is_ascii_whitespace()) {
            cursor.next();
        }
        let Some((_, quote @ ('\'' | '"'))) = cursor.next() else {
            continue;
        };
        let Some((value_start, _)) = cursor.clone().next() else {
            break;
        };
        let mut value_cursor = cursor.clone();
        let Some(value_end) =
            value_cursor.find_map(|(offset, character)| (character == quote).then_some(offset))
        else {
            break;
        };
        cursor = value_cursor;
        let name = &inner[name_start..name_end];
        attributes.insert(
            name.to_ascii_lowercase(),
            decode_html_entities(&inner[value_start..value_end]),
        );
    }
    attributes
}

fn decode_html_entities(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut remainder = value;
    while let Some(start) = remainder.find('&') {
        decoded.push_str(&remainder[..start]);
        let Some(end_offset) = remainder[start..].find(';') else {
            decoded.push_str(&remainder[start..]);
            return decoded;
        };
        let end = start + end_offset;
        let entity = &remainder[start + 1..end];
        let replacement = match entity {
            "amp" => Some("&".to_owned()),
            "lt" => Some("<".to_owned()),
            "gt" => Some(">".to_owned()),
            "quot" => Some("\"".to_owned()),
            "apos" => Some("'".to_owned()),
            _ if entity.strip_prefix("#x").is_some() || entity.strip_prefix("#X").is_some() => {
                entity
                    .get(2..)
                    .and_then(|digits| u32::from_str_radix(digits, 16).ok())
                    .and_then(char::from_u32)
                    .map(|character| character.to_string())
            }
            _ if entity.strip_prefix('#').is_some() => entity
                .get(1..)
                .and_then(|digits| digits.parse::<u32>().ok())
                .and_then(char::from_u32)
                .map(|character| character.to_string()),
            _ => None,
        };
        if let Some(replacement) = replacement {
            decoded.push_str(&replacement);
        } else {
            decoded.push_str(&remainder[start..=end]);
        }
        remainder = &remainder[end + 1..];
    }
    decoded.push_str(remainder);
    decoded
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
    if !is_boundary_before(source, index) {
        return None;
    }
    let bytes = source.as_bytes();
    let mut cursor = index + 1;
    let start = cursor;
    if !bytes.get(cursor).is_some_and(|byte| byte.is_ascii_lowercase() || *byte == b'_') {
        return None;
    }
    while bytes
        .get(cursor)
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
    {
        cursor += 1;
    }
    if cursor == start || bytes.get(cursor) != Some(&b':') || !is_boundary_after(source, cursor + 1)
    {
        return None;
    }
    Some((source[start..cursor].to_owned(), cursor + 1))
}

/// Returns the last byte offset that is not inside an unfinished extension.
/// This is intentionally a small tail lexer: it never scans beyond the
/// suffixes that can still become one of our semantic tokens.
fn known_extension_end(source: &str) -> usize {
    let mut candidate = source.len();

    for (index, _) in source.match_indices('<') {
        let suffix = &source[index..];
        if looks_like_html_extension_prefix(suffix) || is_unclosed_html_literal(source, index) {
            candidate = candidate.min(index);
        }
    }

    if let Some(index) = source.rfind('[') {
        if parse_markdown_link(source, index).is_none() {
            candidate = candidate.min(index);
        }
    }

    if let Some(index) = source.rfind(':') {
        let suffix = &source[index + 1..];
        let escaped = index > 0 && source.as_bytes()[index - 1] == b'\\';
        let emoji_prefix = is_boundary_before(source, index) && !escaped;
        if emoji_prefix
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
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

    for (colon, _) in source.match_indices(':') {
        let before = &source[..colon];
        let mut digits_start = before.len();
        while digits_start > 0 && before.as_bytes()[digits_start - 1].is_ascii_digit() {
            digits_start -= 1;
        }
        if colon >= digits_start + 2
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
    rest.bytes()
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

fn is_boundary_after(source: &str, index: usize) -> bool {
    source[index..]
        .chars()
        .next()
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

fn push_literal_for_frontend(output: &mut String, value: &str, frontend: Frontend) {
    match frontend {
        Frontend::Markdown => output.push_str(&escape_markdown_literal(value)),
        Frontend::Html => output.push_str(&escape_html(value)),
    }
}

fn escape_markdown_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(
            character,
            '\\' | '_'
                | '*'
                | '['
                | ']'
                | '('
                | ')'
                | '~'
                | '`'
                | '>'
                | '#'
                | '+'
                | '-'
                | '='
                | '|'
                | '{'
                | '}'
                | '.'
                | '!'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
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
        let formatter = MainMarkdownFormatter::new();
        let rendered = formatter
            .render_at("Встреча @time(14:00).", &context(), &TimeBindings::default(), instant())
            .unwrap();
        assert!(rendered.markdown.contains("tg://time?unix="));
        assert_eq!(rendered.fallback_text, "Встреча 14:00.");
    }

    #[test]
    fn main_formatter_ignores_code_and_url_destination() {
        let formatter = MainMarkdownFormatter::new();
        let rendered = formatter
            .render_at(
                "`@time(14:00)` [@time(15:00)](https://example.test/@time(16:00))",
                &context(),
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
        let formatter = LlmMarkdownFormatter::new();
        let rendered = formatter
            .render_at("14:::00/ and now/ now-15m/ now+2h30m/.", &context(), instant())
            .unwrap();
        assert_eq!(rendered.fallback_text, "14:00 and 13:00 12:45 15:30.");
    }

    #[test]
    fn llm_formatter_maps_full_local_datetime() {
        let formatter = LlmMarkdownFormatter::new();
        let rendered =
            formatter.render_at("Release: 2026-08-03 14:::00/.", &context(), instant()).unwrap();
        assert_eq!(rendered.fallback_text, "Release: 2026-08-03 14:00.");
    }

    #[test]
    fn llm_formatter_ignores_code_url_and_escaped_marker() {
        let formatter = LlmMarkdownFormatter::new();
        let rendered = formatter
            .render_at(
                "`14:::00/` [14:::00/](https://example.test/14:::00/) \\::: 14:::00/",
                &context(),
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
        let formatter = LlmMarkdownFormatter::new();
        for source in [
            "now we continue / later",
            "nowadays/path",
            "::: section",
            "https://example.org/now/latest",
            "https://example.org/archive/14:::00/path",
            "<https://example.org/14:::00/path>",
            "[link](https://example.org/now/latest)",
        ] {
            let rendered = formatter.render_at(source, &context(), instant()).unwrap();
            assert_eq!(rendered.fallback_text, source);
            assert!(!rendered.markdown.contains("tg://time"), "unexpected marker in {source}");
        }

        let rendered = formatter
            .render_at(
                "У нас 2 встречи: первая в 14:::00/\nВерсия 2, запуск в 2026-08-03 14:::00/",
                &context(),
                instant(),
            )
            .unwrap();
        assert_eq!(
            rendered.fallback_text,
            "У нас 2 встречи: первая в 14:00\nВерсия 2, запуск в 2026-08-03 14:00"
        );

        for source in ["2026-08-03 release", "2026-08-03 в 14:00", "2026-08-03T14:00"] {
            let rendered = formatter.render_at(source, &context(), instant()).unwrap();
            assert_eq!(rendered.fallback_text, source);
        }

        for source in ["2026-08-03 14:::00/", "2026-08-03T14:::00/"] {
            let rendered = formatter.render_at(source, &context(), instant()).unwrap();
            assert_eq!(rendered.fallback_text, "2026-08-03 14:00");
        }

        for source in ["2026-08-03 14:::xx/", "2026-08-03 14::::00/"] {
            let rendered = formatter.render_at(source, &context(), instant()).unwrap();
            assert_eq!(rendered.fallback_text, source);
            assert_eq!(rendered.diagnostics.len(), 1);
        }
    }

    #[test]
    fn llm_formatter_rejects_malformed_marker() {
        let formatter = LlmMarkdownFormatter::new();
        for source in ["24:::00/", "now+3hours/", "14::::00/"] {
            let rendered = formatter.render_at(source, &context(), instant()).unwrap();
            assert_eq!(rendered.fallback_text, source);
            assert_eq!(rendered.diagnostics.len(), 1);
        }
        let time = context();
        let time_bindings = TimeBindings::default();
        let bindings = RichTextBindings::default();
        let strict = RichTextRenderContext::for_llm(&time, &time_bindings, &bindings)
            .with_policies(RichTextPolicies {
                invalid_time: InvalidTimePolicy::Error,
                ..RichTextPolicies::llm()
            });
        assert!(formatter.render_with_context_at("24:::00/", &strict, instant()).is_err());
    }

    #[test]
    fn main_formatter_rejects_missing_binding_and_invalid_relative_value() {
        let formatter = MainMarkdownFormatter::new();
        assert!(formatter
            .render_at("@time($missing)", &context(), &TimeBindings::default(), instant())
            .is_err());
        assert!(formatter
            .render_at("@relative(14:00)", &context(), &TimeBindings::default(), instant())
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
        let formatter = MainMarkdownFormatter::new();
        let mut bindings = TimeBindings::new();
        bindings.insert("retry_at", TimeValue::Instant(instant()));
        let rendered = formatter
            .render_at("retry @time($retry_at)", &context(), &bindings, instant())
            .unwrap();
        assert_eq!(rendered.fallback_text, "retry 13:00");
    }

    #[test]
    fn render_at_reuses_one_captured_now() {
        let formatter = LlmMarkdownFormatter::new();
        let rendered = formatter.render_at("now/ and now+1h/", &context(), instant()).unwrap();
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
        let time_bindings = TimeBindings::default();
        let context = RichTextRenderContext::new(&time, &time_bindings, &bindings);
        let expected = "Релиз источник (https://example.com/source) 🎉";

        let markdown = LlmMarkdownFormatter::new()
            .render_with_context_at("Релиз [источник](source_1) :party:", &context, instant())
            .unwrap();
        assert_eq!(markdown.fallback_text, expected);
        assert!(markdown.compiled.contains("tg://emoji?id=123"));

        let html = HtmlFormatter::new()
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
    fn developer_context_combines_time_bindings_with_rich_bindings() {
        let time = context();
        let mut time_bindings = TimeBindings::default();
        time_bindings.insert("release", TimeValue::Instant(instant()));
        let mut bindings = RichTextBindings::default();
        bindings.insert_link("chat", Url::parse("https://example.com/chat").unwrap()).unwrap();
        bindings
            .insert_custom_emoji(
                "party",
                CustomEmojiBinding {
                    custom_emoji_id: CustomEmojiId("123".into()),
                    fallback: "🎉".into(),
                },
            )
            .unwrap();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        let rendered = MainMarkdownFormatter::new()
            .render_with_context_at("@time($release) [чат](chat) :party:", &context, instant())
            .unwrap();
        assert_eq!(rendered.fallback_text, "13:00 чат (https://example.com/chat) 🎉");
    }

    #[test]
    fn emoji_aliases_require_word_boundaries_and_named_start() {
        let mut bindings = RichTextBindings::default();
        bindings
            .insert_custom_emoji(
                "party",
                CustomEmojiBinding {
                    custom_emoji_id: CustomEmojiId("123".into()),
                    fallback: "🎉".into(),
                },
            )
            .unwrap();
        assert!(bindings
            .insert_custom_emoji(
                "30",
                CustomEmojiBinding {
                    custom_emoji_id: CustomEmojiId("456".into()),
                    fallback: "🔢".into(),
                },
            )
            .is_err());
        let time = context();
        let time_bindings = TimeBindings::default();
        let context = RichTextRenderContext::for_llm(&time, &time_bindings, &bindings);
        let rendered = LlmMarkdownFormatter::new()
            .render_with_context_at("12:30: foo:party: :party:", &context, instant())
            .unwrap();
        assert_eq!(rendered.fallback_text, "12:30: foo:party: 🎉");
    }

    #[test]
    fn literal_links_are_validated_by_policy() {
        let time = context();
        let time_bindings = TimeBindings::default();
        let bindings = RichTextBindings::default();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        let formatter = MainMarkdownFormatter::new();
        assert!(formatter
            .render_with_context_at("[bad](javascript:alert(1))", &context, instant())
            .is_err());
        assert!(formatter
            .render_with_context_at("[bad](foo.bar invalid)", &context, instant())
            .is_err());
        assert!(formatter
            .render_with_context_at("[ok](https://example.com/path)", &context, instant())
            .is_ok());
    }

    #[test]
    fn parsed_links_expose_aliases_and_literal_destinations() {
        let parsed = LlmMarkdownFormatter::new()
            .parse("[сообщение](message_42) [сайт](https://example.com)")
            .unwrap();
        assert_eq!(parsed.link_aliases(), vec!["message_42"]);
        assert_eq!(parsed.link_destinations(), vec!["https://example.com"]);
    }

    #[test]
    fn link_label_time_errors_keep_absolute_source_offsets() {
        let time = context();
        let time_bindings = TimeBindings::default();
        let mut bindings = RichTextBindings::default();
        bindings.insert_link("source_1", Url::parse("https://example.com").unwrap()).unwrap();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        let error = MainMarkdownFormatter::new()
            .render_with_context_at("prefix [@time($missing)](source_1)", &context, instant())
            .unwrap_err();
        match error {
            RenderError::UnknownBinding { byte_offset, .. } => assert_eq!(byte_offset, 8),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn html_frontend_respects_code_contexts_and_unicode_attributes() {
        let time = context();
        let time_bindings = TimeBindings::default();
        let mut bindings = RichTextBindings::default();
        bindings
            .insert_custom_emoji(
                "party",
                CustomEmojiBinding {
                    custom_emoji_id: CustomEmojiId("123".into()),
                    fallback: "🎉".into(),
                },
            )
            .unwrap();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        let rendered = HtmlFormatter::new()
            .render_at(
                "<code><tg-emoji alias=\"party\" /></code> <a href=\"https://example.com/привет\">сайт</a> <TG-EMOJI alias=\"party\"></tg-emoji>",
                &context,
                instant(),
            )
            .unwrap();
        assert!(rendered.fallback_text.contains("<tg-emoji alias=\"party\" />"));
        assert!(rendered.fallback_text.ends_with("сайт 🎉"));
    }

    #[test]
    fn incomplete_html_extensions_are_pending() {
        for source in [
            "<tg-emoji alias=\"party\"",
            "<tg-emoji alias=\"party\">",
            "<code><tg-emoji alias=\"party\" />",
        ] {
            let parsed = HtmlFormatter::new().parse(source).unwrap();
            assert_eq!(parsed.known_extension_end_points().last().copied(), Some(0), "{source}");
            assert_eq!(parsed.nodes.len(), 1, "{source}");
        }
    }

    #[test]
    fn developer_policy_is_strict_and_llm_policy_keeps_readable_fallback() {
        let time = context();
        let bindings = RichTextBindings::default();
        let time_bindings = TimeBindings::default();
        let developer = RichTextRenderContext::new(&time, &time_bindings, &bindings)
            .with_policies(RichTextPolicies::developer());
        assert!(MainMarkdownFormatter::new()
            .render_with_context_at("[missing](source_1) :party:", &developer, instant())
            .is_err());

        let llm = RichTextRenderContext::for_llm(&time, &time_bindings, &bindings);
        let rendered = LlmMarkdownFormatter::new()
            .render_with_context_at("[missing](source_1) :rust_sad:", &llm, instant())
            .unwrap();
        assert_eq!(rendered.fallback_text, "missing :rust_sad:");
        assert!(rendered.compiled.contains(":rust\\_sad:"));
        assert_eq!(rendered.diagnostics.len(), 2);
    }

    #[test]
    fn html_time_and_links_use_common_ir() {
        let time = context();
        let bindings = RichTextBindings::default();
        let time_bindings = TimeBindings::default();
        let context = RichTextRenderContext::new(&time, &time_bindings, &bindings);
        let rendered = HtmlFormatter::new()
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
    fn html_time_format_is_explicit_and_now_defaults_to_time() {
        let time = context();
        let bindings = RichTextBindings::default();
        let time_bindings = TimeBindings::default();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        let rendered = HtmlFormatter::new()
            .render_at(
                "<tg-time value=\"now+3h\" /> <tg-time value=\"now+3h\" format=\"relative\" />",
                &context,
                instant(),
            )
            .unwrap();
        assert!(rendered.compiled.contains("format=\"t\""));
        assert!(rendered.compiled.contains("format=\"r\""));
    }

    #[test]
    fn html_attributes_decode_entities_without_double_escaping() {
        let time = context();
        let bindings = RichTextBindings::default();
        let time_bindings = TimeBindings::default();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        let rendered = HtmlFormatter::new()
            .render_at("<a href=\"https://example.com/?a=1&amp;b=2\">сайт</a>", &context, instant())
            .unwrap();
        assert!(rendered.compiled.contains("a=1&amp;b=2"));
        assert!(!rendered.compiled.contains("amp;amp"));
    }

    #[test]
    fn html_literal_context_search_starts_after_the_current_tag() {
        let time = context();
        let time_bindings = TimeBindings::default();
        let mut bindings = RichTextBindings::default();
        bindings
            .insert_custom_emoji(
                "party",
                CustomEmojiBinding {
                    custom_emoji_id: CustomEmojiId("123".into()),
                    fallback: "🎉".into(),
                },
            )
            .unwrap();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        let rendered = HtmlFormatter::new()
            .render_at(
                "<code></code><code><tg-emoji alias=\"party\" /></code>",
                &context,
                instant(),
            )
            .unwrap();
        assert_eq!(
            rendered.fallback_text,
            "<code></code><code><tg-emoji alias=\"party\" /></code>"
        );
    }

    #[test]
    fn link_and_emoji_errors_keep_absolute_source_offsets() {
        let time = context();
        let time_bindings = TimeBindings::default();
        let bindings = RichTextBindings::default();
        let context = RichTextRenderContext::for_developer(&time, &time_bindings, &bindings);
        match MainMarkdownFormatter::new()
            .render_with_context_at("prefix [x](missing)", &context, instant())
            .unwrap_err()
        {
            RenderError::InvalidMarkup { byte_offset, .. } => assert_eq!(byte_offset, 7),
            other => panic!("unexpected error: {other:?}"),
        }
        match MainMarkdownFormatter::new()
            .render_with_context_at("prefix :missing:", &context, instant())
            .unwrap_err()
        {
            RenderError::InvalidMarkup { byte_offset, .. } => assert_eq!(byte_offset, 7),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn incomplete_extensions_are_pending_and_not_segmentation_guarantees() {
        let formatter = LlmMarkdownFormatter::new();
        for source in [
            "14:",
            "14::",
            "14:::",
            "14:::00",
            "now",
            "now+",
            "now+3h",
            ":",
            ":party",
            "[источник](source_",
        ] {
            let parsed = formatter.parse(source).unwrap();
            assert_eq!(parsed.known_extension_end_points().last().copied(), Some(0), "{source}");
            assert_eq!(parsed.nodes.len(), 1, "{source}");
        }
        for source in ["14:::00/", "now+3h/", ":party:", "[источник](source_1)"] {
            let parsed = formatter.parse(source).unwrap();
            assert_eq!(
                parsed.known_extension_end_points().last().copied(),
                Some(source.len()),
                "{source}"
            );
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
