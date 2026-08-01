use jiff::Timestamp;
use teloxide_core::types::InputRichMessage;

use super::{
    DateTimeFormat, DateTimeNode, RenderError, RichNode, TimeBindings, TimeContext, TimeExpression,
    model::parse_expression,
};

#[derive(Clone)]
pub struct MainMarkdownFormatter {
    time: TimeContext,
}

#[derive(Clone)]
pub struct LlmMarkdownFormatter {
    time: TimeContext,
}

#[derive(Clone, Debug)]
pub struct ParsedMainMarkdown {
    source: String,
    nodes: Vec<RichNode>,
}

#[derive(Clone, Debug)]
pub struct ParsedLlmMarkdown {
    source: String,
    nodes: Vec<RichNode>,
}

#[derive(Clone, Debug)]
pub struct RenderedMessage {
    /// Markdown with Telegram's internal time entity links. This is an
    /// implementation detail; persist the original source instead.
    pub markdown: String,
    pub rich_message: InputRichMessage,
    pub fallback_text: String,
    pub captured_now: Timestamp,
}

impl MainMarkdownFormatter {
    pub fn new(time: TimeContext) -> Self {
        Self { time }
    }

    pub fn time(&self) -> &TimeContext {
        &self.time
    }

    pub fn parse(&self, source: &str) -> Result<ParsedMainMarkdown, RenderError> {
        Ok(ParsedMainMarkdown { source: source.to_owned(), nodes: scan_main(source)? })
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
}

impl LlmMarkdownFormatter {
    pub fn new(time: TimeContext) -> Self {
        Self { time }
    }

    pub fn time(&self) -> &TimeContext {
        &self.time
    }

    pub fn parse(&self, source: &str) -> Result<ParsedLlmMarkdown, RenderError> {
        Ok(ParsedLlmMarkdown { source: source.to_owned(), nodes: scan_llm(source)? })
    }

    pub fn render(&self, source: &str) -> Result<RenderedMessage, RenderError> {
        self.render_at(source, Timestamp::now())
    }

    pub fn render_at(
        &self,
        source: &str,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        self.parse(source)?.render_at(&self.time, captured_now)
    }
}

impl ParsedMainMarkdown {
    pub fn render_at(
        &self,
        time: &TimeContext,
        bindings: &TimeBindings,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        render_nodes(&self.source, &self.nodes, time, bindings, captured_now)
    }
}

impl ParsedLlmMarkdown {
    pub fn render_at(
        &self,
        time: &TimeContext,
        captured_now: Timestamp,
    ) -> Result<RenderedMessage, RenderError> {
        render_nodes(&self.source, &self.nodes, time, &TimeBindings::default(), captured_now)
    }
}

fn render_nodes(
    source: &str,
    nodes: &[RichNode],
    time: &TimeContext,
    bindings: &TimeBindings,
    captured_now: Timestamp,
) -> Result<RenderedMessage, RenderError> {
    let mut markdown = String::new();
    let mut fallback = String::new();
    for node in nodes {
        match node {
            RichNode::Text(text) => {
                markdown.push_str(text);
                fallback.push_str(text);
            }
            RichNode::DateTime(node) => {
                let normalized = time
                    .normalize(&node.expression, node.format, captured_now, bindings)
                    .map_err(|error| {
                        RenderError::from_time_error(source, node.source_range.start, error)
                    })?;
                markdown.push_str(&format!(
                    "![{}](tg://time?unix={}&format={})",
                    normalized.fallback_text,
                    normalized.unix_time,
                    normalized.format.wire_value()
                ));
                fallback.push_str(&normalized.fallback_text);
            }
        }
    }
    Ok(RenderedMessage {
        rich_message: InputRichMessage::markdown(markdown.clone()),
        markdown,
        fallback_text: fallback,
        captured_now,
    })
}

fn scan_main(source: &str) -> Result<Vec<RichNode>, RenderError> {
    scan(source, "main", scan_main_marker)
}

fn scan_llm(source: &str) -> Result<Vec<RichNode>, RenderError> {
    scan(source, "llm", scan_llm_marker)
}

fn scan(
    source: &str,
    dialect: &'static str,
    marker: impl Fn(&str, usize, &'static str) -> Result<Option<(DateTimeNode, usize)>, RenderError>,
) -> Result<Vec<RichNode>, RenderError> {
    let mut nodes = Vec::new();
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
        if source.as_bytes()[index] == b']' && source[index..].starts_with("](") {
            if let Some(end) = source[index + 2..].find(')') {
                index += 2 + end + 1;
                continue;
            }
        }
        if let Some((node, end)) = marker(source, index, dialect)? {
            push_text(&mut nodes, &source[text_start..index]);
            nodes.push(RichNode::DateTime(node));
            index = end;
            text_start = end;
            continue;
        }
        index = next_char_boundary(source, index);
    }
    push_text(&mut nodes, &source[text_start..]);
    Ok(nodes)
}

fn scan_main_marker(
    source: &str,
    index: usize,
    dialect: &'static str,
) -> Result<Option<(DateTimeNode, usize)>, RenderError> {
    if !is_boundary_before(source, index) {
        return Ok(None);
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
        return Ok(None);
    };
    let content_start = index + literal.len();
    let Some(close_offset) = source[content_start..].find(')') else {
        return Err(RenderError::invalid(
            dialect,
            source,
            index,
            &source[index..],
            "directive is missing a closing `)`",
        ));
    };
    let content_end = content_start + close_offset;
    let content = &source[content_start..content_end];
    let expression = parse_expression(content).map_err(|message| {
        RenderError::invalid(dialect, source, index, &source[index..=content_end], message)
    })?;
    if matches!(format, DateTimeFormat::Relative)
        && !matches!(expression, TimeExpression::Now { .. } | TimeExpression::Variable { .. })
    {
        return Err(RenderError::invalid(
            dialect,
            source,
            index,
            &source[index..=content_end],
            "@relative accepts only `now` or a typed binding",
        ));
    }
    Ok(Some((
        DateTimeNode { expression, format: *format, source_range: index..content_end + 1 },
        content_end + 1,
    )))
}

fn scan_llm_marker(
    source: &str,
    index: usize,
    dialect: &'static str,
) -> Result<Option<(DateTimeNode, usize)>, RenderError> {
    if !is_boundary_before(source, index) {
        return Ok(None);
    }
    if source[index..].starts_with(":::") {
        return Err(RenderError::invalid(
            dialect,
            source,
            index,
            ":::",
            "a literal `:::` must be escaped as `\\:::`",
        ));
    }
    if source[index..].starts_with("now") {
        return parse_llm_now(source, index, dialect);
    }
    if !source.as_bytes()[index].is_ascii_digit() {
        return Ok(None);
    }
    let Some(slash_offset) = source[index..].find('/') else {
        return Ok(None);
    };
    let end = index + slash_offset + 1;
    let literal = &source[index..end];
    let Some(marker_offset) = literal.find(":::") else {
        return Ok(None);
    };
    let before = &literal[..marker_offset];
    let after = &literal[marker_offset + 3..literal.len() - 1];
    if after.contains(":::") || before.is_empty() || after.is_empty() {
        return Err(RenderError::invalid(
            dialect,
            source,
            index,
            literal,
            "malformed LLM time marker",
        ));
    }
    if after.len() != 2 || !after.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RenderError::invalid(
            dialect,
            source,
            index,
            literal,
            "minutes must contain exactly two digits",
        ));
    }
    let expression_text = if before.len() == 2 {
        format!("{before}:{after}")
    } else if before.len() == 13 && matches!(before.as_bytes().get(10), Some(b' ' | b'T')) {
        format!("{}T{}:{}", &before[..10], &before[11..], after)
    } else {
        format!("{before}:{after}")
    };
    let expression_text = expression_text.replace(' ', "T");
    let expression = parse_expression(&expression_text)
        .map_err(|message| RenderError::invalid(dialect, source, index, literal, message))?;
    if !matches!(expression, TimeExpression::Clock(_) | TimeExpression::CivilDateTime(_)) {
        return Err(RenderError::invalid(
            dialect,
            source,
            index,
            literal,
            "LLM marker must be a clock or local datetime",
        ));
    }
    let format = if matches!(expression, TimeExpression::Clock(_)) {
        DateTimeFormat::Time
    } else {
        DateTimeFormat::DateTime
    };
    Ok(Some((DateTimeNode { expression, format, source_range: index..end }, end)))
}

fn parse_llm_now(
    source: &str,
    index: usize,
    dialect: &'static str,
) -> Result<Option<(DateTimeNode, usize)>, RenderError> {
    let Some(slash_offset) = source[index..].find('/') else {
        return Ok(None);
    };
    let end = index + slash_offset + 1;
    let literal = &source[index..end];
    let expression = parse_expression(&literal[..literal.len() - 1])
        .map_err(|message| RenderError::invalid(dialect, source, index, literal, message))?;
    if !matches!(expression, TimeExpression::Now { .. }) {
        return Err(RenderError::invalid(
            dialect,
            source,
            index,
            literal,
            "invalid LLM relative time marker",
        ));
    }
    Ok(Some((
        DateTimeNode { expression, format: DateTimeFormat::Time, source_range: index..end },
        end,
    )))
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

fn skip_escaped(source: &str, index: usize) -> usize {
    let next = next_char_boundary(source, index);
    if next < source.len() { next_char_boundary(source, next) } else { next }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::time::TimeValue;

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
        let rendered = formatter.render_at("14:::00/ and now+3h/.", instant()).unwrap();
        assert_eq!(rendered.fallback_text, "14:00 and 16:00.");
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
    fn llm_formatter_rejects_malformed_marker() {
        let formatter = LlmMarkdownFormatter::new(context());
        assert!(formatter.render_at("24:::00/", instant()).is_err());
        assert!(formatter.render_at("now+3hours/", instant()).is_err());
        assert!(formatter.render_at("14::::00/", instant()).is_err());
    }

    #[test]
    fn main_formatter_rejects_missing_binding_and_invalid_relative_value() {
        let formatter = MainMarkdownFormatter::new(context());
        assert!(
            formatter.render_at("@time($missing)", &TimeBindings::default(), instant()).is_err()
        );
        assert!(
            formatter.render_at("@relative(14:00)", &TimeBindings::default(), instant()).is_err()
        );
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
}
