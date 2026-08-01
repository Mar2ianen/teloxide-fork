use jiff::Timestamp;
use teloxide_core::types::InputRichMessage;

use super::{
    model::parse_expression, DateTimeFormat, DateTimeNode, RenderError, RichNode, TimeBindings,
    TimeContext, TimeExpression,
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

enum MarkerScan {
    NoMatch,
    Parsed(DateTimeNode, usize),
    MalformedIntent(RenderError),
}

fn scan(
    source: &str,
    dialect: &'static str,
    marker: impl Fn(&str, usize, &'static str) -> MarkerScan,
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
        if let Some(end) = skip_uri(source, index) {
            index = end;
            continue;
        }
        if source.as_bytes()[index] == b']' && source[index..].starts_with("](") {
            if let Some(end) = source[index + 2..].find(')') {
                index += 2 + end + 1;
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
                continue;
            }
            MarkerScan::MalformedIntent(error) => return Err(error),
        }
        index = next_char_boundary(source, index);
    }
    push_text(&mut nodes, &source[text_start..]);
    Ok(nodes)
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
        while cursor < source.len() {
            let digits_start = cursor;
            while source.as_bytes().get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
            if cursor == digits_start {
                return malformed_llm(
                    source,
                    index,
                    cursor,
                    dialect,
                    "relative offset needs a number",
                );
            }
            let Some(unit) = source.as_bytes().get(cursor).copied() else {
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
        if source.as_bytes().get(cursor) != Some(&b'/') {
            return malformed_llm(source, index, cursor, dialect, "relative marker is missing `/`");
        }
        cursor + 1
    };
    let literal = &source[index..end];
    let expression = match parse_expression(&literal[..literal.len() - 1]) {
        Ok(expression) => expression,
        Err(message) => {
            return MarkerScan::MalformedIntent(RenderError::invalid(
                dialect, source, index, literal, message,
            ));
        }
    };
    MarkerScan::Parsed(
        DateTimeNode { expression, format: DateTimeFormat::Time, source_range: index..end },
        end,
    )
}

fn parse_llm_numeric(source: &str, index: usize, dialect: &'static str) -> MarkerScan {
    let bytes = source.as_bytes();
    let clock = bytes.get(index..index + 5).is_some_and(|part| {
        part[0].is_ascii_digit() && part[1].is_ascii_digit() && part[2..] == *b":::"
    });
    if clock {
        let end = index + 8;
        if bytes
            .get(index + 5..index + 7)
            .is_none_or(|part| part.len() != 2 || !part.iter().all(u8::is_ascii_digit))
            || bytes.get(index + 7) != Some(&b'/')
        {
            return malformed_llm(
                source,
                index,
                end.min(source.len()),
                dialect,
                "malformed clock marker",
            );
        }
        let literal = &source[index..end];
        return parsed_llm_time(
            source,
            index,
            end,
            literal,
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
    let Some(separator @ (b' ' | b'T')) = bytes.get(index + 10).copied() else {
        return MarkerScan::NoMatch;
    };
    if !date_prefix {
        return MarkerScan::NoMatch;
    }
    // An ISO date followed by ordinary prose is not an explicit marker. Only
    // the local datetime prefix through `HH:::` establishes marker intent;
    // after that prefix, an invalid minute or terminator is malformed.
    let has_clock_prefix = bytes.get(index + 11..index + 16).is_some_and(|part| {
        part[0].is_ascii_digit() && part[1].is_ascii_digit() && part[2..] == *b":::"
    });
    if !has_clock_prefix {
        return MarkerScan::NoMatch;
    }
    let marker_end = index + 19;
    let has_clock_shape = bytes.get(index + 16..index + 19).is_some_and(|part| {
        part[0].is_ascii_digit() && part[1].is_ascii_digit() && part[2] == b'/'
    });
    if !has_clock_shape {
        return malformed_llm(
            source,
            index,
            marker_end.min(source.len()),
            dialect,
            "malformed local datetime marker",
        );
    }
    let literal = &source[index..marker_end];
    let expression_text = format!("{}T{}:{}", &literal[..10], &literal[11..13], &literal[16..18]);
    let _ = separator;
    parsed_llm_time(source, index, marker_end, literal, expression_text, dialect)
}

fn parsed_llm_time(
    source: &str,
    index: usize,
    end: usize,
    _literal: &str,
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
        && (source[index + 1..].starts_with("http://")
            || source[index + 1..].starts_with("https://")
            || source[index + 1..].starts_with("tg://"))
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
}
