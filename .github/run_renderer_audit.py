from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


# Renderer state and UTF-16 newline positions.
path = ROOT / "crates/teloxide/src/utils/render.rs"
text = path.read_text()
text = replace_once(
    text,
    "    tags: Vec<Tag<'a>>,\n}",
    "    tags: Vec<Tag<'a>>,\n    passthrough_entities: Vec<&'a MEK>,\n}",
    "renderer fields",
)
text = replace_once(
    text,
    "        let mut tags = Vec::with_capacity(needed_size);\n\n        for (index, entity) in entities.iter().enumerate() {",
    "        let mut tags = Vec::with_capacity(needed_size);\n        let mut passthrough_entities = Vec::new();\n\n        for (index, entity) in entities.iter().enumerate() {",
    "renderer locals",
)
text = replace_once(
    text,
    "                _ => continue,",
    "                _ => {\n                    passthrough_entities.push(&entity.kind);\n                    continue;\n                }",
    "passthrough match",
)
pattern = re.compile(
    r"                let new_lines_indexes: Vec<usize> = text\n.*?                }\n",
    re.S,
)
replacement = """                let new_line_offsets = text
                    .encode_utf16()
                    .enumerate()
                    .skip(entity.offset)
                    .take(entity.length)
                    .filter_map(|(offset, unit)| (unit == u16::from(b'\\n')).then_some(offset + 1));

                for new_line_offset in new_line_offsets {
                    tags.push(Tag::mid_new_line(kind.clone(), new_line_offset, index));
                }
"""
text, count = pattern.subn(replacement, text, count=1)
if count != 1:
    raise RuntimeError(f"blockquote newline block: replaced {count}")
text = replace_once(
    text,
    "        Self { text, tags }\n    }\n\n    /// Renders text with a given [`TagWriter`].",
    """        Self { text, tags, passthrough_entities }
    }

    /// Returns entities whose semantic kind is preserved only as visible text.
    ///
    /// Telegram doesn't provide explicit HTML or MarkdownV2 markup for these
    /// entity kinds. The renderer keeps their text, while callers can inspect
    /// this iterator when a lossless semantic round-trip is required.
    pub fn passthrough_entities(&self) -> impl ExactSizeIterator<Item = &'a MEK> + '_ {
        self.passthrough_entities.iter().copied()
    }

    /// Returns `true` when at least one entity couldn't be represented as markup.
    #[must_use]
    pub fn has_passthrough_entities(&self) -> bool {
        !self.passthrough_entities.is_empty()
    }

    /// Renders text with a given [`TagWriter`].""",
    "renderer constructor",
)
tests = r'''

    #[test]
    fn blockquote_newline_offsets_use_utf16_units() {
        let text = "😀a\nb";
        let entities = vec![MessageEntity { kind: MEK::Blockquote, offset: 0, length: 5 }];
        let render = Renderer::new(text, &entities);

        assert_eq!(render.as_html(), "<blockquote>😀a\nb</blockquote>");
        assert_eq!(render.as_markdown(), "**>😀a\n>b");
    }

    #[test]
    fn passthrough_entities_are_reported() {
        let text = "@name #tag https://example.com";
        let entities = vec![
            MessageEntity { kind: MEK::Mention, offset: 0, length: 5 },
            MessageEntity { kind: MEK::Hashtag, offset: 6, length: 4 },
            MessageEntity { kind: MEK::Url, offset: 11, length: 19 },
        ];
        let render = Renderer::new(text, &entities);

        assert!(render.has_passthrough_entities());
        assert_eq!(render.passthrough_entities().count(), 3);
        assert_eq!(render.as_html(), text);
        assert_eq!(render.as_markdown(), "@name \\#tag https://example\\.com");
    }

    #[test]
    fn renderer_escapes_dynamic_attribute_and_link_values() {
        let text = "link time code";
        let entities = vec![
            MessageEntity {
                kind: MEK::TextLink {
                    url: reqwest::Url::parse("https://example.com/a)?x=1&y=2").unwrap(),
                },
                offset: 0,
                length: 4,
            },
            MessageEntity {
                kind: MEK::DateTime {
                    unix_time: Some(1),
                    date_time_format: Some("x&\")\\".to_owned()),
                },
                offset: 5,
                length: 4,
            },
            MessageEntity {
                kind: MEK::Pre { language: Some("ru&\"".to_owned()) },
                offset: 10,
                length: 4,
            },
        ];
        let render = Renderer::new(text, &entities);

        assert_eq!(
            render.as_html(),
            "<a href=\"https://example.com/a)?x=1&amp;y=2\">link</a> <tg-time unix=\"1\" format=\"x&amp;&quot;)\\\">time</tg-time> <pre><code class=\"language-ru&amp;&quot;\">code</code></pre>"
        );
        assert_eq!(
            render.as_markdown(),
            "[link](https://example.com/a\\)?x=1&y=2) ![time](tg://time?unix=1&format=x&\"\\)\\\\) ```ru&\"\ncode```\n"
        );
    }
'''
head, tail = text.rsplit("\n}", 1)
path.write_text(head + tests + "\n}" + tail)

# HTML writer: escape every dynamic attribute and avoid infallible write! unwraps.
path = ROOT / "crates/teloxide/src/utils/render/html.rs"
text = path.read_text()
text = text.replace("use std::fmt::Write;\n\n", "", 1)
text = replace_once(
    text,
    '                Some(lang) => write!(buf, "{}{}{}", HTML.pre.start, lang, HTML.pre.middle).unwrap(),',
    '''                Some(lang) => {
                    buf.push_str(HTML.pre.start);
                    write_attribute(lang, buf);
                    buf.push_str(HTML.pre.middle);
                }''',
    "HTML pre language",
)
text = replace_once(
    text,
    '''            Place::Start => {
                write!(buf, "{}{}{}", HTML.text_link.start, url, HTML.text_link.middle).unwrap()
            }''',
    '''            Place::Start => {
                buf.push_str(HTML.text_link.start);
                write_attribute(url, buf);
                buf.push_str(HTML.text_link.middle);
            }''',
    "HTML link",
)
text = replace_once(
    text,
    '''            Place::Start => {
                write!(buf, "{}{}{}", HTML.text_mention.start, id, HTML.text_mention.middle)
                    .unwrap()
            }''',
    '''            Place::Start => {
                buf.push_str(HTML.text_mention.start);
                buf.push_str(&id.to_string());
                buf.push_str(HTML.text_mention.middle);
            }''',
    "HTML mention",
)
text = replace_once(
    text,
    '''            Place::Start => write!(
                buf,
                "{}{}{}",
                HTML.custom_emoji.start, custom_emoji_id, HTML.custom_emoji.middle
            )
            .unwrap(),''',
    '''            Place::Start => {
                buf.push_str(HTML.custom_emoji.start);
                write_attribute(&custom_emoji_id.0, buf);
                buf.push_str(HTML.custom_emoji.middle);
            }''',
    "HTML emoji",
)
text = replace_once(
    text,
    '''            Place::Start => {
                write!(buf, "<tg-time unix=\"{unix_time}\"").unwrap();
                if let Some(format) = date_time_format {
                    write!(buf, " format=\"{format}\"").unwrap();
                }
                buf.push('>');
            }''',
    '''            Place::Start => {
                buf.push_str("<tg-time unix=\"");
                buf.push_str(&unix_time.to_string());
                buf.push('"');
                if let Some(format) = date_time_format {
                    buf.push_str(" format=\"");
                    write_attribute(format, buf);
                    buf.push('"');
                }
                buf.push('>');
            }''',
    "HTML time",
)
text = replace_once(
    text,
    "fn write_char(ch: char, buf: &mut String) {",
    '''fn write_attribute(value: &str, buf: &mut String) {
    for ch in value.chars() {
        match ch {
            '&' => buf.push_str("&amp;"),
            '<' => buf.push_str("&lt;"),
            '>' => buf.push_str("&gt;"),
            '"' => buf.push_str("&quot;"),
            ch => buf.push(ch),
        }
    }
}

fn write_char(ch: char, buf: &mut String) {''',
    "HTML attribute helper",
)
text += r'''

#[cfg(test)]
mod tests {
    use super::write_attribute;

    #[test]
    fn attributes_are_escaped() {
        let mut output = String::new();
        write_attribute("a&<b>\"", &mut output);
        assert_eq!(output, "a&amp;&lt;b&gt;&quot;");
    }
}
'''
path.write_text(text)

# Markdown writer: escape link destinations and remove infallible write! unwraps.
path = ROOT / "crates/teloxide/src/utils/render/markdown.rs"
text = path.read_text()
text = text.replace("use std::fmt::Write;\n\n", "", 1)
text = replace_once(
    text,
    '''                Some(lang) => {
                    write!(buf, "{}{}{}", MARKDOWN.pre.start, lang, MARKDOWN.pre.middle).unwrap()
                }''',
    '''                Some(lang) => {
                    buf.push_str(MARKDOWN.pre.start);
                    buf.push_str(lang);
                    buf.push_str(MARKDOWN.pre.middle);
                }''',
    "Markdown pre language",
)
text = replace_once(
    text,
    '''            Place::End => {
                write!(buf, "{}{}{}", MARKDOWN.text_link.middle, url, MARKDOWN.text_link.end)
                    .unwrap()
            }''',
    '''            Place::End => {
                buf.push_str(MARKDOWN.text_link.middle);
                write_link_destination(url, buf);
                buf.push_str(MARKDOWN.text_link.end);
            }''',
    "Markdown link",
)
text = replace_once(
    text,
    '''            Place::End => {
                write!(buf, "{}{}{}", MARKDOWN.text_mention.middle, id, MARKDOWN.text_mention.end)
                    .unwrap()
            }''',
    '''            Place::End => {
                buf.push_str(MARKDOWN.text_mention.middle);
                buf.push_str(&id.to_string());
                buf.push_str(MARKDOWN.text_mention.end);
            }''',
    "Markdown mention",
)
text = replace_once(
    text,
    '''            Place::End => write!(
                buf,
                "{}{}{}",
                MARKDOWN.custom_emoji.middle, custom_emoji_id, MARKDOWN.custom_emoji.end
            )
            .unwrap(),''',
    '''            Place::End => {
                buf.push_str(MARKDOWN.custom_emoji.middle);
                buf.push_str(&custom_emoji_id.0);
                buf.push_str(MARKDOWN.custom_emoji.end);
            }''',
    "Markdown emoji",
)
text = replace_once(
    text,
    '''            Place::End => {
                write!(buf, "](tg://time?unix={unix_time}").unwrap();
                if let Some(format) = date_time_format {
                    write!(buf, "&format={format}").unwrap();
                }
                buf.push(')');
            }''',
    '''            Place::End => {
                buf.push_str("](tg://time?unix=");
                buf.push_str(&unix_time.to_string());
                if let Some(format) = date_time_format {
                    buf.push_str("&format=");
                    write_link_destination(format, buf);
                }
                buf.push(')');
            }''',
    "Markdown time",
)
text = replace_once(
    text,
    "fn write_char(ch: char, buf: &mut String) {",
    '''fn write_link_destination(value: &str, buf: &mut String) {
    for ch in value.chars() {
        if matches!(ch, '\\' | ')') {
            buf.push('\\');
        }
        buf.push(ch);
    }
}

fn write_char(ch: char, buf: &mut String) {''',
    "Markdown destination helper",
)
text += r'''

#[cfg(test)]
mod tests {
    use super::write_link_destination;

    #[test]
    fn link_destinations_are_escaped() {
        let mut output = String::new();
        write_link_destination(r"a)\b", &mut output);
        assert_eq!(output, r"a\)\\b");
    }
}
'''
path.write_text(text)

# Remove both one-shot helpers and restore the normal workflow.
(ROOT / ".github/apply_renderer_audit.py").unlink()
subprocess.run(["git", "fetch", "origin", "next"], cwd=ROOT, check=True)
workflow = subprocess.run(
    ["git", "show", "origin/next:.github/workflows/ci.yml"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
).stdout
(ROOT / ".github/workflows/ci.yml").write_text(workflow)
Path(__file__).unlink()
