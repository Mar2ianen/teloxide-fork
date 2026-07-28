from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    file = ROOT / path
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}")
    file.write_text(text.replace(old, new, 1))


# Keep track of entities whose semantics have no explicit HTML/Markdown representation.
replace_once(
    "crates/teloxide/src/utils/render/mod.rs",
    """pub struct Renderer<'a> {
    text: &'a str,
    tags: Vec<Tag<'a>>,
}
""",
    """pub struct Renderer<'a> {
    text: &'a str,
    tags: Vec<Tag<'a>>,
    passthrough_entities: Vec<&'a MEK>,
}
""",
)
replace_once(
    "crates/teloxide/src/utils/render/mod.rs",
    """        let mut tags = Vec::with_capacity(needed_size);

        for (index, entity) in entities.iter().enumerate() {
""",
    """        let mut tags = Vec::with_capacity(needed_size);
        let mut passthrough_entities = Vec::new();

        for (index, entity) in entities.iter().enumerate() {
""",
)
replace_once(
    "crates/teloxide/src/utils/render/mod.rs",
    """                _ => continue,
            };
""",
    """                _ => {
                    passthrough_entities.push(&entity.kind);
                    continue;
                }
            };
""",
)
replace_once(
    "crates/teloxide/src/utils/render/mod.rs",
    """                let new_lines_indexes: Vec<usize> = text
                    .chars()
                    .skip(entity.offset)
                    .take(entity.length)
                    .enumerate()
                    .filter_map(|(idx, c)| (c == '\n').then_some(idx))
                    .collect();

                for new_line_index in new_lines_indexes.iter() {
                    tags.push(Tag::mid_new_line(
                        kind.clone(),
                        entity.offset + new_line_index + 1,
                        index,
                    ));
                }
""",
    """                let new_line_offsets = text
                    .encode_utf16()
                    .enumerate()
                    .skip(entity.offset)
                    .take(entity.length)
                    .filter_map(|(offset, unit)| (unit == u16::from(b'\n')).then_some(offset + 1));

                for new_line_offset in new_line_offsets {
                    tags.push(Tag::mid_new_line(kind.clone(), new_line_offset, index));
                }
""",
)
replace_once(
    "crates/teloxide/src/utils/render/mod.rs",
    """        Self { text, tags }
    }

    /// Renders text with a given [`TagWriter`].
""",
    """        Self { text, tags, passthrough_entities }
    }

    /// Returns entities whose semantic kind is preserved only as visible text.
    ///
    /// Telegram doesn't provide explicit HTML or MarkdownV2 markup for these
    /// entity kinds. The renderer keeps their text, but callers can inspect this
    /// iterator when a lossless semantic round-trip is required.
    pub fn passthrough_entities(&self) -> impl ExactSizeIterator<Item = &'a MEK> + '_ {
        self.passthrough_entities.iter().copied()
    }

    /// Returns `true` when at least one entity couldn't be represented as markup.
    #[must_use]
    pub fn has_passthrough_entities(&self) -> bool {
        !self.passthrough_entities.is_empty()
    }

    /// Renders text with a given [`TagWriter`].
""",
)

mod_path = ROOT / "crates/teloxide/src/utils/render/mod.rs"
mod_text = mod_path.read_text()
new_tests = r'''

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
'''
head, tail = mod_text.rsplit("\n}", 1)
mod_path.write_text(head + new_tests + "\n}" + tail)

# Escape values written inside HTML attributes.
html_path = ROOT / "crates/teloxide/src/utils/render/html.rs"
html = html_path.read_text()
html = html.replace(
    """                Some(lang) => write!(buf, "{}{}{}", HTML.pre.start, lang, HTML.pre.middle).unwrap(),
""",
    """                Some(lang) => {
                    buf.push_str(HTML.pre.start);
                    write_attribute(lang, buf);
                    buf.push_str(HTML.pre.middle);
                }
""",
    1,
)
html = html.replace(
    """            Place::Start => {
                write!(buf, "{}{}{}", HTML.text_link.start, url, HTML.text_link.middle).unwrap()
            }
""",
    """            Place::Start => {
                buf.push_str(HTML.text_link.start);
                write_attribute(url, buf);
                buf.push_str(HTML.text_link.middle);
            }
""",
    1,
)
html = html.replace(
    """            Place::Start => write!(
                buf,
                "{}{}{}",
                HTML.custom_emoji.start, custom_emoji_id, HTML.custom_emoji.middle
            )
            .unwrap(),
""",
    """            Place::Start => {
                buf.push_str(HTML.custom_emoji.start);
                write_attribute(&custom_emoji_id.0, buf);
                buf.push_str(HTML.custom_emoji.middle);
            }
""",
    1,
)
html = html.replace(
    """                if let Some(format) = date_time_format {
                    write!(buf, " format=\"{format}\"").unwrap();
                }
""",
    """                if let Some(format) = date_time_format {
                    buf.push_str(" format=\"");
                    write_attribute(format, buf);
                    buf.push('"');
                }
""",
    1,
)
html = html.replace(
    """fn write_char(ch: char, buf: &mut String) {
""",
    """fn write_attribute(value: &str, buf: &mut String) {
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

fn write_char(ch: char, buf: &mut String) {
""",
    1,
)
html += r'''

#[cfg(test)]
mod tests {
    use super::write_attribute;

    #[test]
    fn html_attributes_are_escaped() {
        let mut output = String::new();
        write_attribute("a&<b>\"", &mut output);
        assert_eq!(output, "a&amp;&lt;b&gt;&quot;");
    }
}
'''
html_path.write_text(html)

# Escape MarkdownV2 link destinations.
markdown_path = ROOT / "crates/teloxide/src/utils/render/markdown.rs"
markdown = markdown_path.read_text()
markdown = markdown.replace(
    """            Place::End => {
                write!(buf, "{}{}{}", MARKDOWN.text_link.middle, url, MARKDOWN.text_link.end)
                    .unwrap()
            }
""",
    """            Place::End => {
                buf.push_str(MARKDOWN.text_link.middle);
                write_link_destination(url, buf);
                buf.push_str(MARKDOWN.text_link.end);
            }
""",
    1,
)
markdown = markdown.replace(
    """                if let Some(format) = date_time_format {
                    write!(buf, "&format={format}").unwrap();
                }
""",
    """                if let Some(format) = date_time_format {
                    buf.push_str("&format=");
                    write_link_destination(format, buf);
                }
""",
    1,
)
markdown = markdown.replace(
    """fn write_char(ch: char, buf: &mut String) {
""",
    """fn write_link_destination(value: &str, buf: &mut String) {
    for ch in value.chars() {
        if matches!(ch, '\\' | ')') {
            buf.push('\\');
        }
        buf.push(ch);
    }
}

fn write_char(ch: char, buf: &mut String) {
""",
    1,
)
markdown += r'''

#[cfg(test)]
mod tests {
    use super::write_link_destination;

    #[test]
    fn markdown_link_destinations_are_escaped() {
        let mut output = String::new();
        write_link_destination(r"a)\b", &mut output);
        assert_eq!(output, r"a\)\\b");
    }
}
'''
markdown_path.write_text(markdown)

# Add integration-level attribute and link tests.
mod_text = mod_path.read_text()
extra = r'''

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
                    date_time_format: Some(r#"x&\")\\"#.to_owned()),
                },
                offset: 5,
                length: 4,
            },
            MessageEntity {
                kind: MEK::Pre { language: Some(r#"ru&\"st"#.to_owned()) },
                offset: 10,
                length: 4,
            },
        ];
        let render = Renderer::new(text, &entities);

        assert_eq!(
            render.as_html(),
            "<a href=\"https://example.com/a)?x=1&amp;y=2\">link</a> <tg-time unix=\"1\" format=\"x&amp;\\&quot;)\\\">time</tg-time> <pre><code class=\"language-ru&amp;\\&quot;st\">code</code></pre>"
        );
        assert_eq!(
            render.as_markdown(),
            "[link](https://example.com/a\\)?x=1&y=2) ![time](tg://time?unix=1&format=x&\\\\\"\\)\\\\) ```ru&\"st\ncode```\n"
        );
    }
'''
head, tail = mod_text.rsplit("\n}", 1)
mod_path.write_text(head + extra + "\n}" + tail)

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
