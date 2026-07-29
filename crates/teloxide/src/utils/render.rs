//! Utils for rendering HTML and Markdown output.

use teloxide_core::types::{MessageEntity, MessageEntityKind as MEK};

use tag::*;

pub use helper::RenderMessageTextHelper;

mod helper;
mod html;
mod markdown;
mod tag;

/// Parses text and message entities to produce the final formatted output.
#[derive(Clone, Eq, PartialEq)]
pub struct Renderer<'a> {
    text: &'a str,
    tags: Vec<Tag<'a>>,
    passthrough_entities: Vec<&'a MEK>,
}

impl<'a> Renderer<'a> {
    /// Creates a new [`Renderer`] instance with given text and message
    /// entities.
    ///
    /// # Arguments
    ///
    /// - `text`: The input text to be parsed.
    /// - `entities`: The message entities (formatting, links, etc.) to be
    ///   applied to the text.
    #[must_use]
    pub fn new(text: &'a str, entities: &'a [MessageEntity]) -> Self {
        // get the needed size for the new tags that we want to parse from entities
        let needed_size: usize = entities
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    MEK::Bold
                        | MEK::Blockquote
                        | MEK::ExpandableBlockquote
                        | MEK::Italic
                        | MEK::Underline
                        | MEK::Strikethrough
                        | MEK::Spoiler
                        | MEK::Code
                        | MEK::Pre { .. }
                        | MEK::TextLink { .. }
                        | MEK::TextMention { .. }
                        | MEK::CustomEmoji { .. }
                        | MEK::DateTime { unix_time: Some(_), .. }
                )
            })
            .count()
            * 2; // 2 because we insert two tags for each entity

        let mut tags = Vec::with_capacity(needed_size);
        let mut passthrough_entities = Vec::new();

        for (index, entity) in entities.iter().enumerate() {
            let kind = match &entity.kind {
                MEK::Bold => Kind::Bold,
                MEK::Blockquote => Kind::Blockquote,
                MEK::ExpandableBlockquote => Kind::ExpandableBlockquote,
                MEK::Italic => Kind::Italic,
                MEK::Underline => Kind::Underline,
                MEK::Strikethrough => Kind::Strikethrough,
                MEK::Spoiler => Kind::Spoiler,
                MEK::Code => Kind::Code,
                MEK::Pre { language } => Kind::Pre(language.as_ref().map(String::as_str)),
                MEK::TextLink { url } => Kind::TextLink(url.as_str()),
                MEK::TextMention { user } => Kind::TextMention(user.id.0),
                MEK::CustomEmoji { custom_emoji_id } => Kind::CustomEmoji(custom_emoji_id),
                MEK::DateTime { unix_time: Some(unix_time), date_time_format } => Kind::DateTime {
                    unix_time: *unix_time,
                    date_time_format: date_time_format.as_deref(),
                },
                _ => {
                    passthrough_entities.push(&entity.kind);
                    continue;
                }
            };

            // FIXME: maybe instead of clone store all the `kind`s in a seperate
            // vector and then just store the index here?
            tags.push(Tag::start(kind.clone(), entity.offset, index));

            if matches!(kind, Kind::Blockquote | Kind::ExpandableBlockquote) {
                let new_line_offsets = text
                    .encode_utf16()
                    .enumerate()
                    .skip(entity.offset)
                    .take(entity.length)
                    .filter_map(|(offset, unit)| (unit == u16::from(b'\n')).then_some(offset + 1));

                for new_line_offset in new_line_offsets {
                    tags.push(Tag::mid_new_line(kind.clone(), new_line_offset, index));
                }
            }

            tags.push(Tag::end(kind, entity.offset + entity.length, index));
        }

        tags.sort_unstable();

        Self { text, tags, passthrough_entities }
    }

    /// Returns entities whose semantic kind is preserved only as visible text.
    ///
    /// Telegram doesn't provide explicit HTML or MarkdownV2 markup for these
    /// entity kinds. The renderer keeps their text, while callers can inspect
    /// this iterator when a lossless semantic round-trip is required.
    pub fn passthrough_entities(&self) -> impl ExactSizeIterator<Item = &'a MEK> + '_ {
        self.passthrough_entities.iter().copied()
    }

    /// Returns `true` when at least one entity couldn't be represented as
    /// markup.
    #[must_use]
    pub fn has_passthrough_entities(&self) -> bool {
        !self.passthrough_entities.is_empty()
    }

    /// Renders text with a given [`TagWriter`].
    ///
    /// This method iterates through the text and the associated position tags
    /// and writes the text with the appropriate tags to a buffer, which is then
    /// returned as a `String`.
    ///
    /// If input have no tags we just return the original text as-is.
    #[must_use]
    fn format(&self, writer: &TagWriter) -> String {
        if self.tags.is_empty() {
            return self.text.to_owned();
        }

        let mut buffer =
            String::with_capacity(self.text.len() + writer.get_extra_size_for_tags(&self.tags));
        let mut tags = self.tags.iter();
        let mut current_tag = tags.next();

        let mut prev_point = None;

        for (idx, point) in self.text.encode_utf16().enumerate() {
            loop {
                match current_tag {
                    Some(tag) if tag.offset == idx => {
                        (writer.write_tag_fn)(tag, &mut buffer);
                        current_tag = tags.next();
                    }
                    _ => break,
                }
            }

            let ch = if let Some(previous) = prev_point.take() {
                char::decode_utf16([previous, point]).next().unwrap().unwrap()
            } else {
                match char::decode_utf16([point]).next().unwrap() {
                    Ok(c) => c,
                    Err(unpaired) => {
                        prev_point = Some(unpaired.unpaired_surrogate());
                        continue;
                    }
                }
            };

            (writer.write_char_fn)(ch, &mut buffer);
        }

        for tag in current_tag.into_iter().chain(tags) {
            (writer.write_tag_fn)(tag, &mut buffer);
        }

        buffer
    }

    /// Renders and returns the text as an **HTML-formatted** string.
    #[must_use]
    #[inline]
    pub fn as_html(&self) -> String {
        self.format(&html::HTML)
    }

    /// Renders and returns the text as a **MarkdownV2-formatted** string.
    #[must_use]
    #[inline]
    pub fn as_markdown(&self) -> String {
        self.format(&markdown::MARKDOWN)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_render_simple() {
        let text = "Bold italic <underline_";
        let entities = vec![
            MessageEntity { kind: MEK::Bold, offset: 0, length: 4 },
            MessageEntity { kind: MEK::Italic, offset: 5, length: 6 },
            MessageEntity { kind: MEK::Underline, offset: 12, length: 10 },
        ];

        let render = Renderer::new(text, &entities);

        assert_eq!(render.as_html(), "<b>Bold</b> <i>italic</i> <u>&lt;underline</u>_");
        assert_eq!(render.as_markdown(), "*Bold* _\ritalic_\r __\r<underline__\r\\_");
    }

    #[test]
    fn test_render_pre_with_lang() {
        let text = "Some pre, normal and rusty code";
        let entities = vec![
            MessageEntity { kind: MEK::Pre { language: None }, offset: 5, length: 3 },
            MessageEntity { kind: MEK::Code, offset: 10, length: 6 },
            MessageEntity {
                kind: MEK::Pre { language: Some("rust".to_owned()) },
                offset: 21,
                length: 5,
            },
        ];

        let render = Renderer::new(text, &entities);

        assert_eq!(
            render.as_html(),
            "Some <pre>pre</pre>, <code>normal</code> and <pre><code \
             class=\"language-rust\">rusty</code></pre> code",
        );
        assert_eq!(
            render.as_markdown(),
            "Some ```\npre```\n, `normal` and ```rust\nrusty```\n code",
        );
    }

    #[test]
    fn test_render_date_time() {
        let text = "tomorrow";
        let entities = vec![MessageEntity {
            kind: MEK::DateTime {
                unix_time: Some(1_647_531_900),
                date_time_format: Some("wDT".to_owned()),
            },
            offset: 0,
            length: 8,
        }];

        let render = Renderer::new(text, &entities);

        assert_eq!(
            render.as_html(),
            "<tg-time unix=\"1647531900\" format=\"wDT\">tomorrow</tg-time>"
        );
        assert_eq!(render.as_markdown(), "![tomorrow](tg://time?unix=1647531900&format=wDT)");
    }

    #[test]
    fn test_render_nested() {
        let text = "Some bold both italics";
        let entities = vec![
            MessageEntity { kind: MEK::Bold, offset: 5, length: 9 },
            MessageEntity { kind: MEK::Italic, offset: 10, length: 12 },
        ];

        let render = Renderer::new(text, &entities);

        assert_eq!(render.as_html(), "Some <b>bold <i>both</b> italics</i>");
        assert_eq!(render.as_markdown(), "Some *bold _\rboth* italics_\r");
    }

    #[test]
    fn test_render_complex() {
        let text = "Hi how are you?\nnested entities are cool\nIm in a Blockquote!\nIm in a \
                    multiline Blockquote!\n\nIm in a multiline Blockquote!\nIm in an expandable \
                    Blockquote!\nIm in an expandable multiline Blockquote!\n\nIm in an expandable \
                    multiline Blockquote!";
        let entities = vec![
            MessageEntity { kind: MEK::Bold, offset: 0, length: 2 },
            MessageEntity { kind: MEK::Italic, offset: 3, length: 3 },
            MessageEntity { kind: MEK::Underline, offset: 7, length: 3 },
            MessageEntity { kind: MEK::Strikethrough, offset: 11, length: 3 },
            MessageEntity { kind: MEK::Bold, offset: 16, length: 1 },
            MessageEntity { kind: MEK::Bold, offset: 17, length: 5 },
            MessageEntity { kind: MEK::Underline, offset: 17, length: 4 },
            MessageEntity { kind: MEK::Strikethrough, offset: 17, length: 4 },
            MessageEntity {
                kind: MEK::TextLink { url: reqwest::Url::parse("https://t.me/").unwrap() },
                offset: 23,
                length: 8,
            },
            MessageEntity {
                kind: MEK::TextLink { url: reqwest::Url::parse("tg://user?id=1234567").unwrap() },
                offset: 32,
                length: 3,
            },
            MessageEntity { kind: MEK::Code, offset: 36, length: 4 },
            MessageEntity { kind: MEK::Blockquote, offset: 41, length: 19 },
            MessageEntity { kind: MEK::Blockquote, offset: 61, length: 60 },
            MessageEntity { kind: MEK::ExpandableBlockquote, offset: 122, length: 31 },
            MessageEntity { kind: MEK::ExpandableBlockquote, offset: 154, length: 84 },
        ];

        let render = Renderer::new(text, &entities);

        assert_eq!(
            render.as_html(),
            "<b>Hi</b> <i>how</i> <u>are</u> <s>you</s>?\n<b>n</b><b><u><s>este</s></u>d</b> \
            <a href=\"https://t.me/\">entities</a> <a href=\"tg://user?id=1234567\">are</a> <code>cool</code>\n\
            <blockquote>Im in a Blockquote!</blockquote>\n\
            <blockquote>Im in a multiline Blockquote!\n\nIm in a multiline Blockquote!</blockquote>\n\
            <blockquote expandable>Im in an expandable Blockquote!</blockquote>\n\
            <blockquote expandable>Im in an expandable multiline Blockquote!\n\nIm in an expandable multiline Blockquote!</blockquote>"
        );
        assert_eq!(
            render.as_markdown(),
            "*Hi* _\rhow_\r __\rare__\r ~you~?\n*n**__\r~este~__\rd* [entities](https://t.me/) \
             [are](tg://user?id=1234567) `cool`\n**>Im in a Blockquote\\!\n**>Im in a multiline \
             Blockquote\\!\n>\n>Im in a multiline Blockquote\\!\n**>Im in an expandable \
             Blockquote\\!||\n**>Im in an expandable multiline Blockquote\\!\n>\n>Im in an \
             expandable multiline Blockquote\\!||"
        );
    }

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
        assert_eq!(render.as_markdown(), text);
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
            "<a href=\"https://example.com/a)?x=1&amp;y=2\">link</a> <tg-time unix=\"1\" \
             format=\"x&amp;&quot;)\\\">time</tg-time> <pre><code \
             class=\"language-ru&amp;&quot;\">code</code></pre>"
        );
        assert_eq!(
            render.as_markdown(),
            "[link](https://example.com/a\\)?x=1&y=2) \
             ![time](tg://time?unix=1&format=x&\"\\)\\\\) ```ru&\"\ncode```\n"
        );
    }
}
