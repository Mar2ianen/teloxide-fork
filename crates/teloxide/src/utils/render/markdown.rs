use crate::utils::markdown::ESCAPE_CHARS;

use super::{ComplexTag, Kind, NewLineRepeatedTag, Place, SimpleTag, Tag, TagWriter};

pub static MARKDOWN: TagWriter = TagWriter {
    bold: SimpleTag::new("*", "*"),
    blockquote: NewLineRepeatedTag::new("**>", ">", ""),
    expandable_blockquote: NewLineRepeatedTag::new("**>", ">", "||"),
    italic: SimpleTag::new("_\r", "_\r"),
    underline: SimpleTag::new("__\r", "__\r"),
    strikethrough: SimpleTag::new("~", "~"),
    spoiler: SimpleTag::new("||", "||"),
    code: SimpleTag::new("`", "`"),
    pre_no_lang: SimpleTag::new("```\n", "```\n"),
    pre: ComplexTag::new("```", "\n", "```\n"),
    text_link: ComplexTag::new("[", "](", ")"),
    text_mention: ComplexTag::new("[", "](tg://user?id=", ")"),
    custom_emoji: ComplexTag::new("[", "](tg://emoji?id=", ")"),
    write_tag_fn: write_tag,
    write_char_fn: write_char,
};

fn write_tag(tag: &Tag, buf: &mut String) {
    match tag.kind {
        Kind::Bold => buf.push_str(MARKDOWN.bold.get_tag(tag.place)),
        Kind::Blockquote => match tag.place {
            Place::Start => buf.push_str(MARKDOWN.blockquote.start),
            Place::MidNewLine => buf.push_str(MARKDOWN.blockquote.repeat),
            Place::End => buf.push_str(MARKDOWN.blockquote.end),
        },
        Kind::ExpandableBlockquote => match tag.place {
            Place::Start => buf.push_str(MARKDOWN.expandable_blockquote.start),
            Place::MidNewLine => buf.push_str(MARKDOWN.expandable_blockquote.repeat),
            Place::End => buf.push_str(MARKDOWN.expandable_blockquote.end),
        },
        Kind::Italic => buf.push_str(MARKDOWN.italic.get_tag(tag.place)),
        Kind::Underline => buf.push_str(MARKDOWN.underline.get_tag(tag.place)),
        Kind::Strikethrough => buf.push_str(MARKDOWN.strikethrough.get_tag(tag.place)),
        Kind::Spoiler => buf.push_str(MARKDOWN.spoiler.get_tag(tag.place)),
        Kind::Code => buf.push_str(MARKDOWN.code.get_tag(tag.place)),
        Kind::Pre(lang) => match tag.place {
            Place::Start => match lang {
                Some(lang) => {
                    buf.push_str(MARKDOWN.pre.start);
                    write_pre_language(lang, buf);
                    buf.push_str(MARKDOWN.pre.middle);
                }
                None => buf.push_str(MARKDOWN.pre_no_lang.start),
            },
            Place::MidNewLine => unreachable!(),
            Place::End => buf.push_str(lang.map_or(MARKDOWN.pre_no_lang.end, |_| MARKDOWN.pre.end)),
        },
        Kind::TextLink(url) => match tag.place {
            Place::Start => buf.push_str(MARKDOWN.text_link.start),
            Place::MidNewLine => unreachable!(),
            Place::End => {
                buf.push_str(MARKDOWN.text_link.middle);
                write_link_destination(url, buf);
                buf.push_str(MARKDOWN.text_link.end);
            }
        },
        Kind::TextMention(id) => match tag.place {
            Place::Start => buf.push_str(MARKDOWN.text_mention.start),
            Place::MidNewLine => unreachable!(),
            Place::End => {
                buf.push_str(MARKDOWN.text_mention.middle);
                buf.push_str(&id.to_string());
                buf.push_str(MARKDOWN.text_mention.end);
            }
        },
        Kind::CustomEmoji(custom_emoji_id) => match tag.place {
            Place::Start => buf.push_str(MARKDOWN.custom_emoji.start),
            Place::MidNewLine => unreachable!(),
            Place::End => {
                buf.push_str(MARKDOWN.custom_emoji.middle);
                buf.push_str(&custom_emoji_id.0);
                buf.push_str(MARKDOWN.custom_emoji.end);
            }
        },
        Kind::DateTime { unix_time, date_time_format } => match tag.place {
            Place::Start => buf.push_str("!["),
            Place::MidNewLine => unreachable!(),
            Place::End => {
                buf.push_str("](tg://time?unix=");
                buf.push_str(&unix_time.to_string());
                if let Some(format) = date_time_format {
                    buf.push_str("&format=");
                    write_link_destination(format, buf);
                }
                buf.push(')');
            }
        },
    }
}

fn write_link_destination(value: &str, buf: &mut String) {
    for ch in value.chars() {
        if matches!(ch, '\\' | ')') {
            buf.push('\\');
        }
        buf.push(ch);
    }
}

fn write_pre_language(language: &str, buf: &mut String) {
    for ch in language.chars() {
        if matches!(ch, '\r' | '\n') {
            continue;
        }
        if matches!(ch, '`' | '\\') {
            buf.push('\\');
        }
        buf.push(ch);
    }
}

fn write_char(ch: char, buf: &mut String) {
    if ESCAPE_CHARS.contains(&ch) {
        buf.push('\\');
    }
    buf.push(ch);
}

#[cfg(test)]
mod tests {
    use super::{write_link_destination, write_tag};
    use crate::utils::render::{Kind, Tag};

    #[test]
    fn link_destinations_are_escaped() {
        let mut output = String::new();
        write_link_destination(r"a)\b", &mut output);
        assert_eq!(output, r"a\)\\b");
    }

    #[test]
    fn pre_languages_are_escaped() {
        for (language, expected) in [
            ("rust", "```rust\n"),
            ("ru`st", "```ru\\`st\n"),
            (r"ru\st", "```ru\\\\st\n"),
            ("ru\nst\r", "```rust\n"),
        ] {
            let mut output = String::new();
            write_tag(&Tag::start(Kind::Pre(Some(language)), 0, 0), &mut output);
            assert_eq!(output, expected);
        }
    }
}
