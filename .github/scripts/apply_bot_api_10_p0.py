from __future__ import annotations

from pathlib import Path


def ron_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def param(name: str, ty: str, description: str) -> str:
    return f'''                Param(
                    name: "{name}",
                    ty: {ty},
                    descr: Doc(md: "{ron_string(description)}"),
                ),'''


def method_bounds(text: str, api_name: str) -> tuple[int, int]:
    marker = f'names: ("{api_name}",'
    marker_pos = text.index(marker)
    start = text.rfind("        Method(", 0, marker_pos)
    end = text.find("\n        Method(", marker_pos)
    if end == -1:
        end = text.index("\n    ],", marker_pos)
    return start, end


def param_span(block: str, name: str) -> tuple[int, int]:
    marker_pos = block.index(f'name: "{name}"')
    start = block.rfind("                Param(", 0, marker_pos)
    end = block.find("\n                Param(", marker_pos)
    if end == -1:
        end = block.index("\n            ],", marker_pos)
    return start, end


def replace_param(block: str, name: str, replacement: str) -> str:
    start, end = param_span(block, name)
    return block[:start] + replacement.rstrip("\n") + block[end:]


def insert_after_param(block: str, name: str, addition: str) -> str:
    _, end = param_span(block, name)
    return block[:end] + "\n" + addition.rstrip("\n") + block[end:]


def replace_once(text: str, old: str, new: str, description: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{description}: expected one match, got {count}")
    return text.replace(old, new, 1)


schema_path = Path("crates/teloxide-core/schema.ron")
schema = schema_path.read_text()


def update_method(api_name: str, transform) -> None:
    global schema
    start, end = method_bounds(schema, api_name)
    old = schema[start:end]
    new = transform(old)
    if new == old:
        raise RuntimeError(f"method {api_name} was not changed")
    schema = schema[:start] + new + schema[end:]


def update_send_poll(block: str) -> str:
    block = replace_once(
        block,
        "A JSON-serialized list of 2-10 answer options",
        "A JSON-serialized list of 1-12 answer options",
        "sendPoll options range",
    )
    block = replace_once(
        block,
        "True, if the poll allows multiple answers, ignored for polls in quiz mode, defaults to False",
        "Pass True if the poll allows multiple answers, defaults to False",
        "sendPoll allows_multiple_answers docs",
    )

    poll_controls = "\n".join(
        [
            param(
                "allows_revoting",
                "Option(bool)",
                "Pass True if the poll allows changing chosen answer options; defaults to False for quizzes and True for regular polls",
            ),
            param(
                "shuffle_options",
                "Option(bool)",
                "Pass True if the poll options must be shown in random order",
            ),
            param(
                "allow_adding_options",
                "Option(bool)",
                "Pass True if answer options can be added after creation; not supported for anonymous polls and quizzes",
            ),
            param(
                "hide_results_until_closes",
                "Option(bool)",
                "Pass True if poll results must be shown only after the poll closes",
            ),
            param(
                "members_only",
                "Option(bool)",
                "Pass True if voting is limited to users who have been members of the target chat for more than 24 hours; for channel chats only",
            ),
            param(
                "country_codes",
                "Option(ArrayOf(String))",
                "A JSON-serialized list of 0-12 two-letter ISO 3166-1 alpha-2 country codes allowed to vote; use FT for anonymous numbers",
            ),
            param(
                "correct_option_ids",
                "Option(ArrayOf(u8))",
                "A JSON-serialized list of monotonically increasing 0-based identifiers of correct answers, required for quiz polls",
            ),
        ]
    )
    block = replace_param(block, "correct_option_id", poll_controls)

    block = insert_after_param(
        block,
        "explanation_entities",
        param(
            "explanation_media",
            'Option(RawTy("InputPollMedia"))',
            "Media added to the quiz explanation",
        ),
    )
    block = replace_param(
        block,
        "open_period",
        param(
            "open_period",
            "Option(u32)",
            "Amount of time in seconds the poll will be active after creation, 5-2628000. Can't be used together with close_date.",
        ),
    )
    block = replace_param(
        block,
        "close_date",
        param(
            "close_date",
            "Option(u64)",
            "Point in time when the poll will automatically close. Must be at least 5 and no more than 2628000 seconds in the future. Can't be used together with open_period.",
        ),
    )

    rich_poll = "\n".join(
        [
            param(
                "description",
                "Option(String)",
                "Description of the poll to be sent, 0-1024 characters after entities parsing",
            ),
            param(
                "description_parse_mode",
                'Option(RawTy("ParseMode"))',
                "Mode for parsing entities in the poll description",
            ),
            param(
                "description_entities",
                'Option(ArrayOf(RawTy("MessageEntity")))',
                "A JSON-serialized list of special entities in the poll description, specified instead of description_parse_mode",
            ),
            param(
                "media",
                'Option(RawTy("InputPollMedia"))',
                "Media added to the poll description",
            ),
        ]
    )
    return insert_after_param(block, "is_closed", rich_poll)


update_method("sendPoll", update_send_poll)


def add_optional_after(
    api_name: str,
    after: str,
    name: str,
    ty: str,
    description: str,
) -> None:
    def transform(block: str) -> str:
        if f'name: "{name}"' in block:
            raise RuntimeError(f"{api_name}.{name} already exists")
        return insert_after_param(block, after, param(name, ty, description))

    update_method(api_name, transform)


add_optional_after(
    "getChatAdministrators",
    "chat_id",
    "return_bots",
    "Option(bool)",
    "Pass True to additionally receive all bots that are administrators of the chat; other bots are omitted by default",
)
add_optional_after(
    "promoteChatMember",
    "can_manage_direct_messages",
    "can_manage_tags",
    "Option(bool)",
    "Pass True if the administrator can edit tags of regular members; for groups and supergroups only",
)
add_optional_after(
    "forwardMessage",
    "protect_content",
    "message_effect_id",
    'Option(RawTy("EffectId"))',
    "Unique identifier of the message effect to add to the forwarded message; for private chats only",
)
add_optional_after(
    "copyMessage",
    "allow_paid_broadcast",
    "message_effect_id",
    'Option(RawTy("EffectId"))',
    "Unique identifier of the message effect to add to the copied message; for private chats only",
)

schema_path.write_text(schema)

poll_media_path = Path("crates/teloxide-core/src/types/poll_media.rs")
poll_media = poll_media_path.read_text()
poll_media = replace_once(
    poll_media,
    '''pub enum InputPollOptionMedia {
    Photo(crate::types::InputMediaPhoto),
    Sticker(crate::types::InputMediaSticker),
    Video(crate::types::InputMediaVideo),
}''',
    '''pub enum InputPollOptionMedia {
    Animation(crate::types::InputMediaAnimation),
    LivePhoto(crate::types::InputMediaLivePhoto),
    Location(crate::types::InputMediaLocation),
    Photo(crate::types::InputMediaPhoto),
    Sticker(crate::types::InputMediaSticker),
    Venue(crate::types::InputMediaVenue),
    Video(crate::types::InputMediaVideo),
}''',
    "InputPollOptionMedia variants",
)
poll_media_path.write_text(poll_media)

input_poll_option_path = Path("crates/teloxide-core/src/types/input_poll_option.rs")
input_poll_option = input_poll_option_path.read_text()
impl_start = input_poll_option.index("impl InputPollOption {")
impl_end = input_poll_option.index("\n}\n\nimpl From<String>", impl_start)
media_setter = '''

    pub fn media(mut self, media: InputPollOptionMedia) -> Self {
        self.media = Some(media);
        self
    }'''
input_poll_option = input_poll_option[:impl_end] + media_setter + input_poll_option[impl_end:]
input_poll_option_path.write_text(input_poll_option)

render_path = Path("crates/teloxide/src/utils/render.rs")
render = render_path.read_text()
render = replace_once(
    render,
    '''                        | MEK::CustomEmoji { .. }
                )''',
    '''                        | MEK::CustomEmoji { .. }
                        | MEK::DateTime { unix_time: Some(_), .. }
                )''',
    "DateTime renderer filter",
)
render = replace_once(
    render,
    '''            MEK::CustomEmoji { custom_emoji_id } => Kind::CustomEmoji(custom_emoji_id),
            _ => continue,''',
    '''            MEK::CustomEmoji { custom_emoji_id } => Kind::CustomEmoji(custom_emoji_id),
            MEK::DateTime { unix_time: Some(unix_time), date_time_format } => Kind::DateTime {
                unix_time: *unix_time,
                date_time_format: date_time_format.as_deref(),
            },
            _ => continue,''',
    "DateTime renderer mapping",
)
date_time_test = '''    #[test]
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
            "<tg-time unix=\\\"1647531900\\\" format=\\\"wDT\\\">tomorrow</tg-time>"
        );
        assert_eq!(
            render.as_markdown(),
            "![tomorrow](tg://time?unix=1647531900&format=wDT)"
        );
    }

'''
render = replace_once(
    render,
    "    #[test]\n    fn test_render_nested() {",
    date_time_test + "    #[test]\n    fn test_render_nested() {",
    "DateTime renderer test anchor",
)
render_path.write_text(render)

tag_path = Path("crates/teloxide/src/utils/render/tag.rs")
tag = tag_path.read_text()
tag = replace_once(
    tag,
    "    CustomEmoji(&'a CustomEmojiId),\n}",
    "    CustomEmoji(&'a CustomEmojiId),\n    DateTime { unix_time: i64, date_time_format: Option<&'a str> },\n}",
    "DateTime tag kind",
)
tag = replace_once(
    tag,
    '''            Kind::CustomEmoji(custom_emoji_id) => match tag.place {
                Place::Start => self.custom_emoji.start.len() + custom_emoji_id.0.len(),
                Place::MidNewLine => unreachable!(),
                Place::End => self.custom_emoji.middle.len() + self.custom_emoji.end.len(),
            },''',
    '''            Kind::CustomEmoji(custom_emoji_id) => match tag.place {
                Place::Start => self.custom_emoji.start.len() + custom_emoji_id.0.len(),
                Place::MidNewLine => unreachable!(),
                Place::End => self.custom_emoji.middle.len() + self.custom_emoji.end.len(),
            },
            Kind::DateTime { unix_time, date_time_format } => {
                64 + unix_time.to_string().len() + date_time_format.map_or(0, str::len)
            }''',
    "DateTime tag capacity",
)
tag_path.write_text(tag)

html_path = Path("crates/teloxide/src/utils/render/html.rs")
html = html_path.read_text()
html = replace_once(
    html,
    '''        Kind::CustomEmoji(custom_emoji_id) => match tag.place {
            Place::Start => write!(
                buf,
                "{}{}{}",
                HTML.custom_emoji.start, custom_emoji_id, HTML.custom_emoji.middle
            )
            .unwrap(),
            Place::MidNewLine => unreachable!(),
            Place::End => buf.push_str(HTML.custom_emoji.end),
        },''',
    '''        Kind::CustomEmoji(custom_emoji_id) => match tag.place {
            Place::Start => write!(
                buf,
                "{}{}{}",
                HTML.custom_emoji.start, custom_emoji_id, HTML.custom_emoji.middle
            )
            .unwrap(),
            Place::MidNewLine => unreachable!(),
            Place::End => buf.push_str(HTML.custom_emoji.end),
        },
        Kind::DateTime { unix_time, date_time_format } => match tag.place {
            Place::Start => {
                write!(buf, "<tg-time unix=\\\"{unix_time}\\\"").unwrap();
                if let Some(format) = date_time_format {
                    write!(buf, " format=\\\"{format}\\\"").unwrap();
                }
                buf.push('>');
            }
            Place::MidNewLine => unreachable!(),
            Place::End => buf.push_str("</tg-time>"),
        },''',
    "DateTime HTML writer",
)
html_path.write_text(html)

markdown_path = Path("crates/teloxide/src/utils/render/markdown.rs")
markdown = markdown_path.read_text()
markdown = replace_once(
    markdown,
    '''        Kind::CustomEmoji(custom_emoji_id) => match tag.place {
            Place::Start => buf.push_str(MARKDOWN.custom_emoji.start),
            Place::MidNewLine => unreachable!(),
            Place::End => write!(
                buf,
                "{}{}{}",
                MARKDOWN.custom_emoji.middle, custom_emoji_id, MARKDOWN.custom_emoji.end
            )
            .unwrap(),
        },''',
    '''        Kind::CustomEmoji(custom_emoji_id) => match tag.place {
            Place::Start => buf.push_str(MARKDOWN.custom_emoji.start),
            Place::MidNewLine => unreachable!(),
            Place::End => write!(
                buf,
                "{}{}{}",
                MARKDOWN.custom_emoji.middle, custom_emoji_id, MARKDOWN.custom_emoji.end
            )
            .unwrap(),
        },
        Kind::DateTime { unix_time, date_time_format } => match tag.place {
            Place::Start => buf.push_str("!["),
            Place::MidNewLine => unreachable!(),
            Place::End => {
                write!(buf, "](tg://time?unix={unix_time}").unwrap();
                if let Some(format) = date_time_format {
                    write!(buf, "&format={format}").unwrap();
                }
                buf.push(')');
            }
        },''',
    "DateTime Markdown writer",
)
markdown_path.write_text(markdown)

for readme_path in [Path("README.md"), Path("crates/teloxide-core/README.md")]:
    readme = readme_path.read_text()
    readme = readme.replace(
        "API%20coverage-Up%20to%2010.0%20(inclusively)-green.svg",
        "API%20coverage-Bot%20API%2010.0%20core-yellowgreen.svg",
    )
    readme_path.write_text(readme)
