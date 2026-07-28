from pathlib import Path

path = Path(".github/scripts/apply_bot_api_10_p0.py")
text = path.read_text()

old_render = "\n".join(
    [
        "    '''            MEK::CustomEmoji { custom_emoji_id } => Kind::CustomEmoji(custom_emoji_id),",
        "            _ => continue,''',",
        "    '''            MEK::CustomEmoji { custom_emoji_id } => Kind::CustomEmoji(custom_emoji_id),",
        "            MEK::DateTime { unix_time: Some(unix_time), date_time_format } => Kind::DateTime {",
        "                unix_time: *unix_time,",
        "                date_time_format: date_time_format.as_deref(),",
        "            },",
        "            _ => continue,''',",
    ]
)
new_render = "\n".join(
    [
        "    '''                MEK::CustomEmoji { custom_emoji_id } => Kind::CustomEmoji(custom_emoji_id),",
        "                _ => continue,''',",
        "    '''                MEK::CustomEmoji { custom_emoji_id } => Kind::CustomEmoji(custom_emoji_id),",
        "                MEK::DateTime { unix_time: Some(unix_time), date_time_format } => Kind::DateTime {",
        "                    unix_time: *unix_time,",
        "                    date_time_format: date_time_format.as_deref(),",
        "                },",
        "                _ => continue,''',",
    ]
)

old_tag = "\n".join(
    [
        "    '''            Kind::CustomEmoji(custom_emoji_id) => match tag.place {",
        "                Place::Start => self.custom_emoji.start.len() + custom_emoji_id.0.len(),",
        "                Place::MidNewLine => unreachable!(),",
        "                Place::End => self.custom_emoji.middle.len() + self.custom_emoji.end.len(),",
        "            },''',",
        "    '''            Kind::CustomEmoji(custom_emoji_id) => match tag.place {",
        "                Place::Start => self.custom_emoji.start.len() + custom_emoji_id.0.len(),",
        "                Place::MidNewLine => unreachable!(),",
        "                Place::End => self.custom_emoji.middle.len() + self.custom_emoji.end.len(),",
        "            },",
        "            Kind::DateTime { unix_time, date_time_format } => {",
        "                64 + unix_time.to_string().len() + date_time_format.map_or(0, str::len)",
        "            }''',",
    ]
)
new_tag = "\n".join(
    [
        "    '''                Kind::CustomEmoji(custom_emoji_id) => match tag.place {",
        "                    Place::Start => self.custom_emoji.start.len() + custom_emoji_id.0.len(),",
        "                    Place::MidNewLine => unreachable!(),",
        "                    Place::End => self.custom_emoji.middle.len() + self.custom_emoji.end.len(),",
        "                },''',",
        "    '''                Kind::CustomEmoji(custom_emoji_id) => match tag.place {",
        "                    Place::Start => self.custom_emoji.start.len() + custom_emoji_id.0.len(),",
        "                    Place::MidNewLine => unreachable!(),",
        "                    Place::End => self.custom_emoji.middle.len() + self.custom_emoji.end.len(),",
        "                },",
        "                Kind::DateTime { unix_time, date_time_format } => {",
        "                    64 + unix_time.to_string().len() + date_time_format.map_or(0, str::len)",
        "                }''',",
    ]
)

for old, new, label in [
    (old_render, new_render, "render mapping"),
    (old_tag, new_tag, "tag capacity"),
]:
    old_count = text.count(old)
    new_count = text.count(new)
    if old_count == 1 and new_count == 0:
        text = text.replace(old, new, 1)
    elif old_count == 0 and new_count == 1:
        continue
    else:
        raise RuntimeError(
            f"{label}: expected exactly old or new form, got old={old_count}, new={new_count}"
        )

path.write_text(text)
