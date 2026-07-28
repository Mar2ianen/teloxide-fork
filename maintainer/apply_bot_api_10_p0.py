#!/usr/bin/env python3
from pathlib import Path

SCHEMA = Path("crates/teloxide-core/schema.ron")
POLL_MEDIA = Path("crates/teloxide-core/src/types/poll_media.rs")


def balanced_end(text: str, start: int, opening: str, closing: str) -> int:
    depth = 0
    in_string = False
    escaped = False
    for i in range(start, len(text)):
        c = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif c == "\\":
                escaped = True
            elif c == '"':
                in_string = False
            continue
        if c == '"':
            in_string = True
        elif c == opening:
            depth += 1
        elif c == closing:
            depth -= 1
            if depth == 0:
                return i + 1
    raise RuntimeError(f"unbalanced {opening}{closing}")


def method_span(text: str, api_name: str) -> tuple[int, int]:
    marker = f'names: ("{api_name}"'
    p = text.index(marker)
    start = text.rfind("Method(", 0, p)
    return start, balanced_end(text, start + len("Method"), "(", ")")


def param_span(block: str, name: str) -> tuple[int, int] | None:
    marker = f'name: "{name}"'
    try:
        p = block.index(marker)
    except ValueError:
        return None
    start = block.rfind("Param(", 0, p)
    end = balanced_end(block, start + len("Param"), "(", ")")
    while end < len(block) and block[end] in ",\n ":
        end += 1
    return start, end


def params_close(block: str) -> int:
    p = block.index("params: [") + len("params: ")
    return balanced_end(block, p, "[", "]") - 1


def add_param(block: str, name: str, ty: str, descr: str) -> str:
    if f'name: "{name}"' in block:
        return block
    pos = params_close(block)
    indent = " " * 16
    item = (
        f"\n{indent}Param(\n"
        f"{indent}    name: \"{name}\",\n"
        f"{indent}    ty: Option({ty}),\n"
        f"{indent}    descr: Doc(md: \"{descr}\"),\n"
        f"{indent}),"
    )
    return block[:pos] + item + block[pos:]


def remove_param(block: str, name: str) -> str:
    span = param_span(block, name)
    return block if span is None else block[:span[0]] + block[span[1]:]


def replace_param_type(block: str, name: str, old: str, new: str) -> str:
    span = param_span(block, name)
    if span is None:
        raise RuntimeError(f"missing parameter {name}")
    a, b = span
    param = block[a:b]
    param2 = param.replace(f"ty: Option({old})", f"ty: Option({new})")
    if param == param2:
        raise RuntimeError(f"type pattern not found for {name}")
    return block[:a] + param2 + block[b:]


def patch_method(text: str, name: str, fn) -> str:
    a, b = method_span(text, name)
    return text[:a] + fn(text[a:b]) + text[b:]


schema = SCHEMA.read_text()


def patch_send_poll(block: str) -> str:
    block = remove_param(block, "correct_option_id")
    block = replace_param_type(block, "open_period", "u16", "u32")
    additions = [
        ("allows_revoting", "bool", "Pass True, if the poll allows voters to change their answer."),
        ("shuffle_options", "bool", "Pass True, if the poll options need to be shuffled."),
        ("allow_adding_options", "bool", "Pass True, if users may add answer options."),
        ("hide_results_until_closes", "bool", "Pass True, if results must stay hidden until the poll closes."),
        ("correct_option_ids", 'ArrayOf(u8)', "0-based identifiers of the correct answer options; required for quizzes."),
        ("description", "String", "Description of the poll."),
        ("description_parse_mode", 'RawTy("ParseMode")', "Mode for parsing entities in the poll description."),
        ("description_entities", 'ArrayOf(RawTy("MessageEntity"))', "Special entities in the poll description."),
        ("media", 'RawTy("InputPollMedia")', "Media attached to the poll description."),
        ("explanation_media", 'RawTy("InputPollMedia")', "Media attached to the quiz explanation."),
        ("members_only", "bool", "Pass True to limit voting to established chat members."),
        ("country_codes", "ArrayOf(String)", "Two-letter country codes from which users can vote."),
    ]
    for args in additions:
        block = add_param(block, *args)
    return block


schema = patch_method(schema, "sendPoll", patch_send_poll)
schema = patch_method(schema, "getChatAdministrators", lambda b: add_param(
    b, "return_bots", "bool", "Pass True to include other bots in the returned administrator list."
))
schema = patch_method(schema, "promoteChatMember", lambda b: add_param(
    b, "can_manage_tags", "bool", "Pass True if the administrator can edit tags of regular members."
))
schema = patch_method(schema, "forwardMessage", lambda b: add_param(
    b, "message_effect_id", 'RawTy("EffectId")', "Unique identifier of the message effect to add to the forwarded message."
))
schema = patch_method(schema, "copyMessage", lambda b: add_param(
    b, "message_effect_id", 'RawTy("EffectId")', "Unique identifier of the message effect to add to the copied message."
))
SCHEMA.write_text(schema)

poll_media = POLL_MEDIA.read_text()
needle = "pub enum InputPollOptionMedia {\n    Photo(crate::types::InputMediaPhoto),\n    Sticker(crate::types::InputMediaSticker),\n    Video(crate::types::InputMediaVideo),\n}"
replacement = "pub enum InputPollOptionMedia {\n    Animation(crate::types::InputMediaAnimation),\n    LivePhoto(crate::types::InputMediaLivePhoto),\n    Location(crate::types::InputMediaLocation),\n    Photo(crate::types::InputMediaPhoto),\n    Sticker(crate::types::InputMediaSticker),\n    Venue(crate::types::InputMediaVenue),\n    Video(crate::types::InputMediaVideo),\n}"
if needle in poll_media:
    poll_media = poll_media.replace(needle, replacement)
elif replacement not in poll_media:
    raise RuntimeError("unexpected InputPollOptionMedia layout")
POLL_MEDIA.write_text(poll_media)
