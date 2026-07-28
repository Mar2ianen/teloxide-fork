from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CUSTOM_SCHEMA = Path("crates/teloxide-core/custom_v2.json")
PAYLOAD_CODEGEN = Path("crates/teloxide-core/src/payloads/codegen.rs")


def scalar(kind: str) -> dict[str, Any]:
    result: dict[str, Any] = {"type": kind}
    if kind in {"string", "integer"}:
        result["enumeration"] = []
    return result


def reference(name: str) -> dict[str, Any]:
    return {"type": "reference", "reference": name}


def array(inner: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "array": inner}


def argument(
    name: str,
    description: str,
    type_info: dict[str, Any],
    *,
    required: bool = False,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "required": required,
        "type_info": type_info,
    }


def method(methods: dict[str, dict[str, Any]], name: str) -> dict[str, Any]:
    try:
        return methods[name]
    except KeyError as error:
        raise RuntimeError(f"missing method {name}") from error


def argument_index(method_data: dict[str, Any], name: str) -> int:
    for index, item in enumerate(method_data["arguments"]):
        if item["name"] == name:
            return index
    raise RuntimeError(f"{method_data['name']}: missing argument {name}")


def replace_argument(
    method_data: dict[str, Any],
    old_name: str,
    replacements: list[dict[str, Any]],
) -> None:
    index = argument_index(method_data, old_name)
    method_data["arguments"][index : index + 1] = replacements


def insert_after(
    method_data: dict[str, Any],
    after: str,
    item: dict[str, Any],
) -> None:
    if any(existing["name"] == item["name"] for existing in method_data["arguments"]):
        return
    index = argument_index(method_data, after)
    method_data["arguments"].insert(index + 1, item)


data = json.loads(CUSTOM_SCHEMA.read_text())
methods = {item["name"]: item for item in data["methods"]}

send_poll = method(methods, "sendPoll")
for item in send_poll["arguments"]:
    if item["name"] == "options":
        item["description"] = "A JSON-serialized list of 1-12 answer options"
    elif item["name"] == "allows_multiple_answers":
        item["description"] = "Pass True if the poll allows multiple answers, defaults to False"
    elif item["name"] == "open_period":
        item["description"] = (
            "Amount of time in seconds the poll will be active after creation, "
            "5-2628000. Can't be used together with close_date."
        )
    elif item["name"] == "close_date":
        item["description"] = (
            "Point in time when the poll will automatically close. Must be at least 5 "
            "and no more than 2628000 seconds in the future. Can't be used together "
            "with open_period."
        )

replace_argument(
    send_poll,
    "correct_option_id",
    [
        argument(
            "allows_revoting",
            "Pass True if the poll allows changing chosen answer options; defaults to False for quizzes and True for regular polls",
            scalar("bool"),
        ),
        argument(
            "shuffle_options",
            "Pass True if the poll options must be shown in random order",
            scalar("bool"),
        ),
        argument(
            "allow_adding_options",
            "Pass True if answer options can be added after creation; not supported for anonymous polls and quizzes",
            scalar("bool"),
        ),
        argument(
            "hide_results_until_closes",
            "Pass True if poll results must be shown only after the poll closes",
            scalar("bool"),
        ),
        argument(
            "members_only",
            "Pass True if voting is limited to users who have been members of the target chat for more than 24 hours; for channel chats only",
            scalar("bool"),
        ),
        argument(
            "country_codes",
            "A JSON-serialized list of 0-12 two-letter ISO 3166-1 alpha-2 country codes allowed to vote; use FT for anonymous numbers",
            array(scalar("string")),
        ),
        argument(
            "correct_option_ids",
            "A JSON-serialized list of monotonically increasing 0-based identifiers of correct answers, required for quiz polls",
            array(scalar("integer")),
        ),
    ],
)

insert_after(
    send_poll,
    "explanation_entities",
    argument(
        "explanation_media",
        "Media added to the quiz explanation",
        reference("InputPollMedia"),
    ),
)
insert_after(
    send_poll,
    "is_closed",
    argument(
        "description",
        "Description of the poll to be sent, 0-1024 characters after entities parsing",
        scalar("string"),
    ),
)
insert_after(
    send_poll,
    "description",
    argument(
        "description_parse_mode",
        "Mode for parsing entities in the poll description",
        scalar("string"),
    ),
)
insert_after(
    send_poll,
    "description_parse_mode",
    argument(
        "description_entities",
        "A JSON-serialized list of special entities in the poll description, specified instead of description_parse_mode",
        array(reference("MessageEntity")),
    ),
)
insert_after(
    send_poll,
    "description_entities",
    argument(
        "media",
        "Media added to the poll description",
        reference("InputPollMedia"),
    ),
)

insert_after(
    method(methods, "getChatAdministrators"),
    "chat_id",
    argument(
        "return_bots",
        "Pass True to additionally receive all bots that are administrators of the chat; other bots are omitted by default",
        scalar("bool"),
    ),
)
insert_after(
    method(methods, "promoteChatMember"),
    "can_manage_direct_messages",
    argument(
        "can_manage_tags",
        "Pass True if the administrator can edit tags of regular members; for groups and supergroups only",
        scalar("bool"),
    ),
)
insert_after(
    method(methods, "forwardMessage"),
    "protect_content",
    argument(
        "message_effect_id",
        "Unique identifier of the message effect to add to the forwarded message; for private chats only",
        scalar("string"),
    ),
)
insert_after(
    method(methods, "copyMessage"),
    "allow_paid_broadcast",
    argument(
        "message_effect_id",
        "Unique identifier of the message effect to add to the copied message; for private chats only",
        scalar("string"),
    ),
)

CUSTOM_SCHEMA.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")

codegen = PAYLOAD_CODEGEN.read_text()
old = '''                "SendPaidMedia"
                    | "SendMediaGroup"'''
new = '''                "SendPaidMedia"
                    | "SendMediaGroup"
                    | "SendPoll"'''
if old not in codegen:
    raise RuntimeError("payload derive exception anchor changed")
PAYLOAD_CODEGEN.write_text(codegen.replace(old, new, 1))
