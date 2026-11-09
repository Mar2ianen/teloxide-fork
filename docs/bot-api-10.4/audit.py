#!/usr/bin/env python3
"""Audit the 10.4 delta against the frozen official HTML, using only stdlib.

This deliberately does not import surface.json, schema.ron, or generated Rust
as its source of expected fields. The Rust codegen tests check those layers.
"""
import gzip
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent


class Tables(HTMLParser):
    def __init__(self):
        super().__init__()
        self.sections = {}
        self.heading = None
        self.current = None
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "h4":
            self.heading = []
        elif tag == "tr":
            self.row = []
        elif tag == "td":
            self.cell = []

    def handle_data(self, data):
        if self.heading is not None:
            self.heading.append(data)
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag == "h4":
            self.current = "".join(self.heading).strip()
            self.sections[self.current] = []
            self.heading = None
        elif tag == "td" and self.cell is not None:
            self.row.append(re.sub(r"\s+", " ", "".join(self.cell)).strip())
            self.cell = None
        elif tag == "tr" and self.row:
            if self.current:
                self.sections[self.current].append(self.row)
            self.row = None


def wire_type(info):
    kind = info["type"]
    if kind == "reference":
        return {"Recipient": "Integer or String"}.get(info["reference"], info["reference"])
    if kind == "array":
        return "Array of " + wire_type(info["array"])
    if kind == "any_of":
        return " or ".join(wire_type(item) for item in info["any_of"])
    return {"integer": "Integer", "float": "Float", "string": "String", "bool": "Boolean"}[kind]


def main():
    raw = gzip.decompress((HERE / "api.html.gz").read_bytes())
    source = json.loads((HERE / "source.json").read_text())
    assert hashlib.sha256(raw).hexdigest() == source["sha256"]
    assert source["version"] == "10.4"
    parser = Tables()
    parser.feed(raw.decode())
    schema = json.loads((ROOT / "crates/teloxide-core/custom_v2.json").read_text())
    methods = {m["name"]: m for m in schema["methods"]}
    objects = {o["name"]: o for o in schema["objects"]}
    checks = 0

    def fields(name, actual, method=False, selected=None):
        nonlocal checks
        expected = parser.sections[name]
        if selected:
            expected = [r for r in expected if r[0] in selected]
        else:
            assert {r[0] for r in expected} == {p["name"] for p in actual}, name
        props = {p["name"]: p for p in actual}
        for row in expected:
            field, kind = row[:2]
            required = row[2] == "Yes" if method else not row[-1].startswith("Optional")
            assert props[field]["required"] == required, (name, field, "requiredness")
            assert wire_type(props[field]["type_info"]) == kind, (name, field, "type")
            checks += 1

    for name in "sendWidget sendSharedWidget answerWidgetQuery editMessageWidgetData editSharedWidgetData getChatShareThemeBackdrops getUniqueGift deleteBusinessAccountGift".split():
        fields(name, methods[name]["arguments"], method=True)
        assert not methods[name]["maybe_multipart"], name
        returns = {"sendWidget": "Message", "sendSharedWidget": "Message", "getChatShareThemeBackdrops": "ChatShareThemeBackdrops", "getUniqueGift": "UniqueGift"}
        actual_return = methods[name]["return_type"]
        if name in returns:
            assert actual_return == {"type": "reference", "reference": returns[name]}, name
        else:
            assert actual_return == {"type": "bool", "default": True}, name
        checks += 1
    for name in "Widget WidgetQuery SharedWidgetQuery WidgetQueryTypeData WidgetQueryTypeSignData WidgetQueryTypeSentTransaction WidgetQueryWallet TonWalletCard TonWalletCardSticker TonNft TonNftAttribute TonWalletTransfer EmojiTypeEmoji EmojiTypeCustomEmoji ChatShareThemeEmoji ChatShareThemeUniqueGift ChatShareThemeBackdrop ChatShareThemeBackdrops AreaPosition PaidMessagesWaived RichTextTonAddress InputWidgetMessageContent InputSharedWidgetMessageContent EphemeralMessageParameters".split():
        fields(name, objects[name]["properties"])
    changes = {"Update": ["widget_query", "shared_widget_query"], "Message": ["widget", "ton_wallet_transfer", "paid_messages_waived"], "ExternalReplyInfo": ["widget"], "ChatFullInfo": ["ton_wallet_card", "share_theme"], "TextQuote": ["rich_texts"], "ReplyParameters": ["quote_rich_texts"]}
    for name, selected in changes.items():
        fields(name, objects[name]["properties"], selected=selected)
    for name in "verifyUser verifyChat".split():
        fields(name, methods[name]["arguments"], method=True, selected=["custom_description_parse_mode", "custom_description_entities"])
    for name in "sendMessage sendRichMessage sendAnimation sendAudio sendContact sendDocument sendLivePhoto sendLocation sendPhoto sendSticker sendVenue sendVideo sendVideoNote sendVoice".split():
        fields(name, methods[name]["arguments"], method=True, selected=["chat_id"])
    for name, variants in {"WidgetQueryType": ["WidgetQueryTypeData", "WidgetQueryTypeSignData", "WidgetQueryTypeSentTransaction"], "EmojiType": ["EmojiTypeEmoji", "EmojiTypeCustomEmoji"], "ChatShareTheme": ["ChatShareThemeEmoji", "ChatShareThemeUniqueGift"]}.items():
        assert [x["reference"] for x in objects[name]["any_of"]] == variants
        checks += 1
    entities = next(p for p in objects["MessageEntity"]["properties"] if p["name"] == "type")
    description = next(r[-1] for r in parser.sections["MessageEntity"] if r[0] == "type")
    for name in ["ton_address", "bank_card_number"]:
        assert name in entities["type_info"]["enumeration"] and name in description
        checks += 1
    content_variants = [v["reference"] for v in objects["InputMessageContent"]["any_of"]]
    assert "InputWidgetMessageContent" in content_variants and "InputSharedWidgetMessageContent" in content_variants
    checks += 1
    print(f"Bot API 10.4 official snapshot: {checks} field/union checks passed")


if __name__ == "__main__":
    main()
