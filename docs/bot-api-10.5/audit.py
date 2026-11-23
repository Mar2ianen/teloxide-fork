#!/usr/bin/env python3
"""Check Bot API 10.5 models and method fields against the frozen official HTML."""
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
    if kind == "bool" and info.get("default") is True:
        return "True"
    return {"integer": "Integer", "float": "Float", "string": "String", "bool": "Boolean"}[kind]


def main():
    raw = gzip.decompress((HERE / "api.html.gz").read_bytes())
    source = json.loads((HERE / "source.json").read_text())
    assert hashlib.sha256(raw).hexdigest() == source["sha256"]
    assert source["version"] == "10.5"
    parser = Tables()
    parser.feed(raw.decode())
    schema = json.loads((ROOT / "crates/teloxide-core/custom_v2.json").read_text())
    methods = {m["name"]: m for m in schema["methods"]}
    objects = {o["name"]: o for o in schema["objects"]}
    checks = 0

    def fields(name, actual, method=False):
        nonlocal checks
        expected = parser.sections[name]
        assert {r[0] for r in expected} == {p["name"] for p in actual}, name
        props = {p["name"]: p for p in actual}
        for row in expected:
            field, kind = row[:2]
            required = row[2] == "Yes" if method else not row[-1].startswith("Optional")
            assert props[field]["required"] == required, (name, field, "requiredness")
            assert wire_type(props[field]["type_info"]) == kind, (name, field, "type")
            checks += 1

    settings = "supports_inline_queries inline_query_placeholder needs_inline_query_location inline_feedback_share_denominator can_join_groups can_read_all_group_messages can_connect_to_business can_manage_bots supports_guest_queries supports_join_request_queries has_topics_enabled allows_users_to_create_topics has_topic_sidebar can_communicate_with_bots callback_query_mode".split()
    fields("setMySettings", methods["setMySettings"]["arguments"], method=True)
    assert [p["name"] for p in methods["setMySettings"]["arguments"]] == settings
    assert methods["setMySettings"]["return_type"] == {"type": "bool", "default": True}
    assert methods["getMe"]["return_type"] == {"type": "reference", "reference": "BotFullInfo"}
    checks += 2

    for name in ("BotFullInfo", "User", "WriteAccessAllowed", "RichBlockExpandableBlockQuotation", "InputRichBlockExpandableBlockQuotation"):
        fields(name, objects[name]["properties"])
    assert "inviter" in {p["name"] for p in objects["WriteAccessAllowed"]["properties"]}
    assert [p["name"] for p in objects["RichBlockExpandableBlockQuotation"]["properties"]][-2:] == ["blocks", "credit"]
    assert [p["name"] for p in objects["InputRichBlockExpandableBlockQuotation"]["properties"]][-2:] == ["blocks", "credit"]
    checks += 3
    print(f"Bot API 10.5 official snapshot: {checks} field/type/requiredness/return checks passed")


if __name__ == "__main__":
    main()
