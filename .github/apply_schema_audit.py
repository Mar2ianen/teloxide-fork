from pathlib import Path
import json
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "crates/teloxide-core"
NIGHTLY = "nightly-2025-06-12"

# Finish the official getBusinessAccountGifts filter surface in schema.ron.
schema_path = CORE / "schema.ron"
schema = schema_path.read_text()
pattern = re.compile(
    r'                Param\(\n                    name: "exclude_limited",.*?\n                \),\n',
    re.S,
)
replacement = '''                Param(
                    name: "exclude_limited_upgradable",
                    ty: Option(bool),
                    descr: Doc(md: "Pass _true_ to exclude gifts that can be upgraded to a unique gift"),
                ),
                Param(
                    name: "exclude_limited_non_upgradable",
                    ty: Option(bool),
                    descr: Doc(md: "Pass _true_ to exclude gifts that can't be upgraded to a unique gift"),
                ),
                Param(
                    name: "exclude_from_blockchain",
                    ty: Option(bool),
                    descr: Doc(md: "Pass _true_ to exclude gifts that were assigned from the TON blockchain"),
                ),
'''
schema, count = pattern.subn(replacement, schema, count=1)
if count != 1:
    raise RuntimeError(f"expected one legacy gift parameter, replaced {count}")
schema_path.write_text(schema)

# Make the same change in the independent checking schema without reordering it.
custom_path = CORE / "custom_v2.json"
custom = json.loads(custom_path.read_text())
method = next(method for method in custom["methods"] if method["name"] == "getBusinessAccountGifts")
index = next(i for i, arg in enumerate(method["arguments"]) if arg["name"] == "exclude_limited")
method["arguments"][index:index + 1] = [
    {
        "name": "exclude_limited_upgradable",
        "description": "Pass True to exclude gifts that can be upgraded to a unique gift",
        "required": False,
        "type_info": {"type": "bool"},
    },
    {
        "name": "exclude_limited_non_upgradable",
        "description": "Pass True to exclude gifts that can't be upgraded to a unique gift",
        "required": False,
        "type_info": {"type": "bool"},
    },
    {
        "name": "exclude_from_blockchain",
        "description": "Pass True to exclude gifts that were assigned from the TON blockchain",
        "required": False,
        "type_info": {"type": "bool"},
    },
]
custom_path.write_text(json.dumps(custom, ensure_ascii=False, indent=2) + "\n")

# Export every RON method currently missing from custom_v2.json once. The
# resulting checked-in JSON remains independent after this helper is removed.
export_path = CORE / "src/codegen/schema_check/export_missing.rs"
export_path.write_text(r'''use std::{collections::HashSet, fs};

use serde_json::{json, Value};

use crate::codegen::{
    project_root,
    schema::{self, Type},
};

use super::api_schema::get_api_schema;

fn kind(ty: &Type) -> Value {
    match ty {
        Type::True => json!({"type": "bool", "default": true}),
        Type::u8 | Type::u16 | Type::u32 | Type::i32 | Type::u64 | Type::i64 | Type::DateTime => {
            json!({"type": "integer", "enumeration": []})
        }
        Type::f64 => json!({"type": "float"}),
        Type::bool => json!({"type": "bool"}),
        Type::String | Type::Url => json!({"type": "string", "enumeration": []}),
        Type::Option(inner) => kind(inner),
        Type::ArrayOf(inner) => json!({"type": "array", "array": kind(inner)}),
        Type::RawTy(reference) => json!({"type": "reference", "reference": reference}),
    }
}

fn can_contain_file(ty: &Type) -> bool {
    match ty {
        Type::Option(inner) | Type::ArrayOf(inner) => can_contain_file(inner),
        Type::RawTy(name) => matches!(
            name.as_str(),
            "InputFile"
                | "InputSticker"
                | "InputProfilePhoto"
                | "InputStoryContent"
                | "InputMedia"
                | "InputPaidMedia"
                | "InputPollMedia"
                | "InputPollOption"
                | "InputPollOptionMedia"
        ),
        _ => false,
    }
}

#[test]
fn export_missing_methods() {
    let existing: HashSet<_> =
        get_api_schema().methods.into_iter().map(|method| method.name).collect();
    let schema = schema::get();
    let methods: Vec<_> = schema
        .methods
        .iter()
        .filter(|method| !existing.contains(&method.names.0))
        .map(|method| {
            let arguments: Vec<_> = method
                .params
                .iter()
                .map(|param| {
                    let (required, ty) = match &param.ty {
                        Type::Option(inner) => (false, &**inner),
                        ty => (true, ty),
                    };
                    json!({
                        "name": param.name,
                        "description": param.descr.md,
                        "required": required,
                        "type_info": kind(ty),
                    })
                })
                .collect();
            json!({
                "name": method.names.0,
                "description": method.doc.md,
                "arguments": arguments,
                "maybe_multipart": method.params.iter().any(|param| can_contain_file(&param.ty)),
                "return_type": kind(&method.return_ty),
                "documentation_link": method.tg_doc,
            })
        })
        .collect();

    assert_eq!(methods.len(), 20, "unexpected number of unchecked RON methods");
    fs::write(
        project_root().join(".missing_methods.json"),
        serde_json::to_string_pretty(&methods).unwrap(),
    )
    .unwrap();
}
''')
mod_path = CORE / "src/codegen/schema_check.rs"
mod_text = mod_path.read_text()
mod_text += "\n#[cfg(test)]\nmod export_missing;\n"
mod_path.write_text(mod_text)
subprocess.run(
    ["cargo", f"+{NIGHTLY}", "test", "-p", "teloxide-core", "export_missing_methods", "--", "--nocapture"],
    cwd=ROOT,
    check=True,
)
missing = json.loads((CORE / ".missing_methods.json").read_text())
custom = json.loads(custom_path.read_text())
existing = {method["name"] for method in custom["methods"]}
for method in missing:
    if method["name"] in existing:
        raise RuntimeError(f"duplicate exported method {method['name']}")
    custom["methods"].append(method)
custom_path.write_text(json.dumps(custom, ensure_ascii=False, indent=2) + "\n")
(CORE / ".missing_methods.json").unlink()
export_path.unlink()
mod_path.write_text(mod_path.read_text().replace("\n#[cfg(test)]\nmod export_missing;\n", ""))

# Make schema validation symmetric: every method in either schema must exist in the other.
ron_path = CORE / "src/codegen/schema_check/ron_check.rs"
ron = ron_path.read_text()
ron = ron.replace(
    "use crate::codegen::{patch::escape_kw, schema, schema_check::api_schema::*};\n",
    "use std::collections::HashSet;\n\nuse crate::codegen::{patch::escape_kw, schema, schema_check::api_schema::*};\n",
    1,
)
ron = ron.replace(
    '''    #[display("Method `{method}` does not exist")]
    MethodDoesNotExist { method: String },
''',
    '''    #[display("Method `{method}` does not exist in schema.ron")]
    MethodDoesNotExist { method: String },
    #[display("Method `{method}` exists in schema.ron but is absent from custom_v2.json")]
    MethodIsNotChecked { method: String },
''',
    1,
)
needle = '''        for method in api_schema.methods {
'''
reverse = '''        let checked_methods: HashSet<_> =
            api_schema.methods.iter().map(|method| method.name.as_str()).collect();
        for method in &ron_schema.methods {
            if !checked_methods.contains(method.names.0.as_str()) {
                errors.push(ApiCheckError::MethodIsNotChecked { method: method.names.0.clone() });
            }
        }

        for method in api_schema.methods {
'''
if ron.count(needle) != 1:
    raise RuntimeError("RON checker loop changed")
ron_path.write_text(ron.replace(needle, reverse, 1))

# Replace the fragile method-name derive list with structural type checks.
payload_codegen_path = CORE / "src/codegen/payloads.rs"
if not payload_codegen_path.exists():
    payload_codegen_path = CORE / "src/codegen/payloads/codegen.rs"
codegen = payload_codegen_path.read_text()
old = '''        // FIXME: CreateNewStickerSet has to be be only Debug + Clone + Serialize (maybe
        // better fix?)
        let derive = if !multipart.is_empty()
            || matches!(
                &*method.names.1,
                "SendPaidMedia"
                    | "SendMediaGroup"
                    | "SendPoll"
                    | "SetBusinessAccountProfilePhoto"
                    | "PostStory"
                    | "EditStory"
                    | "EditMessageMedia"
                    | "EditMessageMediaInline"
                    | "CreateNewStickerSet"
            ) {
            "#[derive(Debug, Clone, Serialize)]".to_owned()
        } else {
            format!("#[derive(Debug, PartialEq,{eq_hash_derive}{default_derive} Clone, Serialize)]")
        };
'''
new = '''        let derive = if !multipart.is_empty() || !partial_eq_suitable(&method) {
            "#[derive(Debug, Clone, Serialize)]".to_owned()
        } else {
            format!("#[derive(Debug, PartialEq,{eq_hash_derive}{default_derive} Clone, Serialize)]")
        };
'''
if codegen.count(old) != 1:
    raise RuntimeError("payload derive special-case block changed")
codegen = codegen.replace(old, new, 1)
insert_before = '''fn eq_hash_suitable(method: &Method) -> bool {
'''
helper = '''fn partial_eq_suitable(method: &Method) -> bool {
    fn ty_partial_eq_suitable(ty: &Type) -> bool {
        match ty {
            Type::Option(inner) | Type::ArrayOf(inner) => ty_partial_eq_suitable(inner),
            Type::RawTy(raw) => !matches!(
                raw.as_str(),
                "InputSticker"
                    | "InputProfilePhoto"
                    | "InputStoryContent"
                    | "InputMedia"
                    | "InputPaidMedia"
                    | "InputPollMedia"
                    | "InputPollOption"
                    | "InputPollOptionMedia"
            ),
            _ => true,
        }
    }

    method.params.iter().all(|param| ty_partial_eq_suitable(&param.ty))
}

'''
if codegen.count(insert_before) != 1:
    raise RuntimeError("eq_hash_suitable marker changed")
payload_codegen_path.write_text(codegen.replace(insert_before, helper + insert_before, 1))

# External serialization regression test for the new filter names.
test_path = CORE / "tests/business_account_gift_filters.rs"
test_path.write_text('''use teloxide_core::{
    payloads::{GetBusinessAccountGifts, GetBusinessAccountGiftsSetters},
    types::BusinessConnectionId,
};

#[test]
fn business_account_gift_filters_use_current_wire_names() {
    let payload = GetBusinessAccountGifts::new(BusinessConnectionId("business".to_owned()))
        .exclude_limited_upgradable(true)
        .exclude_limited_non_upgradable(true)
        .exclude_from_blockchain(true);
    let value = serde_json::to_value(payload).unwrap();
    let object = value.as_object().unwrap();

    assert_eq!(object["exclude_limited_upgradable"], true);
    assert_eq!(object["exclude_limited_non_upgradable"], true);
    assert_eq!(object["exclude_from_blockchain"], true);
    assert!(!object.contains_key("exclude_limited"));
}
''')

# First codegen pass may update generated files; the second must be clean.
subprocess.run(
    ["cargo", f"+{NIGHTLY}", "test", "-p", "teloxide-core", "--features", "full nightly", "codegen", "--", "--nocapture"],
    cwd=ROOT,
    check=False,
)
subprocess.run(
    ["cargo", f"+{NIGHTLY}", "test", "-p", "teloxide-core", "--features", "full nightly", "codegen", "--", "--nocapture"],
    cwd=ROOT,
    check=True,
)

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
