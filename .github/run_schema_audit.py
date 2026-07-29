from pathlib import Path

wrapper = Path(__file__)
script = wrapper.with_name("apply_schema_audit.py")
source = script.read_text()
source = source.replace(
    'assert_eq!(methods.len(), 20, "unexpected number of unchecked RON methods");',
    'assert_eq!(methods.len(), 28, "unexpected number of unchecked RON methods");',
)
source = source.replace(
    'CORE / "src/codegen/payloads.rs"',
    'CORE / "src/payloads/codegen.rs"',
)
source = source.replace(
    'test_path = CORE / "tests/business_account_gift_filters.rs"\ntest_path.write_text(',
    'test_path = CORE / "tests/business_account_gift_filters.rs"\ntest_path.parent.mkdir(exist_ok=True)\ntest_path.write_text(',
)
source = source.replace(
    '["cargo", f"+{NIGHTLY}", "test", "-p", "teloxide-core", "--features", "full nightly", "codegen", "--", "--nocapture"]',
    '["cargo", f"+{NIGHTLY}", "test", "-p", "teloxide-core", "--lib", "--features", "full nightly", "codegen", "--", "--nocapture"]',
)
source = source.replace(
    'ron_path.write_text(ron.replace(needle, reverse, 1))',
    '''ron = ron.replace(
    "        let mut exceptions = Exceptions::new(vec![\\n",
    "        let mut exceptions = Exceptions::new(vec![\\n"
    "            // Legacy combined checking schema field, replaced by link_preview_options.\\n"
    "            Exception::MethodField {\\n"
    "                method: \\\"editMessageText\\\".to_owned(),\\n"
    "                param: \\\"disable_web_page_preview\\\".to_owned(),\\n"
    "            },\\n",
    1,
)
ron_path.write_text(ron.replace(needle, reverse, 1))''',
)
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
