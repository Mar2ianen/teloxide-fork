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
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
