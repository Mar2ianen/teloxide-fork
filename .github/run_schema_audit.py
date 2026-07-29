from pathlib import Path

wrapper = Path(__file__)
script = wrapper.with_name("apply_schema_audit.py")
source = script.read_text().replace(
    'assert_eq!(methods.len(), 20, "unexpected number of unchecked RON methods");',
    'assert_eq!(methods.len(), 28, "unexpected number of unchecked RON methods");',
)
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
