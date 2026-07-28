from pathlib import Path

wrapper = Path(__file__)
script = wrapper.with_name("apply_renderer_audit.py")
source = script.read_text().replace(
    "crates/teloxide/src/utils/render/mod.rs",
    "crates/teloxide/src/utils/render.rs",
)
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
