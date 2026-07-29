from pathlib import Path

wrapper = Path(__file__)
script = wrapper.with_name("run_renderer_audit.py")
source = script.read_text().replace(
    "    '''fn write_link_destination(value: &str, buf: &mut String) {",
    "    r'''fn write_link_destination(value: &str, buf: &mut String) {",
)
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
