from pathlib import Path

wrapper = Path(__file__)
script = wrapper.with_name("run_renderer_audit.py")
source = script.read_text()
start = source.index('text = replace_once(\n    text,\n    \'\'\'            Place::Start => {\n                write!(buf, "<tg-time')
end = source.index('\ntext = replace_once(', start + 1)
block = source[start:end]
block = block.replace("    '''", "    r'''", 2)
source = source[:start] + block + source[end:]
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
