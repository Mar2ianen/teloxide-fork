from pathlib import Path

path = Path(".github/scripts/fix_send_poll_multipart.py")
text = path.read_text()

replacements = [
    (
        '    "types::{\\n        InputFile, InputFileLike, InputMedia, InputPaidMedia, InputPollMedia,\\n        InputPollOptionMedia, InputSticker,\\n    }",',
        '    "types::{\\n        InputFile, InputFileLike, InputMedia, InputPaidMedia, InputSticker,\\n    }",',
    ),
    ("InputFile::memory([1_u8])", "InputFile::memory(vec![1_u8])"),
]

for old, new in replacements:
    if old in text:
        text = text.replace(old, new, 1)
    elif new not in text:
        raise RuntimeError(f"patch fixture anchor not found: {old}")

path.write_text(text)
