from pathlib import Path
import re


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if text.count(old) != 1:
        raise RuntimeError(f"expected one match in {path}: {old!r}")
    file.write_text(text.replace(old, new, 1))


# Align current public MSRV declarations with the version already tested by CI.
replace_once("Cargo.toml", 'rust-version = "1.82"', 'rust-version = "1.85"')

for path in [Path("README.md"), *Path("crates").glob("*/README.md"), *Path("crates").glob("*/src/lib.rs")]:
    if not path.exists():
        continue
    text = path.read_text()
    text = text.replace("rustc at least version 1.82", "rustc at least version 1.85")
    text = text.replace("requires rustc 1.82+", "requires rustc 1.85+")
    path.write_text(text)

# Code generation is a pinned-nightly maintenance concern. Normal stable/beta/MSRV
# test runs must not invoke it implicitly.
for path in Path("crates/teloxide-core/src").rglob("*.rs"):
    text = path.read_text()
    text = text.replace(
        "#[cfg(test)]\nmod codegen;",
        '#[cfg(all(test, feature = "nightly"))]\nmod codegen;',
    )
    text = re.sub(
        r'(?m)^([ \t]*)#\[test\]\n([ \t]*fn codegen[_a-zA-Z0-9]*\s*\()',
        r'\1#[cfg(feature = "nightly")]\n\1#[test]\n\2',
        text,
    )
    path.write_text(text)

replace_once(
    "Justfile",
    '    cargo clippy --all-targets --features "full nightly"',
    '    cargo clippy --all-targets --features "full nightly" -- -D warnings',
)

# Add a dedicated pinned-nightly codegen job and require it in the aggregate job.
path = Path(".github/workflows/ci.yml")
text = path.read_text()
text = text.replace(
    """      - fmt
      - test
      - check-examples
      - clippy
      - doc
""",
    """      - fmt
      - codegen
      - test
      - check-examples
      - clippy
      - doc
""",
    1,
)
marker = """  test:
    name: Test
"""
job = """  codegen:
    name: Check generated code
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install Rust ${{ env.rust_nightly }}
        uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ env.rust_nightly }}
          components: rustfmt

      - name: Cache Dependencies
        uses: Swatinem/rust-cache@v2

      - name: Check generated files
        run: |
          cargo +${{ env.rust_nightly }} test -p teloxide-core \\
            --features "full nightly" codegen -- --nocapture
          git diff --exit-code

"""
if text.count(marker) != 1:
    raise RuntimeError("CI test job marker mismatch")
path.write_text(text.replace(marker, job + marker, 1))

# Record the intentional MSRV change in the active changelogs without rewriting history.
for changelog in Path("crates").glob("*/CHANGELOG.md"):
    text = changelog.read_text()
    if "Raise MSRV from Rust 1.82 to 1.85" in text:
        continue
    marker = "## unreleased\n"
    if marker not in text:
        continue
    entry = "\n### Changed\n\n- Raise MSRV from Rust 1.82 to 1.85.\n"
    changelog.write_text(text.replace(marker, marker + entry, 1))
