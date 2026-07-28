from pathlib import Path
import subprocess

# Revert the partial cfg-gating experiment. Existing codegen tests remain enabled;
# CI will install their pinned rustfmt toolchain explicitly for every matrix entry.
subprocess.run(["git", "fetch", "origin", "next", "--depth=1"], check=True)
subprocess.run(
    [
        "git",
        "checkout",
        "origin/next",
        "--",
        "crates/teloxide-core/src/codegen/schema_check/rust_types_check_codegen.rs",
        "crates/teloxide-core/src/lib.rs",
        "crates/teloxide-core/src/payloads.rs",
        "crates/teloxide-core/src/payloads/codegen.rs",
    ],
    check=True,
)

core_lib = Path("crates/teloxide-core/src/lib.rs")
core_lib.write_text(core_lib.read_text().replace("requires rustc 1.82+", "requires rustc 1.85+"))

ci = Path(".github/workflows/ci.yml").read_text()
needle = """      - name: Install Rust ${{ matrix.toolchain }}
        uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ matrix.toolchain }}

"""
replacement = needle + """      - name: Install pinned rustfmt for codegen
        uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ env.rust_nightly }}
          components: rustfmt

"""
if ci.count(needle) != 1:
    raise RuntimeError("test matrix toolchain step not found")
Path(".github/ci-proposed.yml").write_text(ci.replace(needle, replacement, 1))

Path(__file__).unlink()
