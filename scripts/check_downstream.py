#!/usr/bin/env python3
"""Compile downstream public consumers against this checkout of Teloxide.

Uses a disposable checkout and rewrites *only* matching Git dependencies
for teloxide / teloxide-core. It never commits to a consumer repository.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib


ROOT = Path(__file__).resolve().parent.parent
PROJECTS = tomllib.loads((ROOT / "downstream.toml").read_text())["project"]
SOURCE_URL = "https://github.com/Mar2ianen/teloxide-fork"
INLINE_DEP = re.compile(
    r"(?m)^(?P<indent>[ \t]*)(?P<name>teloxide(?:-core)?)[ \t]*="
    r"[ \t]*\{(?P<body>[^\n}]*)\}(?P<suffix>[ \t]*(?:#[^\n]*)?)$"
)


def patch_manifest(path: Path) -> int:
    source = path.read_text()
    count = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal count
        body = match["body"]
        if SOURCE_URL not in body:
            return match[0]

        name = match["name"]
        local = ROOT / "crates" / name
        if not (local / "Cargo.toml").exists():
            raise RuntimeError(f"missing local dependency: {local}")

        # Keep requested features/default-features. Only replace Git source
        # selection with an explicit local path.
        body = re.sub(
            r"\b(?:git|rev|branch|tag)\s*=\s*\"[^\"]*\"\s*,?\s*",
            "",
            body,
        ).strip().strip(",").strip()
        source_field = f'path = "{local.as_posix()}"'
        new_body = f"{source_field}, {body}" if body else source_field
        count += 1
        return f'{match["indent"]}{name} = {{ {new_body} }}{match["suffix"]}'

    updated = INLINE_DEP.sub(replace, source)
    if count:
        path.write_text(updated)
    return count


def run(project_id: str) -> None:
    options = {project["id"]: project for project in PROJECTS}
    project = options[project_id]
    repository = project["repository"]
    ref = project["ref"]

    with tempfile.TemporaryDirectory(prefix="teloxide-downstream-") as directory:
        checkout = Path(directory) / "consumer"
        subprocess.run(
            [
                "git", "clone", "--depth=1", "--branch", ref,
                f"https://github.com/{repository}.git", str(checkout),
            ],
            check=True,
        )
        changed = sum(
            patch_manifest(path)
            for path in checkout.rglob("Cargo.toml")
            if ".git" not in path.parts and "target" not in path.parts
        )
        if changed == 0:
            raise RuntimeError(f"{repository}: no pinned Teloxide Git dependency found")

        print(f"{repository}: patched {changed} dependency references", flush=True)
        # --locked is intentionally omitted: path overrides change resolution.
        subprocess.run(
            ["cargo", "+stable", "check", "--workspace", "--lib"],
            cwd=checkout,
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project", choices=[p["id"] for p in PROJECTS])
    args = parser.parse_args()
    run(args.project)


if __name__ == "__main__":
    main()
