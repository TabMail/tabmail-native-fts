#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Verify that THIRD_PARTY_NOTICES.txt credits every crate the helper is built from.

For every release target named in about.toml, every crate in the helper's normal dependency
graph (Cargo.lock, no build or dev dependencies) must appear in the notices file's "Used by"
lists at its exact version. The file must also hold no unfilled license template, which is what
cargo-about writes when it cannot find a crate's own license file.

Run it after any Cargo.lock change, and before a release. It runs `cargo tree --locked` for all
the targets at once, so Cargo fetches any crate a target needs that is not yet in the local cache
(a build for one platform caches only that platform's crates). It does not need cargo-about.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

DEFAULT_ROOT = Path(__file__).resolve().parents[1]

# Left by cargo-about's generic MIT or BSD template when a crate's real license file was not found
# (the real Apache-2.0 text has its own "[yyyy]" appendix, so that one is not a sign).
PLACEHOLDERS = ("<year>", "<owner>", "<copyright holders>")


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def targets(root: Path) -> list[str]:
    # A plain read of about.toml's `targets = [...]` (no tomllib: release machines may run Python < 3.11).
    match = re.search(r"^targets\s*=\s*\[(.*?)\]", (root / "about.toml").read_text(encoding="utf-8"),
                      re.M | re.S)
    names = re.findall(r'"([^"]+)"', match.group(1)) if match else []
    if not names:
        fail("about.toml names no release targets")
    return names


def shipped_crates(root: Path, target_list: list[str]) -> set[str]:
    cargo = os.environ.get("CARGO", "cargo")
    command = [cargo, "tree", "--edges", "normal", "--prefix", "none", "--format", "{p}",
               "--locked", "--manifest-path", str(root / "Cargo.toml")]
    for target in target_list:
        command += ["--target", target]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        fail(f"cargo tree failed:\n{result.stderr.strip()}")
    crates = set()
    for line in result.stdout.splitlines():
        match = re.match(r"(\S+) v(\S+)", line.strip())
        if match:
            crates.add(f"{match.group(1)} {match.group(2)}")
    if not crates:
        fail("cargo tree listed no crates")
    return crates


# Each license section in part 2 of the notices (about.hbs): a rule, the license name, the "Used by"
# list, a second rule, then the license's full text.
PART_2 = "PART 2. RUST CRATES"
SECTION_RULE = "=" * 80
TEXT_RULE = "-" * 80


def sections_without_text(notices: str) -> list[str]:
    """The names of the license sections whose text is empty."""
    empty = []
    for section in notices.partition(PART_2)[2].split(SECTION_RULE)[1:]:
        name = section.strip().splitlines()[0] if section.strip() else "?"
        head, rule, text = section.partition(TEXT_RULE)
        if not rule or not text.strip():
            empty.append(name)
    return empty


def credited_crates(notices: str) -> set[str]:
    return {f"{m.group(1)} {m.group(2)}" for m in re.finditer(r"^- (\S+) (\d\S*)", notices, re.M)}


def problems(notices: str, shipped: set[str]) -> list[str]:
    """What is wrong with a notices file, given the crates the helper is built from."""
    found = [p for p in PLACEHOLDERS if p in notices]
    result = []
    if found:
        result.append(f"it holds an unfilled license template ({', '.join(found)}); add a "
                      "clarification for that crate to about.toml and generate the file again")
    empty = sections_without_text(notices)
    if empty:
        result.append(f"these licenses have no text: {', '.join(empty)}; generate the file again")
    missing = sorted(shipped - credited_crates(notices))
    if missing:
        result.append(f"it does not credit: {', '.join(missing)}; run "
                      "scripts/generate-third-party-notices.sh")
    return result


def main() -> None:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_ROOT
    path = root / "THIRD_PARTY_NOTICES.txt"
    if not path.is_file():
        fail(f"{path.name} is missing; run scripts/generate-third-party-notices.sh")
    found = problems(path.read_text(encoding="utf-8"), shipped_crates(root, targets(root)))
    if found:
        fail(f"{path.name}: " + "; ".join(found))
    print(f"{path.name} credits every shipped crate.")


if __name__ == "__main__":
    main()
