#!/usr/bin/env python3
"""Verify that the compiled update-key trust set matches the tracked policy."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "release" / "update-signing-policy.json"
SOURCE_PATH = ROOT / "src" / "update_signature.rs"
CARGO_PATH = ROOT / "Cargo.toml"


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def version_tuple(value: str) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in value.split("."))
    except ValueError:
        fail(f"invalid numeric version in signing policy: {value!r}")


policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
source = SOURCE_PATH.read_text(encoding="utf-8")
cargo = CARGO_PATH.read_text(encoding="utf-8")

array_match = re.search(
    r"pub const UPDATE_PUBLIC_KEYS_BASE64:.*?=\s*&\[(.*?)\];",
    source,
    flags=re.DOTALL,
)
if not array_match:
    fail("could not locate UPDATE_PUBLIC_KEYS_BASE64 in src/update_signature.rs")

compiled_keys = re.findall(r'"([A-Za-z0-9+/]{43}=)"', array_match.group(1))
active_key = policy.get("activeManifestSigningPublicKeyBase64", "")
previous_keys = policy.get("previousTrustedManifestSigningPublicKeysBase64", [])
pending_key = policy.get("pendingManifestSigningPublicKeyBase64", "")
expected_keys = [active_key, *previous_keys]
if pending_key:
    expected_keys.append(pending_key)

if not active_key:
    fail("activeManifestSigningPublicKeyBase64 is missing")
if len(compiled_keys) != len(set(compiled_keys)):
    fail("UPDATE_PUBLIC_KEYS_BASE64 contains a duplicate key")
if len(expected_keys) != len(set(expected_keys)):
    fail("tracked active/transitional signing policy contains a duplicate key")
if set(compiled_keys) != set(expected_keys):
    fail(
        "compiled update-key trust set does not match active + transitional policy keys"
    )

version_match = re.search(r'^version\s*=\s*"([0-9]+(?:\.[0-9]+)+)"', cargo, re.MULTILINE)
if not version_match:
    fail("could not read the package version from Cargo.toml")
current_version = version_tuple(version_match.group(1))

bridge_version = policy.get("bridgeReleaseVersion", "")
if previous_keys and bridge_version and current_version > version_tuple(bridge_version):
    fail(
        "previous transitional update key survived beyond bridge release "
        f"{bridge_version}; retire it before releasing {version_match.group(1)}"
    )

for retired in policy.get("retiredManifestSigningPublicKeys", []):
    public_key = retired.get("publicKeyBase64", "")
    first_untrusted = retired.get("firstUntrustedVersion", "")
    if not public_key or not first_untrusted:
        fail("each retired key needs publicKeyBase64 and firstUntrustedVersion")
    if current_version >= version_tuple(first_untrusted) and public_key in compiled_keys:
        fail(
            f"retired update key is still trusted in {version_match.group(1)} "
            f"(retirement begins at {first_untrusted})"
        )

print(
    f"OK: native FTS {version_match.group(1)} trusts exactly "
    f"{len(compiled_keys)} policy-approved update key(s)"
)
