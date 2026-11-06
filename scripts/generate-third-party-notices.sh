#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# Generate THIRD_PARTY_NOTICES.txt from Cargo.lock with cargo-about (about.toml, about.hbs), then
# check it. Run it after every Cargo.lock change and commit the result.
#
# Needs cargo-about at the pinned version:
#   cargo install cargo-about --version 0.9.3 --locked --features cli
# and the network (Cargo fetches the crates; a few license files come from the crates' repositories
# at pinned checksums, see about.toml).
set -euo pipefail

ABOUT_VERSION="0.9.3"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

installed="$(cargo about --version 2>/dev/null || true)"
if [[ "$installed" != "cargo-about $ABOUT_VERSION" ]]; then
  echo "ERROR: needs cargo-about $ABOUT_VERSION (found: ${installed:-none})." >&2
  echo "Install it with: cargo install cargo-about --version $ABOUT_VERSION --locked --features cli" >&2
  exit 1
fi

cargo about generate --locked --fail about.hbs -o THIRD_PARTY_NOTICES.txt
python3 -I scripts/check-third-party-notices.py "$ROOT"
