#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

set -euo pipefail

# Rotate the native-fts update signing key (Ed25519).
#
# This script:
# - Generates a new Ed25519 keypair PEM under tabmail-native-fts/.secrets/
# - Prints the raw public key as base64 and the private PEM path
# - Records the new PEM as PENDING in tabmail-native-fts/.dev.vars
# - Leaves the active manifest signer unchanged for the bridge release
#
# Rotation support requirements:
# - The Rust host supports accepting multiple public keys (see TM_UPDATE_PUBLIC_KEYS_BASE64
#   and the compiled-in UPDATE_PUBLIC_KEYS_BASE64 array in src/update_signature.rs).
# - The array is an ACCUMULATING trust pool: on rotation we add the new pubkey,
#   KEEP the old one, and ship a bridge binary signed by the OLD key.
# - Only after the overlap/adoption window may the pending key be promoted to
#   active manifest signer. Changing the signer in the bridge release strands
#   every older client, because update manifests currently carry one signature.
#
# IMPORTANT: This script does NOT commit anything.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRETS_DIR="${ROOT_DIR}/.secrets"
DEV_VARS="${ROOT_DIR}/.dev.vars"

# macOS ships LibreSSL as /usr/bin/openssl, which does not support Ed25519.
# Prefer an explicitly configured binary, then Homebrew OpenSSL 3.
OPENSSL_BIN="${OPENSSL_BIN:-}"
if [[ -z "$OPENSSL_BIN" ]]; then
  for candidate in \
    /opt/homebrew/opt/openssl@3/bin/openssl \
    /usr/local/opt/openssl@3/bin/openssl \
    /opt/homebrew/bin/openssl \
    openssl; do
    if [[ "$candidate" == */* ]]; then
      [[ -x "$candidate" ]] || continue
    elif ! command -v "$candidate" >/dev/null 2>&1; then
      continue
    fi
    if "$candidate" list -public-key-algorithms 2>/dev/null | grep -q ED25519; then
      OPENSSL_BIN="$candidate"
      break
    fi
  done
fi
if [[ -z "$OPENSSL_BIN" ]]; then
  echo "ERROR: OpenSSL with Ed25519 support is required (install openssl@3)." >&2
  exit 1
fi

mkdir -p "$SECRETS_DIR"

TS="$(date +%Y%m%d-%H%M%S)"
KEY_PEM="${SECRETS_DIR}/update_signing_ed25519.${TS}.pem"

echo "=== TabMail native-fts update signing key rotation ==="
echo ""
echo "Generating new Ed25519 private key PEM:"
echo "  $KEY_PEM"
"$OPENSSL_BIN" genpkey -algorithm ED25519 -out "$KEY_PEM" >/dev/null 2>&1

KEYTXT="$("$OPENSSL_BIN" pkey -in "$KEY_PEM" -text -noout)"
PUB_HEX="$(echo "$KEYTXT" | awk 'BEGIN{inpub=0} /^pub:/{inpub=1; next} /^priv:/{inpub=0} {if(inpub){gsub(/[^0-9a-f:]/,"",$0); if($0!=""){print $0}}}' | tr -d ':' | tr -d '\n')"

PUB_B64="$(python3 - <<PY
import base64
print(base64.b64encode(bytes.fromhex("$PUB_HEX")).decode())
PY
)"

echo ""
echo "New public key (base64)  : $PUB_B64"
echo ""
echo "PEM path: $KEY_PEM"

if [ -f "$DEV_VARS" ]; then
  echo ""
  echo "Updating $DEV_VARS (backup + set TM_UPDATE_NEXT_PRIVATE_KEY_PEM_PATH)..."
  cp "$DEV_VARS" "$DEV_VARS.backup.$TS"
  DEV_VARS_TMP="$(mktemp)"
  awk -v next_key_pem="$KEY_PEM" '
    BEGIN { found = 0 }
    /^TM_UPDATE_NEXT_PRIVATE_KEY_PEM_PATH=/ {
      print "TM_UPDATE_NEXT_PRIVATE_KEY_PEM_PATH=\"" next_key_pem "\""
      found = 1
      next
    }
    { print }
    END {
      if (!found) print "TM_UPDATE_NEXT_PRIVATE_KEY_PEM_PATH=\"" next_key_pem "\""
    }
  ' "$DEV_VARS" > "$DEV_VARS_TMP"
  chmod 600 "$DEV_VARS_TMP"
  mv "$DEV_VARS_TMP" "$DEV_VARS"
  echo "✓ Updated TM_UPDATE_NEXT_PRIVATE_KEY_PEM_PATH"
  echo "✓ Left TM_UPDATE_PRIVATE_KEY_PEM_PATH unchanged (bridge signer)"
else
  echo ""
  echo "NOTE: $DEV_VARS not found; set TM_UPDATE_NEXT_PRIVATE_KEY_PEM_PATH manually."
fi

echo ""
echo "Next steps (manual):"
echo "1) Add the NEW public key to the ACCUMULATING array (keep the old one):"
echo "   src/update_signature.rs → UPDATE_PUBLIC_KEYS_BASE64"
echo "2) Record it as pending in release/update-signing-policy.json."
echo "3) Ship a bridge host + installers while manifests remain signed by the"
echo "   OLD active TM_UPDATE_PRIVATE_KEY_PEM_PATH. Old clients can then update"
echo "   into the binary that trusts both keys."
echo "4) After the overlap/adoption window, explicitly promote the pending PEM"
echo "   for a LATER manifest. Never switch the signer in the bridge release."
echo "5) DO NOT remove the old public key. The array is an accumulating trust"
echo "   pool; removal strands clients that haven't self-updated. Removal is"
echo "   only a compromise-response action."
echo ""
