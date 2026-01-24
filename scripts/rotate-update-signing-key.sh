#!/usr/bin/env bash
set -euo pipefail

# Rotate the native-fts update signing key (Ed25519).
#
# This script:
# - Generates a new Ed25519 keypair PEM under tabmail-native-fts/.secrets/
# - Prints raw public/private keys as base64 (for CI secret stores)
# - Updates tabmail-native-fts/.dev.vars to point to the new PEM
#
# Rotation support requirements:
# - The Rust host supports accepting multiple public keys (see TM_UPDATE_PUBLIC_KEYS_BASE64)
# - During rotation, publish a host build that accepts BOTH old+new pubkeys, then switch signing.
#
# IMPORTANT: This script does NOT commit anything.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRETS_DIR="${ROOT_DIR}/.secrets"
DEV_VARS="${ROOT_DIR}/.dev.vars"

mkdir -p "$SECRETS_DIR"

TS="$(date +%Y%m%d-%H%M%S)"
KEY_PEM="${SECRETS_DIR}/update_signing_ed25519.${TS}.pem"

echo "=== TabMail native-fts update signing key rotation ==="
echo ""
echo "Generating new Ed25519 private key PEM:"
echo "  $KEY_PEM"
openssl genpkey -algorithm ED25519 -out "$KEY_PEM" >/dev/null 2>&1

KEYTXT="$(openssl pkey -in "$KEY_PEM" -text -noout)"
PUB_HEX="$(echo "$KEYTXT" | awk 'BEGIN{inpub=0} /^pub:/{inpub=1; next} /^priv:/{inpub=0} {if(inpub){gsub(/[^0-9a-f:]/,\"\",$0); if($0!=\"\"){print $0}}}' | tr -d ':' | tr -d '\n')"
PRIV_HEX="$(echo "$KEYTXT" | awk 'BEGIN{inpriv=0} /^priv:/{inpriv=1; next} /^pub:/{inpriv=0} {if(inpriv){gsub(/[^0-9a-f:]/,\"\",$0); if($0!=\"\"){print $0}}}' | tr -d ':' | tr -d '\n')"

PUB_B64="$(python3 - <<PY
import base64
print(base64.b64encode(bytes.fromhex("$PUB_HEX")).decode())
PY
)"

PRIV_B64="$(python3 - <<PY
import base64
print(base64.b64encode(bytes.fromhex("$PRIV_HEX")).decode())
PY
)"

echo ""
echo "New public key (base64)  : $PUB_B64"
echo "New private key (base64) : $PRIV_B64"
echo ""
echo "PEM path: $KEY_PEM"

if [ -f "$DEV_VARS" ]; then
  echo ""
  echo "Updating $DEV_VARS (backup + set TM_UPDATE_PRIVATE_KEY_PEM_PATH)..."
  cp "$DEV_VARS" "$DEV_VARS.backup.$TS"
  if command -v perl >/dev/null 2>&1; then
    perl -i -pe "s|^TM_UPDATE_PRIVATE_KEY_PEM_PATH=.*|TM_UPDATE_PRIVATE_KEY_PEM_PATH=\"$KEY_PEM\"|g" "$DEV_VARS" || true
  else
    # macOS sed -i requires extension
    sed -i.bak "s|^TM_UPDATE_PRIVATE_KEY_PEM_PATH=.*|TM_UPDATE_PRIVATE_KEY_PEM_PATH=\"$KEY_PEM\"|g" "$DEV_VARS" || true
    rm -f "$DEV_VARS.bak"
  fi
  echo "✓ Updated TM_UPDATE_PRIVATE_KEY_PEM_PATH"
else
  echo ""
  echo "NOTE: $DEV_VARS not found; set TM_UPDATE_PRIVATE_KEY_PEM_PATH manually."
fi

echo ""
echo "Next steps (manual):"
echo "1) Add the NEW public key to the host allowlist during rotation:"
echo "   - Set TM_UPDATE_PUBLIC_KEYS_BASE64=\"<old>,<new>\" during build, or"
echo "   - Add it to UPDATE_PUBLIC_KEYS_BASE64 in src/update_signature.rs and ship a host update"
echo "2) Switch release signing to use the NEW PEM (TM_UPDATE_PRIVATE_KEY_PEM_PATH)"
echo "3) After the fleet is upgraded, remove the old public key from the allowlist"
echo ""

