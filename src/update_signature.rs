use anyhow::{bail, Context};
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

use crate::config;

// Public keys used to verify update signatures (rotation-safe).
//
// Rotation model:
// - During rotation, include BOTH the old and new public keys here (or via env var) so that
//   manifests signed with either key are accepted.
// - After all clients have upgraded and you no longer need the old key, remove it.
//
// Env override (comma-separated):
//   TM_UPDATE_PUBLIC_KEYS_BASE64="base64key1,base64key2"
pub const UPDATE_PUBLIC_KEYS_BASE64: &[&str] = &[
    "Uirza74DhxMIoj54D/XkTymObvX/SpZiG1l1g+6BADE=",
];

pub fn make_signed_message(version: &str, platform: &str, sha256_hex: &str, url: &str) -> String {
    // Deterministic signing payload.
    //
    // We sign metadata rather than raw bytes so TB can validate “what is being installed”
    // and the host can validate the same without ambiguity.
    format!(
        "tabmail-native-fts|host_version={}|platform={}|sha256={}|url={}",
        version, platform, sha256_hex, url
    )
}

pub fn verify_update_signature(
    version: &str,
    platform: &str,
    sha256_hex: &str,
    url: &str,
    signature_base64: &str,
) -> anyhow::Result<()> {
    let mut keys: Vec<String> = vec![];
    if let Ok(v) = std::env::var("TM_UPDATE_PUBLIC_KEYS_BASE64") {
        keys.extend(
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        );
    }
    if keys.is_empty() {
        // Fallback to compiled-in list for production.
        keys.extend(
            UPDATE_PUBLIC_KEYS_BASE64
                .iter()
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty()),
        );
    }
    if keys.is_empty() {
        bail!(
            "update signature verification not configured (missing UPDATE_PUBLIC_KEYS_BASE64 / TM_UPDATE_PUBLIC_KEYS_BASE64); host version {}",
            config::HOST_VERSION
        );
    }

    let msg = make_signed_message(version, platform, sha256_hex, url);

    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(signature_base64)
        .context("invalid signature base64")?;
    let sig = Signature::from_slice(&sig_bytes).context("invalid signature bytes")?;

    for pk_b64 in keys {
        let pk_bytes = match base64::engine::general_purpose::STANDARD.decode(pk_b64) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let pk_arr: [u8; 32] = match pk_bytes.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => continue,
        };
        let vk = match VerifyingKey::from_bytes(&pk_arr) {
            Ok(vk) => vk,
            Err(_) => continue,
        };

        if vk.verify(msg.as_bytes(), &sig).is_ok() {
            return Ok(());
        }
    }

    bail!("update signature verification failed");
}


