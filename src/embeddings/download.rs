// download.rs — Model file download with SHA256 verification.
//
// Downloads model weights from CDN on first use, caches locally at ~/.tabmail/models/.
// Files are verified against known SHA256 hashes to ensure integrity.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use sha2::{Digest, Sha256};

use crate::config;

/// Returns the local model directory path (~/.tabmail/models/all-MiniLM-L6-v2/).
pub fn model_dir() -> anyhow::Result<PathBuf> {
    let home = dirs_home()?;
    Ok(home.join(config::embedding::MODEL_DIR_REL))
}

/// Check if all required model files exist locally.
pub fn model_files_exist() -> anyhow::Result<bool> {
    let dir = model_dir()?;
    Ok(dir.join("model.safetensors").exists()
        && dir.join("tokenizer.json").exists()
        && dir.join("config.json").exists())
}

/// Download all model files if not already cached. Returns the model directory path.
pub fn ensure_model_files() -> anyhow::Result<PathBuf> {
    let dir = model_dir()?;

    if model_files_exist()? {
        log::info!("Model files already cached at {}", dir.display());
        return Ok(dir);
    }

    log::info!("Downloading embedding model to {}", dir.display());
    fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create model dir {}", dir.display()))?;

    let base = config::embedding::MODEL_CDN_BASE;

    download_and_verify(
        &format!("{base}/model.safetensors"),
        &dir.join("model.safetensors"),
        config::embedding::MODEL_SAFETENSORS_SHA256,
    )?;

    download_and_verify(
        &format!("{base}/tokenizer.json"),
        &dir.join("tokenizer.json"),
        config::embedding::TOKENIZER_JSON_SHA256,
    )?;

    download_and_verify(
        &format!("{base}/config.json"),
        &dir.join("config.json"),
        config::embedding::CONFIG_JSON_SHA256,
    )?;

    log::info!("Model download complete");
    Ok(dir)
}

/// Download a file from URL and verify its SHA256 hash.
fn download_and_verify(url: &str, dest: &Path, expected_sha256: &str) -> anyhow::Result<()> {
    let filename = dest.file_name().unwrap_or_default().to_string_lossy();
    log::info!("Downloading {} from {}", filename, url);

    let resp = ureq::get(url)
        .timeout(std::time::Duration::from_secs(config::update::DOWNLOAD_TIMEOUT_SECS * 3))
        .call()
        .with_context(|| format!("failed to download {url}"))?;

    let status = resp.status();
    if status != 200 {
        bail!("HTTP {status} downloading {url}");
    }

    // Read body into memory (model is ~87 MB, fits in RAM)
    let mut body = Vec::new();
    resp.into_reader()
        .read_to_end(&mut body)
        .with_context(|| format!("failed to read response body for {url}"))?;

    // Verify SHA256
    let mut hasher = Sha256::new();
    hasher.update(&body);
    let actual_hash = hex::encode(hasher.finalize());

    if actual_hash != expected_sha256 {
        bail!(
            "SHA256 mismatch for {}: expected {}, got {}",
            filename,
            expected_sha256,
            actual_hash
        );
    }

    log::info!("SHA256 verified for {} ({})", filename, &actual_hash[..12]);

    // Write atomically: write to .tmp, then rename
    let tmp_path = dest.with_extension("tmp");
    let mut file = fs::File::create(&tmp_path)
        .with_context(|| format!("failed to create {}", tmp_path.display()))?;
    file.write_all(&body)?;
    file.flush()?;
    drop(file);

    fs::rename(&tmp_path, dest)
        .with_context(|| format!("failed to rename {} -> {}", tmp_path.display(), dest.display()))?;

    Ok(())
}

/// Get the user's home directory.
fn dirs_home() -> anyhow::Result<PathBuf> {
    // Use $HOME on all platforms (macOS, Linux, Windows via MSYS/Git Bash)
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(PathBuf::from)
        .context("cannot determine home directory (neither HOME nor USERPROFILE is set)")
}
