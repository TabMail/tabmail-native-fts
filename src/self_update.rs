use std::path::{Path, PathBuf};
use std::process::Command;
use std::io::Read;

use anyhow::{bail, Context};
use sha2::{Digest, Sha256};

use crate::{config, install_paths, update_signature};

pub fn version_less_than(a: &str, b: &str) -> bool {
    let pa = parse_version(a);
    let pb = parse_version(b);
    let len = std::cmp::max(pa.len(), pb.len());
    for i in 0..len {
        let va = *pa.get(i).unwrap_or(&0);
        let vb = *pb.get(i).unwrap_or(&0);
        if va < vb {
            return true;
        }
        if va > vb {
            return false;
        }
    }
    false
}

fn parse_version(v: &str) -> Vec<u64> {
    v.split('.')
        .filter_map(|p| p.parse::<u64>().ok())
        .collect()
}

pub fn auto_migrate_to_user_local() -> anyhow::Result<bool> {
    let current_path = install_paths::current_exe_path();
    let user_dir = install_paths::get_user_install_dir()?;

    // Check if we're running from any known system install location
    if !install_paths::is_in_system_install_dir(&current_path) {
        log::info!("Already running from user-local or custom location: {}", current_path.display());
        return Ok(false);
    }

    let user_exe = user_dir.join(install_paths::exe_file_name());
    if user_exe.exists() {
        log::info!("User-local install already exists at {}", user_exe.display());
        return Ok(true);
    }

    log::info!("🔄 Auto-migrating from system to user-local for auto-updates...");
    log::info!("   From: {}", current_path.display());
    log::info!("   To:   {}", user_exe.display());

    install_paths::ensure_dir(&user_dir)?;
    std::fs::copy(&current_path, &user_exe)
        .with_context(|| format!("failed copying to {}", user_exe.display()))?;
    make_executable(&user_exe)?;
    remove_quarantine(&user_exe);

    for manifest_dir in install_paths::native_manifest_dirs_user()? {
        install_paths::ensure_dir(&manifest_dir)?;
        let manifest_path = manifest_dir.join("tabmail_fts.json");
        let manifest = serde_json::json!({
            "name": "tabmail_fts",
            "description": "TabMail FTS Native Helper (user-local, auto-updating)",
            "path": user_exe.to_string_lossy(),
            "type": "stdio",
            "allowed_extensions": ["thunderbird@tabmail.ai"]
        });
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)
            .with_context(|| format!("failed writing manifest {}", manifest_path.display()))?;

        log::info!("✅ User-local manifest created: {}", manifest_path.display());
    }

    log::info!("✅ Migration complete! User-local install ready for auto-updates");
    log::info!("   Next Thunderbird restart will use: {}", user_exe.display());
    Ok(true)
}

pub fn update_check(target_version: &str) -> anyhow::Result<(bool, bool)> {
    let needs_update = version_less_than(config::HOST_VERSION, target_version);
    let current = install_paths::current_exe_path();
    let can_update = current.parent().map(|p| install_paths::can_write_dir(&p.to_path_buf())).unwrap_or(false);
    Ok((needs_update, can_update))
}

pub struct UpdateParams<'a> {
    pub target_version: &'a str,
    pub update_url: &'a str,
    pub sha256_hex: &'a str,
    pub platform: &'a str,
    pub signature_base64: &'a str,
}

pub struct UpdateResult {
    pub success: bool,
    pub old_version: String,
    pub new_version: String,
    pub install_path: PathBuf,
    pub requires_restart: bool,
    pub message: String,
}

pub fn update_request(p: UpdateParams<'_>) -> anyhow::Result<UpdateResult> {
    log::info!(
        "Starting self-update from {} to {}",
        config::HOST_VERSION,
        p.target_version
    );
    log::info!("Download URL: {}", p.update_url);

    // Verify signature BEFORE download so we fail fast on manifest tampering.
    update_signature::verify_update_signature(
        p.target_version,
        p.platform,
        p.sha256_hex,
        p.update_url,
        p.signature_base64,
    )?;

    let current_path = install_paths::current_exe_path();
    let user_dir = install_paths::get_user_install_dir()?;

    let can_write_here = current_path
        .parent()
        .map(|d| install_paths::can_write_dir(&d.to_path_buf()))
        .unwrap_or(false);

    let target_path = if can_write_here {
        current_path.clone()
    } else {
        log::warn!(
            "Cannot write to {}, installing update into user directory instead",
            current_path.parent().map(|p| p.display().to_string()).unwrap_or_else(|| "unknown".to_string())
        );
        install_paths::ensure_dir(&user_dir)?;
        user_dir.join(install_paths::exe_file_name())
    };

    // If current is system install and cannot write, and target is user-local, ensure manifest exists.
    if install_paths::is_in_system_install_dir(&current_path) && target_path != current_path {
        for manifest_dir in install_paths::native_manifest_dirs_user()? {
            install_paths::ensure_dir(&manifest_dir)?;
            let manifest_path = manifest_dir.join("tabmail_fts.json");
            let manifest = serde_json::json!({
                "name": "tabmail_fts",
                "description": "TabMail FTS Native Helper (user-local, auto-updating)",
                "path": target_path.to_string_lossy(),
                "type": "stdio",
                "allowed_extensions": ["thunderbird@tabmail.ai"]
            });
            std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)
                .with_context(|| format!("failed writing manifest {}", manifest_path.display()))?;
            log::info!("✅ User-local manifest ensured: {}", manifest_path.display());
        }
    }

    // Backup current version (if exists).
    let backup_path = target_path.with_extension(format!(
        "{}backup",
        target_path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    if target_path.exists() {
        std::fs::copy(&target_path, &backup_path)
            .with_context(|| format!("failed backing up to {}", backup_path.display()))?;
        log::info!("Backed up current version to {}", backup_path.display());
    }

    // Download to staged file first.
    let staged_path = PathBuf::from(format!("{}{}", target_path.display(), config::update::STAGED_SUFFIX));
    download_to(&staged_path, p.update_url, p.sha256_hex)?;
    make_executable(&staged_path)?;
    remove_quarantine(&staged_path);

    // Apply: on unix we can atomically replace even if current is running; on windows we need a helper.
    if std::env::consts::OS == "windows" {
        spawn_apply_update_helper(&target_path, &staged_path)?;
        Ok(UpdateResult {
            success: true,
            old_version: config::HOST_VERSION.to_string(),
            new_version: p.target_version.to_string(),
            install_path: target_path,
            requires_restart: true,
            message: format!(
                "Updated from {} to {}. Restarting...",
                config::HOST_VERSION,
                p.target_version
            ),
        })
    } else {
        // Atomic replace
        std::fs::rename(&staged_path, &target_path)
            .with_context(|| format!("failed replacing {}", target_path.display()))?;
        
        // Remove quarantine from final target path (macOS adds it during write)
        remove_quarantine(&target_path);
        
        Ok(UpdateResult {
            success: true,
            old_version: config::HOST_VERSION.to_string(),
            new_version: p.target_version.to_string(),
            install_path: target_path,
            requires_restart: false,
            message: format!(
                "Updated from {} to {}. Reconnecting automatically...",
                config::HOST_VERSION,
                p.target_version
            ),
        })
    }
}

fn download_to(dest_path: &Path, url: &str, expected_sha256_hex: &str) -> anyhow::Result<()> {
    log::info!("Downloading {} to {}", url, dest_path.display());

    let resp = ureq::get(url)
        .timeout(std::time::Duration::from_secs(config::update::DOWNLOAD_TIMEOUT_SECS))
        .call()
        .context("download failed")?;

    if resp.status() >= 400 {
        bail!("download failed with status {}", resp.status());
    }

    let mut reader = resp.into_reader();
    let mut bytes: Vec<u8> = vec![];
    reader
        .read_to_end(&mut bytes)
        .context("failed reading download body")?;

    let actual_sha = Sha256::digest(&bytes);
    let actual_hex = hex::encode(actual_sha);
    if !eq_hex_lower(&actual_hex, expected_sha256_hex) {
        bail!("SHA256 mismatch: expected {}, got {}", expected_sha256_hex, actual_hex);
    }
    log::info!("SHA256 verified: {}", actual_hex);

    std::fs::write(dest_path, &bytes)
        .with_context(|| format!("failed writing {}", dest_path.display()))?;

    Ok(())
}

fn eq_hex_lower(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

fn make_executable(p: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(p)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(p, perms)?;
    }
    Ok(())
}

/// Remove macOS quarantine attribute from downloaded file.
/// Downloaded binaries can't be stapled, so we remove quarantine to prevent
/// the "downloaded from internet" popup on first run.
fn remove_quarantine(p: &Path) {
    #[cfg(target_os = "macos")]
    {
        log::info!("Attempting to remove quarantine from: {}", p.display());
        
        // First check if quarantine exists
        let check = Command::new("xattr")
            .arg("-p")
            .arg("com.apple.quarantine")
            .arg(p)
            .output();
        
        match &check {
            Ok(output) => {
                if output.status.success() {
                    log::info!("Quarantine attribute present: {}", 
                        String::from_utf8_lossy(&output.stdout).trim());
                } else {
                    log::info!("No quarantine attribute found on file");
                    return;
                }
            }
            Err(e) => {
                log::warn!("Failed to check quarantine: {}", e);
            }
        }
        
        // Now remove it
        match Command::new("xattr")
            .arg("-d")
            .arg("com.apple.quarantine")
            .arg(p)
            .output()
        {
            Ok(output) => {
                if output.status.success() {
                    log::info!("✅ Removed quarantine attribute from {}", p.display());
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    log::warn!("xattr -d failed (exit {}): {}", 
                        output.status.code().unwrap_or(-1), stderr.trim());
                }
            }
            Err(e) => {
                log::warn!("Failed to run xattr -d: {}", e);
            }
        }
        
        // Verify removal
        let verify = Command::new("xattr")
            .arg("-p")
            .arg("com.apple.quarantine")
            .arg(p)
            .output();
        
        match verify {
            Ok(output) => {
                if output.status.success() {
                    log::warn!("⚠️ Quarantine still present after removal attempt: {}", 
                        String::from_utf8_lossy(&output.stdout).trim());
                } else {
                    log::info!("✅ Verified: quarantine attribute removed");
                }
            }
            Err(e) => {
                log::warn!("Failed to verify quarantine removal: {}", e);
            }
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = p; // silence unused warning
    }
}

fn spawn_apply_update_helper(target: &Path, staged: &Path) -> anyhow::Result<()> {
    // On Windows, we can't overwrite a running exe. We spawn a TEMP COPY of ourselves to perform the swap.
    let current = install_paths::current_exe_path();
    let tmp_dir = std::env::temp_dir().join("tabmail-native-fts-update");
    std::fs::create_dir_all(&tmp_dir)?;
    let helper = tmp_dir.join("fts_helper_apply_update.exe");
    std::fs::copy(&current, &helper).with_context(|| format!("failed copying helper to {}", helper.display()))?;

    log::info!("Spawning apply-update helper: {}", helper.display());
    Command::new(&helper)
        .arg("--apply-update")
        .arg("--target")
        .arg(target)
        .arg("--staged")
        .arg(staged)
        .spawn()
        .context("failed spawning apply-update helper")?;
    Ok(())
}

pub fn apply_update_mode(target: &Path, staged: &Path) -> anyhow::Result<()> {
    log::info!("Apply-update mode: target={}, staged={}", target.display(), staged.display());

    // Give parent time to exit.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Move current to backup, then staged -> target.
    let backup = target.with_extension("exe.backup");

    // Best-effort retries.
    let retries: usize = 20;
    let delay = std::time::Duration::from_millis(250);
    for attempt in 1..=retries {
        match try_swap_files(target, staged, &backup) {
            Ok(_) => {
                log::info!("✅ Update applied successfully");
                return Ok(());
            }
            Err(e) => {
                log::warn!("Apply-update attempt {} failed: {}", attempt, e);
                std::thread::sleep(delay);
            }
        }
    }

    bail!("failed applying update after {} attempts", retries);
}

fn try_swap_files(target: &Path, staged: &Path, backup: &Path) -> anyhow::Result<()> {
    if target.exists() {
        let _ = std::fs::remove_file(backup);
        std::fs::rename(target, backup).with_context(|| format!("failed renaming target to {}", backup.display()))?;
    }
    std::fs::rename(staged, target).with_context(|| format!("failed renaming staged to {}", target.display()))?;
    Ok(())
}


