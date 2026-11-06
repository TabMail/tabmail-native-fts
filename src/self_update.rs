/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

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

/// This build's third-party notices: the repository's checked `THIRD_PARTY_NOTICES.txt`,
/// compiled in. Because they are part of the signed, SHA-256-checked binary, a helper that
/// replaced itself can always say exactly which notices belong to it (#62).
pub const THIRD_PARTY_NOTICES: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/THIRD_PARTY_NOTICES.txt"));

/// The notices file next to the helper at `exe`.
fn notices_beside(exe: &Path) -> anyhow::Result<PathBuf> {
    let dir = exe.parent().context("the helper's path has no folder")?;
    Ok(dir.join(config::update::NOTICES_FILE_NAME))
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{}", path.display(), suffix))
}

/// The helper version a notices text credits: its `- tabmail-native-fts <version>` line, which
/// the release check requires to match the build (ADR-NF-024).
fn credited_helper_version(notices: &str) -> Option<&str> {
    notices
        .lines()
        .find_map(|line| line.strip_prefix(config::update::NOTICES_HELPER_CREDIT_PREFIX))
        .map(str::trim)
}

/// Writes `text` to `dest`: beside it first, then renamed into place, so a reader never sees
/// a partial file. Replaces what is there.
fn write_replacing(dest: &Path, text: &[u8]) -> anyhow::Result<()> {
    let staged = with_suffix(dest, config::update::STAGED_SUFFIX);
    let written = std::fs::write(&staged, text)
        .with_context(|| format!("failed writing {}", staged.display()))
        .and_then(|_| {
            std::fs::rename(&staged, dest).with_context(|| format!("failed replacing {}", dest.display()))
        });
    if written.is_err() {
        let _ = std::fs::remove_file(&staged);
    }
    written
}

/// Publishes `text` at `dest` only if nothing is there, atomically: written to a file only
/// this process uses, then hard-linked into place, which fails rather than replace a file.
/// Returns whether it published.
fn publish_if_absent(dest: &Path, text: &[u8]) -> anyhow::Result<bool> {
    let own = with_suffix(dest, &format!(".{}{}", std::process::id(), config::update::STAGED_SUFFIX));
    std::fs::write(&own, text).with_context(|| format!("failed writing {}", own.display()))?;
    let linked = std::fs::hard_link(&own, dest);
    let _ = std::fs::remove_file(&own);
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e).with_context(|| format!("failed publishing {}", dest.display())),
    }
}

/// On `hello`: when there are no notices next to the helper at `exe`, puts this build's there.
/// That is the state an install from before the notices existed is in after an update applied
/// by an updater from before #62. It never replaces a notices file: every updater since #62
/// installs the new build's notices together with its binary, and an older process still
/// running from the folder must not put its own back. Writes nothing when `exe` is not named as
/// a TabMail FTS install: a client that bundles its own copy (Voice) ships its notices under
/// its own name, inside a signed app this must not change.
pub fn install_missing_notices(exe: &Path) -> anyhow::Result<bool> {
    if exe.file_name().and_then(|n| n.to_str()) != Some(install_paths::exe_file_name()) {
        return Ok(false);
    }
    let notices = notices_beside(exe)?;
    if std::fs::symlink_metadata(&notices).is_ok() {
        return Ok(false);
    }
    publish_if_absent(&notices, THIRD_PARTY_NOTICES.as_bytes())
}

pub fn auto_migrate_to_user_local() -> anyhow::Result<bool> {
    let current_path = install_paths::current_exe_path();
    migrate_from(
        &current_path,
        &install_paths::get_user_install_dir()?,
        install_paths::is_in_system_install_dir(&current_path),
        install_paths::native_manifest_dirs_user,
    )
}

/// The migration of the helper at `current` into `user_dir`, when it runs from a system
/// install. Its writes go in this order: this build's notices beside the copy, the copy, then
/// the native-messaging manifests pointing at it. So the copy is never there without its
/// notices and nothing points at it before both are; a failure stops before the later writes,
/// and the next `hello` retries.
fn migrate_from(
    current_path: &Path,
    user_dir: &Path,
    is_system_install: bool,
    manifest_dirs: impl FnOnce() -> anyhow::Result<Vec<PathBuf>>,
) -> anyhow::Result<bool> {
    // Check if we're running from any known system install location
    if !is_system_install {
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

    install_paths::ensure_dir(&user_dir.to_path_buf())?;
    let manifest_dirs = manifest_dirs()?;
    write_replacing(&notices_beside(&user_exe)?, THIRD_PARTY_NOTICES.as_bytes())?;
    std::fs::copy(current_path, &user_exe)
        .with_context(|| format!("failed copying to {}", user_exe.display()))?;
    make_executable(&user_exe)?;
    remove_quarantine(&user_exe);

    for manifest_dir in manifest_dirs {
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

/// The notices a downloaded helper carries: it is run with `PRINT_NOTICES_ARG` (it has already
/// passed the signature and SHA-256 checks) and must exit 0 within `timeout`, printing notices
/// that credit `version`.
fn notices_of(binary: &Path, version: &str, timeout: std::time::Duration) -> anyhow::Result<Vec<u8>> {
    let mut child = Command::new(binary)
        .arg(config::update::PRINT_NOTICES_ARG)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .with_context(|| format!("failed running {} for its notices", binary.display()))?;
    let mut out = child.stdout.take().context("no stdout from the downloaded helper")?;
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        out.read_to_end(&mut bytes).map(|_| bytes)
    });
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("the downloaded helper did not print its notices within {:?}", timeout);
        }
        std::thread::sleep(std::time::Duration::from_millis(config::update::NOTICES_READ_POLL_MS));
    };
    let bytes = reader
        .join()
        .map_err(|_| anyhow::anyhow!("reading the downloaded helper's notices panicked"))?
        .context("failed reading the downloaded helper's notices")?;
    if !status.success() {
        bail!("the downloaded helper exited with {} printing its notices", status);
    }
    let text = std::str::from_utf8(&bytes).context("the downloaded helper's notices are not UTF-8")?;
    match credited_helper_version(text) {
        Some(credited) if credited == version => Ok(bytes),
        credited => bail!("the downloaded helper's notices credit version {:?}, not {}", credited, version),
    }
}

/// Installs a new build's notices and binary as a pair. The installed notices are kept as a
/// backup, `staged_notices` is renamed into place, and then `swap_binary` runs. If it fails, the
/// old notices are put back (or the new ones removed when there were none), so the folder holds
/// the old pair or the new pair. If the notices cannot be put in place, the binary is not
/// touched.
fn commit_pair(
    notices: &Path,
    staged_notices: &Path,
    swap_binary: impl FnOnce() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let backup = with_suffix(notices, config::update::NOTICES_BACKUP_SUFFIX);
    let had_old = notices.exists();
    if had_old {
        std::fs::copy(notices, &backup)
            .with_context(|| format!("failed backing up {}", notices.display()))?;
    }
    if let Err(e) = std::fs::rename(staged_notices, notices) {
        let _ = std::fs::remove_file(&backup);
        return Err(e).with_context(|| format!("failed replacing {}", notices.display()));
    }
    if let Err(e) = swap_binary() {
        let restored = if had_old {
            std::fs::rename(&backup, notices)
        } else {
            std::fs::remove_file(notices)
        };
        if let Err(r) = restored {
            log::error!("Could not put the previous notices back at {}: {}", notices.display(), r);
        }
        return Err(e);
    }
    let _ = std::fs::remove_file(&backup);
    Ok(())
}

/// Stages the downloaded helper's notices next to `target` and installs them with the binary.
/// On Unix the pair is committed here; on Windows the apply-update process does it once this
/// process has exited. A failure before the commit removes both staged files and leaves the
/// installed pair as it was. Returns whether the helper must restart to finish the update.
fn install_downloaded(target: &Path, staged: &Path, version: &str) -> anyhow::Result<bool> {
    let notices = notices_beside(target)?;
    let staged_notices = with_suffix(&notices, config::update::STAGED_SUFFIX);
    let timeout = std::time::Duration::from_secs(config::update::NOTICES_READ_TIMEOUT_SECS);
    let installed = notices_of(staged, version, timeout)
        .and_then(|bytes| {
            std::fs::write(&staged_notices, bytes)
                .with_context(|| format!("failed writing {}", staged_notices.display()))
        })
        .and_then(|_| {
            if std::env::consts::OS == "windows" {
                spawn_apply_update_helper(target, staged, &staged_notices).map(|_| true)
            } else {
                commit_pair(&notices, &staged_notices, || {
                    std::fs::rename(staged, target)
                        .with_context(|| format!("failed replacing {}", target.display()))
                })
                .map(|_| false)
            }
        });
    if installed.is_err() {
        let _ = std::fs::remove_file(staged);
        let _ = std::fs::remove_file(&staged_notices);
    }
    installed
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
    // Either way the new build's notices are installed with it (#62).
    let requires_restart = install_downloaded(&target_path, &staged_path, p.target_version)?;
    if requires_restart {
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

fn spawn_apply_update_helper(target: &Path, staged: &Path, staged_notices: &Path) -> anyhow::Result<()> {
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
        .arg("--staged-notices")
        .arg(staged_notices)
        .spawn()
        .context("failed spawning apply-update helper")?;
    Ok(())
}

/// The Windows apply-update process: swaps in the staged binary once the old one has exited.
/// With `staged_notices` (every updater since #62 passes them) the new build's notices are
/// installed with it as a pair; an apply-update process started by an older updater passes
/// none.
pub fn apply_update_mode(target: &Path, staged: &Path, staged_notices: Option<&Path>) -> anyhow::Result<()> {
    log::info!("Apply-update mode: target={}, staged={}", target.display(), staged.display());

    // Give parent time to exit.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Move current to backup, then staged -> target.
    let backup = target.with_extension("exe.backup");

    let swap = || -> anyhow::Result<()> {
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
    };
    match staged_notices {
        Some(staged_notices) => commit_pair(&notices_beside(target)?, staged_notices, swap),
        None => swap(),
    }
}

/// Moves the installed binary to `backup` and the staged one into its place. If the staged one
/// cannot be moved in, the installed binary is put back, so a failed attempt never leaves the
/// install without a helper.
fn try_swap_files(target: &Path, staged: &Path, backup: &Path) -> anyhow::Result<()> {
    let moved_aside = target.exists();
    if moved_aside {
        let _ = std::fs::remove_file(backup);
        std::fs::rename(target, backup).with_context(|| format!("failed renaming target to {}", backup.display()))?;
    }
    if let Err(e) = std::fs::rename(staged, target) {
        if moved_aside {
            if let Err(r) = std::fs::rename(backup, target) {
                log::error!("Could not put the installed helper back at {}: {}", target.display(), r);
            }
        }
        return Err(e).with_context(|| format!("failed renaming staged to {}", target.display()));
    }
    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tabmail-notices-{}-{}", name, std::process::id()));
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn notices_in(dir: &Path) -> PathBuf {
        dir.join(config::update::NOTICES_FILE_NAME)
    }

    fn names_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn notices_crediting(version: &str) -> String {
        format!("notices\n{}{}\nend\n", config::update::NOTICES_HELPER_CREDIT_PREFIX, version)
    }

    // ---- what the binary carries ----

    #[test]
    fn the_compiled_notices_are_the_repository_file_and_credit_this_version() {
        let repo = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/THIRD_PARTY_NOTICES.txt")).unwrap();
        assert_eq!(THIRD_PARTY_NOTICES.as_bytes(), repo.as_slice());
        assert_eq!(credited_helper_version(THIRD_PARTY_NOTICES), Some(config::HOST_VERSION));
    }

    // ---- hello: create only ----

    #[test]
    fn hello_puts_notices_beside_a_helper_that_has_none() {
        let dir = test_dir("hello-absent");
        assert!(install_missing_notices(&dir.join(install_paths::exe_file_name())).unwrap());
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), THIRD_PARTY_NOTICES);
        assert_eq!(names_in(&dir), vec![config::update::NOTICES_FILE_NAME.to_string()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn hello_never_replaces_existing_notices() {
        let dir = test_dir("hello-existing");
        let other = notices_crediting("999.0.0");
        std::fs::write(notices_in(&dir), &other).unwrap();
        assert!(!install_missing_notices(&dir.join(install_paths::exe_file_name())).unwrap());
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), other);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_file_that_appears_before_the_link_is_not_replaced() {
        // The race the existence check cannot see: another process publishes first.
        let dir = test_dir("publish-race");
        let theirs = notices_crediting("999.0.0");
        std::fs::write(notices_in(&dir), &theirs).unwrap();
        assert!(!publish_if_absent(&notices_in(&dir), THIRD_PARTY_NOTICES.as_bytes()).unwrap());
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), theirs);
        assert_eq!(names_in(&dir), vec![config::update::NOTICES_FILE_NAME.to_string()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn hello_writes_nothing_for_a_helper_under_another_name() {
        let dir = test_dir("hello-bundled");
        assert!(!install_missing_notices(&dir.join("voice-memory")).unwrap());
        assert!(names_in(&dir).is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn hello_in_a_folder_it_cannot_write_fails_and_leaves_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let dir = test_dir("hello-readonly");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = install_missing_notices(&dir.join(install_paths::exe_file_name()));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err());
        assert!(names_in(&dir).is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // ---- migration ----

    struct Migration {
        dir: PathBuf,
        source: PathBuf,
        user: PathBuf,
        manifests: PathBuf,
    }

    fn migration(name: &str) -> Migration {
        let dir = test_dir(name);
        let source = dir.join("system").join(install_paths::exe_file_name());
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, b"helper bytes").unwrap();
        Migration { user: dir.join("user"), manifests: dir.join("manifests"), source, dir }
    }

    impl Migration {
        fn run(&self, is_system: bool) -> anyhow::Result<bool> {
            let manifests = self.manifests.clone();
            migrate_from(&self.source, &self.user, is_system, move || Ok(vec![manifests]))
        }
    }

    #[test]
    fn a_migrated_copy_arrives_with_its_notices_and_its_manifest() {
        let m = migration("migrate");
        assert!(m.run(true).unwrap());
        let user_exe = m.user.join(install_paths::exe_file_name());
        assert_eq!(std::fs::read(&user_exe).unwrap(), b"helper bytes");
        assert_eq!(std::fs::read_to_string(notices_in(&m.user)).unwrap(), THIRD_PARTY_NOTICES);
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(m.manifests.join("tabmail_fts.json")).unwrap()).unwrap();
        assert_eq!(manifest["path"], user_exe.to_string_lossy().as_ref());
        assert_eq!(manifest["name"], "tabmail_fts");
        std::fs::remove_dir_all(&m.dir).unwrap();
    }

    #[test]
    fn a_migration_replaces_notices_left_in_the_user_folder() {
        let m = migration("migrate-stale");
        std::fs::create_dir_all(&m.user).unwrap();
        std::fs::write(notices_in(&m.user), notices_crediting("0.0.1")).unwrap();
        assert!(m.run(true).unwrap());
        assert_eq!(std::fs::read_to_string(notices_in(&m.user)).unwrap(), THIRD_PARTY_NOTICES);
        std::fs::remove_dir_all(&m.dir).unwrap();
    }

    #[test]
    fn a_migration_that_cannot_write_the_notices_copies_nothing_and_points_at_nothing() {
        let m = migration("migrate-blocked");
        // A folder where the notices go: writing them fails, the copy itself would not.
        std::fs::create_dir_all(notices_in(&m.user).join("blocker")).unwrap();
        assert!(m.run(true).is_err());
        assert_eq!(names_in(&m.user), vec![config::update::NOTICES_FILE_NAME.to_string()]);
        assert!(!m.manifests.exists());
        // Nothing half-done blocks the retry once the obstacle is gone.
        std::fs::remove_dir_all(notices_in(&m.user)).unwrap();
        assert!(m.run(true).unwrap());
        assert_eq!(std::fs::read_to_string(notices_in(&m.user)).unwrap(), THIRD_PARTY_NOTICES);
        std::fs::remove_dir_all(&m.dir).unwrap();
    }

    #[test]
    fn a_helper_outside_a_system_install_does_not_migrate() {
        let m = migration("migrate-not-system");
        assert!(!m.run(false).unwrap());
        assert!(!m.user.exists());
        assert!(!m.manifests.exists());
        std::fs::remove_dir_all(&m.dir).unwrap();
    }

    // ---- update: the pair ----

    #[test]
    fn a_committed_pair_replaces_both_and_leaves_no_backup() {
        let dir = test_dir("commit");
        std::fs::write(notices_in(&dir), "old notices").unwrap();
        let staged = dir.join("notices.staged");
        std::fs::write(&staged, "new notices").unwrap();
        let mut swapped = false;
        commit_pair(&notices_in(&dir), &staged, || {
            swapped = true;
            Ok(())
        })
        .unwrap();
        assert!(swapped);
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), "new notices");
        assert_eq!(names_in(&dir), vec![config::update::NOTICES_FILE_NAME.to_string()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_binary_swap_that_fails_puts_the_old_notices_back() {
        let dir = test_dir("commit-swap-fails");
        std::fs::write(notices_in(&dir), "old notices").unwrap();
        let staged = dir.join("notices.staged");
        std::fs::write(&staged, "new notices").unwrap();
        let result = commit_pair(&notices_in(&dir), &staged, || {
            // The new notices are in place while the binary is swapped.
            assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), "new notices");
            bail!("swap failed")
        });
        assert!(result.is_err());
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), "old notices");
        assert_eq!(names_in(&dir), vec![config::update::NOTICES_FILE_NAME.to_string()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_binary_swap_that_fails_with_no_old_notices_leaves_none() {
        let dir = test_dir("commit-swap-fails-fresh");
        let staged = dir.join("notices.staged");
        std::fs::write(&staged, "new notices").unwrap();
        assert!(commit_pair(&notices_in(&dir), &staged, || bail!("swap failed")).is_err());
        assert!(names_in(&dir).is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn notices_that_cannot_be_put_in_place_leave_the_binary_alone() {
        let dir = test_dir("commit-notices-fail");
        std::fs::write(notices_in(&dir), "old notices").unwrap();
        let mut swapped = false;
        let result = commit_pair(&notices_in(&dir), &dir.join("missing.staged"), || {
            swapped = true;
            Ok(())
        });
        assert!(result.is_err());
        assert!(!swapped);
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), "old notices");
        assert_eq!(names_in(&dir), vec![config::update::NOTICES_FILE_NAME.to_string()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_swap_whose_staged_binary_cannot_move_in_puts_the_installed_one_back() {
        let dir = test_dir("swap-fails");
        let target = dir.join("fts_helper.exe");
        std::fs::write(&target, b"installed helper").unwrap();
        let backup = target.with_extension("exe.backup");
        assert!(try_swap_files(&target, &dir.join("missing-staged"), &backup).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"installed helper");
        assert!(!backup.exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_swap_moves_the_staged_binary_in_and_keeps_a_backup() {
        let dir = test_dir("swap");
        let target = dir.join("fts_helper.exe");
        std::fs::write(&target, b"installed helper").unwrap();
        let staged = dir.join("staged");
        std::fs::write(&staged, b"new helper").unwrap();
        let backup = target.with_extension("exe.backup");
        try_swap_files(&target, &staged, &backup).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new helper");
        assert_eq!(std::fs::read(&backup).unwrap(), b"installed helper");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // ---- update: reading a downloaded helper's notices ----

    #[cfg(unix)]
    fn fake_helper(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n[ \"$1\" = \"{}\" ] || exit 9\n{}\n", config::update::PRINT_NOTICES_ARG, body)).unwrap();
        make_executable(&path).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn a_downloaded_helper_s_notices_are_read_when_they_credit_its_version() {
        let dir = test_dir("read-ok");
        let helper = fake_helper(&dir, "helper", &format!("printf '{}'", notices_crediting("1.2.3").replace('\n', "\\n")));
        let bytes = notices_of(&helper, "1.2.3", std::time::Duration::from_secs(10)).unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), notices_crediting("1.2.3"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn notices_large_enough_to_fill_a_pipe_are_read_whole() {
        let dir = test_dir("read-large");
        let helper = fake_helper(&dir, "helper", &format!(
            "i=0; while [ $i -lt 20000 ]; do echo 'a line of notice text'; i=$((i+1)); done; echo '{}1.2.3'",
            config::update::NOTICES_HELPER_CREDIT_PREFIX));
        let bytes = notices_of(&helper, "1.2.3", std::time::Duration::from_secs(30)).unwrap();
        assert!(bytes.len() > 400_000);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn notices_crediting_another_version_are_refused() {
        let dir = test_dir("read-wrong-version");
        let helper = fake_helper(&dir, "helper", &format!("printf '{}'", notices_crediting("1.2.2").replace('\n', "\\n")));
        assert!(notices_of(&helper, "1.2.3", std::time::Duration::from_secs(10)).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_downloaded_helper_that_fails_is_refused() {
        let dir = test_dir("read-fails");
        let helper = fake_helper(&dir, "helper", &format!("printf '{}'; exit 3", notices_crediting("1.2.3").replace('\n', "\\n")));
        assert!(notices_of(&helper, "1.2.3", std::time::Duration::from_secs(10)).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_downloaded_helper_that_hangs_is_stopped_and_refused() {
        let dir = test_dir("read-hangs");
        let helper = fake_helper(&dir, "helper", "exec sleep 30");
        let started = std::time::Instant::now();
        assert!(notices_of(&helper, "1.2.3", std::time::Duration::from_millis(300)).is_err());
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // ---- update: end to end on Unix ----

    #[cfg(unix)]
    #[test]
    fn an_installed_update_ends_with_the_new_binary_and_its_notices() {
        let dir = test_dir("install");
        let target = dir.join(install_paths::exe_file_name());
        std::fs::write(&target, b"old helper").unwrap();
        std::fs::write(notices_in(&dir), "old notices").unwrap();
        let staged = fake_helper(&dir, "staged", &format!("printf '{}'", notices_crediting("1.2.3").replace('\n', "\\n")));
        let staged_bytes = std::fs::read(&staged).unwrap();
        assert!(!install_downloaded(&target, &staged, "1.2.3").unwrap());
        assert_eq!(std::fs::read(&target).unwrap(), staged_bytes);
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), notices_crediting("1.2.3"));
        let mut expected = vec![install_paths::exe_file_name().to_string(), config::update::NOTICES_FILE_NAME.to_string()];
        expected.sort();
        assert_eq!(names_in(&dir), expected);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn an_update_whose_notices_are_wrong_leaves_the_installed_pair_alone() {
        let dir = test_dir("install-wrong");
        let target = dir.join(install_paths::exe_file_name());
        std::fs::write(&target, b"old helper").unwrap();
        std::fs::write(notices_in(&dir), "old notices").unwrap();
        let staged = fake_helper(&dir, "staged", &format!("printf '{}'", notices_crediting("1.2.2").replace('\n', "\\n")));
        assert!(install_downloaded(&target, &staged, "1.2.3").is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"old helper");
        assert_eq!(std::fs::read_to_string(notices_in(&dir)).unwrap(), "old notices");
        let mut expected = vec![install_paths::exe_file_name().to_string(), config::update::NOTICES_FILE_NAME.to_string()];
        expected.sort();
        assert_eq!(names_in(&dir), expected);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
