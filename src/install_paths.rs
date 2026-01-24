use std::path::PathBuf;

use anyhow::Context;

pub fn current_exe_path() -> PathBuf {
    std::env::current_exe().unwrap_or_else(|_| PathBuf::from("unknown"))
}

pub fn get_user_install_dir() -> anyhow::Result<PathBuf> {
    match std::env::consts::OS {
        "macos" => Ok(home_dir()?.join("Library/Application Support/TabMail/native")),
        "windows" => {
            let local = std::env::var("LOCALAPPDATA")
                .ok()
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| home_dir().unwrap_or_else(|_| PathBuf::from("C:\\")).join("AppData/Local"));
            Ok(local.join("TabMail/native"))
        }
        _ => Ok(home_dir()?.join(".local/share/tabmail/native")),
    }
}

pub fn get_system_install_dir() -> anyhow::Result<PathBuf> {
    match std::env::consts::OS {
        "macos" => Ok(PathBuf::from("/Applications/TabMail.app/Contents/Resources")),
        "windows" => {
            let pf = std::env::var("PROGRAMFILES")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "C:\\Program Files".to_string());
            Ok(PathBuf::from(pf).join("TabMail/native"))
        }
        _ => Ok(PathBuf::from("/opt/tabmail")),
    }
}

/// Get all possible system install directories.
/// On Windows, the installer may place the helper in either:
/// - C:\Program Files\TabMail\native (dedicated TabMail dir)
/// - C:\Program Files\Mozilla Thunderbird (inside TB's directory)
/// - C:\Program Files (x86)\Mozilla Thunderbird (32-bit TB)
pub fn get_system_install_dirs() -> Vec<PathBuf> {
    match std::env::consts::OS {
        "macos" => vec![PathBuf::from("/Applications/TabMail.app/Contents/Resources")],
        "windows" => {
            let mut dirs = Vec::new();
            
            // Primary: dedicated TabMail directory
            let pf = std::env::var("PROGRAMFILES")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "C:\\Program Files".to_string());
            dirs.push(PathBuf::from(&pf).join("TabMail\\native"));
            
            // Secondary: inside Mozilla Thunderbird directory (current installer behavior)
            dirs.push(PathBuf::from(&pf).join("Mozilla Thunderbird"));
            
            // Also check 32-bit Program Files for Thunderbird
            let pf86 = std::env::var("PROGRAMFILES(X86)")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "C:\\Program Files (x86)".to_string());
            dirs.push(PathBuf::from(&pf86).join("Mozilla Thunderbird"));
            
            dirs
        }
        _ => vec![PathBuf::from("/opt/tabmail")],
    }
}

/// Check if a path is within any known system install directory.
/// Used to detect if we're running from a system install (requiring migration to user-local).
pub fn is_in_system_install_dir(path: &PathBuf) -> bool {
    for system_dir in get_system_install_dirs() {
        if is_relative_to(path, &system_dir) {
            return true;
        }
    }
    false
}

pub fn native_manifest_dirs_user() -> anyhow::Result<Vec<PathBuf>> {
    match std::env::consts::OS {
        "macos" => Ok(vec![
            home_dir()?.join("Library/Application Support/Mozilla/NativeMessagingHosts"),
            home_dir()?.join("Library/Mozilla/NativeMessagingHosts"),
        ]),
        "windows" => Ok(vec![]), // registry-based
        _ => Ok(vec![home_dir()?.join(".mozilla/native-messaging-hosts")]),
    }
}

pub fn is_relative_to(path: &PathBuf, base: &PathBuf) -> bool {
    // Stable path prefix comparison without unstable Path::is_relative_to.
    let p = path.to_string_lossy();
    let b = base.to_string_lossy();
    p.starts_with(b.as_ref())
}

pub fn home_dir() -> anyhow::Result<PathBuf> {
    if let Ok(v) = std::env::var("HOME") {
        if !v.is_empty() {
            return Ok(PathBuf::from(v));
        }
    }
    if let Ok(v) = std::env::var("USERPROFILE") {
        if !v.is_empty() {
            return Ok(PathBuf::from(v));
        }
    }
    anyhow::bail!("cannot determine home directory")
}

pub fn exe_file_name() -> &'static str {
    if std::env::consts::OS == "windows" {
        "fts_helper.exe"
    } else {
        "fts_helper"
    }
}

pub fn can_write_dir(dir: &PathBuf) -> bool {
    // Best-effort: attempt create a temp file.
    let probe = dir.join(".tabmail_write_probe.tmp");
    match std::fs::write(&probe, b"probe") {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    }
}

pub fn ensure_dir(dir: &PathBuf) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("failed to create dir {}", dir.display()))
}


