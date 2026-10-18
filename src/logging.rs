/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::path::{Path, PathBuf};

use anyhow::Context;
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming};

use crate::config;

/// Starts file logging at `level` (one of `config::logging::LOG_LEVELS`) in
/// `log_dir_override` (the `--log-dir` argument) when given, else in
/// ~/.tabmail/logs. At "off" nothing is logged and no folder or file is made.
pub fn init_logging(log_dir_override: Option<&Path>, level: &str) -> anyhow::Result<()> {
    if level == "off" {
        return Ok(());
    }
    let log_dir = match log_dir_override {
        Some(dir) => {
            std::fs::create_dir_all(dir).with_context(|| format!("failed creating log dir {}", dir.display()))?;
            dir.to_path_buf()
        }
        None => tabmail_log_dir()?,
    };

    // The file logs at the caller's level (`--log-level`, debug by default);
    // warnings and errors are also copied to stderr, which Thunderbird captures.
    Logger::try_with_str(level)?
        .log_to_file(FileSpec::default().directory(log_dir).basename(config::logging::LOG_FILE_NAME))
        .rotate(
            Criterion::Size(config::logging::LOG_ROTATE_SIZE_BYTES),
            Naming::Numbers,
            Cleanup::KeepLogFiles(config::logging::LOG_ROTATE_KEEP_FILES),
        )
        .duplicate_to_stderr(Duplicate::Warn)
        .format(flexi_logger::detailed_format)
        .start()
        .context("failed to start logger")?;

    log::info!("{}", "=".repeat(60));
    log::info!("TabMail FTS Helper starting (Rust)");
    log::info!("Version: {}", config::HOST_VERSION);
    log::info!("Platform: {}", std::env::consts::OS);
    log::info!("{}", "=".repeat(60));

    Ok(())
}

fn tabmail_log_dir() -> anyhow::Result<PathBuf> {
    let home = home_dir().context("cannot determine home directory for logs")?;
    let dir = home.join(config::logging::LOG_DIR_REL);
    std::fs::create_dir_all(&dir).with_context(|| format!("failed creating log dir {}", dir.display()))?;
    Ok(dir)
}

fn home_dir() -> Option<PathBuf> {
    if let Ok(v) = std::env::var("HOME") {
        if !v.is_empty() {
            return Some(PathBuf::from(v));
        }
    }
    // Windows fallback
    if let Ok(v) = std::env::var("USERPROFILE") {
        if !v.is_empty() {
            return Some(PathBuf::from(v));
        }
    }
    None
}


