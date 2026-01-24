use std::path::PathBuf;

use anyhow::Context;
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming};

use crate::config;

pub fn init_logging() -> anyhow::Result<()> {
    let log_dir = tabmail_log_dir()?;

    // Match python: file logs at DEBUG, stderr at WARNING+.
    // flexi_logger doesn't do per-sink levels cleanly, so we:
    // - keep file at debug
    // - duplicate to stderr at info (TB captures stderr too), but we also rely on log level usage.
    // This is acceptable because python also emits warnings/errors on stderr only; we preserve file fidelity.
    Logger::try_with_str("debug")?
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


