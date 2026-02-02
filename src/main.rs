mod config;
mod embeddings;
mod fts;
mod install_paths;
mod logging;
mod native_messaging;
mod protocol;
mod self_update;
mod update_signature;

use std::io::{stdin, stdout};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use serde_json::Value;

use crate::fts::db::{DbState, open_or_create_db};
use crate::fts::memory_db;

fn main() {
    if let Err(e) = real_main() {
        // Keep stderr noisy for user bug reports; logs also go to file.
        eprintln!("[TabMail FTS] fatal error: {e:?}");
        log::error!("Fatal error: {:?}", e);
        std::process::exit(1);
    }
}

fn real_main() -> anyhow::Result<()> {
    logging::init_logging()?;

    // Special mode used by Windows self-update: run as a short-lived helper that swaps files.
    // This must happen BEFORE we start native-messaging framing.
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 2 && args[1] == "--apply-update" {
        let target = read_arg_value(&args, "--target").context("missing --target")?;
        let staged = read_arg_value(&args, "--staged").context("missing --staged")?;
        return self_update::apply_update_mode(Path::new(&target), Path::new(&staged));
    }

    // Register sqlite-vec as an auto-extension before any DB connections are opened.
    // This makes vec0 virtual tables available in all connections.
    unsafe {
        rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute(
            sqlite_vec::sqlite3_vec_init as *const (),
        )));
    }

    log::info!("=== TabMail FTS Helper Started ===");
    log::info!("Waiting for messages from Thunderbird extension...");

    let mut state = DbState::new();
    let mut in_stream = stdin();
    let mut out_stream = stdout();

    let mut message_count: u64 = 0;
    loop {
        let req = match native_messaging::read_message(&mut in_stream) {
            Ok(Some(r)) => r,
            Ok(None) => {
                log::info!("No more messages after {} messages, exiting", message_count);
                break;
            }
            Err(e) => {
                log::error!("Error reading message: {:?}", e);
                break;
            }
        };

        message_count += 1;
        log::info!(
            "Processing message #{}: {} (id: {})",
            message_count,
            req.method,
            req.id
        );

        let resp = handle_request(&mut state, &req.method, &req.id, &req.params);
        match resp {
            Ok(v) => {
                if let Err(e) = native_messaging::write_json(&mut out_stream, &v) {
                    log::error!("Error sending response: {:?}", e);
                    break;
                }
                if state.should_exit {
                    log::info!("Exiting process as requested by handler (e.g. update)");
                    break;
                }
            }
            Err(e) => {
                log::error!("Handler error: {:?}", e);
                let err = serde_json::json!({ "id": req.id, "error": format!("{e}") });
                let _ = native_messaging::write_json(&mut out_stream, &err);
            }
        }
    }

    log::info!("=== TabMail FTS Helper Stopped ===");
    Ok(())
}

fn handle_request(state: &mut DbState, method: &str, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    match method {
        // Core handlers
        "hello" => handle_hello(msg_id, params),
        "init" => handle_init(state, msg_id, params),
        "updateCheck" => handle_update_check(msg_id, params),
        "updateRequest" => handle_update_request(state, msg_id, params),
        // Email FTS handlers
        "indexBatch" => handle_index_batch(state, msg_id, params),
        "search" => handle_search(state, msg_id, params),
        "stats" => handle_stats(state, msg_id),
        "clear" => handle_clear(state, msg_id),
        "optimize" => handle_optimize(state, msg_id),
        "filterNewMessages" => handle_filter_new_messages(state, msg_id, params),
        "removeBatch" => handle_remove_batch(state, msg_id, params),
        "getMessageByMsgId" => handle_get_message_by_msgid(state, msg_id, params),
        "queryByDateRange" => handle_query_by_date_range(state, msg_id, params),
        "debugSample" => handle_debug_sample(state, msg_id),
        // Memory database handlers
        "memoryIndexBatch" => handle_memory_index_batch(state, msg_id, params),
        "memorySearch" => handle_memory_search(state, msg_id, params),
        "memoryStats" => handle_memory_stats(state, msg_id),
        "memoryClear" => handle_memory_clear(state, msg_id),
        "memoryRemoveBatch" => handle_memory_remove_batch(state, msg_id, params),
        "memoryDebugSample" => handle_memory_debug_sample(state, msg_id),
        "memoryRead" => handle_memory_read(state, msg_id, params),
        _ => Ok(serde_json::json!({ "id": msg_id, "error": format!("Unknown method: {method}") })),
    }
}

fn handle_hello(msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let addon_version = params.get("addonVersion").and_then(|v| v.as_str()).unwrap_or("unknown");
    log::info!(
        "Hello from addon version {}, responding with host version {}",
        addon_version,
        config::HOST_VERSION
    );

    // Auto-migrate to user-local if running from system install (non-blocking; parity with Python helper).
    let migrated = match self_update::auto_migrate_to_user_local() {
        Ok(v) => v,
        Err(e) => {
            log::error!("Auto-migration failed (non-fatal): {:?}", e);
            false
        }
    };

    let current_path = install_paths::current_exe_path();
    let user_dir = install_paths::get_user_install_dir().unwrap_or_else(|_| PathBuf::from("unknown"));
    let is_user_install = install_paths::is_relative_to(&current_path, &user_dir);
    let is_system_install = install_paths::is_in_system_install_dir(&current_path);
    let can_self_update = current_path.parent().map(|p| install_paths::can_write_dir(&p.to_path_buf())).unwrap_or(false);

    Ok(serde_json::json!({
        "id": msg_id,
        "result": {
            "type": "hello-response",
            "hostImpl": "rust",
            "hostVersion": config::HOST_VERSION,
            "installPath": current_path.to_string_lossy(),
            "isUserInstall": is_user_install,
            "isSystemInstall": is_system_install,
            "canSelfUpdate": can_self_update,
            "userLocalReady": migrated,
            "addonVersion": addon_version
        }
    }))
}

fn handle_update_check(msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let target_version = params.get("targetVersion").and_then(|v| v.as_str()).context("Missing required parameters: targetVersion")?;
    let (needs_update, can_update) = self_update::update_check(target_version)?;
    Ok(serde_json::json!({
        "id": msg_id,
        "result": {
            "currentVersion": config::HOST_VERSION,
            "targetVersion": target_version,
            "needsUpdate": needs_update,
            "canUpdate": can_update
        }
    }))
}

fn handle_update_request(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let target_version = params.get("targetVersion").and_then(|v| v.as_str()).context("Missing required parameters: targetVersion")?;
    let update_url = params.get("updateUrl").and_then(|v| v.as_str()).context("Missing required parameters: updateUrl")?;
    let sha256_hex = params.get("sha256").and_then(|v| v.as_str()).context("Missing required parameters: sha256")?;
    let platform = params.get("platform").and_then(|v| v.as_str()).context("Missing required parameters: platform")?;
    let signature = params.get("signature").and_then(|v| v.as_str()).context("Missing required parameters: signature")?;

    log::info!("Update request: {} → {}", config::HOST_VERSION, target_version);

    let result = self_update::update_request(self_update::UpdateParams {
        target_version,
        update_url,
        sha256_hex,
        platform,
        signature_base64: signature,
    })?;

    // TB expects disconnect after successful update so it can reconnect.
    if result.success {
        state.should_exit = true;
        log::info!("Update successful, exiting to allow restart with new version");
    }

    Ok(serde_json::json!({
        "id": msg_id,
        "result": {
            "success": result.success,
            "oldVersion": result.old_version,
            "newVersion": result.new_version,
            "installPath": result.install_path.to_string_lossy(),
            "requiresRestart": result.requires_restart,
            "message": result.message
        }
    }))
}

fn read_arg_value(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn handle_init(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    // Get addon ID (required for new storage location)
    let addon_id = params
        .get("addonId")
        .and_then(|v| v.as_str())
        .unwrap_or("thunderbird@tabmail.ai");

    // profilePath override (for testing): use the provided path directly, skip auto-detection
    let (tb_profile, new_fts_parent) = if let Some(override_path) = params.get("profilePath").and_then(|v| v.as_str()) {
        let p = PathBuf::from(override_path);
        log::info!("Using explicit profilePath: {}", p.display());
        std::fs::create_dir_all(&p)?;
        (p.clone(), p)
    } else {
        // Auto-detect Thunderbird profile
        let tb_profile = find_thunderbird_profile_dir()?;
        log::info!("Detected TB profile: {}", tb_profile.display());

        // Old location: <profile>/tabmail_fts/
        // New location: <profile>/browser-extension-data/<addon-id>/tabmail_fts/
        let old_fts_dir = tb_profile.join("tabmail_fts");
        let new_fts_parent = tb_profile.join("browser-extension-data").join(addon_id);
        let new_fts_dir = new_fts_parent.join("tabmail_fts");

        log::info!("FTS paths:");
        log::info!("  Old: {}", old_fts_dir.display());
        log::info!("  New: {}", new_fts_dir.display());

        // Check for migration from old to new location
        let migration_result = migrate_fts_data(&old_fts_dir, &new_fts_dir);
        if let Ok(true) = migration_result {
            log::info!("✅ Migrated FTS data from old location");
        }

        // Ensure new parent directory exists
        if let Err(e) = std::fs::create_dir_all(&new_fts_parent) {
            log::warn!("Could not create addon data dir: {}", e);
        }

        (tb_profile, new_fts_parent)
    };

    // Initialize email FTS DB
    let (db_path, conn) = open_or_create_db(&new_fts_parent)?;
    state.db_path = Some(db_path.clone());
    state.conn = Some(conn);

    let docs = {
        let conn = state.conn.as_ref().context("db connection missing after init")?;
        crate::fts::db::db_count(conn)?
    };

    // Initialize memory DB (separate database file, inside tabmail_fts/ subdir)
    let fts_subdir = new_fts_parent.join("tabmail_fts");
    std::fs::create_dir_all(&fts_subdir)?;
    let (memory_db_path, memory_conn) = memory_db::open_or_create_memory_db(&fts_subdir)?;
    state.memory_db_path = Some(memory_db_path.clone());
    state.memory_conn = Some(memory_conn);

    let memory_docs = {
        let conn = state.memory_conn.as_ref().context("memory db connection missing after init")?;
        memory_db::memory_db_count(conn)?
    };

    log::info!("Both databases initialized: {} email docs, {} memory entries", docs, memory_docs);

    // Initialize embedding engine (lazy model download on first init).
    // If download or load fails, we continue in FTS-only mode (graceful degradation).
    let has_embeddings = match crate::embeddings::download::ensure_model_files() {
        Ok(model_dir) => match crate::embeddings::engine::EmbeddingEngine::load(&model_dir) {
            Ok(engine) => {
                log::info!("Embedding engine loaded successfully");
                state.embedding_engine = Some(engine);
                true
            }
            Err(e) => {
                log::warn!("Failed to load embedding engine (FTS-only mode): {:?}", e);
                false
            }
        },
        Err(e) => {
            log::warn!("Failed to download model files (FTS-only mode): {:?}", e);
            false
        }
    };

    Ok(serde_json::json!({
        "id": msg_id,
        "result": {
            "ok": true,
            "dbPath": db_path.to_string_lossy(),
            "memoryDbPath": memory_db_path.to_string_lossy(),
            "persistent": true,
            "docs": docs,
            "memoryDocs": memory_docs,
            "vfs": "native",
            "tbProfile": tb_profile.to_string_lossy(),
            "addonDataDir": new_fts_parent.to_string_lossy(),
            "hasEmbeddings": has_embeddings
        }
    }))
}

/// Migrate FTS data from old TB profile location to new addon data directory.
/// Returns Ok(true) if migration was performed, Ok(false) if not needed.
fn migrate_fts_data(old_fts_dir: &Path, new_fts_dir: &Path) -> anyhow::Result<bool> {
    let old_db = old_fts_dir.join("fts.db");
    
    // No old data to migrate
    if !old_db.exists() {
        log::info!("No old FTS data found, skipping migration");
        return Ok(false);
    }

    let new_db = new_fts_dir.join("fts.db");
    
    // New location already has data - skip
    if new_db.exists() {
        log::info!("New location already has FTS data, skipping migration");
        // Clean up old data
        if let Err(e) = std::fs::remove_dir_all(old_fts_dir) {
            log::warn!("Could not remove old FTS directory: {}", e);
        }
        return Ok(false);
    }

    log::info!("Migrating FTS data from old to new location...");

    // Create new directory
    std::fs::create_dir_all(new_fts_dir)?;

    // Copy files
    let mut copied = 0;
    for entry in std::fs::read_dir(old_fts_dir)? {
        let entry = entry?;
        let src = entry.path();
        let dst = new_fts_dir.join(entry.file_name());
        
        if let Err(e) = std::fs::copy(&src, &dst) {
            log::error!("Failed to copy {:?}: {}", entry.file_name(), e);
        } else {
            copied += 1;
        }
    }

    log::info!("Copied {} files to new location", copied);

    // Remove old directory
    if let Err(e) = std::fs::remove_dir_all(old_fts_dir) {
        log::warn!("Could not remove old FTS directory: {}", e);
    }

    Ok(copied > 0)
}

fn find_thunderbird_profile_dir() -> anyhow::Result<PathBuf> {
    let system = std::env::consts::OS;
    let profiles_dir = match system {
        "macos" => home_dir()?.join("Library/Thunderbird/Profiles"),
        "linux" => home_dir()?.join(".thunderbird"),
        "windows" => {
            let appdata = std::env::var("APPDATA").unwrap_or_default();
            PathBuf::from(appdata).join("Thunderbird/Profiles")
        }
        _ => {
            log::warn!("Unknown OS: {}, using fallback", system);
            return Ok(home_dir()?.join(".tabmail"));
        }
    };

    if !profiles_dir.exists() {
        log::warn!("TB profiles directory not found: {}", profiles_dir.display());
        return Ok(home_dir()?.join(".tabmail"));
    }

    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&profiles_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter(|p| p.file_name().and_then(|s| s.to_str()).map(|s| !s.starts_with('.')).unwrap_or(false))
        .collect();

    if candidates.is_empty() {
        log::warn!("No profiles found in {}", profiles_dir.display());
        return Ok(home_dir()?.join(".tabmail"));
    }

    candidates.sort_by_key(|p| {
        std::fs::metadata(p)
            .and_then(|m| m.modified())
            .ok()
    });
    let most_recent = candidates.last().cloned().unwrap();
    log::info!("Found TB profile: {}", most_recent.display());
    Ok(most_recent)
}

fn home_dir() -> anyhow::Result<PathBuf> {
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
    bail!("Cannot determine home directory")
}

fn require_conn_mut<'a>(state: &'a mut DbState) -> anyhow::Result<&'a mut rusqlite::Connection> {
    state.conn.as_mut().context("Database not initialized. Call 'init' first.")
}

fn require_conn<'a>(state: &'a DbState) -> anyhow::Result<&'a rusqlite::Connection> {
    state.conn.as_ref().context("Database not initialized. Call 'init' first.")
}

fn handle_index_batch(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let rows = params.get("rows").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let engine = state.embedding_engine.as_ref();
    let conn = state.conn.as_mut().context("Database not initialized. Call 'init' first.")?;
    let (count, skipped) = crate::fts::db::index_batch(conn, &rows, engine)?;

    Ok(serde_json::json!({
        "id": msg_id,
        "result": { "ok": true, "count": count, "skippedDuplicates": skipped }
    }))
}

fn handle_search(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let q = params.get("q").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let engine = state.embedding_engine.as_ref();
    let conn = state.conn.as_ref().context("Database not initialized. Call 'init' first.")?;
    let results = crate::fts::db::search(conn, &q, params, &state.synonyms, engine)?;
    Ok(serde_json::json!({ "id": msg_id, "result": results }))
}

fn handle_stats(state: &mut DbState, msg_id: &str) -> anyhow::Result<Value> {
    let conn = require_conn(state)?;
    let docs = crate::fts::db::db_count(conn)?;
    let db_bytes = state
        .db_path
        .as_ref()
        .and_then(|p| std::fs::metadata(p).ok().map(|m| m.len() as i64))
        .unwrap_or(0);
    Ok(serde_json::json!({
        "id": msg_id,
        "result": { "ok": true, "docs": docs, "dbBytes": db_bytes }
    }))
}

fn handle_clear(state: &mut DbState, msg_id: &str) -> anyhow::Result<Value> {
    crate::fts::db::clear_rebuild(state)?;
    Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true } }))
}

fn handle_optimize(state: &mut DbState, msg_id: &str) -> anyhow::Result<Value> {
    let conn = require_conn(state)?;
    crate::fts::db::optimize(conn)?;
    Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true } }))
}

fn handle_filter_new_messages(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let rows = params.get("rows").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let conn = require_conn(state)?;
    let result = crate::fts::db::filter_new_messages(conn, &rows)?;
    Ok(serde_json::json!({ "id": msg_id, "result": result }))
}

fn handle_remove_batch(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let ids = params.get("ids").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let conn = require_conn_mut(state)?;
    let removed = crate::fts::db::remove_batch(conn, &ids)?;
    Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true, "count": removed } }))
}

fn handle_get_message_by_msgid(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let target = params.get("msgId").and_then(|v| v.as_str()).context("msgId parameter is required and must be a string")?;
    log::info!("Getting message by msgId: {}", target);
    let conn = require_conn(state)?;
    let res = crate::fts::db::get_message_by_msgid(conn, target)?;
    Ok(serde_json::json!({ "id": msg_id, "result": res }))
}

fn handle_query_by_date_range(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let from_v = params.get("from").context("from and to parameters are required")?;
    let to_v = params.get("to").context("from and to parameters are required")?;
    let limit = params.get("limit").and_then(|v| v.as_i64()).unwrap_or(config::sqlite::QUERY_BY_DATE_RANGE_DEFAULT_LIMIT);
    let conn = require_conn(state)?;
    let res = crate::fts::db::query_by_date_range(conn, from_v, to_v, limit)?;
    Ok(serde_json::json!({ "id": msg_id, "result": res }))
}

fn handle_debug_sample(state: &mut DbState, msg_id: &str) -> anyhow::Result<Value> {
    let conn = require_conn(state)?;
    let res = crate::fts::db::debug_sample(conn)?;
    Ok(serde_json::json!({ "id": msg_id, "result": res }))
}

// ============================================================================
// Memory database handlers
// ============================================================================

fn require_memory_conn_mut<'a>(state: &'a mut DbState) -> anyhow::Result<&'a mut rusqlite::Connection> {
    state.memory_conn.as_mut().context("Memory database not initialized. Call 'init' first.")
}

fn require_memory_conn<'a>(state: &'a DbState) -> anyhow::Result<&'a rusqlite::Connection> {
    state.memory_conn.as_ref().context("Memory database not initialized. Call 'init' first.")
}

fn handle_memory_index_batch(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let rows = params.get("rows").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let engine = state.embedding_engine.as_ref();
    let conn = state.memory_conn.as_mut().context("Memory database not initialized. Call 'init' first.")?;
    let (count, skipped) = memory_db::memory_index_batch(conn, &rows, engine)?;

    Ok(serde_json::json!({
        "id": msg_id,
        "result": { "ok": true, "count": count, "skippedDuplicates": skipped }
    }))
}

fn handle_memory_search(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let q = params.get("q").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let engine = state.embedding_engine.as_ref();
    let conn = state.memory_conn.as_ref().context("Memory database not initialized. Call 'init' first.")?;
    let results = memory_db::memory_search(conn, &q, params, &state.synonyms, engine)?;
    Ok(serde_json::json!({ "id": msg_id, "result": results }))
}

fn handle_memory_stats(state: &mut DbState, msg_id: &str) -> anyhow::Result<Value> {
    let conn = require_memory_conn(state)?;
    let docs = memory_db::memory_db_count(conn)?;
    let db_bytes = state
        .memory_db_path
        .as_ref()
        .and_then(|p| std::fs::metadata(p).ok().map(|m| m.len() as i64))
        .unwrap_or(0);
    Ok(serde_json::json!({
        "id": msg_id,
        "result": { "ok": true, "docs": docs, "dbBytes": db_bytes }
    }))
}

fn handle_memory_clear(state: &mut DbState, msg_id: &str) -> anyhow::Result<Value> {
    memory_db::memory_clear_rebuild(&mut state.memory_db_path, &mut state.memory_conn)?;
    Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true } }))
}

fn handle_memory_remove_batch(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let ids = params.get("ids").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let conn = require_memory_conn_mut(state)?;
    let removed = memory_db::memory_remove_batch(conn, &ids)?;
    Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true, "count": removed } }))
}

fn handle_memory_debug_sample(state: &mut DbState, msg_id: &str) -> anyhow::Result<Value> {
    let conn = require_memory_conn(state)?;
    let res = memory_db::memory_debug_sample(conn)?;
    Ok(serde_json::json!({ "id": msg_id, "result": res }))
}

fn handle_memory_read(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    // Default tolerance: 10 minutes (600,000 ms)
    const DEFAULT_TOLERANCE_MS: i64 = 600_000;

    let timestamp_ms = params
        .get("timestampMs")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let tolerance_ms = params
        .get("toleranceMs")
        .and_then(|v| v.as_i64())
        .unwrap_or(DEFAULT_TOLERANCE_MS);

    if timestamp_ms == 0 {
        return Ok(serde_json::json!({
            "id": msg_id,
            "error": "Missing or invalid timestampMs parameter"
        }));
    }

    let conn = require_memory_conn(state)?;
    let results = memory_db::memory_read_by_timestamp(conn, timestamp_ms, tolerance_ms)?;
    Ok(serde_json::json!({ "id": msg_id, "result": results }))
}