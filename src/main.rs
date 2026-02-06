mod config;
mod embeddings;
mod fts;
mod install_paths;
mod logging;
mod native_messaging;
mod protocol;
mod self_update;
mod update_signature;

use std::io::{stdin, stdout, Stdin, Stdout};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use anyhow::{bail, Context};
use rusqlite::Connection;
use serde_json::Value;

use crate::embeddings::engine::EmbeddingEngine;
use crate::fts::db::{DbState, open_or_create_db};
use crate::fts::memory_db;
use crate::fts::synonyms::SynonymLookup;

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

    // ========================================================================
    // Phase A: Pre-init (single-threaded)
    // Handle hello, updateCheck, updateRequest, and init before spawning threads.
    // ========================================================================
    let mut message_count: u64 = 0;
    loop {
        let req = match native_messaging::read_message(&mut in_stream) {
            Ok(Some(r)) => r,
            Ok(None) => {
                log::info!("EOF during pre-init after {} messages, exiting", message_count);
                return Ok(());
            }
            Err(e) => {
                log::error!("Error reading message during pre-init: {:?}", e);
                return Ok(());
            }
        };

        message_count += 1;
        log::info!(
            "Pre-init message #{}: {} (id: {})",
            message_count,
            req.method,
            req.id
        );

        match req.method.as_str() {
            "hello" => {
                let resp = handle_hello(&req.id, &req.params)?;
                native_messaging::write_json(&mut out_stream, &resp)?;
            }
            "updateCheck" => {
                let resp = handle_update_check(&req.id, &req.params)?;
                native_messaging::write_json(&mut out_stream, &resp)?;
            }
            "updateRequest" => {
                let resp = handle_update_request(&req.id, &req.params)?;
                native_messaging::write_json(&mut out_stream, &resp)?;
                // updateRequest with success means process should exit for restart
                if resp.get("result").and_then(|r| r.get("success")).and_then(|v| v.as_bool()).unwrap_or(false) {
                    log::info!("Update successful, exiting to allow restart with new version");
                    return Ok(());
                }
            }
            "init" => {
                let resp = handle_init(&mut state, &req.id, &req.params)?;
                native_messaging::write_json(&mut out_stream, &resp)?;
                // init done — transition to Phase B (multi-threaded)
                break;
            }
            other => {
                let err = serde_json::json!({ "id": req.id, "error": format!("Must call 'init' first, got '{other}'") });
                native_messaging::write_json(&mut out_stream, &err)?;
            }
        }
    }

    // ========================================================================
    // Phase B: Multi-threaded operation
    // Reader thread (read-only ops) + Writer thread (write ops) + Main (stdin dispatch)
    // ========================================================================
    run_multi_threaded(state, in_stream, out_stream, message_count)
}

// ============================================================================
// Thread types and dispatch
// ============================================================================

/// Message sent from main thread to reader/writer threads.
struct ThreadMessage {
    method: String,
    id: String,
    params: Value,
}

enum MethodTarget {
    Reader,
    Writer,
    Unknown,
}

fn classify_method(method: &str) -> MethodTarget {
    match method {
        // Read-only email operations
        "search" | "stats" | "filterNewMessages" | "getMessageByMsgId"
        | "findByHeaderMessageId" | "queryByDateRange" | "debugSample" => MethodTarget::Reader,

        // Read-only memory operations
        "memorySearch" | "memoryStats" | "memoryDebugSample" | "memoryRead" => MethodTarget::Reader,

        // Write email operations
        "indexBatch" | "removeBatch" | "optimize" | "clear"
        | "rebuildEmbeddingsStart" | "rebuildEmbeddingsBatch" => MethodTarget::Writer,

        // Write memory operations
        "memoryIndexBatch" | "memoryRemoveBatch" | "memoryClear" => MethodTarget::Writer,

        _ => MethodTarget::Unknown,
    }
}

fn run_multi_threaded(
    state: DbState,
    mut in_stream: Stdin,
    out_stream: Stdout,
    mut message_count: u64,
) -> anyhow::Result<()> {
    // Extract resources from init state
    let email_db_path = state.db_path.context("email DB path missing after init")?;
    let memory_db_path = state.memory_db_path.context("memory DB path missing after init")?;
    let writer_email_conn = state.conn.context("email conn missing after init")?;
    let writer_memory_conn = state.memory_conn.context("memory conn missing after init")?;
    let engine: Option<Arc<EmbeddingEngine>> = state.embedding_engine.map(Arc::new);
    let synonyms = Arc::new(state.synonyms);

    // Open read-only connections for reader thread
    let reader_email_conn = crate::fts::db::open_read_only_connection(&email_db_path)?;
    let reader_memory_conn = memory_db::open_read_only_memory_connection(&memory_db_path)?;

    // Shared stdout for writing responses from both threads
    let shared_stdout: Arc<Mutex<Stdout>> = Arc::new(Mutex::new(out_stream));

    // AtomicBool flags: writer signals reader to reopen after clear/memoryClear
    let email_reopen = Arc::new(AtomicBool::new(false));
    let memory_reopen = Arc::new(AtomicBool::new(false));

    // Channels: main → reader, main → writer
    let (reader_tx, reader_rx) = mpsc::channel::<ThreadMessage>();
    let (writer_tx, writer_rx) = mpsc::channel::<ThreadMessage>();

    // Spawn reader thread
    let reader_handle = {
        let stdout = Arc::clone(&shared_stdout);
        let engine = engine.clone();
        let synonyms = Arc::clone(&synonyms);
        let email_path = email_db_path.clone();
        let memory_path = memory_db_path.clone();
        let email_reopen = Arc::clone(&email_reopen);
        let memory_reopen = Arc::clone(&memory_reopen);

        std::thread::Builder::new()
            .name("fts-reader".to_string())
            .spawn(move || {
                reader_thread_main(
                    reader_rx,
                    reader_email_conn,
                    reader_memory_conn,
                    engine,
                    synonyms,
                    stdout,
                    email_path,
                    memory_path,
                    email_reopen,
                    memory_reopen,
                );
            })?
    };

    // Spawn writer thread
    let writer_handle = {
        let stdout = Arc::clone(&shared_stdout);
        let engine = engine.clone();
        let email_path = email_db_path.clone();
        let memory_path = memory_db_path.clone();
        let email_reopen = Arc::clone(&email_reopen);
        let memory_reopen = Arc::clone(&memory_reopen);

        std::thread::Builder::new()
            .name("fts-writer".to_string())
            .spawn(move || {
                writer_thread_main(
                    writer_rx,
                    writer_email_conn,
                    writer_memory_conn,
                    engine,
                    stdout,
                    email_path,
                    memory_path,
                    email_reopen,
                    memory_reopen,
                );
            })?
    };

    log::info!("Multi-threaded mode active: reader + writer threads spawned");

    // Main thread: stdin dispatch loop
    loop {
        let req = match native_messaging::read_message(&mut in_stream) {
            Ok(Some(r)) => r,
            Ok(None) => {
                log::info!("EOF after {} messages, shutting down", message_count);
                break;
            }
            Err(e) => {
                log::error!("Error reading message: {:?}", e);
                break;
            }
        };

        message_count += 1;
        log::info!(
            "Dispatching message #{}: {} (id: {})",
            message_count,
            req.method,
            req.id
        );

        let msg = ThreadMessage {
            method: req.method.clone(),
            id: req.id.clone(),
            params: req.params,
        };

        match classify_method(&req.method) {
            MethodTarget::Reader => {
                if reader_tx.send(msg).is_err() {
                    log::error!("Reader thread channel closed");
                    break;
                }
            }
            MethodTarget::Writer => {
                if writer_tx.send(msg).is_err() {
                    log::error!("Writer thread channel closed");
                    break;
                }
            }
            MethodTarget::Unknown => {
                let err =
                    serde_json::json!({ "id": req.id, "error": format!("Unknown method: {}", req.method) });
                let mut out = shared_stdout.lock().unwrap();
                let _ = native_messaging::write_json(&mut *out, &err);
            }
        }
    }

    // Shutdown: drop senders so threads exit their recv() loops
    drop(reader_tx);
    drop(writer_tx);
    let _ = reader_handle.join();
    let _ = writer_handle.join();

    log::info!("=== TabMail FTS Helper Stopped ===");
    Ok(())
}

// ============================================================================
// Reader thread
// ============================================================================

fn reader_thread_main(
    rx: mpsc::Receiver<ThreadMessage>,
    mut email_conn: Connection,
    mut memory_conn: Connection,
    engine: Option<Arc<EmbeddingEngine>>,
    synonyms: Arc<SynonymLookup>,
    stdout: Arc<Mutex<Stdout>>,
    email_db_path: PathBuf,
    memory_db_path: PathBuf,
    email_reopen: Arc<AtomicBool>,
    memory_reopen: Arc<AtomicBool>,
) {
    log::info!("[reader] Thread started");

    while let Ok(msg) = rx.recv() {
        // Check if writer signaled us to reopen after clear
        if email_reopen.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            log::info!("[reader] Reopening email read-only connection after clear");
            match crate::fts::db::open_read_only_connection(&email_db_path) {
                Ok(new_conn) => email_conn = new_conn,
                Err(e) => log::error!("[reader] Failed to reopen email conn: {:?}", e),
            }
        }
        if memory_reopen.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            log::info!("[reader] Reopening memory read-only connection after clear");
            match memory_db::open_read_only_memory_connection(&memory_db_path) {
                Ok(new_conn) => memory_conn = new_conn,
                Err(e) => log::error!("[reader] Failed to reopen memory conn: {:?}", e),
            }
        }

        let engine_ref = engine.as_deref();
        let resp = handle_read_request(
            &email_conn,
            &memory_conn,
            &email_db_path,
            &memory_db_path,
            engine_ref,
            &synonyms,
            &msg.method,
            &msg.id,
            &msg.params,
        );

        write_response(&stdout, &msg.id, resp);
    }

    log::info!("[reader] Thread stopped (channel closed)");
}

fn handle_read_request(
    email_conn: &Connection,
    memory_conn: &Connection,
    email_db_path: &Path,
    memory_db_path: &Path,
    engine: Option<&EmbeddingEngine>,
    synonyms: &SynonymLookup,
    method: &str,
    msg_id: &str,
    params: &Value,
) -> anyhow::Result<Value> {
    match method {
        "search" => {
            let q = params
                .get("q")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let results = crate::fts::db::search(email_conn, &q, params, synonyms, engine)?;
            Ok(serde_json::json!({ "id": msg_id, "result": results }))
        }
        "stats" => {
            let docs = crate::fts::db::db_count(email_conn)?;
            let vec_docs = crate::fts::db::vec_count(email_conn);
            let db_bytes = std::fs::metadata(email_db_path)
                .ok()
                .map(|m| m.len() as i64)
                .unwrap_or(0);
            Ok(serde_json::json!({
                "id": msg_id,
                "result": { "ok": true, "docs": docs, "vecDocs": vec_docs, "dbBytes": db_bytes }
            }))
        }
        "filterNewMessages" => {
            let rows = params
                .get("rows")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let result = crate::fts::db::filter_new_messages(email_conn, &rows)?;
            Ok(serde_json::json!({ "id": msg_id, "result": result }))
        }
        "getMessageByMsgId" => {
            let target = params
                .get("msgId")
                .and_then(|v| v.as_str())
                .context("msgId parameter is required and must be a string")?;
            log::info!("Getting message by msgId: {}", target);
            let res = crate::fts::db::get_message_by_msgid(email_conn, target)?;
            Ok(serde_json::json!({ "id": msg_id, "result": res }))
        }
        "findByHeaderMessageId" => {
            let account_id = params
                .get("accountId")
                .and_then(|v| v.as_str())
                .context("accountId parameter is required")?;
            let header_message_id = params
                .get("headerMessageId")
                .and_then(|v| v.as_str())
                .context("headerMessageId parameter is required")?;
            log::info!("Finding by headerMessageId: {} (account={})", header_message_id, account_id);
            let res = crate::fts::db::find_by_header_message_id(email_conn, account_id, header_message_id)?;
            Ok(serde_json::json!({ "id": msg_id, "result": res }))
        }
        "queryByDateRange" => {
            let from_v = params.get("from").context("from and to parameters are required")?;
            let to_v = params.get("to").context("from and to parameters are required")?;
            let limit = params
                .get("limit")
                .and_then(|v| v.as_i64())
                .unwrap_or(config::sqlite::QUERY_BY_DATE_RANGE_DEFAULT_LIMIT);
            let res = crate::fts::db::query_by_date_range(email_conn, from_v, to_v, limit)?;
            Ok(serde_json::json!({ "id": msg_id, "result": res }))
        }
        "debugSample" => {
            let res = crate::fts::db::debug_sample(email_conn)?;
            Ok(serde_json::json!({ "id": msg_id, "result": res }))
        }
        "memorySearch" => {
            let q = params
                .get("q")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let results = memory_db::memory_search(memory_conn, &q, params, synonyms, engine)?;
            Ok(serde_json::json!({ "id": msg_id, "result": results }))
        }
        "memoryStats" => {
            let docs = memory_db::memory_db_count(memory_conn)?;
            let vec_docs = memory_db::memory_vec_count(memory_conn);
            let db_bytes = std::fs::metadata(memory_db_path)
                .ok()
                .map(|m| m.len() as i64)
                .unwrap_or(0);
            Ok(serde_json::json!({
                "id": msg_id,
                "result": { "ok": true, "docs": docs, "vecDocs": vec_docs, "dbBytes": db_bytes }
            }))
        }
        "memoryDebugSample" => {
            let res = memory_db::memory_debug_sample(memory_conn)?;
            Ok(serde_json::json!({ "id": msg_id, "result": res }))
        }
        "memoryRead" => {
            const DEFAULT_TOLERANCE_MS: i64 = 600_000;
            let timestamp_ms = params.get("timestampMs").and_then(|v| v.as_i64()).unwrap_or(0);
            let tolerance_ms = params
                .get("toleranceMs")
                .and_then(|v| v.as_i64())
                .unwrap_or(DEFAULT_TOLERANCE_MS);
            if timestamp_ms == 0 {
                return Ok(
                    serde_json::json!({ "id": msg_id, "error": "Missing or invalid timestampMs parameter" }),
                );
            }
            let results = memory_db::memory_read_by_timestamp(memory_conn, timestamp_ms, tolerance_ms)?;
            Ok(serde_json::json!({ "id": msg_id, "result": results }))
        }
        _ => Ok(serde_json::json!({ "id": msg_id, "error": format!("Unknown reader method: {method}") })),
    }
}

// ============================================================================
// Writer thread
// ============================================================================

fn writer_thread_main(
    rx: mpsc::Receiver<ThreadMessage>,
    mut email_conn: Connection,
    mut memory_conn: Connection,
    engine: Option<Arc<EmbeddingEngine>>,
    stdout: Arc<Mutex<Stdout>>,
    email_db_path: PathBuf,
    memory_db_path: PathBuf,
    email_reopen: Arc<AtomicBool>,
    memory_reopen: Arc<AtomicBool>,
) {
    log::info!("[writer] Thread started");

    while let Ok(msg) = rx.recv() {
        let engine_ref = engine.as_deref();
        let resp = handle_write_request(
            &mut email_conn,
            &mut memory_conn,
            &email_db_path,
            &memory_db_path,
            engine_ref,
            &email_reopen,
            &memory_reopen,
            &msg.method,
            &msg.id,
            &msg.params,
        );

        write_response(&stdout, &msg.id, resp);
    }

    log::info!("[writer] Thread stopped (channel closed)");
}

fn handle_write_request(
    email_conn: &mut Connection,
    memory_conn: &mut Connection,
    email_db_path: &Path,
    memory_db_path: &Path,
    engine: Option<&EmbeddingEngine>,
    email_reopen: &AtomicBool,
    memory_reopen: &AtomicBool,
    method: &str,
    msg_id: &str,
    params: &Value,
) -> anyhow::Result<Value> {
    match method {
        "indexBatch" => {
            let rows = params
                .get("rows")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let (count, skipped) = crate::fts::db::index_batch(email_conn, &rows, engine)?;
            Ok(serde_json::json!({
                "id": msg_id,
                "result": { "ok": true, "count": count, "skippedDuplicates": skipped }
            }))
        }
        "removeBatch" => {
            let ids = params
                .get("ids")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let removed = crate::fts::db::remove_batch(email_conn, &ids)?;
            Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true, "count": removed } }))
        }
        "optimize" => {
            crate::fts::db::optimize(email_conn)?;
            Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true } }))
        }
        "clear" => {
            // Swap connection with a temporary in-memory one, clear + rebuild, swap back
            let old_conn = std::mem::replace(email_conn, Connection::open_in_memory()?);
            let new_conn = crate::fts::db::clear_rebuild_standalone(email_db_path, old_conn)?;
            *email_conn = new_conn;
            // Signal reader to reopen its read-only connection
            email_reopen.store(true, Ordering::SeqCst);
            Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true } }))
        }
        "rebuildEmbeddingsStart" => {
            engine.context("Embedding engine not available — cannot rebuild embeddings")?;
            let email_total = crate::fts::db::rebuild_embeddings_start(email_conn)?;
            let memory_total = memory_db::rebuild_memory_embeddings_start(memory_conn)?;
            Ok(serde_json::json!({
                "id": msg_id,
                "result": { "ok": true, "emailTotal": email_total, "memoryTotal": memory_total }
            }))
        }
        "rebuildEmbeddingsBatch" => {
            let target = params.get("target").and_then(|v| v.as_str()).unwrap_or("email");
            let last_rowid = params.get("lastRowid").and_then(|v| v.as_i64()).unwrap_or(0);
            let batch_size = params.get("batchSize").and_then(|v| v.as_i64()).unwrap_or(500);
            let eng = engine.context("Embedding engine not available — cannot rebuild embeddings")?;
            let (new_last, processed, embedded, done) = match target {
                "memory" => {
                    memory_db::rebuild_memory_embeddings_batch(memory_conn, eng, last_rowid, batch_size)?
                }
                _ => crate::fts::db::rebuild_embeddings_batch(email_conn, eng, last_rowid, batch_size)?,
            };
            Ok(serde_json::json!({
                "id": msg_id,
                "result": {
                    "ok": true, "target": target,
                    "lastRowid": new_last, "processed": processed,
                    "embedded": embedded, "done": done
                }
            }))
        }
        "memoryIndexBatch" => {
            let rows = params
                .get("rows")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let (count, skipped) = memory_db::memory_index_batch(memory_conn, &rows, engine)?;
            Ok(serde_json::json!({
                "id": msg_id,
                "result": { "ok": true, "count": count, "skippedDuplicates": skipped }
            }))
        }
        "memoryRemoveBatch" => {
            let ids = params
                .get("ids")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let removed = memory_db::memory_remove_batch(memory_conn, &ids)?;
            Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true, "count": removed } }))
        }
        "memoryClear" => {
            let old_conn = std::mem::replace(memory_conn, Connection::open_in_memory()?);
            let new_conn = memory_db::memory_clear_rebuild_standalone(memory_db_path, old_conn)?;
            *memory_conn = new_conn;
            memory_reopen.store(true, Ordering::SeqCst);
            Ok(serde_json::json!({ "id": msg_id, "result": { "ok": true } }))
        }
        _ => Ok(serde_json::json!({ "id": msg_id, "error": format!("Unknown writer method: {method}") })),
    }
}

// ============================================================================
// Shared helpers
// ============================================================================

fn write_response(stdout: &Arc<Mutex<Stdout>>, msg_id: &str, result: anyhow::Result<Value>) {
    let value = match result {
        Ok(v) => v,
        Err(e) => {
            log::error!("Handler error for {}: {:?}", msg_id, e);
            serde_json::json!({ "id": msg_id, "error": format!("{e}") })
        }
    };

    let mut out = stdout.lock().unwrap();
    if let Err(e) = native_messaging::write_json(&mut *out, &value) {
        log::error!("Error writing response for {}: {:?}", msg_id, e);
    }
}

// ============================================================================
// Pre-init handlers (run on main thread before spawning reader/writer)
// ============================================================================

fn handle_hello(msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let addon_version = params
        .get("addonVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
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
    let user_dir =
        install_paths::get_user_install_dir().unwrap_or_else(|_| PathBuf::from("unknown"));
    let is_user_install = install_paths::is_relative_to(&current_path, &user_dir);
    let is_system_install = install_paths::is_in_system_install_dir(&current_path);
    let can_self_update = current_path
        .parent()
        .map(|p| install_paths::can_write_dir(&p.to_path_buf()))
        .unwrap_or(false);

    Ok(serde_json::json!({
        "id": msg_id,
        "result": {
            "type": "hello-response",
            "hostImpl": "rust",
            "hostVersion": config::HOST_VERSION,
            "schemaVersion": config::SCHEMA_VERSION,
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
    let target_version = params
        .get("targetVersion")
        .and_then(|v| v.as_str())
        .context("Missing required parameters: targetVersion")?;
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

fn handle_update_request(msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    let target_version = params
        .get("targetVersion")
        .and_then(|v| v.as_str())
        .context("Missing required parameters: targetVersion")?;
    let update_url = params
        .get("updateUrl")
        .and_then(|v| v.as_str())
        .context("Missing required parameters: updateUrl")?;
    let sha256_hex = params
        .get("sha256")
        .and_then(|v| v.as_str())
        .context("Missing required parameters: sha256")?;
    let platform = params
        .get("platform")
        .and_then(|v| v.as_str())
        .context("Missing required parameters: platform")?;
    let signature = params
        .get("signature")
        .and_then(|v| v.as_str())
        .context("Missing required parameters: signature")?;

    log::info!("Update request: {} → {}", config::HOST_VERSION, target_version);

    let result = self_update::update_request(self_update::UpdateParams {
        target_version,
        update_url,
        sha256_hex,
        platform,
        signature_base64: signature,
    })?;

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

fn handle_init(state: &mut DbState, msg_id: &str, params: &Value) -> anyhow::Result<Value> {
    // Get addon ID (required for new storage location)
    let addon_id = params
        .get("addonId")
        .and_then(|v| v.as_str())
        .unwrap_or("thunderbird@tabmail.ai");

    // profilePath override (for testing): use the provided path directly, skip auto-detection
    let (tb_profile, new_fts_parent) =
        if let Some(override_path) = params.get("profilePath").and_then(|v| v.as_str()) {
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
        let conn = state
            .conn
            .as_ref()
            .context("db connection missing after init")?;
        crate::fts::db::db_count(conn)?
    };

    // Initialize memory DB (separate database file, inside tabmail_fts/ subdir)
    let fts_subdir = new_fts_parent.join("tabmail_fts");
    std::fs::create_dir_all(&fts_subdir)?;
    let (memory_db_path, memory_conn) = memory_db::open_or_create_memory_db(&fts_subdir)?;
    state.memory_db_path = Some(memory_db_path.clone());
    state.memory_conn = Some(memory_conn);

    let memory_docs = {
        let conn = state
            .memory_conn
            .as_ref()
            .context("memory db connection missing after init")?;
        memory_db::memory_db_count(conn)?
    };

    log::info!(
        "Both databases initialized: {} email docs, {} memory entries",
        docs,
        memory_docs
    );

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

// ============================================================================
// Utility functions
// ============================================================================

fn read_arg_value(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
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
        log::warn!(
            "TB profiles directory not found: {}",
            profiles_dir.display()
        );
        return Ok(home_dir()?.join(".tabmail"));
    }

    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&profiles_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|s| !s.starts_with('.'))
                .unwrap_or(false)
        })
        .collect();

    if candidates.is_empty() {
        log::warn!("No profiles found in {}", profiles_dir.display());
        return Ok(home_dir()?.join(".tabmail"));
    }

    candidates.sort_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok());
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
