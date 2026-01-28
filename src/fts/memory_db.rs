// memory_db.rs - Separate memory database for chat history and learned facts
// This is stored in a separate file from the email FTS database so that:
// 1. Re-indexing emails doesn't wipe memory
// 2. No major version bump required for email FTS schema changes

use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::fts::query::build_fts_match;
use crate::fts::synonyms::SynonymLookup;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;

use crate::config;

/// Initialize the memory database schema
pub fn init_memory_database(conn: &Connection) -> anyhow::Result<()> {
    log::info!("Initializing memory database schema");

    // Apply same PRAGMA settings as main FTS database
    conn.execute_batch(&format!(
        "\
PRAGMA journal_mode = WAL;\n\
PRAGMA synchronous = NORMAL;\n\
PRAGMA temp_store = MEMORY;\n\
PRAGMA cache_size = {cache_size};\n\
PRAGMA mmap_size = {mmap_size};\n\
PRAGMA busy_timeout = {busy_timeout};\n\
PRAGMA wal_autocheckpoint = {wal_autocheckpoint};\n\
",
        cache_size = config::sqlite::PRAGMA_CACHE_SIZE_KIB_NEG,
        mmap_size = config::sqlite::PRAGMA_MMAP_SIZE_BYTES,
        busy_timeout = config::sqlite::PRAGMA_BUSY_TIMEOUT_MS,
        wal_autocheckpoint = config::sqlite::PRAGMA_WAL_AUTOCHECKPOINT_PAGES,
    ))?;

    // Memory schema: simpler than email (no cc, bcc, attachments, etc.)
    // Fields:
    // - memId: unique identifier (e.g., "chat:sessionId:turnIndex" or "kb:hash")
    // - role: "user" or "assistant" (or "kb" for knowledge base entries)
    // - content: the actual text content
    // - sessionId: chat session identifier (for grouping turns)
    conn.execute_batch(&format!(
        r#"
        CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
            memId,
            role,
            content,
            sessionId,
            tokenize = "{tokenize}",
            prefix = '{prefix}'
        );

        CREATE TABLE IF NOT EXISTS memory_meta (
            rowid INTEGER PRIMARY KEY,
            dateMs INTEGER NOT NULL,
            sessionId TEXT,
            turnIndex INTEGER
        );

        CREATE TABLE IF NOT EXISTS memory_ids (
            memId TEXT PRIMARY KEY
        );
        "#,
        tokenize = config::sqlite::FTS_TOKENIZE,
        prefix = config::sqlite::FTS_PREFIXES
    ))?;

    // FTS5 automerge settings
    conn.execute(
        "INSERT INTO memory_fts(memory_fts, rank) VALUES('automerge', 2)",
        [],
    )?;
    conn.execute(
        "INSERT INTO memory_fts(memory_fts, rank) VALUES('usermerge', 2)",
        [],
    )?;

    log::info!("Memory database schema initialized (3 tables: memory_fts, memory_meta, memory_ids)");
    Ok(())
}

/// Open or create the memory database
pub fn open_or_create_memory_db(fts_dir: &Path) -> anyhow::Result<(PathBuf, Connection)> {
    let db_path = fts_dir.join("memory.db");

    log::info!("Initializing memory database");
    log::info!("  Memory DB Path: {}", db_path.display());

    let conn = Connection::open(&db_path)
        .with_context(|| format!("open memory db {}", db_path.display()))?;
    
    // Verify FTS5 is available
    super::db::ensure_fts5_available(&conn)?;

    // Check if schema exists
    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='memory_fts'",
            [],
            |r| r.get(0),
        )
        .optional()?;

    if exists.is_none() {
        log::info!("Creating new memory database schema");
        init_memory_database(&conn)?;
    } else {
        log::info!("Using existing memory database schema");
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM memory_fts", [], |r| r.get(0))?;
    log::info!("Memory database initialized: {} entries indexed", count);

    Ok((db_path, conn))
}

/// Get count of entries in memory database
pub fn memory_db_count(conn: &Connection) -> anyhow::Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM memory_fts", [], |r| r.get(0))?)
}

/// Index a batch of memory entries
/// Each row should have: memId, role, content, sessionId, dateMs, turnIndex
pub fn memory_index_batch(conn: &mut Connection, rows: &[Value]) -> anyhow::Result<(i64, i64)> {
    log::info!("Indexing batch of {} memory entries", rows.len());

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let mut inserted: i64 = 0;
    let mut skipped_duplicates: i64 = 0;

    for row in rows {
        let Some(mem_id_val) = row.get("memId").and_then(|v| v.as_str()) else { continue };
        if mem_id_val.is_empty() {
            continue;
        }

        // Check for duplicates
        let changed = tx.execute(
            "INSERT OR IGNORE INTO memory_ids (memId) VALUES (?1)",
            params![mem_id_val],
        )?;
        if changed == 0 {
            skipped_duplicates += 1;
            log::debug!("Skipping duplicate memId: {}...", truncate_for_log(mem_id_val));
            continue;
        }

        let row_id: i64 = tx.query_row(
            "SELECT rowid FROM memory_ids WHERE memId = ?1",
            params![mem_id_val],
            |r| r.get(0),
        )?;

        let role = row.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let content = row.get("content").and_then(|v| v.as_str()).unwrap_or("");
        let session_id = row.get("sessionId").and_then(|v| v.as_str()).unwrap_or("");

        tx.execute(
            r#"
            INSERT INTO memory_fts (rowid, memId, role, content, sessionId)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![row_id, mem_id_val, role, content, session_id],
        )?;

        let date_ms = row.get("dateMs").and_then(|v| v.as_i64()).unwrap_or(0);
        let turn_index = row.get("turnIndex").and_then(|v| v.as_i64()).unwrap_or(0);

        tx.execute(
            r#"
            INSERT INTO memory_meta (rowid, dateMs, sessionId, turnIndex)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![row_id, date_ms, session_id, turn_index],
        )?;

        inserted += 1;
    }

    tx.commit()?;
    if skipped_duplicates > 0 {
        log::info!(
            "Indexed {} memory entries successfully, {} duplicates skipped",
            inserted,
            skipped_duplicates
        );
    } else {
        log::info!("Indexed {} memory entries successfully", inserted);
    }

    Ok((inserted, skipped_duplicates))
}

/// Search memory database (uses same FTS5/BM25/synonyms as email search)
pub fn memory_search(conn: &Connection, q: &str, params: &Value, synonyms: &SynonymLookup) -> anyhow::Result<Vec<Value>> {
    let query = q.trim();
    if query.is_empty() {
        return Ok(vec![]);
    }

    // Use the same query builder as email search (with synonyms enabled)
    let fts_query = build_fts_match(Some(query), true, synonyms);
    log::info!(
        "Memory query transformation (with synonyms): \"{}\" -> \"{}\"",
        query,
        fts_query
    );
    if fts_query.is_empty() {
        log::info!("Empty memory FTS query after normalization");
        return Ok(vec![]);
    }

    let ignore_date = params.get("ignoreDate").and_then(|v| v.as_bool()).unwrap_or(false);
    let limit = params
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(config::sqlite::SEARCH_DEFAULT_LIMIT);

    // BM25 weights: (memId, role, content, sessionId)
    // memId: 0.0 (not useful for ranking)
    // role: 1.0 (low weight)
    // content: 5.0 (main content, high weight - like email body)
    // sessionId: 0.0 (not useful for ranking)
    let mut sql = format!(
        r#"
        SELECT
            fts.memId, fts.role, fts.content, fts.sessionId, meta.dateMs,
            snippet(memory_fts, 2, '[', ']', '…', {snippet_tokens}) AS snippet,
            bm25(memory_fts, 0.0, 1.0, 5.0, 0.0) AS rank
        FROM memory_fts fts
        JOIN memory_meta meta ON fts.rowid = meta.rowid
        WHERE memory_fts MATCH ?1
        "#,
        snippet_tokens = config::sqlite::SEARCH_SNIPPET_TOKENS
    );

    let mut bind: Vec<rusqlite::types::Value> = vec![rusqlite::types::Value::from(fts_query.clone())];

    // Date range filtering (same as email search)
    if !ignore_date {
        if let Some(from_v) = params.get("from") {
            if let Some(ts) = super::db::parse_date_param(from_v)? {
                sql.push_str(" AND meta.dateMs >= ?");
                bind.push(rusqlite::types::Value::from(ts));
            }
        }
        if let Some(to_v) = params.get("to") {
            if let Some(ts) = super::db::parse_date_param(to_v)? {
                sql.push_str(" AND meta.dateMs <= ?");
                bind.push(rusqlite::types::Value::from(ts));
            }
        }
    }

    sql.push_str(" ORDER BY rank ASC, meta.dateMs DESC LIMIT ?");
    bind.push(rusqlite::types::Value::from(limit));

    log::info!("Memory search SQL: {}", sql);
    log::info!("Memory search params: {:?}", bind);

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(bind.iter()), |r| {
        let mem_id: String = r.get(0)?;
        let role: String = r.get(1)?;
        let content: String = r.get(2)?;
        let session_id: String = r.get(3)?;
        let date_ms: i64 = r.get(4)?;
        let snippet: String = r.get(5)?;
        let rank: f64 = r.get(6)?;

        Ok(serde_json::json!({
            "memId": mem_id,
            "role": role,
            "content": content,
            "sessionId": session_id,
            "dateMs": date_ms,
            "snippet": snippet,
            "rank": rank
        }))
    })?;

    let mut results: Vec<Value> = vec![];
    for r in rows {
        results.push(r?);
    }

    log::info!("Memory search completed: found {} results", results.len());
    Ok(results)
}

/// Remove entries from memory database by memId
pub fn memory_remove_batch(conn: &mut Connection, ids: &[Value]) -> anyhow::Result<i64> {
    if ids.is_empty() {
        return Ok(0);
    }

    let ids: Vec<String> = ids
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    log::info!("Removing {} entries from memory index", ids.len());

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut removed: i64 = 0;

    for mem_id_val in ids {
        if mem_id_val.is_empty() {
            continue;
        }
        let row_id: Option<i64> = tx
            .query_row(
                "SELECT rowid FROM memory_ids WHERE memId = ?1",
                params![mem_id_val],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(row_id) = row_id {
            tx.execute("DELETE FROM memory_fts WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM memory_meta WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM memory_ids WHERE memId = ?1", params![mem_id_val])?;
            removed += 1;
        }
    }

    tx.commit()?;
    log::info!("Removed {} memory entries", removed);
    Ok(removed)
}

/// Clear and rebuild memory database
pub fn memory_clear_rebuild(
    memory_db_path: &mut Option<PathBuf>,
    memory_conn: &mut Option<Connection>,
) -> anyhow::Result<()> {
    log::info!("Clearing memory database by deleting database file (rebuild from scratch)");
    let db_path = memory_db_path
        .clone()
        .context("Memory DB not initialized (missing db_path)")?;

    // Close connection first
    memory_conn.take();
    log::info!("Memory database connection closed");

    // Delete db + wal/shm
    delete_file_if_exists(&db_path)?;
    delete_file_if_exists(&PathBuf::from(format!("{}-wal", db_path.display())))?;
    delete_file_if_exists(&PathBuf::from(format!("{}-shm", db_path.display())))?;

    log::info!("Recreating memory database...");
    let conn = Connection::open(&db_path)?;
    super::db::ensure_fts5_available(&conn)?;
    init_memory_database(&conn)?;
    *memory_conn = Some(conn);
    log::info!("Memory database recreated and initialized successfully");

    Ok(())
}

/// Get debug sample from memory database
pub fn memory_debug_sample(conn: &Connection) -> anyhow::Result<Vec<Value>> {
    log::info!("Getting memory debug sample");
    let mut stmt = conn.prepare(
        r#"
        SELECT fts.memId, fts.role, fts.content, meta.dateMs
        FROM memory_fts fts
        JOIN memory_meta meta ON fts.rowid = meta.rowid
        ORDER BY meta.dateMs DESC
        LIMIT ?1
        "#,
    )?;

    let mut rows = stmt.query(params![config::sqlite::SEARCH_DEBUG_SAMPLE_LIMIT])?;
    let mut out: Vec<Value> = vec![];
    while let Some(r) = rows.next()? {
        let mem_id: String = r.get(0)?;
        let role: String = r.get(1)?;
        let content: String = r.get(2)?;
        let date_ms: i64 = r.get(3)?;
        out.push(serde_json::json!({
            "memId": mem_id,
            "role": role,
            "content": content,
            "dateMs": date_ms
        }));
    }
    Ok(out)
}

fn delete_file_if_exists(p: &Path) -> anyhow::Result<()> {
    if p.exists() {
        match std::fs::remove_file(p) {
            Ok(_) => log::info!("Deleted {}", p.display()),
            Err(e) => log::warn!("Failed to delete {}: {}", p.display(), e),
        }
    }
    Ok(())
}

fn truncate_for_log(s: &str) -> String {
    let max = 80usize;
    if s.len() <= max {
        return s.to_string();
    }
    s.chars().take(max).collect()
}
