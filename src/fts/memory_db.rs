/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// memory_db.rs - Separate memory database for chat history and learned facts
// This is stored in a separate file from the email FTS database so that:
// 1. Re-indexing emails doesn't wipe memory
// 2. No major version bump required for email FTS schema changes

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::embeddings::engine::EmbeddingEngine;
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

    // Vector tables for semantic search (sqlite-vec).
    // memory_vec rowids match memory_fts rowids for joining.
    conn.execute_batch(&format!(
        r#"
        CREATE VIRTUAL TABLE IF NOT EXISTS memory_vec USING vec0(
            embedding FLOAT[{dims}] distance_metric=cosine
        );

        CREATE TABLE IF NOT EXISTS embed_cache (
            content_hash TEXT PRIMARY KEY,
            embedding BLOB NOT NULL,
            model TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        "#,
        dims = config::embedding::EMBEDDING_DIMS,
    ))?;

    log::info!("Memory database schema initialized (5 tables: memory_fts, memory_meta, memory_ids, memory_vec, embed_cache)");
    Ok(())
}

/// Open or create the memory database
/// In-place tokenizer migration for memory_fts (mirrors db.rs
/// rebuild_next_stale_shard — see ADR-024). Stale = the stored CREATE
/// statement still carries `tokenchars`. The FTS5 table stores its content, so
/// a rowid-preserving copy re-tokenizes locally; memory_meta / memory_vec
/// rowid alignment is untouched. memory.db is small (chat turns) — one
/// transaction, no time budget needed. NO schema-version involvement.
fn rebuild_memory_fts_if_stale_tokenizer(conn: &mut Connection) -> anyhow::Result<()> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='memory_fts'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let Some(create_sql) = sql else { return Ok(()) };
    if !create_sql.contains("tokenchars") {
        return Ok(());
    }
    log::info!("Memory tokenizer migration: rebuilding memory_fts in place");
    let tx = conn.transaction()?;
    tx.execute_batch("DROP TABLE IF EXISTS memory_fts_retok")?;
    tx.execute_batch(&format!(
        r#"CREATE VIRTUAL TABLE memory_fts_retok USING fts5(
            memId,
            role,
            content,
            sessionId,
            tokenize = "{tokenize}",
            prefix = '{prefix}'
        )"#,
        tokenize = config::sqlite::FTS_TOKENIZE,
        prefix = config::sqlite::FTS_PREFIXES,
    ))?;
    // rowid-preserving copy; SQLite streams rows internally
    tx.execute_batch(
        "INSERT INTO memory_fts_retok(rowid, memId, role, content, sessionId)
         SELECT rowid, memId, role, content, sessionId FROM memory_fts",
    )?;
    tx.execute_batch("DROP TABLE memory_fts")?;
    tx.execute_batch("ALTER TABLE memory_fts_retok RENAME TO memory_fts")?;
    // Re-apply the write tuning init_memory_database sets on creation
    tx.execute(
        "INSERT INTO memory_fts(memory_fts, rank) VALUES('automerge', 2)",
        [],
    )?;
    tx.execute(
        "INSERT INTO memory_fts(memory_fts, rank) VALUES('usermerge', 2)",
        [],
    )?;
    tx.commit()?;
    log::info!("Memory tokenizer migration complete");
    Ok(())
}

pub fn open_or_create_memory_db(fts_dir: &Path) -> anyhow::Result<(PathBuf, Connection)> {
    let db_path = fts_dir.join("memory.db");

    log::info!("Initializing memory database");
    log::info!("  Memory DB Path: {}", db_path.display());

    let mut conn = Connection::open(&db_path)
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
        // Migrate: add vector tables if missing (pre-v0.7.0 databases)
        ensure_memory_vector_tables(&conn)?;
        // In-place tokenizer migration (ADR-024) — same lockstep change as the
        // email shards; without this, memory search would tokenize differently
        // from email search on pre-existing profiles.
        rebuild_memory_fts_if_stale_tokenizer(&mut conn)?;
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM memory_fts", [], |r| r.get(0))?;
    log::info!("Memory database initialized: {} entries indexed", count);

    Ok((db_path, conn))
}

/// Add vector tables to an existing memory database (migration for pre-v0.7.0 databases).
/// Also handles migration from L2 to cosine distance metric (v0.7.0-dev → v0.7.0).
fn ensure_memory_vector_tables(conn: &Connection) -> anyhow::Result<()> {
    let vec_exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='memory_vec'",
            [],
            |r| r.get(0),
        )
        .optional()?;

    if vec_exists.is_none() {
        log::info!("Migrating memory DB: adding vector tables (memory_vec, embed_cache)");
        conn.execute_batch(&format!(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_vec USING vec0(
                embedding FLOAT[{dims}] distance_metric=cosine
            );
            CREATE TABLE IF NOT EXISTS embed_cache (
                content_hash TEXT PRIMARY KEY,
                embedding BLOB NOT NULL,
                model TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            "#,
            dims = config::embedding::EMBEDDING_DIMS,
        ))?;
        log::info!("Vector tables added to memory database");
    } else {
        // Check if existing vec0 table uses cosine distance (v0.7.0-dev had L2 by mistake).
        let needs_cosine_migration = super::db::needs_vec_cosine_migration(conn, "memory_vec")?;
        if needs_cosine_migration {
            log::info!("Migrating memory_vec: L2 → cosine distance metric (dropping and recreating)");
            conn.execute_batch(&format!(
                r#"
                DROP TABLE IF EXISTS memory_vec;
                CREATE VIRTUAL TABLE memory_vec USING vec0(
                    embedding FLOAT[{dims}] distance_metric=cosine
                );
                "#,
                dims = config::embedding::EMBEDDING_DIMS,
            ))?;
            // Clear embed_cache so embeddings get regenerated on next memoryIndexBatch
            conn.execute("DELETE FROM embed_cache", []).ok(); // ok() in case embed_cache doesn't exist
            log::info!("memory_vec recreated with cosine distance. Embeddings will regenerate on next memoryIndexBatch.");
        }
    }

    Ok(())
}

/// Open a read-only connection to an existing memory database.
/// Used by the reader thread in multi-threaded mode.
pub fn open_read_only_memory_connection(db_path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open read-only memory db {}", db_path.display()))?;

    conn.execute_batch(&format!(
        "\
PRAGMA cache_size = {cache_size};\n\
PRAGMA mmap_size = {mmap_size};\n\
PRAGMA busy_timeout = {busy_timeout};\n\
",
        cache_size = config::sqlite::PRAGMA_CACHE_SIZE_KIB_NEG,
        mmap_size = config::sqlite::PRAGMA_MMAP_SIZE_BYTES,
        busy_timeout = config::sqlite::PRAGMA_BUSY_TIMEOUT_MS,
    ))?;

    log::info!("Opened read-only memory connection to {}", db_path.display());
    Ok(conn)
}

/// Get count of entries in memory database
pub fn memory_db_count(conn: &Connection) -> anyhow::Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM memory_fts", [], |r| r.get(0))?)
}

/// Count rows in the memory vector embedding table (0 if table missing or query fails).
pub fn memory_vec_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM memory_vec", [], |r| r.get(0)).unwrap_or(0)
}

/// Index a batch of memory entries
/// Each row should have: memId, role, content, sessionId, dateMs, turnIndex
pub fn memory_index_batch(conn: &mut Connection, rows: &[Value], engine: Option<&EmbeddingEngine>) -> anyhow::Result<(i64, i64)> {
    log::info!("Indexing batch of {} memory entries (embeddings={})", rows.len(), engine.is_some());

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let mut inserted: i64 = 0;
    let mut skipped_duplicates: i64 = 0;
    let mut embedded: i64 = 0;

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

        // Generate and store embedding if engine is available
        if let Some(engine) = engine {
            let embed_text = crate::embeddings::text_prep::prepare_memory_text(role, content);
            match engine.embed(&embed_text) {
                Ok(embedding) => {
                    let blob = super::db::f32_vec_to_blob(&embedding);
                    tx.execute(
                        "INSERT INTO memory_vec (rowid, embedding) VALUES (?1, ?2)",
                        params![row_id, blob],
                    )?;
                    embedded += 1;
                }
                Err(e) => {
                    log::warn!("Failed to embed memory {}: {}", truncate_for_log(mem_id_val), e);
                }
            }
        }

        inserted += 1;
    }

    tx.commit()?;
    if engine.is_some() {
        log::info!(
            "Indexed {} memory entries ({} embedded), {} duplicates skipped",
            inserted, embedded, skipped_duplicates
        );
    } else if skipped_duplicates > 0 {
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

// Internal struct for memory FTS candidate data during hybrid merge.
struct MemoryFtsCandidate {
    rowid: i64,
    mem_id: String,
    role: String,
    content: String,
    session_id: String,
    date_ms: i64,
    snippet: String,
    rank: f64,
}

// Lightweight metadata for vector-only memory results.
struct MemoryMeta {
    mem_id: String,
    role: String,
    content: String,
    session_id: String,
    date_ms: i64,
}

/// Search memory database (uses same FTS5/BM25/synonyms as email search)
/// If query is empty, returns all entries sorted by date (for browsing)
///
/// With `previewChars` (a positive integer), each result's `content` and `snippet` come back cut to
/// that many characters (code points): a caller that shows only the start of each exchange keeps its reply small
/// whatever the rows hold (Thunderbird refuses a native message over 1 MiB, and memory rows are kept
/// in full). The stored rows are unchanged; without it every result is whole, as before.
pub fn memory_search(
    conn: &Connection,
    q: &str,
    params: &Value,
    synonyms: &SynonymLookup,
    engine: Option<&EmbeddingEngine>,
) -> anyhow::Result<Vec<Value>> {
    // One read transaction, so the search's queries all read the memory as it was when it began: a
    // write landing between them (an exchange removed and its rowid reused) can't put another
    // exchange, or one outside the dates, in its results.
    let tx = conn.unchecked_transaction()?;
    let mut results = memory_search_rows(&tx, q, params, synonyms, engine)?;
    drop(tx);
    let preview_chars = params.get("previewChars").and_then(|v| v.as_u64()).filter(|&n| n > 0);
    if let Some(max) = preview_chars {
        let max = usize::try_from(max).unwrap_or(usize::MAX);
        for result in &mut results {
            for key in ["content", "snippet"] {
                if let Some(Value::String(text)) = result.get_mut(key) {
                    if let Some((end, _)) = text.char_indices().nth(max) {
                        text.truncate(end);
                    }
                }
            }
        }
    }
    Ok(results)
}

fn memory_search_rows(
    conn: &Connection,
    q: &str,
    params: &Value,
    synonyms: &SynonymLookup,
    engine: Option<&EmbeddingEngine>,
) -> anyhow::Result<Vec<Value>> {
    let query = q.trim();
    let ignore_date = params.get("ignoreDate").and_then(|v| v.as_bool()).unwrap_or(false);
    let limit = params
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(config::sqlite::SEARCH_DEFAULT_LIMIT);

    // Empty query = list all by date (for browsing mode)
    if query.is_empty() {
        return memory_list_all(conn, params, ignore_date, limit);
    }
    let limit = super::db::cap_search_limit(limit);

    // Fall back to FTS-only when no embedding engine
    let engine = match engine {
        Some(e) => e,
        None => return memory_search_fts_only(conn, query, params, synonyms, ignore_date, limit),
    };

    let from_ts = if !ignore_date {
        params.get("from").and_then(|v| super::db::parse_date_param(v).ok().flatten())
    } else {
        None
    };
    let to_ts = if !ignore_date {
        params.get("to").and_then(|v| super::db::parse_date_param(v).ok().flatten())
    } else {
        None
    };

    let candidate_limit = limit * config::hybrid::CANDIDATE_MULTIPLIER;

    // --- FTS5 candidates ---
    let fts_query = build_fts_match(Some(query), true, synonyms);
    log::info!(
        "Memory hybrid search: \"{}\" -> FTS \"{}\"",
        query,
        fts_query
    );
    let fts_candidates = if !fts_query.is_empty() {
        memory_search_fts_candidates(conn, &fts_query, from_ts, to_ts, candidate_limit)?
    } else {
        vec![]
    };

    // --- Vector candidates ---
    let query_embedding = engine.embed(query)?;
    let query_blob = super::db::f32_vec_to_blob(&query_embedding);
    let vec_candidates = super::db::search_vec_candidates(conn, "memory_vec", &query_blob, candidate_limit).unwrap_or_else(|e| {
        log::warn!("Memory vector search failed, searching by keyword only: {e}");
        vec![]
    });

    // Keep only the vector candidates within the dates asked, before the merge keeps the best
    // `limit` (the FTS candidates are filtered by date in their query).
    let vec_candidates = super::db::vec_candidates_within_dates(conn, "memory_meta", vec_candidates, from_ts, to_ts)?;

    // Fall back to FTS-only when no vector candidates are left: the vec table is empty (e.g., during
    // embedding rebuild), or none of them is within the dates.
    if vec_candidates.is_empty() {
        log::info!("No memory vector candidates (vec table may be empty, or none within the dates), falling back to FTS-only search");
        return memory_search_fts_only(conn, query, params, synonyms, ignore_date, limit);
    }

    // --- Merge ---
    let text_pairs: Vec<(i64, f64)> = fts_candidates.iter().map(|c| (c.rowid, c.rank)).collect();
    let merged = crate::fts::hybrid::merge_results(
        &text_pairs,
        &vec_candidates,
        config::hybrid::MEMORY_VECTOR_WEIGHT,
        config::hybrid::MEMORY_TEXT_WEIGHT,
        limit as usize,
    );

    // --- Assemble results ---
    let mut fts_map: HashMap<i64, MemoryFtsCandidate> =
        fts_candidates.into_iter().map(|c| (c.rowid, c)).collect();
    let mut results = Vec::with_capacity(merged.len());

    for hr in &merged {
        if let Some(fts_c) = fts_map.remove(&hr.rowid) {
            results.push(serde_json::json!({
                "memId": fts_c.mem_id,
                "role": fts_c.role,
                "content": fts_c.content,
                "sessionId": fts_c.session_id,
                "dateMs": fts_c.date_ms,
                "snippet": fts_c.snippet,
                "rank": -hr.final_score
            }));
        } else {
            // Vector-only result (within the dates: filtered before the merge)
            if let Some(meta) = fetch_memory_meta(conn, hr.rowid)? {
                results.push(serde_json::json!({
                    "memId": meta.mem_id,
                    "role": meta.role,
                    "content": meta.content,
                    "sessionId": meta.session_id,
                    "dateMs": meta.date_ms,
                    "snippet": "",
                    "rank": -hr.final_score
                }));
            }
        }
    }

    log::info!(
        "Memory hybrid search completed: {} results (FTS cands: {}, Vec cands: {})",
        results.len(),
        text_pairs.len(),
        vec_candidates.len()
    );
    Ok(results)
}

/// List all memory entries by date (empty query browsing mode).
fn memory_list_all(
    conn: &Connection,
    params: &Value,
    ignore_date: bool,
    limit: i64,
) -> anyhow::Result<Vec<Value>> {
    log::info!("Memory search with empty query - listing all by date (limit={})", limit);

    // The page's rows are chosen from memory_meta (dates only) before any content is read: sorting
    // the join itself read every row's content, the whole memory, for each request. CROSS JOIN keeps
    // that order: with a plain JOIN, SQLite scans memory_fts and looks each row up in the page.
    let mut sql = r#"
        SELECT rowid, dateMs FROM memory_meta
        WHERE 1=1
    "#
    .to_string();

    let mut bind: Vec<rusqlite::types::Value> = vec![];

    if !ignore_date {
        if let Some(from_v) = params.get("from") {
            if let Some(ts) = super::db::parse_date_param(from_v)? {
                sql.push_str(" AND dateMs >= ?");
                bind.push(rusqlite::types::Value::from(ts));
            }
        }
        if let Some(to_v) = params.get("to") {
            if let Some(ts) = super::db::parse_date_param(to_v)? {
                sql.push_str(" AND dateMs <= ?");
                bind.push(rusqlite::types::Value::from(ts));
            }
        }
    }

    sql.push_str(" ORDER BY dateMs DESC LIMIT ?");
    bind.push(rusqlite::types::Value::from(limit));
    let sql = format!(
        r#"
        SELECT fts.memId, fts.role, fts.content, fts.sessionId, meta.dateMs
        FROM ({sql}) meta
        CROSS JOIN memory_fts fts ON fts.rowid = meta.rowid
        ORDER BY meta.dateMs DESC
        "#
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(bind.iter()), |r| {
        let mem_id: String = r.get(0)?;
        let role: String = r.get(1)?;
        let content: String = r.get(2)?;
        let session_id: String = r.get(3)?;
        let date_ms: i64 = r.get(4)?;

        Ok(serde_json::json!({
            "memId": mem_id,
            "role": role,
            "content": content,
            "sessionId": session_id,
            "dateMs": date_ms,
            "snippet": null,
            "rank": 0.0
        }))
    })?;

    let mut results: Vec<Value> = vec![];
    for r in rows {
        results.push(r?);
    }
    log::info!("Memory list completed: found {} results", results.len());
    Ok(results)
}

/// Original FTS-only memory search (used when embedding engine is not available).
fn memory_search_fts_only(
    conn: &Connection,
    query: &str,
    params: &Value,
    synonyms: &SynonymLookup,
    ignore_date: bool,
    limit: i64,
) -> anyhow::Result<Vec<Value>> {
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

    let mut bind: Vec<rusqlite::types::Value> =
        vec![rusqlite::types::Value::from(fts_query.clone())];

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

/// Get FTS5 candidates with full metadata for memory hybrid merge.
fn memory_search_fts_candidates(
    conn: &Connection,
    fts_query: &str,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
    limit: i64,
) -> anyhow::Result<Vec<MemoryFtsCandidate>> {
    let mut sql = format!(
        r#"
        SELECT
            fts.rowid,
            fts.memId, fts.role, fts.content, fts.sessionId, meta.dateMs,
            snippet(memory_fts, 2, '[', ']', '…', {snippet_tokens}) AS snippet,
            bm25(memory_fts, 0.0, 1.0, 5.0, 0.0) AS rank
        FROM memory_fts fts
        JOIN memory_meta meta ON fts.rowid = meta.rowid
        WHERE memory_fts MATCH ?1
        "#,
        snippet_tokens = config::sqlite::SEARCH_SNIPPET_TOKENS
    );

    let mut bind: Vec<rusqlite::types::Value> =
        vec![rusqlite::types::Value::from(fts_query.to_string())];

    if let Some(from) = from_ts {
        sql.push_str(" AND meta.dateMs >= ?");
        bind.push(rusqlite::types::Value::from(from));
    }
    if let Some(to) = to_ts {
        sql.push_str(" AND meta.dateMs <= ?");
        bind.push(rusqlite::types::Value::from(to));
    }

    sql.push_str(" ORDER BY rank ASC LIMIT ?");
    bind.push(rusqlite::types::Value::from(limit));

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(bind.iter()), |r| {
        Ok(MemoryFtsCandidate {
            rowid: r.get(0)?,
            mem_id: r.get(1)?,
            role: r.get(2)?,
            content: r.get(3)?,
            session_id: r.get(4)?,
            date_ms: r.get(5)?,
            snippet: r.get(6)?,
            rank: r.get(7)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Fetch metadata for a single memory entry by rowid (used for vector-only results).
fn fetch_memory_meta(conn: &Connection, rowid: i64) -> anyhow::Result<Option<MemoryMeta>> {
    conn.query_row(
        r#"
        SELECT fts.memId, fts.role, fts.content, fts.sessionId, meta.dateMs
        FROM memory_fts fts
        JOIN memory_meta meta ON fts.rowid = meta.rowid
        WHERE fts.rowid = ?1
        "#,
        params![rowid],
        |r| {
            Ok(MemoryMeta {
                mem_id: r.get(0)?,
                role: r.get(1)?,
                content: r.get(2)?,
                session_id: r.get(3)?,
                date_ms: r.get(4)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
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
            tx.execute("DELETE FROM memory_vec WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM memory_ids WHERE memId = ?1", params![mem_id_val])?;
            removed += 1;
        }
    }

    tx.commit()?;
    log::info!("Removed {} memory entries", removed);
    // Start the merge that drops the removed rows' FTS entries and frees
    // their pages, as the trim does after its own removals: a later trim's
    // positive steps can only continue a merge, so without this it would
    // count the dead entries against the rows still kept and remove more of
    // them than the limit needs. A negative step restarts a merge in
    // progress, so this suits occasional removals, not one before every write.
    if removed > 0 {
        merge_memory_fts_step(conn, -config::memory::TRIM_MERGE_STEP_PAGES)?;
    }
    Ok(removed)
}

/// Bytes the memory database uses: pages in use times the page size. Freed
/// pages stay in the file (the freelist) and are reused, so this is the size
/// the limit applies to, not the file length.
pub fn memory_used_bytes(conn: &Connection) -> anyhow::Result<i64> {
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let freelist_count: i64 = conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    Ok((page_count - freelist_count) * page_size)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryTrimResult {
    pub removed: i64,
    pub used_bytes: i64,
    pub within_limit: bool,
}

/// Remove the oldest memory rows (by dateMs, then rowid) until the used size is
/// at or below `max_bytes`. Each pass removes the oldest rows whose estimated
/// share of the used size covers the excess, then re-measures. Stops when
/// within the limit, when no rows are left, or when `budget` runs out
/// (`within_limit` false; the next call continues, even after rows are written
/// in between: see `trim_until`). Callers pass
/// `config::memory::TRIM_TIME_BUDGET_MS`.
pub fn memory_trim_to_bytes(
    conn: &mut Connection,
    max_bytes: i64,
    budget: std::time::Duration,
) -> anyhow::Result<MemoryTrimResult> {
    let deadline = std::time::Instant::now() + budget;
    trim_until(conn, max_bytes, &mut || std::time::Instant::now() >= deadline)
}

/// `memory_trim_to_bytes`, stopping once `out_of_time` answers true. It is
/// asked after every row removed and every positive merge step. Past the
/// deadline a call can still commit its removals, start the merge for them
/// (one negative step) and run one more positive step before it stops. FTS5
/// does not bound a step's work by its page count; measured at 5 GiB, a step
/// took at most 0.23 s and a call ran at most 1.7 s past the deadline (see
/// ADR-NF-008). The rows removed before it stopped stay removed.
///
/// Sizing a pass: a row's share of the used size is estimated from its
/// content length plus `config::memory::TRIM_ROW_OVERHEAD_BYTES`, scaled so
/// all rows' estimates add up to the used size. Rows differ widely in size
/// (a whole-session row can be a hundred times an exchange), so counting rows
/// instead would remove far more than needed when the oldest are the largest.
/// The estimate only sizes a pass; the loop re-measures after each one.
///
/// FTS5 merge sign discipline: a negative `merge` merges every segment into
/// one, and restarts from the first term whenever a segment was added since
/// the last step; a positive `merge` continues a merge in progress even after
/// new segments arrive. So the merge that drops removed rows' entries is
/// started once, negative, right after they are removed, and every later step
/// is positive. A merge cut short by the budget is then continued by the next
/// call, though each memory write adds a segment first.
fn trim_until(
    conn: &mut Connection,
    max_bytes: i64,
    out_of_time: &mut dyn FnMut() -> bool,
) -> anyhow::Result<MemoryTrimResult> {
    let mut removed: i64 = 0;
    // The rows' estimated weights added up, once a pass needs it.
    let mut total_weight: Option<i64> = None;
    loop {
        let used = memory_used_bytes(conn)?;
        if used <= max_bytes {
            if removed > 0 {
                log::info!("Memory trim: removed {} oldest entries, {} bytes used (limit {})", removed, used, max_bytes);
            }
            return Ok(MemoryTrimResult { removed, used_bytes: used, within_limit: true });
        }
        // Rows already removed keep their FTS entries, and the pages holding
        // them, until the segments are merged: free that space before
        // counting it against the rows still kept.
        if merge_memory_fts_step(conn, config::memory::TRIM_MERGE_STEP_PAGES)? {
            if out_of_time() {
                log::warn!("Memory trim stopped while merging: {} bytes used, limit {}, removed {}", used, max_bytes, removed);
                return Ok(MemoryTrimResult { removed, used_bytes: used, within_limit: false });
            }
            continue;
        }
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM memory_meta", [], |r| r.get(0))?;
        if count == 0 || out_of_time() {
            log::warn!(
                "Memory trim stopped above the limit: {} bytes used, limit {}, {} entries left, removed {}",
                used, max_bytes, count, removed
            );
            return Ok(MemoryTrimResult { removed, used_bytes: used, within_limit: false });
        }
        let target = max_bytes * config::memory::TRIM_TARGET_PERCENT / 100;
        let total = match total_weight {
            Some(total) => total,
            None => memory_total_weight(conn)?,
        };
        // The weight whose share of the used size is the excess.
        let excess = (used - target) as i128;
        let needed = ((total as i128 * excess + used as i128 - 1) / used as i128) as i64;
        let (n, weight) = remove_oldest_memory_rows(conn, needed, out_of_time)?;
        removed += n;
        total_weight = Some(total - weight);
        merge_memory_fts_step(conn, -config::memory::TRIM_MERGE_STEP_PAGES)?;
    }
}

/// One FTS5 `merge` step of about `pages.abs()` pages written: negative starts
/// merging every segment into one, positive continues a merge in progress (see
/// `trim_until`). The merge drops removed rows' entries and frees their pages.
/// Answers whether there was work to do: FTS5 makes fewer than two changes
/// when there is none.
fn merge_memory_fts_step(conn: &Connection, pages: i64) -> anyhow::Result<bool> {
    let before = conn.total_changes();
    conn.execute(
        "INSERT INTO memory_fts(memory_fts, rank) VALUES('merge', ?1)",
        params![pages],
    )?;
    Ok(conn.total_changes() - before >= 2)
}

/// The estimated weights of all memory rows added up (see `trim_until`).
fn memory_total_weight(conn: &Connection) -> anyhow::Result<i64> {
    let (count, content): (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(octet_length(content)), 0) FROM memory_fts",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    Ok(content + count * config::memory::TRIM_ROW_OVERHEAD_BYTES)
}

/// Removes the oldest rows from all four tables in one transaction, until
/// their estimated weights reach `weight` (at least one row), committing early
/// once `out_of_time` answers true. Answers the rows removed and their weight.
fn remove_oldest_memory_rows(
    conn: &mut Connection,
    weight: i64,
    out_of_time: &mut dyn FnMut() -> bool,
) -> anyhow::Result<(i64, i64)> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (mut removed, mut removed_weight): (i64, i64) = (0, 0);
    {
        let mut oldest = tx.prepare("SELECT rowid FROM memory_meta ORDER BY dateMs ASC, rowid ASC")?;
        let mut length = tx.prepare("SELECT octet_length(content) FROM memory_fts WHERE rowid = ?1")?;
        let mut rows = oldest.query([])?;
        while removed_weight < weight {
            let Some(row) = rows.next()? else { break };
            let row_id: i64 = row.get(0)?;
            let content: Option<i64> = length.query_row(params![row_id], |r| r.get(0)).optional()?.flatten();
            tx.execute("DELETE FROM memory_fts WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM memory_meta WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM memory_vec WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM memory_ids WHERE rowid = ?1", params![row_id])?;
            removed += 1;
            removed_weight += content.unwrap_or(0) + config::memory::TRIM_ROW_OVERHEAD_BYTES;
            if out_of_time() {
                break;
            }
        }
    }
    tx.commit()?;
    Ok((removed, removed_weight))
}

/// Start rebuilding memory vector embeddings: clear vec tables and return total count.
/// Call this once, then call `rebuild_memory_embeddings_batch` repeatedly until done.
pub fn rebuild_memory_embeddings_start(conn: &mut Connection) -> anyhow::Result<i64> {
    log::info!("Starting memory embedding rebuild — clearing vector tables");
    conn.execute("DELETE FROM memory_vec", [])?;
    conn.execute("DELETE FROM embed_cache", []).ok(); // ok() in case embed_cache doesn't exist
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM memory_fts", [], |r| r.get(0))?;
    log::info!("Cleared memory_vec and embed_cache, {} entries to embed", total);
    Ok(total)
}

/// Process one batch of memory embedding rebuild.
/// Returns (last_rowid, processed_in_batch, embedded_in_batch, done).
pub fn rebuild_memory_embeddings_batch(
    conn: &mut Connection,
    engine: &EmbeddingEngine,
    last_rowid: i64,
    batch_size: i64,
) -> anyhow::Result<(i64, i64, i64, bool)> {
    let batch: Vec<(i64, String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT rowid, role, content FROM memory_fts WHERE rowid > ?1 ORDER BY rowid ASC LIMIT ?2"
        )?;
        let rows = stmt.query_map(params![last_rowid, batch_size], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    if batch.is_empty() {
        return Ok((last_rowid, 0, 0, true));
    }

    let mut new_last_rowid = last_rowid;
    let mut embedded: i64 = 0;
    let processed = batch.len() as i64;
    let done = (batch.len() as i64) < batch_size;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    for (rowid, role, content) in &batch {
        let embed_text = crate::embeddings::text_prep::prepare_memory_text(role, content);
        match engine.embed(&embed_text) {
            Ok(embedding) => {
                let blob = super::db::f32_vec_to_blob(&embedding);
                // vec0 virtual tables don't support INSERT OR REPLACE,
                // so delete first to handle checkpoint-resume overlaps.
                tx.execute("DELETE FROM memory_vec WHERE rowid = ?1", params![rowid])?;
                tx.execute(
                    "INSERT INTO memory_vec (rowid, embedding) VALUES (?1, ?2)",
                    params![rowid, blob],
                )?;
                embedded += 1;
            }
            Err(e) => {
                log::warn!("Failed to embed memory rowid {}: {}", rowid, e);
            }
        }
        new_last_rowid = *rowid;
    }
    tx.commit()?;

    Ok((new_last_rowid, processed, embedded, done))
}

/// Clear and rebuild memory database.
/// Takes ownership of the writer's connection to close it. The caller must have closed every
/// other connection to the file first (the reader's): Windows refuses to delete an open file.
/// Returns the writer's new connection and whether the files were deleted. When they were not,
/// the new connection is to the old database with every row still in it.
pub fn memory_clear_rebuild_standalone(db_path: &Path, conn: Connection) -> anyhow::Result<(Connection, anyhow::Result<()>)> {
    log::info!("Clearing memory database by deleting database file (rebuild from scratch)");
    drop(conn);
    log::info!("Memory database connection closed");

    let deleted = super::db::delete_database_files(db_path);

    log::info!("Recreating memory database...");
    let new_conn = open_memory_writer_connection(db_path)?;
    log::info!("Memory database recreated and initialized successfully");
    Ok((new_conn, deleted))
}

/// The writer's connection to the memory database at `db_path`, created empty when the file is
/// not there: what memoryClear opens after deleting the files, and what the writer opens again
/// when a memoryClear failed before it could.
pub fn open_memory_writer_connection(db_path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(db_path)?;
    super::db::ensure_fts5_available(&conn)?;
    init_memory_database(&conn)?;
    Ok(conn)
}

/// Read memory entries around a given timestamp (±tolerance_ms)
/// Returns full conversation content for entries within the time window
pub fn memory_read_by_timestamp(
    conn: &Connection,
    timestamp_ms: i64,
    tolerance_ms: i64,
) -> anyhow::Result<Vec<Value>> {
    let from_ms = timestamp_ms - tolerance_ms;
    let to_ms = timestamp_ms + tolerance_ms;

    log::info!(
        "Memory read by timestamp: {} (±{}ms = {} to {})",
        timestamp_ms,
        tolerance_ms,
        from_ms,
        to_ms
    );

    let mut stmt = conn.prepare(
        r#"
        SELECT fts.memId, fts.role, fts.content, fts.sessionId, meta.dateMs
        FROM memory_fts fts
        JOIN memory_meta meta ON fts.rowid = meta.rowid
        WHERE meta.dateMs >= ?1 AND meta.dateMs <= ?2
        ORDER BY meta.dateMs ASC
        LIMIT 50
        "#,
    )?;

    let rows = stmt.query_map(params![from_ms, to_ms], |r| {
        let mem_id: String = r.get(0)?;
        let role: String = r.get(1)?;
        let content: String = r.get(2)?;
        let session_id: String = r.get(3)?;
        let date_ms: i64 = r.get(4)?;

        Ok(serde_json::json!({
            "memId": mem_id,
            "role": role,
            "content": content,
            "sessionId": session_id,
            "dateMs": date_ms
        }))
    })?;

    let mut results: Vec<Value> = vec![];
    for r in rows {
        results.push(r?);
    }

    log::info!(
        "Memory read by timestamp: found {} entries in time window",
        results.len()
    );
    Ok(results)
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

fn truncate_for_log(s: &str) -> String {
    let max = 80usize;
    if s.len() <= max {
        return s.to_string();
    }
    s.chars().take(max).collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A fresh on-disk memory database (WAL, like the helper's) in its own temp folder.
    fn temp_memory_db(name: &str) -> (PathBuf, Connection) {
        // As main() does before opening any connection; registering twice is a no-op.
        type ExtensionInit = unsafe extern "C" fn(
            *mut rusqlite::ffi::sqlite3,
            *mut *mut std::os::raw::c_char,
            *const rusqlite::ffi::sqlite3_api_routines,
        ) -> std::os::raw::c_int;
        unsafe {
            rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute::<*const (), ExtensionInit>(
                sqlite_vec::sqlite3_vec_init as *const (),
            )));
        }
        let dir = std::env::temp_dir().join(format!("tabmail-memory-trim-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (_path, conn) = open_or_create_memory_db(&dir).unwrap();
        (dir, conn)
    }

    /// Index `n` exchanges whose dateMs follows `date_of(i)`, each with a vector row.
    fn fill(conn: &mut Connection, n: usize, date_of: impl Fn(usize) -> i64) {
        let filler = "planning notes for the quarterly review meeting ".repeat(20);
        let rows: Vec<Value> = (0..n)
            .map(|i| serde_json::json!({
                "memId": format!("turn:{i}"), "role": "session",
                "content": format!("[USER]: question {i}\n\n[AGENT]: {filler}"),
                "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": date_of(i),
            }))
            .collect();
        memory_index_batch(conn, &rows, None).unwrap();
        let blob = crate::fts::db::f32_vec_to_blob(&vec![0.5f32; config::embedding::EMBEDDING_DIMS]);
        let ids: Vec<i64> = conn
            .prepare("SELECT rowid FROM memory_ids").unwrap()
            .query_map([], |r| r.get(0)).unwrap()
            .collect::<Result<_, _>>().unwrap();
        for id in ids {
            conn.execute("INSERT INTO memory_vec (rowid, embedding) VALUES (?1, ?2)", params![id, blob]).unwrap();
        }
    }

    fn table_counts(conn: &Connection) -> [i64; 4] {
        ["memory_fts", "memory_meta", "memory_vec", "memory_ids"].map(|t| {
            conn.query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0)).unwrap()
        })
    }

    fn budget() -> std::time::Duration {
        std::time::Duration::from_millis(config::memory::TRIM_TIME_BUDGET_MS)
    }

    fn kept_dates(conn: &Connection) -> Vec<i64> {
        conn.prepare("SELECT dateMs FROM memory_meta ORDER BY dateMs").unwrap()
            .query_map([], |r| r.get(0)).unwrap()
            .collect::<Result<_, _>>().unwrap()
    }

    fn rowids(conn: &Connection, table: &str) -> Vec<i64> {
        conn.prepare(&format!("SELECT rowid FROM {table} ORDER BY rowid")).unwrap()
            .query_map([], |r| r.get(0)).unwrap()
            .collect::<Result<_, _>>().unwrap()
    }

    /// Every table holds the same exchanges: equal counts are not enough, as
    /// vectors removed for the wrong rows leave the counts equal.
    fn assert_tables_hold(conn: &Connection, expected: &[i64]) {
        for table in ["memory_fts", "memory_meta", "memory_vec", "memory_ids"] {
            assert_eq!(rowids(conn, table), expected, "{table}");
        }
    }

    #[test]
    fn test_a_trim_removes_each_exchange_from_every_table_and_keeps_the_rest_whole() {
        let (dir, mut conn) = temp_memory_db("identity");
        let n = 3000;
        fill(&mut conn, n, |i| i as i64);
        let all = rowids(&conn, "memory_meta");
        assert_tables_hold(&conn, &all);
        let limit = memory_used_bytes(&conn).unwrap() * 9 / 10;

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit && trim.removed > 0 && trim.removed < n as i64);
        drop(conn);
        let (_path, conn) = open_or_create_memory_db(&dir).unwrap();
        // The oldest (lowest dates, so lowest rowids) went; the rest kept their
        // text, metadata, vector and id.
        assert_tables_hold(&conn, &all[trim.removed as usize..]);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_a_trim_that_fails_partway_through_an_exchange_leaves_every_table_whole() {
        let (dir, mut conn) = temp_memory_db("failure");
        let n = 3000;
        fill(&mut conn, n, |i| i as i64);
        let all = rowids(&conn, "memory_meta");
        let limit = memory_used_bytes(&conn).unwrap() / 2;
        // A storage error on an exchange's last table, after its other rows
        // were deleted.
        conn.execute_batch(
            "CREATE TRIGGER fail_id_delete BEFORE DELETE ON memory_ids
             BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END",
        ).unwrap();

        let error = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap_err();

        assert!(error.to_string().contains("injected storage failure"), "{error}");
        drop(conn);
        let (_path, mut conn) = open_or_create_memory_db(&dir).unwrap();
        assert_tables_hold(&conn, &all);
        // Once the storage works again, the next trim removes the oldest whole.
        conn.execute_batch("DROP TRIGGER fail_id_delete").unwrap();
        let retry = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();
        assert!(retry.within_limit && retry.removed > 0);
        assert_tables_hold(&conn, &all[retry.removed as usize..]);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_trim_brings_used_size_within_limit_keeping_the_newest() {
        let (dir, mut conn) = temp_memory_db("limit");
        // Dates run against rowid order, so "oldest" can only mean dateMs.
        let n = 3000;
        fill(&mut conn, n, |i| 1_000_000 - i as i64);
        let before = memory_used_bytes(&conn).unwrap();
        let limit = before / 2;

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit);
        let used = memory_used_bytes(&conn).unwrap();
        assert_eq!(trim.used_bytes, used);
        assert!(used <= limit, "used {used} > limit {limit}");
        let counts = table_counts(&conn);
        let kept = counts[0];
        assert_eq!(counts, [kept; 4], "FTS, meta, vector and id rows are removed together");
        assert_eq!(trim.removed, n as i64 - kept);
        // Kept rows are exactly the newest `kept` dates.
        let expected: Vec<i64> = ((1_000_000 - kept + 1)..=1_000_000).collect();
        assert_eq!(kept_dates(&conn), expected);
        // Half the size should keep a fair share, not empty the memory.
        assert!(kept * 100 >= n as i64 * 30, "kept only {kept} of {n}");
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Merges the FTS index fully, as the trim does: started once, then continued.
    fn merge_all(conn: &Connection) {
        if merge_memory_fts_step(conn, -config::memory::TRIM_MERGE_STEP_PAGES).unwrap() {
            while merge_memory_fts_step(conn, config::memory::TRIM_MERGE_STEP_PAGES).unwrap() {}
        }
    }

    /// Answers true from its `k`th question on.
    fn out_of_time_after(k: usize) -> impl FnMut() -> bool {
        let mut asked = 0;
        move || {
            asked += 1;
            asked >= k
        }
    }

    #[test]
    fn test_trim_cut_short_keeps_its_progress_and_the_next_call_continues() {
        let (dir, mut conn) = temp_memory_db("budget");
        let n = 3000;
        fill(&mut conn, n, |i| i as i64);
        let limit = memory_used_bytes(&conn).unwrap() / 4;
        // Index merged first, so the trim's first question about time comes
        // just before it removes rows.
        merge_all(&conn);

        // Time runs out after the 100th row of a pass that wanted far more:
        // the call stops there, over the limit, and those 100 rows (the
        // oldest) are gone from every table, durably.
        let cut_short = trim_until(&mut conn, limit, &mut out_of_time_after(101)).unwrap();
        assert!(!cut_short.within_limit);
        assert!(cut_short.used_bytes > limit);
        assert_eq!(cut_short.removed, 100);
        drop(conn);
        let (_path, mut conn) = open_or_create_memory_db(&dir).unwrap();
        assert_eq!(table_counts(&conn), [n as i64 - 100; 4]);
        assert_eq!(kept_dates(&conn), (100..n as i64).collect::<Vec<_>>());

        // Out of time while merging the removed rows' entries: nothing more
        // is removed until that space is freed.
        let merging = trim_until(&mut conn, limit, &mut out_of_time_after(1)).unwrap();
        assert_eq!(merging.removed, 0);
        assert_eq!(table_counts(&conn), [n as i64 - 100; 4]);
        // Out of time once rows are being removed: a call removes one and stops.
        merge_all(&conn);
        let one = trim_until(&mut conn, limit, &mut out_of_time_after(2)).unwrap();
        assert_eq!(one.removed, 1);
        assert_eq!(kept_dates(&conn).first(), Some(&101));

        // The next call, with the normal budget, finishes the job.
        let finished = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();
        assert!(finished.within_limit);
        assert!(memory_used_bytes(&conn).unwrap() <= limit);
        let kept = table_counts(&conn)[0];
        assert!(kept > 0);
        assert_eq!(table_counts(&conn), [kept; 4]);
        assert_eq!(kept_dates(&conn), ((n as i64 - kept)..n as i64).collect::<Vec<_>>());
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Index `batches` batches of `per_batch` exchanges of varied words, as
    /// that many index calls (so the FTS index holds several segments), with
    /// no vectors: the helper's mode when the model can't load.
    fn fill_text_only(conn: &mut Connection, batches: usize, per_batch: usize, words: usize) {
        let mut seed: u64 = 7;
        let mut word = move || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            format!("w{}", (seed >> 33) % 20_000)
        };
        for b in 0..batches {
            let rows: Vec<Value> = (0..per_batch)
                .map(|j| {
                    let i = b * per_batch + j;
                    let text: Vec<String> = (0..words).map(|_| word()).collect();
                    serde_json::json!({
                        "memId": format!("turn:{i}"), "role": "session",
                        "content": format!("[USER]: question {i}\n\n[AGENT]: {}", text.join(" ")),
                        "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": i as i64,
                    })
                })
                .collect();
            memory_index_batch(conn, &rows, None).unwrap();
        }
    }

    #[test]
    fn test_trim_keeps_the_newest_rows_that_fit_rather_than_emptying_memory() {
        let (dir, mut conn) = temp_memory_db("fts-only");
        let (batches, per_batch) = (10, 200);
        let n = (batches * per_batch) as i64;
        fill_text_only(&mut conn, batches, per_batch, 400);
        let full = memory_used_bytes(&conn).unwrap();
        // The newest twentieth of the rows fits in about a twentieth of the
        // space, so the trim keeps rows; a deleted row's index entries are
        // freed as it goes, not left behind to be counted against the rest.
        let limit = full / 20;

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit, "{trim:?}, limit {limit}, was {full}");
        assert!(memory_used_bytes(&conn).unwrap() <= limit);
        let kept = table_counts(&conn)[0];
        assert_eq!(&table_counts(&conn)[..2], &[kept; 2]);
        assert!(kept * 100 >= n * 2, "kept {kept} of {n}");
        assert_eq!(kept_dates(&conn), ((n - kept)..n).collect::<Vec<_>>());
        // Already within the limit: a second trim removes nothing.
        assert_eq!(memory_trim_to_bytes(&mut conn, limit, budget()).unwrap().removed, 0);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }


    /// Indexes one text-only exchange dated `i`, as one memory write does.
    fn add_text(conn: &mut Connection, i: usize) {
        let row = serde_json::json!({
            "memId": format!("turn:{i}"), "role": "session",
            "content": format!("[USER]: question {i}\n\n[AGENT]: answer {i} about the launch"),
            "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": i as i64,
        });
        memory_index_batch(conn, &[row], None).unwrap();
    }

    /// Where a full merge takes longer than one call's budget, each call cut
    /// short and each followed by a memory write (as `memoryIndexBatch` writes
    /// before it trims), the calls still continue the merge and go on removing
    /// the oldest rows; restarting the merge at every write would remove none.
    #[test]
    fn test_trims_cut_short_while_merging_keep_removing_the_oldest_across_writes() {
        let (dir, mut conn) = temp_memory_db("merge-across-writes");
        let (batches, per_batch) = (10, 200);
        let n = batches * per_batch;
        fill_text_only(&mut conn, batches, per_batch, 400);
        let limit = memory_used_bytes(&conn).unwrap() / 20;

        let (mut removed, mut cut_short_merging) = (0, 0);
        for round in 0..30 {
            let trim = trim_until(&mut conn, limit, &mut out_of_time_after(4)).unwrap();
            if trim.removed == 0 && !trim.within_limit {
                cut_short_merging += 1;
            }
            removed += trim.removed;
            add_text(&mut conn, n + round);
        }
        // The budget really did cut merges short, and rows still went.
        assert!(cut_short_merging > 0);
        assert!(removed > 0, "no row removed in 30 calls");
        let kept = table_counts(&conn)[0];
        assert_eq!(&table_counts(&conn)[..2], &[kept; 2]);
        let newest = (n + 29) as i64;
        assert_eq!(kept_dates(&conn), ((newest + 1 - kept)..=newest).collect::<Vec<_>>());
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Out of time while merging, a call stops there, with the merge unfinished:
    /// one call never runs a whole merge past its budget.
    #[test]
    fn test_trim_out_of_time_while_merging_stops_with_the_merge_unfinished() {
        let (dir, mut conn) = temp_memory_db("merge-budget");
        fill_text_only(&mut conn, 10, 200, 400);
        let limit = memory_used_bytes(&conn).unwrap() / 20;
        // A merge in progress, as one a call cut short leaves.
        assert!(merge_memory_fts_step(&conn, -config::memory::TRIM_MERGE_STEP_PAGES).unwrap());

        let trim = trim_until(&mut conn, limit, &mut out_of_time_after(1)).unwrap();

        assert_eq!(trim.removed, 0);
        assert!(!trim.within_limit);
        assert!(merge_memory_fts_step(&conn, config::memory::TRIM_MERGE_STEP_PAGES).unwrap(), "the merge was finished past the budget");
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A trim aims below the limit, so the writes right after it don't each
    /// trim again: memory can grow by a few percent of the limit first.
    #[test]
    fn test_the_writes_just_after_a_trim_remove_nothing() {
        let (dir, mut conn) = temp_memory_db("aim");
        let n = 2000;
        fill_text_only(&mut conn, 10, n / 10, 400);
        let limit = memory_used_bytes(&conn).unwrap() / 2;
        assert!(memory_trim_to_bytes(&mut conn, limit, budget()).unwrap().within_limit);
        let kept = table_counts(&conn)[0];
        let grown_to = memory_used_bytes(&conn).unwrap() + limit * 3 / 100;

        let mut i = n;
        while memory_used_bytes(&conn).unwrap() < grown_to {
            let words: Vec<String> = (0..400).map(|k| format!("x{i}y{k}")).collect();
            let row = serde_json::json!({
                "memId": format!("turn:{i}"), "role": "session",
                "content": format!("[USER]: question {i}\n\n[AGENT]: {}", words.join(" ")),
                "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": i as i64,
            });
            memory_index_batch(&mut conn, &[row], None).unwrap();
            let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();
            assert_eq!((trim.removed, trim.within_limit), (0, true), "after exchange {i}");
            i += 1;
        }
        assert_eq!(table_counts(&conn)[0], kept + (i - n) as i64);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Keeps exchange `i` (dated `i`) with its vector row, as one index call.
    fn add(conn: &mut Connection, i: usize) {
        let row = serde_json::json!({
            "memId": format!("turn:{i}"), "role": "session",
            "content": format!("[USER]: question {i}\n\n[AGENT]: {}", "planning notes for the quarterly review meeting ".repeat(20)),
            "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": i as i64,
        });
        memory_index_batch(conn, &[row], None).unwrap();
        let id: i64 = conn.query_row("SELECT max(rowid) FROM memory_ids", [], |r| r.get(0)).unwrap();
        let blob = crate::fts::db::f32_vec_to_blob(&vec![0.5f32; config::embedding::EMBEDDING_DIMS]);
        conn.execute("INSERT INTO memory_vec (rowid, embedding) VALUES (?1, ?2)", params![id, blob]).unwrap();
    }

    /// At the limit, as each new exchange is kept and trimmed for, only the
    /// oldest go and memory stays near the limit, never emptied. The vector
    /// index frees its space a chunk at a time (sqlite-vec's 1,024 rows) and
    /// adds only to its newest chunk, so exchanges kept in date order empty
    /// the oldest chunk first: a trim removes at most a chunk's worth more
    /// than the text alone would need.
    #[test]
    fn test_at_the_limit_each_new_exchange_removes_only_the_oldest() {
        const CHUNK_ROWS: i64 = 1_024;
        let (dir, mut conn) = temp_memory_db("steady");
        let n = 3000;
        fill(&mut conn, n, |i| i as i64);
        let limit = memory_used_bytes(&conn).unwrap() * 9 / 10;
        assert!(memory_trim_to_bytes(&mut conn, limit, budget()).unwrap().within_limit);
        let fitted = table_counts(&conn)[0];
        let mut fewest = fitted;

        for i in n..n + CHUNK_ROWS as usize + 200 {
            add(&mut conn, i);
            let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();
            assert!(trim.within_limit, "{trim:?} after exchange {i}");
            let kept = table_counts(&conn)[0];
            assert_eq!(table_counts(&conn), [kept; 4]);
            // The newest `kept`, the new one among them.
            assert_eq!(kept_dates(&conn), ((i as i64 + 1 - kept)..=i as i64).collect::<Vec<_>>());
            fewest = fewest.min(kept);
        }
        assert!(fewest >= fitted - CHUNK_ROWS, "kept as few as {fewest}; {fitted} fitted the limit");
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Keeps `count` text-only rows of about `words` words each, dated
    /// `first..first + count`, in one index call.
    fn add_sized(conn: &mut Connection, first: usize, count: usize, words: usize) {
        let rows: Vec<Value> = (first..first + count)
            .map(|i| {
                let text: Vec<String> = (0..words).map(|k| format!("w{}", (i * 7919 + k * 104_729) % 50_000)).collect();
                serde_json::json!({
                    "memId": format!("turn:{i}"), "role": "session",
                    "content": format!("[USER]: question {i}\n\n[AGENT]: {}", text.join(" ")),
                    "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": i as i64,
                })
            })
            .collect();
        memory_index_batch(conn, &rows, None).unwrap();
    }

    /// Memory where the oldest rows are much larger than the newer ones, as
    /// whole-session rows written before per-exchange rows are: 300 rows of
    /// about 6,000 words, then 3,000 of about 60. Answers the number of rows.
    fn fill_large_then_small(conn: &mut Connection) -> usize {
        for b in 0..3 {
            add_sized(conn, b * 100, 100, 6_000);
        }
        for b in 0..3 {
            add_sized(conn, 300 + b * 1_000, 1_000, 60);
        }
        merge_all(conn);
        3_300
    }

    /// The trim removes only as many of the oldest rows as the limit needs,
    /// even when they are far larger than the rest: memory lands near its aim
    /// below the limit, and every newer row that fits is kept.
    #[test]
    fn test_trim_keeps_every_newer_row_that_fits_when_the_oldest_are_largest() {
        let (dir, mut conn) = temp_memory_db("skew");
        let n = fill_large_then_small(&mut conn) as i64;
        let limit = memory_used_bytes(&conn).unwrap() * 8 / 10;

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit, "{trim:?}");
        let used = memory_used_bytes(&conn).unwrap();
        assert!(used <= limit);
        assert!(used * 100 >= limit * 85, "used {used} of the limit {limit} after removing {}", trim.removed);
        let kept = table_counts(&conn)[0];
        assert_eq!(kept_dates(&conn), ((n - kept)..n).collect::<Vec<_>>());
        // Only large rows went: all 3,000 newer small ones are kept.
        assert!(kept >= 3_000, "kept {kept}, removed {}", trim.removed);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// At the limit with the oldest rows largest, one new exchange removes
    /// about one old row, not a share of every row.
    #[test]
    fn test_at_the_limit_a_new_exchange_removes_only_what_it_needs_when_the_oldest_are_largest() {
        let (dir, mut conn) = temp_memory_db("skew-steady");
        let n = fill_large_then_small(&mut conn);
        let limit = memory_used_bytes(&conn).unwrap() * 99 / 100;
        add_sized(&mut conn, n, 1, 60);
        let used_before = memory_used_bytes(&conn).unwrap();
        assert!(used_before > limit);

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit, "{trim:?}");
        let used = memory_used_bytes(&conn).unwrap();
        assert!(used * 100 >= limit * 90, "used {used} of the limit {limit} after removing {}", trim.removed);
        let kept = table_counts(&conn)[0];
        assert_eq!(kept_dates(&conn), ((n as i64 + 1 - kept)..=n as i64).collect::<Vec<_>>());
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 2,000 text-only exchanges of about 400 words, in ten index calls,
    /// merged. Answers the used size.
    fn fill_merged_text_only(conn: &mut Connection) -> i64 {
        fill_text_only(conn, 10, 200, 400);
        merge_all(conn);
        memory_used_bytes(conn).unwrap()
    }

    /// A trim that needs several passes reclaims the space of each pass's
    /// removed rows before sizing the next, so memory lands near its aim
    /// below the limit rather than far under it.
    #[test]
    fn test_a_trim_of_several_passes_lands_near_its_aim() {
        let (dir, mut conn) = temp_memory_db("passes");
        let full = fill_merged_text_only(&mut conn);
        let limit = full / 2;

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit, "{trim:?}");
        merge_all(&conn);
        let used = memory_used_bytes(&conn).unwrap();
        assert!(used <= limit);
        assert!(used * 100 >= limit * 85, "used {used} of the limit {limit} after removing {}", trim.removed);
        let kept = table_counts(&conn)[0];
        assert!(kept >= 900, "kept {kept} of 2000");
        assert_eq!(kept_dates(&conn), ((2_000 - kept)..2_000).collect::<Vec<_>>());
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Rows removed by id free their space before a later trim measures it:
    /// the trim removes only what the rows still kept need, as many as if
    /// the removed rows had never been written.
    #[test]
    fn test_a_trim_after_rows_were_removed_removes_only_what_the_limit_needs() {
        let (dir, mut conn) = temp_memory_db("removed-then-trim");
        let full = fill_merged_text_only(&mut conn);
        let oldest: Vec<Value> = (0..1_000).map(|i| Value::from(format!("turn:{i}"))).collect();
        assert_eq!(memory_remove_batch(&mut conn, &oldest).unwrap(), 1_000);

        let trim = memory_trim_to_bytes(&mut conn, full / 2, budget()).unwrap();

        assert!(trim.within_limit, "{trim:?}");
        let kept = table_counts(&conn)[0];
        assert!(kept >= 900, "kept {kept} of the 1000 left, removed {}", trim.removed);
        assert_eq!(kept_dates(&conn), ((2_000 - kept)..2_000).collect::<Vec<_>>());
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The trim removes only as many of the oldest rows as the limit needs
    /// when they are small next to their fixed cost and the newest are large.
    #[test]
    fn test_trim_keeps_every_newer_row_that_fits_when_the_oldest_are_smallest() {
        let (dir, mut conn) = temp_memory_db("skew-small");
        for b in 0..10 {
            add_sized(&mut conn, b * 1_000, 1_000, 20);
        }
        for b in 0..3 {
            add_sized(&mut conn, 10_000 + b * 100, 100, 6_000);
        }
        merge_all(&conn);
        let n = 10_300;
        let limit = memory_used_bytes(&conn).unwrap() * 9 / 10;

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit, "{trim:?}");
        let kept = table_counts(&conn)[0];
        assert_eq!(kept_dates(&conn), ((n - kept)..n).collect::<Vec<_>>());
        assert!(kept >= 4_000, "kept {kept}, removed {}", trim.removed);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A trim with no time left removes nothing and says it is over the
    /// limit; given time, the same trim finishes. The wall clock is what
    /// bounds a request, not only the size of the work.
    #[test]
    fn test_a_trim_with_no_time_left_removes_nothing() {
        let (dir, mut conn) = temp_memory_db("no-time");
        fill_text_only(&mut conn, 3, 100, 50);
        merge_all(&conn);
        let limit = memory_used_bytes(&conn).unwrap() / 2;

        let none = memory_trim_to_bytes(&mut conn, limit, std::time::Duration::ZERO).unwrap();

        assert!(!none.within_limit, "{none:?}");
        assert_eq!(none.removed, 0);
        assert_eq!(&table_counts(&conn)[..2], &[300; 2]);
        let given_time = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();
        assert!(given_time.within_limit && given_time.removed > 0, "{given_time:?}");
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Rows with the same date go in the order they were written.
    #[test]
    fn test_trim_removes_rows_of_the_same_date_first_written_first() {
        let (dir, mut conn) = temp_memory_db("same-date");
        let rows: Vec<Value> = (0..40)
            .map(|i| serde_json::json!({
                "memId": format!("turn:{i}"), "role": "session",
                "content": format!("[USER]: question {i}\n\n[AGENT]: {}", "notes for the launch review ".repeat(40)),
                "sessionId": "persistent_chat", "turnIndex": 0, "dateMs": 5,
            }))
            .collect();
        memory_index_batch(&mut conn, &rows, None).unwrap();
        merge_all(&conn);
        let limit = memory_used_bytes(&conn).unwrap() - 1;

        let trim = memory_trim_to_bytes(&mut conn, limit, budget()).unwrap();

        assert!(trim.within_limit && trim.removed > 0, "{trim:?}");
        let kept: Vec<String> = conn
            .prepare("SELECT memId FROM memory_ids ORDER BY rowid").unwrap()
            .query_map([], |r| r.get(0)).unwrap()
            .collect::<Result<_, _>>().unwrap();
        let expected: Vec<String> = (trim.removed..40).map(|i| format!("turn:{i}")).collect();
        assert_eq!(kept, expected);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_trim_within_limit_removes_nothing() {
        let (dir, mut conn) = temp_memory_db("noop");
        fill(&mut conn, 50, |i| i as i64);
        let used = memory_used_bytes(&conn).unwrap();
        let trim = memory_trim_to_bytes(&mut conn, used, budget()).unwrap();
        assert_eq!(trim, MemoryTrimResult { removed: 0, used_bytes: used, within_limit: true });
        assert_eq!(table_counts(&conn), [50; 4]);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_trim_below_the_empty_schema_removes_everything_and_reports_over_limit() {
        let (dir, mut conn) = temp_memory_db("tiny");
        fill(&mut conn, 20, |i| i as i64);
        let trim = memory_trim_to_bytes(&mut conn, 1, budget()).unwrap();
        assert!(!trim.within_limit);
        assert_eq!(trim.removed, 20);
        assert_eq!(table_counts(&conn), [0; 4]);
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_memory_fts_tokenizer_rebuild_in_place() {
        // Existing memory.db with the OLD tokenchars tokenizer must be
        // rebuilt in place (rowid preserved, parts matchable) — NO schema
        // version involvement (ADR-024).
        let mut conn = Connection::open_in_memory().unwrap();
        let old_tokenize = "porter unicode61 remove_diacritics 2 tokenchars '-_.@'";
        conn.execute_batch(&format!(
            r#"CREATE VIRTUAL TABLE memory_fts USING fts5(
                memId, role, content, sessionId,
                tokenize = "{old_tokenize}", prefix = '2 3 4')"#
        )).unwrap();
        conn.execute(
            "INSERT INTO memory_fts(rowid, memId, role, content, sessionId)
             VALUES (3, 'chat:s1:0', 'user', 'ask billing-alerts@domain.com about the invoice', 's1')",
            [],
        ).unwrap();

        // Old tokenizer: part query cannot match the glued address token
        let n: i64 = conn.query_row(
            "SELECT count(*) FROM memory_fts WHERE memory_fts MATCH 'billing'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 0, "precondition: glued token must not part-match");

        rebuild_memory_fts_if_stale_tokenizer(&mut conn).unwrap();

        let sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE name='memory_fts'",
            [], |r| r.get(0),
        ).unwrap();
        assert!(!sql.contains("tokenchars"), "rebuilt memory_fts still has tokenchars: {sql}");

        let (rowid, content): (i64, String) = conn.query_row(
            "SELECT rowid, content FROM memory_fts WHERE memory_fts MATCH 'billing'",
            [], |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!(rowid, 3, "rowid must be preserved");
        assert!(content.contains("invoice"), "content must survive rebuild");

        // Idempotent: second run no-ops
        rebuild_memory_fts_if_stale_tokenizer(&mut conn).unwrap();
        let n: i64 = conn.query_row(
            "SELECT count(*) FROM memory_fts WHERE memory_fts MATCH 'billing'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 1);
    }

    /// A listing (no query) gives the newest entries within the dates asked, newest first, at most
    /// `limit` of them, each whole; both bounds include their millisecond and `ignoreDate` wins.
    #[test]
    fn memory_listing_gives_the_newest_within_the_dates_each_whole() {
        let (dir, mut conn) = temp_memory_db("listing");
        let base = chrono::Utc::now().timestamp_millis() - 100_000;
        // Kept out of date order, so an order the table happens to hold shows.
        let order = [4, 9, 0, 7, 2, 5, 8, 1, 6, 3];
        let rows: Vec<Value> = order
            .iter()
            .map(|&i| serde_json::json!({
                "memId": format!("turn:{i}"), "role": "session",
                "content": format!("[USER]: question {i}\n\n[AGENT]: answer {i}"),
                "sessionId": format!("session-{i}"), "turnIndex": i, "dateMs": base + i * 1_000,
            }))
            .collect();
        memory_index_batch(&mut conn, &rows, None).unwrap();
        let synonyms = SynonymLookup::new();
        let list = |params: Value| -> Vec<i64> {
            let results = memory_search(&conn, "", &params, &synonyms, None).unwrap();
            for r in &results {
                let i = r["memId"].as_str().unwrap().strip_prefix("turn:").unwrap().parse::<i64>().unwrap();
                assert_eq!(r["content"], format!("[USER]: question {i}\n\n[AGENT]: answer {i}"));
                assert_eq!(r["sessionId"], format!("session-{i}"));
                assert_eq!(r["role"], "session");
                assert_eq!(r["dateMs"], base + i * 1_000);
                assert!(r["snippet"].is_null());
            }
            results.iter().map(|r| (r["dateMs"].as_i64().unwrap() - base) / 1_000).collect()
        };
        assert_eq!(list(serde_json::json!({ "ignoreDate": true, "limit": 3 })), vec![9, 8, 7]);
        assert_eq!(list(serde_json::json!({ "ignoreDate": true, "limit": 50 })), vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
        // Both bounds include their millisecond.
        let within = serde_json::json!({ "from": base + 2_000, "to": base + 6_000, "limit": 3 });
        assert_eq!(list(within), vec![6, 5, 4]);
        let within = serde_json::json!({ "from": base + 2_000, "to": base + 6_000, "limit": 50 });
        assert_eq!(list(within), vec![6, 5, 4, 3, 2]);
        assert_eq!(list(serde_json::json!({ "to": base + 1_000, "limit": 50 })), vec![1, 0]);
        // ignoreDate wins over the dates.
        assert_eq!(list(serde_json::json!({ "ignoreDate": true, "from": base + 2_000, "to": base + 6_000, "limit": 2 })), vec![9, 8]);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A caller that asks for previews gets each result's text and snippet cut to that many
    /// characters (never inside a character), in a listing and in a search; the stored rows stay
    /// whole, and without the parameter (or with one that isn't a positive integer) every result is
    /// whole, as before.
    #[test]
    fn memory_search_cuts_results_to_the_preview_asked_for() {
        let (dir, mut conn) = temp_memory_db("preview");
        let long = format!("[USER]: 日本語 plan {}\n\n[AGENT]: done", "word ".repeat(2_000));
        let now = chrono::Utc::now().timestamp_millis();
        // Every other field longer than the cut asked for below, so a cut reaching one shows.
        let long_id = format!("turn:long:{}", "x".repeat(40));
        let long_session = format!("persistent_chat:{}", "s".repeat(40));
        let rows = [
            serde_json::json!({
                "memId": long_id, "role": "session", "content": long,
                "sessionId": long_session, "turnIndex": 0, "dateMs": now,
            }),
            // Shorter than every cut asked for below: returned whole.
            serde_json::json!({
                "memId": "turn:short", "role": "session", "content": "plan ok",
                "sessionId": "persistent_chat", "turnIndex": 1, "dateMs": now - 1_000,
            }),
        ];
        memory_index_batch(&mut conn, &rows, None).unwrap();
        let synonyms = SynonymLookup::new();
        let search = |q: &str, extra: Value| {
            let mut params = serde_json::json!({ "ignoreDate": true, "limit": 10 });
            for (k, v) in extra.as_object().unwrap() {
                params[k] = v.clone();
            }
            memory_search(&conn, q, &params, &synonyms, None).unwrap()
        };
        // 10 characters: "[USER]: 日本" ends inside the multi-byte word, on a character.
        for q in ["", "plan"] {
            let whole = search(q, serde_json::json!({}));
            let cut = search(q, serde_json::json!({ "previewChars": 10 }));
            assert_eq!(cut.len(), 2, "{q:?}");
            assert_eq!(whole.len(), 2, "{q:?}");
            let long_cut = cut.iter().find(|r| r["memId"] == long_id.as_str()).unwrap();
            let long_whole = whole.iter().find(|r| r["memId"] == long_id.as_str()).unwrap();
            assert_eq!(long_cut["content"], "[USER]: 日本", "{q:?}");
            assert_eq!(long_whole["content"], long.as_str(), "{q:?}");
            if let Some(snippet) = long_whole["snippet"].as_str() {
                assert_eq!(long_cut["snippet"], snippet.chars().take(10).collect::<String>().as_str(), "{q:?}");
            }
            let short_cut = cut.iter().find(|r| r["memId"] == "turn:short").unwrap();
            let short_whole = whole.iter().find(|r| r["memId"] == "turn:short").unwrap();
            assert_eq!(short_cut["content"], "plan ok", "{q:?}");
            // Only content and snippet differ, and only where they were longer than the cut.
            for (c, w) in cut.iter().zip(whole.iter()) {
                let mut c = c.clone();
                let mut w = w.clone();
                for key in ["content", "snippet"] {
                    c.as_object_mut().unwrap().remove(key);
                    w.as_object_mut().unwrap().remove(key);
                }
                assert_eq!(c, w, "{q:?}");
            }
            assert_eq!(short_cut, short_whole, "{q:?}");
            assert_eq!(long_cut["sessionId"], long_session.as_str());
            for extra in [serde_json::json!({ "previewChars": 0 }), serde_json::json!({ "previewChars": -5 }), serde_json::json!({ "previewChars": "10" })] {
                assert_eq!(search(q, extra.clone()), whole, "{q:?} {extra}");
            }
        }
        // The snippet of a match is cut too.
        let snippet_of = |results: Vec<Value>| results.into_iter().find(|r| r["memId"] == long_id.as_str()).unwrap()["snippet"].as_str().unwrap().to_string();
        let snippet_whole = snippet_of(search("plan", serde_json::json!({})));
        assert!(snippet_whole.chars().count() > 3);
        let snippet_cut = snippet_of(search("plan", serde_json::json!({ "previewChars": 3 })));
        assert_eq!(snippet_cut, snippet_whole.chars().take(3).collect::<String>());
        // Stored whole.
        let stored: String = conn.query_row("SELECT content FROM memory_fts WHERE memId = ?1", [&long_id], |r| r.get(0)).unwrap();
        assert_eq!(stored, long);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// What a write landing during a search does, at its `at`-th VM step (counted from 1): the
    /// exchange the search finds by meaning is removed and another, outside the dates, is kept in its
    /// rowid. `at` 0 only counts the steps.
    struct WriteDuringSearch {
        writer: Connection,
        engine: *const EmbeddingEngine,
        at: usize,
        steps: usize,
        written: bool,
    }

    extern "C" fn write_during_search(state: *mut std::os::raw::c_void) -> std::os::raw::c_int {
        let state = unsafe { &mut *(state as *mut WriteDuringSearch) };
        state.steps += 1;
        if state.steps == state.at {
            state.written = true;
            let engine = unsafe { &*state.engine };
            let replacement = serde_json::json!({ "memId": "replacement", "role": "session", "sessionId": "persistent_chat",
                "content": "A completely unrelated exchange", "dateMs": chrono::Utc::now().timestamp_millis() + 86_400_000, "turnIndex": 1 });
            assert_eq!(memory_remove_batch(&mut state.writer, &[serde_json::json!("found")]).unwrap(), 1);
            memory_index_batch(&mut state.writer, &[replacement], Some(engine)).unwrap();
        }
        0
    }

    /// The embedding model from its usual folder (not downloaded here); without it, a test that
    /// needs it is skipped.
    pub(crate) fn embedding_engine() -> Option<EmbeddingEngine> {
        let model = crate::embeddings::download::model_dir(None).unwrap();
        let engine = EmbeddingEngine::load(&model).ok();
        if engine.is_none() {
            eprintln!("skipped: no embedding model in {}", model.display());
        }
        engine
    }

    /// A match found by meaning alone (no keyword in it) is cut to the preview asked for too.
    #[test]
    fn memory_search_cuts_a_match_by_meaning_to_the_preview_asked_for() {
        let Some(engine) = embedding_engine() else { return };
        let (dir, mut conn) = temp_memory_db("meaning-preview");
        let content = "Here is the vacation itinerary";
        let row = serde_json::json!({ "memId": "found", "role": "session", "sessionId": "persistent_chat",
            "content": content, "dateMs": chrono::Utc::now().timestamp_millis(), "turnIndex": 0 });
        memory_index_batch(&mut conn, &[row], Some(&engine)).unwrap();
        let search = |params: Value| memory_search(&conn, "plan the vacation itinerary", &params, &SynonymLookup::new(), Some(&engine)).unwrap();
        let whole = search(serde_json::json!({ "ignoreDate": true }));
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0]["snippet"], "", "found by meaning, not by its words");
        assert_eq!(whole[0]["content"], content);
        let cut = search(serde_json::json!({ "ignoreDate": true, "previewChars": 10 }));
        assert_eq!(cut.len(), 1);
        assert_eq!(cut[0]["content"], &content[..10]);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A search answers from the memory as it was when it began: wherever in it a write lands, it
    /// gives the exchange it found, never one that took its rowid meanwhile, outside the dates. With
    /// dates or without: a search without them could give that other exchange the same way.
    #[test]
    fn memory_search_answers_from_the_memory_as_it_began() {
        let Some(engine) = embedding_engine() else { return };
        let (dir, mut conn) = temp_memory_db("search-snapshot");
        let synonyms = SynonymLookup::new();
        let now = chrono::Utc::now().timestamp_millis();
        let found = serde_json::json!({ "memId": "found", "role": "session", "sessionId": "persistent_chat",
            "content": "Here is the vacation itinerary", "dateMs": now, "turnIndex": 0 });
        let query = "plan the vacation itinerary";
        let ids = |rows: &[Value]| rows.iter().map(|r| r["memId"].as_str().unwrap().to_string()).collect::<Vec<_>>();
        let mut state = WriteDuringSearch {
            writer: open_memory_writer_connection(&dir.join("memory.db")).unwrap(),
            engine: &engine,
            at: 0,
            steps: 0,
            written: false,
        };
        let search_with_write_at = |conn: &Connection, state: &mut WriteDuringSearch, params: &Value, at: usize| {
            (state.at, state.steps, state.written) = (at, 0, false);
            unsafe {
                rusqlite::ffi::sqlite3_progress_handler(conn.handle(), 1, Some(write_during_search), state as *mut WriteDuringSearch as *mut std::os::raw::c_void);
            }
            let rows = memory_search(conn, query, params, &synonyms, Some(&engine)).unwrap();
            unsafe { rusqlite::ffi::sqlite3_progress_handler(conn.handle(), 0, None, std::ptr::null_mut()) };
            rows
        };
        let listing = serde_json::json!({ "ignoreDate": true });

        memory_index_batch(&mut conn, &[found.clone()], Some(&engine)).unwrap();
        for params in [serde_json::json!({ "from": now - 1, "to": now + 1, "limit": 10 }), serde_json::json!({ "ignoreDate": true, "limit": 10 })] {
            // Found by meaning only (no keyword match), so the vector leg answers it.
            let before = search_with_write_at(&conn, &mut state, &params, 0);
            assert_eq!(ids(&before), ["found"], "{params}");
            assert_eq!(before[0]["snippet"], "");
            // How many steps a search takes, its query's embedding kept from the first.
            search_with_write_at(&conn, &mut state, &params, 0);
            let steps = state.steps;
            assert!(steps > 100, "a search took {steps} steps");

            // From past the transaction's start to the search's last step.
            for at in (20..=steps).step_by((steps / 40).max(1)) {
                let during = search_with_write_at(&conn, &mut state, &params, at);
                assert!(state.written, "the write landed at step {at} of {steps}");
                assert_eq!(ids(&during), ["found"], "with the write at step {at} of {steps}, {params}");
                assert_eq!(during[0]["dateMs"], now);
                // The write took the found exchange's rowid, and the next request reads it.
                let reused: i64 = conn.query_row("SELECT rowid FROM memory_ids WHERE memId = 'replacement'", [], |r| r.get(0)).unwrap();
                assert_eq!(reused, 1);
                assert_eq!(ids(&memory_search(&conn, "", &listing, &synonyms, Some(&engine)).unwrap()), ["replacement"]);
                // As it was, for the next step.
                assert_eq!(memory_remove_batch(&mut conn, &[serde_json::json!("replacement")]).unwrap(), 1);
                memory_index_batch(&mut conn, &[found.clone()], Some(&engine)).unwrap();
            }
        }
        drop(state);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A search with one date bound keeps a match inside it found by meaning alone, scored as
    /// without dates: the vector candidates are filtered by either bound on its own.
    #[test]
    fn memory_search_with_one_date_scores_a_match_by_meaning_as_without() {
        let Some(engine) = embedding_engine() else { return };
        let (dir, mut conn) = temp_memory_db("one-date-meaning");
        let now = chrono::Utc::now().timestamp_millis();
        let row = serde_json::json!({ "memId": "found", "role": "session", "sessionId": "persistent_chat",
            "content": "Here is the vacation itinerary", "dateMs": now, "turnIndex": 0 });
        memory_index_batch(&mut conn, &[row], Some(&engine)).unwrap();
        let search = |params: Value| memory_search(&conn, "plan the vacation itinerary", &params, &SynonymLookup::new(), Some(&engine)).unwrap();
        let whole = search(serde_json::json!({ "ignoreDate": true }));
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0]["snippet"], "", "found by meaning, not by its words");
        for params in [serde_json::json!({ "from": now - 1 }), serde_json::json!({ "to": now + 1 })] {
            assert_eq!(search(params.clone()), whole, "{params}");
        }
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A hybrid memory search asks sqlite-vec for `limit` x CANDIDATE_MULTIPLIER nearest vectors,
    /// which it refuses past VEC_K_MAX; the `limit` is capped at SEARCH_LIMIT_MAX to keep within it
    /// (TabMail Voice asks for 2,000). At any `limit` asked, a match by meaning alone is still
    /// found: the search stays hybrid.
    #[test]
    fn memory_search_with_a_large_limit_finds_a_match_by_meaning() {
        let Some(engine) = embedding_engine() else { return };
        let (dir, mut conn) = temp_memory_db("large-limit");
        let row = serde_json::json!({ "memId": "found", "role": "session", "sessionId": "persistent_chat",
            "content": "Here is the vacation itinerary", "dateMs": chrono::Utc::now().timestamp_millis(), "turnIndex": 0 });
        memory_index_batch(&mut conn, &[row], Some(&engine)).unwrap();
        let most = config::hybrid::SEARCH_LIMIT_MAX;
        for limit in [10, most, most + 1, 2_000, 4_000] {
            let params = serde_json::json!({ "ignoreDate": true, "limit": limit });
            let results = memory_search(&conn, "plan the vacation itinerary", &params, &SynonymLookup::new(), Some(&engine)).unwrap();
            assert_eq!(results.len(), 1, "at limit {limit}");
            assert_eq!(results[0]["memId"], "found");
            assert_eq!(results[0]["snippet"], "", "found by meaning, not by its words");
        }
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Refuses every read of memory_meta, counting them.
    extern "C" fn deny_meta_reads(
        denied: *mut std::os::raw::c_void,
        action: std::os::raw::c_int,
        table: *const std::os::raw::c_char,
        _column: *const std::os::raw::c_char,
        _db: *const std::os::raw::c_char,
        _trigger: *const std::os::raw::c_char,
    ) -> std::os::raw::c_int {
        if action == rusqlite::ffi::SQLITE_READ && !table.is_null() && unsafe { std::ffi::CStr::from_ptr(table) }.to_bytes() == b"memory_meta" {
            unsafe { *(denied as *mut usize) += 1 };
            return rusqlite::ffi::SQLITE_DENY;
        }
        rusqlite::ffi::SQLITE_OK
    }

    /// A search that fails part way ends its read transaction: the next request on the connection
    /// is answered, whole.
    #[test]
    fn memory_search_after_a_failed_one_is_answered() {
        let (dir, mut conn) = temp_memory_db("failed-search");
        let content = "[USER]: a question\n\n[AGENT]: an answer";
        let row = serde_json::json!({ "memId": "turn:a", "role": "session", "sessionId": "persistent_chat",
            "content": content, "dateMs": chrono::Utc::now().timestamp_millis(), "turnIndex": 0 });
        memory_index_batch(&mut conn, &[row], None).unwrap();
        let params = serde_json::json!({ "ignoreDate": true });
        let mut denied = 0usize;
        unsafe { rusqlite::ffi::sqlite3_set_authorizer(conn.handle(), Some(deny_meta_reads), &mut denied as *mut usize as *mut std::os::raw::c_void) };
        let failed = memory_search(&conn, "", &params, &SynonymLookup::new(), None);
        unsafe { rusqlite::ffi::sqlite3_set_authorizer(conn.handle(), None, std::ptr::null_mut()) };
        assert!(denied > 0 && failed.is_err(), "the search failed on a read inside it");
        let next = memory_search(&conn, "", &params, &SynonymLookup::new(), None).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0]["memId"], "turn:a");
        assert_eq!(next[0]["content"], content);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A page of the listing reads only its own rows' content, not the whole memory's: three rows
    /// of a hundred long ones read a few pages from disk, well under the hundred they take.
    #[test]
    fn memory_listing_reads_only_its_pages_content() {
        let (dir, mut conn) = temp_memory_db("listing-reads");
        let now = chrono::Utc::now().timestamp_millis();
        let rows: Vec<Value> = (0..100)
            .map(|i| serde_json::json!({ "memId": format!("turn:{i}"), "role": "session", "sessionId": "persistent_chat",
                "content": format!("exchange {i} {}", "word ".repeat(1000)), "dateMs": now + i, "turnIndex": i }))
            .collect();
        memory_index_batch(&mut conn, &rows, None).unwrap();
        drop(conn);
        // A cold reader with a tiny cache and no memory map, so each page it needs is a miss.
        let conn = open_read_only_memory_connection(&dir.join("memory.db")).unwrap();
        conn.execute_batch("PRAGMA mmap_size = 0; PRAGMA cache_size = 8;").unwrap();
        let misses = || {
            let (mut current, mut high) = (0, 0);
            assert_eq!(unsafe { rusqlite::ffi::sqlite3_db_status(conn.handle(), rusqlite::ffi::SQLITE_DBSTATUS_CACHE_MISS, &mut current, &mut high, 0) }, rusqlite::ffi::SQLITE_OK);
            current
        };
        let before = misses();
        let page = memory_search(&conn, "", &serde_json::json!({ "ignoreDate": true, "limit": 3 }), &SynonymLookup::new(), None).unwrap();
        let read = misses() - before;
        assert_eq!(page.iter().map(|r| r["memId"].as_str().unwrap()).collect::<Vec<_>>(), ["turn:99", "turn:98", "turn:97"]);
        assert_eq!(page[0]["content"], rows[99]["content"]);
        assert!(read > 0, "the cold reader read from disk");
        assert!(read < 30, "a three-row page read {read} pages");
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A memory search with a query returns at most SEARCH_LIMIT_MAX results, whatever `limit` it is
    /// given; a listing (no query) returns the page it asks for.
    #[test]
    fn memory_search_returns_at_most_search_limit_max_results() {
        let (dir, mut conn) = temp_memory_db("limit-max");
        let most = config::hybrid::SEARCH_LIMIT_MAX;
        let now = chrono::Utc::now().timestamp_millis();
        let rows: Vec<Value> = (0..most + 76)
            .map(|i| serde_json::json!({ "memId": format!("turn:{i}"), "role": "session", "sessionId": "persistent_chat",
                "content": "the quarterly budget", "dateMs": now + i, "turnIndex": i }))
            .collect();
        memory_index_batch(&mut conn, &rows, None).unwrap();
        for limit in [10, most, most + 1, 4_000] {
            let params = serde_json::json!({ "ignoreDate": true, "limit": limit });
            let results = memory_search(&conn, "budget", &params, &SynonymLookup::new(), None).unwrap();
            assert_eq!(results.len() as i64, limit.min(most), "at limit {limit}");
        }
        let listing = memory_search(&conn, "", &serde_json::json!({ "ignoreDate": true, "limit": most + 76 }), &SynonymLookup::new(), None).unwrap();
        assert_eq!(listing.len() as i64, most + 76);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A memory search with dates whose vector candidates are all outside them answers by keyword,
    /// as it does with no vectors at all: every keyword match within the dates, however weak.
    #[test]
    fn memory_search_with_no_vector_within_the_dates_answers_by_keyword() {
        let Some(engine) = embedding_engine() else { return };
        let (dir, mut conn) = temp_memory_db("no-vector-within");
        let day = 86_400_000;
        let now = chrono::Utc::now().timestamp_millis();
        let turn = |i: i64, content: &str, date_ms: i64| serde_json::json!({ "memId": format!("turn:{i}"), "role": "session",
            "sessionId": "persistent_chat", "content": content, "dateMs": date_ms, "turnIndex": i });
        // Within the dates, no vectors yet; "budget" in each, so it weighs little by keyword.
        let within: Vec<Value> = (0..20).map(|i| turn(i, &format!("the quarterly budget, item {i}"), now - i)).collect();
        memory_index_batch(&mut conn, &within, None).unwrap();
        let outside: Vec<Value> = (20..25).map(|i| turn(i, "rain tomorrow", now - (30 + i) * day)).collect();
        memory_index_batch(&mut conn, &outside, Some(&engine)).unwrap();
        let params = serde_json::json!({ "from": now - day, "to": now + day, "limit": 100 });
        let mut found: Vec<String> = memory_search(&conn, "budget", &params, &SynonymLookup::new(), Some(&engine)).unwrap()
            .iter().map(|r| r["memId"].as_str().unwrap().to_string()).collect();
        found.sort();
        let mut expected: Vec<String> = (0..20).map(|i| format!("turn:{i}")).collect();
        expected.sort();
        assert_eq!(found, expected);
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }
}
