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
        // Migrate: add vector tables if missing (pre-v0.7.0 databases)
        ensure_memory_vector_tables(&conn)?;
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
pub fn memory_search(
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
    let vec_candidates = super::db::search_vec_candidates(conn, "memory_vec", &query_blob, candidate_limit)
        .unwrap_or_default(); // empty vec table during rebuild → graceful empty

    // Fall back to FTS-only when vec table is empty (e.g., during embedding rebuild).
    if vec_candidates.is_empty() {
        log::info!("No memory vector candidates (vec table may be empty), falling back to FTS-only search");
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
            // Vector-only result
            if let Some(meta) = fetch_memory_meta(conn, hr.rowid)? {
                if let Some(from) = from_ts {
                    if meta.date_ms < from {
                        continue;
                    }
                }
                if let Some(to) = to_ts {
                    if meta.date_ms > to {
                        continue;
                    }
                }
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

    let mut sql = r#"
        SELECT fts.memId, fts.role, fts.content, fts.sessionId, meta.dateMs
        FROM memory_fts fts
        JOIN memory_meta meta ON fts.rowid = meta.rowid
        WHERE 1=1
    "#
    .to_string();

    let mut bind: Vec<rusqlite::types::Value> = vec![];

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

    sql.push_str(" ORDER BY meta.dateMs DESC LIMIT ?");
    bind.push(rusqlite::types::Value::from(limit));

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
    Ok(removed)
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
