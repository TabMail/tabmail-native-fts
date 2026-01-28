use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use chrono::{DateTime, Local, TimeZone};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde_json::Value;

use crate::{config, fts::query::build_fts_match, fts::synonyms::SynonymLookup};

pub struct DbState {
    // Email FTS database
    pub db_path: Option<PathBuf>,
    pub conn: Option<Connection>,
    pub synonyms: SynonymLookup,
    pub should_exit: bool,
    // Memory database (separate from email FTS)
    pub memory_db_path: Option<PathBuf>,
    pub memory_conn: Option<Connection>,
}

impl DbState {
    pub fn new() -> Self {
        Self {
            db_path: None,
            conn: None,
            synonyms: SynonymLookup::new(),
            should_exit: false,
            memory_db_path: None,
            memory_conn: None,
        }
    }
}

pub fn init_database(conn: &Connection) -> anyhow::Result<()> {
    log::info!("Initializing database schema (matching old WASM implementation)");

    // IMPORTANT:
    // SQLite PRAGMA statements do NOT reliably accept parameters, so we must interpolate.
    // Numeric values still come from config constants (repo rule: no scattered magic numbers).
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

    // Create schema.
    conn.execute_batch(&format!(
        r#"
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            msgId,
            subject, from_, to_, cc, bcc, body,
            tokenize = "{tokenize}",
            prefix = '{prefix}'
        );

        CREATE TABLE IF NOT EXISTS message_meta (
            rowid INTEGER PRIMARY KEY,
            dateMs INTEGER NOT NULL,
            hasAttachments INTEGER NOT NULL,
            parsedIcsAttachments TEXT
        );

        CREATE TABLE IF NOT EXISTS message_ids (
            msgId TEXT PRIMARY KEY
        );
        "#,
        tokenize = config::sqlite::FTS_TOKENIZE,
        prefix = config::sqlite::FTS_PREFIXES
    ))?;

    // FTS5 automerge settings.
    conn.execute(
        "INSERT INTO messages_fts(messages_fts, rank) VALUES('automerge', 2)",
        [],
    )?;
    conn.execute(
        "INSERT INTO messages_fts(messages_fts, rank) VALUES('usermerge', 2)",
        [],
    )?;

    log::info!("Database schema initialized (3 tables: messages_fts, message_meta, message_ids)");
    Ok(())
}

pub fn ensure_fts5_available(conn: &Connection) -> anyhow::Result<()> {
    // No fallbacks: fail loudly if FTS5 isn't present.
    // This also gives us a high-signal log for customer debug reports.
    match conn.execute(
        r#"CREATE VIRTUAL TABLE IF NOT EXISTS __tabmail_fts5_probe USING fts5(x)"#,
        [],
    ) {
        Ok(_) => {
            let _ = conn.execute("DROP TABLE IF EXISTS __tabmail_fts5_probe", []);
            // Also log compile options to confirm FTS5 is compiled in.
            // (This is a debugging signal, not a fallback.)
            if let Ok(mut stmt) = conn.prepare("PRAGMA compile_options") {
                if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    let mut has_fts5 = false;
                    for opt in rows.flatten() {
                        if opt == "ENABLE_FTS5" || opt == "SQLITE_ENABLE_FTS5" {
                            has_fts5 = true;
                        }
                    }
                    log::info!("SQLite compile_options indicates FTS5 enabled: {}", has_fts5);
                }
            }
            log::info!("✅ SQLite FTS5 probe succeeded");
            Ok(())
        }
        Err(e) => {
            bail!("SQLite FTS5 is not available in this build: {e}");
        }
    }
}

pub fn open_or_create_db(profile_dir: &Path) -> anyhow::Result<(PathBuf, Connection)> {
    let fts_dir = profile_dir.join("tabmail_fts");
    std::fs::create_dir_all(&fts_dir)
        .with_context(|| format!("failed to create fts dir {}", fts_dir.display()))?;
    let db_path = fts_dir.join("fts.db");

    log::info!("Initializing FTS database");
    log::info!("  Profile: {}", profile_dir.display());
    log::info!("  FTS Dir: {}", fts_dir.display());
    log::info!("  DB Path: {}", db_path.display());

    let conn = Connection::open(&db_path).with_context(|| format!("open db {}", db_path.display()))?;
    ensure_fts5_available(&conn)?;

    // Does schema exist?
    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='messages_fts'",
            [],
            |r| r.get(0),
        )
        .optional()?;

    if exists.is_none() {
        log::info!("Creating new FTS database schema");
        init_database(&conn)?;
    } else {
        log::info!("Using existing FTS database schema");
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM messages_fts", [], |r| r.get(0))?;
    log::info!("Database initialized: {} documents indexed", count);

    Ok((db_path, conn))
}

pub fn db_count(conn: &Connection) -> anyhow::Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM messages_fts", [], |r| r.get(0))?)
}

pub fn index_batch(conn: &mut Connection, rows: &[Value]) -> anyhow::Result<(i64, i64)> {
    log::info!("Indexing batch of {} messages", rows.len());

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let mut inserted: i64 = 0;
    let mut skipped_duplicates: i64 = 0;

    for row in rows {
        let Some(msg_id_val) = row.get("msgId").and_then(|v| v.as_str()) else { continue };
        if msg_id_val.is_empty() {
            continue;
        }

        let changed = tx.execute(
            "INSERT OR IGNORE INTO message_ids (msgId) VALUES (?1)",
            params![msg_id_val],
        )?;
        if changed == 0 {
            skipped_duplicates += 1;
            log::debug!("Skipping duplicate msgId: {}...", truncate_for_log(msg_id_val));
            continue;
        }

        let row_id: i64 = tx.query_row(
            "SELECT rowid FROM message_ids WHERE msgId = ?1",
            params![msg_id_val],
            |r| r.get(0),
        )?;

        let subject = row.get("subject").and_then(|v| v.as_str()).unwrap_or("");
        let from_ = row
            .get("from_")
            .and_then(|v| v.as_str())
            .or_else(|| row.get("from").and_then(|v| v.as_str()))
            .or_else(|| row.get("author").and_then(|v| v.as_str()))
            .unwrap_or("");
        let to_ = row
            .get("to_")
            .and_then(|v| v.as_str())
            .or_else(|| row.get("to").and_then(|v| v.as_str()))
            .unwrap_or("");
        let cc = row.get("cc").and_then(|v| v.as_str()).unwrap_or("");
        let bcc = row.get("bcc").and_then(|v| v.as_str()).unwrap_or("");
        let body = row.get("body").and_then(|v| v.as_str()).unwrap_or("");

        tx.execute(
            r#"
            INSERT INTO messages_fts (rowid, msgId, subject, from_, to_, cc, bcc, body)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![row_id, msg_id_val, subject, from_, to_, cc, bcc, body],
        )?;

        let date_ms = row.get("dateMs").and_then(|v| v.as_i64()).unwrap_or(0);
        let has_attachments = row
            .get("hasAttachments")
            .and_then(|v| v.as_bool())
            .map(|b| if b { 1 } else { 0 })
            .unwrap_or(0);
        let parsed_ics = row
            .get("parsedIcsAttachments")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        tx.execute(
            r#"
            INSERT INTO message_meta (rowid, dateMs, hasAttachments, parsedIcsAttachments)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![row_id, date_ms, has_attachments, parsed_ics],
        )?;

        inserted += 1;
    }

    tx.commit()?;
    if skipped_duplicates > 0 {
        log::info!(
            "Indexed {} messages successfully, {} duplicates skipped",
            inserted,
            skipped_duplicates
        );
    } else {
        log::info!("Indexed {} messages successfully", inserted);
    }

    Ok((inserted, skipped_duplicates))
}

pub fn parse_date_param(v: &Value) -> anyhow::Result<Option<i64>> {
    if v.is_null() {
        return Ok(None);
    }
    if let Some(i) = v.as_i64() {
        return Ok(Some(i));
    }
    if let Some(f) = v.as_f64() {
        return Ok(Some(f as i64));
    }
    let Some(s) = v.as_str() else {
        return Ok(None);
    };
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }

    let s = if s.ends_with('Z') {
        format!("{}+00:00", &s[..(s.len() - 1)])
    } else {
        s.to_string()
    };

    // Try ISO first.
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Ok(Some(dt.timestamp_millis()));
    }

    // Fallback: numeric string
    if let Ok(f) = s.parse::<f64>() {
        return Ok(Some(f as i64));
    }

    bail!("Invalid date format: '{}'", v);
}

pub fn search(conn: &Connection, q: &str, params: &Value, synonyms: &SynonymLookup) -> anyhow::Result<Vec<Value>> {
    let query = q.trim();
    if query.is_empty() {
        return Ok(vec![]);
    }

    let fts_query = build_fts_match(Some(query), true, synonyms);
    log::info!(
        "Query transformation (with synonyms): \"{}\" -> \"{}\"",
        query,
        fts_query
    );
    if fts_query.is_empty() {
        log::info!("Empty FTS query after normalization (e.g. only stop words or wildcards provided)");
        return Ok(vec![]);
    }

    let ignore_date = params.get("ignoreDate").and_then(|v| v.as_bool()).unwrap_or(false);
    let limit = params
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(config::sqlite::SEARCH_DEFAULT_LIMIT);

    let mut sql = format!(
        r#"
        SELECT
            fts.msgId, fts.from_, fts.subject, meta.dateMs, meta.hasAttachments,
            snippet(messages_fts, -1, '[', ']', '…', {snippet_tokens}) AS snippet,
            bm25(messages_fts, 0.0, 5.0, 3.0, 2.0, 1.0, 1.0, 1.0) AS rank
        FROM messages_fts fts
        JOIN message_meta meta ON fts.rowid = meta.rowid
        WHERE messages_fts MATCH ?1
        "#,
        snippet_tokens = config::sqlite::SEARCH_SNIPPET_TOKENS
    );

    let mut bind: Vec<rusqlite::types::Value> = vec![rusqlite::types::Value::from(fts_query.clone())];

    if !ignore_date {
        if let Some(from_v) = params.get("from") {
            if let Some(ts) = parse_date_param(from_v)? {
                sql.push_str(" AND meta.dateMs >= ?");
                bind.push(rusqlite::types::Value::from(ts));
            }
        }
        if let Some(to_v) = params.get("to") {
            if let Some(ts) = parse_date_param(to_v)? {
                sql.push_str(" AND meta.dateMs <= ?");
                bind.push(rusqlite::types::Value::from(ts));
            }
        }
    }

    sql.push_str(" ORDER BY meta.dateMs DESC, rank ASC LIMIT ?");
    bind.push(rusqlite::types::Value::from(limit));

    log::info!("Search SQL: {}", sql);
    log::info!("Search params: {:?}", bind);

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(bind.iter()), |r| row_to_search_result(r))?;

    let mut results: Vec<Value> = vec![];
    for r in rows {
        results.push(r?);
    }

    log::info!("Search completed: found {} results", results.len());
    Ok(results)
}

fn row_to_search_result(r: &Row<'_>) -> rusqlite::Result<Value> {
    let unique_id: String = r.get(0)?;
    let author: String = r.get(1)?;
    let subject: String = r.get(2)?;
    let date_ms: i64 = r.get(3)?;
    let has_attachments: i64 = r.get(4)?;
    let snippet: String = r.get(5)?;
    let rank: f64 = r.get(6)?;

    Ok(serde_json::json!({
        "uniqueId": unique_id,
        "author": author,
        "subject": subject,
        "dateMs": date_ms,
        "hasAttachments": has_attachments != 0,
        "snippet": snippet,
        "rank": rank
    }))
}

pub fn clear_rebuild(state: &mut DbState) -> anyhow::Result<()> {
    log::info!("Clearing all indexes by deleting database file (rebuild from scratch)");
    let db_path = state
        .db_path
        .clone()
        .context("DB not initialized (missing db_path)")?;

    // Close connection first.
    state.conn.take();
    log::info!("Database connection closed");

    // Delete db + wal/shm
    delete_file_if_exists(&db_path)?;
    delete_file_if_exists(&PathBuf::from(format!("{}-wal", db_path.display())))?;
    delete_file_if_exists(&PathBuf::from(format!("{}-shm", db_path.display())))?;

    log::info!("Recreating database...");
    let conn = Connection::open(&db_path)?;
    ensure_fts5_available(&conn)?;
    init_database(&conn)?;
    state.conn = Some(conn);
    log::info!("Database recreated and initialized successfully");

    Ok(())
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

pub fn optimize(conn: &Connection) -> anyhow::Result<()> {
    log::info!("Optimizing FTS index");
    conn.execute("INSERT INTO messages_fts(messages_fts) VALUES('optimize')", [])?;
    Ok(())
}

pub fn filter_new_messages(conn: &Connection, rows: &[Value]) -> anyhow::Result<Value> {
    if rows.is_empty() {
        return Ok(serde_json::json!({
            "ok": true,
            "newMsgIds": [],
            "totalChecked": 0,
            "newCount": 0,
            "skippedCount": 0
        }));
    }

    log::info!("Filtering {} messages to find new ones", rows.len());
    let mut new_msg_ids: Vec<String> = vec![];
    let mut skipped: i64 = 0;

    for row in rows {
        let Some(msg_id_val) = row.get("msgId").and_then(|v| v.as_str()) else { continue };
        if msg_id_val.is_empty() {
            continue;
        }

        let exists: Option<String> = conn
            .query_row(
                "SELECT msgId FROM message_ids WHERE msgId = ?1",
                params![msg_id_val],
                |r| r.get(0),
            )
            .optional()?;

        if exists.is_none() {
            new_msg_ids.push(msg_id_val.to_string());
        } else {
            skipped += 1;
        }
    }

    log::info!(
        "Filtered {} new messages out of {} total ({} already indexed)",
        new_msg_ids.len(),
        rows.len(),
        skipped
    );

    Ok(serde_json::json!({
        "ok": true,
        "newMsgIds": new_msg_ids,
        "totalChecked": rows.len(),
        "newCount": new_msg_ids.len(),
        "skippedCount": skipped
    }))
}

pub fn remove_batch(conn: &mut Connection, ids: &[Value]) -> anyhow::Result<i64> {
    if ids.is_empty() {
        return Ok(0);
    }

    let ids: Vec<String> = ids
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    log::info!("Removing {} messages from index", ids.len());

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut removed: i64 = 0;

    for msg_id_val in ids {
        if msg_id_val.is_empty() {
            continue;
        }
        let row_id: Option<i64> = tx
            .query_row(
                "SELECT rowid FROM message_ids WHERE msgId = ?1",
                params![msg_id_val],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(row_id) = row_id {
            tx.execute("DELETE FROM messages_fts WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM message_meta WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM message_ids WHERE msgId = ?1", params![msg_id_val])?;
            removed += 1;
        }
    }

    tx.commit()?;
    log::info!("Removed {} messages", removed);
    Ok(removed)
}

pub fn get_message_by_msgid(conn: &Connection, msg_id: &str) -> anyhow::Result<Option<Value>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            f.msgId, f.body, f.subject, f.from_, f.to_, f.cc, f.bcc,
            m.hasAttachments, m.parsedIcsAttachments, m.dateMs
        FROM messages_fts f
        JOIN message_meta m ON f.rowid = m.rowid
        WHERE f.msgId = ?1
        LIMIT 1
        "#,
    )?;

    let row: Option<Value> = stmt
        .query_row(params![msg_id], |r| {
            let msg_id: String = r.get(0)?;
            let body: String = r.get(1)?;
            let subject: String = r.get(2)?;
            let from_: String = r.get(3)?;
            let to_: String = r.get(4)?;
            let cc: String = r.get(5)?;
            let bcc: String = r.get(6)?;
            let has_attachments: i64 = r.get(7)?;
            let parsed_ics: Option<String> = r.get(8)?;
            let date_ms: i64 = r.get(9)?;

            Ok(serde_json::json!({
                "msgId": msg_id,
                "body": body,
                "subject": subject,
                "from_": from_,
                "to_": to_,
                "cc": cc,
                "bcc": bcc,
                "hasAttachments": has_attachments,
                "parsedIcsAttachments": parsed_ics.unwrap_or_default(),
                "dateMs": date_ms
            }))
        })
        .optional()?;

    Ok(row)
}

pub fn query_by_date_range(conn: &Connection, from_v: &Value, to_v: &Value, limit: i64) -> anyhow::Result<Vec<Value>> {
    let Some(from_ts) = parse_date_param(from_v)? else { bail!("from and to parameters are required") };
    let Some(to_ts) = parse_date_param(to_v)? else { bail!("from and to parameters are required") };

    log::info!(
        "Querying messages from {} to {}, limit {}",
        from_ts,
        to_ts,
        limit
    );

    let mut stmt = conn.prepare(
        r#"
        SELECT f.msgId, f.subject, m.dateMs
        FROM messages_fts f
        JOIN message_meta m ON f.rowid = m.rowid
        WHERE m.dateMs >= ?1 AND m.dateMs <= ?2
        ORDER BY m.dateMs DESC
        LIMIT ?3
        "#,
    )?;

    let mut rows = stmt.query(params![from_ts, to_ts, limit])?;
    let mut out: Vec<Value> = vec![];
    while let Some(r) = rows.next()? {
        let msg_id: String = r.get(0)?;
        let subject: String = r.get(1)?;
        let date_ms: i64 = r.get(2)?;
        let date_str = format_date_iso_like_python(date_ms);
        out.push(serde_json::json!({
            "msgId": msg_id,
            "subject": subject,
            "dateMs": date_ms,
            "dateStr": date_str
        }));
    }

    log::info!("Found {} messages in date range", out.len());
    Ok(out)
}

pub fn debug_sample(conn: &Connection) -> anyhow::Result<Vec<Value>> {
    log::info!("Getting debug sample");
    let mut stmt = conn.prepare(
        r#"
        SELECT f.msgId, f.subject, m.dateMs
        FROM messages_fts f
        JOIN message_meta m ON f.rowid = m.rowid
        ORDER BY m.dateMs DESC
        LIMIT ?1
        "#,
    )?;

    let mut rows = stmt.query(params![config::sqlite::SEARCH_DEBUG_SAMPLE_LIMIT])?;
    let mut out: Vec<Value> = vec![];
    while let Some(r) = rows.next()? {
        let msg_id: String = r.get(0)?;
        let subject: String = r.get(1)?;
        let date_ms: i64 = r.get(2)?;
        out.push(serde_json::json!({
            "msgId": msg_id,
            "subject": subject,
            "dateMs": date_ms
        }));
    }
    Ok(out)
}

fn format_date_iso_like_python(date_ms: i64) -> String {
    if date_ms == 0 {
        return String::new();
    }
    let secs = date_ms as f64 / 1000.0;
    let whole = secs.trunc() as i64;
    let frac = secs - (whole as f64);
    let micros = (frac * 1_000_000.0).round() as u32;

    let dt: DateTime<Local> = Local.timestamp_opt(whole, micros * 1000).single().unwrap_or_else(|| {
        // If local conversion fails, fall back to epoch-based safe value.
        Local.timestamp_opt(0, 0).single().unwrap()
    });

    // Python's datetime.isoformat() for naive local datetime includes microseconds if non-zero.
    if micros == 0 {
        dt.format("%Y-%m-%dT%H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    }
}

fn truncate_for_log(s: &str) -> String {
    // Keep parity with python which logs first 80-ish chars.
    let max = 80usize;
    if s.len() <= max {
        return s.to_string();
    }
    s.chars().take(max).collect()
}


