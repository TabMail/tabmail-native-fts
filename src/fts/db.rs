/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use chrono::{DateTime, Datelike, Local, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;

use crate::{config, embeddings::engine::EmbeddingEngine, fts::query::build_fts_match, fts::synonyms::SynonymLookup};

pub struct DbState {
    // Email FTS database
    pub db_path: Option<PathBuf>,
    pub conn: Option<Connection>,
    pub synonyms: SynonymLookup,
    // Known year shards (e.g., {2023, 2024, 2025})
    pub known_years: HashSet<i32>,
    // Memory database (separate from email FTS)
    pub memory_db_path: Option<PathBuf>,
    pub memory_conn: Option<Connection>,
    // Embedding engine (None if model not available — falls back to FTS-only)
    pub embedding_engine: Option<EmbeddingEngine>,
}

impl DbState {
    pub fn new() -> Self {
        Self {
            db_path: None,
            conn: None,
            synonyms: SynonymLookup::new(),
            known_years: HashSet::new(),
            memory_db_path: None,
            memory_conn: None,
            embedding_engine: None,
        }
    }
}

// =============================================================================
// Year-based FTS sharding helpers
// =============================================================================

fn fts_table_name(year: i32) -> String {
    format!("messages_fts_{}", year)
}

fn year_from_date_ms(date_ms: i64) -> i32 {
    if date_ms <= 0 {
        // dateMs=0 or negative = broken/missing date, use fallback shard
        return config::sqlite::SHARD_MIN_YEAR;
    }
    let secs = date_ms / 1000;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
        .year()
}

fn ensure_shard(conn: &Connection, year: i32, known_years: &mut HashSet<i32>) -> anyhow::Result<()> {
    if known_years.contains(&year) {
        return Ok(());
    }
    let table = fts_table_name(year);
    conn.execute_batch(&format!(
        r#"CREATE VIRTUAL TABLE IF NOT EXISTS {table} USING fts5(
            msgId,
            subject, from_, to_, cc, bcc, body,
            tokenize = "{tokenize}",
            prefix = '{prefix}'
        )"#,
        table = table,
        tokenize = config::sqlite::FTS_TOKENIZE,
        prefix = config::sqlite::FTS_PREFIXES,
    ))?;
    conn.execute(
        &format!("INSERT INTO {table}({table}, rank) VALUES('automerge', ?1)"),
        params![config::sqlite::FTS_AUTOMERGE],
    )?;
    conn.execute(
        &format!("INSERT INTO {table}({table}, rank) VALUES('usermerge', ?1)"),
        params![config::sqlite::FTS_USERMERGE],
    )?;
    known_years.insert(year);
    log::debug!("Created/verified FTS shard: {}", table);
    Ok(())
}

/// Discover existing year shard tables from sqlite_master.
pub fn load_known_years(conn: &Connection) -> anyhow::Result<HashSet<i32>> {
    let mut stmt = conn.prepare(
        r"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'messages\_fts\_%' ESCAPE '\'"
    )?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    let mut years = HashSet::new();
    for name in rows {
        let name = name?;
        if let Some(suffix) = name.strip_prefix("messages_fts_") {
            if let Ok(year) = suffix.parse::<i32>() {
                years.insert(year);
            }
        }
    }
    Ok(years)
}

/// Crash-safe migration from monolithic messages_fts to year-sharded tables.
/// The presence of the monolithic `messages_fts` table means migration is incomplete.
/// Each step is idempotent — a crash at any point is recoverable on next boot.
fn migrate_monolithic_to_shards(conn: &Connection, known_years: &mut HashSet<i32>) -> anyhow::Result<()> {
    log::info!("[migration] Starting monolithic → year-sharded FTS migration");

    // Step 1: Ensure shardYear column exists on message_meta (idempotent)
    let has_shard_year = {
        let mut stmt = conn.prepare("PRAGMA table_info(message_meta)")?;
        let cols: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(1))?
            .filter_map(|r| r.ok())
            .collect();
        cols.contains(&"shardYear".to_string())
    };
    if !has_shard_year {
        log::info!("[migration] Adding shardYear column to message_meta");
        conn.execute_batch(
            "ALTER TABLE message_meta ADD COLUMN shardYear INTEGER NOT NULL DEFAULT 0"
        )?;
    }

    // Step 2: Backfill shardYear from dateMs (WHERE shardYear = 0 makes it idempotent)
    let backfilled = conn.execute(
        "UPDATE message_meta SET shardYear = CAST(strftime('%Y', dateMs / 1000, 'unixepoch') AS INTEGER) WHERE shardYear = 0 AND dateMs > 0",
        [],
    )?;
    if backfilled > 0 {
        log::info!("[migration] Backfilled shardYear for {} rows from dateMs", backfilled);
    }
    let clamped = conn.execute(
        &format!("UPDATE message_meta SET shardYear = {} WHERE shardYear = 0", config::sqlite::SHARD_MIN_YEAR),
        [],
    )?;
    if clamped > 0 {
        log::info!("[migration] Clamped {} rows with dateMs=0 to year {}", clamped, config::sqlite::SHARD_MIN_YEAR);
    }

    // Step 3: Per-year shard migration (crash-safe per-year with count verification)
    let years: Vec<i32> = {
        let mut stmt = conn.prepare("SELECT DISTINCT shardYear FROM message_meta ORDER BY shardYear")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        rows.filter_map(|r| r.ok()).collect()
    };
    log::info!("[migration] Found {} distinct years to migrate: {:?}", years.len(), years);

    let monolithic_total: i64 = conn.query_row("SELECT COUNT(*) FROM messages_fts", [], |r| r.get(0))?;

    for &year in &years {
        let expected: i64 = conn.query_row(
            "SELECT COUNT(*) FROM message_meta WHERE shardYear = ?1",
            params![year],
            |r| r.get(0),
        )?;

        let table = fts_table_name(year);

        // Check if shard already has all rows
        let actual: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap_or(0);

        if actual == expected {
            log::info!("[migration] Year {} already complete ({} rows), skipping", year, actual);
            known_years.insert(year);
            continue;
        }

        // Mismatch or new shard — drop partial state and redo
        log::info!("[migration] Migrating year {}: expected={}, actual={} → rebuilding shard", year, expected, actual);
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {table}"))?;
        // Remove from known_years in case it was partially tracked
        known_years.remove(&year);

        // Create fresh shard
        ensure_shard(conn, year, known_years)?;

        // Copy all rows for this year in a single transaction
        conn.execute(
            &format!(
                r#"INSERT INTO {table} (rowid, msgId, subject, from_, to_, cc, bcc, body)
                SELECT fts.rowid, fts.msgId, fts.subject, fts.from_, fts.to_, fts.cc, fts.bcc, fts.body
                FROM messages_fts fts
                JOIN message_meta meta ON fts.rowid = meta.rowid
                WHERE meta.shardYear = ?1"#
            ),
            params![year],
        )?;

        let copied: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
        log::info!("[migration] Year {}: copied {} rows to {}", year, copied, table);
    }

    // Step 4: Verify totals and drop monolithic table
    let shard_total: i64 = years.iter().map(|&y| {
        let table = fts_table_name(y);
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0)).unwrap_or(0)
    }).sum();

    if shard_total == monolithic_total {
        log::info!("[migration] Verification passed: {} rows in shards == {} in monolithic. Dropping messages_fts", shard_total, monolithic_total);
        conn.execute_batch("DROP TABLE messages_fts")?;
        log::info!("[migration] ✅ Migration complete — monolithic table dropped");
    } else {
        log::warn!(
            "[migration] Count mismatch: shards={} vs monolithic={}. NOT dropping monolithic — will retry on next boot",
            shard_total, monolithic_total
        );
    }

    Ok(())
}

pub fn init_database(conn: &Connection) -> anyhow::Result<()> {
    log::info!("Initializing database schema (year-sharded FTS)");

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

    // Create non-FTS tables. FTS shard tables are created lazily via ensure_shard().
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS message_meta (
            rowid INTEGER PRIMARY KEY,
            dateMs INTEGER NOT NULL,
            hasAttachments INTEGER NOT NULL,
            parsedIcsAttachments TEXT,
            shardYear INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS message_ids (
            msgId TEXT PRIMARY KEY
        );
        "#,
    )?;

    // Vector tables for semantic search (sqlite-vec).
    // messages_vec rowids match FTS shard rowids (globally unique via message_ids.rowid).
    conn.execute_batch(&format!(
        r#"
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_vec USING vec0(
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

    log::info!("Database schema initialized (4 tables: message_meta, message_ids, messages_vec, embed_cache; FTS shards created lazily)");
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

pub fn open_or_create_db(profile_dir: &Path) -> anyhow::Result<(PathBuf, Connection, HashSet<i32>)> {
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

    let mut known_years = HashSet::new();

    // Check for monolithic messages_fts table (pre-sharding schema)
    let has_monolithic: bool = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='messages_fts'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .is_some();

    // Check for any existing year shard tables
    let has_shards = !load_known_years(&conn).unwrap_or_default().is_empty();

    if has_monolithic {
        log::info!("Detected monolithic messages_fts — running year-shard migration");
        // Ensure vector tables exist (pre-v0.7.0 migration)
        ensure_vector_tables(&conn)?;
        // Run crash-safe migration to year shards
        migrate_monolithic_to_shards(&conn, &mut known_years)?;
        // Reload known years after migration (in case monolithic wasn't fully dropped)
        known_years = load_known_years(&conn)?;
    } else if has_shards {
        log::info!("Using existing year-sharded FTS database");
        ensure_vector_tables(&conn)?;
        known_years = load_known_years(&conn)?;
    } else {
        log::info!("Creating new year-sharded FTS database schema");
        init_database(&conn)?;
    }

    let count = db_count(&conn)?;
    log::info!("Database initialized: {} documents indexed, {} year shards: {:?}", count, known_years.len(), known_years);

    Ok((db_path, conn, known_years))
}

/// Add vector tables to an existing database (migration for pre-v0.7.0 databases).
/// Also handles migration from L2 to cosine distance metric (v0.7.0-dev → v0.7.0).
fn ensure_vector_tables(conn: &Connection) -> anyhow::Result<()> {
    let vec_exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='messages_vec'",
            [],
            |r| r.get(0),
        )
        .optional()?;

    if vec_exists.is_none() {
        log::info!("Migrating email DB: adding vector tables (messages_vec, embed_cache)");
        conn.execute_batch(&format!(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS messages_vec USING vec0(
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
        log::info!("Vector tables added to email database");
    } else {
        // Check if existing vec0 table uses cosine distance (v0.7.0-dev had L2 by mistake).
        // The table SQL in sqlite_master contains the distance_metric if set.
        let needs_cosine_migration = needs_vec_cosine_migration(conn, "messages_vec")?;
        if needs_cosine_migration {
            log::info!("Migrating messages_vec: L2 → cosine distance metric (dropping and recreating)");
            conn.execute_batch(&format!(
                r#"
                DROP TABLE IF EXISTS messages_vec;
                CREATE VIRTUAL TABLE messages_vec USING vec0(
                    embedding FLOAT[{dims}] distance_metric=cosine
                );
                "#,
                dims = config::embedding::EMBEDDING_DIMS,
            ))?;
            // Clear embed_cache so embeddings get regenerated on next indexBatch
            conn.execute("DELETE FROM embed_cache", [])?;
            log::info!("messages_vec recreated with cosine distance. Embeddings will regenerate on next indexBatch.");
        }
    }

    Ok(())
}

/// Check if a vec0 table needs migration from L2 to cosine distance.
/// Returns true if the table exists but was created WITHOUT distance_metric=cosine.
/// sqlite-vec stores the full CREATE statement in sqlite_master.sql.
pub(crate) fn needs_vec_cosine_migration(conn: &Connection, table_name: &str) -> anyhow::Result<bool> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            params![table_name],
            |r| r.get(0),
        )
        .optional()?;

    match sql {
        Some(create_sql) => {
            // If the CREATE statement contains "cosine" it's already migrated
            let has_cosine = create_sql.to_lowercase().contains("cosine");
            Ok(!has_cosine)
        }
        None => Ok(false), // table doesn't exist, nothing to migrate
    }
}

/// Open a read-only connection to an existing FTS database.
/// Used by the reader thread in multi-threaded mode.
/// Applies same cache/mmap/busy_timeout pragmas as the primary connection.
pub fn open_read_only_connection(db_path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open read-only db {}", db_path.display()))?;

    // Read-only connections still benefit from cache/mmap and need busy_timeout
    // for WAL mode coordination. No journal_mode or wal_autocheckpoint needed.
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

    log::info!("Opened read-only connection to {}", db_path.display());
    Ok(conn)
}

pub fn db_count(conn: &Connection) -> anyhow::Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM message_ids", [], |r| r.get(0))?)
}

/// Count rows in the vector embedding table (0 if table missing or query fails).
pub fn vec_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM messages_vec", [], |r| r.get(0)).unwrap_or(0)
}

pub fn index_batch(conn: &mut Connection, rows: &[Value], engine: Option<&EmbeddingEngine>, known_years: &mut HashSet<i32>) -> anyhow::Result<(i64, i64)> {
    log::info!("Indexing batch of {} messages (embeddings={})", rows.len(), engine.is_some());

    // Pre-create any new shards needed outside the transaction (DDL in FTS5 is auto-commit).
    // Collect the years we'll need so we can ensure shards exist.
    for row in rows {
        let date_ms = row.get("dateMs").and_then(|v| v.as_i64()).unwrap_or(0);
        let year = year_from_date_ms(date_ms);
        ensure_shard(conn, year, known_years)?;
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let mut inserted: i64 = 0;
    let mut skipped_duplicates: i64 = 0;
    let mut embedded: i64 = 0;

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

        let date_ms = row.get("dateMs").and_then(|v| v.as_i64()).unwrap_or(0);
        let year = year_from_date_ms(date_ms);
        let table = fts_table_name(year);

        tx.execute(
            &format!(
                "INSERT INTO {table} (rowid, msgId, subject, from_, to_, cc, bcc, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
            ),
            params![row_id, msg_id_val, subject, from_, to_, cc, bcc, body],
        )?;

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
            INSERT INTO message_meta (rowid, dateMs, hasAttachments, parsedIcsAttachments, shardYear)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![row_id, date_ms, has_attachments, parsed_ics, year],
        )?;

        // Generate and store embedding if engine is available
        if let Some(engine) = engine {
            let embed_text = crate::embeddings::text_prep::prepare_email_text(subject, from_, to_, body);
            match engine.embed(&embed_text) {
                Ok(embedding) => {
                    let blob = f32_vec_to_blob(&embedding);
                    tx.execute(
                        "INSERT INTO messages_vec (rowid, embedding) VALUES (?1, ?2)",
                        params![row_id, blob],
                    )?;
                    embedded += 1;
                }
                Err(e) => {
                    log::warn!("Failed to embed message {}: {}", truncate_for_log(msg_id_val), e);
                }
            }
        }

        inserted += 1;
    }

    tx.commit()?;
    if engine.is_some() {
        log::info!(
            "Indexed {} messages ({} embedded), {} duplicates skipped",
            inserted, embedded, skipped_duplicates
        );
    } else if skipped_duplicates > 0 {
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

/// Convert a Vec<f32> to a little-endian byte blob for sqlite-vec.
pub(crate) fn f32_vec_to_blob(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
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

// Internal struct for FTS candidate data during hybrid merge.
struct FtsCandidate {
    rowid: i64,
    msg_id: String,
    from_: String,
    subject: String,
    date_ms: i64,
    has_attachments: bool,
    snippet: String,
    rank: f64,
}

// Lightweight metadata for vector-only results (no snippet).
struct MessageMeta {
    msg_id: String,
    from_: String,
    subject: String,
    date_ms: i64,
    has_attachments: bool,
}

// Column-scope prefixes used in FTS5 queries after alias translation.
const COLUMN_SCOPE_PREFIXES: &[&str] = &["from_:", "to_:", "subject:", "cc:", "bcc:", "body:"];

/// Check if a processed FTS5 query contains column-scoped terms.
fn query_has_column_scope(fts_query: &str) -> bool {
    COLUMN_SCOPE_PREFIXES.iter().any(|p| fts_query.contains(p))
}

/// Extract only the column-scoped terms from a processed FTS5 query.
/// Given `from_:"tmm@cs.ubc.ca" hiring*`, returns `from_:"tmm@cs.ubc.ca"`.
fn extract_column_scope_filter(fts_query: &str) -> String {
    let mut scoped_terms: Vec<String> = Vec::new();
    let bytes = fts_query.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Skip whitespace
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }

        let remaining = &fts_query[i..];

        // Check if current position starts with a column scope prefix
        let matched_prefix = COLUMN_SCOPE_PREFIXES.iter().find(|p| remaining.starts_with(**p));

        if let Some(prefix) = matched_prefix {
            let start = i;
            i += prefix.len();

            if i < bytes.len() && bytes[i] == b'"' {
                // Quoted value: field:"value"
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1; // closing quote
                }
            } else {
                // Unquoted value: field:value*
                while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
            }

            scoped_terms.push(fts_query[start..i].to_string());
        } else {
            // Non-scoped token — skip it
            if bytes[i] == b'"' {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1;
                }
            } else if bytes[i] == b'(' {
                let mut depth = 1;
                i += 1;
                while i < bytes.len() && depth > 0 {
                    if bytes[i] == b'(' { depth += 1; }
                    if bytes[i] == b')' { depth -= 1; }
                    i += 1;
                }
            } else {
                while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
            }
        }
    }

    scoped_terms.join(" ")
}

/// Fetch all rowids matching a column-scope filter query across all year shards.
fn fetch_eligible_rowids(
    conn: &Connection,
    filter_query: &str,
    known_years: &HashSet<i32>,
) -> anyhow::Result<HashSet<i64>> {
    let mut rowids = HashSet::new();
    for &year in known_years {
        let table = fts_table_name(year);
        let sql = format!("SELECT fts.rowid FROM {table} fts WHERE {table} MATCH ?1");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![filter_query], |r| r.get::<_, i64>(0))?;
        for rowid in rows {
            rowids.insert(rowid?);
        }
    }
    Ok(rowids)
}

pub fn search(
    conn: &Connection,
    q: &str,
    params: &Value,
    synonyms: &SynonymLookup,
    engine: Option<&EmbeddingEngine>,
    known_years: &HashSet<i32>,
) -> anyhow::Result<Vec<Value>> {
    let query = q.trim();
    if query.is_empty() {
        return Ok(vec![]);
    }

    let limit = params
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(config::sqlite::SEARCH_DEFAULT_LIMIT);

    // Fall back to FTS-only when no embedding engine
    let engine = match engine {
        Some(e) => e,
        None => return search_fts_only(conn, query, params, synonyms, limit, known_years),
    };

    let ignore_date = params.get("ignoreDate").and_then(|v| v.as_bool()).unwrap_or(false);
    let from_ts = if !ignore_date {
        params.get("from").and_then(|v| parse_date_param(v).ok().flatten())
    } else {
        None
    };
    let to_ts = if !ignore_date {
        params.get("to").and_then(|v| parse_date_param(v).ok().flatten())
    } else {
        None
    };

    let candidate_limit = limit * config::hybrid::CANDIDATE_MULTIPLIER;

    // --- FTS5 candidates (across all year shards) ---
    let fts_query = build_fts_match(Some(query), true, synonyms);
    log::info!(
        "Hybrid search: \"{}\" -> FTS \"{}\"",
        query,
        fts_query
    );
    let fts_candidates = if !fts_query.is_empty() {
        search_fts_candidates(conn, &fts_query, from_ts, to_ts, candidate_limit, known_years)?
    } else {
        vec![]
    };

    // --- Column-scope filter-first: restrict vector candidates to eligible rowids ---
    let eligible_rowids = if query_has_column_scope(&fts_query) {
        let filter_query = extract_column_scope_filter(&fts_query);
        if !filter_query.is_empty() {
            Some(fetch_eligible_rowids(conn, &filter_query, known_years)?)
        } else {
            None
        }
    } else {
        None
    };

    // --- Vector candidates ---
    let query_embedding = engine.embed(query)?;
    let query_blob = f32_vec_to_blob(&query_embedding);
    let vec_limit = if eligible_rowids.is_some() { candidate_limit * 2 } else { candidate_limit };
    let all_vec = search_vec_candidates(conn, "messages_vec", &query_blob, vec_limit)
        .unwrap_or_default(); // empty vec table during rebuild → graceful empty

    // Filter vector candidates to eligible set when column-scoped
    let vec_candidates: Vec<(i64, f64)> = if let Some(ref eligible) = eligible_rowids {
        all_vec.into_iter().filter(|(rowid, _)| eligible.contains(rowid)).collect()
    } else {
        all_vec
    };

    // Fall back to FTS-only when vec table is empty (e.g., during embedding rebuild).
    // Without this, hybrid weights (text_weight=0.3) penalize text-only results below MIN_SCORE.
    if vec_candidates.is_empty() {
        log::info!("No vector candidates (vec table may be empty), falling back to FTS-only search");
        return search_fts_only(conn, query, params, synonyms, limit, known_years);
    }

    // --- Merge ---
    let text_pairs: Vec<(i64, f64)> = fts_candidates.iter().map(|c| (c.rowid, c.rank)).collect();
    let merged = crate::fts::hybrid::merge_results(
        &text_pairs,
        &vec_candidates,
        config::hybrid::EMAIL_VECTOR_WEIGHT,
        config::hybrid::EMAIL_TEXT_WEIGHT,
        limit as usize,
    );

    // --- Assemble results ---
    let mut fts_map: HashMap<i64, FtsCandidate> =
        fts_candidates.into_iter().map(|c| (c.rowid, c)).collect();
    let mut results = Vec::with_capacity(merged.len());

    for hr in &merged {
        if let Some(fts_c) = fts_map.remove(&hr.rowid) {
            // FTS result — has snippet
            results.push(serde_json::json!({
                "uniqueId": fts_c.msg_id,
                "author": fts_c.from_,
                "subject": fts_c.subject,
                "dateMs": fts_c.date_ms,
                "hasAttachments": fts_c.has_attachments,
                "snippet": fts_c.snippet,
                "rank": -hr.final_score
            }));
        } else {
            // Vector-only result — fetch metadata, apply date filter
            if let Some(meta) = fetch_message_meta(conn, hr.rowid)? {
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
                    "uniqueId": meta.msg_id,
                    "author": meta.from_,
                    "subject": meta.subject,
                    "dateMs": meta.date_ms,
                    "hasAttachments": meta.has_attachments,
                    "snippet": "",
                    "rank": -hr.final_score
                }));
            }
        }
    }

    let filter_info = if let Some(ref eligible) = eligible_rowids {
        format!(", filtered to {} eligible", eligible.len())
    } else {
        String::new()
    };
    log::info!(
        "Hybrid search completed: {} results (FTS cands: {}, Vec cands: {}{})",
        results.len(),
        text_pairs.len(),
        vec_candidates.len(),
        filter_info
    );
    Ok(results)
}

/// FTS-only search across all year shards using a single UNION ALL query.
fn search_fts_only(
    conn: &Connection,
    query: &str,
    params: &Value,
    synonyms: &SynonymLookup,
    limit: i64,
    known_years: &HashSet<i32>,
) -> anyhow::Result<Vec<Value>> {
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
    if known_years.is_empty() {
        return Ok(vec![]);
    }

    let ignore_date = params.get("ignoreDate").and_then(|v| v.as_bool()).unwrap_or(false);
    let from_ts = if !ignore_date {
        params.get("from").and_then(|v| parse_date_param(v).ok().flatten())
    } else {
        None
    };
    let to_ts = if !ignore_date {
        params.get("to").and_then(|v| parse_date_param(v).ok().flatten())
    } else {
        None
    };

    // Build single UNION ALL query across all year shards (avoids sequential per-shard overhead).
    // Numbered params reused across UNION branches: ?1=fts_query, optional ?2/?3=dates, ?N=limit.
    let mut next_param = 2usize;
    let from_param = from_ts.map(|_| { let p = next_param; next_param += 1; p });
    let to_param = to_ts.map(|_| { let p = next_param; next_param += 1; p });
    let limit_param = next_param;

    let years: Vec<i32> = known_years.iter().copied().collect();
    let subqueries: Vec<String> = years.iter().map(|&year| {
        let table = fts_table_name(year);
        let mut sq = format!(
            r#"SELECT fts.msgId, fts.from_, fts.subject, meta.dateMs, meta.hasAttachments,
                snippet({table}, -1, '[', ']', '…', {st}) AS snippet,
                bm25({table}, 0.0, 5.0, 3.0, 2.0, 1.0, 1.0, 1.0) AS rank
            FROM {table} fts
            JOIN message_meta meta ON fts.rowid = meta.rowid
            WHERE {table} MATCH ?1"#,
            table = table,
            st = config::sqlite::SEARCH_SNIPPET_TOKENS,
        );
        if let Some(p) = from_param {
            sq.push_str(&format!(" AND meta.dateMs >= ?{p}"));
        }
        if let Some(p) = to_param {
            sq.push_str(&format!(" AND meta.dateMs <= ?{p}"));
        }
        sq
    }).collect();

    let sql = format!(
        "{} ORDER BY dateMs DESC, rank ASC LIMIT ?{lp}",
        subqueries.join(" UNION ALL "),
        lp = limit_param,
    );

    let mut bind: Vec<rusqlite::types::Value> = Vec::new();
    bind.push(rusqlite::types::Value::from(fts_query));
    if let Some(from) = from_ts {
        bind.push(rusqlite::types::Value::from(from));
    }
    if let Some(to) = to_ts {
        bind.push(rusqlite::types::Value::from(to));
    }
    bind.push(rusqlite::types::Value::from(limit));

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(bind.iter()), |r| {
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
    })?;

    let results: Vec<Value> = rows.collect::<Result<Vec<_>, _>>()?;
    log::info!("Search completed: found {} results across {} shards", results.len(), years.len());
    Ok(results)
}

/// Get FTS5 candidates with full metadata for hybrid merge using a single UNION ALL query.
fn search_fts_candidates(
    conn: &Connection,
    fts_query: &str,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
    limit: i64,
    known_years: &HashSet<i32>,
) -> anyhow::Result<Vec<FtsCandidate>> {
    if known_years.is_empty() {
        return Ok(vec![]);
    }

    // Build single UNION ALL query: ?1=fts_query (reused), optional ?2/?3=dates, ?N=limit.
    let mut next_param = 2usize;
    let from_param = from_ts.map(|_| { let p = next_param; next_param += 1; p });
    let to_param = to_ts.map(|_| { let p = next_param; next_param += 1; p });
    let limit_param = next_param;

    let years: Vec<i32> = known_years.iter().copied().collect();
    let subqueries: Vec<String> = years.iter().map(|&year| {
        let table = fts_table_name(year);
        let mut sq = format!(
            r#"SELECT fts.rowid, fts.msgId, fts.from_, fts.subject, meta.dateMs, meta.hasAttachments,
                snippet({table}, -1, '[', ']', '…', {st}) AS snippet,
                bm25({table}, 0.0, 5.0, 3.0, 2.0, 1.0, 1.0, 1.0) AS rank
            FROM {table} fts
            JOIN message_meta meta ON fts.rowid = meta.rowid
            WHERE {table} MATCH ?1"#,
            table = table,
            st = config::sqlite::SEARCH_SNIPPET_TOKENS,
        );
        if let Some(p) = from_param {
            sq.push_str(&format!(" AND meta.dateMs >= ?{p}"));
        }
        if let Some(p) = to_param {
            sq.push_str(&format!(" AND meta.dateMs <= ?{p}"));
        }
        sq
    }).collect();

    let sql = format!(
        "{} ORDER BY rank ASC LIMIT ?{lp}",
        subqueries.join(" UNION ALL "),
        lp = limit_param,
    );

    let mut bind: Vec<rusqlite::types::Value> = Vec::new();
    bind.push(rusqlite::types::Value::from(fts_query.to_string()));
    if let Some(from) = from_ts {
        bind.push(rusqlite::types::Value::from(from));
    }
    if let Some(to) = to_ts {
        bind.push(rusqlite::types::Value::from(to));
    }
    bind.push(rusqlite::types::Value::from(limit));

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(bind.iter()), |r| {
        Ok(FtsCandidate {
            rowid: r.get(0)?,
            msg_id: r.get(1)?,
            from_: r.get(2)?,
            subject: r.get(3)?,
            date_ms: r.get(4)?,
            has_attachments: r.get::<_, i64>(5)? != 0,
            snippet: r.get(6)?,
            rank: r.get(7)?,
        })
    })?;

    let candidates: Vec<FtsCandidate> = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(candidates)
}

/// Get vector similarity candidates from a vec0 table.
pub(crate) fn search_vec_candidates(
    conn: &Connection,
    table: &str,
    query_blob: &[u8],
    limit: i64,
) -> anyhow::Result<Vec<(i64, f64)>> {
    let sql = format!(
        "SELECT rowid, distance FROM {table} WHERE embedding MATCH ?1 AND k = ?2"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![query_blob, limit], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Fetch metadata for a single message by rowid (used for vector-only results).
/// Looks up shardYear from message_meta, then fetches FTS columns from the specific shard.
fn fetch_message_meta(conn: &Connection, rowid: i64) -> anyhow::Result<Option<MessageMeta>> {
    // Step 1: Get shardYear + non-FTS metadata from message_meta
    let meta_row: Option<(i32, i64, bool)> = conn
        .query_row(
            "SELECT shardYear, dateMs, hasAttachments FROM message_meta WHERE rowid = ?1",
            params![rowid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? != 0)),
        )
        .optional()?;

    let Some((shard_year, date_ms, has_attachments)) = meta_row else {
        return Ok(None);
    };

    // Step 2: Get FTS columns from the specific shard
    let table = fts_table_name(shard_year);
    let fts_row: Option<(String, String, String)> = conn
        .query_row(
            &format!("SELECT msgId, from_, subject FROM {table} WHERE rowid = ?1"),
            params![rowid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;

    let Some((msg_id, from_, subject)) = fts_row else {
        return Ok(None);
    };

    Ok(Some(MessageMeta {
        msg_id,
        from_,
        subject,
        date_ms,
        has_attachments,
    }))
}

/// Start rebuilding vector embeddings: clear vec tables and return total count.
/// Call this once, then call `rebuild_embeddings_batch` repeatedly until done.
pub fn rebuild_embeddings_start(conn: &mut Connection) -> anyhow::Result<i64> {
    log::info!("Starting email embedding rebuild — clearing vector tables");
    conn.execute("DELETE FROM messages_vec", [])?;
    conn.execute("DELETE FROM embed_cache", [])?;
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM message_ids", [], |r| r.get(0))?;
    log::info!("Cleared messages_vec and embed_cache, {} documents to embed", total);
    Ok(total)
}

/// Process one batch of email embedding rebuild.
/// Uses message_meta to iterate by rowid and fetch FTS data from specific shards.
/// Returns (last_rowid, processed_in_batch, embedded_in_batch, done).
pub fn rebuild_embeddings_batch(
    conn: &mut Connection,
    engine: &EmbeddingEngine,
    last_rowid: i64,
    batch_size: i64,
) -> anyhow::Result<(i64, i64, i64, bool)> {
    // Step 1: Get next batch of (rowid, shardYear) from message_meta
    let meta_batch: Vec<(i64, i32)> = {
        let mut stmt = conn.prepare(
            "SELECT rowid, shardYear FROM message_meta WHERE rowid > ?1 ORDER BY rowid ASC LIMIT ?2"
        )?;
        let rows = stmt.query_map(params![last_rowid, batch_size], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    if meta_batch.is_empty() {
        return Ok((last_rowid, 0, 0, true));
    }

    // Step 2: Group by year and fetch FTS data from specific shards
    let mut by_year: HashMap<i32, Vec<i64>> = HashMap::new();
    for &(rowid, year) in &meta_batch {
        by_year.entry(year).or_default().push(rowid);
    }

    let mut fts_data: HashMap<i64, (String, String, String, String)> = HashMap::new();
    for (year, rowids) in &by_year {
        let table = fts_table_name(*year);
        let placeholders: String = rowids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT rowid, subject, from_, to_, body FROM {table} WHERE rowid IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<rusqlite::types::Value> = rowids.iter().map(|&r| rusqlite::types::Value::from(r)).collect();
        let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?))
        })?;
        for row in rows {
            let (rowid, subject, from_, to_, body) = row?;
            fts_data.insert(rowid, (subject, from_, to_, body));
        }
    }

    let mut new_last_rowid = last_rowid;
    let mut embedded: i64 = 0;
    let processed = meta_batch.len() as i64;
    let done = (meta_batch.len() as i64) < batch_size;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    for &(rowid, _year) in &meta_batch {
        if let Some((subject, from_, to_, body)) = fts_data.get(&rowid) {
            let embed_text = crate::embeddings::text_prep::prepare_email_text(subject, from_, to_, body);
            match engine.embed(&embed_text) {
                Ok(embedding) => {
                    let blob = f32_vec_to_blob(&embedding);
                    // vec0 virtual tables don't support INSERT OR REPLACE,
                    // so delete first to handle checkpoint-resume overlaps.
                    tx.execute("DELETE FROM messages_vec WHERE rowid = ?1", params![rowid])?;
                    tx.execute(
                        "INSERT INTO messages_vec (rowid, embedding) VALUES (?1, ?2)",
                        params![rowid, blob],
                    )?;
                    embedded += 1;
                }
                Err(e) => {
                    log::warn!("Failed to embed rowid {}: {}", rowid, e);
                }
            }
        }
        new_last_rowid = rowid;
    }
    tx.commit()?;

    Ok((new_last_rowid, processed, embedded, done))
}

/// Clear and rebuild the email FTS database.
/// Takes ownership of the connection to close it, returns a new connection after rebuild.
/// Caller must signal the reader thread to reopen its read-only connection and clear known_years.
pub fn clear_rebuild_standalone(db_path: &Path, conn: Connection) -> anyhow::Result<Connection> {
    log::info!("Clearing email FTS by deleting database file (rebuild from scratch)");
    drop(conn);
    log::info!("Database connection closed");

    delete_file_if_exists(db_path)?;
    delete_file_if_exists(&PathBuf::from(format!("{}-wal", db_path.display())))?;
    delete_file_if_exists(&PathBuf::from(format!("{}-shm", db_path.display())))?;

    log::info!("Recreating database...");
    let new_conn = Connection::open(db_path)?;
    ensure_fts5_available(&new_conn)?;
    init_database(&new_conn)?;
    log::info!("Database recreated and initialized successfully (FTS shards will be created lazily)");
    Ok(new_conn)
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

pub fn optimize(conn: &Connection, known_years: &HashSet<i32>) -> anyhow::Result<()> {
    log::info!("Optimizing FTS index across {} shards", known_years.len());
    for &year in known_years {
        let table = fts_table_name(year);
        conn.execute(&format!("INSERT INTO {table}({table}) VALUES('optimize')"), [])?;
    }
    log::info!("FTS optimization complete");
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
        // Look up rowid + shardYear in a single query
        let resolved: Option<(i64, i32)> = tx
            .query_row(
                "SELECT mi.rowid, mm.shardYear FROM message_ids mi JOIN message_meta mm ON mi.rowid = mm.rowid WHERE mi.msgId = ?1",
                params![msg_id_val],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((row_id, shard_year)) = resolved {
            let table = fts_table_name(shard_year);
            tx.execute(&format!("DELETE FROM {table} WHERE rowid = ?1"), params![row_id])?;
            tx.execute("DELETE FROM message_meta WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM messages_vec WHERE rowid = ?1", params![row_id])?;
            tx.execute("DELETE FROM message_ids WHERE msgId = ?1", params![msg_id_val])?;
            removed += 1;
        }
    }

    tx.commit()?;
    log::info!("Removed {} messages", removed);
    Ok(removed)
}

pub fn get_message_by_msgid(conn: &Connection, msg_id: &str) -> anyhow::Result<Option<Value>> {
    // Step 1: Get rowid from message_ids
    let row_id: Option<i64> = conn
        .query_row(
            "SELECT rowid FROM message_ids WHERE msgId = ?1",
            params![msg_id],
            |r| r.get(0),
        )
        .optional()?;
    let Some(row_id) = row_id else {
        return Ok(None);
    };

    // Step 2: Get metadata + shardYear from message_meta
    let meta: Option<(i64, i64, Option<String>, i32)> = conn
        .query_row(
            "SELECT dateMs, hasAttachments, parsedIcsAttachments, shardYear FROM message_meta WHERE rowid = ?1",
            params![row_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((date_ms, has_attachments, parsed_ics, shard_year)) = meta else {
        return Ok(None);
    };

    // Step 3: Get FTS columns from specific shard
    let table = fts_table_name(shard_year);
    let fts_row: Option<(String, String, String, String, String, String)> = conn
        .query_row(
            &format!("SELECT subject, from_, to_, cc, bcc, body FROM {table} WHERE rowid = ?1"),
            params![row_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )
        .optional()?;
    let Some((subject, from_, to_, cc, bcc, body)) = fts_row else {
        return Ok(None);
    };

    Ok(Some(serde_json::json!({
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
    })))
}

/// Find all indexed entries matching a specific headerMessageId within an account.
/// Uses the unsharded message_ids table (no need to iterate FTS shards).
/// Used by incremental indexer when the exact folder path is unknown (deletion events
/// sometimes have stale/wrong folder info from Gmail virtual folders).
/// Returns list of matching msgId keys (format: accountId:folderPath:headerMessageId).
pub fn find_by_header_message_id(conn: &Connection, account_id: &str, header_message_id: &str) -> anyhow::Result<Vec<String>> {
    // Key format is accountId:folderPath:headerMessageId
    // We search for entries that start with accountId: and end with :headerMessageId
    let pattern = format!("{}:%:{}", account_id, header_message_id);
    log::info!("Finding entries matching pattern: {}", pattern);

    let mut stmt = conn.prepare(
        "SELECT msgId FROM message_ids WHERE msgId LIKE ?1"
    )?;

    let rows = stmt.query_map(params![pattern], |r| r.get::<_, String>(0))?;
    let results: Vec<String> = rows.filter_map(|r| r.ok()).collect();

    log::info!("Found {} entries matching headerMessageId {}", results.len(), header_message_id);
    Ok(results)
}

pub fn query_by_date_range(conn: &Connection, from_v: &Value, to_v: &Value, limit: i64, known_years: &HashSet<i32>) -> anyhow::Result<Vec<Value>> {
    let Some(from_ts) = parse_date_param(from_v)? else { bail!("from and to parameters are required") };
    let Some(to_ts) = parse_date_param(to_v)? else { bail!("from and to parameters are required") };

    log::info!(
        "Querying messages from {} to {}, limit {}",
        from_ts,
        to_ts,
        limit
    );

    // Only query year shards that overlap the date range
    let from_year = year_from_date_ms(from_ts);
    let to_year = year_from_date_ms(to_ts);
    let years: Vec<i32> = known_years.iter().copied()
        .filter(|&y| y >= from_year && y <= to_year)
        .collect();

    if years.is_empty() {
        return Ok(vec![]);
    }

    // Single UNION ALL query: ?1=from, ?2=to (reused across branches), ?3=limit.
    let subqueries: Vec<String> = years.iter().map(|&year| {
        let table = fts_table_name(year);
        format!(
            r#"SELECT f.msgId, f.subject, m.dateMs
            FROM {table} f
            JOIN message_meta m ON f.rowid = m.rowid
            WHERE m.dateMs >= ?1 AND m.dateMs <= ?2"#,
            table = table,
        )
    }).collect();

    let sql = format!(
        "{} ORDER BY dateMs DESC LIMIT ?3",
        subqueries.join(" UNION ALL "),
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![from_ts, to_ts, limit])?;
    let mut results = Vec::new();
    while let Some(r) = rows.next()? {
        let msg_id: String = r.get(0)?;
        let subject: String = r.get(1)?;
        let date_ms: i64 = r.get(2)?;
        let date_str = format_date_iso_like_python(date_ms);
        results.push(serde_json::json!({
            "msgId": msg_id,
            "subject": subject,
            "dateMs": date_ms,
            "dateStr": date_str
        }));
    }

    log::info!("Found {} messages in date range across {} shards", results.len(), years.len());
    Ok(results)
}

pub fn debug_sample(conn: &Connection, known_years: &HashSet<i32>) -> anyhow::Result<Vec<Value>> {
    log::info!("Getting debug sample");
    let limit = config::sqlite::SEARCH_DEBUG_SAMPLE_LIMIT;

    if known_years.is_empty() {
        return Ok(vec![]);
    }

    // Single UNION ALL query across all shards, ?1=limit.
    let years: Vec<i32> = known_years.iter().copied().collect();
    let subqueries: Vec<String> = years.iter().map(|&year| {
        let table = fts_table_name(year);
        format!(
            r#"SELECT f.msgId, f.subject, m.dateMs
            FROM {table} f
            JOIN message_meta m ON f.rowid = m.rowid"#,
            table = table,
        )
    }).collect();

    let sql = format!(
        "{} ORDER BY dateMs DESC LIMIT ?1",
        subqueries.join(" UNION ALL "),
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![limit])?;
    let mut results = Vec::new();
    while let Some(r) = rows.next()? {
        let msg_id: String = r.get(0)?;
        let subject: String = r.get(1)?;
        let date_ms: i64 = r.get(2)?;
        results.push(serde_json::json!({
            "msgId": msg_id,
            "subject": subject,
            "dateMs": date_ms
        }));
    }

    Ok(results)
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Create an in-memory database with year-sharded FTS schema for testing.
    /// Returns (connection, known_years set).
    fn setup_test_db() -> (Connection, HashSet<i32>) {
        let conn = Connection::open_in_memory().unwrap();
        let mut known_years = HashSet::new();

        // Create supporting tables (year-sharded schema)
        conn.execute_batch(r#"
            CREATE TABLE IF NOT EXISTS message_meta (
                rowid INTEGER PRIMARY KEY,
                dateMs INTEGER NOT NULL,
                hasAttachments INTEGER NOT NULL,
                parsedIcsAttachments TEXT,
                shardYear INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS message_ids (
                msgId TEXT PRIMARY KEY
            );
        "#).unwrap();

        // Create shard for year 2000 (SHARD_MIN_YEAR — where dateMs=0/small values are clamped)
        ensure_shard(&conn, 2000, &mut known_years).unwrap();
        // Create shard for 2023 (used by tests with realistic timestamps)
        ensure_shard(&conn, 2023, &mut known_years).unwrap();

        (conn, known_years)
    }

    /// Insert a test message into the database, routing to the correct year shard.
    fn insert_test_message(conn: &Connection, known_years: &mut HashSet<i32>, msg_id: &str, subject: &str, date_ms: i64) {
        // Insert into message_ids first
        conn.execute(
            "INSERT OR IGNORE INTO message_ids (msgId) VALUES (?1)",
            params![msg_id],
        ).unwrap();

        let row_id: i64 = conn.query_row(
            "SELECT rowid FROM message_ids WHERE msgId = ?1",
            params![msg_id],
            |r| r.get(0),
        ).unwrap();

        let year = year_from_date_ms(date_ms);
        ensure_shard(conn, year, known_years).unwrap();
        let table = fts_table_name(year);

        // Insert into year-specific FTS shard
        conn.execute(
            &format!("INSERT INTO {table} (rowid, msgId, subject, from_, to_, cc, bcc, body) VALUES (?1, ?2, ?3, '', '', '', '', '')"),
            params![row_id, msg_id, subject],
        ).unwrap();

        // Insert into meta table with shardYear
        conn.execute(
            "INSERT INTO message_meta (rowid, dateMs, hasAttachments, parsedIcsAttachments, shardYear) VALUES (?1, ?2, 0, '', ?3)",
            params![row_id, date_ms, year],
        ).unwrap();
    }

    /// Insert a test message with from/to/body fields for column-scope testing.
    fn insert_test_message_full(
        conn: &Connection,
        known_years: &mut HashSet<i32>,
        msg_id: &str,
        subject: &str,
        from: &str,
        to: &str,
        body: &str,
        date_ms: i64,
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO message_ids (msgId) VALUES (?1)",
            params![msg_id],
        ).unwrap();

        let row_id: i64 = conn.query_row(
            "SELECT rowid FROM message_ids WHERE msgId = ?1",
            params![msg_id],
            |r| r.get(0),
        ).unwrap();

        let year = year_from_date_ms(date_ms);
        ensure_shard(conn, year, known_years).unwrap();
        let table = fts_table_name(year);

        conn.execute(
            &format!("INSERT INTO {table} (rowid, msgId, subject, from_, to_, cc, bcc, body) VALUES (?1, ?2, ?3, ?4, ?5, '', '', ?6)"),
            params![row_id, msg_id, subject, from, to, body],
        ).unwrap();

        conn.execute(
            "INSERT INTO message_meta (rowid, dateMs, hasAttachments, parsedIcsAttachments, shardYear) VALUES (?1, ?2, 0, '', ?3)",
            params![row_id, date_ms, year],
        ).unwrap();
    }

    // --- Column-scope filter-first tests ---

    #[test]
    fn test_query_has_column_scope_detects_prefixes() {
        assert!(query_has_column_scope(r#"from_:"alice@example.com" hiring*"#));
        assert!(query_has_column_scope(r#"to_:"bob@example.com""#));
        assert!(query_has_column_scope("subject:budget*"));
        assert!(query_has_column_scope(r#"cc:"team@example.com""#));
        assert!(query_has_column_scope(r#"bcc:"secret@example.com""#));
        assert!(query_has_column_scope("body:quarterly"));

        // No column scope
        assert!(!query_has_column_scope("hiring budget*"));
        assert!(!query_has_column_scope(r#""quarterly report""#));
        assert!(!query_has_column_scope("simple search"));
    }

    #[test]
    fn test_extract_column_scope_filter_quoted() {
        let result = extract_column_scope_filter(r#"from_:"alice@example.com" hiring*"#);
        assert_eq!(result, r#"from_:"alice@example.com""#);
    }

    #[test]
    fn test_extract_column_scope_filter_unquoted() {
        let result = extract_column_scope_filter("subject:budget* hiring*");
        assert_eq!(result, "subject:budget*");
    }

    #[test]
    fn test_extract_column_scope_filter_multiple() {
        let result = extract_column_scope_filter(r#"from_:"alice@example.com" to_:"bob@example.com" meeting"#);
        assert_eq!(result, r#"from_:"alice@example.com" to_:"bob@example.com""#);
    }

    #[test]
    fn test_extract_column_scope_filter_no_scope() {
        let result = extract_column_scope_filter("hiring budget*");
        assert_eq!(result, "");
    }

    #[test]
    fn test_extract_column_scope_filter_parenthesized_groups() {
        // Synonym OR expansions should be skipped
        let result = extract_column_scope_filter(r#"from_:"alice@example.com" (hiring OR recruit)"#);
        assert_eq!(result, r#"from_:"alice@example.com""#);
    }

    #[test]
    fn test_fetch_eligible_rowids_filters_by_sender() {
        let (conn, mut known_years) = setup_test_db();

        insert_test_message_full(&conn, &mut known_years,
            "acc:/:msg1", "Budget Report", "alice@example.com", "team@co.com", "Q1 numbers", 1704067200000);
        insert_test_message_full(&conn, &mut known_years,
            "acc:/:msg2", "Hiring Plan", "bob@example.com", "team@co.com", "New roles", 1704153600000);
        insert_test_message_full(&conn, &mut known_years,
            "acc:/:msg3", "Budget Update", "alice@example.com", "team@co.com", "Q2 forecast", 1704240000000);

        let eligible = fetch_eligible_rowids(&conn, r#"from_:"alice@example.com""#, &known_years).unwrap();
        assert_eq!(eligible.len(), 2, "Should find 2 messages from alice");

        // Get rowids for alice's messages to verify
        let msg1_rowid: i64 = conn.query_row("SELECT rowid FROM message_ids WHERE msgId = 'acc:/:msg1'", [], |r| r.get(0)).unwrap();
        let msg3_rowid: i64 = conn.query_row("SELECT rowid FROM message_ids WHERE msgId = 'acc:/:msg3'", [], |r| r.get(0)).unwrap();
        assert!(eligible.contains(&msg1_rowid));
        assert!(eligible.contains(&msg3_rowid));
    }

    #[test]
    fn test_fetch_eligible_rowids_across_year_shards() {
        let (conn, mut known_years) = setup_test_db();

        // Messages in different years from the same sender
        insert_test_message_full(&conn, &mut known_years,
            "acc:/:msg1", "Old Report", "alice@example.com", "", "", 946684800000);  // 2000
        insert_test_message_full(&conn, &mut known_years,
            "acc:/:msg2", "New Report", "alice@example.com", "", "", 1704067200000); // 2024
        insert_test_message_full(&conn, &mut known_years,
            "acc:/:msg3", "Other", "bob@example.com", "", "", 1704067200000); // 2024

        let eligible = fetch_eligible_rowids(&conn, r#"from_:"alice@example.com""#, &known_years).unwrap();
        assert_eq!(eligible.len(), 2, "Should find alice's messages across year shards");
    }

    #[test]
    fn test_find_by_header_message_id_basic() {
        let (conn, mut known_years) = setup_test_db();

        // Insert messages with the same headerMessageId in different folders
        insert_test_message(&conn, &mut known_years, "account1:/INBOX:msg123", "Test Subject", 1000);
        insert_test_message(&conn, &mut known_years, "account1:/Deleted Messages:msg123", "Test Subject", 1001);
        insert_test_message(&conn, &mut known_years, "account1:/[Gmail]/All Mail:msg123", "Test Subject", 1002);

        // Search for msg123 in account1
        let results = find_by_header_message_id(&conn, "account1", "msg123").unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.contains(&"account1:/INBOX:msg123".to_string()));
        assert!(results.contains(&"account1:/Deleted Messages:msg123".to_string()));
        assert!(results.contains(&"account1:/[Gmail]/All Mail:msg123".to_string()));
    }

    #[test]
    fn test_find_by_header_message_id_different_accounts() {
        let (conn, mut known_years) = setup_test_db();

        // Insert same headerMessageId in different accounts
        insert_test_message(&conn, &mut known_years, "account1:/INBOX:msg123", "Test Subject 1", 1000);
        insert_test_message(&conn, &mut known_years, "account2:/INBOX:msg123", "Test Subject 2", 1001);
        insert_test_message(&conn, &mut known_years, "account3:/INBOX:msg123", "Test Subject 3", 1002);

        // Search should only return entries for the specified account
        let results = find_by_header_message_id(&conn, "account1", "msg123").unwrap();

        assert_eq!(results.len(), 1);
        assert!(results.contains(&"account1:/INBOX:msg123".to_string()));
    }

    #[test]
    fn test_find_by_header_message_id_different_messages() {
        let (conn, mut known_years) = setup_test_db();

        // Insert different headerMessageIds in same account
        insert_test_message(&conn, &mut known_years, "account1:/INBOX:msg123", "Test Subject 1", 1000);
        insert_test_message(&conn, &mut known_years, "account1:/INBOX:msg456", "Test Subject 2", 1001);
        insert_test_message(&conn, &mut known_years, "account1:/INBOX:msg789", "Test Subject 3", 1002);

        // Search should only return entries with matching headerMessageId
        let results = find_by_header_message_id(&conn, "account1", "msg456").unwrap();

        assert_eq!(results.len(), 1);
        assert!(results.contains(&"account1:/INBOX:msg456".to_string()));
    }

    #[test]
    fn test_find_by_header_message_id_no_matches() {
        let (conn, mut known_years) = setup_test_db();

        // Insert some messages
        insert_test_message(&conn, &mut known_years, "account1:/INBOX:msg123", "Test Subject", 1000);

        // Search for non-existent headerMessageId
        let results = find_by_header_message_id(&conn, "account1", "nonexistent").unwrap();
        assert_eq!(results.len(), 0);

        // Search for non-existent account
        let results = find_by_header_message_id(&conn, "nonexistent", "msg123").unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_find_by_header_message_id_empty_database() {
        let (conn, _known_years) = setup_test_db();

        // Search in empty database
        let results = find_by_header_message_id(&conn, "account1", "msg123").unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_find_by_header_message_id_special_characters_in_folder() {
        let (conn, mut known_years) = setup_test_db();

        // Insert messages with special characters in folder paths
        insert_test_message(&conn, &mut known_years, "account1:/[Gmail]/All Mail:msg123", "Test 1", 1000);
        insert_test_message(&conn, &mut known_years, "account1:/Folder With Spaces:msg123", "Test 2", 1001);
        insert_test_message(&conn, &mut known_years, "account1:/Folder/With/Slashes:msg123", "Test 3", 1002);

        let results = find_by_header_message_id(&conn, "account1", "msg123").unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.contains(&"account1:/[Gmail]/All Mail:msg123".to_string()));
        assert!(results.contains(&"account1:/Folder With Spaces:msg123".to_string()));
        assert!(results.contains(&"account1:/Folder/With/Slashes:msg123".to_string()));
    }

    #[test]
    fn test_find_by_header_message_id_gmail_style_folders() {
        let (conn, mut known_years) = setup_test_db();

        // Insert Gmail-style virtual folder entries (the original bug scenario)
        insert_test_message(&conn, &mut known_years, "account1:/[Gmail]/All Mail:test@example.com", "Gmail Test", 1000);
        insert_test_message(&conn, &mut known_years, "account1:/[Gmail]/Important:test@example.com", "Gmail Test", 1001);
        insert_test_message(&conn, &mut known_years, "account1:/[Gmail]/Starred:test@example.com", "Gmail Test", 1002);
        insert_test_message(&conn, &mut known_years, "account1:/[Gmail]/Trash:test@example.com", "Gmail Test", 1003);
        insert_test_message(&conn, &mut known_years, "account1:/INBOX:test@example.com", "Gmail Test", 1004);

        // When a message is deleted, we might only know the headerMessageId
        // This function should find all occurrences regardless of folder
        let results = find_by_header_message_id(&conn, "account1", "test@example.com").unwrap();

        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_get_message_by_msgid() {
        let (conn, mut known_years) = setup_test_db();

        insert_test_message(&conn, &mut known_years, "account1:/INBOX:msg123", "Test Subject", 1699900000000);

        // Test exact match
        let result = get_message_by_msgid(&conn, "account1:/INBOX:msg123").unwrap();
        assert!(result.is_some());
        let msg = result.unwrap();
        assert_eq!(msg["msgId"], "account1:/INBOX:msg123");
        assert_eq!(msg["subject"], "Test Subject");

        // Test non-existent message
        let result = get_message_by_msgid(&conn, "account1:/INBOX:nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_year_from_date_ms() {
        // Normal dates
        assert_eq!(year_from_date_ms(1704067200000), 2024); // 2024-01-01T00:00:00Z
        assert_eq!(year_from_date_ms(1672531200000), 2023); // 2023-01-01T00:00:00Z
        assert_eq!(year_from_date_ms(946684800000), 2000); // 2000-01-01T00:00:00Z
        assert_eq!(year_from_date_ms(852076800000), 1997); // 1997-01-01T00:00:00Z — pre-2000 preserved

        // Edge cases — only dateMs=0/negative gets clamped
        assert_eq!(year_from_date_ms(0), config::sqlite::SHARD_MIN_YEAR); // epoch → clamped
        assert_eq!(year_from_date_ms(-1), config::sqlite::SHARD_MIN_YEAR); // negative → clamped
    }

    #[test]
    fn test_load_known_years() {
        let (conn, known_years) = setup_test_db();

        let loaded = load_known_years(&conn).unwrap();
        assert_eq!(loaded, known_years);
        assert!(loaded.contains(&2000));
        assert!(loaded.contains(&2023));
    }

    #[test]
    fn test_multi_year_sharding() {
        let (conn, mut known_years) = setup_test_db();

        // Insert messages spanning multiple years
        insert_test_message(&conn, &mut known_years, "acc:/:msg1", "Old Message", 946684800000); // 2000-01-01
        insert_test_message(&conn, &mut known_years, "acc:/:msg2", "Recent Message", 1704067200000); // 2024-01-01

        // Verify messages are in different shards
        assert!(known_years.contains(&2000));
        assert!(known_years.contains(&2024));

        // Both messages should be findable
        let result1 = get_message_by_msgid(&conn, "acc:/:msg1").unwrap();
        assert!(result1.is_some());
        assert_eq!(result1.unwrap()["subject"], "Old Message");

        let result2 = get_message_by_msgid(&conn, "acc:/:msg2").unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap()["subject"], "Recent Message");

        // db_count should count across all shards
        let count = db_count(&conn).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_search_across_year_shards() {
        let (conn, mut known_years) = setup_test_db();
        let synonyms = SynonymLookup::new();

        // Insert messages with searchable content across 3 different year shards
        insert_test_message(&conn, &mut known_years, "acc:/:msg1", "Budget analysis for Q1", 946684800000);  // 2000-01-01
        insert_test_message(&conn, &mut known_years, "acc:/:msg2", "Budget review meeting", 1672531200000);  // 2023-01-01
        insert_test_message(&conn, &mut known_years, "acc:/:msg3", "Budget forecast update", 1704067200000); // 2024-01-01
        insert_test_message(&conn, &mut known_years, "acc:/:msg4", "Unrelated email topic", 1704153600000);  // 2024-01-02

        // Verify we have 3 distinct shards
        assert!(known_years.contains(&2000));
        assert!(known_years.contains(&2023));
        assert!(known_years.contains(&2024));

        // Search across all shards using the public search() function (FTS-only path, no engine)
        let params = serde_json::json!({"ignoreDate": true});
        let results = search(&conn, "budget", &params, &synonyms, None, &known_years).unwrap();

        // Should find exactly 3 results (the 3 budget messages, not the unrelated one)
        assert_eq!(results.len(), 3, "Expected 3 budget results across 3 shards, got {}", results.len());

        // Results should be sorted by dateMs DESC (newest first)
        let dates: Vec<i64> = results.iter().map(|r| r["dateMs"].as_i64().unwrap()).collect();
        assert!(dates[0] >= dates[1] && dates[1] >= dates[2],
            "Results should be sorted by date DESC: {:?}", dates);

        // All 3 budget subjects should be present
        let subjects: Vec<&str> = results.iter().map(|r| r["subject"].as_str().unwrap()).collect();
        assert!(subjects.iter().any(|s| s.contains("forecast")), "Missing 2024 result");
        assert!(subjects.iter().any(|s| s.contains("review")), "Missing 2023 result");
        assert!(subjects.iter().any(|s| s.contains("analysis")), "Missing 2000 result");
    }
}
