/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use chrono::{DateTime, Datelike, Local, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{config, embeddings::engine::EmbeddingEngine, fts::hybrid::SearchSort, fts::query::build_fts_match, fts::synonyms::SynonymLookup};

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

/// The current shard schema. `attachmentNames` is last so every existing column
/// keeps its position (bm25 weights and snippet indexes are positional).
fn shard_create_sql(table: &str) -> String {
    format!(
        r#"CREATE VIRTUAL TABLE IF NOT EXISTS {table} USING fts5(
            msgId,
            subject, from_, to_, cc, bcc, body, attachmentNames,
            tokenize = "{tokenize}",
            prefix = '{prefix}'
        )"#,
        table = table,
        tokenize = config::sqlite::FTS_TOKENIZE,
        prefix = config::sqlite::FTS_PREFIXES,
    )
}

/// Whether a shard has the `attachmentNames` column. Shards created before it
/// lack it until `rebuild_next_stale_shard` converts them.
fn shard_has_attachment_names(conn: &Connection, table: &str) -> anyhow::Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let cols = stmt.query_map([], |r| r.get::<_, String>(1))?;
    for col in cols {
        if col? == "attachmentNames" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_shard(conn: &Connection, year: i32, known_years: &mut HashSet<i32>) -> anyhow::Result<()> {
    if known_years.contains(&year) {
        return Ok(());
    }
    let table = fts_table_name(year);
    conn.execute_batch(&shard_create_sql(&table))?;
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

/// Year shards whose schema is out of date: created with an outdated `tokenize=`
/// string (the stored CREATE statement in sqlite_master still carries
/// `tokenchars`) or without the `attachmentNames` column. Detection is stateless
/// and needs NO schema version — bumping SCHEMA_VERSION would make the addon run
/// a FULL re-index from Thunderbird (see config.rs). Newest years first.
fn stale_shard_years(conn: &Connection) -> anyhow::Result<Vec<i32>> {
    let mut stmt = conn.prepare(
        r"SELECT name, sql FROM sqlite_master WHERE type='table' AND name LIKE 'messages\_fts\_%' ESCAPE '\'",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
    })?;
    let mut years = Vec::new();
    for row in rows {
        let (name, sql) = row?;
        let stale_tokenizer = sql.unwrap_or_default().contains("tokenchars");
        if !stale_tokenizer && shard_has_attachment_names(conn, &name)? {
            continue;
        }
        if let Some(suffix) = name.strip_prefix("messages_fts_") {
            if let Ok(year) = suffix.parse::<i32>() {
                years.push(year);
            }
        }
    }
    years.sort_unstable_by(|a, b| b.cmp(a));
    Ok(years)
}

/// In-place shard migration: rebuild the newest stale shard not yet in
/// `attempted` with the current schema (`FTS_TOKENIZE`, `attachmentNames`
/// column) via a rowid-preserving copy (FTS5 tables store their content, so
/// `INSERT .. SELECT` re-tokenizes locally — NO re-feed from Thunderbird). Rows
/// copied from a shard without the column get an empty `attachmentNames`.
/// One transaction per shard: crash-safe, idempotent, message_meta/messages_vec
/// rowid alignment untouched.
///
/// The writer thread calls this while it has no request waiting, never `init`:
/// one large shard can take longer than the addon's init RPC timeout, and a
/// timed-out init kills the helper and rolls the shard back, every start. A
/// write request that arrives mid-shard waits for that shard.
///
/// Returns false when no stale shard is left outside `attempted`. Each shard
/// is attempted once per process; a failed one is logged and retried on the
/// next start, so a broken shard cannot keep the writer busy.
pub fn rebuild_next_stale_shard(conn: &mut Connection, attempted: &mut HashSet<i32>) -> anyhow::Result<bool> {
    let Some(year) = stale_shard_years(conn)?.into_iter().find(|y| !attempted.contains(y)) else {
        return Ok(false);
    };
    attempted.insert(year);
    let shard_start = std::time::Instant::now();
    let table = fts_table_name(year);
    let tmp = format!("{table}_retok");
    let result = (|| -> anyhow::Result<()> {
        let tx = conn.transaction()?;
        tx.execute_batch(&format!("DROP TABLE IF EXISTS {tmp}"))?;
        tx.execute_batch(&shard_create_sql(&tmp))?;
        // rowid-preserving copy; SQLite streams rows internally (bounded memory)
        let columns = if shard_has_attachment_names(&tx, &table)? {
            "rowid, msgId, subject, from_, to_, cc, bcc, body, attachmentNames"
        } else {
            "rowid, msgId, subject, from_, to_, cc, bcc, body"
        };
        tx.execute_batch(&format!(
            "INSERT INTO {tmp}({columns}) SELECT {columns} FROM {table}"
        ))?;
        tx.execute_batch(&format!("DROP TABLE {table}"))?;
        tx.execute_batch(&format!("ALTER TABLE {tmp} RENAME TO {table}"))?;
        // Re-apply the write tuning ensure_shard sets on creation
        tx.execute(
            &format!("INSERT INTO {table}({table}, rank) VALUES('automerge', ?1)"),
            params![config::sqlite::FTS_AUTOMERGE],
        )?;
        tx.execute(
            &format!("INSERT INTO {table}({table}, rank) VALUES('usermerge', ?1)"),
            params![config::sqlite::FTS_USERMERGE],
        )?;
        tx.commit()?;
        Ok(())
    })();
    match result {
        Ok(()) => log::info!(
            "Shard migration: rebuilt {} in {:.1}s",
            table,
            shard_start.elapsed().as_secs_f64()
        ),
        Err(e) => log::error!("Shard migration: shard {} failed (will retry next start): {:?}", table, e),
    }
    Ok(true)
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

        CREATE TABLE IF NOT EXISTS message_folder_membership (
            msgId TEXT PRIMARY KEY,
            folderId TEXT NOT NULL
        ) WITHOUT ROWID;
        "#,
    )?;
    ensure_folder_membership_schema(conn)?;

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

    log::info!("Database schema initialized (message_meta, message_ids, message_folder_membership, messages_vec, embed_cache; FTS shards created lazily)");
    Ok(())
}

/// Add the exact, opaque folder identity relation without changing or scanning
/// existing msgId rows. The empty relation table and index are constant-size
/// startup DDL; bounded fresh/backfill writes grow them incrementally. A legacy
/// msgId remains unassigned until it has a corresponding relation row.
pub fn ensure_folder_membership_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS message_folder_membership (
            msgId TEXT PRIMARY KEY,
            folderId TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_message_folder_membership_folder_msg
            ON message_folder_membership(folderId, msgId);
        "#,
    )?;
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

    // Existing sharded and monolithic databases take migration paths that do
    // not call init_database(), so apply the additive relation on every open.
    ensure_folder_membership_schema(&conn)?;

    // Shards created with an outdated tokenize= string or without the
    // attachmentNames column convert later on the writer thread
    // (rebuild_next_stale_shard), never here: init must answer within the
    // addon's RPC timeout. NO SCHEMA_VERSION bump — the addon must never see
    // this (a version change triggers a full re-index from Thunderbird).

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

    // Validate every supplied relation before any shard DDL or row write. The
    // field is optional for older addons, but malformed/empty identities must
    // fail the whole request rather than create rows invisible to exact scans.
    for row in rows {
        validate_optional_folder_id(row)?;
    }

    // Pre-create any new shards needed outside the transaction (DDL in FTS5 is auto-commit).
    // Collect the years we'll need so we can ensure shards exist.
    for row in rows {
        let date_ms = row.get("dateMs").and_then(|v| v.as_i64()).unwrap_or(0);
        let year = year_from_date_ms(date_ms);
        ensure_shard(conn, year, known_years)?;
    }

    // A shard not yet converted by rebuild_next_stale_shard has no attachmentNames
    // column; rows written to it keep the old column set.
    let mut shard_has_names: HashMap<i32, bool> = HashMap::new();
    for row in rows {
        let year = year_from_date_ms(row.get("dateMs").and_then(|v| v.as_i64()).unwrap_or(0));
        if !shard_has_names.contains_key(&year) {
            shard_has_names.insert(year, shard_has_attachment_names(conn, &fts_table_name(year))?);
        }
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

        let incoming_folder_id = validate_optional_folder_id(row)?;
        let changed = tx.execute(
            "INSERT OR IGNORE INTO message_ids (msgId) VALUES (?1)",
            params![msg_id_val],
        )?;
        if changed == 0 {
            if let Some(incoming_folder_id) = incoming_folder_id {
                let stored_folder_id: Option<String> = tx
                    .query_row(
                        "SELECT folderId FROM message_folder_membership WHERE msgId = ?1",
                        params![msg_id_val],
                        |row| row.get(0),
                    )
                    .optional()?;
                match stored_folder_id.as_deref() {
                    None => {
                        tx.execute(
                            "INSERT INTO message_folder_membership (msgId, folderId) VALUES (?1, ?2)",
                            params![msg_id_val, incoming_folder_id],
                        )?;
                    }
                    Some(stored) if stored == incoming_folder_id => {}
                    Some(stored) => {
                        bail!(
                            "conflicting folderId for msgId {}: stored {}, incoming {}",
                            truncate_for_log(msg_id_val),
                            truncate_for_log(stored),
                            truncate_for_log(incoming_folder_id)
                        );
                    }
                }
            }
            skipped_duplicates += 1;
            log::debug!("Skipping duplicate msgId: {}...", truncate_for_log(msg_id_val));
            continue;
        }

        if let Some(folder_id) = incoming_folder_id {
            tx.execute(
                "INSERT INTO message_folder_membership (msgId, folderId) VALUES (?1, ?2)",
                params![msg_id_val, folder_id],
            )?;
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
        let attachment_names = row.get("attachmentNames").and_then(|v| v.as_str()).unwrap_or("");

        let date_ms = row.get("dateMs").and_then(|v| v.as_i64()).unwrap_or(0);
        let year = year_from_date_ms(date_ms);
        let table = fts_table_name(year);

        if shard_has_names[&year] {
            tx.execute(
                &format!(
                    "INSERT INTO {table} (rowid, msgId, subject, from_, to_, cc, bcc, body, attachmentNames) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
                ),
                params![row_id, msg_id_val, subject, from_, to_, cc, bcc, body, attachment_names],
            )?;
        } else {
            tx.execute(
                &format!(
                    "INSERT INTO {table} (rowid, msgId, subject, from_, to_, cc, bcc, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
                ),
                params![row_id, msg_id_val, subject, from_, to_, cc, bcc, body],
            )?;
        }

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

fn validate_optional_folder_id(row: &Value) -> anyhow::Result<Option<&str>> {
    match row.get("folderId") {
        None => Ok(None),
        Some(Value::String(folder_id)) if !folder_id.is_empty() => Ok(Some(folder_id.as_str())),
        Some(_) => bail!("folderId must be a non-empty string when present"),
    }
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

/// Fields a mail search may scope to (`from:alice`): typed name → shard column. Each column
/// is in `COLUMN_SCOPE_PREFIXES`, so a scoped hybrid search filters its vector candidates.
const QUERY_COLUMNS: &[(&str, &str)] = &[
    ("subject", "subject"),
    ("from", "from_"),
    ("from_", "from_"),
    ("to", "to_"),
    ("to_", "to_"),
    ("cc", "cc"),
    ("bcc", "bcc"),
    ("body", "body"),
];

// Column-scope prefixes in the FTS5 queries `build_fts_match` writes.
const COLUMN_SCOPE_PREFIXES: &[&str] = &["from_:", "to_:", "subject:", "cc:", "bcc:", "body:"];

/// Check if a processed FTS5 query contains column-scoped terms.
fn query_has_column_scope(fts_query: &str) -> bool {
    COLUMN_SCOPE_PREFIXES.iter().any(|p| fts_query.contains(p))
}

/// Extract only the column-scoped terms from a processed FTS5 query.
/// Given `from_:"alice@example.com" hiring*`, returns `from_:"alice@example.com"`.
fn extract_column_scope_filter(fts_query: &str) -> String {
    split_column_scope(fts_query).0.join(" ")
}

/// Whether a processed FTS5 query is only column-scoped terms, with operators between them: no free
/// word or phrase (`from:alice@example.com`, `from:a@example.com OR to:a@example.com`).
fn query_is_field_only(fts_query: &str) -> bool {
    let (scoped_terms, has_free_text, _) = split_column_scope(fts_query);
    !scoped_terms.is_empty() && !has_free_text
}

/// Whether a processed FTS5 query excludes something (a kept NOT): `lunch NOT from_:"erin@example.com"`,
/// `"vacation"* NOT "itinerary"*`. A search by meaning can't exclude, so such a search answers by
/// keyword (issue #21).
pub(crate) fn query_excludes(fts_query: &str) -> bool {
    split_column_scope(fts_query).2
}

/// A processed FTS5 query's column-scoped terms, whether it has any other term than an operator, and
/// whether it keeps a NOT. Every free word, phrase and group `build_fts_match` writes starts with a
/// quote or a parenthesis, so an operator is the only bare word outside them.
fn split_column_scope(fts_query: &str) -> (Vec<String>, bool, bool) {
    let mut scoped_terms: Vec<String> = Vec::new();
    let mut has_free_text = false;
    let mut excludes = false;
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
                // Quoted value: field:"value" or prefix form field:"value"*
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1; // closing quote
                }
                if i < bytes.len() && bytes[i] == b'*' {
                    i += 1; // prefix star — keep so the filter query preserves
                            // the main query's prefix semantics
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
            has_free_text |= bytes[i] == b'"' || bytes[i] == b'(';
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
                let start = i;
                while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                excludes |= &fts_query[start..i] == "NOT";
            }
        }
    }

    (scoped_terms, has_free_text, excludes)
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
    // One read transaction, so the search's queries all read the archive as it was when it began:
    // a write landing between them (a message removed and its rowid reused) can't put another
    // message, or one outside the dates, in its results.
    let tx = conn.unchecked_transaction()?;
    let results = search_rows(&tx, q, params, synonyms, engine, known_years)?;
    drop(tx);
    Ok(results)
}

fn search_rows(
    conn: &Connection,
    q: &str,
    params: &Value,
    synonyms: &SynonymLookup,
    engine: Option<&EmbeddingEngine>,
    known_years: &HashSet<i32>,
) -> anyhow::Result<Vec<Value>> {
    let sort = SearchSort::from_params(params)?;
    let query = q.trim();
    if query.is_empty() {
        return Ok(vec![]);
    }

    let limit = cap_search_limit(
        params
            .get("limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(config::sqlite::SEARCH_DEFAULT_LIMIT),
    );

    let fts_query = build_fts_match(Some(query), QUERY_COLUMNS, true, synonyms);
    log::info!(
        "Search: \"{}\" -> FTS \"{}\"",
        query,
        fts_query
    );

    // Only fields (`from:a@example.com`): nothing for the vector leg to mean, so the answer is by
    // keyword over every match, in sort order. A field names which messages, not how well they match:
    // its matches' keyword scores (each year shard's statistics, the field's length, how often the
    // value is in it) are no measure of relevance, so for relevance they are tied: newest first, and
    // "the latest from X" is exact under the default sort.
    if query_is_field_only(&fts_query) {
        let order = if sort == SearchSort::Relevance { SearchSort::DateDesc } else { sort };
        log::info!("Search of fields only, by keyword in {order:?} order");
        return search_fts_only(conn, &fts_query, params, limit, known_years, order);
    }

    // By keyword without the model, and for a query that excludes a sender or a word: the vector leg
    // would return the nearest by meaning, the excluded mail among them (issue #21).
    let engine = match engine {
        Some(e) if !query_excludes(&fts_query) => e,
        _ => return search_fts_only(conn, &fts_query, params, limit, known_years, sort),
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

    // A date sort orders all of a larger pool's matches by date, so the newest (oldest) match is
    // found even when its score is far from the best.
    let candidate_limit = match sort {
        SearchSort::Relevance => limit * config::hybrid::CANDIDATE_MULTIPLIER,
        SearchSort::DateDesc | SearchSort::DateAsc => config::hybrid::DATE_SORT_POOL,
    };

    // --- FTS5 candidates (across all year shards) ---
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

    // --- Vector candidates: the nearest within the dates (and the field's matches) ---
    let query_embedding = engine.embed(query)?;
    let query_blob = f32_vec_to_blob(&query_embedding);
    let vec_candidates = search_vec_candidates(
        conn,
        "messages_vec",
        "message_meta",
        &query_blob,
        candidate_limit,
        from_ts,
        to_ts,
        eligible_rowids.as_ref(),
    )
    .unwrap_or_else(|e| {
        log::warn!("Vector search failed, searching by keyword only: {e}");
        vec![]
    });

    // Fall back to FTS-only when there are no vector candidates: the vec table is empty (e.g., during
    // embedding rebuild), or no vector is within the dates (or among the field's matches).
    // Without this, hybrid weights (text_weight=0.3) penalize text-only results below MIN_SCORE.
    if vec_candidates.is_empty() {
        log::info!("No vector candidates (vec table may be empty, or none within the dates or the field), falling back to FTS-only search");
        return search_fts_only(conn, &fts_query, params, limit, known_years, sort);
    }

    // --- Merge ---
    let text_pairs: Vec<(i64, f64)> = fts_candidates.iter().map(|c| (c.rowid, c.rank)).collect();
    let mut dates: HashMap<i64, i64> = fts_candidates.iter().map(|c| (c.rowid, c.date_ms)).collect();
    let vector_only = vec_candidates.iter().map(|&(rowid, _)| rowid).filter(|rowid| !dates.contains_key(rowid));
    let vector_only_dates = fetch_dates(conn, "message_meta", vector_only)?;
    dates.extend(vector_only_dates);
    let merged = crate::fts::hybrid::merge_results(
        &text_pairs,
        &vec_candidates,
        &dates,
        config::hybrid::EMAIL_VECTOR_WEIGHT,
        config::hybrid::EMAIL_TEXT_WEIGHT,
        sort,
        limit as usize,
    );

    // Words matched but the merge kept nothing (a word in at least half the messages scores about 0 by keyword, and a
    // weak vector match adds too little to reach MIN_SCORE): answer by keyword, as with no vectors.
    if merged.is_empty() && !text_pairs.is_empty() {
        log::info!("Hybrid merge kept none of {} keyword matches, falling back to FTS-only search", text_pairs.len());
        return search_fts_only(conn, &fts_query, params, limit, known_years, sort);
    }

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
            // Vector-only result (within the dates and the field: sqlite-vec searched only those)
            if let Some(meta) = fetch_message_meta(conn, hr.rowid)? {
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

/// FTS-only search across all year shards using a single UNION ALL query: every keyword match
/// within the dates in `sort` order (relevance: the best keyword match first, ties newer first;
/// a date: ties the better keyword match first), then the `limit`.
fn search_fts_only(
    conn: &Connection,
    fts_query: &str,
    params: &Value,
    limit: i64,
    known_years: &HashSet<i32>,
    sort: SearchSort,
) -> anyhow::Result<Vec<Value>> {
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
                bm25({table}, 0.0, 5.0, 3.0, 2.0, 1.0, 1.0, 1.0, 3.0) AS rank
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

    let order = match sort {
        SearchSort::Relevance => "rank ASC, dateMs DESC",
        SearchSort::DateDesc => "dateMs DESC, rank ASC",
        SearchSort::DateAsc => "dateMs ASC, rank ASC",
    };
    let sql = format!(
        "{} ORDER BY {order} LIMIT ?{lp}",
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
                bm25({table}, 0.0, 5.0, 3.0, 2.0, 1.0, 1.0, 1.0, 3.0) AS rank
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

/// A search's positive `limit`, at most `SEARCH_LIMIT_MAX`: past it, the vector candidates the search
/// asks for are more than sqlite-vec answers, and it would search by keyword only.
pub(crate) fn cap_search_limit(limit: i64) -> i64 {
    if limit > config::hybrid::SEARCH_LIMIT_MAX {
        log::info!("Search limit {limit} capped to {}", config::hybrid::SEARCH_LIMIT_MAX);
    }
    limit.min(config::hybrid::SEARCH_LIMIT_MAX)
}

/// The `k` nearest vectors in a vec0 table. With dates (either bound alone, or both, each inclusive)
/// or `eligible` rowids (an email query naming a field), only rows of `meta_table` dated within the
/// dates and among those rowids: sqlite-vec searches just them (`rowid IN`), so a match inside them is
/// found however many nearer rows lie outside. sqlite-vec refuses a `k` over `VEC_K_MAX`, which
/// `cap_search_limit` keeps a search's within.
#[allow(clippy::too_many_arguments)]
pub(crate) fn search_vec_candidates(
    conn: &Connection,
    vec_table: &str,
    meta_table: &str,
    query_blob: &[u8],
    k: i64,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
    eligible: Option<&HashSet<i64>>,
) -> anyhow::Result<Vec<(i64, f64)>> {
    let read = |r: &rusqlite::Row| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?));
    if from_ts.is_none() && to_ts.is_none() && eligible.is_none() {
        let mut stmt = conn.prepare(&format!("SELECT rowid, distance FROM {vec_table} WHERE embedding MATCH ?1 AND k = ?2"))?;
        let rows = stmt.query_map(params![query_blob, k], read)?;
        return rows.collect::<Result<Vec<_>, _>>().map_err(Into::into);
    }
    let sql = format!(
        "SELECT rowid, distance FROM {vec_table} WHERE embedding MATCH ?1 AND k = ?2 AND rowid IN \
         (SELECT rowid FROM {meta_table} WHERE dateMs >= ?3 AND dateMs <= ?4 \
          AND (?5 IS NULL OR rowid IN (SELECT value FROM json_each(?5))))"
    );
    let eligible_json = eligible.map(|e| serde_json::to_string(&e.iter().collect::<Vec<_>>())).transpose()?;
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params![query_blob, k, from_ts.unwrap_or(i64::MIN), to_ts.unwrap_or(i64::MAX), eligible_json],
        read,
    )?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// The dateMs of each of `rowids` in `meta_table` (`message_meta`, `memory_meta`), in one query; a
/// rowid with no row there is left out.
pub(crate) fn fetch_dates(
    conn: &Connection,
    meta_table: &str,
    rowids: impl Iterator<Item = i64>,
) -> anyhow::Result<HashMap<i64, i64>> {
    let rowids_json = serde_json::to_string(&rowids.collect::<Vec<_>>())?;
    let mut stmt = conn.prepare(&format!(
        "SELECT rowid, dateMs FROM {meta_table} WHERE rowid IN (SELECT value FROM json_each(?1))"
    ))?;
    let rows = stmt.query_map(params![rowids_json], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
    rows.collect::<Result<HashMap<_, _>, _>>().map_err(Into::into)
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
/// Takes ownership of the writer's connection to close it. The caller must have closed every
/// other connection to the file first (the reader's): Windows refuses to delete an open file.
/// Returns the writer's new connection and whether the files were deleted. When they were not,
/// the new connection is to the old database with every row still in it. The caller empties
/// known_years either way: a shard still there is only verified again.
pub fn clear_rebuild_standalone(db_path: &Path, conn: Connection) -> anyhow::Result<(Connection, anyhow::Result<()>)> {
    log::info!("Clearing email FTS by deleting database file (rebuild from scratch)");
    drop(conn);
    log::info!("Database connection closed");

    let deleted = delete_database_files(db_path);

    log::info!("Recreating database...");
    let new_conn = open_writer_connection(db_path)?;
    log::info!("Database recreated and initialized successfully (FTS shards will be created lazily)");
    Ok((new_conn, deleted))
}

/// The writer's connection to the email database at `db_path`, created empty when the file is
/// not there: what a clear opens after deleting the files, and what the writer opens again when
/// a clear failed before it could.
pub fn open_writer_connection(db_path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(db_path)?;
    ensure_fts5_available(&conn)?;
    init_database(&conn)?;
    Ok(conn)
}

/// Delete a database's write-ahead log, its shared-memory file, then the database itself.
/// The database goes last, so a failure leaves it in place, and a new database is never
/// opened next to an old log.
pub(crate) fn delete_database_files(db_path: &Path) -> anyhow::Result<()> {
    for p in [
        PathBuf::from(format!("{}-wal", db_path.display())),
        PathBuf::from(format!("{}-shm", db_path.display())),
        db_path.to_path_buf(),
    ] {
        if p.exists() {
            std::fs::remove_file(&p).with_context(|| format!("delete {}", p.display()))?;
            log::info!("Deleted {}", p.display());
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

/// What one `removeBatch` deleted: the row count, the distinct folders whose
/// exact membership rows were deleted, and how many deleted rows had none.
#[derive(Debug, Default, PartialEq)]
pub struct RemoveBatchOutcome {
    pub count: i64,
    pub removed_folder_ids: Vec<String>,
    pub removed_ownerless: i64,
}

pub fn remove_batch(conn: &mut Connection, ids: &[Value]) -> anyhow::Result<RemoveBatchOutcome> {
    if ids.is_empty() {
        return Ok(RemoveBatchOutcome::default());
    }

    let ids: Vec<String> = ids
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    log::info!("Removing {} messages from index", ids.len());

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut removed: i64 = 0;
    let mut removed_folder_ids: BTreeSet<String> = BTreeSet::new();
    let mut removed_ownerless: i64 = 0;

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
            let owner: Option<String> = tx
                .query_row(
                    "DELETE FROM message_folder_membership WHERE msgId = ?1 RETURNING folderId",
                    params![msg_id_val],
                    |r| r.get(0),
                )
                .optional()?;
            match owner {
                Some(folder_id) => {
                    removed_folder_ids.insert(folder_id);
                }
                None => removed_ownerless += 1,
            }
            tx.execute("DELETE FROM message_ids WHERE msgId = ?1", params![msg_id_val])?;
            removed += 1;
        }
    }

    tx.commit()?;
    log::info!("Removed {} messages", removed);
    Ok(RemoveBatchOutcome {
        count: removed,
        removed_folder_ids: removed_folder_ids.into_iter().collect(),
        removed_ownerless,
    })
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
    // Rows copied in by a shard migration have a NULL attachmentNames.
    let names_column = if shard_has_attachment_names(conn, &table)? { "COALESCE(attachmentNames, '')" } else { "''" };
    let fts_row: Option<(String, String, String, String, String, String, String)> = conn
        .query_row(
            &format!("SELECT subject, from_, to_, cc, bcc, body, {names_column} FROM {table} WHERE rowid = ?1"),
            params![row_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
        )
        .optional()?;
    let Some((subject, from_, to_, cc, bcc, body, attachment_names)) = fts_row else {
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
        "attachmentNames": attachment_names,
        "hasAttachments": has_attachments,
        "parsedIcsAttachments": parsed_ics.unwrap_or_default(),
        "dateMs": date_ms
    })))
}

/// Each msgId's stored `hasAttachments` flag, in request order: true / false, or null when the
/// msgId is not indexed. Reads only `message_ids` and `message_meta`, never a shard or a body.
pub fn get_attachment_flags(conn: &Connection, msg_ids: &[&str]) -> anyhow::Result<Value> {
    let mut stmt = conn.prepare_cached(
        "SELECT mm.hasAttachments FROM message_ids mi JOIN message_meta mm ON mi.rowid = mm.rowid WHERE mi.msgId = ?1",
    )?;
    let mut flags = Vec::with_capacity(msg_ids.len());
    for msg_id in msg_ids {
        let flag: Option<i64> = stmt.query_row(params![msg_id], |r| r.get(0)).optional()?;
        flags.push(flag.map_or(Value::Null, |v| Value::Bool(v != 0)));
    }
    Ok(serde_json::json!({ "ok": true, "flags": flags }))
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

/// Count msgIds in the half-open key range [start_key, end_key).
/// PK index range scan on the unsharded `message_ids` table — the caller
/// (addon) computes the bounds; this function does NO msgId parsing.
/// Used by the per-folder count-invariant reconcile (PLAN_FOLDER_SET_RECONCILE.md):
/// for a folder prefix `P = "<accountId>:<folderPath>:"`, the addon passes
/// `start_key = P` and `end_key = P[..len-1] + ";"` (';' = ':'+1), which
/// covers exactly that folder's keys — subfolder keys (".../INBOX/sub:...")
/// sort BEFORE ".../INBOX:" ('/' < ':') and are correctly excluded.
pub fn count_msg_id_range(conn: &Connection, start_key: &str, end_key: &str) -> anyhow::Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM message_ids WHERE msgId >= ?1 AND msgId < ?2",
        params![start_key, end_key],
        |r| r.get(0),
    )?)
}

/// Return a deterministic SHA-256 fingerprint of all msgIds in the half-open
/// key range [start_key, end_key). Keys are read in SQLite BINARY order and
/// framed as an unsigned 64-bit big-endian byte length followed by their UTF-8
/// bytes. The framing prevents concatenation ambiguity and is mirrored by the
/// Thunderbird add-on when it fingerprints the folder msgDB.
pub fn fingerprint_msg_id_range(
    conn: &Connection,
    start_key: &str,
    end_key: &str,
) -> anyhow::Result<Value> {
    let mut stmt = conn.prepare(
        "SELECT msgId FROM message_ids WHERE msgId >= ?1 AND msgId < ?2 ORDER BY msgId"
    )?;
    let rows = stmt.query_map(params![start_key, end_key], |r| r.get::<_, String>(0))?;

    let mut hasher = Sha256::new();
    let mut count: i64 = 0;
    for row in rows {
        let msg_id = row?;
        let bytes = msg_id.as_bytes();
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
        count += 1;
    }

    Ok(serde_json::json!({
        "ok": true,
        "count": count,
        "sha256": hex::encode(hasher.finalize()),
    }))
}

/// List msgIds in the half-open key range [start_key, end_key), ascending,
/// paginated. `after_key` is the exclusive pagination cursor (the last msgId
/// of the previous page); when it lies at or above `start_key` it replaces
/// the inclusive lower bound. `done` is true when fewer than `limit` rows
/// were returned (the range is exhausted).
pub fn list_msg_id_range(
    conn: &Connection,
    start_key: &str,
    end_key: &str,
    after_key: Option<&str>,
    limit: i64,
) -> anyhow::Result<Value> {
    let limit = if limit > 0 {
        limit
    } else {
        config::sqlite::LIST_MSG_ID_RANGE_DEFAULT_LIMIT
    };

    // Effective lower bound = max(start_key inclusive, after_key exclusive).
    // Resolved host-side so the PK index sees a plain range predicate.
    let (lower, exclusive): (&str, bool) = match after_key {
        Some(a) if a >= start_key => (a, true),
        _ => (start_key, false),
    };

    let sql = if exclusive {
        "SELECT msgId FROM message_ids WHERE msgId > ?1 AND msgId < ?2 ORDER BY msgId LIMIT ?3"
    } else {
        "SELECT msgId FROM message_ids WHERE msgId >= ?1 AND msgId < ?2 ORDER BY msgId LIMIT ?3"
    };

    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![lower, end_key, limit], |r| r.get::<_, String>(0))?;
    let msg_ids: Vec<String> = rows.collect::<Result<_, _>>()?;
    let done = (msg_ids.len() as i64) < limit;

    Ok(serde_json::json!({ "ok": true, "msgIds": msg_ids, "done": done }))
}

fn validate_folder_id(folder_id: &str) -> anyhow::Result<()> {
    if folder_id.is_empty() {
        bail!("folderId parameter must be a non-empty string");
    }
    Ok(())
}

fn validate_folder_membership_page_limit(limit: i64) -> anyhow::Result<()> {
    if !(1..=config::sqlite::FOLDER_MEMBERSHIP_PAGE_MAX_LIMIT).contains(&limit) {
        bail!(
            "limit must be between 1 and {}",
            config::sqlite::FOLDER_MEMBERSHIP_PAGE_MAX_LIMIT
        );
    }
    Ok(())
}

/// Page exact folder membership in msgId BINARY order. Repeated bounded calls
/// allow an unbounded reconciliation without monopolizing the reader thread.
pub fn list_folder_membership(
    conn: &Connection,
    folder_id: &str,
    after_msg_id: Option<&str>,
    limit: i64,
) -> anyhow::Result<Value> {
    validate_folder_id(folder_id)?;
    validate_folder_membership_page_limit(limit)?;

    let (sql, cursor) = match after_msg_id {
        Some(after) => (
            "SELECT msgId FROM message_folder_membership WHERE folderId = ?1 COLLATE BINARY AND msgId > ?2 COLLATE BINARY ORDER BY msgId COLLATE BINARY LIMIT ?3",
            after,
        ),
        None => (
            "SELECT msgId FROM message_folder_membership WHERE folderId = ?1 COLLATE BINARY ORDER BY msgId COLLATE BINARY LIMIT ?2",
            "",
        ),
    };
    let mut stmt = conn.prepare(sql)?;
    let msg_ids: Vec<String> = if after_msg_id.is_some() {
        stmt.query_map(params![folder_id, cursor, limit], |row| row.get(0))?
            .collect::<Result<_, _>>()?
    } else {
        stmt.query_map(params![folder_id, limit], |row| row.get(0))?
            .collect::<Result<_, _>>()?
    };
    let done = (msg_ids.len() as i64) < limit;
    Ok(serde_json::json!({ "ok": true, "msgIds": msg_ids, "done": done }))
}

/// Page the global msgId keyspace together with its optional exact folder
/// relation. The MATERIALIZED key page applies LIMIT before the LEFT JOIN, so
/// every request inspects at most `limit` archive keys even when almost every
/// key is already assigned. The exclusive BINARY msgId cursor is stateless and
/// resumable; callers use their live-scan epoch to detect insertions at/below a
/// cursor and restart before treating absent live keys as stale.
pub fn list_folder_membership_state(
    conn: &Connection,
    after_msg_id: Option<&str>,
    limit: i64,
) -> anyhow::Result<Value> {
    validate_folder_membership_page_limit(limit)?;

    let (sql, cursor) = match after_msg_id {
        Some(after) => (
            "WITH page AS MATERIALIZED (SELECT msgId FROM message_ids WHERE msgId > ?1 COLLATE BINARY ORDER BY msgId COLLATE BINARY LIMIT ?2) SELECT page.msgId, fm.folderId FROM page LEFT JOIN message_folder_membership fm ON fm.msgId = page.msgId ORDER BY page.msgId COLLATE BINARY",
            after,
        ),
        None => (
            "WITH page AS MATERIALIZED (SELECT msgId FROM message_ids ORDER BY msgId COLLATE BINARY LIMIT ?1) SELECT page.msgId, fm.folderId FROM page LEFT JOIN message_folder_membership fm ON fm.msgId = page.msgId ORDER BY page.msgId COLLATE BINARY",
            "",
        ),
    };
    let mut stmt = conn.prepare(sql)?;
    let entries: Vec<Value> = if after_msg_id.is_some() {
        stmt.query_map(params![cursor, limit], |row| {
            let msg_id: String = row.get(0)?;
            let folder_id: Option<String> = row.get(1)?;
            Ok(serde_json::json!({ "msgId": msg_id, "folderId": folder_id }))
        })?
            .collect::<Result<_, _>>()?
    } else {
        stmt.query_map(params![limit], |row| {
            let msg_id: String = row.get(0)?;
            let folder_id: Option<String> = row.get(1)?;
            Ok(serde_json::json!({ "msgId": msg_id, "folderId": folder_id }))
        })?
            .collect::<Result<_, _>>()?
    };
    let done = (entries.len() as i64) < limit;
    Ok(serde_json::json!({ "ok": true, "entries": entries, "done": done }))
}

/// Count, in one statement (so one read snapshot), what a client's global
/// membership walk would have to repair: `message_ids` rows with no folder
/// relation, and relation rows whose folder is not in `folder_ids`, split by
/// whether the row's msgId account prefix is in `trusted_account_ids`. The
/// prefix is the text before the first `:` when that colon is not the first
/// character; otherwise the account is empty and never trusted. Folder ids are
/// compared, never decoded. The reply is three counts whatever the archive size.
pub fn folder_membership_summary(
    conn: &Connection,
    folder_ids: &[&str],
    trusted_account_ids: &[&str],
) -> anyhow::Result<Value> {
    for folder_id in folder_ids {
        validate_folder_id(folder_id)?;
    }
    if trusted_account_ids.iter().any(|account_id| account_id.is_empty()) {
        bail!("trustedAccountIds entries must be non-empty strings");
    }
    let folder_ids_json = serde_json::to_string(folder_ids)?;
    let trusted_json = serde_json::to_string(trusted_account_ids)?;
    let (ownerless, stray_trusted, stray_untrusted): (i64, i64, i64) = conn.query_row(
        "SELECT \
            (SELECT COUNT(*) FROM message_ids mi WHERE NOT EXISTS (SELECT 1 FROM message_folder_membership fm WHERE fm.msgId = mi.msgId)), \
            COALESCE(SUM(trusted), 0), \
            COALESCE(SUM(1 - trusted), 0) \
         FROM ( \
            SELECT (CASE WHEN instr(fm.msgId, ':') > 1 THEN substr(fm.msgId, 1, instr(fm.msgId, ':') - 1) ELSE '' END) \
                IN (SELECT value FROM json_each(?2)) AS trusted \
            FROM message_folder_membership fm JOIN message_ids mi ON mi.msgId = fm.msgId \
            WHERE fm.folderId NOT IN (SELECT value FROM json_each(?1)) \
         )",
        params![folder_ids_json, trusted_json],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    Ok(serde_json::json!({
        "ok": true,
        "ownerlessRows": ownerless,
        "strayTrustedRows": stray_trusted,
        "strayUntrustedRows": stray_untrusted,
    }))
}

/// Adopt exact folder membership for already-indexed rows. The batch is
/// transactional: NULL adopts, equal non-NULL is idempotent, missing msgIds are
/// counted no-ops, and a conflicting non-NULL relation fails closed and rolls
/// back every adoption.
pub fn assign_folder_membership_batch(
    conn: &mut Connection,
    assignments: &Value,
) -> anyhow::Result<(i64, i64, i64)> {
    let assignments = assignments
        .as_array()
        .context("assignments parameter is required and must be an array")?;
    if assignments.is_empty() {
        bail!("assignments must contain at least one item");
    }
    if assignments.len() > config::sqlite::ASSIGN_FOLDER_MEMBERSHIP_BATCH_MAX {
        bail!(
            "assignments exceeds maximum batch size {}",
            config::sqlite::ASSIGN_FOLDER_MEMBERSHIP_BATCH_MAX
        );
    }

    let mut validated = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let msg_id = assignment
            .get("msgId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("assignment msgId is required and must be a non-empty string")?;
        let folder_id = assignment
            .get("folderId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("assignment folderId is required and must be a non-empty string")?;
        validated.push((msg_id, folder_id));
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut assigned: i64 = 0;
    let mut already_assigned: i64 = 0;
    let mut missing: i64 = 0;
    for (msg_id, incoming_folder_id) in validated {
        let indexed: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM message_ids WHERE msgId = ?1)",
            params![msg_id],
            |row| row.get(0),
        )?;
        if !indexed {
            missing += 1;
            continue;
        }
        let stored_folder_id: Option<String> = tx
            .query_row(
                "SELECT folderId FROM message_folder_membership WHERE msgId = ?1",
                params![msg_id],
                |row| row.get(0),
            )
            .optional()?;
        match stored_folder_id {
            None => {
                tx.execute(
                    "INSERT INTO message_folder_membership (msgId, folderId) VALUES (?1, ?2)",
                    params![msg_id, incoming_folder_id],
                )?;
                assigned += 1;
            }
            Some(stored) if stored == incoming_folder_id => {
                already_assigned += 1;
            }
            Some(stored) => {
                bail!(
                    "conflicting folderId for msgId {}: stored {}, incoming {}",
                    truncate_for_log(msg_id),
                    truncate_for_log(&stored),
                    truncate_for_log(incoming_folder_id)
                );
            }
        }
    }
    tx.commit()?;
    Ok((assigned, already_assigned, missing))
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
pub(crate) mod tests {
    use super::*;
    use rusqlite::{ffi, Connection};
    use std::ffi::CStr;
    use std::os::raw::{c_char, c_int, c_void};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    unsafe extern "C" fn count_message_ids_reads(
        context: *mut c_void,
        action: c_int,
        table_name: *const c_char,
        _column_name: *const c_char,
        _database_name: *const c_char,
        _trigger_name: *const c_char,
    ) -> c_int {
        if action == ffi::SQLITE_READ && !table_name.is_null() {
            let table_name = unsafe { CStr::from_ptr(table_name) };
            if table_name.to_bytes() == b"message_ids" {
                let reads = unsafe { &*(context as *const AtomicUsize) };
                reads.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }
        ffi::SQLITE_OK
    }

    unsafe extern "C" fn count_vm_steps(context: *mut c_void) -> c_int {
        let steps = unsafe { &*(context as *const AtomicUsize) };
        steps.fetch_add(1, AtomicOrdering::SeqCst);
        0
    }

    /// What the writer thread does while idle, until nothing is left to convert.
    fn convert_all_stale_shards(conn: &mut Connection) {
        let mut attempted = HashSet::new();
        while rebuild_next_stale_shard(conn, &mut attempted).unwrap() {}
    }

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

            CREATE TABLE IF NOT EXISTS message_folder_membership (
                msgId TEXT PRIMARY KEY,
                folderId TEXT NOT NULL
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_message_folder_membership_folder_msg
                ON message_folder_membership(folderId, msgId);
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
    fn test_tokenizer_shard_rebuild_in_place() {
        // Seed a shard with the OLD tokenchars tokenizer, then verify the
        // in-place rebuild re-tokenizes it (rowid preserved, parts matchable)
        // WITHOUT any schema version involvement.
        let mut conn = Connection::open_in_memory().unwrap();
        let old_tokenize = "porter unicode61 remove_diacritics 2 tokenchars '-_.@'";
        conn.execute_batch(&format!(
            r#"CREATE VIRTUAL TABLE messages_fts_2001 USING fts5(
                msgId, subject, from_, to_, cc, bcc, body,
                tokenize = "{old_tokenize}", prefix = '2 3 4')"#
        )).unwrap();
        conn.execute(
            "INSERT INTO messages_fts_2001(rowid, msgId, subject, from_, to_, cc, bcc, body)
             VALUES (7, '<retok@test>', 'Weekly digest', 'billing-alerts@domain.com', '', '', '', 'body text')",
            [],
        ).unwrap();
        conn.execute_batch("CREATE TABLE message_ids (msgId TEXT PRIMARY KEY);")
            .unwrap();
        ensure_folder_membership_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO message_ids(rowid, msgId) VALUES (7, '<retok@test>')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message_folder_membership(msgId, folderId) VALUES ('<retok@test>', 'folder:retokenize')",
            [],
        )
        .unwrap();
        let membership_before =
            list_folder_membership(&conn, "folder:retokenize", None, 1).unwrap();

        // Old tokenizer: a part query cannot match the glued address token
        let n: i64 = conn.query_row(
            "SELECT count(*) FROM messages_fts_2001 WHERE messages_fts_2001 MATCH 'billing'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 0, "precondition: glued token must not part-match");

        convert_all_stale_shards(&mut conn);

        // Shard now uses the current tokenizer (no tokenchars)
        let sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE name='messages_fts_2001'",
            [], |r| r.get(0),
        ).unwrap();
        assert!(!sql.contains("tokenchars"), "rebuilt shard still has tokenchars: {sql}");

        // rowid preserved, parts matchable, body intact
        let (rowid, body): (i64, String) = conn.query_row(
            "SELECT rowid, body FROM messages_fts_2001 WHERE messages_fts_2001 MATCH 'billing'",
            [], |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!(rowid, 7);
        assert_eq!(body, "body text");
        assert_eq!(
            list_folder_membership(&conn, "folder:retokenize", None, 1).unwrap(),
            membership_before,
            "tokenizer-shard rebuild must not modify exact folder membership"
        );
        assert_eq!(
            list_folder_membership(&conn, "folder:retokenize", None, 1).unwrap()["msgIds"],
            serde_json::json!(["<retok@test>"])
        );

        // Idempotent: second run is a no-op
        convert_all_stale_shards(&mut conn);
        let n: i64 = conn.query_row(
            "SELECT count(*) FROM messages_fts_2001 WHERE messages_fts_2001 MATCH 'billing'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            list_folder_membership(&conn, "folder:retokenize", None, 1).unwrap(),
            membership_before,
            "idempotent tokenizer-shard rebuild must preserve membership"
        );
    }

    #[test]
    fn test_stale_detection_ignores_shadow_and_retok_tables() {
        // Stale detection must list ONLY real legacy shards — never FTS5 shadow
        // tables (messages_fts_2004_data/_idx/_config/…) nor a leftover _retok
        // temp table from a crashed rebuild.
        let mut conn = Connection::open_in_memory().unwrap();
        let old_tokenize = "porter unicode61 remove_diacritics 2 tokenchars '-_.@'";
        conn.execute_batch(&format!(
            r#"CREATE VIRTUAL TABLE messages_fts_2004 USING fts5(
                msgId, subject, from_, to_, cc, bcc, body,
                tokenize = "{old_tokenize}", prefix = '2 3 4')"#
        )).unwrap();
        // Row insert materializes the shadow tables in sqlite_master
        conn.execute(
            "INSERT INTO messages_fts_2004(rowid, msgId, subject, from_, to_, cc, bcc, body)
             VALUES (1, '<a@test>', 'subj', 'a@domain.com', '', '', '', 'body')",
            [],
        ).unwrap();
        // Leftover temp table from a crashed rebuild (already-new tokenizer)
        conn.execute_batch(&format!(
            r#"CREATE VIRTUAL TABLE messages_fts_2004_retok USING fts5(
                msgId, subject, from_, to_, cc, bcc, body,
                tokenize = "{}", prefix = '2 3 4')"#,
            config::sqlite::FTS_TOKENIZE
        )).unwrap();

        let stale = stale_shard_years(&conn).unwrap();
        assert_eq!(stale, vec![2004], "only the real legacy shard must be stale, got {stale:?}");

        // Rebuild succeeds despite the leftover _retok (dropped + recreated)
        convert_all_stale_shards(&mut conn);
        let stale_after = stale_shard_years(&conn).unwrap();
        assert!(stale_after.is_empty(), "no stale shards after rebuild, got {stale_after:?}");
        let n: i64 = conn.query_row(
            "SELECT count(*) FROM messages_fts_2004 WHERE messages_fts_2004 MATCH 'domain'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 1, "address part must match after rebuild");
    }

    // --- Query text reaches FTS5 as text ---

    /// Ids of the messages an FTS-only search for `q` finds; panics if the search fails.
    fn search_ok(conn: &Connection, q: &str, known_years: &HashSet<i32>) -> Vec<String> {
        let params = serde_json::json!({"ignoreDate": true});
        search(conn, q, &params, &SynonymLookup::new(), None, known_years)
            .unwrap_or_else(|e| panic!("search for {q:?} failed: {e:?}"))
            .iter()
            .map(|r| r["uniqueId"].as_str().unwrap().to_string())
            .collect()
    }

    /// Whatever a user or model types, search never fails, and every word is searched as
    /// text (non-ASCII intact). Only the documented fields scope to a column.
    #[test]
    fn test_search_reads_any_query_text_as_text() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(
            &conn, &mut known_years, "acc:/:one",
            "Budget plan for the café",
            "alice@example.com", "team@example.com",
            "R&D notes: 50% off the launch, and the meeting with bob@example.com",
            a_week_ago,
        );
        insert_test_message_full(
            &conn, &mut known_years, "acc:/:two",
            "Greetings", "carol@example.com", "dave@example.com",
            "日本語 हिन्दी Привет",
            a_week_ago,
        );
        let one = vec!["acc:/:one".to_string()];
        let two = vec!["acc:/:two".to_string()];
        let none: Vec<String> = vec![];

        // Characters FTS5 would read as syntax, and FTS5 operator words.
        for q in [
            "50% off", "hello!", "a/b", "#launch", "$100", "budget, plan", "invoice OR", "NOT budget",
            "budget NOT", "(budget", "budget)", "AND", "NEAR", "OR budget", "\"unclosed", "\"\"",
            "x^y", "budget -plan", "{budget}", "budget AND", "re:budget", "__FQ0__", "_",
            // Fields with no value before the next field.
            "from: to: meeting", "subject: cc: budget", "to: body: call", "from: from: invoice",
            // A NUL, which FTS5 reads as the end of the query.
            "budget\u{0}", "budget\u{0}plan", "\"a\u{0}b\"", "\u{0}alice",
        ] {
            search_ok(&conn, q, &known_years);
        }

        // Words are searched as typed, punctuation inside them included.
        assert_eq!(search_ok(&conn, "café", &known_years), one, "non-ASCII word");
        assert_eq!(search_ok(&conn, "cafe", &known_years), one, "without its accent");
        assert_eq!(search_ok(&conn, "50% off", &known_years), one);
        assert_eq!(search_ok(&conn, "budget\u{0}", &known_years), one, "a NUL separates words");
        // Words in any script.
        for q in ["日本語", "हिन्दी", "Привет"] {
            assert_eq!(search_ok(&conn, q, &known_years), two, "{q}");
        }
        assert_eq!(search_ok(&conn, "R&D", &known_years), one);
        assert_eq!(search_ok(&conn, "budget, plan", &known_years), one);
        assert_eq!(search_ok(&conn, "(budget", &known_years), one);
        // A name before a colon that is not a field is text: "note" matches "notes".
        assert_eq!(search_ok(&conn, "note: launch", &known_years), one);
        // A phrase beside a word that has synonyms.
        assert_eq!(search_ok(&conn, "meeting \"budget plan\"", &known_years), one);
        assert_eq!(search_ok(&conn, "\"budget plan\" meeting", &known_years), one);

        // The documented fields still scope, whatever their case, with or without a space
        // after the colon: bob@example.com is in the body, not the sender.
        assert_eq!(search_ok(&conn, "from:alice@example.com", &known_years), one);
        assert_eq!(search_ok(&conn, "from: \"alice@example.com\"", &known_years), one);
        assert_eq!(search_ok(&conn, "from:\"alice@example.com\" launch", &known_years), one);
        assert_eq!(search_ok(&conn, "from:bob@example.com", &known_years), none);
        assert_eq!(search_ok(&conn, "Subject:budget", &known_years), one);
        assert_eq!(search_ok(&conn, "subject:launch", &known_years), none);
        assert_eq!(search_ok(&conn, "to:team", &known_years), one);
        assert_eq!(search_ok(&conn, "body:launch", &known_years), one);
    }

    /// A partial word of 4+ bytes gets the prefix star, so a short word in another script finds
    /// the longer one, and a CJK word finds the unspaced run it starts (indexed as one token).
    #[test]
    fn test_search_finds_short_partial_words_in_any_script() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(&conn, &mut known_years, "acc:/:p1", "会議は明日です", "erin@example.com", "team@example.com", "Agenda attached.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:p2", "Привет", "erin@example.com", "team@example.com", "Greetings.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:p3", "Zürich trip", "erin@example.com", "team@example.com", "Tickets.", a_week_ago);
        for (q, id) in [("会議", "acc:/:p1"), ("При", "acc:/:p2"), ("Zür", "acc:/:p3")] {
            assert_eq!(search_ok(&conn, q, &known_years), vec![id.to_string()], "{q}");
        }
    }

    /// A field is the text before the first colon, so a value may hold colons (`subject:Re:`,
    /// `body:https://…`), as in 0.11.8.
    #[test]
    fn test_search_field_value_may_hold_a_colon() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(&conn, &mut known_years, "acc:/:c1", "Re: budget", "erin@example.com", "team@example.com", "Notes.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:c2", "Notes", "erin@example.com", "team@example.com", "Re the budget.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:c3", "Link", "erin@example.com", "team@example.com", "See https://example.com/x today.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:c4", "https://example.com/x", "erin@example.com", "team@example.com", "Other.", a_week_ago);
        assert_eq!(search_ok(&conn, "subject:Re: budget", &known_years), vec!["acc:/:c1".to_string()]);
        assert_eq!(search_ok(&conn, "body:https://example.com/x", &known_years), vec!["acc:/:c3".to_string()]);
    }

    /// A possessive looks up its word with the `s`, as 0.11.8 did: `comment's` gets the synonyms
    /// of `comments`, in either apostrophe and either case.
    #[test]
    fn test_search_possessive_of_a_plural_synonym_key_expands() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(&conn, &mut known_years, "acc:/:p1", "Thoughts on the deck", "erin@example.com", "team@example.com", "My feedback on the slides", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:p2", "Lunch plans", "erin@example.com", "team@example.com", "Pizza on Friday", a_week_ago);
        for q in ["comment's", "COMMENT'S", "comment\u{2019}s", "COMMENT\u{2019}S"] {
            assert_eq!(search_ok(&conn, q, &known_years), vec!["acc:/:p1".to_string()], "{q:?}");
        }
    }

    /// A kept NOT excludes the word typed: its synonyms would also exclude the left side's.
    #[test]
    fn test_search_not_excludes_only_the_word_typed() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(&conn, &mut known_years, "acc:/:r1", "Invoice for March", "accounts@example.com", "team@example.com", "The invoice is attached.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:r2", "Receipt for March", "accounts@example.com", "team@example.com", "Here is the receipt.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:r3", "Payment due in March", "accounts@example.com", "team@example.com", "Please pay soon.", a_week_ago);
        let found = |q: &str| -> Vec<String> {
            let mut ids = search_ok(&conn, q, &known_years);
            ids.sort();
            ids
        };
        let ids = |v: &[&str]| -> Vec<String> { v.iter().map(|s| s.to_string()).collect() };

        // The left side still expands (payment); only mail with the word typed is excluded.
        assert_eq!(found("invoice NOT receipt"), ids(&["acc:/:r1", "acc:/:r3"]));
        assert_eq!(found("March NOT receipt"), ids(&["acc:/:r1", "acc:/:r3"]));
        // A NOT that is dropped leaves the word a search term with its synonyms.
        assert_eq!(found("NOT receipt"), ids(&["acc:/:r1", "acc:/:r2", "acc:/:r3"]));
        // A word after a kept AND or OR still expands.
        assert_eq!(found("zebra OR receipt"), ids(&["acc:/:r1", "acc:/:r2", "acc:/:r3"]));
        assert_eq!(found("March AND receipt"), ids(&["acc:/:r1", "acc:/:r2", "acc:/:r3"]));
    }

    /// Capital AND, OR and NOT between two terms are operators, as they always were
    /// (`from:X OR to:X`); first, last or doubled they are dropped. Any whitespace separates
    /// words, and a word with no letter or digit never hides a match.
    #[test]
    fn test_search_operators_separators_and_text_less_words() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(
            &conn, &mut known_years, "acc:/:one",
            "Budget plan for the café",
            "alice@example.com", "team@example.com",
            "R&D notes: 50% off the launch, and the meeting with bob@example.com",
            a_week_ago,
        );
        insert_test_message_full(
            &conn, &mut known_years, "acc:/:two",
            "Greetings", "carol@example.com", "dave@example.com",
            "日本語 हिन्दी Привет",
            a_week_ago,
        );
        let found = |q: &str| -> Vec<String> {
            let mut ids = search_ok(&conn, q, &known_years);
            ids.sort();
            ids
        };
        let one = vec!["acc:/:one".to_string()];
        let two = vec!["acc:/:two".to_string()];
        let both = vec!["acc:/:one".to_string(), "acc:/:two".to_string()];
        let none: Vec<String> = vec![];

        assert_eq!(found("from:alice@example.com OR from:carol@example.com"), both);
        assert_eq!(found("from:\"alice@example.com\" OR to:\"dave@example.com\""), both);
        assert_eq!(found("\"budget plan\" OR greetings"), both);
        // Beside a synonym group.
        assert_eq!(found("meeting OR greetings"), both);
        assert_eq!(found("from:alice@example.com AND from:carol@example.com"), none);
        assert_eq!(found("from:alice@example.com AND launch"), one);
        assert_eq!(found("from:alice@example.com NOT launch"), none);
        assert_eq!(found("from:alice@example.com NOT greetings"), one);
        assert_eq!(found("greetings NOT budget"), two);
        // An operator replaces one join: this is café, or greetings and budget.
        assert_eq!(found("café OR greetings budget"), one);
        // Not between two terms: dropped.
        assert_eq!(found("NOT budget"), one);
        assert_eq!(found("OR greetings"), two);
        assert_eq!(found("greetings OR"), two);
        assert_eq!(found("greetings OR NOT budget"), both);
        assert_eq!(found("greetings OR -- budget"), both);

        // Tabs, no-break and ideographic spaces separate words like a space: the words are
        // found apart, not as the phrase "plan budget".
        assert_eq!(found("\"plan budget\""), none);
        for q in ["plan budget", "plan\tbudget", "plan\u{a0}budget", "plan\u{3000}budget"] {
            assert_eq!(found(q), one, "{q:?}");
        }

        // A word with no letter or digit, beside words with synonyms (an AND join).
        assert_eq!(found("budget ?"), one);
        assert_eq!(found("meeting -- \"\""), one);
        // Letters the tokenizer does not index (enclosed-letter symbols, a lone mark) too.
        for q in ["meeting \u{24d0}", "budget \u{1f17f}\u{fe0f}", "\"\u{24d0}\" meeting", "meeting AND \u{24d0}", "meeting \u{93e}"] {
            assert_eq!(found(q), one, "{q:?}");
        }
    }

    /// A private-use character is indexed, so a query keeps it: it finds the mail that has it.
    #[test]
    fn test_search_keeps_private_use_characters() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(&conn, &mut known_years, "acc:/:mark", "Plan \u{e000} launch", "alice@example.com", "team@example.com", "", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:plain", "Plan launch", "alice@example.com", "team@example.com", "", a_week_ago);
        assert_eq!(search_ok(&conn, "plan \u{e000}", &known_years), vec!["acc:/:mark".to_string()]);
    }

    /// A word with an inner apostrophe finds both spellings: the text it was written as (the
    /// tokenizer splits `O'Brien` into `o` and `brien`) and the one without it ("OBrien").
    #[test]
    fn test_search_finds_words_with_apostrophes() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(
            &conn, &mut known_years, "acc:/:apostrophe",
            "Lunch with O'Brien", "alice@example.com", "team@example.com",
            "Don't forget the receipts.",
            a_week_ago,
        );
        insert_test_message_full(
            &conn, &mut known_years, "acc:/:joined",
            "Lunch with OBrien", "carol@example.com", "team@example.com",
            "Dont forget the receipts.",
            a_week_ago,
        );
        insert_test_message_full(&conn, &mut known_years, "acc:/:sarah", "Invoice from Sarah", "sarah@example.com", "team@example.com", "Thanks, Sarah", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:sarahs", "Sarah's invoice", "billing@example.com", "team@example.com", "", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:john", "Travel", "agent@example.com", "team@example.com", "John booked the flight to Denver.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:team", "Team budget", "carol@example.com", "team@example.com", "", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:expense", "Travel expense report", "agent@example.com", "office@example.com", "", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:r5", "Files", "agent@example.com", "office@example.com", "Tommy sent the files.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:took", "Bus", "agent@example.com", "office@example.com", "Don took the bus.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:noon", "Lunch at noon", "agent@example.com", "office@example.com", "See you there.", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:r6", "L'\u{e9}tudiant arrives", "agent@example.com", "office@example.com", "", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:r7", "L\u{e9}tude draft", "agent@example.com", "office@example.com", "", a_week_ago);
        let found = |q: &str| -> Vec<String> {
            let mut ids = search_ok(&conn, q, &known_years);
            ids.sort();
            ids
        };
        // Never "Don took": a one-letter last part is not a prefix.
        let both = vec!["acc:/:apostrophe".to_string(), "acc:/:joined".to_string()];
        for q in ["O'Brien", "o'brien", "O\u{2019}Brien", "don't forget", "Don't", "Don\u{2019}t", "DON'T"] {
            assert_eq!(found(q), both, "{q:?}");
        }
        // A word beside it keeps its prefix star, as beside any word.
        for q in ["don't forg", "forg don't"] {
            assert_eq!(found(q), both, "{q:?}");
        }
        // A joined spelling of 4 bytes is a prefix, counted in bytes: "lét" finds "Létude".
        assert_eq!(found("l'\u{e9}t"), vec!["acc:/:r6".to_string(), "acc:/:r7".to_string()]);
        // A kept NOT excludes both spellings.
        assert_eq!(found("lunch NOT don't"), vec!["acc:/:noon".to_string()]);
        // More capital NOTs than FTS5's expression depth allows (#22), beside a spelling group or
        // a synonym group: the first ones exclude, the rest are left out, and the query answers.
        let nots = |n: usize| (0..n).map(|k| format!(" NOT qz{k:03}")).collect::<String>();
        for n in [255, 256, 257, 300] {
            assert_eq!(found(&format!("don't NOT obrien{}", nots(n))), vec!["acc:/:apostrophe".to_string()], "{n} NOTs");
            assert_eq!(found(&format!("'budget'{}", nots(n))), found("'budget'"), "{n} NOTs");
        }
        // A field's value is searched as typed.
        assert_eq!(found("subject:O'Brien"), vec!["acc:/:apostrophe".to_string()]);
        // A possessive finds what it names, written with or without the 's.
        let sarah = vec!["acc:/:sarah".to_string(), "acc:/:sarahs".to_string()];
        for q in ["Sarah's", "Sarah\u{2019}s invoice", "SARAH'S", "Sarah's.", "Sarah's...", "Sarah's,", "(Sarah's)", "Sarah's?"] {
            assert_eq!(found(q), sarah, "{q:?}");
        }
        assert_eq!(found("John's flight"), vec!["acc:/:john".to_string()]);
        // A short name's possessive stays a prefix, as 0.11.8's `Toms*` was.
        assert_eq!(found("Tom's"), vec!["acc:/:r5".to_string()]);
        assert_eq!(found("team's budget"), vec!["acc:/:team".to_string()]);
        // Quote marks around a word are not part of it: it still expands to its synonyms.
        assert_eq!(found("'budget'"), vec!["acc:/:expense".to_string(), "acc:/:team".to_string()]);
    }

    /// A word gets its automatic prefix star unless a word with synonyms is in its own run of
    /// words (between quoted phrases), as before: `notes` here still finds "Notesheet".
    #[test]
    fn test_search_prefix_star_is_decided_per_run_of_words() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(&conn, &mut known_years, "acc:/:sheet", "Roadmapping", "alice@example.com", "team@example.com", "Notesheet from Jane Doe for the meeting", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:other", "Roadmapping", "alice@example.com", "team@example.com", "Jane Doe set up the meeting", a_week_ago);
        assert_eq!(search_ok(&conn, "notes \"Jane Doe\" roadmapping meeting", &known_years), vec!["acc:/:sheet".to_string()]);
        // A possessive does not expand, so `notes` beside it stays a prefix: this finds
        // "Notesheet", and not the mail that has only a synonym of "client".
        insert_test_message_full(&conn, &mut known_years, "acc:/:r1", "Notesheet for the client", "alice@example.com", "office@example.com", "", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:r2", "Notes for the customer", "alice@example.com", "office@example.com", "", a_week_ago);
        assert_eq!(search_ok(&conn, "client's notes", &known_years), vec!["acc:/:r1".to_string()]);
        // A field's value beside a synonym group is not a prefix either, as before.
        insert_test_message_full(&conn, &mut known_years, "acc:/:r3", "Meeting", "alex@example.com", "office@example.com", "", a_week_ago);
        insert_test_message_full(&conn, &mut known_years, "acc:/:r4", "Meeting", "alexandra@example.com", "office@example.com", "", a_week_ago);
        for q in ["meeting from:alex", "meeting from: alex"] {
            assert_eq!(search_ok(&conn, q, &known_years), vec!["acc:/:r3".to_string()], "{q}");
        }
    }

    /// Inserts a message with the given FTS columns set and the others empty.
    fn insert_with_fields(conn: &Connection, known_years: &mut HashSet<i32>, msg_id: &str, fields: &[(&str, &str)]) {
        let date_ms = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        conn.execute("INSERT OR IGNORE INTO message_ids (msgId) VALUES (?1)", params![msg_id]).unwrap();
        let row_id: i64 = conn.query_row("SELECT rowid FROM message_ids WHERE msgId = ?1", params![msg_id], |r| r.get(0)).unwrap();
        let year = year_from_date_ms(date_ms);
        ensure_shard(conn, year, known_years).unwrap();
        let table = fts_table_name(year);
        let field = |c: &str| fields.iter().find(|(k, _)| *k == c).map(|(_, v)| *v).unwrap_or("");
        conn.execute(
            &format!("INSERT INTO {table} (rowid, msgId, subject, from_, to_, cc, bcc, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"),
            params![row_id, msg_id, field("subject"), field("from_"), field("to_"), field("cc"), field("bcc"), field("body")],
        ).unwrap();
        conn.execute(
            "INSERT INTO message_meta (rowid, dateMs, hasAttachments, parsedIcsAttachments, shardYear) VALUES (?1, ?2, 0, '', ?3)",
            params![row_id, date_ms, year],
        ).unwrap();
    }

    /// Each field the search tools document scopes to its own column, in FTS search (typed in
    /// either case) and in the hybrid search's vector filter (checked for the lowercase name); no
    /// other name scopes.
    #[test]
    fn test_each_documented_field_scopes_to_its_column() {
        // Written out here, not read from QUERY_COLUMNS, so a dropped or wrong entry fails.
        let documented = [
            ("subject", "subject"), ("from", "from_"), ("from_", "from_"), ("to", "to_"),
            ("to_", "to_"), ("cc", "cc"), ("bcc", "bcc"), ("body", "body"),
        ];
        for (typed, col) in documented {
            let (conn, mut known_years) = setup_test_db();
            insert_with_fields(&conn, &mut known_years, "acc:/:in-field", &[(col, "zorblax@example.com")]);
            let elsewhere = if col == "body" { "subject" } else { "body" };
            insert_with_fields(&conn, &mut known_years, "acc:/:elsewhere", &[(elsewhere, "zorblax@example.com")]);
            for q in [format!("{typed}:zorblax@example.com"), format!("{} zorblax@example.com", format!("{typed}:").to_uppercase())] {
                assert_eq!(search_ok(&conn, &q, &known_years), vec!["acc:/:in-field".to_string()], "{q}");
            }
            let mut both = search_ok(&conn, "zorblax@example.com", &known_years);
            both.sort();
            assert_eq!(both, vec!["acc:/:elsewhere".to_string(), "acc:/:in-field".to_string()]);

            let built = build_fts_match(Some(&format!("{typed}:\"a@example.com\" meeting")), QUERY_COLUMNS, true, &SynonymLookup::new());
            assert!(query_has_column_scope(&built), "{built}");
            assert_eq!(extract_column_scope_filter(&built), format!("{col}:\"a@example.com\""), "{built}");
        }
        let mut listed: Vec<_> = QUERY_COLUMNS.to_vec();
        listed.sort();
        let mut expected = documented.to_vec();
        expected.sort();
        assert_eq!(listed, expected);
    }

    /// Words that may be typed together, joined at random: no query makes search fail.
    pub(crate) const QUERY_FRAGMENTS: &[&str] = &[
        "from:", "From:", "to:", "cc:", "bcc:", "subject:", "body:", "from_:", "to_:", "note:", ":", "re:",
        "meeting", "budget", "plan", "call", "invoice", "done", "AND", "OR", "NOT", "NEAR", "\"",
        "\"budget plan\"", "(", ")", "*", "-", "^", "+", "%", "!", "?", "/", "'", ",", ".", "@", "#", "$",
        "&", "{", "}", "[", "]", "~", "`", "\\", "|", "<", ">", "=", ";", "_", "café", "日本語", "हिन्दी",
        "Привет", "ⓐ", "\u{301}", "😀", "\u{a0}", "\u{3000}", "\u{200b}", "\u{feff}", "\u{2028}",
        "\u{e000}", "\u{1}", "\u{0}", "\t", " ", "a@example.com", "don't", "O\u{2019}Brien", "I'm",
    ];

    /// `count` queries of 1 to 7 fragments, from a fixed seed, so a failure repeats.
    pub(crate) fn random_queries(count: usize) -> Vec<String> {
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        (0..count)
            .map(|_| {
                let n = 1 + (next() % 7) as usize;
                let mut q = String::new();
                for _ in 0..n {
                    q.push_str(QUERY_FRAGMENTS[(next() % QUERY_FRAGMENTS.len() as u64) as usize]);
                    if next() % 2 == 0 {
                        q.push(' ');
                    }
                }
                q
            })
            .collect()
    }

    #[test]
    fn test_no_query_text_makes_search_fail() {
        let (conn, mut known_years) = setup_test_db();
        let a_week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        insert_test_message_full(
            &conn, &mut known_years, "acc:/:one", "Budget plan meeting", "alice@example.com",
            "team@example.com", "the call about the invoice", a_week_ago,
        );
        for q in random_queries(3000) {
            search_ok(&conn, &q, &known_years);
        }
    }

    #[test]
    fn test_extract_column_scope_filter_quoted_prefix_star() {
        // build_fts_match emits field:"value"* for special-char values — the
        // star must survive extraction so the filter keeps prefix semantics.
        let result = extract_column_scope_filter(r#"from_:"alice@example.com"* hiring*"#);
        assert_eq!(result, r#"from_:"alice@example.com"*"#);
    }

    #[test]
    fn test_build_fts_match_special_chars_prefix_star() {
        // Special-char tokens become quoted phrase-prefix queries (ADR-024):
        // "dmarc-helper" → phrase [dmarc, helper*]. The trailing star keeps
        // mid-typing partials (incomplete last fragment) matching.
        let synonyms = SynonymLookup::new();
        assert_eq!(build_fts_match(Some("dmarc-helper"), QUERY_COLUMNS, true, &synonyms), r#""dmarc-helper"*"#);
        assert_eq!(build_fts_match(Some("user@example.com"), QUERY_COLUMNS, true, &synonyms), r#""user@example.com"*"#);
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
    fn test_get_attachment_flags_returns_each_stored_flag_in_request_order() {
        let (mut conn, mut known_years) = setup_test_db();
        let row = |id: &str, has: bool| serde_json::json!({
            "msgId": id, "subject": "s", "body": "b", "dateMs": 1_704_067_200_000_i64, "hasAttachments": has,
        });
        index_batch(&mut conn, &[row("acc:/:with", true), row("acc:/:without", false)], None, &mut known_years).unwrap();

        let res = get_attachment_flags(&conn, &["acc:/:without", "acc:/:missing", "acc:/:with", "acc:/:with"]).unwrap();
        assert_eq!(res, serde_json::json!({ "ok": true, "flags": [false, null, true, true] }));
        assert_eq!(get_attachment_flags(&conn, &[]).unwrap(), serde_json::json!({ "ok": true, "flags": [] }));
    }

    #[test]
    fn test_get_attachment_flags_reads_null_for_an_id_without_metadata() {
        let (conn, _) = setup_test_db();
        conn.execute("INSERT INTO message_ids (msgId) VALUES ('acc:/:orphan')", []).unwrap();
        let res = get_attachment_flags(&conn, &["acc:/:orphan"]).unwrap();
        assert_eq!(res["flags"], serde_json::json!([null]));
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

    // --- attachmentNames column ---

    /// A shard in the schema every install had before the attachmentNames
    /// column: current tokenizer, seven columns.
    fn create_shard_without_attachment_names(conn: &Connection, year: i32) {
        conn.execute_batch(&format!(
            r#"CREATE VIRTUAL TABLE messages_fts_{year} USING fts5(
                msgId, subject, from_, to_, cc, bcc, body,
                tokenize = "{}", prefix = '{}')"#,
            config::sqlite::FTS_TOKENIZE,
            config::sqlite::FTS_PREFIXES,
        )).unwrap();
    }

    fn row_with_names(msg_id: &str, date_ms: i64, subject: &str, names: Option<&str>) -> Value {
        let mut row = serde_json::json!({
            "msgId": msg_id,
            "dateMs": date_ms,
            "subject": subject,
            "from": "sender@example.com",
            "body": "body",
        });
        if let Some(names) = names {
            row["attachmentNames"] = Value::String(names.to_string());
        }
        row
    }

    fn search_ids(conn: &Connection, q: &str, known_years: &HashSet<i32>) -> Vec<String> {
        let params = serde_json::json!({"ignoreDate": true});
        let mut ids: Vec<String> = search(conn, q, &params, &SynonymLookup::new(), None, known_years)
            .unwrap()
            .iter()
            .map(|r| r["uniqueId"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    }

    #[test]
    fn test_index_batch_stores_and_searches_attachment_names() {
        let (mut conn, mut known_years) = setup_test_db();
        let rows = vec![
            row_with_names("acc:/:named", 0, "Monthly update", Some("Statement-Quarterly.pdf")),
            row_with_names("acc:/:plain", 0, "Monthly update", None),
        ];
        index_batch(&mut conn, &rows, None, &mut known_years).unwrap();

        assert_eq!(search_ids(&conn, "quarterly", &known_years), vec!["acc:/:named"]);
        assert_eq!(search_ids(&conn, "monthly", &known_years), vec!["acc:/:named", "acc:/:plain"]);

        let named = get_message_by_msgid(&conn, "acc:/:named").unwrap().unwrap();
        assert_eq!(named["attachmentNames"], "Statement-Quarterly.pdf");
        let plain = get_message_by_msgid(&conn, "acc:/:plain").unwrap().unwrap();
        assert_eq!(plain["attachmentNames"], "");
    }

    #[test]
    fn test_shard_without_attachment_names_is_rebuilt_in_place() {
        let (mut conn, mut known_years) = setup_test_db();
        let year_2005_ms = 1104537600000; // 2005-01-01: the shard's year is what matters
        create_shard_without_attachment_names(&conn, 2005);
        known_years.insert(2005);
        index_batch(&mut conn, &[row_with_names("acc:/:old", year_2005_ms, "Budget review", None)], None, &mut known_years).unwrap();
        let old_rowid: i64 = conn.query_row(
            "SELECT rowid FROM message_ids WHERE msgId = 'acc:/:old'", [], |r| r.get(0),
        ).unwrap();
        assert_eq!(stale_shard_years(&conn).unwrap(), vec![2005]);

        // Before the shard converts: writes to it succeed without the names,
        // and search spans old and new shards together.
        index_batch(&mut conn, &[
            row_with_names("acc:/:early", year_2005_ms, "Budget early", Some("Early-Invoice.pdf")),
            row_with_names("acc:/:new", 0, "Budget new", Some("Fresh-Invoice.pdf")),
        ], None, &mut known_years).unwrap();
        assert_eq!(get_message_by_msgid(&conn, "acc:/:early").unwrap().unwrap()["attachmentNames"], "");
        assert_eq!(search_ids(&conn, "budget", &known_years), vec!["acc:/:early", "acc:/:new", "acc:/:old"]);
        assert_eq!(search_ids(&conn, "invoice", &known_years), vec!["acc:/:new"]);

        convert_all_stale_shards(&mut conn);

        assert!(stale_shard_years(&conn).unwrap().is_empty());
        assert!(shard_has_attachment_names(&conn, "messages_fts_2005").unwrap());
        let (rowid, body): (i64, String) = conn.query_row(
            "SELECT rowid, body FROM messages_fts_2005 WHERE msgId = 'acc:/:old'", [], |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!((rowid, body.as_str()), (old_rowid, "body"));
        let old = get_message_by_msgid(&conn, "acc:/:old").unwrap().unwrap();
        assert_eq!((old["subject"].as_str(), old["attachmentNames"].as_str()), (Some("Budget review"), Some("")));

        // After converting, new rows in that shard keep their names.
        index_batch(&mut conn, &[row_with_names("acc:/:late", year_2005_ms, "Budget late", Some("Late-Invoice.pdf"))], None, &mut known_years).unwrap();
        assert_eq!(search_ids(&conn, "invoice", &known_years), vec!["acc:/:late", "acc:/:new"]);

        convert_all_stale_shards(&mut conn);
        assert_eq!(search_ids(&conn, "budget", &known_years).len(), 4, "second run must be a no-op");
    }

    #[test]
    fn test_shard_rebuild_keeps_existing_attachment_names() {
        // A shard that already has the column but an outdated tokenizer keeps
        // its names through the rebuild.
        let (mut conn, mut known_years) = setup_test_db();
        conn.execute_batch(
            r#"CREATE VIRTUAL TABLE messages_fts_2006 USING fts5(
                msgId, subject, from_, to_, cc, bcc, body, attachmentNames,
                tokenize = "porter unicode61 remove_diacritics 2 tokenchars '-_.@'", prefix = '2 3 4')"#,
        ).unwrap();
        known_years.insert(2006);
        // Earlier messages in other shards, so this row's rowid is not the 1 an
        // empty copy target would assign on its own.
        index_batch(&mut conn, &[row_with_names("acc:/:a", 0, "Other", None), row_with_names("acc:/:b", 0, "Other", None)], None, &mut known_years).unwrap();
        index_batch(&mut conn, &[row_with_names("acc:/:kept", 1136073600000, "Note", Some("Kept-Report.pdf"))], None, &mut known_years).unwrap();
        let kept_rowid: i64 = conn.query_row("SELECT rowid FROM message_ids WHERE msgId = 'acc:/:kept'", [], |r| r.get(0)).unwrap();
        assert_eq!(kept_rowid, 3);
        assert_eq!(stale_shard_years(&conn).unwrap(), vec![2006]);

        convert_all_stale_shards(&mut conn);

        assert!(stale_shard_years(&conn).unwrap().is_empty());
        let shard_rowid: i64 = conn.query_row("SELECT rowid FROM messages_fts_2006 WHERE msgId = 'acc:/:kept'", [], |r| r.get(0)).unwrap();
        assert_eq!(shard_rowid, kept_rowid);
        assert_eq!(get_message_by_msgid(&conn, "acc:/:kept").unwrap().unwrap()["attachmentNames"], "Kept-Report.pdf");
        assert_eq!(search_ids(&conn, "report", &known_years), vec!["acc:/:kept"]);
    }

    #[test]
    fn test_attachment_name_match_ranks_above_a_body_match() {
        // A file name weighs like the sender, above body text.
        let (mut conn, mut known_years) = setup_test_db();
        let mut by_name = row_with_names("acc:/:by-name", 0, "Update", Some("Quarterly.pdf"));
        by_name["body"] = Value::String("see attached".to_string());
        let mut by_body = row_with_names("acc:/:by-body", 0, "Update", None);
        by_body["body"] = Value::String("quarterly attached".to_string());
        // Unrelated messages, so the term is rare enough for bm25 to score it.
        let mut rows: Vec<Value> = (0..8).map(|i| row_with_names(&format!("acc:/:other{i}"), 0, "Update", None)).collect();
        rows.push(by_name);
        rows.push(by_body);
        index_batch(&mut conn, &rows, None, &mut known_years).unwrap();

        let params = serde_json::json!({"ignoreDate": true});
        let results = search(&conn, "quarterly", &params, &SynonymLookup::new(), None, &known_years).unwrap();
        assert_eq!(results.len(), 2);
        let rank_of = |id: &str| results.iter().find(|r| r["uniqueId"] == id).unwrap()["rank"].as_f64().unwrap();
        // bm25 ranks are negative; more negative is a better match.
        assert!(
            rank_of("acc:/:by-name") < rank_of("acc:/:by-body"),
            "name match {} should rank above body match {}",
            rank_of("acc:/:by-name"),
            rank_of("acc:/:by-body")
        );
    }

    #[test]
    fn test_shard_rebuild_failing_after_the_drop_keeps_the_original_shard() {
        // The copy, DROP and RENAME are one transaction: a failure at the
        // rename must roll the DROP back, not leave indexed rows shardless.
        let (mut conn, mut known_years) = setup_test_db();
        create_shard_without_attachment_names(&conn, 2005);
        known_years.insert(2005);
        index_batch(&mut conn, &[row_with_names("acc:/:kept", 1104537600000, "Budget review", None)], None, &mut known_years).unwrap();
        let before = get_message_by_msgid(&conn, "acc:/:kept").unwrap().unwrap();

        unsafe extern "C" fn deny_alter_table(
            denied: *mut std::ffi::c_void,
            action: i32,
            _: *const std::ffi::c_char,
            _: *const std::ffi::c_char,
            _: *const std::ffi::c_char,
            _: *const std::ffi::c_char,
        ) -> i32 {
            if action == rusqlite::ffi::SQLITE_ALTER_TABLE {
                *(denied as *mut usize) += 1;
                rusqlite::ffi::SQLITE_DENY
            } else {
                rusqlite::ffi::SQLITE_OK
            }
        }
        let mut denied = 0usize;
        unsafe {
            assert_eq!(rusqlite::ffi::sqlite3_set_authorizer(conn.handle(), Some(deny_alter_table), &mut denied as *mut usize as *mut _), 0);
        }
        assert!(rebuild_next_stale_shard(&mut conn, &mut HashSet::new()).unwrap());
        unsafe {
            rusqlite::ffi::sqlite3_set_authorizer(conn.handle(), None, std::ptr::null_mut());
        }
        assert!(denied > 0, "the rename, after the copy and the DROP, must have been refused");

        assert_eq!(stale_shard_years(&conn).unwrap(), vec![2005]);
        assert_eq!(get_message_by_msgid(&conn, "acc:/:kept").unwrap().unwrap(), before);
        assert_eq!(search_ids(&conn, "budget", &known_years), vec!["acc:/:kept"]);
        let leftovers: i64 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name = 'messages_fts_2005_retok'", [], |r| r.get(0),
        ).unwrap();
        assert_eq!(leftovers, 0);

        // The next start converts it.
        assert!(rebuild_next_stale_shard(&mut conn, &mut HashSet::new()).unwrap());
        assert!(stale_shard_years(&conn).unwrap().is_empty());
        assert_eq!(get_message_by_msgid(&conn, "acc:/:kept").unwrap().unwrap(), before);
    }

    #[test]
    fn test_shard_rebuild_copies_full_length_content() {
        // Root data rule 11: the converter copies stored content whole.
        let long_body = format!("{} 終端é bodytail", "fullcontent ".repeat(1000));
        let long_names = format!("{} Final-Nametail.pdf", "Appendix.pdf ".repeat(100));

        // Seven-column shard: the body survives.
        let (mut conn, mut known_years) = setup_test_db();
        create_shard_without_attachment_names(&conn, 2005);
        known_years.insert(2005);
        let mut row = row_with_names("acc:/:long", 1104537600000, "Subject", None);
        row["body"] = Value::String(long_body.clone());
        index_batch(&mut conn, &[row], None, &mut known_years).unwrap();
        convert_all_stale_shards(&mut conn);
        assert!(stale_shard_years(&conn).unwrap().is_empty());
        assert_eq!(get_message_by_msgid(&conn, "acc:/:long").unwrap().unwrap()["body"], long_body.as_str());
        assert_eq!(search_ids(&conn, "bodytail", &known_years), vec!["acc:/:long"]);

        // Eight-column shard on an outdated tokenizer: body and names survive.
        let (mut conn, mut known_years) = setup_test_db();
        conn.execute_batch(
            r#"CREATE VIRTUAL TABLE messages_fts_2006 USING fts5(
                msgId, subject, from_, to_, cc, bcc, body, attachmentNames,
                tokenize = "porter unicode61 remove_diacritics 2 tokenchars '-_.@'", prefix = '2 3 4')"#,
        ).unwrap();
        known_years.insert(2006);
        let mut row = row_with_names("acc:/:named", 1136073600000, "Subject", Some(&long_names));
        row["body"] = Value::String(long_body.clone());
        index_batch(&mut conn, &[row], None, &mut known_years).unwrap();
        assert_eq!(stale_shard_years(&conn).unwrap(), vec![2006]);
        convert_all_stale_shards(&mut conn);
        assert!(stale_shard_years(&conn).unwrap().is_empty());
        let named = get_message_by_msgid(&conn, "acc:/:named").unwrap().unwrap();
        assert_eq!(named["body"], long_body.as_str());
        assert_eq!(named["attachmentNames"], long_names.as_str());
        assert_eq!(search_ids(&conn, "nametail", &known_years), vec!["acc:/:named"]);
    }

    #[test]
    fn test_attachment_name_match_ranks_above_a_body_match_in_hybrid_candidates() {
        // The hybrid path ranks FTS candidates with its own bm25 call; it must
        // weigh file names the same way search_fts_only does.
        let (mut conn, mut known_years) = setup_test_db();
        let mut by_name = row_with_names("acc:/:by-name", 0, "Update", Some("Quarterly.pdf"));
        by_name["body"] = Value::String("see attached".to_string());
        let mut by_body = row_with_names("acc:/:by-body", 0, "Update", None);
        by_body["body"] = Value::String("quarterly attached".to_string());
        let mut rows: Vec<Value> = (0..8).map(|i| row_with_names(&format!("acc:/:other{i}"), 0, "Update", None)).collect();
        rows.push(by_name);
        rows.push(by_body);
        index_batch(&mut conn, &rows, None, &mut known_years).unwrap();

        let candidates = search_fts_candidates(&conn, "quarterly", None, None, 50, &known_years).unwrap();
        assert_eq!(candidates.len(), 2);
        let rank_of = |id: &str| candidates.iter().find(|c| c.msg_id == id).unwrap().rank;
        assert!(
            rank_of("acc:/:by-name") < rank_of("acc:/:by-body"),
            "name match {} should rank above body match {}",
            rank_of("acc:/:by-name"),
            rank_of("acc:/:by-body")
        );
    }

    /// Subject outranks sender outranks body, on the shard's current columns.
    fn assert_column_weights(conn: &mut Connection, known_years: &mut HashSet<i32>, date_ms: i64) {
        let mut rows: Vec<Value> = (0..8).map(|i| {
            let mut r = row_with_names(&format!("acc:/:filler{date_ms}-{i}"), date_ms, "lunch", None);
            r["from"] = Value::String("friend@example.com".to_string());
            r
        }).collect();
        let mut in_subject = row_with_names(&format!("acc:/:subject{date_ms}"), date_ms, "zephyr plans", None);
        in_subject["from"] = Value::String("friend@example.com".to_string());
        let mut in_from = row_with_names(&format!("acc:/:from{date_ms}"), date_ms, "plans", None);
        in_from["from"] = Value::String("zephyr@example.com".to_string());
        let mut in_body = row_with_names(&format!("acc:/:body{date_ms}"), date_ms, "plans", None);
        in_body["from"] = Value::String("friend@example.com".to_string());
        in_body["body"] = Value::String("zephyr".to_string());
        rows.extend([in_subject, in_from, in_body]);
        index_batch(conn, &rows, None, known_years).unwrap();

        let params = serde_json::json!({"ignoreDate": true});
        let results = search(conn, "zephyr", &params, &SynonymLookup::new(), None, known_years).unwrap();
        let rank_of = |id: String| results.iter().find(|r| r["uniqueId"] == id.as_str()).unwrap()["rank"].as_f64().unwrap();
        let (subject, from, body) = (
            rank_of(format!("acc:/:subject{date_ms}")),
            rank_of(format!("acc:/:from{date_ms}")),
            rank_of(format!("acc:/:body{date_ms}")),
        );
        assert!(subject < from && from < body, "subject {subject} < from {from} < body {body}");
    }

    #[test]
    fn test_attachment_names_column_keeps_existing_column_weights() {
        // bm25 weights are positional: attachmentNames must stay the last column.
        let (mut conn, mut known_years) = setup_test_db();
        assert_column_weights(&mut conn, &mut known_years, 0);

        let year_2005_ms = 1104537600000;
        create_shard_without_attachment_names(&conn, 2005);
        known_years.insert(2005);
        convert_all_stale_shards(&mut conn);
        assert_column_weights(&mut conn, &mut known_years, year_2005_ms);
    }

    #[test]
    fn test_writer_idle_step_converts_newest_shard_first_one_at_a_time() {
        let (mut conn, mut known_years) = setup_test_db();
        for year in [2005, 2007] {
            create_shard_without_attachment_names(&conn, year);
            known_years.insert(year);
        }
        let mut attempted = HashSet::new();

        assert!(rebuild_next_stale_shard(&mut conn, &mut attempted).unwrap());
        assert_eq!(stale_shard_years(&conn).unwrap(), vec![2005], "newest year converts first");
        assert!(rebuild_next_stale_shard(&mut conn, &mut attempted).unwrap());
        assert!(stale_shard_years(&conn).unwrap().is_empty());
        assert!(!rebuild_next_stale_shard(&mut conn, &mut attempted).unwrap());
    }

    #[test]
    fn test_writer_idle_step_tries_a_failing_shard_once_per_process() {
        let (mut conn, _known_years) = setup_test_db();
        // Stale (no attachmentNames column) and cannot be copied (no FTS columns).
        conn.execute_batch("CREATE TABLE messages_fts_2008 (x TEXT)").unwrap();
        let mut attempted = HashSet::new();

        assert!(rebuild_next_stale_shard(&mut conn, &mut attempted).unwrap());
        assert_eq!(stale_shard_years(&conn).unwrap(), vec![2008], "the failed copy rolled back");
        assert!(!rebuild_next_stale_shard(&mut conn, &mut attempted).unwrap(), "not retried until the next start");
        assert!(rebuild_next_stale_shard(&mut conn, &mut HashSet::new()).unwrap(), "a new process tries it again");
    }

    #[test]
    fn test_init_answers_without_converting_stale_shards() {
        // One large shard can outlast the addon's init RPC timeout, so opening the
        // database must leave conversion to the writer thread.
        // As main() does before opening any connection; registering twice is a no-op.
        unsafe {
            rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute(
                sqlite_vec::sqlite3_vec_init as *const (),
            )));
        }
        let profile = std::env::temp_dir().join(format!("tabmail-fts-init-test-{}", std::process::id()));
        std::fs::create_dir_all(&profile).unwrap();
        {
            let (_path, conn, _years) = open_or_create_db(&profile).unwrap();
            create_shard_without_attachment_names(&conn, 2005);
        }
        let (_path, mut conn, known_years) = open_or_create_db(&profile).unwrap();
        assert!(known_years.contains(&2005));
        assert_eq!(stale_shard_years(&conn).unwrap(), vec![2005]);

        convert_all_stale_shards(&mut conn);
        assert!(stale_shard_years(&conn).unwrap().is_empty());
        drop(conn);
        std::fs::remove_dir_all(&profile).unwrap();
    }

    // --- countMsgIdRange / fingerprintMsgIdRange / listMsgIdRange ---

    /// Insert bare msgId keys (the range RPCs only touch `message_ids`).
    fn insert_msg_id_keys(conn: &Connection, keys: &[&str]) {
        for key in keys {
            conn.execute(
                "INSERT OR IGNORE INTO message_ids (msgId) VALUES (?1)",
                params![key],
            )
            .unwrap();
        }
    }

    fn index_row(msg_id: &str, folder_id: Option<&str>) -> Value {
        let mut row = serde_json::json!({
            "msgId": msg_id,
            "dateMs": 0,
            "subject": "subject",
            "from": "sender@example.com",
            "to": "recipient@example.com",
            "body": "body",
        });
        if let Some(folder_id) = folder_id {
            row["folderId"] = Value::String(folder_id.to_string());
        }
        row
    }

    fn folder_id(conn: &Connection, msg_id: &str) -> Option<String> {
        conn.query_row(
            "SELECT folderId FROM message_folder_membership WHERE msgId = ?1",
            params![msg_id],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
    }

    #[test]
    fn test_folder_membership_schema_adds_empty_relation_and_exact_index_in_place() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE message_ids (msgId TEXT PRIMARY KEY);")
            .unwrap();
        for index in 0..64 {
            conn.execute(
                "INSERT INTO message_ids(msgId) VALUES (?1)",
                params![format!("legacy:{index:04}")],
            )
            .unwrap();
        }

        // SQLite's authorizer fires when a prepared statement reads a table.
        // A positive control proves the hook observes message_ids, then the
        // schema initializer must complete without a single such read. This
        // deterministically rejects startup backfills/index builds regardless
        // of archive size, rather than using row counts or elapsed time.
        let message_ids_reads = AtomicUsize::new(0);
        let install_result = unsafe {
            ffi::sqlite3_set_authorizer(
                conn.handle(),
                Some(count_message_ids_reads),
                &message_ids_reads as *const AtomicUsize as *mut c_void,
            )
        };
        assert_eq!(install_result, ffi::SQLITE_OK);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM message_ids", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            64
        );
        assert!(message_ids_reads.load(AtomicOrdering::SeqCst) > 0);
        message_ids_reads.store(0, AtomicOrdering::SeqCst);

        let schema_result = ensure_folder_membership_schema(&conn);
        let uninstall_result = unsafe {
            ffi::sqlite3_set_authorizer(conn.handle(), None, std::ptr::null_mut())
        };
        assert_eq!(uninstall_result, ffi::SQLITE_OK);
        schema_result.unwrap();
        assert_eq!(
            message_ids_reads.load(AtomicOrdering::SeqCst),
            0,
            "startup relation DDL must not read or scan populated message_ids"
        );

        assert_eq!(folder_id(&conn, "legacy:0000"), None);
        let message_id_columns: Vec<String> = conn
            .prepare("PRAGMA table_info(message_ids)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(message_id_columns, vec!["msgId"]);
        let relation_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'message_folder_membership'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(relation_sql.contains("WITHOUT ROWID"));
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM message_folder_membership",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
        let index_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_message_folder_membership_folder_msg'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(index_sql.contains("folderId, msgId"));

        // Idempotent on every init.
        ensure_folder_membership_schema(&conn).unwrap();
    }

    #[test]
    fn test_index_batch_writes_adopts_and_rejects_conflicting_folder_membership() {
        let (mut conn, mut known_years) = setup_test_db();

        let fresh = index_row("account:/Client:Acme:fresh@example.com", Some("folder:opaque:one"));
        assert_eq!(index_batch(&mut conn, &[fresh], None, &mut known_years).unwrap(), (1, 0));
        assert_eq!(
            folder_id(&conn, "account:/Client:Acme:fresh@example.com").as_deref(),
            Some("folder:opaque:one")
        );

        let legacy = index_row("account:/legacy@example.com", None);
        assert_eq!(index_batch(&mut conn, &[legacy], None, &mut known_years).unwrap(), (1, 0));
        let adopt = index_row("account:/legacy@example.com", Some("folder:opaque:two"));
        assert_eq!(index_batch(&mut conn, &[adopt], None, &mut known_years).unwrap(), (0, 1));
        assert_eq!(
            folder_id(&conn, "account:/legacy@example.com").as_deref(),
            Some("folder:opaque:two")
        );

        let idempotent = index_row("account:/legacy@example.com", Some("folder:opaque:two"));
        assert_eq!(index_batch(&mut conn, &[idempotent], None, &mut known_years).unwrap(), (0, 1));

        let conflict = index_row("account:/legacy@example.com", Some("folder:opaque:other"));
        let error = index_batch(&mut conn, &[conflict], None, &mut known_years).unwrap_err();
        assert!(error.to_string().contains("conflicting folderId"), "got: {error}");
        assert_eq!(
            folder_id(&conn, "account:/legacy@example.com").as_deref(),
            Some("folder:opaque:two")
        );
    }

    #[test]
    fn test_index_batch_rejects_empty_or_non_string_folder_id_atomically() {
        let (mut conn, mut known_years) = setup_test_db();
        let valid = index_row("valid@example.com", Some("folder-valid"));
        let empty = index_row("empty@example.com", Some(""));

        let error = index_batch(&mut conn, &[valid, empty], None, &mut known_years).unwrap_err();
        assert!(error.to_string().contains("folderId"), "got: {error}");
        assert_eq!(db_count(&conn).unwrap(), 0, "validation must happen before writes");

        let mut non_string = index_row("number@example.com", None);
        non_string["folderId"] = serde_json::json!(42);
        let error = index_batch(&mut conn, &[non_string], None, &mut known_years).unwrap_err();
        assert!(error.to_string().contains("folderId"), "got: {error}");
    }

    #[test]
    fn test_exact_folder_membership_uses_binary_equality_order_and_fingerprint() {
        let (conn, _known_years) = setup_test_db();
        let folder_id = "Folder:%_É😀";
        let binary_order = [
            "Msg:A",
            "msg:%",
            "msg:A",
            "msg:_",
            "msg:a",
            "msg:e\u{301}",
            "msg:é",
            "msg:😀",
        ];
        for msg_id in binary_order {
            conn.execute(
                "INSERT INTO message_ids (msgId) VALUES (?1)",
                params![msg_id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO message_folder_membership (msgId, folderId) VALUES (?1, ?2)",
                params![msg_id, folder_id],
            )
            .unwrap();
        }
        for (msg_id, decoy_folder_id) in [
            ("decoy:ascii-case", "folder:anyZÉ😀"),
            ("decoy:wildcard-literal", "folder:%_É😀"),
            ("decoy:decomposed", "Folder:%_E\u{301}😀"),
        ] {
            conn.execute(
                "INSERT INTO message_ids (msgId) VALUES (?1)",
                params![msg_id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO message_folder_membership (msgId, folderId) VALUES (?1, ?2)",
                params![msg_id, decoy_folder_id],
            )
            .unwrap();
        }

        let first = list_folder_membership(&conn, folder_id, None, 3).unwrap();
        assert_eq!(first["msgIds"], serde_json::json!(["Msg:A", "msg:%", "msg:A"]));
        assert_eq!(first["done"], false);
        let second = list_folder_membership(&conn, folder_id, Some("msg:A"), 3).unwrap();
        assert_eq!(
            second["msgIds"],
            serde_json::json!(["msg:_", "msg:a", "msg:e\u{301}"])
        );
        assert_eq!(second["done"], false);
        let third = list_folder_membership(&conn, folder_id, Some("msg:e\u{301}"), 3).unwrap();
        assert_eq!(third["msgIds"], serde_json::json!(["msg:é", "msg:😀"]));
        assert_eq!(third["done"], true);

        // Thunderbird computes this same u64-length-framed digest while it
        // consumes bounded pages; native never performs an unbounded hash.
        let mut incremental_digest = Sha256::new();
        for page in [&first, &second, &third] {
            for msg_id in page["msgIds"].as_array().unwrap() {
                let bytes = msg_id.as_str().unwrap().as_bytes();
                incremental_digest.update((bytes.len() as u64).to_be_bytes());
                incremental_digest.update(bytes);
            }
        }
        assert_eq!(
            hex::encode(incremental_digest.finalize()),
            "ffe830a2a4f235bcbd4330cfeb6d321d7680930e615d2fbcecd527c691bf6ec1"
        );

        // Explicit mutant witnesses: LIKE would treat the opaque '%' and '_'
        // as wildcards (and fold ASCII case), while NOCASE changes page order.
        let like_mutant: Vec<String> = conn
            .prepare(
                "SELECT msgId FROM message_folder_membership WHERE folderId LIKE ?1 ORDER BY msgId COLLATE BINARY",
            )
            .unwrap()
            .query_map(params![folder_id], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(like_mutant.contains(&"decoy:ascii-case".to_string()));
        assert!(like_mutant.contains(&"decoy:wildcard-literal".to_string()));
        assert_ne!(like_mutant, binary_order);

        let nocase_mutant: Vec<String> = conn
            .prepare(
                "SELECT msgId FROM message_folder_membership WHERE folderId = ?1 ORDER BY msgId COLLATE NOCASE",
            )
            .unwrap()
            .query_map(params![folder_id], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_ne!(nocase_mutant, binary_order);
    }

    #[test]
    fn test_folder_membership_page_limit_boundaries() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &["assigned", "unassigned"]);
        conn.execute(
            "INSERT INTO message_folder_membership(msgId, folderId) VALUES ('assigned', 'folder')",
            [],
        )
        .unwrap();

        for limit in [1, config::sqlite::FOLDER_MEMBERSHIP_PAGE_MAX_LIMIT] {
            assert!(list_folder_membership(&conn, "folder", None, limit).is_ok());
            assert!(list_folder_membership_state(&conn, None, limit).is_ok());
        }
        let over_max = config::sqlite::FOLDER_MEMBERSHIP_PAGE_MAX_LIMIT + 1;
        assert!(list_folder_membership(&conn, "folder", None, over_max).is_err());
        assert!(list_folder_membership_state(&conn, None, over_max).is_err());
    }

    #[test]
    fn test_folder_membership_state_pages_assigned_and_unassigned_rows() {
        let (conn, _known_years) = setup_test_db();
        conn.execute(
            "INSERT INTO message_ids (msgId) VALUES ('assigned')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message_folder_membership (msgId, folderId) VALUES ('assigned', 'folder')",
            [],
        )
        .unwrap();
        insert_msg_id_keys(&conn, &["legacy-b", "legacy-a"]);

        let first = list_folder_membership_state(&conn, None, 2).unwrap();
        assert_eq!(
            first["entries"],
            serde_json::json!([
                {"msgId": "assigned", "folderId": "folder"},
                {"msgId": "legacy-a", "folderId": null}
            ])
        );
        assert_eq!(first["done"], false);
        let second = list_folder_membership_state(&conn, Some("legacy-a"), 2).unwrap();
        assert_eq!(
            second["entries"],
            serde_json::json!([{"msgId": "legacy-b", "folderId": null}])
        );
        assert_eq!(second["done"], true);
    }

    #[test]
    fn test_membership_state_cursor_survives_assignment_and_deletion_between_pages() {
        let (mut conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &["a", "b", "c", "d"]);

        let first = list_folder_membership_state(&conn, None, 2).unwrap();
        assert_eq!(
            first["entries"],
            serde_json::json!([
                {"msgId": "a", "folderId": null},
                {"msgId": "b", "folderId": null}
            ])
        );
        assert_eq!(first["done"], false);

        // Mutate both sides of the cursor. The cursor is the returned msgId,
        // not a relation-table rowid, so deleting it remains resumable; an
        // assignment ahead of it remains visible as state rather than shifting
        // later base rows out of the page.
        conn.execute("DELETE FROM message_ids WHERE msgId = 'b'", [])
            .unwrap();
        assign_folder_membership_batch(
            &mut conn,
            &serde_json::json!([{"msgId": "c", "folderId": "folder"}]),
        )
        .unwrap();

        let second = list_folder_membership_state(&conn, Some("b"), 2).unwrap();
        assert_eq!(
            second["entries"],
            serde_json::json!([
                {"msgId": "c", "folderId": "folder"},
                {"msgId": "d", "folderId": null}
            ])
        );
        assert_eq!(second["done"], false);
        let terminal = list_folder_membership_state(&conn, Some("d"), 2).unwrap();
        assert_eq!(terminal["entries"], serde_json::json!([]));
        assert_eq!(terminal["done"], true);
    }

    #[test]
    fn test_membership_state_page_bounds_archive_inspection() {
        let (conn, _known_years) = setup_test_db();
        const ARCHIVE_ROWS: usize = 4_096;
        for index in 0..ARCHIVE_ROWS {
            let msg_id = format!("archive:{index:05}");
            conn.execute(
                "INSERT INTO message_ids(msgId) VALUES (?1)",
                params![msg_id],
            )
            .unwrap();
            if index + 1 < ARCHIVE_ROWS {
                conn.execute(
                    "INSERT INTO message_folder_membership(msgId, folderId) VALUES (?1, 'assigned')",
                    params![msg_id],
                )
                .unwrap();
            }
        }

        let bounded_steps = AtomicUsize::new(0);
        unsafe {
            ffi::sqlite3_progress_handler(
                conn.handle(),
                1,
                Some(count_vm_steps),
                &bounded_steps as *const AtomicUsize as *mut c_void,
            );
        }
        let page = list_folder_membership_state(&conn, None, 1).unwrap();
        unsafe { ffi::sqlite3_progress_handler(conn.handle(), 0, None, std::ptr::null_mut()) };
        assert_eq!(page["entries"].as_array().unwrap().len(), 1);
        let bounded_steps = bounded_steps.load(AtomicOrdering::SeqCst);
        assert!(bounded_steps < 1_000, "bounded page used {bounded_steps} VM steps");

        // Positive mutant: the removed NULL-filtering anti-join must inspect
        // nearly the entire assigned archive to find the final unassigned row.
        let mutant_steps = AtomicUsize::new(0);
        unsafe {
            ffi::sqlite3_progress_handler(
                conn.handle(),
                1,
                Some(count_vm_steps),
                &mutant_steps as *const AtomicUsize as *mut c_void,
            );
        }
        let mutant_result: String = conn
            .query_row(
                "SELECT mi.msgId FROM message_ids mi LEFT JOIN message_folder_membership fm ON fm.msgId = mi.msgId WHERE fm.msgId IS NULL ORDER BY mi.msgId COLLATE BINARY LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        unsafe { ffi::sqlite3_progress_handler(conn.handle(), 0, None, std::ptr::null_mut()) };
        assert_eq!(mutant_result, "archive:04095");
        let mutant_steps = mutant_steps.load(AtomicOrdering::SeqCst);
        assert!(
            mutant_steps > bounded_steps * 100,
            "anti-join mutant used {mutant_steps} steps vs bounded page {bounded_steps}"
        );
    }

    #[test]
    fn test_list_then_remove_batch_clears_exact_folder_relation() {
        let (mut conn, mut known_years) = setup_test_db();
        conn.execute_batch("CREATE TABLE messages_vec (rowid INTEGER PRIMARY KEY);")
            .unwrap();
        let rows = [
            index_row("remove:a", Some("folder:opaque")),
            index_row("remove:b", Some("folder:opaque")),
        ];
        assert_eq!(index_batch(&mut conn, &rows, None, &mut known_years).unwrap(), (2, 0));

        let listed = list_folder_membership(&conn, "folder:opaque", None, 10).unwrap();
        let ids = listed["msgIds"].as_array().unwrap().clone();
        assert_eq!(remove_batch(&mut conn, &ids).unwrap().count, 2);
        assert_eq!(db_count(&conn).unwrap(), 0);
        assert_eq!(
            list_folder_membership(&conn, "folder:opaque", None, 10).unwrap()["msgIds"],
            serde_json::json!([])
        );
        assert_eq!(
            list_folder_membership_state(&conn, None, 10).unwrap()["entries"],
            serde_json::json!([])
        );
    }

    fn summary_counts(conn: &Connection, folder_ids: &[&str], trusted: &[&str]) -> (i64, i64, i64) {
        let res = folder_membership_summary(conn, folder_ids, trusted).unwrap();
        assert_eq!(res["ok"], true);
        (
            res["ownerlessRows"].as_i64().unwrap(),
            res["strayTrustedRows"].as_i64().unwrap(),
            res["strayUntrustedRows"].as_i64().unwrap(),
        )
    }

    fn insert_owned(conn: &Connection, rows: &[(&str, &str)]) {
        for (msg_id, folder) in rows {
            conn.execute("INSERT INTO message_ids (msgId) VALUES (?1)", params![msg_id]).unwrap();
            conn.execute(
                "INSERT INTO message_folder_membership (msgId, folderId) VALUES (?1, ?2)",
                params![msg_id, folder],
            )
            .unwrap();
        }
    }

    #[test]
    fn test_folder_membership_summary_empty_and_fully_owned() {
        let (conn, _known_years) = setup_test_db();
        assert_eq!(summary_counts(&conn, &[], &[]), (0, 0, 0));
        insert_owned(&conn, &[("acct1:/Inbox:a", "f-inbox"), ("acct1:/Inbox:b", "f-inbox"), ("acct2:/Sent:c", "f-sent")]);
        assert_eq!(summary_counts(&conn, &["f-inbox", "f-sent"], &["acct1", "acct2"]), (0, 0, 0));
    }

    #[test]
    fn test_folder_membership_summary_counts_ownerless_rows() {
        let (conn, _known_years) = setup_test_db();
        insert_owned(&conn, &[("acct1:/Inbox:a", "f-inbox")]);
        insert_msg_id_keys(&conn, &["acct1:/Inbox:legacy1", "acct9:/Old:legacy2"]);
        assert_eq!(summary_counts(&conn, &["f-inbox"], &["acct1"]), (2, 0, 0));
    }

    #[test]
    fn test_folder_membership_summary_splits_stray_rows_by_msg_id_account() {
        let (conn, _known_years) = setup_test_db();
        insert_owned(&conn, &[
            ("acct1:/Inbox:live", "f-inbox"),
            // Stray (folder not in the request) rows of a trusted account.
            ("acct1:/Gone:a", "f-gone"),
            ("acct1:/Gone:b", "f-gone"),
            // An account id that extends a trusted one is a different account.
            ("acct10:/Gone:c", "f-gone10"),
            // No colon, or a colon first: the empty account, never trusted.
            ("nocolon", "f-gone"),
            (":leading", "f-gone"),
            // A row in a request folder whose msgId names another account
            // counts as owned: the summary compares folders, never decodes.
            ("acct2:/Elsewhere:d", "f-inbox"),
        ]);
        assert_eq!(summary_counts(&conn, &["f-inbox"], &["acct1"]), (0, 2, 3));
        assert_eq!(summary_counts(&conn, &["f-inbox"], &["acct1", "acct10"]), (0, 3, 2));
    }

    #[test]
    fn test_folder_membership_summary_with_no_folders_or_no_trusted_accounts() {
        let (conn, _known_years) = setup_test_db();
        insert_owned(&conn, &[("acct1:/Inbox:a", "f-inbox"), ("acct2:/Inbox:b", "f-inbox2")]);
        assert_eq!(summary_counts(&conn, &[], &["acct1", "acct2"]), (0, 2, 0));
        assert_eq!(summary_counts(&conn, &["f-inbox"], &[]), (0, 0, 1));
        assert_eq!(summary_counts(&conn, &[], &[]), (0, 0, 2));
    }

    #[test]
    fn test_folder_membership_summary_ignores_relation_rows_without_a_message() {
        let (conn, _known_years) = setup_test_db();
        conn.execute(
            "INSERT INTO message_folder_membership (msgId, folderId) VALUES ('acct1:/Gone:x', 'f-gone')",
            [],
        )
        .unwrap();
        assert_eq!(summary_counts(&conn, &[], &["acct1"]), (0, 0, 0));
    }

    #[test]
    fn test_folder_membership_summary_compares_folder_ids_exactly() {
        let (conn, _known_years) = setup_test_db();
        insert_owned(&conn, &[("acct1:/A:a", "Folder"), ("acct1:/A:b", "folder"), ("acct1:/A:c", "Folder ")]);
        assert_eq!(summary_counts(&conn, &["Folder"], &["acct1"]), (0, 2, 0));
    }

    #[test]
    fn test_folder_membership_summary_rejects_empty_ids() {
        let (conn, _known_years) = setup_test_db();
        assert!(folder_membership_summary(&conn, &[""], &[]).is_err());
        assert!(folder_membership_summary(&conn, &["f"], &[""]).is_err());
    }

    #[test]
    fn test_folder_membership_summary_reads_one_snapshot_under_a_concurrent_writer() {
        // A WAL file database (`:memory:` has no concurrent readers). The writer
        // moves one row between ownerless and stray in single transactions, so
        // every snapshot has exactly one of the two; a summary that read the
        // two counts from different snapshots could see none or both.
        let dir = std::env::temp_dir().join(format!("tabmail-fts-summary-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("summary.sqlite");
        let _ = std::fs::remove_file(&path);
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode = WAL;\
                 CREATE TABLE message_ids (msgId TEXT PRIMARY KEY);\
                 CREATE TABLE message_folder_membership (msgId TEXT PRIMARY KEY, folderId TEXT NOT NULL) WITHOUT ROWID;\
                 INSERT INTO message_ids (msgId) VALUES ('acct1:/Gone:x');",
            )
            .unwrap();
        let reader = open_read_only_connection(&path).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_stop = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut writes = 0usize;
            while !writer_stop.load(AtomicOrdering::SeqCst) {
                writer
                    .execute("INSERT INTO message_folder_membership (msgId, folderId) VALUES ('acct1:/Gone:x', 'f-gone')", [])
                    .unwrap();
                writer
                    .execute("DELETE FROM message_folder_membership WHERE msgId = 'acct1:/Gone:x'", [])
                    .unwrap();
                writes += 2;
            }
            writes
        });
        let mut seen = HashSet::new();
        for _ in 0..3000 {
            let (ownerless, trusted, untrusted) = summary_counts(&reader, &[], &[]);
            assert_eq!(trusted, 0);
            assert_eq!(ownerless + untrusted, 1, "a torn read saw ({ownerless}, {untrusted})");
            seen.insert(ownerless);
        }
        stop.store(true, AtomicOrdering::SeqCst);
        assert!(handle.join().unwrap() > 0);
        drop(reader);
        std::fs::remove_dir_all(&dir).unwrap();
        // Both states were observed, so the reader really ran against the writer.
        assert_eq!(seen.len(), 2);
    }

    fn remove_ids(conn: &mut Connection, ids: &[&str]) -> anyhow::Result<RemoveBatchOutcome> {
        let ids: Vec<Value> = ids.iter().map(|id| Value::String(id.to_string())).collect();
        remove_batch(conn, &ids)
    }

    fn removal_db() -> (Connection, HashSet<i32>) {
        let (mut conn, mut known_years) = setup_test_db();
        conn.execute_batch("CREATE TABLE messages_vec (rowid INTEGER PRIMARY KEY);").unwrap();
        let rows = [
            index_row("acct1:/A:a1", Some("f-a")),
            index_row("acct1:/A:a2", Some("f-a")),
            index_row("acct1:/B:b1", Some("f-b")),
            index_row("acct1:/C:legacy", None),
        ];
        assert_eq!(index_batch(&mut conn, &rows, None, &mut known_years).unwrap(), (4, 0));
        (conn, known_years)
    }

    #[test]
    fn test_remove_batch_reports_owners_of_deleted_rows() {
        let (mut conn, _known_years) = removal_db();
        let outcome = remove_ids(&mut conn, &["acct1:/A:a1"]).unwrap();
        assert_eq!(outcome, RemoveBatchOutcome { count: 1, removed_folder_ids: vec!["f-a".to_string()], removed_ownerless: 0 });

        let outcome = remove_ids(&mut conn, &["acct1:/C:legacy"]).unwrap();
        assert_eq!(outcome, RemoveBatchOutcome { count: 1, removed_folder_ids: vec![], removed_ownerless: 1 });

        let outcome = remove_ids(&mut conn, &["acct1:/Z:absent", ""]).unwrap();
        assert_eq!(outcome, RemoveBatchOutcome::default());
    }

    #[test]
    fn test_remove_batch_reports_distinct_owners_for_a_mixed_batch() {
        let (mut conn, _known_years) = removal_db();
        let outcome = remove_ids(
            &mut conn,
            &["acct1:/B:b1", "acct1:/A:a1", "acct1:/A:a2", "acct1:/A:a1", "acct1:/C:legacy", "acct1:/Z:absent"],
        )
        .unwrap();
        assert_eq!(
            outcome,
            RemoveBatchOutcome {
                count: 4,
                removed_folder_ids: vec!["f-a".to_string(), "f-b".to_string()],
                removed_ownerless: 1,
            }
        );
        assert_eq!(db_count(&conn).unwrap(), 0);
    }

    #[test]
    fn test_remove_batch_that_rolls_back_reports_nothing() {
        let (mut conn, _known_years) = removal_db();
        unsafe extern "C" fn deny_message_ids_delete(
            _: *mut std::ffi::c_void,
            action: i32,
            table: *const std::ffi::c_char,
            _: *const std::ffi::c_char,
            _: *const std::ffi::c_char,
            _: *const std::ffi::c_char,
        ) -> i32 {
            if action == rusqlite::ffi::SQLITE_DELETE
                && !table.is_null()
                && unsafe { CStr::from_ptr(table) }.to_bytes() == b"message_ids"
            {
                rusqlite::ffi::SQLITE_DENY
            } else {
                rusqlite::ffi::SQLITE_OK
            }
        }
        unsafe {
            assert_eq!(
                rusqlite::ffi::sqlite3_set_authorizer(conn.handle(), Some(deny_message_ids_delete), std::ptr::null_mut()),
                0
            );
        }
        assert!(remove_ids(&mut conn, &["acct1:/A:a1", "acct1:/C:legacy"]).is_err());
        unsafe {
            rusqlite::ffi::sqlite3_set_authorizer(conn.handle(), None, std::ptr::null_mut());
        }
        assert_eq!(db_count(&conn).unwrap(), 4);
        assert_eq!(folder_id(&conn, "acct1:/A:a1").as_deref(), Some("f-a"));
    }

    #[test]
    fn test_assign_folder_membership_batch_is_idempotent_and_conflict_atomic() {
        let (mut conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &["a", "b", "c", "d"]);

        let assigned = assign_folder_membership_batch(
            &mut conn,
            &serde_json::json!([
                {"msgId": "a", "folderId": "folder:one"},
                {"msgId": "b", "folderId": "folder:one"},
            ]),
        )
        .unwrap();
        assert_eq!(assigned, (2, 0, 0));

        let idempotent = assign_folder_membership_batch(
            &mut conn,
            &serde_json::json!([{"msgId": "a", "folderId": "folder:one"}]),
        )
        .unwrap();
        assert_eq!(idempotent, (0, 1, 0));

        let mixed = assign_folder_membership_batch(
            &mut conn,
            &serde_json::json!([
                {"msgId": "c", "folderId": "folder:two"},
                {"msgId": "outside-policy-window", "folderId": "folder:two"},
            ]),
        )
        .unwrap();
        assert_eq!(mixed, (1, 0, 1));
        assert_eq!(mixed.0 + mixed.1 + mixed.2, 2);

        let conflict = assign_folder_membership_batch(
            &mut conn,
            &serde_json::json!([
                {"msgId": "d", "folderId": "folder:two"},
                {"msgId": "another-missing", "folderId": "folder:two"},
                {"msgId": "a", "folderId": "folder:other"},
            ]),
        )
        .unwrap_err();
        assert!(conflict.to_string().contains("conflicting folderId"), "got: {conflict}");
        assert_eq!(folder_id(&conn, "d"), None, "whole assignment batch must roll back");

        let missing = assign_folder_membership_batch(
            &mut conn,
            &serde_json::json!([{"msgId": "missing", "folderId": "folder"}]),
        )
        .unwrap();
        assert_eq!(missing, (0, 0, 1));

        for invalid in [
            serde_json::json!("not-an-array"),
            serde_json::json!([{"folderId": "folder"}]),
            serde_json::json!([{"msgId": "a", "folderId": ""}]),
        ] {
            let error = assign_folder_membership_batch(&mut conn, &invalid).unwrap_err();
            assert!(
                error.to_string().contains("array")
                    || error.to_string().contains("msgId")
                    || error.to_string().contains("folderId"),
                "got: {error}"
            );
        }

        let oversized = Value::Array(
            (0..=config::sqlite::ASSIGN_FOLDER_MEMBERSHIP_BATCH_MAX)
                .map(|index| {
                    serde_json::json!({
                        "msgId": format!("message-{index}"),
                        "folderId": "folder"
                    })
                })
                .collect(),
        );
        let error = assign_folder_membership_batch(&mut conn, &oversized).unwrap_err();
        assert!(error.to_string().contains("maximum batch size"), "got: {error}");
    }

    #[test]
    fn test_assign_folder_membership_batch_size_boundaries() {
        let (mut conn, _known_years) = setup_test_db();

        let empty_error = assign_folder_membership_batch(&mut conn, &serde_json::json!([]))
            .unwrap_err();
        assert!(
            empty_error.to_string().contains("at least one"),
            "got: {empty_error}"
        );

        let max = config::sqlite::ASSIGN_FOLDER_MEMBERSHIP_BATCH_MAX;
        let msg_ids: Vec<String> = (0..max).map(|index| format!("boundary:{index:04}")).collect();
        for msg_id in &msg_ids {
            conn.execute(
                "INSERT INTO message_ids(msgId) VALUES (?1)",
                params![msg_id],
            )
            .unwrap();
        }

        let one = serde_json::json!([{"msgId": msg_ids[0], "folderId": "folder:one"}]);
        assert_eq!(assign_folder_membership_batch(&mut conn, &one).unwrap(), (1, 0, 0));

        let exact_max = Value::Array(
            msg_ids
                .iter()
                .map(|msg_id| serde_json::json!({"msgId": msg_id, "folderId": "folder:max"}))
                .collect(),
        );
        let conflict = assign_folder_membership_batch(&mut conn, &exact_max).unwrap_err();
        assert!(conflict.to_string().contains("conflicting folderId"));

        // Use a fresh database so exactly max assignments can all adopt.
        let (mut exact_conn, _known_years) = setup_test_db();
        for msg_id in &msg_ids {
            exact_conn
                .execute(
                    "INSERT INTO message_ids(msgId) VALUES (?1)",
                    params![msg_id],
                )
                .unwrap();
        }
        assert_eq!(
            assign_folder_membership_batch(&mut exact_conn, &exact_max).unwrap(),
            (max as i64, 0, 0)
        );

        let over_max = Value::Array(
            (0..=max)
                .map(|index| {
                    serde_json::json!({"msgId": format!("over:{index}"), "folderId": "folder"})
                })
                .collect(),
        );
        let over_max_error = assign_folder_membership_batch(&mut exact_conn, &over_max).unwrap_err();
        assert!(
            over_max_error.to_string().contains("maximum batch size"),
            "got: {over_max_error}"
        );
    }

    /// Folder range bounds the addon computes: start = prefix, end = prefix
    /// with the trailing ':' replaced by ';' (':' + 1).
    fn folder_range(account_id: &str, folder_path: &str) -> (String, String) {
        let prefix = format!("{account_id}:{folder_path}:");
        let end = format!("{}{}", &prefix[..prefix.len() - 1], ";");
        (prefix, end)
    }

    #[test]
    fn test_count_msg_id_range_inclusion_and_folder_scoping() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:msg1@example.com",
            "account1:/INBOX:msg2@example.com",
            "account1:/Sent:msg3@example.com",
            "account2:/INBOX:msg4@example.com",
        ]);

        let (start, end) = folder_range("account1", "/INBOX");
        assert_eq!(count_msg_id_range(&conn, &start, &end).unwrap(), 2);

        let (start, end) = folder_range("account1", "/Sent");
        assert_eq!(count_msg_id_range(&conn, &start, &end).unwrap(), 1);

        let (start, end) = folder_range("account2", "/INBOX");
        assert_eq!(count_msg_id_range(&conn, &start, &end).unwrap(), 1);
    }

    #[test]
    fn test_count_msg_id_range_excludes_subfolder_keys() {
        let (conn, _known_years) = setup_test_db();
        // Subfolder keys sort BEFORE the parent's ':' boundary ('/' < ':'),
        // so they must be excluded from the parent folder's range — and the
        // subfolder's own range must see exactly its own keys.
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:a@example.com",
            "account1:/INBOX:b@example.com",
            "account1:/INBOX/sub:c@example.com",
            "account1:/INBOX/sub:d@example.com",
            "account1:/INBOX/sub/deeper:e@example.com",
        ]);

        let (start, end) = folder_range("account1", "/INBOX");
        assert_eq!(count_msg_id_range(&conn, &start, &end).unwrap(), 2);

        let (start, end) = folder_range("account1", "/INBOX/sub");
        assert_eq!(count_msg_id_range(&conn, &start, &end).unwrap(), 2);

        let (start, end) = folder_range("account1", "/INBOX/sub/deeper");
        assert_eq!(count_msg_id_range(&conn, &start, &end).unwrap(), 1);
    }

    #[test]
    fn test_count_msg_id_range_empty_range() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &["account1:/INBOX:a@example.com"]);

        let (start, end) = folder_range("account1", "/Nowhere");
        assert_eq!(count_msg_id_range(&conn, &start, &end).unwrap(), 0);

        // Degenerate range (start == end) is empty by half-open semantics
        assert_eq!(count_msg_id_range(&conn, "account1:", "account1:").unwrap(), 0);
    }

    #[test]
    fn test_count_msg_id_range_full_keyspace() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:a@example.com",
            "account1:/Sent:b@example.com",
            "account2:/INBOX:c@example.com",
        ]);

        // ("", "\u{FFFF}") is the full-keyspace range the orphan sweep uses.
        assert_eq!(count_msg_id_range(&conn, "", "\u{FFFF}").unwrap(), 3);
    }

    #[test]
    fn test_fingerprint_msg_id_range_is_deterministic_and_membership_sensitive() {
        let (conn, _known_years) = setup_test_db();
        let (start, end) = folder_range("account1", "/INBOX");

        let empty = fingerprint_msg_id_range(&conn, &start, &end).unwrap();
        assert_eq!(empty["count"], 0);
        assert_eq!(
            empty["sha256"],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        // Insert out of order: ORDER BY msgId makes the digest deterministic.
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:b@example.com",
            "account1:/Sent:outside@example.com",
            "account1:/INBOX:a@example.com",
        ]);
        let first = fingerprint_msg_id_range(&conn, &start, &end).unwrap();
        let second = fingerprint_msg_id_range(&conn, &start, &end).unwrap();
        assert_eq!(first, second);
        assert_eq!(first["count"], 2);
        assert_eq!(first["sha256"].as_str().unwrap().len(), 64);

        insert_msg_id_keys(&conn, &["account1:/INBOX:c@example.com"]);
        let changed = fingerprint_msg_id_range(&conn, &start, &end).unwrap();
        assert_eq!(changed["count"], 3);
        assert_ne!(changed["sha256"], first["sha256"]);
    }

    #[test]
    fn test_list_msg_id_range_pagination_cursor() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:a@example.com",
            "account1:/INBOX:b@example.com",
            "account1:/INBOX:c@example.com",
            "account1:/INBOX:d@example.com",
            "account1:/INBOX:e@example.com",
            "account1:/Sent:z@example.com", // outside the range
        ]);
        let (start, end) = folder_range("account1", "/INBOX");

        // Page 1: no cursor
        let page1 = list_msg_id_range(&conn, &start, &end, None, 2).unwrap();
        let ids1: Vec<&str> = page1["msgIds"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(ids1, vec!["account1:/INBOX:a@example.com", "account1:/INBOX:b@example.com"]);
        assert_eq!(page1["done"], false);

        // Page 2: cursor = last key of page 1 (exclusive)
        let page2 = list_msg_id_range(&conn, &start, &end, Some(ids1[1]), 2).unwrap();
        let ids2: Vec<&str> = page2["msgIds"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(ids2, vec!["account1:/INBOX:c@example.com", "account1:/INBOX:d@example.com"]);
        assert_eq!(page2["done"], false);

        // Page 3: final partial page → done
        let page3 = list_msg_id_range(&conn, &start, &end, Some(ids2[1]), 2).unwrap();
        let ids3: Vec<&str> = page3["msgIds"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(ids3, vec!["account1:/INBOX:e@example.com"]);
        assert_eq!(page3["done"], true);
    }

    #[test]
    fn test_list_msg_id_range_after_key_below_start_is_ignored() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:a@example.com",
            "account1:/INBOX:b@example.com",
        ]);
        let (start, end) = folder_range("account1", "/INBOX");

        // A cursor below the range start must not widen the range: the
        // effective lower bound is max(start inclusive, after exclusive).
        let page = list_msg_id_range(&conn, &start, &end, Some(""), 10).unwrap();
        let ids: Vec<&str> = page["msgIds"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(ids, vec!["account1:/INBOX:a@example.com", "account1:/INBOX:b@example.com"]);
        assert_eq!(page["done"], true);
    }

    #[test]
    fn test_list_msg_id_range_excludes_subfolder_keys() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:a@example.com",
            "account1:/INBOX/sub:b@example.com",
        ]);
        let (start, end) = folder_range("account1", "/INBOX");

        let page = list_msg_id_range(&conn, &start, &end, None, 10).unwrap();
        let ids: Vec<&str> = page["msgIds"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(ids, vec!["account1:/INBOX:a@example.com"]);
        assert_eq!(page["done"], true);
    }

    #[test]
    fn test_list_msg_id_range_empty_range_and_default_limit() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &["account1:/INBOX:a@example.com"]);

        // Empty range
        let (start, end) = folder_range("account1", "/Nowhere");
        let page = list_msg_id_range(&conn, &start, &end, None, 10).unwrap();
        assert!(page["msgIds"].as_array().unwrap().is_empty());
        assert_eq!(page["done"], true);

        // Non-positive limit falls back to the config default (still returns rows)
        let (start, end) = folder_range("account1", "/INBOX");
        let page = list_msg_id_range(&conn, &start, &end, None, 0).unwrap();
        assert_eq!(page["msgIds"].as_array().unwrap().len(), 1);
        assert_eq!(page["done"], true);
    }

    #[test]
    fn test_list_msg_id_range_full_keyspace_walk() {
        let (conn, _known_years) = setup_test_db();
        insert_msg_id_keys(&conn, &[
            "account1:/INBOX:a@example.com",
            "account2:/Archive:b@example.com",
            "orphanAcct:/Gone:c@example.com",
        ]);

        // Orphan sweep walks ("", "\u{FFFF}") — must see every key, sorted.
        let page = list_msg_id_range(&conn, "", "\u{FFFF}", None, 10).unwrap();
        let ids: Vec<&str> = page["msgIds"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(ids.len(), 3);
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "msgIds must be ascending");
        assert_eq!(page["done"], true);
    }

    /// Clear deletes the log files before the database and stops at the first it cannot delete,
    /// so a failed clear never leaves a new database beside the old log, nor deletes the database
    /// while its log is kept. A non-empty directory in a log file's place cannot be removed as a
    /// file on any platform.
    #[test]
    fn delete_database_files_stops_before_the_database_when_a_log_file_cannot_go() {
        for (suffix, kept_log) in [("-wal", None), ("-shm", Some("-wal"))] {
            let dir = std::env::temp_dir().join(format!("tabmail-fts-delete-order-{}{suffix}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let db = dir.join("x.db");
            std::fs::write(&db, b"database").unwrap();
            if let Some(log) = kept_log {
                std::fs::write(dir.join(format!("x.db{log}")), b"log").unwrap();
            }
            let blocker = dir.join(format!("x.db{suffix}"));
            std::fs::create_dir_all(blocker.join("inside")).unwrap();

            assert!(delete_database_files(&db).is_err(), "{suffix}: a file that could not be deleted answered ok");
            assert!(db.exists(), "{suffix}: the database was deleted although its log was kept");
            if let Some(log) = kept_log {
                assert!(!dir.join(format!("x.db{log}")).exists(), "{suffix}: the log before it was not deleted");
            }
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    // --- hybrid search: sqlite-vec's k, dates, one read transaction ---

    use crate::fts::memory_db::tests::embedding_engine;

    /// An email database in its own folder, with sqlite-vec registered as main() does.
    fn temp_email_db(name: &str) -> (PathBuf, PathBuf, Connection, HashSet<i32>) {
        // Registering twice is a no-op.
        unsafe {
            rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute(
                sqlite_vec::sqlite3_vec_init as *const (),
            )));
        }
        let profile = std::env::temp_dir().join(format!("tabmail-fts-hybrid-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&profile);
        std::fs::create_dir_all(&profile).unwrap();
        let (path, conn, known_years) = open_or_create_db(&profile).unwrap();
        (profile, path, conn, known_years)
    }

    fn email_row(msg_id: &str, subject: &str, body: &str, date_ms: i64) -> Value {
        serde_json::json!({ "msgId": msg_id, "subject": subject, "from": "sender@example.com", "body": body, "dateMs": date_ms })
    }

    fn result_ids(results: &[Value]) -> Vec<String> {
        results.iter().map(|r| r["uniqueId"].as_str().unwrap().to_string()).collect()
    }

    /// With the model loaded, text that leaves no keyword term (`-- ?`, `AND`, `""`) or only a
    /// scope-like word (`nobody: cares`, an empty column filter) still searches, by meaning.
    #[test]
    fn hybrid_search_reads_any_query_text_as_text() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("any-text");
        let now = chrono::Utc::now().timestamp_millis();
        let row = email_row("acc:/:one", "Budget plan meeting", "the call about the invoice", now);
        index_batch(&mut conn, &[row], Some(&engine), &mut known_years).unwrap();
        let params = serde_json::json!({ "ignoreDate": true });
        let synonyms = SynonymLookup::new();
        for q in ["-- ?", "_", "AND", "OR", "NOT", "\"\"", "ⓐ", "nobody: cares", "acc:x", "\"meet body:x\"", "everybody: be ready",
            "subject: O'Brien don't", "subject:lunch don't", "don't NOT O'Brien"] {
            search(&conn, q, &params, &synonyms, Some(&engine), &known_years)
                .unwrap_or_else(|e| panic!("hybrid search for {q:?} failed: {e:?}"));
        }
        let many_nots = format!("budget don't{}", (0..300).map(|k| format!(" NOT qz{k:03}")).collect::<String>());
        search(&conn, &many_nots, &params, &synonyms, Some(&engine), &known_years)
            .unwrap_or_else(|e| panic!("hybrid search with 300 NOTs failed: {e:?}"));
        let results = search(&conn, "budget plan meeting", &params, &synonyms, Some(&engine), &known_years).unwrap();
        assert_eq!(result_ids(&results), vec!["acc:/:one".to_string()]);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// sqlite-vec answers at most VEC_K_MAX nearest vectors and refuses a larger k (a search at
    /// SEARCH_LIMIT_MAX asks for no more: a compile-time check in config).
    #[test]
    fn sqlite_vec_answers_at_most_vec_k_max() {
        let (profile, _path, conn, _years) = temp_email_db("k-max");
        let dims = config::embedding::EMBEDDING_DIMS;
        let vector = |i: usize| {
            let mut v = vec![0.0f32; dims];
            v[0] = 1.0;
            v[1 + i % (dims - 1)] = (i as f32) / 10_000.0;
            f32_vec_to_blob(&v)
        };
        let rows = config::hybrid::VEC_K_MAX as usize + 10;
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..rows {
            tx.execute("INSERT INTO messages_vec (rowid, embedding) VALUES (?1, ?2)", params![i as i64 + 1, vector(i)]).unwrap();
        }
        tx.commit().unwrap();
        let query = vector(0);
        for limit in [config::hybrid::VEC_K_MAX - 1, config::hybrid::VEC_K_MAX] {
            let found = search_vec_candidates(&conn, "messages_vec", "message_meta", &query, limit, None, None, None).unwrap();
            assert_eq!(found.len() as i64, limit, "asked for {limit}");
            assert!(found.iter().any(|c| c.0 == 1), "the nearest is among them");
        }
        assert!(search_vec_candidates(&conn, "messages_vec", "message_meta", &query, config::hybrid::VEC_K_MAX + 1, None, None, None).is_err());
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// With dates or a field's rowids, sqlite-vec searches only the rows dated within the dates (either
    /// bound alone, or both, each inclusive) and among those rowids: the k nearest of them, however many
    /// nearer rows lie outside. A vector with no metadata row is found only without either.
    #[test]
    fn search_vec_candidates_searches_only_within_the_dates_and_the_field() {
        let (profile, _path, conn, _years) = temp_email_db("vec-within");
        let dims = config::embedding::EMBEDDING_DIMS;
        // Farther from the query the larger i is.
        let vector = |i: i64| {
            let mut v = vec![0.0f32; dims];
            v[0] = 1.0;
            v[1] = i as f32 / 100.0;
            f32_vec_to_blob(&v)
        };
        let tx = conn.unchecked_transaction().unwrap();
        for i in 1..=20 {
            tx.execute("INSERT INTO messages_vec (rowid, embedding) VALUES (?1, ?2)", params![i, vector(i)]).unwrap();
            tx.execute("INSERT INTO message_meta (rowid, dateMs, hasAttachments, shardYear) VALUES (?1, ?2, 0, 2000)", params![i, i * 100]).unwrap();
        }
        // Dated far before 1970 and far in the future (a message's own date is stored as it is):
        // a missing bound leaves the dates open on that side.
        for (i, date_ms) in [(21, -5_000_000_000_000i64), (22, 10_000_000_000_000)] {
            tx.execute("INSERT INTO messages_vec (rowid, embedding) VALUES (?1, ?2)", params![i, vector(i)]).unwrap();
            tx.execute("INSERT INTO message_meta (rowid, dateMs, hasAttachments, shardYear) VALUES (?1, ?2, 0, 2000)", params![i, date_ms]).unwrap();
        }
        // The nearest of all, with no metadata row.
        tx.execute("INSERT INTO messages_vec (rowid, embedding) VALUES (99, ?1)", params![vector(0)]).unwrap();
        tx.commit().unwrap();
        let query = vector(0);
        let nearest = |k: i64, from: Option<i64>, to: Option<i64>, eligible: Option<&[i64]>| {
            let eligible: Option<HashSet<i64>> = eligible.map(|e| e.iter().copied().collect());
            search_vec_candidates(&conn, "messages_vec", "message_meta", &query, k, from, to, eligible.as_ref())
                .unwrap()
                .iter()
                .map(|c| c.0)
                .collect::<Vec<_>>()
        };
        assert_eq!(nearest(3, None, None, None), [99, 1, 2]);
        assert_eq!(nearest(3, Some(1000), Some(1500), None), [10, 11, 12]);
        assert_eq!(nearest(3, Some(1500), Some(1500), None), [15]);
        assert_eq!(nearest(4, Some(1800), None, None), [18, 19, 20, 22]);
        assert_eq!(nearest(3, None, Some(200), None), [1, 2, 21]);
        assert_eq!(nearest(3, Some(2100), Some(2200), None), Vec::<i64>::new());
        assert_eq!(nearest(3, None, None, Some(&[17, 5, 2, 99])), [2, 5, 17]);
        assert_eq!(nearest(3, Some(1000), None, Some(&[17, 5, 2, 99])), [17]);
        assert_eq!(nearest(3, None, None, Some(&[])), Vec::<i64>::new());
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A search with dates finds a match inside them by meaning alone however many nearer messages lie
    /// outside them (more than the vector candidates it asks for), and a search naming a field finds
    /// one among the field's matches however many nearer messages lie outside the field (#26).
    #[test]
    fn search_finds_a_match_by_meaning_past_nearer_messages_outside_the_dates_or_the_field() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("past-nearer");
        let day = 86_400_000;
        let start = chrono::Utc::now().timestamp_millis() - 30 * day;
        let end = start + day;
        let limit = 2;
        // Twice the vector candidates a search asks for, and more.
        let nearer = limit * config::hybrid::CANDIDATE_MULTIPLIER * 2 + 4;
        let row = |id: &str, subject: &str, from: &str, body: &str, date_ms: i64| {
            serde_json::json!({ "msgId": id, "subject": subject, "from": from, "body": body, "dateMs": date_ms })
        };
        // Near the queries by meaning, with none of their words (or synonyms) but "vacation".
        let mut rows = vec![row("acc:/:inside", "Vacation", "alice@example.com", "The route and hotels for our summer holiday", start + day / 2)];
        for i in 0..nearer {
            // Nearer the first query, on both sides of the dates.
            rows.push(row(&format!("acc:/:before-{i}"), "Vacation", "bob@example.com", "please plan the vacation itinerary", start - day - i));
            rows.push(row(&format!("acc:/:after-{i}"), "Vacation", "bob@example.com", "please plan the vacation itinerary", end + day + i));
            // Nearer the second, from someone else.
            rows.push(row(&format!("acc:/:other-{i}"), "From Alice: plan the itinerary", "bob@example.com", "from alice plan the itinerary", start - 2 * day - i));
        }
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let inside: i64 = conn.query_row("SELECT rowid FROM message_ids WHERE msgId = 'acc:/:inside'", [], |r| r.get(0)).unwrap();
        for (q, params) in [
            ("plan the vacation itinerary", serde_json::json!({ "from": start, "to": end, "limit": limit })),
            ("from:alice plan the itinerary", serde_json::json!({ "ignoreDate": true, "limit": limit })),
        ] {
            // The match is not among the nearest over the whole archive.
            let query = f32_vec_to_blob(&engine.embed(q).unwrap());
            let nearest = search_vec_candidates(&conn, "messages_vec", "message_meta", &query, nearer, None, None, None).unwrap();
            assert!(nearest.iter().all(|c| c.0 != inside), "{q}");
            let results = search(&conn, q, &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap();
            assert_eq!(result_ids(&results), vec!["acc:/:inside".to_string()], "{q} {params}");
            assert_eq!(results[0]["snippet"], "", "found by meaning, not by its words");
        }
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A search that excludes a sender or a word (a kept NOT) returns nothing it excludes: what a
    /// message means can't exclude it, so the search answers by keyword. Issue #21.
    #[test]
    fn search_excluding_a_sender_or_a_word_returns_none_of_it() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("not");
        let now = chrono::Utc::now().timestamp_millis();
        let row = |id: &str, from: &str, subject: &str, body: &str| {
            serde_json::json!({ "msgId": id, "subject": subject, "from": from, "body": body, "dateMs": now })
        };
        let rows = [
            row("acc:/:erin", "erin@example.com", "Lunch", "Lunch on Friday at noon"),
            row("acc:/:frank", "frank@example.com", "Lunch", "Lunch on Friday at noon"),
            row("acc:/:itinerary", "erin@example.com", "Vacation", "The vacation itinerary and hotels"),
            row("acc:/:plans", "erin@example.com", "Vacation", "Vacation plans for the summer"),
        ];
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        for (q, expected) in [
            ("lunch NOT from:erin@example.com", vec!["acc:/:frank"]),
            ("lunch NOT from:frank@example.com", vec!["acc:/:erin"]),
            ("lunch on friday NOT from:erin@example.com", vec!["acc:/:frank"]),
            ("vacation NOT itinerary", vec!["acc:/:plans"]),
            ("vacation NOT from:erin@example.com", vec![]),
        ] {
            for sort in ["relevance", "date_desc", "date_asc"] {
                let params = serde_json::json!({ "ignoreDate": true, "sort": sort });
                let results = search(&conn, q, &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap();
                assert_eq!(result_ids(&results), expected, "{q} {sort}");
            }
        }
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A hybrid search asks sqlite-vec for `limit` x CANDIDATE_MULTIPLIER nearest vectors, which it
    /// refuses past VEC_K_MAX; the `limit` is capped at
    /// SEARCH_LIMIT_MAX to keep within it. At any `limit` asked, a match by meaning alone is still
    /// found: the search stays hybrid. With a field, a match by meaning outside the field is left out.
    #[test]
    fn search_with_a_large_limit_finds_a_match_by_meaning() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("large-limit");
        let now = chrono::Utc::now().timestamp_millis();
        let found = email_row("acc:/:found", "Vacation", "Here is the vacation itinerary", now);
        let outside_field = email_row("acc:/:outside-field", "Holiday", "Here is the vacation itinerary", now);
        index_batch(&mut conn, &[found, outside_field], Some(&engine), &mut known_years).unwrap();
        let most = config::hybrid::SEARCH_LIMIT_MAX;
        for (q, expected) in [
            ("plan the vacation itinerary", vec!["acc:/:found", "acc:/:outside-field"]),
            ("subject:vacation plan the itinerary", vec!["acc:/:found"]),
        ] {
            for limit in [10, most, most + 1, 2_000, 4_000] {
                let params = serde_json::json!({ "ignoreDate": true, "limit": limit });
                let results = search(&conn, q, &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap();
                let mut ids = result_ids(&results);
                ids.sort();
                assert_eq!(ids, expected, "{q} at limit {limit}");
                assert!(results.iter().all(|r| r["snippet"] == ""), "found by meaning, not by their words");
            }
        }
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A search with dates returns its best matches within them however many nearer messages lie
    /// before or after them, either bound alone keeping those on its side.
    #[test]
    fn search_with_dates_keeps_the_matches_within_them() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("dates");
        let day = 86_400_000;
        let start = chrono::Utc::now().timestamp_millis() - 30 * day;
        let end = start + day;
        let mut rows = Vec::new();
        // Nearer the query than the matches within the dates.
        for (side, at) in [("before", start - 2 * day), ("after", end + day)] {
            for i in 0..8 {
                rows.push(email_row(&format!("acc:/:{side}-{i}"), "Trip itinerary", &format!("trip itinerary {i}"), at + i * 1000));
            }
        }
        // Within the dates, one at each bound: messages that name it once in a long body, so they
        // score below the nearer ones (above MIN_SCORE).
        let inside: Vec<String> = [start, start + 1000, end].iter().enumerate().map(|(i, &at)| {
            let id = format!("acc:/:inside-{i}");
            rows.push(email_row(&id, &format!("Lunch order {i}"), &format!("lunch order {i}, soup and sandwich, then the trip itinerary, {}", "the quarterly budget review, the weather, the parking. ".repeat(50)), at));
            id
        }).collect();
        // Unrelated messages within the dates, so the word is rare and a keyword match scores.
        for i in 0..100 {
            rows.push(email_row(&format!("acc:/:filler-{i}"), &format!("Weather report {i}"), &format!("sunny on day {i}"), start + 100_000 + i * 1000));
        }
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let search_with = |params: Value| {
            let mut ids = result_ids(&search(&conn, "trip itinerary", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap());
            ids.sort();
            ids
        };

        // The nearer messages outside the dates are found without them.
        let undated = search_with(serde_json::json!({ "ignoreDate": true, "limit": 50 }));
        assert!(undated.contains(&"acc:/:before-0".to_string()) && undated.contains(&"acc:/:after-0".to_string()), "{undated:?}");
        assert_eq!(search_with(serde_json::json!({ "from": start, "to": end, "limit": inside.len() })), inside);
        // One bound alone: the matches within it, and the nearer messages on its side, nothing else.
        let after: Vec<String> = (0..8).map(|i| format!("acc:/:after-{i}")).collect();
        let before: Vec<String> = (0..8).map(|i| format!("acc:/:before-{i}")).collect();
        let mut from_only = [inside.clone(), after].concat();
        from_only.sort();
        let mut to_only = [inside.clone(), before].concat();
        to_only.sort();
        assert_eq!(search_with(serde_json::json!({ "from": start, "limit": from_only.len() })), from_only);
        assert_eq!(search_with(serde_json::json!({ "to": end, "limit": to_only.len() })), to_only);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    struct WriteDuringSearch {
        writer: Connection,
        known_years: HashSet<i32>,
        engine: *const EmbeddingEngine,
        at: usize,
        steps: usize,
        written: bool,
    }

    extern "C" fn write_during_search(state: *mut c_void) -> c_int {
        let state = unsafe { &mut *(state as *mut WriteDuringSearch) };
        state.steps += 1;
        if state.steps == state.at {
            state.written = true;
            let engine = unsafe { &*state.engine };
            let tomorrow = chrono::Utc::now().timestamp_millis() + 86_400_000;
            let replacement = email_row("acc:/:replacement", "Unrelated", "A completely unrelated message", tomorrow);
            assert_eq!(remove_batch(&mut state.writer, &[serde_json::json!("acc:/:found")]).unwrap().count, 1);
            index_batch(&mut state.writer, &[replacement], Some(engine), &mut state.known_years).unwrap();
        }
        0
    }

    /// A search answers from the archive as it was when it began: wherever in it a write lands, it
    /// gives the message it found, never one that took its rowid meanwhile, outside the dates. With
    /// dates or without: a search without them could give that other message the same way.
    #[test]
    fn search_answers_from_the_archive_as_it_began() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, path, mut conn, mut known_years) = temp_email_db("snapshot");
        let synonyms = SynonymLookup::new();
        let now = chrono::Utc::now().timestamp_millis();
        let found = email_row("acc:/:found", "Vacation", "Here is the vacation itinerary", now);
        let query = "plan the vacation itinerary";
        let mut state = WriteDuringSearch {
            writer: open_writer_connection(&path).unwrap(),
            known_years: known_years.clone(),
            engine: &engine,
            at: 0,
            steps: 0,
            written: false,
        };
        let search_with_write_at = |conn: &Connection, known_years: &HashSet<i32>, state: &mut WriteDuringSearch, params: &Value, at: usize| {
            (state.at, state.steps, state.written) = (at, 0, false);
            unsafe { ffi::sqlite3_progress_handler(conn.handle(), 1, Some(write_during_search), state as *mut WriteDuringSearch as *mut c_void) };
            let rows = search(conn, query, params, &synonyms, Some(&engine), known_years).unwrap();
            unsafe { ffi::sqlite3_progress_handler(conn.handle(), 0, None, std::ptr::null_mut()) };
            rows
        };

        index_batch(&mut conn, &[found.clone()], Some(&engine), &mut known_years).unwrap();
        for params in [serde_json::json!({ "from": now - 1, "to": now + 1, "limit": 10 }), serde_json::json!({ "ignoreDate": true, "limit": 10 })] {
            // Found by meaning only (no keyword match), so the vector leg answers it.
            let before = search_with_write_at(&conn, &known_years, &mut state, &params, 0);
            assert_eq!(result_ids(&before), ["acc:/:found"], "{params}");
            assert_eq!(before[0]["snippet"], "");
            // From past the transaction's start to the search's last step. How many steps a search
            // takes changes with the writes before it, so it is counted before each (on the second
            // search: the first after a write takes a few more).
            for point in 0..=40 {
                search_with_write_at(&conn, &known_years, &mut state, &params, 0);
                search_with_write_at(&conn, &known_years, &mut state, &params, 0);
                let steps = state.steps;
                assert!(steps > 100, "a search took {steps} steps");
                let at = 20 + (steps - 20) * point / 40;
                let during = search_with_write_at(&conn, &known_years, &mut state, &params, at);
                assert!(state.written, "the write landed at step {at} of {steps}");
                assert_eq!(result_ids(&during), ["acc:/:found"], "with the write at step {at} of {steps}, {params}");
                assert_eq!(during[0]["dateMs"], now);
                // The write took the found message's rowid.
                let reused: i64 = conn.query_row("SELECT rowid FROM message_ids WHERE msgId = 'acc:/:replacement'", [], |r| r.get(0)).unwrap();
                assert_eq!(reused, 1);
                // As it was, for the next step.
                known_years.extend(state.known_years.iter().copied());
                assert_eq!(remove_ids(&mut conn, &["acc:/:replacement"]).unwrap().count, 1);
                index_batch(&mut conn, &[found.clone()], Some(&engine), &mut known_years).unwrap();
            }
        }
        drop(state);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// Refuses every read of message_meta, counting them.
    extern "C" fn deny_message_meta_reads(
        denied: *mut c_void,
        action: c_int,
        table: *const std::os::raw::c_char,
        _column: *const std::os::raw::c_char,
        _db: *const std::os::raw::c_char,
        _trigger: *const std::os::raw::c_char,
    ) -> c_int {
        if action == ffi::SQLITE_READ && !table.is_null() && unsafe { CStr::from_ptr(table) }.to_bytes() == b"message_meta" {
            unsafe { *(denied as *mut usize) += 1 };
            return ffi::SQLITE_DENY;
        }
        ffi::SQLITE_OK
    }

    /// A search that fails part way ends its read transaction: the next request on the connection
    /// is answered.
    #[test]
    fn search_after_a_failed_one_is_answered() {
        let (mut conn, mut known_years) = setup_test_db();
        index_batch(&mut conn, &[email_row("acc:/:budget", "Budget", "the quarterly budget", 0)], None, &mut known_years).unwrap();
        let params = serde_json::json!({ "ignoreDate": true });
        let mut denied = 0usize;
        unsafe { ffi::sqlite3_set_authorizer(conn.handle(), Some(deny_message_meta_reads), &mut denied as *mut usize as *mut c_void) };
        let failed = search(&conn, "budget", &params, &SynonymLookup::new(), None, &known_years);
        unsafe { ffi::sqlite3_set_authorizer(conn.handle(), None, std::ptr::null_mut()) };
        assert!(denied > 0 && failed.is_err(), "the search failed on a read inside it");
        let next = search(&conn, "budget", &params, &SynonymLookup::new(), None, &known_years).unwrap();
        assert_eq!(result_ids(&next), ["acc:/:budget"]);
    }

    /// Refuses every read of the table a `DeniedTable` names, counting them.
    struct DeniedTable {
        name: &'static [u8],
        denied: usize,
    }

    extern "C" fn deny_reads_of_table(
        context: *mut c_void,
        action: c_int,
        table: *const std::os::raw::c_char,
        _column: *const std::os::raw::c_char,
        _db: *const std::os::raw::c_char,
        _trigger: *const std::os::raw::c_char,
    ) -> c_int {
        let state = unsafe { &mut *(context as *mut DeniedTable) };
        if action == ffi::SQLITE_READ && !table.is_null() && unsafe { CStr::from_ptr(table) }.to_bytes() == state.name {
            state.denied += 1;
            return ffi::SQLITE_DENY;
        }
        ffi::SQLITE_OK
    }

    /// When the vector search itself fails, an email or memory search still answers, by keyword
    /// only (the error is logged), within its dates and field, and the next search is answered.
    #[test]
    fn search_whose_vector_leg_fails_answers_by_keyword() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("vector-fails");
        let day = 86_400_000;
        let now = chrono::Utc::now().timestamp_millis();
        let rows = [
            email_row("acc:/:budget", "Budget", "the quarterly budget", now),
            email_row("acc:/:older", "Review", "the quarterly budget", now - 30 * day),
        ];
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let (_memory_path, mut memory) = crate::fts::memory_db::open_or_create_memory_db(&profile).unwrap();
        let exchange = |id: &str, date_ms: i64, i: i64| serde_json::json!({ "memId": id, "role": "session", "sessionId": "persistent_chat",
            "content": "the quarterly budget", "dateMs": date_ms, "turnIndex": i });
        crate::fts::memory_db::memory_index_batch(&mut memory, &[exchange("budget", now, 0), exchange("older", now - 30 * day, 1)], Some(&engine)).unwrap();
        let with_denied = |conn: &Connection, name: &'static [u8], run: &dyn Fn() -> Vec<Value>| {
            let mut state = DeniedTable { name, denied: 0 };
            unsafe { ffi::sqlite3_set_authorizer(conn.handle(), Some(deny_reads_of_table), &mut state as *mut DeniedTable as *mut c_void) };
            let results = run();
            unsafe { ffi::sqlite3_set_authorizer(conn.handle(), None, std::ptr::null_mut()) };
            assert!(state.denied > 0, "the vector search was refused");
            results
        };
        let undated = serde_json::json!({ "ignoreDate": true });
        let dated = serde_json::json!({ "from": now - day, "to": now + day });
        let email_search = |q: &str, params: &Value| search(&conn, q, params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap();
        let memory_search = |params: &Value| crate::fts::memory_db::memory_search(&memory, "budget", params, &SynonymLookup::new(), Some(&engine)).unwrap();
        let sorted = |mut ids: Vec<String>| {
            ids.sort();
            ids
        };

        // Without dates or a field, with dates, and with a field: the keyword matches in scope.
        for (q, params, expected) in [
            ("budget", &undated, vec!["acc:/:budget", "acc:/:older"]),
            ("budget", &dated, vec!["acc:/:budget"]),
            ("subject:budget quarterly", &undated, vec!["acc:/:budget"]),
        ] {
            let email = with_denied(&conn, b"messages_vec", &|| email_search(q, params));
            assert_eq!(sorted(result_ids(&email)), expected, "{q} {params}");
            assert!(email.iter().all(|r| r["snippet"] != ""), "found by their words");
        }
        for (params, expected) in [(&undated, vec!["budget", "older"]), (&dated, vec!["budget"])] {
            let memories = with_denied(&memory, b"memory_vec", &|| memory_search(params));
            assert_eq!(sorted(memories.iter().map(|r| r["memId"].as_str().unwrap().to_string()).collect()), expected, "{params}");
            assert!(memories.iter().all(|r| r["snippet"] != ""), "found by their words");
        }

        // The next searches, with the vector table readable, are answered.
        assert_eq!(result_ids(&email_search("budget", &dated)), ["acc:/:budget"]);
        assert_eq!(memory_search(&dated).len(), 1);
        drop(conn);
        drop(memory);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A search returns at most 512 results, whatever `limit` it is given (owner 2026-10-21; the
    /// expected counts are the decided numbers, not derived from the config they pin).
    #[test]
    fn search_returns_at_most_search_limit_max_results() {
        let (mut conn, mut known_years) = setup_test_db();
        let rows: Vec<Value> = (0..600).map(|i| email_row(&format!("acc:/:budget-{i}"), "Budget", "the quarterly budget", i)).collect();
        index_batch(&mut conn, &rows, None, &mut known_years).unwrap();
        for (limit, expected) in [(10, 10), (512, 512), (513, 512), (2_000, 512), (4_000, 512)] {
            let params = serde_json::json!({ "ignoreDate": true, "limit": limit });
            let results = search(&conn, "budget", &params, &SynonymLookup::new(), None, &known_years).unwrap();
            assert_eq!(results.len(), expected, "at limit {limit}");
        }
    }

    /// A search with dates whose vector candidates are all outside them answers by keyword, as it
    /// does with no vectors at all: every keyword match within the dates, however weak.
    #[test]
    fn search_with_no_vector_within_the_dates_answers_by_keyword() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("no-vector-within");
        let day = 86_400_000;
        let now = chrono::Utc::now().timestamp_millis();
        // Within the dates, no vectors yet; "budget" in each, so it weighs little by keyword.
        let within: Vec<Value> = (0..20).map(|i| email_row(&format!("acc:/:within-{i}"), "Notes", &format!("the quarterly budget, item {i}"), now - i)).collect();
        index_batch(&mut conn, &within, None, &mut known_years).unwrap();
        let outside: Vec<Value> = (0..5).map(|i| email_row(&format!("acc:/:outside-{i}"), "Weather", "rain tomorrow", now - (30 + i) * day)).collect();
        index_batch(&mut conn, &outside, Some(&engine), &mut known_years).unwrap();
        let mut expected: Vec<String> = (0..20).map(|i| format!("acc:/:within-{i}")).collect();
        expected.sort();
        let params = serde_json::json!({ "from": now - day, "to": now + day, "limit": 100 });
        let mut found = result_ids(&search(&conn, "budget", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap());
        found.sort();
        assert_eq!(found, expected);
        // In the sort asked for.
        let params = serde_json::json!({ "from": now - day, "to": now + day, "limit": 3, "sort": "date_asc" });
        let found = result_ids(&search(&conn, "budget", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap());
        assert_eq!(found, ["acc:/:within-19", "acc:/:within-18", "acc:/:within-17"]);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A search whose merge keeps nothing although words match answers by keyword: a word in most
    /// messages scores about 0 by keyword (FTS5 clamps its IDF), and a one-word query's vector scores
    /// are weak, so every merged score fell under MIN_SCORE and the search returned nothing (#28).
    #[test]
    fn search_whose_merge_keeps_nothing_answers_by_keyword() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("merge-keeps-nothing");
        let hour = 3_600_000;
        // One year shard: the 30 equal matches score equal by keyword.
        let now = mid_year_ms();
        let mut rows: Vec<Value> = (0..30).map(|i| email_row(&format!("acc:/:budget-{i}"), "Budget", "the quarterly budget", now - i * hour)).collect();
        rows.extend((0..10).map(|i| email_row(&format!("acc:/:weather-{i}"), "Weather", "rain tomorrow", now - (100 + i) * hour)));
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let budget = |range: std::ops::Range<i64>| -> Vec<String> { range.map(|i| format!("acc:/:budget-{i}")).collect() };
        let search_with = |params: Value| result_ids(&search(&conn, "budget", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap());

        let mut found = search_with(serde_json::json!({ "ignoreDate": true, "limit": 100 }));
        found.sort();
        let mut expected = budget(0..30);
        expected.sort();
        assert_eq!(found, expected);
        // Within dates: only the matches within them, with matches both before and after them.
        let mut dated = search_with(serde_json::json!({ "from": now - 9 * hour - hour / 2, "to": now - 4 * hour - hour / 2, "limit": 100 }));
        dated.sort();
        let mut expected = budget(5..10);
        expected.sort();
        assert_eq!(dated, expected);
        // Fewer than the matches: the best `limit` (all tied), newest first; in a date sort the
        // newest or oldest `limit`.
        assert_eq!(search_with(serde_json::json!({ "ignoreDate": true, "limit": 5 })), budget(0..5));
        assert_eq!(search_with(serde_json::json!({ "ignoreDate": true, "limit": 5, "sort": "date_desc" })), budget(0..5));
        let mut oldest = budget(25..30);
        oldest.reverse();
        assert_eq!(search_with(serde_json::json!({ "ignoreDate": true, "limit": 5, "sort": "date_asc" })), oldest);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A query whose words match a row answers by the merge when it keeps something: a match by
    /// meaning alone, without the words, is among the results.
    #[test]
    fn search_whose_merge_keeps_results_answers_by_meaning_too() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("merge-keeps-results");
        let now = chrono::Utc::now().timestamp_millis();
        let rows = vec![
            email_row("acc:/:words", "Plan", "plan the vacation itinerary for the team", now),
            email_row("acc:/:meaning", "Vacation", "Here is the vacation itinerary", now - 1),
        ];
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let params = serde_json::json!({ "ignoreDate": true, "limit": 10 });
        let mut found = result_ids(&search(&conn, "plan the vacation itinerary", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap());
        found.sort();
        assert_eq!(found, vec!["acc:/:meaning".to_string(), "acc:/:words".to_string()]);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A merge that keeps a single result is the answer too: the words of the query are in 30
    /// messages but past the embedding input, so they score about 0 both ways, and one message
    /// without the words is kept by meaning.
    #[test]
    fn search_whose_merge_keeps_one_result_answers_by_it() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("merge-keeps-one");
        let now = chrono::Utc::now().timestamp_millis();
        let query = "plan the vacation itinerary";
        let long = format!("{} {query}", "rain tomorrow ".repeat(160));
        let mut rows: Vec<Value> = (0..30).map(|i| email_row(&format!("acc:/:words-{i}"), "Weather", &long, now - i)).collect();
        rows.extend((0..10).map(|i| email_row(&format!("acc:/:other-{i}"), "Weather", "rain tomorrow", now - 100 - i)));
        rows.push(email_row("acc:/:meaning", "Vacation", "Here is the vacation itinerary", now - 200));
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let params = serde_json::json!({ "ignoreDate": true, "limit": 100 });
        let found = result_ids(&search(&conn, query, &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap());
        assert_eq!(found, vec!["acc:/:meaning".to_string()]);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A time in the middle of the current year: rows within weeks of it share a year shard, so equal
    /// rows score equal by keyword.
    fn mid_year_ms() -> i64 {
        Utc.with_ymd_and_hms(Utc::now().year(), 7, 1, 0, 0, 0).unwrap().timestamp_millis()
    }

    fn email_from(msg_id: &str, from: &str, subject: &str, date_ms: i64) -> Value {
        serde_json::json!({ "msgId": msg_id, "subject": subject, "from": from, "body": "status of the week", "dateMs": date_ms })
    }

    /// Whether each result comes after the one before it in `sort` order (by `rank`, lower is
    /// better, and `dateMs`).
    fn assert_in_sort_order(results: &[Value], sort: SearchSort) {
        for pair in results.windows(2) {
            let (rank_a, rank_b) = (pair[0]["rank"].as_f64().unwrap(), pair[1]["rank"].as_f64().unwrap());
            let (date_a, date_b) = (pair[0]["dateMs"].as_i64().unwrap(), pair[1]["dateMs"].as_i64().unwrap());
            let in_order = match sort {
                SearchSort::Relevance => rank_a < rank_b || (rank_a == rank_b && date_a >= date_b),
                SearchSort::DateDesc => date_a > date_b || (date_a == date_b && rank_a <= rank_b),
                SearchSort::DateAsc => date_a < date_b || (date_a == date_b && rank_a <= rank_b),
            };
            assert!(in_order, "{sort:?}: {} before {}", pair[0], pair[1]);
        }
    }

    fn sort_name(sort: SearchSort) -> &'static str {
        match sort {
            SearchSort::Relevance => "relevance",
            SearchSort::DateDesc => "date_desc",
            SearchSort::DateAsc => "date_asc",
        }
    }

    /// A query of only fields (with operators between them) is one the vector leg has nothing to
    /// mean in; a free word, phrase or group, or text that is not a field, is not.
    #[test]
    fn a_query_of_only_fields_is_field_only() {
        let built = |q: &str| build_fts_match(Some(q), QUERY_COLUMNS, true, &SynonymLookup::new());
        for q in [
            "from:alice@example.com",
            "from: alice@example.com",
            r#"from:"Alice Example""#,
            "from:a@example.com OR to:a@example.com",
            "subject:budget NOT from:bob@example.com",
            "To:bob@example.com cc:carol@example.com bcc:dan@example.com",
            "body:quarterly",
        ] {
            assert!(query_is_field_only(&built(q)), "{q} -> {}", built(q));
        }
        for q in [
            "budget",
            "from:alice@example.com budget",
            r#"from:alice@example.com "quarterly budget""#,
            "from:alice@example.com meeting",
            "from:alice@example.com don't",
            "note:launch",
            "from:",
            "AND",
            "",
        ] {
            assert!(!query_is_field_only(&built(q)), "{q} -> {}", built(q));
        }
    }

    /// A query excludes when it keeps a NOT, before a field, a word, a phrase or a group; a NOT
    /// dropped, quoted, lower-case or a field's value excludes nothing.
    #[test]
    fn a_query_that_keeps_a_not_excludes() {
        let built = |q: &str| build_fts_match(Some(q), QUERY_COLUMNS, true, &SynonymLookup::new());
        for q in [
            "lunch NOT from:erin@example.com",
            "subject:budget NOT from:bob@example.com",
            "vacation NOT itinerary",
            r#"vacation NOT "summer itinerary""#,
            "invoice NOT receipt",
            "abcd NOT don't",
            "lunch OR dinner NOT from:erin@example.com",
        ] {
            assert!(query_excludes(&built(q)), "{q} -> {}", built(q));
        }
        for q in [
            "lunch",
            "NOT receipt",
            "abcd NOT",
            "abcd OR NOT efgh",
            r#""lunch NOT dinner""#,
            "lunch not dinner",
            "subject:NOT lunch",
            "from:a@example.com OR to:a@example.com",
            "",
        ] {
            assert!(!query_excludes(&built(q)), "{q} -> {}", built(q));
        }
    }

    /// An unknown `sort` is refused, with or without a query, rather than answered in another order.
    #[test]
    fn search_refuses_an_unknown_sort() {
        let (mut conn, mut known_years) = setup_test_db();
        index_batch(&mut conn, &[email_row("acc:/:budget", "Budget", "the quarterly budget", mid_year_ms())], None, &mut known_years).unwrap();
        for (q, sort) in [("budget", serde_json::json!("newest")), ("budget", serde_json::json!(1)), ("", serde_json::json!("oldest"))] {
            let params = serde_json::json!({ "ignoreDate": true, "sort": sort });
            assert!(search(&conn, q, &params, &SynonymLookup::new(), None, &known_years).is_err(), "{q:?} {sort}");
        }
        let params = serde_json::json!({ "ignoreDate": true, "sort": "date_asc" });
        assert_eq!(result_ids(&search(&conn, "budget", &params, &SynonymLookup::new(), None, &known_years).unwrap()), ["acc:/:budget"]);
    }

    /// A search by keyword only (no model) sorts every match before it takes the limit: relevance,
    /// the best keyword match first and ties newer first; a date sort, ties the better match first.
    #[test]
    fn keyword_search_sorts_every_match_before_the_limit() {
        let (mut conn, mut known_years) = setup_test_db();
        let mid = mid_year_ms();
        let day = 86_400_000;
        let long = "a long note about the garden, the trip, the car, the house and then the budget";
        let mut rows = vec![
            email_row("acc:/:strong-old", "Budget", "budget budget budget", mid - 10 * day),
            email_row("acc:/:weak-new", "Notes", long, mid - day),
            // Each tie written in the other order than its tie-break gives.
            email_row("acc:/:tie-b", "Report", "the budget report", mid - 5 * day),
            email_row("acc:/:tie-a", "Report", "the budget report", mid - 3 * day),
            email_row("acc:/:day-weak", "Notes", long, mid - 7 * day),
            email_row("acc:/:day-strong", "Budget", "budget budget", mid - 7 * day),
        ];
        rows.extend((0..10).map(|i| email_row(&format!("acc:/:other-{i}"), "Weather", "rain tomorrow", mid - (20 + i) * day)));
        index_batch(&mut conn, &rows, None, &mut known_years).unwrap();
        let search_by = |sort: SearchSort, limit: i64| {
            let params = serde_json::json!({ "ignoreDate": true, "limit": limit, "sort": sort_name(sort) });
            search(&conn, "budget", &params, &SynonymLookup::new(), None, &known_years).unwrap()
        };
        let position = |results: &[Value], id: &str| result_ids(results).iter().position(|r| r == id).unwrap();
        let rank = |results: &[Value], id: &str| results[position(results, id)]["rank"].as_f64().unwrap();
        for sort in [SearchSort::Relevance, SearchSort::DateDesc, SearchSort::DateAsc] {
            let all = search_by(sort, 100);
            assert_eq!(all.len(), 6, "{sort:?}");
            assert_in_sort_order(&all, sort);
            // The limit is the first of that order.
            assert_eq!(result_ids(&search_by(sort, 2)), result_ids(&all[..2]), "{sort:?}");
            // An exact tie in relevance, newer first; a tie in date, the better match first.
            assert_eq!(rank(&all, "acc:/:tie-a"), rank(&all, "acc:/:tie-b"));
            assert!(rank(&all, "acc:/:day-strong") < rank(&all, "acc:/:day-weak"));
            assert!(position(&all, "acc:/:day-strong") < position(&all, "acc:/:day-weak"), "{sort:?}");
        }
        let relevance = search_by(SearchSort::Relevance, 100);
        assert!(position(&relevance, "acc:/:tie-a") < position(&relevance, "acc:/:tie-b"));
        assert_ne!(result_ids(&relevance)[0], "acc:/:weak-new", "the best match first, not the newest");
        assert_eq!(result_ids(&search_by(SearchSort::DateDesc, 1)), ["acc:/:weak-new"]);
        assert_eq!(result_ids(&search_by(SearchSort::DateAsc, 1)), ["acc:/:strong-old"]);
    }

    /// A query of only fields answers by keyword over every match, in sort order (relevance: newest
    /// first), with or without the model: the newest and the oldest from a sender are exact although
    /// they score worst (a longer From).
    #[test]
    fn a_search_of_only_fields_sorts_every_match_by_date() {
        let engine = embedding_engine();
        let (profile, _path, mut conn, mut known_years) = temp_email_db("fields-only");
        let mid = mid_year_ms();
        let day = 86_400_000;
        let alice = |i: i64| {
            let from = if !(5..25).contains(&i) { "Alice Example, with a long name for the test <alice@example.com>" } else { "alice@example.com" };
            email_from(&format!("acc:/:alice-{i}"), from, &format!("Weekly update {i}"), mid - i * day)
        };
        let mut rows: Vec<Value> = (0..30).map(alice).collect();
        rows.extend((0..10).map(|i| email_from(&format!("acc:/:bob-{i}"), "bob@example.com", &format!("Notes {i}"), mid - i * day - day / 2)));
        index_batch(&mut conn, &rows, engine.as_ref(), &mut known_years).unwrap();
        let ids = |names: &[&str]| -> Vec<String> { names.iter().map(|n| format!("acc:/:{n}")).collect() };
        for engine in [None, engine.as_ref()] {
            let search_by = |q: &str, sort: &str, limit: i64| {
                let params = serde_json::json!({ "ignoreDate": true, "limit": limit, "sort": sort });
                result_ids(&search(&conn, q, &params, &SynonymLookup::new(), engine, &known_years).unwrap())
            };
            let q = "from:alice@example.com";
            assert_eq!(search_by(q, "relevance", 3), ids(&["alice-0", "alice-1", "alice-2"]), "model: {}", engine.is_some());
            assert_eq!(search_by(q, "date_desc", 3), ids(&["alice-0", "alice-1", "alice-2"]));
            assert_eq!(search_by(q, "date_asc", 3), ids(&["alice-29", "alice-28", "alice-27"]));
            assert_eq!(search_by(q, "date_asc", 100).len(), 30);
            let either = "from:alice@example.com OR from:bob@example.com";
            assert_eq!(search_by(either, "relevance", 3), ids(&["alice-0", "bob-0", "alice-1"]));
        }
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A date-sorted hybrid search sorts a larger pool's matches by date before it takes the limit:
    /// the newest (oldest) match is first although many matches score better.
    #[test]
    fn a_date_sorted_search_finds_the_newest_and_oldest_match() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("date-sorted");
        let mid = mid_year_ms();
        let day = 86_400_000;
        let mut rows: Vec<Value> = (0..20).map(|i| email_row(&format!("acc:/:strong-{i}"), "Budget", "the quarterly budget, budget review", mid - (2 + i) * day)).collect();
        rows.push(email_row("acc:/:weak-new", "Notes", "notes on the trip and the budget", mid));
        rows.push(email_row("acc:/:weak-old", "Notes", "notes on the trip and the budget", mid - 40 * day));
        rows.extend((0..40).map(|i| email_row(&format!("acc:/:other-{i}"), "Weather", "rain tomorrow", mid - (50 + i) * day)));
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let search_by = |sort: &str, limit: i64| {
            let params = serde_json::json!({ "ignoreDate": true, "limit": limit, "sort": sort });
            search(&conn, "budget", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap()
        };
        // Both are matches; neither is among the best 3.
        let kept = result_ids(&search_by("relevance", 100));
        assert!(kept.contains(&"acc:/:weak-new".to_string()) && kept.contains(&"acc:/:weak-old".to_string()), "{kept:?}");
        let best = result_ids(&search_by("relevance", 3));
        assert!(!best.contains(&"acc:/:weak-new".to_string()) && !best.contains(&"acc:/:weak-old".to_string()), "{best:?}");
        assert_eq!(result_ids(&search_by("date_desc", 3)), ["acc:/:weak-new", "acc:/:strong-0", "acc:/:strong-1"]);
        assert_eq!(result_ids(&search_by("date_asc", 3)), ["acc:/:weak-old", "acc:/:strong-19", "acc:/:strong-18"]);
        for sort in [SearchSort::Relevance, SearchSort::DateDesc, SearchSort::DateAsc] {
            assert_in_sort_order(&search_by(sort_name(sort), 100), sort);
        }
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A match by meaning alone sorts by its own date.
    #[test]
    fn a_date_sorted_search_orders_a_match_by_meaning_by_its_date() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("meaning-by-date");
        let mid = mid_year_ms();
        let rows = vec![
            email_row("acc:/:words", "Plan", "plan the vacation itinerary for the team", mid - 86_400_000),
            email_row("acc:/:meaning", "Vacation", "Here is the vacation itinerary", mid),
        ];
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let search_by = |sort: &str| {
            let params = serde_json::json!({ "ignoreDate": true, "limit": 10, "sort": sort });
            search(&conn, "plan the vacation itinerary", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap()
        };
        let desc = search_by("date_desc");
        assert_eq!(result_ids(&desc), ["acc:/:meaning", "acc:/:words"]);
        assert_eq!(desc[0]["snippet"], "", "found by meaning");
        assert_eq!(result_ids(&search_by("date_asc")), ["acc:/:words", "acc:/:meaning"]);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// Equal matches (the same text, so the same score both ways) come newer first, whatever order
    /// they were written in, and the limit keeps the newest of them.
    #[test]
    fn a_hybrid_search_puts_equal_matches_newer_first() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("equal-newer-first");
        let mid = mid_year_ms();
        let hour = 3_600_000;
        let ages = [5, 2, 7, 0, 3, 6, 1, 4];
        let mut rows: Vec<Value> = ages.iter().map(|&h| email_row(&format!("acc:/:same-{h}"), "Review", "the quarterly budget review", mid - h * hour)).collect();
        rows.extend((0..20).map(|i| email_row(&format!("acc:/:other-{i}"), "Weather", "rain tomorrow", mid - (100 + i) * hour)));
        index_batch(&mut conn, &rows, Some(&engine), &mut known_years).unwrap();
        let search_with = |limit: i64| {
            let params = serde_json::json!({ "ignoreDate": true, "limit": limit });
            search(&conn, "quarterly budget review", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap()
        };
        let all = search_with(100);
        let newest_first: Vec<String> = (0..8).map(|h| format!("acc:/:same-{h}")).collect();
        assert_eq!(result_ids(&all[..8]), newest_first);
        assert!(all[..8].iter().all(|r| r["rank"] == all[0]["rank"]), "an exact tie");
        assert_eq!(result_ids(&search_with(3)), newest_first[..3]);
        drop(conn);
        let _ = std::fs::remove_dir_all(profile);
    }

    /// A date sort reads 4,096 keyword candidates: with exactly that many matches, the newest
    /// (oldest) one is found although it is the worst by keyword, so any smaller pool misses it.
    /// One vector keeps the search hybrid (an index part way through its embedding rebuild).
    #[test]
    fn a_date_sort_reads_4096_keyword_candidates() {
        let Some(engine) = embedding_engine() else { return };
        let now = mid_year_ms();
        for (sort, weak_date) in [("date_desc", now + 1000), ("date_asc", now - 4095 * 1000)] {
            let (profile, _path, mut conn, mut known_years) = temp_email_db(&format!("pool-keyword-{sort}"));
            let strong = |i: i64| email_row(&format!("acc:/:strong-{i}"), "Budget", "budget budget budget budget", now - i * 1000);
            index_batch(&mut conn, &[strong(0)], Some(&engine), &mut known_years).unwrap();
            let mut rows: Vec<Value> = (1..4095).map(strong).collect();
            rows.push(email_row("acc:/:weak", "Notes", "budget", weak_date));
            rows.extend((0..16384).map(|i| email_row(&format!("acc:/:other-{i}"), "Weather", "rain tomorrow", now - 10_000_000 - i * 1000)));
            index_batch(&mut conn, &rows, None, &mut known_years).unwrap();
            // The weak row is the last of the 4,096 keyword matches, and still a match.
            let fts_query = build_fts_match(Some("budget"), QUERY_COLUMNS, true, &SynonymLookup::new());
            let all = search_fts_candidates(&conn, &fts_query, None, None, 4096, &known_years).unwrap();
            assert_eq!(all.len(), 4096);
            assert_eq!(all.last().unwrap().msg_id, "acc:/:weak");
            assert!(crate::fts::hybrid::bm25_rank_to_score(all.last().unwrap().rank) * config::hybrid::EMAIL_TEXT_WEIGHT >= config::hybrid::MIN_SCORE);
            assert_eq!(vec_count(&conn), 1);
            let params = serde_json::json!({ "ignoreDate": true, "limit": 1, "sort": sort });
            let results = search(&conn, "budget", &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap();
            assert_eq!(result_ids(&results), ["acc:/:weak"], "{sort}");
            drop(conn);
            let _ = std::fs::remove_dir_all(profile);
        }
    }

    /// A date sort reads 4,096 vector candidates: with exactly that many, the newest (oldest) match
    /// by meaning alone is found although it is the farthest, so any smaller pool misses it.
    #[test]
    fn a_date_sort_reads_4096_vector_candidates() {
        let Some(engine) = embedding_engine() else { return };
        let now = mid_year_ms();
        let q = "plan the vacation itinerary";
        let body = "plan the vacation itinerary for the team";
        let meaning_body = "Here is the vacation itinerary";
        // The rows' embeddings as indexing writes them; the 4,095 equal rows share one.
        let embed = |subject: &str, body: &str| f32_vec_to_blob(&engine.embed(&crate::embeddings::text_prep::prepare_email_text(subject, "sender@example.com", "", body)).unwrap());
        let (strong_blob, meaning_blob) = (embed("Plan", body), embed("Vacation", meaning_body));
        for (sort, meaning_date) in [("date_desc", now + 1000), ("date_asc", now - 4095 * 1000)] {
            let (profile, _path, mut conn, mut known_years) = temp_email_db(&format!("pool-vector-{sort}"));
            let mut rows: Vec<Value> = (0..4095).map(|i| email_row(&format!("acc:/:strong-{i}"), "Plan", body, now - i * 1000)).collect();
            rows.push(email_row("acc:/:meaning", "Vacation", meaning_body, meaning_date));
            index_batch(&mut conn, &rows, None, &mut known_years).unwrap();
            let tx = conn.transaction().unwrap();
            for rowid in 1..=4096 {
                let blob = if rowid == 4096 { &meaning_blob } else { &strong_blob };
                tx.execute("INSERT INTO messages_vec(rowid, embedding) VALUES (?1, ?2)", params![rowid, blob]).unwrap();
            }
            tx.commit().unwrap();
            // The meaning row is the farthest of the 4,096 vectors, and still a match.
            let query_blob = f32_vec_to_blob(&engine.embed(q).unwrap());
            let all = search_vec_candidates(&conn, "messages_vec", "message_meta", &query_blob, 4096, None, None, None).unwrap();
            assert_eq!(all.len(), 4096);
            assert_eq!(all.last().unwrap().0, 4096);
            assert!(all.last().unwrap().1 > all[0].1);
            let dates = HashMap::from([(4096, meaning_date)]);
            let merged = crate::fts::hybrid::merge_results(&[], &[*all.last().unwrap()], &dates, config::hybrid::EMAIL_VECTOR_WEIGHT, config::hybrid::EMAIL_TEXT_WEIGHT, SearchSort::Relevance, 1);
            assert_eq!(merged.len(), 1, "a match by meaning");
            let params = serde_json::json!({ "ignoreDate": true, "limit": 1, "sort": sort });
            let results = search(&conn, q, &params, &SynonymLookup::new(), Some(&engine), &known_years).unwrap();
            assert_eq!(result_ids(&results), ["acc:/:meaning"], "{sort}");
            assert_eq!(results[0]["snippet"], "", "found by meaning alone");
            drop(conn);
            let _ = std::fs::remove_dir_all(profile);
        }
    }

    /// When reading the vector-only candidates' dates fails, the email or memory search fails (not
    /// an empty answer), ends its read transaction, and the next search is answered.
    #[test]
    fn a_search_whose_date_lookup_fails_fails_and_the_next_is_answered() {
        let Some(engine) = embedding_engine() else { return };
        let (profile, _path, mut conn, mut known_years) = temp_email_db("dates-fail");
        index_batch(&mut conn, &[email_row("acc:/:found", "Vacation", "Here is the vacation itinerary", mid_year_ms())], Some(&engine), &mut known_years).unwrap();
        let (_memory_path, mut memory) = crate::fts::memory_db::open_or_create_memory_db(&profile).unwrap();
        let row = serde_json::json!({ "memId": "found", "role": "session", "sessionId": "persistent_chat",
            "content": "Here is the vacation itinerary", "dateMs": mid_year_ms(), "turnIndex": 0 });
        crate::fts::memory_db::memory_index_batch(&mut memory, &[row], Some(&engine)).unwrap();
        let params = serde_json::json!({ "ignoreDate": true });
        let synonyms = SynonymLookup::new();
        // `AND` leaves no keyword term, and an undated vector search reads no metadata: the first
        // metadata read is the date lookup.
        assert_eq!(build_fts_match(Some("AND"), QUERY_COLUMNS, true, &synonyms), "");
        assert_eq!(build_fts_match(Some("AND"), &[], true, &synonyms), "");
        assert_eq!(vec_count(&conn), 1);
        assert_eq!(crate::fts::memory_db::memory_vec_count(&memory), 1);
        let denied_search = |c: &Connection, name: &'static [u8], run: &dyn Fn() -> anyhow::Result<Vec<Value>>| {
            let mut state = DeniedTable { name, denied: 0 };
            unsafe { ffi::sqlite3_set_authorizer(c.handle(), Some(deny_reads_of_table), &mut state as *mut DeniedTable as *mut c_void) };
            let result = run();
            unsafe { ffi::sqlite3_set_authorizer(c.handle(), None, std::ptr::null_mut()) };
            assert!(state.denied > 0, "the date lookup was refused");
            assert!(result.is_err(), "a failed date lookup is not an empty answer");
            assert!(c.is_autocommit(), "the failed search ended its read transaction");
        };
        denied_search(&conn, b"message_meta", &|| search(&conn, "AND", &params, &synonyms, Some(&engine), &known_years));
        denied_search(&memory, b"memory_meta", &|| crate::fts::memory_db::memory_search(&memory, "AND", &params, &synonyms, Some(&engine)));
        let next = search(&conn, "plan the vacation itinerary", &params, &synonyms, Some(&engine), &known_years).unwrap();
        assert_eq!(result_ids(&next), ["acc:/:found"]);
        let next = crate::fts::memory_db::memory_search(&memory, "plan the vacation itinerary", &params, &synonyms, Some(&engine)).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0]["memId"], "found");
        drop(conn);
        drop(memory);
        let _ = std::fs::remove_dir_all(profile);
    }
}
