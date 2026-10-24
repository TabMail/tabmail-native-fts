/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// IMPORTANT:
// Keep ALL numeric values centralized here (repo rule: no hardcoded numeric values scattered around).

// NOTE: HOST_VERSION must stay in sync with the `version` field in Cargo.toml.
pub const HOST_VERSION: &str = "0.12.0";

/// Schema version: bump ONLY for changes that genuinely require a FULL re-index
/// from Thunderbird (the addon re-feeds every message through native messaging —
/// hours on big archives; see nativeEngine.js checkSchemaVersionChange). DB schema
/// and FTS tokenizer changes must instead migrate IN PLACE host-side whenever the
/// FTS tables still hold the content (see db.rs rebuild_next_stale_shard —
/// the 2026-06 tokenchars drop migrated this way with NO version bump).
pub const SCHEMA_VERSION: u32 = 1;

pub mod logging {
    pub const LOG_DIR_REL: &str = ".tabmail/logs";
    pub const LOG_FILE_NAME: &str = "fts_helper.log";

    pub const LOG_ROTATE_SIZE_BYTES: u64 = 10 * 1024 * 1024;
    pub const LOG_ROTATE_KEEP_FILES: usize = 5;

    // `--log-level`: the file log's level, chosen by the caller (ADR-NF-008).
    // "off" writes no log file. Absent: DEFAULT_LOG_LEVEL.
    pub const LOG_LEVELS: [&str; 5] = ["off", "error", "warn", "info", "debug"];
    pub const DEFAULT_LOG_LEVEL: &str = "debug";
}

pub mod native_messaging {
    pub const MAX_MESSAGE_SIZE_BYTES: u32 = 128 * 1024 * 1024;
}

pub mod update {
    // When the host downloads a new binary, it is staged with this suffix (needed for Windows
    // because you cannot overwrite a running .exe).
    pub const STAGED_SUFFIX: &str = ".new";

    pub const DOWNLOAD_TIMEOUT_SECS: u64 = 30;
}

pub mod sqlite {
    pub const PRAGMA_BUSY_TIMEOUT_MS: i64 = 2000;
    pub const PRAGMA_CACHE_SIZE_KIB_NEG: i64 = -64000;
    pub const PRAGMA_MMAP_SIZE_BYTES: i64 = 268_435_456;
    pub const PRAGMA_WAL_AUTOCHECKPOINT_PAGES: i64 = 200_000;

    pub const FTS_PREFIXES: &str = "2 3 4";
    /// No tokenchars (ADR-024): unicode61 treats ALL non-alphanumeric characters
    /// as separators by default; the old `tokenchars '-_.@'` option made exactly
    /// those four token-INTERNAL, gluing addresses into single tokens that could
    /// only be prefix-matched from the token start. Without it, addresses index
    /// as parts and any part is matchable. Changing this string triggers
    /// the in-place shard rebuild in db.rs (NO SCHEMA_VERSION bump — see above).
    /// Keep in lockstep with tabmail-ios SearchConfig.ftsTokenize.
    pub const FTS_TOKENIZE: &str = "porter unicode61 remove_diacritics 2";

    pub const SEARCH_DEFAULT_LIMIT: i64 = 50;
    /// Capital `NOT` operators a search query keeps. FTS5 nests each `NOT` (it flattens `AND`
    /// and `OR` chains) and refuses an expression deeper than 256, so a query with more answered
    /// an error (#22). Well below 256, to leave room for the groups and column filters around
    /// them; an exclusion past it is left out of the query.
    pub const SEARCH_MAX_KEPT_NOTS: usize = 100;
    pub const SEARCH_SNIPPET_TOKENS: i64 = 16;
    pub const SEARCH_DEBUG_SAMPLE_LIMIT: i64 = 10;
    pub const QUERY_BY_DATE_RANGE_DEFAULT_LIMIT: i64 = 1000;
    /// Default page size for `listMsgIdRange` when the caller omits `limit`
    /// (the addon normally passes its own FOLDER_RECON_KEYS_CHUNK).
    pub const LIST_MSG_ID_RANGE_DEFAULT_LIMIT: i64 = 500;
    /// Folder-membership reconciliation is unbounded overall and advances through
    /// bounded pages so each reader request remains responsive.
    pub const FOLDER_MEMBERSHIP_PAGE_DEFAULT_LIMIT: i64 = 500;
    pub const FOLDER_MEMBERSHIP_PAGE_MAX_LIMIT: i64 = 2_000;
    pub const ASSIGN_FOLDER_MEMBERSHIP_BATCH_MAX: usize = 1_000;
    /// Most msgIds one `getAttachmentFlags` request may ask about.
    pub const GET_ATTACHMENT_FLAGS_MAX_IDS: usize = 1_000;

    // Year-based FTS sharding
    pub const SHARD_MIN_YEAR: i32 = 2000;
    pub const FTS_AUTOMERGE: i32 = 2;
    pub const FTS_USERMERGE: i32 = 2;
}

pub mod embedding {
    pub const EMBEDDING_DIMS: usize = 384;

    // Max word-piece tokens for all-MiniLM-L6-v2 (model context limit is 256).
    // We pre-truncate to control what gets embedded.
    pub const MAX_TOKENS: usize = 256;

    // Model download URL base (lazy download on first use).
    // Hosted on CF R2 bucket (tabmail-cdn) at cdn.tabmail.ai.
    pub const MODEL_CDN_BASE: &str = "https://cdn.tabmail.ai/releases/models/all-MiniLM-L6-v2";

    // SHA256 hashes for integrity verification
    pub const MODEL_SAFETENSORS_SHA256: &str =
        "53aa51172d142c89d9012cce15ae4d6cc0ca6895895114379cacb4fab128d9db";
    pub const TOKENIZER_JSON_SHA256: &str =
        "be50c3628f2bf5bb5e3a7f17b1f74611b2561a3a27eeab05e5aa30f411572037";
    pub const CONFIG_JSON_SHA256: &str =
        "953f9c0d463486b10a6871cc2fd59f223b2c70184f49815e7efbcab5d8908b41";

    // Local model storage directory (relative to home)
    pub const MODEL_DIR_REL: &str = ".tabmail/models/all-MiniLM-L6-v2";
}

pub mod memory {
    // Memory size limit (client-supplied `maxBytes`). Size = pages in use
    // ((page_count - freelist_count) * page_size): freed pages are reused for
    // new rows, so the file keeps its largest size and memory stays within the
    // limit inside it.

    // A trim removes oldest rows until the used size is at or below this
    // percentage of the limit, so the next few batches do not each trim again.
    pub const TRIM_TARGET_PERCENT: i64 = 95;
    // Wall-time budget for one trim, checked after every row removed; well
    // under the add-on's native RPC timeout (60 s). A trim cut short keeps the
    // rows it removed, reports `withinLimit: false`, and the next write or trim
    // continues it.
    pub const TRIM_TIME_BUDGET_MS: u64 = 20_000;
    // Pages one FTS merge step writes; the budget is checked between steps.
    // The trim starts its merge once (negative) after removing rows and
    // continues it with positive steps, so a merge cut short survives the
    // next write's new segment.
    pub const TRIM_MERGE_STEP_PAGES: i64 = 256;
    // A trim estimates each row's share of the used size from its content
    // length plus this many bytes, for its index entries, metadata and vector
    // row. Only how rows compare matters: the estimates are scaled to the
    // measured used size, and the trim re-measures after each pass.
    pub const TRIM_ROW_OVERHEAD_BYTES: i64 = 512;
}

pub mod hybrid {
    // Hybrid search weights: how much each engine contributes to final score.
    // Semantic dominant — the LLM crafts queries blind (doesn't know user's email vocabulary).
    pub const EMAIL_VECTOR_WEIGHT: f64 = 0.7;
    pub const EMAIL_TEXT_WEIGHT: f64 = 0.3;

    pub const MEMORY_VECTOR_WEIGHT: f64 = 0.7;
    pub const MEMORY_TEXT_WEIGHT: f64 = 0.3;

    // Fetch N× candidates from each engine, merge to final limit.
    pub const CANDIDATE_MULTIPLIER: i64 = 4;

    // The most nearest vectors sqlite-vec answers in one query (`SQLITE_VEC_VEC0_K_MAX`): it
    // refuses a larger k.
    pub const VEC_K_MAX: i64 = 4096;

    // The most results a search with a query returns, at any positive `limit` (one cap for every
    // search, owner 2026-10-21, ADR-NF-013). Its vector candidates (CANDIDATE_MULTIPLIER per result)
    // stay within what sqlite-vec answers, so every search is fully hybrid at any positive limit asked.
    pub const SEARCH_LIMIT_MAX: i64 = 512;
    const _: () = assert!(SEARCH_LIMIT_MAX * CANDIDATE_MULTIPLIER <= VEC_K_MAX);

    // Candidates each leg of a date-sorted email search takes (ADR-NF-017): its matches are sorted
    // by date from these, so a match outside the best `limit` x CANDIDATE_MULTIPLIER is still found.
    // sqlite-vec's maximum, chosen by a study on a 300,000-message archive (2026-10-22): the first
    // result was the truly newest match in every dated query set, at about the median cost of 2,048.
    pub const DATE_SORT_POOL: i64 = VEC_K_MAX;

    // Minimum combined score of a candidate found by meaning alone (filters noise). A keyword match
    // is a result whatever its score (#51).
    pub const MIN_SCORE: f64 = 0.1;

    // Vector score threshold for rescaling. Scores below this map to 0;
    // scores above are rescaled to 0..1 via max(0, (score - th) / (1 - th)).
    // Prevents weak semantic associations from inflating final scores.
    pub const VECTOR_SCORE_THRESHOLD: f64 = 0.45;
}
