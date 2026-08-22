/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// IMPORTANT:
// Keep ALL numeric values centralized here (repo rule: no hardcoded numeric values scattered around).

// NOTE: HOST_VERSION must stay in sync with the `version` field in Cargo.toml.
pub const HOST_VERSION: &str = "0.11.3";

/// Schema version: bump ONLY for changes that genuinely require a FULL re-index
/// from Thunderbird (the addon re-feeds every message through native messaging —
/// hours on big archives; see nativeEngine.js checkSchemaVersionChange). DB schema
/// and FTS tokenizer changes must instead migrate IN PLACE host-side whenever the
/// FTS tables still hold the content (see db.rs rebuild_stale_tokenizer_shards —
/// the 2026-06 tokenchars drop migrated this way with NO version bump).
pub const SCHEMA_VERSION: u32 = 1;

pub mod logging {
    pub const LOG_DIR_REL: &str = ".tabmail/logs";
    pub const LOG_FILE_NAME: &str = "fts_helper.log";

    pub const LOG_ROTATE_SIZE_BYTES: u64 = 10 * 1024 * 1024;
    pub const LOG_ROTATE_KEEP_FILES: usize = 5;
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

    /// Per-init time budget for the in-place tokenizer shard rebuild. Must stay
    /// comfortably under the addon's 60s native RPC timeout (nativeEngine.js
    /// RPC_TIMEOUT_MS); shards left over convert on the next init.
    pub const RETOKENIZE_TIME_BUDGET_SECS: u64 = 45;

    pub const SEARCH_DEFAULT_LIMIT: i64 = 50;
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

pub mod hybrid {
    // Hybrid search weights: how much each engine contributes to final score.
    // Semantic dominant — the LLM crafts queries blind (doesn't know user's email vocabulary).
    pub const EMAIL_VECTOR_WEIGHT: f64 = 0.7;
    pub const EMAIL_TEXT_WEIGHT: f64 = 0.3;

    pub const MEMORY_VECTOR_WEIGHT: f64 = 0.7;
    pub const MEMORY_TEXT_WEIGHT: f64 = 0.3;

    // Fetch N× candidates from each engine, merge to final limit.
    pub const CANDIDATE_MULTIPLIER: i64 = 4;

    // Minimum combined score to return (filters noise).
    pub const MIN_SCORE: f64 = 0.1;

    // Vector score threshold for rescaling. Scores below this map to 0;
    // scores above are rescaled to 0..1 via max(0, (score - th) / (1 - th)).
    // Prevents weak semantic associations from inflating final scores.
    pub const VECTOR_SCORE_THRESHOLD: f64 = 0.45;
}
