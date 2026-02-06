// IMPORTANT:
// Keep ALL numeric values centralized here (repo rule: no hardcoded numeric values scattered around).

// NOTE: HOST_VERSION must stay in sync with the `version` field in Cargo.toml.
pub const HOST_VERSION: &str = "0.8.1";

/// Schema version: bump ONLY when DB schema, FTS tokenizer config, or embedding
/// model changes. Non-schema host updates (e.g., multi-threading) leave this unchanged.
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
    pub const FTS_TOKENIZE: &str = "porter unicode61 remove_diacritics 2 tokenchars '-_.@'";

    pub const SEARCH_DEFAULT_LIMIT: i64 = 50;
    pub const SEARCH_SNIPPET_TOKENS: i64 = 16;
    pub const SEARCH_DEBUG_SAMPLE_LIMIT: i64 = 10;
    pub const QUERY_BY_DATE_RANGE_DEFAULT_LIMIT: i64 = 1000;
}

pub mod embedding {
    pub const EMBEDDING_DIMS: usize = 384;
    pub const EMBEDDING_MODEL_NAME: &str = "all-MiniLM-L6-v2";

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
}


