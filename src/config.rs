// IMPORTANT:
// Keep ALL numeric values centralized here (repo rule: no hardcoded numeric values scattered around).

pub const HOST_VERSION: &str = "0.6.9";

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


