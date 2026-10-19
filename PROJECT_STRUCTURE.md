# TabMail Native FTS - Project Structure

> **Directory tree, entry points, and sub-component map.** Update when the structure changes.

**Last updated:** 2026-08-22

---

## Project Configuration

| Setting | Value |
|---------|-------|
| Language | Rust |
| Version | 0.11.x |
| DB | SQLite (WAL mode) |
| Embeddings | Candle (local inference) |
| Semantic search | sqlite-vec |
| Communication | Native messaging (stdin/stdout JSON) |

---

## Directory Tree

```
tabmail-native-fts/
├── Cargo.toml                   # Dependencies: ureq, rusqlite, candle, sqlite-vec
├── Cargo.lock
│
├── src/
│   ├── main.rs                  # Entry point: native messaging loop, thread dispatch
│   ├── config.rs                # SCHEMA_VERSION, version constants
│   ├── protocol.rs              # Message type definitions
│   ├── native_messaging.rs      # stdin/stdout JSON protocol handler
│   ├── logging.rs               # Logging with file rotation
│   ├── install_paths.rs         # Platform-specific file paths
│   ├── self_update.rs           # Auto-update download & verification
│   ├── update_signature.rs      # Ed25519 signature verification
│   │
│   ├── fts/                     # Full-text search
│   │   ├── mod.rs               # Module exports
│   │   ├── db.rs                # SQLite FTS5 + exact folder-membership operations
│   │   ├── memory_db.rs         # In-memory index
│   │   ├── query.rs             # Query parsing & expansion
│   │   ├── hybrid.rs            # Hybrid search (lexical + semantic)
│   │   └── synonyms.rs          # Query synonym expansion
│   │
│   └── embeddings/              # Local ML inference
│       ├── mod.rs               # Module exports
│       ├── engine.rs            # Candle embedding engine (shared via Arc)
│       ├── text_prep.rs         # Tokenization, text preprocessing
│       └── download.rs          # Model downloading & caching
│
└── dist/                        # Build outputs for distribution
```

---

## Threading Model

```
main thread (stdin reader + dispatcher)
    ├── classify_method() → reader channel OR writer channel
    │
    ├── reader thread (read-only: search, stats, filter)
    │   └── reads through its read-only connections, kept in Arc<Mutex> so the writer can close them on a clear (ADR-NF-009)
    │
    └── writer thread (write: indexBatch, membership adoption, removeBatch, clear)
        └── owns its read-write connections, the last to close at exit
        └── on clear/memoryClear locks the reader's connection, closes it, deletes + recreates the files, reopens it (ADR-NF-009)
        └── while idle, converts stale year shards one at a time (ADR-NF-005)

Shared: EmbeddingEngine (Arc), SynonymLookup (Arc), Stdout (Arc<Mutex>)
```

**Pre-init (single-threaded):** hello, init, updateCheck, updateRequest
**Post-init (multi-threaded):** all other methods dispatched by `classify_method()`

`message_ids` remains unchanged. The additive
`message_folder_membership(msgId, folderId)` table carries a covering
`(folderId, msgId)` index. Reader RPCs use exact folder equality; legacy rows
missing a relation are paged separately for bounded, resumable adoption. The
empty table/index are constant-size startup DDL, so existing archives are not
scanned or synchronously reindexed on init.
