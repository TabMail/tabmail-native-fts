# TabMail Native FTS - Project Structure

> **Directory tree, entry points, and sub-component map.** Update when the structure changes.

**Last updated:** 2026-02-16

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
│   │   ├── db.rs                # SQLite FTS5 operations
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
    │   └── owns rusqlite::Connection (Connection is !Send)
    │
    └── writer thread (write: indexBatch, removeBatch, clear)
        └── owns rusqlite::Connection
        └── signals reader via AtomicBool on clear/memoryClear

Shared: EmbeddingEngine (Arc), SynonymLookup (Arc), Stdout (Arc<Mutex>)
```

**Pre-init (single-threaded):** hello, init, updateCheck, updateRequest
**Post-init (multi-threaded):** all other methods dispatched by `classify_method()`
