# TabMail Native FTS - Project Memory

> **Native FTS specific knowledge.** Claude reads this before every task and updates it when discovering something new. For cross-cutting knowledge, see `../PROJECT_MEMORY.md`.

**Last updated:** 2026-02-16

---

## Architecture

Native messaging host for full-text search + semantic search. Communicates with the Thunderbird add-on via native messaging protocol (stdin/stdout JSON).

### Multi-Threading Model

**Phase A (pre-init, single-threaded):**
- `hello`, `init`, `updateCheck`, `updateRequest` — handled on main thread

**Phase B (post-init, multi-threaded):**
- **Main thread**: reads stdin, dispatches to reader or writer channel via `classify_method()`
- **Reader thread**: read-only ops (search, stats, filter) — owns its own `rusqlite::Connection`
- **Writer thread**: write ops (indexBatch, removeBatch, clear) — owns its own `rusqlite::Connection`

### Shared Resources
- `EmbeddingEngine` and `SynonymLookup` shared via `Arc`
- Responses written through `Arc<Mutex<Stdout>>`
- `clear`/`memoryClear` in writer signals reader via `AtomicBool` to reopen its read-only connection
- No new crate dependencies — uses `std::sync::{mpsc, Arc, Mutex, atomic::AtomicBool}`

### SchemaVersion
`SCHEMA_VERSION: u32 = 1` decouples reindex triggers from host version. JS side compares integer schema version instead of major.minor host version string. Storage key: `ftsLastIndexedSchemaVersion`.

---

## Key Files

| What | Where |
|------|-------|
| Main entry | `src/main.rs` |
| Method dispatch | `classify_method()` in main.rs |
| Reader/writer threads | Spawned after `init` in main.rs |

---

## Recent Discoveries

### 2026-02-01
- Refactored from single-threaded to reader/writer thread split (ADR-018)
- Added `SCHEMA_VERSION` to decouple reindex triggers from host version
- Fixed incrementalIndexer.js delete verify catch block (conservative on error)
- Reader connections cache ~64MB each (PRAGMA cache_size), total memory increase ~128MB
- Connection is `!Send` — each thread must own its own connection

---

## Known Quirks

- Reader may briefly fail during `clear` operations (between writer clearing and reader reopening connection)
- Pre-init messages must remain single-threaded (hello/init/updateCheck/updateRequest)
- JS side handles out-of-order responses via `pendingRPCs` Map — no changes needed for multi-threading
