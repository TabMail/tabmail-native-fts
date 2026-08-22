# TabMail Native FTS - Project Memory

> **Native FTS specific knowledge.** Claude reads this before every task and updates it when discovering something new. For cross-cutting knowledge, see `../PROJECT_MEMORY.md`.

**Last updated:** 2026-08-22

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

### Year-Based FTS Sharding (ADR-NF-002)
- FTS5 tables are per-year: `messages_fts_2023`, `messages_fts_2024`, etc. Created lazily via `ensure_shard()`.
- `message_meta.shardYear` column enables O(1) shard routing for single-message lookups.
- `year_from_date_ms()` converts dateMs → year (UTC), clamped to `SHARD_MIN_YEAR = 2000` in `config.rs`.
- Search uses `UNION ALL` across all shards in a single SQL statement (not sequential per-shard queries).
- Writer thread: `known_years: HashSet<i32>` maintained locally, passed to `index_batch`, `optimize`.
- Reader thread: calls `load_known_years(conn)` per-request (reads `sqlite_master`, cheap in WAL mode).
- `messages_vec` (vector embeddings) stays un-sharded — uses global rowid from `message_ids`.
- `find_by_header_message_id` → queries `message_ids` table directly (unsharded).
- Crash-safe auto-migration from monolithic `messages_fts` on first boot (idempotent steps).

---

## Key Files

| What | Where |
|------|-------|
| Main entry | `src/main.rs` |
| Method dispatch | `classify_method()` in main.rs |
| Reader/writer threads | Spawned after `init` in main.rs |
| Folder-key digest RPC | `fingerprint_msg_id_range()` in `src/fts/db.rs` |
| Exact folder relation RPCs | `list_folder_membership()`, `list_folder_membership_state()`, `assign_folder_membership_batch()` in `src/fts/db.rs` |

---

## Recent Discoveries

### 2026-08-22
- Added capability `folderMembershipV1` and an additive `message_folder_membership(msgId, folderId)` relation with covering `(folderId, msgId)` index. Creating the initially empty relation is constant-size startup DDL; existing archives are not scanned. `indexBatch` accepts optional opaque `folderId`; duplicate rows adopt an absent relation, accept the same value idempotently, and reject a different value transactionally. `listFolderMembership` provides exact BINARY pages for incremental client-side digests. `listFolderMembershipState` pages the global `message_ids` keyspace before joining optional membership, bounding inspected archive rows as well as returned rows. `assignFolderMembershipBatch` reports assigned/alreadyAssigned/missing (outside-policy live messages are missing no-ops) while ownership conflicts remain atomic. Reconciliation is stateless and unbounded overall across bounded calls. Existing msgId keys and range RPCs remain unchanged. `SCHEMA_VERSION` stays 1 because the relation migrates in place and no Thunderbird re-feed is needed (ADR-NF-004).

### 2026-08-13
- Native FTS 0.11.2 retires the pre-rotation update-signing public key from the compiled verifier. v0.11.1 remains the one-release dual-key bridge; clients that missed it recover through Thunderbird 1.7.2+'s unsupported-helper reinstall prompt. `scripts/check-update-signing-policy.py` makes the release fail if the compiled trust set differs from the active + explicitly transitional keys in `release/update-signing-policy.json`, or if a key remains trusted at/after its `firstUntrustedVersion`.
- Added reader RPC `fingerprintMsgIdRange { startKey, endKey }` → `{ count, sha256 }` for Thunderbird ADR-022 startup membership proofs. It scans `message_ids` in SQLite BINARY order and hashes `u64be(UTF-8 byte length) || UTF-8 bytes`, matching the add-on's msgDB fingerprint. Host 0.10.1 → **0.11.0**; `SCHEMA_VERSION` remains 1 (no reindex). Existing `sha2`/`hex` dependencies only.

### 2026-07-13
- `tb-native-fts/maintenance.sh --audit` can print a failed `cargo audit` (for example, when the sandbox cannot lock `~/.cargo/advisory-db`) yet still exit 0 because the script does not propagate that subcommand failure. Treat the script status as non-authoritative: inspect the audit output and rerun `cargo audit`/the maintenance audit with the required host access before recording a clean result.

### 2026-07-12
- `cargo fmt --all -- --check` has repo-wide pre-existing rustfmt drift (over 3,700 reported diff lines as of this date). Keep targeted maintenance diffs minimal unless a dedicated formatter-normalization change is requested.

### 2026-07-03
- Added generic msgId key-range reader RPCs `countMsgIdRange { startKey, endKey }` → `{ count }` and `listMsgIdRange { startKey, endKey, afterKey?, limit }` → `{ msgIds, done }` (PK range scans on the unsharded `message_ids` table; half-open `[startKey, endKey)`; NO schema change, NO msgId parsing host-side — the addon computes folder-prefix bounds). Consumed by the TB addon's per-folder set reconcile (tabmail-thunderbird ADR-021 / PLAN_FOLDER_SET_RECONCILE.md), which feature-detects them — older helpers reject with "Unknown reader method" and the addon degrades gracefully. Version 0.9.1 → **0.10.0** (Cargo.toml + HOST_VERSION); `SCHEMA_VERSION` unchanged (no reindex). Default list page size: `config::sqlite::LIST_MSG_ID_RANGE_DEFAULT_LIMIT`.

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
- FTS5 `CREATE VIRTUAL TABLE` is auto-commit — must be called outside explicit transactions
- FTS5 shadow tables (`messages_fts_2024_content`, `_data`, etc.) also appear in `sqlite_master` — `load_known_years` uses `.parse::<i32>()` filter to ignore them
