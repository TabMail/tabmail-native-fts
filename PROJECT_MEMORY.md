# TabMail Native FTS - Project Memory

> **Native FTS specific knowledge.** Claude reads this before every task and updates it when discovering something new. For cross-cutting knowledge, see `../PROJECT_MEMORY.md`.

**Last updated:** 2026-08-22

---

## Architecture

- **2026-10-21 TLS audit:** CVE-2026-111943 affects `rustls-webpki` 0.103.15 and `webpki-roots` 1.0.9 in v0.11.8's ureq path; fixes are 0.103.16 and 1.0.10, validated with 151 tests and pending release. [Audit evidence](Companion/Memory/Current/2026-10-21-emsign-tls-audit.md).

Native messaging host for full-text search + semantic search. Communicates with the Thunderbird add-on via native messaging protocol (stdin/stdout JSON).

### Multi-Threading Model

**Phase A (pre-init, single-threaded):**
- `hello`, `init`, `updateCheck`, `updateRequest` — handled on main thread

**Phase B (post-init, multi-threaded):**
- **Main thread**: reads stdin, dispatches to reader or writer channel via `classify_method()`
- **Reader thread**: read-only ops (search, stats, filter) — its read-only connections are shared with the writer through `Arc<Mutex>`, so a clear can close them (ADR-NF-009)
- **Writer thread**: write ops (indexBatch, removeBatch, clear) — owns its own read-write `rusqlite::Connection`s

### Shared Resources
- `EmbeddingEngine` and `SynonymLookup` shared via `Arc`
- Responses written through `Arc<Mutex<Stdout>>`
- `clear`/`memoryClear` in writer locks the reader's connection (`Mutex`), closes it so Windows can delete the files, recreates, reopens; a failed delete is an error reply, rows kept; a clear that could not reopen leaves an in-memory stand-in that each thread opens again at its next request (`reopen_stand_in`) (ADR-NF-009)
- `memoryTrim` / `memoryIndexBatch{maxBytes}` trim in the writer: FTS5 `merge` steps reclaim removed rows' segment space before more rows go (started once negative after removing, continued positive so a write between calls never restarts it); budget asked per row and per step; no VACUUM in the trim; the idle writer gives freed pages back afterwards (`compact_memory_step`, incremental vacuum, one-time VACUUM conversion of an older memory, ADR-NF-021); caller with `modelDir` skips the orphan clean-up (ADR-NF-008)
- `memorySearch {previewChars}` cuts each result's `content`/`snippet` to N chars in the reply only (`memory_search` wraps `memory_search_rows`), so a Thunderbird reply stays under its 1 MiB native-message cap; rows stay whole (ADR-NF-012). A dated hybrid memory search asks sqlite-vec only for rows within the dates; the listing chooses rows from `memory_meta` then reads their content. sqlite-vec k is at most 4,096 (`SQLITE_VEC_VEC0_K_MAX`); a larger k failed the vector leg silently and the search ran keyword-only (#20, ADR-NF-013), so a search with a query caps its `limit` at `config::hybrid::SEARCH_LIMIT_MAX` = 512 (`cap_search_limit`; memory listing uncapped). No vector candidate within the dates falls back to keyword-only, as an empty vec table does. A merge that keeps nothing while the keyword leg matched (a word in at least half the rows scores ~0 by FTS5 IDF) also falls back to keyword-only (#28, ADR-NF-014). Email search, like memory search, asks sqlite-vec for the nearest only among rows within the dates and a field's matches (`search_vec_candidates` with `rowid IN`, #26, ADR-NF-016) and runs in one read transaction
- No new crate dependencies — uses `std::sync::{mpsc, Arc, Mutex}`

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

### 2026-10-05
- Added reader RPC `folderMembershipSummary` (capability `folderMembershipSummaryV1`) and owner reporting in `removeBatch` (`removedFolderIds`, `removedOwnerless`); see ADR-NF-006. The summary is ONE SQL statement on purpose: separate statements on the reader connection would each take their own WAL snapshot, and a writer commit between them could make a clean index look dirty or a dirty one clean (`test_folder_membership_summary_reads_one_snapshot_under_a_concurrent_writer` catches a split). `json_each(?)` binds the request's id lists, so no TEMP tables are needed.

### 2026-10-04
- Added reader RPC `getAttachmentFlags { msgIds }` → `{ ok, flags }`: each msgId's stored `message_meta.hasAttachments` in request order, `true`/`false`, or `null` when not indexed. Reads only `message_ids` ⋈ `message_meta` (no shard, no body), so the add-on can label an inbox list from the index without one `getMessageByMsgId` round trip per message. At most `GET_ATTACHMENT_FLAGS_MAX_IDS` ids per request; non-string entries are refused. Older helpers reject it in `classify_method`, so the main thread answers "Unknown method: getAttachmentFlags".
- Shards gained an `attachmentNames` FTS5 column (ADR-NF-005). The tokenizer migration became `rebuild_next_stale_shard`, run one shard at a time by the writer thread while no request waits — NEVER in `init`: a single large shard can outlast the add-on's 60 s init RPC timeout, and a timed-out init gets the helper killed mid-copy every start. Staleness now also means "no `attachmentNames` column" (`shard_has_attachment_names`, PRAGMA table_info). Rows copied into a converted shard, and rows from the monolithic migration, hold NULL in that column — read it with `COALESCE`. `bm25()` accepts more weights than a shard has columns, so one SQL works across converted and unconverted shards.

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

- Reads wait while a `clear` runs; a `clear` waits for a read in progress (ADR-NF-009; before it, the reader could briefly fail)
- Pre-init messages must remain single-threaded (hello/init/updateCheck/updateRequest)
- JS side handles out-of-order responses via `pendingRPCs` Map — no changes needed for multi-threading
- FTS5 `CREATE VIRTUAL TABLE` is auto-commit — must be called outside explicit transactions
- FTS5 shadow tables (`messages_fts_2024_content`, `_data`, etc.) also appear in `sqlite_master` — `load_known_years` uses `.parse::<i32>()` filter to ignore them

- 2026-10-16: Candle’s Zip major constraint leaves three new extraction advisories open; Zip 1.x was un-yanked after the initial resolver failure. Targeted Rustls 0.23.46 clears its four advisories. See [Candle/Zip maintenance and security constraint](Companion/Memory/Current/yanked-zip-blocks-broad-dependency-refresh.md). 2026-10-19: resolved by vendoring candle-core 0.8.4 on Zip 9 (ADR-NF-010, `vendor/candle-core`, `[patch.crates-io]`); audit clean but Paste; 0.11.7 still has Zip 1.1.4.
