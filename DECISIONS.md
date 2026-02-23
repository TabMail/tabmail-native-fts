# TabMail Native FTS - Architectural Decisions

> **Check this file before proposing alternatives.** For cross-cutting decisions, see `../DECISIONS.md`.

---

## ADR-018: SchemaVersion + Reader/Writer Thread Split for Native FTS

**Context:** The native FTS helper processed all operations single-threaded. Heavy write operations blocked read operations for seconds to minutes. Additionally, the JS side compared host version strings to decide reindex triggers, meaning any version bump would trigger a full ~45 minute reindex.

**Decision:**
1. Add integer `SCHEMA_VERSION` constant (starting at 1) that bumps only when DB schema, FTS5 tokenizer config, or embedding model actually changes.
2. After `init`, split processing into three threads: main (stdin reader + dispatcher), reader (read-only DB ops), writer (write/destructive DB ops).
3. `EmbeddingEngine` and `SynonymLookup` shared via `Arc`. Each thread opens its own `rusqlite::Connection` (Connection is `!Send`).
4. Responses written through `Arc<Mutex<Stdout>>`. Native messaging protocol correlates by `id` field, not by order.
5. `clear`/`memoryClear` in writer signals reader via `AtomicBool` to reopen its connection.
6. No new crate dependencies.

**Rationale:**
- SchemaVersion prevents unnecessary full reindex on non-schema host updates
- Reader/writer split eliminates the primary UX problem: search blocked during indexing
- SQLite WAL mode already supports concurrent readers + single writer
- Compiler-enforced thread safety via Rust ownership + Send/Sync traits

**Consequences:**
- Reader connections cache ~64MB each, total memory increase ~128MB
- Clear operations have a brief window where reader may fail
- Pre-init messages remain single-threaded
- JS side needs no changes (already handles out-of-order responses)

---

## ADR-NF-002: Year-Based FTS Sharding

**Context:** The monolithic `messages_fts` FTS5 table grew large over time, slowing down indexing operations. The iOS app already migrated to year-based sharding (`messages_fts_YYYY` tables) with measurable performance improvement.

**Decision:**
1. Split the monolithic `messages_fts` into year-based shard tables (`messages_fts_2023`, `messages_fts_2024`, etc.), one per calendar year.
2. Add `shardYear` column to `message_meta` for O(1) shard routing on single-message lookups.
3. `SCHEMA_VERSION` stays at 1 — crash-safe auto-migration detects the monolithic table's presence and migrates in-place; each step is idempotent.
4. Writer thread maintains `known_years: HashSet<i32>` locally; reader thread discovers shards via `sqlite_master` per-request (cheap, WAL visibility).
5. Search functions use `UNION ALL` across all shards in a single SQL statement (no sequential per-shard loops).
6. `messages_vec` (vector embeddings) stays un-sharded — same global rowid system via `message_ids.rowid`.
7. `find_by_header_message_id` queries `message_ids` table directly (unsharded, no FTS involvement).
8. Years < 2000 clamped to `SHARD_MIN_YEAR = 2000` (matches iOS behavior).

**Rationale:**
- Smaller per-year FTS5 indexes → faster indexing (less b-tree maintenance per write)
- `UNION ALL` keeps search as a single SQL statement — SQLite evaluates all branches in one pass
- No `SCHEMA_VERSION` bump = no forced full reindex on existing installs
- Crash-safe migration uses invariant: monolithic table existing = migration incomplete

**Consequences:**
- Each shard has its own FTS5 internal structures (content, data, idx, docsize, config shadow tables)
- Slightly more disk space overhead from per-shard metadata
- New shard tables created lazily during indexing (not upfront)
- Migration on very large databases may take several seconds on first boot after upgrade

---

## Template for New Decisions

```markdown
## ADR-XXX: [Title]

**Context:** [What situation led to this decision?]

**Decision:** [What did we decide?]

**Rationale:** [Why?]

**Consequences:**
- [Trade-offs, both positive and negative]
```
