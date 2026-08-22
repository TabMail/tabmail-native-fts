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

## ADR-NF-003: Relicensed to MPL 2.0 (PolyForm Noncommercial → MPL 2.0)

**Context:** TabMail's clients were source-available under PolyForm Noncommercial 1.0.0, which bars commercial use and is not OSI-approved open source. As the native-messaging FTS host the (now open-source) Thunderbird add-on depends on for desktop search, `tabmail-native-fts` is relicensed in lockstep so the whole desktop stack is genuinely open.

**Decision:** Relicense `tabmail-native-fts` to the **Mozilla Public License 2.0**, in place (no history rewrite). Per-file MPL headers added; root `LICENSE` carries the full MPL 2.0 text. Version bumped `0.8.9 → 0.9.0` to mark the relicense (`Cargo.toml` + `src/config.rs` `HOST_VERSION`), tag `v0.9.0`. Relicensed alongside the Thunderbird add-on (v1.6.0) and the iOS client.

**Rationale:** MPL 2.0 is OSI-approved weak copyleft at file granularity — modifications to MPL files stay open, while integrators may ship proprietary surrounding code; GPL-compatible via the secondary-license clause; well understood by enterprise legal. Makes the desktop search host auditable and forkable while protecting our changes.

**Consequences:**
- The crate, the prebuilt binary, and the update manifest are genuinely open source — anyone can build, audit, or fork.
- The hosted TabMail backend (AI orchestration, prompts, infra) and signing identities stay proprietary — out of scope.
- The "TabMail" name and logo remain trademarks; forks must rebrand.
- Contributions require a DCO sign-off (`git commit -s`).

---

## ADR-NF-004: Exact Opaque Folder Membership Alongside Stable msgId Keys

**Context:** Thunderbird's historical native key joins account, folder path,
and Message-ID with `:`. IMAP folder names may themselves contain `:`, so
prefix ranges and delimiter parsing cannot prove folder membership. Replacing
the primary key would force a broad cache migration, while the iOS search
implementation avoids this class by keeping folder identity as a structured
relation beside message identity.

**Decision:**
1. Preserve `message_ids.msgId` and every existing RPC unchanged. Add an empty
   `message_folder_membership(msgId PRIMARY KEY, folderId NOT NULL)` relation
   with a covering `(folderId, msgId)` index.
2. Advertise `capabilities.folderMembershipV1`. Let `indexBatch` accept optional
   opaque `folderId` for fresh writes.
3. A duplicate msgId may adopt an absent relation or repeat the same relation.
   A different non-empty relation is a conflict and rolls back the request.
4. Provide an exact-equality bounded folder list from which clients compute
   digests incrementally, plus a bounded global `message_ids` state page whose
   limit is applied before joining the optional relation. Bounded transactional
   backfill counts live messages outside the FTS policy window as missing
   no-ops, while ownership conflicts still roll back the whole request. Pages
   can continue without a total-row cap or server-side session cache.
5. Keep `SCHEMA_VERSION` at 1: the additive relation migrates locally and does
   not require Thunderbird to re-feed message content.

**Rationale:** Exact equality makes colon-bearing and prefix-related folders
independent without changing externally visible message keys. A separate,
initially empty relation avoids a synchronous index build over every historical
msgId at startup. Missing legacy membership is explicit rather than guessed,
and bounded requests keep migration interruptible while eventually covering
archives of any size.

**Consequences:**
- A capable Thunderbird client must complete a stable live membership scan
  before treating relation-absent state entries as stale; this helper only
  exposes and assigns the relation.
- A conflicting non-empty relation fails closed because it may represent a
  historical composite-key collision.
- Older clients continue to index and use the original range RPCs; their new
  rows remain unassigned until a capable client adopts them.
- The source version remains unchanged in this implementation PR. The release
  train must bump `Cargo.toml` and `HOST_VERSION` together before publishing a
  helper binary carrying the new capability.

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
