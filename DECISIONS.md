# TabMail Native FTS - Architectural Decisions

> **Check this file before proposing alternatives.** For cross-cutting decisions, see `../DECISIONS.md`.

---

## ADR-018: SchemaVersion + Reader/Writer Thread Split for Native FTS

**Context:** The native FTS helper processed all operations single-threaded. Heavy write operations blocked read operations for seconds to minutes. Additionally, the JS side compared host version strings to decide reindex triggers, meaning any version bump would trigger a full ~45 minute reindex.

**Decision:**
1. Add integer `SCHEMA_VERSION` constant (starting at 1) that bumps only when DB schema, FTS5 tokenizer config, or embedding model actually changes.
2. After `init`, split processing into three threads: main (stdin reader + dispatcher), reader (read-only DB ops), writer (write/destructive DB ops).
3. `EmbeddingEngine` and `SynonymLookup` shared via `Arc`. Each thread opens its own `rusqlite::Connection` (Connection is `!Send`). *(Amended by ADR-NF-009: the reader's connections are shared with the writer through `Arc<Mutex>`, which `Connection` being `Send` allows.)*
4. Responses written through `Arc<Mutex<Stdout>>`. Native messaging protocol correlates by `id` field, not by order.
5. `clear`/`memoryClear` in writer signals reader via `AtomicBool` to reopen its connection. *(Amended by ADR-NF-009: the writer locks the reader's connection, closes it before deleting the files, and reopens it; the flags are gone.)*
6. No new crate dependencies.

**Rationale:**
- SchemaVersion prevents unnecessary full reindex on non-schema host updates
- Reader/writer split eliminates the primary UX problem: search blocked during indexing
- SQLite WAL mode already supports concurrent readers + single writer
- Compiler-enforced thread safety via Rust ownership + Send/Sync traits

**Consequences:**
- Reader connections cache ~64MB each, total memory increase ~128MB
- Clear operations have a brief window where reader may fail *(ADR-NF-009: reads now wait for a clear instead)*
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

Amended by ADR-NF-006: `folderMembershipSummary` is one unbounded O(rows) read
per call, an exception to the bounded-call property above.

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

## ADR-NF-005: Attachment File Names Get Their Own FTS Column, Migrated In Place

- **[Full ADR](Companion/Decisions/Active/adr-nf-005-attachment-names-column.md)** — eighth shard column `attachmentNames` appended last (positional bm25/snippet unchanged, weight 3.0); optional `indexBatch` field, no capability; stale = `tokenchars` OR missing column → `rebuild_next_stale_shard` in-place rowid-preserving copy on the IDLE WRITER THREAD, never in `init` (one big shard outlasted the 60 s init RPC → kill/rollback loop), newest first, once per process; SCHEMA_VERSION stays 1; unconverted shard drops names on write; `getMessageByMsgId` returns them; no backfill (smart reindex skips indexed rows).

---

## ADR-NF-006: Folder Membership Summary and Removal Owners

- **[Full ADR](Companion/Decisions/Active/adr-nf-006-folder-membership-summary-and-removal-owners.md)** — reader RPC `folderMembershipSummary { folderIds, trustedAccountIds }` → `{ ownerlessRows, strayTrustedRows, strayUntrustedRows }` in ONE statement (one WAL snapshot), capability `folderMembershipSummaryV1`, stray split by msgId account prefix (first `:` at index > 0, else untrusted), fixed-size reply, one O(rows) read on the reader thread (amends ADR-NF-004 bounded-calls for this RPC); `removeBatch` adds `removedFolderIds` (`DELETE … RETURNING folderId`) + `removedOwnerless`, wire-compatible.

---

## ADR-NF-007: Move or Remove Indexes the Profile Guess Left Where No Profile Reads Them

- **[Full ADR](Companion/Decisions/Active/adr-nf-007-move-or-remove-indexes-left-by-the-profile-guess.md)** — `init` with `profilePath` runs `adopt_or_remove_orphaned_indexes` over `guessed_profile_dirs` (`~/.tabmail` fallback + non-hidden profiles-directory children, the set `find_thunderbird_profile_dir` could return). Orphan = `<dir>/browser-extension-data/<addon id>/tabmail_fts` not this profile's own (canonicalized), no `<dir>/extensions/<addon id>.xpi`, no symlink on the way. MOVE FIRST (owner 2026-10-05 "move and fix"): if this profile has no `tabmail_fts`, rename the newest orphan (by `fts.db` mtime) in — index + `memory.db` chat memory carry over, add-on reconciliation repairs; then remove the rest; failed move (other volume) = remove, no copy (init RPC timeout); empty parents only; best effort, never fails init. Orphaned/stray/leftover old index after the multi-profile wrong-guess fix (Thunderbird ADR-025).

## ADR-NF-008: The Caller Names the Log and Model Folders, and Sets a Memory Size Limit

- **[Full ADR](Companion/Decisions/Active/adr-nf-008-caller-named-locations-and-memory-size-limit.md)** — client-agnostic (no client modes): `--log-dir <abs>` start-up arg (logging starts before any message; default `~/.tabmail/logs`; `--log-dir=` form refused; `--log-level off|error|warn|info|debug`, default debug, `off` writes no log), `init` `modelDir` (default `~/.tabmail/models/all-MiniLM-L6-v2`; invalid = error response, stays pre-init), so a bundled copy (TabMail Voice) never shares the model `.tmp` download race or log rotation with Thunderbird's installed helper. Memory size limit (capability `memorySizeLimitV1`): optional `memoryIndexBatch` `maxBytes`, new writer `memoryTrim {maxBytes}` (no VACUUM: unbounded, removed in review), `memoryStats.usedBytes`; used size = (page_count - freelist_count) * page_size; oldest by dateMs then rowid removed from fts/meta/vec/ids together, each pass sized by estimated bytes (content length + `TRIM_ROW_OVERHEAD_BYTES`, scaled to used; row-count sizing over-removed when the oldest rows are largest); FTS5 `merge` steps reclaim removed rows' segment space before more rows go, started once negative after removing and continued positive so a write between calls never restarts it (secure-delete rejected: pages stay allocated); 20 s budget asked per row and per merge step, `withinLimit:false` continues next call; sqlite-vec 1024-row chunk overshoot recorded as accepted, owner decision pending. A caller naming `modelDir` never runs the ADR-NF-007 orphan clean-up.

## ADR-NF-009: Clear Closes the Reader's Connection Before Deleting the Files, and Fails If It Cannot Delete

- **[Full ADR](Companion/Decisions/Active/adr-nf-009-clear-closes-the-reader-before-deleting.md)** — Windows Clear All bug: the reader's open handle blocked deleting `fts.db` / `memory.db` (no `FILE_SHARE_DELETE`), the failure was a warning, every row stayed and `clear` / `memoryClear` answered ok. Now the reader's connections sit behind a `Mutex`; a clear locks one, closes the reader's handle, deletes `-wal`, `-shm`, then the database, recreates, reopens the reader, unlocks; reads wait. A failed delete is an error reply with both connections reopened and every row kept; a clear that cannot reopen leaves a stand-in each thread opens again at its next request (an `indexBatch` that still finds it answers an error, so no shard is cached that the database lacks; a stand-in is an empty name, never a missing one, which a non-UTF-8 path gives). `AtomicBool` reopen flags deleted. Rejected: in-place empty (reset/VACUUM or backup; changed every read), warning-to-error alone.

## ADR-NF-010: Candle-core 0.8.4 Is Vendored and Moved to Zip 9

- **[Full ADR](Companion/Decisions/Active/adr-nf-010-vendored-candle-core-on-zip-9.md)** — `cargo audit` RUSTSEC-2026-0367/0368/0369 (Zip < 9 archive extraction: permission bits, symlinks) reached the helper only through candle-core 0.8.4's `zip ^1.1.1` (unreachable: no `.npz`/`.pth` loading, safetensors only). No candle release uses Zip 9 (0.11 = Zip 8, and a new engine), so `vendor/candle-core` (crates.io 0.8.4, LICENSE kept, `TABMAIL-PATCHES.md`, per-file notices) is used via `[patch.crates-io]` with zip 9.0.5, entry `name()` `Result` passed on (upstream huggingface/candle#4041), elided lifetimes written out; its own Cargo.lock left out (dependency scanners). Remove once a candle release with Zip >= 9 is adopted. Builders on their own computer: `RUSTFLAGS=--remap-path-prefix=$HOME=~` (home folder / user name in binary paths; CI runners need none; `trim-paths` unstable). Paste unmaintained warning (gemm, compile time) remains.


## ADR-NF-012: Memory Search Can Return Previews

- **[Full ADR](Companion/Decisions/Active/adr-nf-012-memory-search-previews.md)** — `memorySearch` `previewChars` cuts `content`/`snippet` in the reply only (Thunderbird refuses a native message over 1 MiB); stored rows and `memoryRead` whole; no capability flag. Amended 2026-10-21: dated hybrid search filters vector candidates by date before the merge; listing and `memoryRead` pick rowids from `memory_meta` first (`memoryRead`: #151, 2026-10-21); a memory search runs in one read transaction (rowid reuse). Open: limit x 4 > 4,096 fails the vector search silently (#20); fixed by ADR-NF-013.

## ADR-NF-013: A Hybrid Search Asks sqlite-vec for at Most Its Maximum, and Email Search Keeps Its Dated Matches

- **[Full ADR](Companion/Decisions/Active/adr-nf-013-hybrid-search-asks-sqlite-vec-for-at-most-its-maximum.md)** — #20: sqlite-vec k max 4,096 (`SQLITE_VEC_VEC0_K_MAX`) made searches with limit > 1,024 silently keyword-only; `cap_search_limit` caps a search with a query at `SEARCH_LIMIT_MAX` 512 (`FIELD_VEC_MULTIPLIER`); vector failure warns; email `vec_candidates_within_dates` before the merge + one read transaction; no vector within the dates falls back to keyword-only. Known limitations (owner-approved): nearest k over the whole archive, #26; a word in most rows can return nothing (FTS5 IDF clamp + `MIN_SCORE`), #28.

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
