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

## ADR-NF-011: Search Query Text Reaches FTS5 as Text

- **[Full ADR](Companion/Decisions/Active/adr-nf-011-search-query-text-reaches-fts5-as-text.md)** — `build_fts_match(q, columns, …)` scans words/`"phrases"` on `&str` (non-ASCII whole; old byte-to-char scanners turned `café` into `cafÃ©`) and writes every one as an FTS5 string: punctuation (`% ! / # $ , & ( )`), `NEAR` and unknown `x:` are text, never syntax errors / `no such column`; text-less items dropped; capital `AND/OR/NOT` between two terms stay operators (`from:X OR to:X`, as before), first/last/doubled dropped, the word after a kept `NOT` searched without synonyms, at most `SEARCH_MAX_KEPT_NOTS` (100) `NOT`s kept, later exclusions left out (FTS5 depth 256, #22); quote marks and one possessive `'s` removed (`John's`, `Sarah's.`; synonyms looked up with the `s`, `comment's` → `comments`), a word whose only punctuation is an inner apostrophe searches both spellings `("don't" OR "dont"*)` (#31; no star on a one-letter last part, `t*` is slow; a spelling group keeps neighbours' prefix stars; field values one spelling); generated synonym `(a OR b)` groups joined with explicit `AND` (FTS5 rejects implicit AND beside parentheses). Column scoping only from the caller's list: mail `QUERY_COLUMNS` (subject from to cc bcc body + from_ to_, case-insensitive, all in `COLUMN_SCOPE_PREFIXES`), memory none.

## ADR-NF-012: Memory Search Can Return Previews

- **[Full ADR](Companion/Decisions/Active/adr-nf-012-memory-search-previews.md)** — `memorySearch` `previewChars` cuts `content`/`snippet` in the reply only (Thunderbird refuses a native message over 1 MiB); stored rows and `memoryRead` whole; no capability flag. Amended 2026-10-21: dated hybrid search filters vector candidates by date before the merge; listing and `memoryRead` pick rowids from `memory_meta` first (`memoryRead`: #151, 2026-10-21); a memory search runs in one read transaction (rowid reuse). Open: limit x 4 > 4,096 fails the vector search silently (#20); fixed by ADR-NF-013. Date scan of item 3 removed by ADR-NF-015's index.

## ADR-NF-013: A Hybrid Search Asks sqlite-vec for at Most Its Maximum, and Email Search Keeps Its Dated Matches

- **[Full ADR](Companion/Decisions/Active/adr-nf-013-hybrid-search-asks-sqlite-vec-for-at-most-its-maximum.md)** — #20: sqlite-vec k max 4,096 (`SQLITE_VEC_VEC0_K_MAX`) made searches with limit > 1,024 silently keyword-only; `cap_search_limit` caps a search with a query at `SEARCH_LIMIT_MAX` 512 (`FIELD_VEC_MULTIPLIER`); vector failure warns; email `vec_candidates_within_dates` before the merge + one read transaction; no vector within the dates falls back to keyword-only. Known limitations (owner-approved): nearest k over the whole archive, #26; a word in most rows can return nothing (FTS5 IDF clamp + `MIN_SCORE`), #28. (#28 fixed by ADR-NF-014; #26 fixed by ADR-NF-016) Caller-side date sort replaced by `search` `sort`, ADR-NF-017.

## ADR-NF-014: A Hybrid Search Whose Merge Keeps Nothing Answers by Keyword

- **[Full ADR](Companion/Decisions/Active/adr-nf-014-an-empty-merge-answers-by-keyword.md)** — #28: a word in at least half the rows (FTS5 IDF clamp to ~0) + weak vectors put every fused score under `MIN_SCORE` 0.1, so hybrid search returned nothing; now an empty merge with keyword matches falls back to `search_fts_only` / `memory_search_fts_only`. Scoring unchanged: study on real TB/iOS email + memory DBs, current `merge_results` best or tied in 4 of 5 DBs (unthresholded RRF higher only on a 25-turn Voice DB, by filling the page with non-matches) vs weighted RRF, min-max normalisation, query-length gating, keyword-only (known-item MRR@10). iOS in step, tabmail-ios#240. Email fallback order now follows `sort` (relevance: best keyword first), ADR-NF-017. **Fallback deleted by ADR-NF-020** (every keyword match is a result).

## ADR-NF-015: Memory Dates Are Indexed

- **[Full ADR](Companion/Decisions/Active/adr-nf-015-memory-dates-are-indexed.md)** — `memory_meta_dateMs` index on `memory_meta(dateMs)` (`MEMORY_META_DATE_INDEX`), created with the schema and on the first open of an older memory (`CREATE INDEX IF NOT EXISTS`, about 0.1 s per 300k dates); `memoryRead`, the listing and the oldest-first removal use it, so a read by date no longer scans every entry's date (one-entry cold read 8 pages at 20k entries, about 170 without). Ties in a millisecond ordered by rowid (written order) in the listing and `memoryRead`; an existing memory is decided by `memory_meta` (a half-created one is completed). Keyword-search plans unchanged. Owner 2026-10-22; follows ADR-NF-012 item 3. **Amended 2026-10-25 (#41):** a memory init cannot open (corrupt schema/`memory_meta`/`memory_fts` page, failed index build, full disk) no longer fails init: it is left closed behind a stand-in, reopened per request, `memoryDocs` null; email search unaffected; Clear chat history (memoryClear) rebuilds it.

## ADR-NF-016: sqlite-vec Searches Only Within a Search's Dates and Field

- **[Full ADR](Companion/Decisions/Active/adr-nf-016-sqlite-vec-searches-only-within-the-dates-and-the-field.md)** — #26: dated / field-scoped hybrid search asked sqlite-vec for the nearest k over the whole archive then filtered, missing meaning-only matches (real TB DB: vector recall 0.002-0.16 with dates, keyword-only fallback for most narrow windows); now KNN with `rowid IN (SELECT rowid FROM message_meta/memory_meta WHERE dateMs ... AND json_each(field rowids))`, exact and faster, no schema change. Deleted `vec_candidates_within_dates`, `FIELD_VEC_MULTIPLIER`; `SEARCH_LIMIT_MAX` stays 512 (const assert vs `VEC_K_MAX`). Rejected vec0 `dateMs` metadata column (migration), brute-force `vec_distance_cosine`. iOS port with tabmail-ios#242.

## ADR-NF-017: A Search Sorts Before It Takes Its Limit

- **[Full ADR](Companion/Decisions/Active/adr-nf-017-a-search-sorts-before-it-takes-its-limit.md)** — #46: callers sorted the best `limit` by date, so date sorts missed the newest/oldest match; now `search` `sort` = `relevance` (default) | `date_desc` | `date_asc` (else error, `SearchSort`), sort THEN limit in `merge_results` (relevance ties newer first, date ties better match first, then rowid; vector-only dates via `fetch_dates` json_each). Date sort pool `DATE_SORT_POOL` 4,096 per leg (study on a 300k-message TB archive: 1,024/2,048/4,096, 4,096 right in every dated set, same median cost). Field-only query (`query_is_field_only`: from:/to:/cc:/bcc:/subject:/body: + operators) skips the vector leg, keyword over ALL matches, relevance = newest first. Keyword-only fallbacks order by the sort (email relevance now rank first, amends ADR-NF-014). Memory merge ties newer first, no sort. iOS tabmail-ios#265.

## ADR-NF-018: Capabilities for the Behaviour 0.11.8 Lacks

- **[Full ADR](Companion/Decisions/Active/adr-nf-018-capabilities-for-the-behaviour-0-11-8-lacks.md)** — `hello` capabilities `memoryPreviewV1` (previewChars), `memoryFilterBeforeLimitV1` (dated memory search filters before the limit), `searchSortV1` (`search` `sort`, TB #164): a 0.11.8 helper reports the same version as main but returned 0 of 6 dated matches and a 1.25 MB listing over the 1 MiB cap (TB release review 2026-10-24); callers refuse on the receiving connection's capabilities. Amends ADR-NF-012 item 4. Voice unaffected (pinned build).

## ADR-NF-019: A Search That Excludes Answers by Keyword; an OR Filters Each Alternative

- **[Full ADR](Companion/Decisions/Active/adr-nf-019-a-search-that-excludes-answers-by-keyword.md)** — #21: hybrid `X NOT from:Y` returned Y's mail (`extract_column_scope_filter` dropped the NOT, the vector leg searched only Y's mail), and a NOT before a word was ignored by the vector leg (email and memory). Now a query keeping a NOT (`query_excludes`, a bare `NOT` token in `split_column_scope`'s scan) answers by keyword only: `search_fts_only` / `memory_search_fts_only`. Dropped/quoted/lower-case NOT unchanged. The vector field filter now splits at each OR outside a group (each alternative = AND of its fields, joined by OR; an alternative without a field = no filter), so `from:a OR from:b x` searches both senders by meaning, not mail from both. Rejected: NOT in the field filter (FTS5 NOT is binary), post-filtering vector candidates (fewer than k). iOS tabmail-ios#301.

## ADR-NF-020: Every Keyword Match Is a Hybrid Result

- **[Full ADR](Companion/Decisions/Active/adr-nf-020-every-keyword-match-is-a-hybrid-result.md)** — #51: a word in most rows (FTS5 IDF clamp, ~0 keyword score) kept only the rows the vector leg lifted over `MIN_SCORE` (smoke test: `meeting` 1 of 8, email + memory). Now every keyword candidate is a result (`HybridCandidate.keyword_match`); `MIN_SCORE` gates vector-only candidates; ranking unchanged. ADR-NF-014's empty-merge fallback DELETED. Keyword pools tie-break `rank ASC, dateMs DESC`. Owner invariant: hybrid never returns fewer than keyword-only. Rejected parallel hybrid+keyword larger-set, max-relative threshold. iOS paired issue.

## ADR-NF-021: The Idle Writer Gives Trimmed Memory Space Back to the Disk

- **[Full ADR](Companion/Decisions/Active/adr-nf-021-the-idle-writer-gives-trimmed-memory-space-back.md)** — #11: `memory.db` kept its largest size after a trim (`auto_vacuum` NONE) and `memory.db-wal` could reach ~800 MB (email's 200,000-page `wal_autocheckpoint`, no `journal_size_limit`); writer pragmas were set only on creation. Now every writer open sets them (`set_memory_writer_pragmas`; memory WAL 1000 pages + 12 MiB `journal_size_limit`), a new memory is `auto_vacuum = INCREMENTAL` (set before the FTS5 probe's table, `open_memory_file`), and the idle writer (`compact_memory_step`, `next_write_request` hook) starts at 10% free, frees 2048 pages per step until none, then `wal_checkpoint(TRUNCATE)`. An older NONE memory converts once by idle `VACUUM` (`temp_store = FILE`); rowids kept (bundled 3.46 `xferOptimization`, pinned by test); over ~3 GB kept one request can pass the 60 s timeout during it (owner-accepted 2026-10-26). The memory reader no longer mmaps (Windows `winTruncate` swallows `ERROR_USER_MAPPED_FILE`). No VACUUM in `memoryTrim`. Amends ADR-NF-008.

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
