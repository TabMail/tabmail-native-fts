# ADR-NF-015: Memory Dates Are Indexed

**Status:** Accepted (2026-10-22, owner: "add an index as immediate followup pr"). Follows ADR-NF-012
item 3, which left each `memoryRead` scanning every entry's date.

## Context

`memory_meta` (`rowid`, `dateMs`, `sessionId`, `turnIndex`) had no index on `dateMs`. Since ADR-NF-012
items 2 and 3, the memory listing (`memorySearch` with no query) and `memoryRead`
(`memory_read_by_timestamp`) pick their rows from `memory_meta` by date before reading any content.
That removed the content scan, but each request still scanned every entry's date. Thunderbird's
`memory_read` (#151) and Chat History issue one `memoryRead` per listed millisecond, so the scan was
paid many times per call, on the reader thread email search also uses. Measured with a cold reader:
a one-entry read in a memory of 20,000 short entries read about 170 pages from disk, against 6 in a
memory of 200.

## Decision

`memory_meta` gets an index on `dateMs` (`memory_meta_dateMs`, `MEMORY_META_DATE_INDEX` in
`memory_db.rs`). `init_memory_database` creates it with the schema, which also covers memoryClear's
fresh database and the writer's reopen after a failed clear. `open_or_create_memory_db` creates it on
the first open of a memory written before it (`CREATE INDEX IF NOT EXISTS`). That open now decides
"existing memory" by `memory_meta`, the table the index is on, not by `memory_fts`: a memory whose
creation stopped between the two (a full disk, a crash) goes through `init_memory_database` again,
whose `IF NOT EXISTS` statements complete it, instead of failing the index, and so every start. Reader connections use it once it exists, because SQLite re-prepares a statement after a schema change.

- **Queries that use it** (checked with `EXPLAIN QUERY PLAN` on the bundled SQLite 3.46.0):
  - `memoryRead`'s date subquery: a covering-index range search.
  - The listing, with and without dates.
  - The oldest-first removal (`ORDER BY dateMs ASC, rowid ASC`; the index carries the rowid).
  - The hybrid search's vector date filter (ADR-NF-016, #26): its eligible rowids come from a
    `memory_meta` date range, now a covering-index range search.
- **Queries whose plan is unchanged:** the keyword searches with dates still start from the
  `memory_fts` MATCH and look up `memory_meta` by rowid. `memoryDebugSample` still scans
  `memory_fts` (#32).

Results are unchanged, including the order of entries sharing a millisecond: the listing and
`memoryRead` now order them by `rowid` (the order they were written), as the oldest-first removal
already did. Without that tiebreak the listing returned them in reverse, because the index is read
backwards for `dateMs DESC`; SQLite leaves the order of tied rows to the plan. The protocol is
unchanged.

**Compatibility:** an older helper (or Voice's bundled one) opening a memory with the index keeps it
up to date, and its queries may use it, with no tiebreak. After a downgrade to 0.11.8 (Voice's pin
and Thunderbird's latest release), only its listing with both `from` and `to` returns entries
sharing a millisecond in reverse write order; its other listings and its `memoryRead` keep their
order. Unreleased builds from #34 on reverse ties in every listing shape. Its
memoryClear recreates the memory without the index, and the next newer helper's open builds it. No schema version is involved (`SCHEMA_VERSION` is the email
index's).

## Consequences

- **A read by date costs about the same whatever the memory's size.** A one-entry `memoryRead` or a
  three-entry dated listing page reads 8 pages from disk at 20,000 entries and 6 at 200 (about 170
  at 20,000 without the index). Tests:
  - `a_read_by_date_costs_the_same_whatever_the_memorys_size` (on memories memoryClear rebuilt)
  - `a_memory_written_before_the_date_index_gets_it_on_open`
  - `entries_sharing_a_millisecond_keep_the_order_they_were_written_in` (listing and `memoryRead`;
    listing pages cut inside a group of tied entries, and a read's 50-entry cap inside one)
  - `a_memory_whose_creation_stopped_after_its_first_table_opens_and_works`
  - `a_new_memory_whose_date_index_cannot_be_built_fails_init_and_keeps_its_entries` and
    `an_older_memory_whose_date_index_cannot_be_built_fails_open_and_the_next_open_builds_it`
    (a failed build fails init or the open visibly, keeps every entry, and the next one builds it;
    *the older-memory test now opens without the index, see the 2026-10-25 amendment below*)
- **A build that fails stops init**, as any init statement does: the helper exits, and the next
  start tries again. If a `memory_meta` page is already corrupt at the first open of an older
  memory, every start fails until the memory is removed (Thunderbird's Clear chat history runs
  through the helper, so it cannot help; Voice's clear deletes the folder). No TabMail code writes
  such a page, and a corrupt `memory_fts` page already stops init. The owner chose to ship this
  as is on 2026-10-22; tracked in #41.
  *Amended 2026-10-25 (#41, raised to P2 by the owner): on an existing memory, a build that fails
  no longer fails the open. It logs a warning, the memory works without the index (reads by date
  scan, as before it), and a later open builds it. The two init-time counts of `memory_fts`
  (`open_or_create_memory_db`'s log line, `handle_init`'s `memoryDocs`) no longer fail init either:
  a failed count is a warning and `memoryDocs` is null (only a Voice test stub reads it). So no
  corrupt page in `memory_meta` or the `memory_fts` tables stops init: email search keeps working, a
  read of the corrupt page answers its error, and memoryClear (Clear chat history) recovers the
  memory, date index included. A NEW memory whose index cannot be built (a full disk at its creation)
  still fails init, and the next start tries again. Tests:
  `an_older_memory_whose_date_index_cannot_be_built_opens_and_the_next_open_builds_it` (was
  `..._fails_open_...`), `an_older_memory_with_a_corrupt_page_opens_and_memory_clear_recovers_it`,
  and the process test `test_a_corrupt_memory_page_never_stops_init_and_clear_recovers_it` (a corrupt
  page in `memory_meta`, `memory_fts_content`, `_data`, `_docsize`, `_idx`; each of the three
  changes is needed: reverting one fails its tables).*
- **Build cost on an existing memory:** once, on the first open. It reads the dates alone: about
  0.1 s for 300,000 dates on an Apple Silicon Mac.
- **Ongoing cost:** each memory write and removal also updates the index. The index takes about 15
  bytes per entry on disk, which counts toward the client's `maxBytes` (ADR-NF-008) like the rest
  of the memory.

## Alternatives considered

- **Keep the date scan (ADR-NF-012 item 3 as merged):** linear in the number of entries, but dates
  only. The owner chose the index.
