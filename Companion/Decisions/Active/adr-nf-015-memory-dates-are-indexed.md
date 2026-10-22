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
a one-entry read in a memory of 20,000 short entries read 171 pages from disk, against 6 in a memory
of 200.

## Decision

`memory_meta` gets an index on `dateMs` (`memory_meta_dateMs`, `MEMORY_META_DATE_INDEX` in
`memory_db.rs`). `init_memory_database` creates it with the schema, which also covers memoryClear's
fresh database. `open_or_create_memory_db` creates it on the first open of a memory written before it
(`CREATE INDEX IF NOT EXISTS`). Reader connections use it once it exists, because SQLite re-prepares a statement after a schema change.

- **Queries that use it** (checked with `EXPLAIN QUERY PLAN` on the bundled SQLite 3.46.0):
  - `memoryRead`'s date subquery: a covering-index range search.
  - The listing, with and without dates.
  - The oldest-first removal (`ORDER BY dateMs ASC, rowid ASC`; the index carries the rowid).
- **Queries whose plan is unchanged:** the keyword searches with dates still start from the
  `memory_fts` MATCH and look up `memory_meta` by rowid, and the vector date filter looks up by
  rowid. `memoryDebugSample` still scans `memory_fts` (#32).

Results are unchanged. The protocol is unchanged.

**Compatibility:** an older helper (or Voice's bundled one) opening a memory with the index keeps it
up to date and ignores it otherwise. No schema version is involved (`SCHEMA_VERSION` is the email
index's).

## Consequences

- **A read by date costs about the same whatever the memory's size.** A one-entry `memoryRead` or a
  three-entry dated listing page reads 8 pages from disk at 20,000 entries and 6 at 200 (171 at
  20,000 without the index). Tests:
  - `a_read_by_date_costs_the_same_whatever_the_memorys_size`
  - `a_memory_written_before_the_date_index_gets_it_on_open`
- **Build cost on an existing memory:** once, on the first open. It reads the dates alone: about
  0.1 s for 300,000 dates on an Apple Silicon Mac.
- **Ongoing cost:** each memory write and removal also updates the index. The index takes a few bytes
  per entry on disk.

## Alternatives considered

- **Keep the date scan (ADR-NF-012 item 3 as merged):** linear in the number of entries, but dates
  only. The owner chose the index.
