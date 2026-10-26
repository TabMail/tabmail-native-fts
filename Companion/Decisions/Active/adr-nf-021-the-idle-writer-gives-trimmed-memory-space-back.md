# ADR-NF-021: The Idle Writer Gives Trimmed Memory Space Back to the Disk

**Status:** Accepted (2026-10-26, issue #11, P2). Amends ADR-NF-008 ("What the limit bounds", and
the 2026-10-18 revision that removed `VACUUM` from `memoryTrim`).

## Context

A trim (ADR-NF-008) keeps the memory's pages in use within the caller's limit, but with
`auto_vacuum = NONE` freed pages stay in the file: `memory.db` kept its largest size after a lowered
limit. The write-ahead log was not bounded either: the memory used the email database's
`wal_autocheckpoint` of 200,000 pages (about 800 MB) and no `journal_size_limit`, so `memory.db-wal`
could grow to that and keep it after a checkpoint. Reading for this issue also found that the
writer's pragmas (`synchronous`, `busy_timeout`, `cache_size`, `mmap_size`, `wal_autocheckpoint`)
were set only when a memory was created: after a restart the writer ran on SQLite's defaults.

Common practice: Android builds SQLite with auto-vacuum FULL and a
512 KiB journal limit; Firefox runs a full `VACUUM` at idle at most every 30 days per database and
sets `journal_size_limit` on every connection (Places: 4 MiB); Chromium checkpoints at 1000 pages.
Bounded work in SQLite itself is `auto_vacuum = INCREMENTAL` with `PRAGMA incremental_vacuum(N)`.

## Decision

1. **Every writer open sets the writer's pragmas** (`set_memory_writer_pragmas`), on an existing
   memory as on a new one, and the memory has its own WAL bounds: `wal_autocheckpoint` 1000 pages
   (`config::memory::WAL_AUTOCHECKPOINT_PAGES`) and `journal_size_limit` 12 MiB
   (`JOURNAL_SIZE_LIMIT_BYTES`). The email database keeps its own settings.
2. **A new memory is created with `auto_vacuum = INCREMENTAL`** (`open_memory_file`). The mode holds
   only before the file's first table, and the FTS5 probe (`ensure_fts5_available`) creates one, so
   it is set right after the file opens, before the probe. A memory rebuilt by memoryClear is new.
3. **The writer gives freed pages back while no request waits** (`compact_memory_step`, run from
   the writer's idle hook after the shard migration, like ADR-NF-012's conversion). It starts once
   at least 10% of the file is free (`COMPACT_MIN_FREE_PERCENT`; fewer are left to later writes,
   which reuse them) and then goes on until no page is free, across the requests served between
   steps. Each step is `incremental_vacuum(2048)` (`COMPACT_STEP_PAGES`, 8 MB at 4 KB pages), so a
   request waits for at most one step. When no page is free, `wal_checkpoint(TRUNCATE)` cuts the log
   so the files shrink now. An error stops compaction until the next start (logged).
4. **An older memory (`auto_vacuum = NONE`) is converted once**, at the first idle moment with at
   least 10% of it free: `auto_vacuum = INCREMENTAL; VACUUM`, with `temp_store = FILE` during the
   VACUUM so the copy goes to disk, not RAM (restored to `MEMORY` after, also on error). Owner
   2026-10-26: convert existing memories.

`memoryTrim` itself is unchanged: no VACUUM inside a request, so ADR-NF-008's time budget holds.

## Why the conversion keeps every rowid

`memory_ids` (`memId TEXT PRIMARY KEY`, no INTEGER PRIMARY KEY) has the implicit rowid that joins
`memory_fts`, `memory_meta` and `memory_vec`, and sqlite-vec 0.1.9's vector chunks
(`<name>_vector_chunks00(rowid PRIMARY KEY, vectors BLOB)`, a column named rowid that is not an
alias) are read by their real rowid. SQLite documents only that VACUUM "may change" such rowids. In
the bundled SQLite (3.46, libsqlite3-sys 0.30.1), VACUUM copies each table with `xferOptimization`,
which writes a new rowid (`OP_NewRowid`) only for a table with neither an INTEGER PRIMARY KEY nor an
index; every table here without an INTEGER PRIMARY KEY has its PRIMARY KEY's index, so its rowids
are copied. `test_idle_compaction_converts_an_older_memory_once_keeping_every_row_and_rowid` pins
it on the bundled version: every row of every ordinary table with its `_rowid_`, and the search
results by meaning and by word, are identical before and after (and after a reopen), with rowid
gaps from removed rows. A SQLite upgrade that changed this fails that test.

## Consequences

- The files on disk come down to about the pages in use shortly after a trim, while the helper is
  idle (`test_the_idle_helper_gives_a_trimmed_memory_back_to_the_disk`,
  `test_the_idle_helper_converts_an_older_memory_and_gives_its_space_back`).
- The one-time conversion copies what is kept: about 14 s per GB (ADR-NF-008's measure), once per
  older memory. A request that arrives meanwhile waits for it; at the 1 GB default that is within
  the clients' 60 s request timeout. It needs free disk for the copy and its log, about twice what
  is kept; without it the VACUUM fails, compaction stops until the next start, and nothing is lost.
- `incremental_vacuum` moves pages but does not defragment, which is why Firefox rejected it for
  Places. The memory is small rows read by rowid and date index, so fragmentation is not measured
  to matter here.
- Each writer open leaves a few free pages from the FTS5 probe's dropped table; below 10% they are
  reused by later writes.

## Rejected

- **VACUUM inside `memoryTrim`**: removed in ADR-NF-008's revision for its unbounded time; still
  rejected.
- **`auto_vacuum = FULL`**: frees at every commit, inside the trim's delete transactions, unbounded.
- **Rebuilding `memory_ids` with an explicit INTEGER PRIMARY KEY before converting** (documented-safe
  rowids): a schema migration across four tables for a guarantee the bundled SQLite already gives
  and a test pins.
- **Converting only new and cleared memories**: an existing over-limit memory would keep its peak
  size until cleared (owner chose to convert).
