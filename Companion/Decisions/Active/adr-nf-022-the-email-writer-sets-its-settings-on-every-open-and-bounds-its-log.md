# ADR-NF-022: The Email Writer Sets Its Settings on Every Open and Bounds Its Log

**Status:** Accepted (2026-10-26, issue #55, P2). The email database's twin of ADR-NF-021.

## Context

`init_database` set the email writer's pragmas (`synchronous = NORMAL`, `busy_timeout`,
`cache_size`, `mmap_size`, `wal_autocheckpoint`), but it ran only when a database was created and
on the writer's reopen after a clear. `open_or_create_db` on an existing sharded database, and on an
older unsharded one it migrates, did not call it. Only `journal_mode` is kept in the file, so after
a restart the writer ran on the defaults of a plain open: `synchronous = FULL`, the default cache
and mmap, and rusqlite's 5 s busy timeout (`Connection::open` sets it) instead of the configured 2 s.

In the session that created the database, which is the one that does a mailbox's first full index,
the log checkpointed only every 200,000 pages (`config::sqlite::PRAGMA_WAL_AUTOCHECKPOINT_PAGES`,
from the first release, no recorded reason), with no `journal_size_limit`. Measured: a full index
of 30,000 synthetic messages (240 MB of text, Thunderbird's 250-row batches) left `fts.db-wal` at
880 MB, kept until the helper exited.

## Decision

1. **Every writer open sets the writer's settings** (`set_email_writer_pragmas`, split from
   `init_database`): `init_database` calls it (a new database, the writer's reopen), and so do the
   existing-sharded and the monolithic-migration branches of `open_or_create_db`.
2. **The log checkpoints at 1000 pages** (`PRAGMA_WAL_AUTOCHECKPOINT_PAGES`, SQLite's default, as
   Chromium and the memory database) **and is cut back to 12 MiB when it starts again**
   (`PRAGMA_JOURNAL_SIZE_LIMIT_BYTES`, as the memory database).

## Measured

Full index of 30,000 synthetic messages, FTS only (no embeddings, so the log's share of the time
is the largest it can be), a reader open alongside, two runs each (2026-10-26):

| checkpoint | limit | msg/s | log peak | log after |
|---|---|---|---|---|
| 1000 | 12 MiB | 318, 362 | 102.5 MB | 99.8 MB |
| 1000 | none | 347, 357 | 102.5 MB | 102.5 MB |
| 4000 | 16 MiB | 363, 360 | 102.5 MB | 99.8 MB |
| 16000 | 64 MiB | 360, 364 | 147.4 MB | 99.8 MB |
| 200,000 | none | 372, 366 | 879.9 MB | 879.9 MB |

The speeds are within the runs' noise; at most about 2-3% slower than 200,000, less with
embeddings and Thunderbird's 250 ms between batches. The peak is the largest single transaction
(a batch whose FTS5 segment merge rewrites much of a shard); the log has to hold a whole
transaction, so no checkpoint setting lowers it.

## Consequences

- The log on disk comes back to at most 12 MiB at the next write after a large transaction, and
  small batches keep it to a few MB (`test_the_email_log_comes_back_down_after_one_large_batch`,
  `test_small_email_batches_keep_the_log_small`).
- Between the last write of a full index and the next write, the log can keep the size of the last
  transaction (about 100 MB measured). New mail writes again soon; no idle truncate was added.
- The settings on all four opens are pinned by
  `test_every_writer_open_of_the_email_database_sets_the_writer_settings`.

## Rejected

- **A larger checkpoint (4000 or 16000 pages)**: measured no faster, and 16000 raised the peak.
- **An idle `wal_checkpoint(TRUNCATE)`** as ADR-NF-021 does after compaction: the log already
  comes down at the next write; more code for a transient size.
