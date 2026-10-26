# ADR-NF-008: The Caller Names the Log and Model Folders, and Sets a Memory Size Limit

**Status:** Accepted (2026-10-17). Callers: Thunderbird (size limit), TabMail Voice (all three).

## Context

TabMail Voice keeps its own agent chat memory and searches it with this helper, which it builds
from this repository at a pinned revision and ships inside the app. Many Voice users also run
Thunderbird with its own installed copy of the helper. The two copies must never share files:

- **Database.** Already caller-named: `init` `profilePath`.
- **Model folder** `~/.tabmail/models/all-MiniLM-L6-v2`. `download_and_verify` writes
  `<file>.tmp` (`File::create` truncates) and then renames, and later starts only check that the
  files exist. Two copies downloading at once could leave a cut-short file, and every later start
  would accept it and lose semantic search.
- **Log folder** `~/.tabmail/logs`. Two processes would rotate one file.

Separately, chat memory (`memory.db`) had no size bound.

The helper must not know who runs it: no client modes, no client names.

## Decision

1. **`--log-dir <absolute path>`** (start-up argument). It is an argument because logging starts
   in `real_main` before the first message. Absent: `~/.tabmail/logs`. A relative or empty value,
   or the `--log-dir=<path>` form, is a fatal start-up error, before anything is logged.
   **`--log-level <off|error|warn|info|debug>`** (start-up argument) sets the file log's level.
   Absent: `debug`, as before. At `off` nothing is logged and no log folder or file is made. A
   caller whose policy keeps user content out of logs (TabMail Voice outside its debug mode) needs
   it: the memory search paths log the query text, and an error reply quotes it. Any other value,
   or the `--log-level=<level>` form, is a fatal start-up error.
2. **`init` `modelDir`** (absolute path). The model is downloaded to and loaded from there.
   Absent: `~/.tabmail/models/all-MiniLM-L6-v2`. A relative or empty value gets an error
   response and opens nothing; the helper stays in pre-init and accepts another `init`.
3. **Memory size limit**, in bytes, chosen by the client:
   - `memoryIndexBatch` takes an optional `maxBytes`. After inserting, the helper removes the
     oldest rows (by `dateMs`, then rowid) from `memory_fts`, `memory_meta`, `memory_vec` and
     `memory_ids` together until the used size is within the limit. The result gains `trimmed`,
     `usedBytes` and `withinLimit`. Without `maxBytes` nothing changes (older clients).
   - New writer method `memoryTrim { maxBytes }`, sent when the user changes the limit: the same
     trim. Result: `{ trimmed, usedBytes, withinLimit }`. It does not compact the file (see
     Revisions).
   - `memoryStats` gains `usedBytes`.
   - **Used size** = `(page_count - freelist_count) * page_size`. Freed pages stay in the file and
     are reused, so the file length is not the measure: the file kept its largest size, and
     memory stays within the limit inside it. (Since ADR-NF-021 the idle writer gives freed pages
     back after a trim.)
   - A non-positive or non-integer `maxBytes` is an error, and nothing is written.

4. **A caller that names its model folder never runs the orphan clean-up** (ADR-NF-007). The
   clean-up takes the add-on id from the last component of `profilePath`, and nothing stops a
   caller's folder from having the add-on's name; with that name, an init would move an index an
   earlier add-on release left behind into the caller's folder, or delete it. Naming `modelDir`
   marks a caller that owns all its locations, without a client mode; the add-on sends no
   `modelDir` and keeps the clean-up. If the add-on ever sends `modelDir`, it loses that clean-up
   with it, so it must then do without it or a successor is needed.

## Trim mechanics

- Each pass removes the oldest rows whose estimated share of the used size covers the excess
  over `TRIM_TARGET_PERCENT` of the limit, then re-measures. Aiming below the limit means the
  next batches do not each trim again (for rows of similar size; with whole-session rows oldest,
  a pass can free less than its estimate and the trim then ends just inside the limit). A row's share is estimated from its content length plus
  `TRIM_ROW_OVERHEAD_BYTES` (its index entries, metadata and vector row), scaled so all rows'
  estimates add up to the used size; the sum is read once per call (one scan of the content),
  when the first pass needs it (0.16 s on a 1 GB FTS-only database of 40,644 rows, warm cache;
  there a trim to 99% of the used size removed 6% of the rows in 5.9 s, and a trim to half
  stopped at the budget after 21 s with `withinLimit: false`). Rows differ widely in size: Thunderbird's whole-session rows,
  written before its per-exchange rows, can be a hundred times larger. Sizing a pass by row
  count instead (the first version) removed the fraction excess/used of the rows: with the
  oldest rows largest, one trim removed 786 rows where about 84 would do and left memory at 41%
  of the limit, and at the limit one new exchange removed 184 rows where about 21 would do.
  Rows with the same date go in the order they were written (rowid).
- **Removed rows' FTS space is reclaimed before more rows go.** An FTS5 delete leaves the row's
  entries (and the pages holding them) in the old segments until those are merged. Measuring
  that space and removing more rows for it empties memory without ever reaching a limit that the
  newest rows fit in. So, while over the limit, each pass first runs FTS5 `merge` steps,
  re-measuring after each, and removes rows only once a step finds no work (fewer than two
  changes). `memoryRemoveBatch` (Thunderbird's Delete Old) leaves the same dead entries, and a
  positive step cannot start the merge that drops them, so it starts it itself, with one negative
  step after it removes rows. Without that, a trim after Delete Old of 1,000 of 2,000 rows to half
  the original size kept 532 rows where 937 fit. A negative step restarts a merge in progress, so
  this suits occasional removals; a caller that removed rows before every write on a multi-GB
  memory would bring back the restart stall described below.
- **Merge sign discipline.** A negative `merge` merges every segment into one, and FTS5 restarts it
  from the first term whenever a segment was added since the last step; a positive `merge`
  continues the merge in progress even after new segments arrive (FTS5 documentation, `merge`
  command). So the trim starts the merge once, with one negative `TRIM_MERGE_STEP_PAGES` step right
  after it removes rows, and every other step is positive. Before this, every step was negative:
  where one full merge took longer than the budget, each `memoryIndexBatch` added a segment before
  trimming, the merge restarted every call, and no row was ever removed (measured at 4.6 GB on an
  M1 Pro under load; a full merge at 1 GB took about 7 s there). FTS5 `secure-delete` was tried and rejected:
  it empties the entries in place, but SQLite rebalances b-tree pages only on delete, so the pages
  stay allocated.
- Wall time is bounded by `TRIM_TIME_BUDGET_MS`, asked after every row removed and every positive
  merge step. Past the deadline a call can still commit its removals, start the merge for them
  (one negative step) and run one more positive step. FTS5 does not bound a step's work by its
  page count, so the overrun is measured: on a 5 GiB memory (291,500 rows with vectors, whole
  sessions oldest), the longest step took 0.23 s, a call ran at most 1.7 s past the deadline, and
  the worst `memoryTrim` over native messaging took 21.7 s (a full merge there took 70 s, which is
  why the merge is never run to its end in one call). The rows it removed stay removed (the
  transaction commits early). That is well under the add-on's 60 s native RPC timeout for one
  request; writer requests queued behind trims over a lowered limit answer one after another
  (three back to back answered at 22, 42 and 62 s), and a late one only rejects the add-on's
  promise while the helper finishes the work. A
  trim cut short reports `withinLimit: false`, and the next write or trim continues it.
  `memory_trim_to_bytes` takes the budget; the request handlers pass the config value, and unit
  tests drive the inner `trim_until` with a stop after a set number of questions.
- **Overshoot, accepted (owner decision pending):** the estimate only sizes a pass, and the loop
  re-measures, so text rows land near the aim whatever their sizes. What remains is the vector
  chunks:
  - sqlite-vec 0.1.9 stores vectors in chunks of 1024 rows (about 1.5 MB at 384 dimensions) and
    frees a chunk only once all its rows are deleted.

  sqlite-vec also adds rows only to its newest chunk. When rows are indexed in date order (each
  new exchange is the newest), the oldest rows fill the oldest chunks, so a trim can remove up to
  about one chunk of rows beyond what is strictly needed: 1,024 exchanges, about 7.8 MB at the
  roughly 7.6 KB per exchange measured with vectors, so about 0.73% of a 1 GB limit (about 130k
  exchanges) and about 7.8% of the smallest choice, 100 MB. Rows indexed out of
  date order (a re-feed of older history) spread the oldest dates over many chunks, and a trim
  then removes more than one chunk's worth; it is still oldest first and within the limit. On a
  tiny database whose rows all share one chunk, a trim below the chunk size empties the memory.
  This is sqlite-vec's observed source behaviour (no documented contract).

- **What the limit bounds.** It bounds the database's pages in use, not the files on disk: the
  write-ahead log (`memory.db-wal`) can grow to `PRAGMA_WAL_AUTOCHECKPOINT_PAGES` pages (about
  800 MB at 4 KB pages) during heavy writing and keeps that size after a checkpoint, and freed
  pages stay in the file. At the 100 MB choice the files can be several times the limit.
  *Amended 2026-10-26 (ADR-NF-021, #11):* the memory's log now checkpoints at 1000 pages and is cut
  to 12 MiB at each reset (`journal_size_limit`, set on every writer open), and the idle writer
  gives freed pages back to the disk, so the files come down to about the pages in use shortly
  after a trim.
- **A bundling caller sends no update requests.** `hello` and `updateCheck`/`updateRequest` run
  the self-update mechanics, which install into Thunderbird's user-local helper folder when the
  helper's own folder can't be written. A client that ships its own copy (Voice) never sends
  them; it sends only `init` and the memory methods.

## Revisions before merge (review round 2026-10-18)

- `memoryTrim` first compacted the file (`VACUUM` + `wal_checkpoint(TRUNCATE)` when at least 10%
  of pages were free, result field `compacted`) so a lowered limit gave disk space back. Removed:
  `VACUUM` copies everything kept and cannot be bounded by the trim's budget (about 31 s for
  2.2 GB measured, with the copy in RAM under `temp_store = MEMORY`), and it relied on `VACUUM`
  keeping `memory_ids` rowids, which SQLite does not document for a table without an INTEGER
  PRIMARY KEY. (ADR-NF-021, 2026-10-26: freed pages now go back at idle instead, by bounded
  `incremental_vacuum` steps, with one idle `VACUUM` to convert an older memory; the rowids are kept
  by the bundled SQLite and pinned by a test.)
- The budget was checked only between delete transactions of up to 5,000 rows
  (`TRIM_DELETE_BATCH_MAX_ROWS`); one transaction of long rows took about 22 s, and 6,000 rows of
  360 KB took 219 s. Now asked after every row; the row cap is gone.
- Without the merge step, 5,000 FTS-only rows trimmed to a twentieth of their size were all
  removed, leaving 38 MB of segments and `withinLimit: false` on every retry.
- The orphan clean-up ran for every caller (point 4).
- Every merge step was negative, so a merge cut short restarted at the next write (merge sign
  discipline above).
- The file log was always at `debug`, so a caller could not keep search text out of it
  (`--log-level`, point 1).

## Consequences

- `hello` advertises `capabilities.memorySizeLimitV1`. Clients send `maxBytes` and `memoryTrim`
  only to a helper that advertises it. An older helper ignores `maxBytes` and answers `memoryTrim`
  with "Unknown method: memoryTrim"; a client without the capability treats the limit as "applies
  once the helper updates" instead of matching that text.
- Thunderbird's behaviour is unchanged until the add-on sends `maxBytes` / `memoryTrim`, except
  that `memoryRemoveBatch` (Delete Old) now starts the merge for the rows it removed, which the
  user does not see.
- Voice gets fully separate files by naming all three locations.
- Tests:
  - `tests/test_caller_locations.py` runs a full memory session with all three locations given,
    under a temporary HOME holding a fake Thunderbird profile index, model folder and log. Every
    file under HOME stays byte-identical.
    The default model files and the default live log are FIFOs there, so a read of either (or a
    write to the log) stalls the session, which a deadline fails.
  - The same file pins point 4 with recoverable orphans and a caller folder named
    `thunderbird@tabmail.ai`, fresh and existing, and refuses every `maxBytes` that is not a
    positive integer (fractions, `2.0`, `1e30`, `2^63`, strings, booleans, arrays, objects)
    without writing.
  - Unit tests in `memory_db.rs` cover the trim: newest kept, all four tables together, the
    FTS-only case converging on a feasible limit, a cut-short trim keeping its progress across
    a reopen and continuing, trims cut short while merging that still remove the oldest rows
    with a write between calls, a call out of time while merging stopping with the merge
    unfinished, the aim below the limit leaving room for the next writes, and memory at the limit
    removing only the oldest as each new exchange is kept (never emptied); a trim of several
    passes landing near its aim, one after `memoryRemoveBatch` removing only what the limit
    needs, memory whose oldest rows are the largest or the smallest keeping every newer row
    that fits, and a trim with no time left removing nothing.
  - The location tests also pin a rejected `init` creating, opening and moving nothing, a batch
    whose own row goes over the limit ending within it, and a log folder that can't be made being
    fatal.
