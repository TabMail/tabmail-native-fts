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

## Amendment 2026-11-06 (#12): a trim left above its limit finishes while the writer is idle

Before this, a trim cut short at its 20 s budget continued only with the next limited write or
`memoryTrim`. After a large drop in the limit (5 GB to 100 MB), memory stayed above the new limit
for many kept requests. Now the writer finishes it without waiting for requests:

- **What is pending.** The writer keeps the limit of the latest limited write (`memoryIndexBatch`
  with `maxBytes`, or `memoryTrim`) that answered `withinLimit: false`. The request handler sets it
  in the writer's `MemoryTrimWork`, next to the trim it just ran, so no second reading of the
  answer can drift from it. These end it:
  - a later limited write that ends within its limit, or that sets another limit (the latest
    wins: the client's latest setting);
  - a `memoryIndexBatch` without `maxBytes` (the client keeps no limit now);
  - `memoryClear`.

  A refused request (a bad `maxBytes`) changes nothing; a request whose own trim fails is
  answered with an error and leaves no new limit pending. It lives only in the process; after a
  restart, the next limited write trims again, as before.
- **Idle steps.** The writer's idle work (`IdleWork::step`) runs one step at a time and checks
  for requests between steps, in this order:
  1. converting stale email shards (ADR-NF-005);
  2. a step of the pending trim, `memory_trim_to_bytes` with `IDLE_TRIM_STEP_BUDGET_MS` (1 s,
     plus the overrun past a deadline described above), so a request waits for at most one step;
  3. compaction (ADR-NF-021). It comes after the trim, so the space the trim freed goes back too.
- **Never spins.** The step ends at the limit, on an error (logged; nothing is left pending, and
  the next limited write trims again), or once no rows are left,
  which happens when the limit is below an empty memory's size. A step with time left removes at
  least one row or runs one merge step.
- **Clients.** They still get the same answers. Their copy ("more go as new requests are kept")
  now understates how soon memory settles; changing it is the clients' call.
- **Tests.** Unit tests in `src/main.rs` cover:
  - the writer thread itself (`writer_thread_main`, which takes its `IdleWork` from the caller
    for this), for `memoryTrim` and a limited `memoryIndexBatch`: the request's own trim cut
    short (1 ms request budget), no further request or only an unrelated write queued behind it,
    and the memory file then within the limit,
    oldest exchanges removed and every newer one kept, after the writer stops;
  - the same for `IdleWork::step` driven directly: within the limit, the oldest
    exchanges removed and every newer one kept, compaction after it;
  - the pending rules through the request handler: the latest limit wins; a refused or unrelated
    request changes nothing; a write without a limit, a trim within its limit, and a clear end it;
  - a storage error during an idle step, in the trim and in the row count after it: the trim
    stops, nothing is retried while idle, every exchange stays whole, and the next limited write
    trims again;
  - the end when no rows are left;
  - no trim in a stand-in, and the trim once the memory is open again.

## Follow-up 2026-11-07 (#12): only `memoryTrim` leaves a trim pending, and idle steps get the trim's own budget

A review of the merged amendment above found two faults; this replaces the rules they name.

- **A batch's limit can be older than the client's latest one.** Voice reads `memorySizeLimit`
  when the key goes down and sends it with that exchange's `memoryIndexBatch`; TB reads it just
  before sending. A user who lowers the limit and then raises it can have a batch built before
  the raise arrive after the raise's `memoryTrim`. Under the amendment above, that batch, cut short,
  set its low limit pending again, and the idle writer went on deleting the memory down to a limit
  the user had already lifted: lost exchanges, the older setting beating the newer. Fixed by
  deletion: a `memoryIndexBatch` still trims to its own `maxBytes` within its own budget, as
  before #12, but neither sets nor ends the pending trim. Only `memoryTrim` (which every client
  sends when the limit changes) sets it when it ends above its limit, and ends it when it ends
  within; `memoryClear` ends it. A batch without `maxBytes` no longer ends it either. The batch
  path's remaining excess is trimmed by the next limited batch, as before #12.
- **The 1 s idle step was too slow.** Each step pays the fixed cost of starting a trim (the size
  reading and the FTS merge start); on a 1.2 GB memory lowered to 100 MB, 1 s steps had not
  finished after 1,201 s, 5 s steps took 329 s and 20 s steps 254 s. `IDLE_TRIM_STEP_BUDGET_MS`
  is deleted; an idle step uses `TRIM_TIME_BUDGET_MS` (20 s), held with the pending limit in
  `MemoryTrimWork.budget`. A request arriving during a step waits for at most one step (about 22 s
  with the overrun), the same as behind a request's own trim and under the clients' 60 s timeout.
  This step size is still to be confirmed; 5 s is the alternative (about 30% slower to finish, a
  5 s worst-case wait).
- **A step stops once the client has gone.** A step holds the memory's write transaction until
  its deadline, so a helper whose client closed stdin mid-step kept the write lock for up to
  about 20 s, and a helper the client started in its place (Voice does, on its next request)
  failed its memory writes (`database is locked`) or left its memory closed at init. Now the
  dispatcher's sender to the writer (`WriterSender`) marks the client gone when it is dropped,
  whichever way the dispatch loop ends (stdin closed, a read error), and the idle trim's stop
  check answers true on that as on its deadline (`memory_trim_to_bytes_unless`), so the step
  commits within one row and the helper exits. Measured with a 200,000-row memory and stdin closed
  5 s into a step: the old helper exited 6 to 16 s after stdin closed and the new helper's writes
  failed in 2 of 3 runs; with the stop, it exited within 0.3 s and every write succeeded (3 of 3).
- **A failed `memoryTrim` still replaces an older limit.** It clears the pending limit before it
  trims, so a newer limit whose own trim fails never leaves an older one deleting while idle.
- **Lifetime.** The pending trim lives only in the helper process, as before. Thunderbird keeps
  its helper running, so the trim finishes there. TabMail Voice ends its helper two minutes after
  its last request once the model has loaded, which drops the rest of a large trim (measured: a
  2.7 GB memory lowered to 100 MiB was left at 1.34 GB). The helper cannot keep trimming after its
  client closes it without two helpers writing the same memory, so keeping the helper running
  while a trim is unfinished is Voice's change (TabMail/tabmail-voice#393). Until then, Voice's
  later limited writes trim as before #12.
- **Tests** now cover: an idle step with the client gone removes nothing and leaves no
  transaction open, and trims once the client is back; the client going in the middle of a step
  (set at the step's first deletion) stops it after one whole exchange, commits, and a successor
  writes at once; dropping `WriterSender` sets the flag before the writer sees the disconnect, and
  a request already sent is still served; a batch whose own trim fails keeps its committed row
  and the pending trim, which the idle writer then finishes; a failed `memoryTrim` after a cut-short one leaves nothing pending and
  the idle writer removes nothing; one idle step with a 1 ms budget leaves the memory above the limit and
  returns more-to-do (a step never runs the whole trim); a batch cut short leaves nothing pending
  and the idle writer removes nothing; `memoryTrim(low)` cut short, `memoryTrim(high)` within,
  then a batch carrying the old low limit cut short: the idle writer removes nothing; the
  pending rules (refused, unrelated and batch requests change nothing; the latest `memoryTrim`
  wins; within limit and clear end it); and the writer-thread hand-off for `memoryTrim`, with and
  without an unrelated write queued behind it.

## Follow-up 2026-11-08 (#75): the shard conversion and the memory VACUUM stop once the client has gone

The trim's stop above covered only the idle trim. The two other idle steps cannot stop at a row:
converting a stale email shard (`rebuild_next_stale_shard`, ADR-NF-005) copies the whole year shard
in one transaction, and converting an older memory (`compact_memory_step`, ADR-NF-021) is one full
`VACUUM`. A helper whose client closed it during either kept its write lock until the step ended,
and a helper started in its place could fail its writes on that database meanwhile.

- **SQLite interrupts them.** `IdleWork::step` runs both under `stopped_once_client_gone`, which
  installs a progress handler (`IDLE_STOP_CHECK_INSTRUCTIONS`, every 1,000 virtual-machine
  instructions) that answers the same `client_gone` flag as the trim's stop check, and removes it
  when the step ends. Once the flag is set the running statement fails, its transaction rolls back
  and the writer exits: the shard stays stale and the memory unconverted, every row with them, and
  the next start converts them. The failed step is logged, as any failed idle step is, and is not
  retried in that process.
- **Only the idle step runs under it.** A request already sent is answered in full after it, as
  before (the handler is off outside the step).
- **What it cannot stop:** the end of each step after SQLite's last check. For a `VACUUM` that is
  the rest of the table being copied (it spends about four instructions per row, so up to about
  250 rows; a sqlite-vec chunk table under about 250 rows, at about 1.5 MB per chunk, is copied
  with no check at all), then the final copy back into the database file, which is one backup step
  with no virtual-machine instructions, then the automatic checkpoint (no write lock) and the
  step's `wal_checkpoint(TRUNCATE)`, which waits up to the 2 s busy timeout for readers. Measured
  on a 90 MB memory, from the last check to the end of the `VACUUM`: 0.53 s with the automatic
  checkpoint off, 0.74 s with it on; the copy back is estimated at about 3 s per GB kept, so
  roughly 3-4 s for 1 GB and 15-20 s for 5 GB. For a shard it is freeing the old shard's pages,
  the rename and the commit (35 ms of 246 ms for 3,000 messages, growing with the shard). It is
  the helper started in its place that can meet this: its writes to that database within the
  window can fail with "database is locked". At most once per older memory, and only if the
  client closes in that window; before #75 the whole step was exposed.
- **Incremental steps too.** The memory compaction step under the handler is also the incremental
  one (`PRAGMA incremental_vacuum`) and its checkpoint; an interrupted incremental step rolls back
  and compaction stops for the process, as on any error.
- **Tests** (`src/main.rs`): a conversion with the client already gone leaves the shard whole and
  stale, no transaction open, and a long statement after it runs in full; the client going during
  the copy (set at the first row written to the new shard) rolls the copy back and leaves no
  `_retok` table; an older memory with the client gone stays at `auto_vacuum` NONE with its rows and
  free pages unchanged; each converts with the client present; and the client going once the
  `VACUUM` has begun (set by an authorizer at its first write to its copy, `vacuum_db`) leaves the
  memory the same way, so a check made only before the step starts does not pass. All four fail
  with the handler removed.

