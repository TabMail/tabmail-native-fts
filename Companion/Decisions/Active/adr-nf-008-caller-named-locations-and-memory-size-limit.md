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
   in `real_main` before the first message. Absent: `~/.tabmail/logs`. A relative or empty value
   is a fatal start-up error.
2. **`init` `modelDir`** (absolute path). The model is downloaded to and loaded from there.
   Absent: `~/.tabmail/models/all-MiniLM-L6-v2`. A relative or empty value gets an error
   response and opens nothing; the helper stays in pre-init and accepts another `init`.
3. **Memory size limit**, in bytes, chosen by the client:
   - `memoryIndexBatch` takes an optional `maxBytes`. After inserting, the helper removes the
     oldest rows (by `dateMs`, then rowid) from `memory_fts`, `memory_meta`, `memory_vec` and
     `memory_ids` together until the used size is within the limit. The result gains `trimmed`,
     `usedBytes` and `withinLimit`. Without `maxBytes` nothing changes (older clients).
   - New writer method `memoryTrim { maxBytes }`, sent when the user lowers the limit: the same
     trim, then `VACUUM` + `wal_checkpoint(TRUNCATE)` when at least
     `TRIM_VACUUM_MIN_FREE_PERCENT` of the pages are free, so the disk space comes back. Result:
     `{ trimmed, usedBytes, withinLimit, compacted }`.
   - `memoryStats` gains `usedBytes`.
   - **Used size** = `(page_count - freelist_count) * page_size`. Freed pages stay in the file and
     are reused, so the file length is not the measure.
   - A non-positive or non-integer `maxBytes` is an error, and nothing is written.

The orphan clean-up (ADR-NF-007) is unchanged. It takes the add-on id from the last component of
`profilePath`, so a caller whose folder is not named `thunderbird@tabmail.ai` only ever looks for
folders under its own name, which Thunderbird never creates.

## Trim mechanics

- Each pass removes a share of the rows proportional to the excess over
  `TRIM_TARGET_PERCENT` of the limit (at most `TRIM_DELETE_BATCH_MAX_ROWS` per transaction), then
  re-measures. Aiming below the limit means the next batches do not each trim again.
- Wall time is bounded by `TRIM_TIME_BUDGET_MS`, checked between delete transactions. Each is
  small, so the budget can interrupt a long trim; a budget checked only around one large unit
  cannot. That is well under the
  add-on's 60 s native RPC timeout. A trim cut short reports `withinLimit: false`, and the next
  write or trim continues it.
- **Overshoot, accepted:**
  - FTS5 deletes leave tombstones until segments merge, so freeing a byte takes deleting a little
    more than a byte of rows.
  - sqlite-vec 0.1.9 stores vectors in chunks of 1024 rows (about 1.5 MB at 384 dimensions) and
    frees a chunk only once all its rows are deleted.

  A trim can therefore remove up to about one chunk of rows beyond what is strictly needed. At a
  1 GB limit (about 700k exchanges) that is about 0.15%. On a tiny database whose rows all share
  one chunk, a trim below the chunk size empties the memory.
- `VACUUM` keeps `memory_ids` rowids aligned with the other tables; a unit test pins this.

## Consequences

- Thunderbird's behaviour is unchanged until the add-on sends `maxBytes` / `memoryTrim`. An older
  helper ignores `maxBytes` and answers `memoryTrim` with "Unknown writer method", so the add-on
  treats that as "no limit until the helper updates".
- Voice gets fully separate files by naming all three locations.
- Tests:
  - `tests/test_caller_locations.py` runs a full memory session with all three locations given,
    under a temporary HOME holding a fake Thunderbird profile index, model folder and log. Every
    file under HOME stays byte-identical.
  - Unit tests in `memory_db.rs` cover trim and compact.
