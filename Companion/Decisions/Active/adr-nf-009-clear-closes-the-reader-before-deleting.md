# ADR-NF-009: Clear Closes the Reader's Connection Before Deleting the Files, and Fails If It Cannot Delete

**Status:** Accepted (2026-10-18). Amends ADR-018 point 5. Callers: Thunderbird (`clear` from the
FTS settings, `memoryClear` when chat history is cleared). TabMail Voice does not send `memoryClear`:
its Clear All ends its own helper and deletes the memory folder itself, for this same reason.

## Context

`clear` and `memoryClear` close the writer's connection, delete `fts.db` / `memory.db` with their
`-wal` and `-shm` files, and create the database again. The reader thread kept its own read-only
connection open the whole time and reopened it at its next request (an `AtomicBool` the writer set).

- **Windows:** SQLite opens its files without `FILE_SHARE_DELETE`, so the reader's open handle makes
  the delete fail. The failure was only a warning; the writer reopened the same file with every row
  in it and answered `ok`. Thunderbird's Clear All on Windows reported success and kept the mail index
  and chat memory.
- **Anywhere:** any other failed delete (a read-only folder, a file another program holds) was also
  answered `ok`.
- macOS and Linux delete an open file, so both worked there; only the reader kept the unlinked old
  file open until its next request.

## Decision

1. The reader's two connections live behind a `Mutex` the writer can lock. Each read request holds
   both for its duration (email first, then memory; the writer only ever takes one, so no deadlock).
2. A clear locks the reader's connection for that database for its whole run. It waits for a read in
   progress, closes the reader's file handle (replaced by a throwaway in-memory connection), closes
   the writer's, deletes the files, creates the database, opens the reader's connection again, and
   unlocks. A read that arrives meanwhile waits and then sees the empty database. The `AtomicBool`
   reopen flags are deleted.
3. Files are deleted log first (`-wal`, `-shm`), the database last. A failure leaves the database in
   place, and a new database is never created next to an old log (SQLite would replay it).
4. A delete that fails is an error reply (`clear: delete <path>: ...`). Both connections are reopened
   on the file still there, every row kept and searchable; for `clear`, `known_years` is kept.
5. Wire protocol, schema, pragmas and the delete-and-recreate approach are unchanged.

## Rejected

- **Emptying the database in place** (SQLite reset + VACUUM, or a backup of an empty database over
  the file). Tried first (unmerged): it needed a schema-cookie fix, a read transaction per request
  and removing the reader's mmap, each a change to how every Thunderbird read runs. Owner direction
  2026-10-18: keep the helper compatible and change only what the bug needs.
- **Only turning the warning into an error.** Honest, but Windows clears would then always fail,
  because the reader's handle is still open.

## Consequences

- A read waits while a clear runs (deleting and creating an empty database: milliseconds).
- A clear waits for a read in progress (a long search delays the clear, not the other way round).
- An error reply rejects the request in Thunderbird 1.9.1: its FTS settings show "Clear failed: …";
  clearing chat history logs a warning and goes on (unchanged; before, the same case answered ok).

## Verification

- `tests/test_multithreaded.py`: a clear that cannot delete answers an error and keeps serving, then
  succeeds once it can (email and memory; red on the previous code: answered `ok`); a clear with the
  reader's connection open leaves no cleared text in any file and none after a restart (email and
  memory; red on Windows only); reads pipelined around clears never fail.
- Differential run of Thunderbird 1.9.1's request shapes (1,161 requests) against the previous
  build: identical answers.
