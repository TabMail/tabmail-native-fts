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
4. A delete that fails is an error reply whose text is only the method (`clear` or `memoryClear`;
   the file and the OS error go to the local log, not over the wire). Both connections are reopened
   on the file still there, every row kept and searchable, and writes go on; for `clear`,
   `known_years` is emptied either way (a shard still there is only verified again).
5. A clear that fails after closing a connection (the files could not be opened again) leaves an
   in-memory stand-in in its place. The reader before each read, and the writer before each write,
   open the database again in place of a stand-in, so service resumes once the files can be opened,
   without another clear or a restart; nothing is deleted then, so rows written since stay.
   `known_years` is emptied before the clear closes anything.
6. Exit is unchanged: the main thread drops its copies of the reader's connections once the threads
   start (sharing them must not keep them open until it returns), and both threads are told to stop
   together, as before.
7. Wire protocol, schema, pragmas and the delete-and-recreate approach are unchanged.

## Rejected

- **Emptying the database in place** (SQLite reset + VACUUM, or a backup of an empty database over
  the file). Tried first (unmerged): it needed a schema-cookie fix, a read transaction per request
  and removing the reader's mmap, each a change to how every Thunderbird read runs. Owner direction
  2026-10-18: keep the helper compatible and change only what the bug needs.
- **Only turning the warning into an error.** Honest, but Windows clears would then always fail,
  because the reader's handle is still open.

## Consequences

- A read waits while a clear runs. The writer's close checkpoints the write-ahead log into the file
  about to be deleted, so the wait grows with the log: about 0.4 s for a 547 MB log (the base answered
  in milliseconds, its close not being the last), far below Thunderbird's 60 s request timeout.
- A clear waits for a read in progress (a long search delays the clear, not the other way round).
- An error reply rejects the request in Thunderbird 1.9.1: its FTS settings show "Clear failed:
  clear". Clearing chat history logs the failed `memoryClear` as an error but still shows "Chat
  history cleared" (the add-on does not show the reply's error; carried as TabMail/tabmail-thunderbird#146). The
  helper itself never answers ok while keeping rows.

## Verification

- `tests/test_multithreaded.py`: a clear that cannot delete answers an error and keeps serving, then
  succeeds once it can (email and memory; red on the previous code: answered `ok`); a clear that
  cannot open the files again answers an error, and once they can be opened the next write and reads
  are served from the database, every row kept and later rows stored, with no other clear and none
  after a restart lost (email and memory; red with either thread's reopen removed); a clear with the
  reader's connection open leaves no cleared text in any file and none after a restart (email and
  memory; red on Windows only); reads pipelined around clears never fail.
- `src/main.rs` unit test: a stand-in is opened again once its database can be, and a connection to
  the database is never replaced.
- Differential run of Thunderbird 1.9.1's request shapes (1,161 requests) against the previous
  build: identical answers.
