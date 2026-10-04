# ADR-NF-005: Attachment File Names Get Their Own FTS Column, Migrated In Place

**Status:** Accepted (owner, 2026-10-04). Thunderbird counterpart: the add-on sends
`attachmentNames` on every newly indexed row.

## Context

Keyword search could not find a message by an attachment's file name. Shards indexed
`msgId, subject, from_, to_, cc, bcc, body`, and the add-on sent no names. A message whose
only content is an attached PDF named after its subject (a receipt, a statement) was
unreachable by that name. The owner weighed putting the names in an existing column
(`body`) against a dedicated column and chose a new column.

## Decision

1. Shards gain an eighth FTS5 column, `attachmentNames`, **appended last**. Every existing
   column keeps its position, so `snippet(..., -1, ...)` and the positional `bm25()`
   weights are unchanged. The new column weighs `3.0`, the same as `from_`.
2. `indexBatch` reads an optional string field `attachmentNames` (newline-separated text
   the add-on builds from the downloaded MIME tree). It is not capability-gated: an older
   helper ignores the unknown field, and an older add-on leaves the column empty.
3. Existing shards migrate **in place** with ADR-024's copy, but **on the writer thread, not
   in `init`** (amends ADR-024's "inside init under a 45 s budget"). A shard is stale when
   its CREATE statement still carries `tokenchars` **or** `PRAGMA table_info` lacks
   `attachmentNames`. Whenever the writer has no request waiting, `rebuild_next_stale_shard`
   rebuilds the newest stale shard with a rowid-preserving `INSERT .. SELECT` in one
   transaction, then the writer checks for requests again. `SCHEMA_VERSION` stays at 1, so
   Thunderbird does not re-feed any mail.
   - Why not in `init`: the budget was checked only between shards, so one large shard
     (85k rows measured at 64 s on an M1 Pro) ran past the add-on's 60 s init RPC timeout.
     A timed-out init disconnects, Thunderbird kills the helper 3 s later, the shard rolls
     back, and the next start repeats it — search permanently down
     (found before release, 2026-10-04). On the writer, init answers at once; a timeout on an ordinary RPC does
     not disconnect.
   - A write that arrives mid-shard waits for that shard, never longer (`next_write_request`
     in `main.rs` checks the queue between shards). On a very large shard the add-on's call
     may time out while the helper still runs the write later, in order. `indexBatch` is
     idempotent; a remove-then-re-add pair must send the re-add even when the remove call
     failed (the add-on's `readdStaleAttachmentRows` does), or a late remove leaves the rows
     deleted. Reads keep working throughout (WAL snapshot of the old table until the swap
     commits).
   - Each shard is attempted at most once per process; a failure is logged and retried on
     the next start, so a broken shard cannot keep the writer busy. Every start makes
     progress, so the migration converges unless a single shard takes longer than a whole
     Thunderbird session.
4. Until a shard converts, writes to it omit the column and drop that row's names, and
   reads return `""`. Search keeps working across mixed shards: `bm25()` ignores the extra
   weight on a seven-column shard, and no query names the column.
5. `getMessageByMsgId` returns `attachmentNames` (`""` when absent or NULL), so the add-on
   can carry the names through a remove-and-re-add of a stored row.
6. **No backfill.** Rows indexed before this change keep an empty column until a full
   reindex. Neither the in-place migration nor a smart reindex fills them: the content was
   never stored, and the add-on's scan skips rows that are already indexed.

## Rationale

- A dedicated column keeps message text and file names apart, gives the names their own
  ranking weight, and lets a later change return them (for example, to list attachments
  of an index-served message) without parsing the body.
- Appending the column and migrating in place costs one local re-copy of each shard
  (minutes on a large archive, once per install). A `SCHEMA_VERSION` bump would cost a
  multi-hour re-feed from Thunderbird.

## Consequences

- After upgrade the writer rebuilds every shard once in the background (CPU, and a temporary
  second copy of one shard on disk), the same one-time cost as the 2026-06 tokenizer
  migration, but no longer on the startup path.
- A row written to a not-yet-converted shard loses its names for good. Newest shard first
  keeps this window short for new mail.
- iOS has its own index and is unchanged.
