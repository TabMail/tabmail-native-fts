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
3. Existing shards migrate **in place** under ADR-024's pattern. `rebuild_stale_shards`
   (renamed from `rebuild_stale_tokenizer_shards`) treats a shard as stale when its CREATE
   statement still carries `tokenchars` **or** `PRAGMA table_info` lacks `attachmentNames`.
   It rebuilds with a rowid-preserving `INSERT .. SELECT`, one transaction per shard, newest
   year first, inside the existing 45 s init budget. Leftover shards convert on the next
   init. `SCHEMA_VERSION` stays at 1, so Thunderbird does not re-feed any mail.
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

- First start after upgrade rebuilds every shard once (CPU and a temporary second copy of
  one shard on disk). This is the same one-time cost the 2026-06 tokenizer migration had.
- A row written to a not-yet-converted shard loses its names for good. This only affects
  older-year shards on archives too large to convert within one init.
- iOS has its own index and is unchanged.
