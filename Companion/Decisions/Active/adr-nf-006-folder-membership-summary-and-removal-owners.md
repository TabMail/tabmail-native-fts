# ADR-NF-006: Folder Membership Summary and Removal Owners

**Status:** Accepted (2026-10-05). Thunderbird counterpart: the add-on's no-change startup
cleanup and its removal attribution (Thunderbird ADR-024).

## Context

To complete its global membership cleanup, a capable Thunderbird client walks every
`message_ids` row through `listFolderMembershipState` pages, even when nothing changed.
That is one full bounded walk per session (and per reconnect). On an index no writer has
corrupted, the walk's verdict reduces to three numbers: rows with no folder relation, and
relation rows whose folder the client no longer knows, split by whether the row's account
is loaded. Two differences remain, both stated so the client can rely on them knowingly:
- The walk also refuses cleanup when a folder-owned row's msgId does not start with that
  folder's `account:path:` prefix; the summary counts such a row as owned. No writer
  produces one (`indexBatch` callers and the add-on's `getUniqueMessageKey` derive msgId
  and folderId from the same folder).
- The walk ignores ownerless rows of accounts not yet loaded; the summary counts every
  ownerless row. That is stricter and only costs a fallback walk.

Separately, `removeBatch` reported only a count, so the add-on attributed a removal to
every folder whose key range could contain the removed key. A removal in `/Cold:Hot`
therefore restarted `/Cold`'s proof. (The add-on's removal handoff also revokes the
colon-overlap folders' retry authorization before it removes; owner reporting narrows
which proofs a committed removal restarts, not that handoff.)

## Decision

1. New reader RPC `folderMembershipSummary { folderIds, trustedAccountIds }` →
   `{ ok, ownerlessRows, strayTrustedRows, strayUntrustedRows }`. It is advertised as
   `capabilities.folderMembershipSummaryV1`.
   - `ownerlessRows` counts `message_ids` rows with no `message_folder_membership` row.
   - Stray rows are relation rows, joined to `message_ids`, whose `folderId` is not in
     `folderIds` (BINARY equality). They are split by the msgId's account prefix: the text
     before the first `:` when that colon is not the first character. Otherwise the account
     is empty and never trusted. This is the add-on's `_folderReconAccountIdOfMsgId` rule.
   - Folder ids are compared, never decoded.
   - It is one SQL statement, so the three counts come from one WAL read snapshot.
   - Empty `folderIds` or `trustedAccountIds` entries are refused.
   - The reply is fixed-size whatever the archive size, so it can never approach the
     1 MiB host→extension message limit.
2. `removeBatch` adds `removedFolderIds` (the distinct non-null owners of the rows it
   deleted, read with `DELETE … RETURNING folderId` inside the existing IMMEDIATE
   transaction) and `removedOwnerless` (deleted rows with no relation). The change is
   wire-compatible: older clients ignore the fields, and a client detects owner reporting
   by the presence of `removedFolderIds`. A transaction that rolls back reports nothing
   (the call fails). A lost or timed-out reply means the owners are unknown, not that the
   list is empty, and ids counted in `removedOwnerless` have no owner to report.

## Rationale

The summary replaces an O(rows) walk of bounded pages with one O(rows) read when the
index is already clean. A dirty index still falls back to the walk, which alone can repair
rows. A count-only reply deletes every reply-size and owner-decoding question. Reporting
owners from the deleting transaction itself is exact: no key-range guess.

## Consequences

- The summary is one unbounded read on the single reader thread. User searches queue
  behind it while it runs. This amends ADR-NF-004's "bounded calls only" property for this
  one RPC. The add-on calls it at most once per cleanup pass, and measures its wall time.
- `SCHEMA_VERSION` stays 1; there are no schema changes.
- The source version is unchanged in this PR. The release train bumps `Cargo.toml` and
  `HOST_VERSION` together before a helper carrying the capability ships.
