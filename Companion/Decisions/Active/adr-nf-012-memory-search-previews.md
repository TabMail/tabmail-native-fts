# ADR-NF-012: Memory Search Can Return Previews

**Status:** Accepted (owner, 2026-10-20). Caller: Thunderbird's `memory_search` chat tool and its
Chat History clean-up. TabMail Voice does not send it (its pipe to the helper has no size cap).
ADR-NF-011 is taken by the FTS query change on its own branch.

## Context

Thunderbird's native messaging refuses a message over 1 MiB from the helper. It then disconnects the
helper, and every request still waiting is rejected. `memorySearch` returns each matching memory row
in full (`content`, plus `snippet` for a query), and memory rows are kept in full: one long chat
exchange can be hundreds of kilobytes. A listing or a search of a few such rows passed the cap,
although Thunderbird shows only the first 400 characters of each.

## Decision

1. `memorySearch` takes an optional `previewChars`, a positive integer. With it, each result's
   `content` and `snippet` are cut to that many characters, on a character boundary. Every other
   field (`memId`, `sessionId`, `dateMs`, `role`, `score`) is unchanged, and so is the ranking.
2. Without it, or with a value that is not a positive integer (0, negative, a string), every result is
   whole, as before.
3. The stored rows are never changed: the cut is made on the reply only. `memoryRead` still returns a
   whole exchange.
4. No capability flag: a helper without this support ignores the parameter and answers whole rows, as
   it always has, and Thunderbird updates its helper itself.

## Rejected

- A byte cap on the whole reply (drop rows past it): the caller then can't tell a short result from
  a cut one, and it pages by date.
- Cutting stored rows at write time: user content is stored in full (root rule 11).

## Evidence

- `memory_search_cuts_results_to_the_preview_asked_for` (unit): listing and FTS search cut to 10
  characters on a multi-byte boundary; 0, -5 and `"10"` give whole results; the snippet is cut; the
  stored rows stay whole. Red with no cut, and with `content` only.
- `test_previews_keep_a_memory_reply_under_the_native_message_cap` (process): three rows of about
  600 KB; the whole listing is over 1 MiB; a listing and a search with `previewChars: 400` are far
  under it, carry all three rows, and each `content` is the row's first 400 characters.
