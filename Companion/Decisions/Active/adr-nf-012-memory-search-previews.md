# ADR-NF-012: Memory Search Can Return Previews

**Status:** Accepted (owner, 2026-10-20). Caller: Thunderbird's `memory_search` chat tool (its
Chat History clean-up still asks for whole rows, Thunderbird #147). TabMail Voice does not send it (its pipe to the helper has no size cap).
ADR-NF-011 is taken by the FTS query change on its own branch.

## Context

Thunderbird's native messaging refuses a message over 1 MiB from the helper. It then disconnects the
helper, and every request still waiting is rejected. `memorySearch` returns each matching memory row
in full (`content`, plus `snippet` for a query), and memory rows are kept in full: one long chat
exchange can be hundreds of kilobytes. A listing or a search of a few such rows passed the cap,
although Thunderbird shows only the first 400 characters of each.

## Decision

1. `memorySearch` takes an optional `previewChars`, a positive integer. With it, each result's
   `content` and `snippet` are cut to that many characters (Unicode code points, so never inside
   one), and only where they are longer. Every other field (`memId`, `sessionId`, `dateMs`, `role`,
   `rank`, ...) is unchanged, and so is the ranking. A caller sizing its reply counts up to 4 bytes a
   character (6 for one JSON escapes); a JavaScript caller showing N UTF-16 units gets at least N.
2. Without it, or with a value that is not a positive integer (0, negative, a string), every result is
   whole, as before.
3. The stored rows are never changed: the cut is made on the reply only. `memoryRead` still returns a
   whole exchange.
4. No capability flag: a helper without this support ignores the parameter and answers whole rows, as
   it always has, and Thunderbird updates its helper itself.

## Rejected

- A byte cap on the whole reply (drop rows past it): the caller then can't tell a reply short of
  rows from a cut one, and it pages a listing by date. (A cut row it can tell: it asks for one
  character more than it shows, if it needs to know.)
- Cutting stored rows at write time: user content is stored in full (root rule 11).

## Evidence

- `memory_search_cuts_results_to_the_preview_asked_for` (unit): listing and FTS search cut to 10
  characters on a multi-byte boundary; a row shorter than the cut and every other field (an id and a
  session longer than the cut) come back as without it; 0, -5 and `"10"` give whole results; the
  snippet is cut to its first characters; the stored rows stay whole. Red with no cut, with `content`
  only, with short text emptied and with the ids cut too.
- `test_memory_search_previews_with_embeddings` (process, model loaded, so the search is hybrid):
  the matching row cut to its first 400 characters, the other fields and results as without it; red
  with no cut.
- `test_previews_keep_a_memory_reply_under_the_native_message_cap` (process): three rows of about
  600 KB; the whole listing is over 1 MiB; a listing and a search with `previewChars: 400` are far
  under it, carry all three rows, and each `content` is the row's first 400 characters.

## Amendment (2026-10-21): what Thunderbird's paged listing and 100-match search need from the helper

Thunderbird's chat now lists memory five entries a request, and a search asks for its 100 best
matches (it asked for 2,000). Review of that change found two helper defects it exposes.

1. **A search with dates kept too few matches.** The vector candidates were not filtered by date:
   the merge kept the best `limit` of all candidates, and those outside the dates were dropped only
   afterwards, so nearer entries outside the dates took the places of matches inside them (7 of 31
   in-range matches at `limit` 100 on 6,000 entries). It never showed before because a `limit` of
   2,000 asks sqlite-vec for k = 8,000, over its maximum of 4,096: the vector search failed, and
   every such search fell back to keyword-only. Now the vector candidates outside the dates are
   removed before the merge (one `memory_meta` lookup per candidate, only when dates are given), and
   the check after the merge, which can no longer drop anything, is deleted. The keyword candidates
   were already filtered by date in their query.
   With that check gone, a write landing between the search's queries could put an exchange outside
   the dates in its results: the candidate's date was read before the write, its row after it, and
   an exchange removed while it is the newest leaves its rowid to the next one kept. So a memory
   search now runs in one read transaction, and all its queries read the memory as it was when it
   began (dated or not: an undated search could give the other exchange the same way).
2. **Each listing request read the whole memory.** The listing sorted the join of `memory_fts` and
   `memory_meta`, so every row's content was read for each request (1.5 to 4 s at 1 GB, on the
   reader thread email search also uses), and a page of 20 takes four requests. Now the page's rows
   are chosen from `memory_meta` (`rowid`, `dateMs`) by date, and only their content is read: the same
   rows in the same order. The join is a `CROSS JOIN`, which SQLite keeps in the order written: with a
   plain `JOIN` it scanned `memory_fts` and looked each row up in the page, still reading every row's
   content (a page of 3 of 100 long rows read 130 pages from disk; now 9). No index or schema change.

Still open (not here): a search's `limit` times `CANDIDATE_MULTIPLIER` over 4,096 still fails the
vector search silently, for email and memory searches alike (#20; Thunderbird's email search and Voice's
memory search ask for 2,000), so those run keyword-only.
The date filter also runs on sqlite-vec's top k over the whole memory: when k or more entries
outside the dates are nearer the query, no match inside them keeps its vector score, so an entry
inside the dates that matches only by meaning is missed (one with the words is still found by the
keyword leg). Asking sqlite-vec for the dates directly would need the date in the vector table.

Evidence: `memory_listing_gives_the_newest_within_the_dates_each_whole` (unit: newest first, the
limit, both bounds inclusive, `ignoreDate` over the dates, every field whole) passes on the old
query and the new; red with the inner order reversed, the outer order dropped, and `from` ignored.
`memory_listing_reads_only_its_pages_content` (unit: that page, on a cold reader) is red with the
plain `JOIN`.
`test_memory_search_with_dates_keeps_the_matches_within_them` (process, model loaded: 20 entries
nearer the query before the dates and 20 after, 6 matches inside with one at each bound, 300 others)
returns the 6; red on the old code (it returned 4 of them) and with `to` not enforced.
`test_memory_search_with_dates_scores_the_matches_within_them_as_without` (2 nearer entries on each
side, 6 matches inside near enough the query that their vector score counts) ranks each match
inside the dates as the search without dates does, and one bound alone keeps the matches on its
side; red with the in-range vector candidates dropped, with `from` exclusive, and with the filter
applied only when both bounds are given. `memory_search_with_one_date_scores_a_match_by_meaning_as_without`
(unit, model loaded) does the same for a match by meaning alone; red with the vector candidates dropped
when only one bound is given.
`memory_search_answers_from_the_memory_as_it_began` (unit, model loaded, skipped without it: a write
removing the match and keeping an exchange outside the dates in its rowid, landing at each of about
40 points across a search with dates and one without) gives the match every time; without the
transaction, 34 of 39 points lost it and one gave the other exchange, and with it only for a search
with dates, the search without them lost it. `memory_search_after_a_failed_one_is_answered` (unit: a
read refused inside the search) answers the next request; red when a failed search leaves its
transaction open. `memory_search_cuts_a_match_by_meaning_to_the_preview_asked_for`
(unit, model loaded) cuts a match by meaning alone; red with results that have no snippet left whole.
