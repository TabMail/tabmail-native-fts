# ADR-NF-013: A Hybrid Search Asks sqlite-vec for at Most Its Maximum, and Email Search Keeps Its Dated Matches

**Status:** Accepted (2026-10-21, issue #20). Follows the ADR-NF-012 amendment, which made the same
date and snapshot changes to memory search and left this open.

## Context

A hybrid search (email `search`, `memorySearch`) asks sqlite-vec for `limit` x
`CANDIDATE_MULTIPLIER` (4) nearest vectors, twice that when an email query names a field
(`from:`, `subject:` ...), so the vector candidates can be filtered to that field's matches.
sqlite-vec refuses a k over 4,096 (`SQLITE_VEC_VEC0_K_MAX` in `sqlite-vec.c`), and the search turned
that error into "no vector candidates" (`unwrap_or_default`) and fell back to keyword-only without a
log line about the error. So any search with a `limit` over 1,024 (512 with a field) never used
its vector leg: Thunderbird's email search and TabMail Voice's memory search ask for 2,000, so
neither ever found a message by meaning alone.

Letting the vector leg run exposes the email search's twin of the defect ADR-NF-012 fixed for
memory: its vector candidates outside the dates were dropped only after the merge had kept the
best `limit`, so nearer messages outside the dates took the places of matches inside them.

## Decision

1. `search_vec_candidates` asks sqlite-vec for at most `config::hybrid::VEC_K_MAX` (4,096) and logs
   when it asks for fewer than the search wanted. The merge keeps only `limit` anyway. This covers
   email and memory searches and the doubled k of a search with a field.
2. When the vector search does fail, the search still runs keyword-only (as before), and it now
   logs the error as a warning.
3. An email search with dates removes the vector candidates outside them before the merge (one
   `message_meta` lookup per candidate, only when dates are given; either bound alone, each
   inclusive). The check after the merge, which can no longer drop anything, is deleted. The
   keyword candidates were already filtered by date in their query. The date filter is one
   function, `vec_candidates_within_dates`, used by email and memory search.
4. An email search runs in one read transaction (`search` wraps `search_rows`), as a memory search
   does: all its queries read the archive as it was when it began. Without it, a write between them
   (a message removed and its rowid, reused by `message_ids`, given to a new one) could put another
   message, or one outside the dates, in the results, now that no check after the merge is left. The
   year shards are listed just before the transaction. A shard conversion keeps the table's name, so
   that list stays valid. A failed search rolls its transaction back.

## Consequences

- Thunderbird's email search (limit 2,000) and Voice's memory search (2,000) are hybrid, like every
  search at a smaller limit already was: ranked by the combined score, and results under `MIN_SCORE`
  (a weak keyword-only match among them) left out. So a search can return fewer results than its
  keyword-only answer gave. Both callers take any count up to `limit`. Thunderbird keeps the
  helper's order for a relevance sort and sorts by date otherwise.
- At most 4,096 vector candidates whatever the `limit`: with a `limit` over 1,024, a match by meaning
  alone outside the 4,096 nearest is missed (one with the words is still found by the keyword leg).
- Still open (not here): the date filter and the field filter run on sqlite-vec's nearest k over
  the whole archive. When k or more messages outside the dates (or the field's matches) are nearer
  the query, a match inside them by meaning alone is missed. Asking sqlite-vec for them directly
  would need the date (and the field) in the vector table.

## Rejected

- Lowering `CANDIDATE_MULTIPLIER` or capping `limit` at the callers: every caller (Thunderbird,
  Voice, any later one) would have to know sqlite-vec's maximum. The helper is the one place that
  knows it.
- Failing the search when the vector leg fails: a keyword-only answer is still an answer, and with
  the clamp the error is rare (a negative `limit`, a broken vector table). It now logs a warning
  instead of failing silently. (A rebuild empties the vector table, which answers no rows, not an
  error: that goes through the fallback for no vector candidates.)

## Evidence

Each test below was checked against a mutated build. "Red" means it fails there.

- `vec_candidates_over_sqlite_vecs_most_are_its_most` (unit, no model: 4,106 vectors written
  directly): asked for 4,095, 4,096, 4,097 and 8,000, it gets 4,095 and then 4,096. Red with no clamp.
- `vec_candidates_within_dates_keeps_those_dated_within_them` (unit, no model): both bounds, each
  alone, each inclusive, order kept, a candidate with no metadata dropped, everything kept without
  dates. Red with either bound exclusive and with the filter only when both are given.
- `search_with_a_large_limit_finds_a_match_by_meaning` (unit, model loaded): limits 10, 1,024,
  1,025, 2,000 unscoped, and 10, 512, 513, 2,000 with `subject:`, all find the match by meaning
  alone. Red with no clamp. `memory_search_with_a_large_limit_finds_a_match_by_meaning` does the
  same for memory.
- `search_with_dates_keeps_the_matches_within_them` (unit, model loaded: 8 nearer messages before
  the dates and 8 after, 3 matches inside with one at each bound, each naming the query once in a
  long body, 100 others): returns the 3; with one bound, the matches and the nearer messages on its
  side. Red on the old code (the old check after the merge: the nearer messages, scoring 0.12 to 0.16
  by vector alone against the matches' 0.13, took the places), with no filter, and with the filter
  only when both bounds are given. (An exclusive bound is red in the test above: here the matches at
  the bounds also come in through the keyword leg.)
- `search_answers_from_the_archive_as_it_began` (unit, model loaded: a write removing the match and
  giving its rowid to a message outside the dates, landing at 41 points across a search with dates
  and one without): the match every time. Red without the transaction.
- `search_after_a_failed_one_is_answered` (unit: a `message_meta` read refused inside the search):
  the next search is answered. Red when a failed search leaves its transaction open.
- `search_whose_vector_leg_fails_answers_by_keyword` (unit, model loaded: reads of `messages_vec` and
  `memory_vec` refused): email and memory search each answer the keyword match. Red when a vector
  error fails the search.
- `test_searches_at_the_callers_limit_are_hybrid` (process, model loaded): email search at limit
  2,000 with and without dates, a `subject:` search at 1,000, and a memory search at 2,000 each find
  the match by meaning alone, and the dated one leaves out the nearer message outside the dates.
  Red with no clamp.
