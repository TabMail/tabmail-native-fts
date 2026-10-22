# ADR-NF-013: A Hybrid Search Asks sqlite-vec for at Most Its Maximum, and Email Search Keeps Its Dated Matches

**Status:** Accepted (2026-10-21, issue #20). Follows the ADR-NF-012 amendment, which made the same
date and snapshot changes to memory search and left this open.

## Context

A hybrid search (email `search`, `memorySearch`) asks sqlite-vec for `limit` x
`CANDIDATE_MULTIPLIER` (4) nearest vectors, `FIELD_VEC_MULTIPLIER` (2) times that when an email
query names a field (`from:`, `subject:` ...), so the vector candidates can be filtered to that
field's matches. sqlite-vec refuses a k over 4,096 (`SQLITE_VEC_VEC0_K_MAX` in `sqlite-vec.c`), and
the search turned that error into "no vector candidates" (`unwrap_or_default`) and fell back to
keyword-only without a log line about the error. So any search with a `limit` over 1,024 (512 with
a field) never used its vector leg: Thunderbird's email search and TabMail Voice's memory search
ask for 2,000, so neither ever found a message by meaning alone.

Letting the vector leg run exposes the email search's twin of the defect ADR-NF-012 fixed for
memory: its vector candidates outside the dates were dropped only after the merge had kept the
best `limit`, so nearer messages outside the dates took the places of matches inside them.

## Decision

1. A search with a query caps its `limit` at `config::hybrid::SEARCH_LIMIT_MAX`, 512
   (`VEC_K_MAX / (CANDIDATE_MULTIPLIER x FIELD_VEC_MULTIPLIER)`), and logs when it does
   (`cap_search_limit`, email and memory). At 512 every search, with a field or without, asks
   sqlite-vec for at most 4,096 vectors, so it is fully hybrid at any `limit` asked (owner
   2026-10-21: "even if requested like 4,000, it should cap itself to a reasonable outcome which
   does not fall back to keyword only"; one cap for all searches; narrow a search by dates rather
   than ask for many results). A memory listing (no query) is not capped: it reads no vectors.
   The callers will ask for 100 once thunderbird#150 and voice#245 land (2,000 today).
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
5. When no vector candidate is left within the dates, an email or memory search answers by keyword
   only, as it does with an empty vector table (owner 2026-10-21): every keyword match within the
   dates, ranked by keyword. Merged with no vectors, the keyword scores (weight 0.3) fell under
   `MIN_SCORE`, so weak keyword matches were dropped for no better ones.

## Consequences

- Thunderbird's email search (limit 2,000) and Voice's memory search (2,000) are hybrid, like every
  search at a smaller limit already was: ranked by the combined score, and results under `MIN_SCORE`
  (a weak keyword-only match among them) left out. So a search can return fewer results than its
  keyword-only answer gave, and at most 512. Both callers take any count up to `limit`.
  Thunderbird keeps the helper's order for a relevance sort and sorts by date otherwise.
- Known limitation, approved by the owner 2026-10-21, fixed next in the train (#26): the date filter
  and the field filter run on sqlite-vec's nearest k over the whole archive. When k or more messages
  outside the dates (or the field's matches) are nearer the query, a match inside them by meaning
  alone is missed; one with the query's words is still found by the keyword leg when it scores over
  `MIN_SCORE`. sqlite-vec 0.1.9 vec0 metadata
  columns accept range constraints in a KNN query, so the date can move into the vector table; #26
  starts with a study measuring that.

## Rejected

- Clamping k to 4,096 inside `search_vec_candidates` while letting `limit` grow (the first version
  of this fix): a search past 1,024 (512 with a field) stays hybrid but asks for fewer vector
  candidates per result than designed. With the cap the clamp can never act, so it is deleted.
- Capping only at the callers: every caller (Thunderbird, Voice, any later one) would have to know
  sqlite-vec's maximum. The helper is the one place that knows it; the callers lower their limits
  as well.
- Capping plain searches at 1,024 and field searches at 512: two numbers and a branch for limits no
  caller needs (owner 2026-10-21).
- Failing the search when the vector leg fails: a keyword-only answer is still an answer, and with
  the cap the error is rare (a negative `limit`, a broken vector table). It now logs a warning
  instead of failing silently. (A rebuild empties the vector table, which answers no rows, not an
  error: that goes through the fallback for no vector candidates.)

## Evidence

Each test below was checked against a mutated build. "Red" means it fails there.

- `sqlite_vec_answers_at_most_vec_k_max` (unit, no model: 4,106 vectors written directly): sqlite-vec
  answers k = 4,095 and 4,096 and refuses 4,097; a search at `SEARCH_LIMIT_MAX` with a field asks
  for no more than 4,096. Red with the cap at 1,024.
- `search_returns_at_most_search_limit_max_results` and
  `memory_search_returns_at_most_search_limit_max_results` (unit, no model: 588 keyword matches):
  limits 10, 512, 513 and 4,000 return 10, 512, 512, 512; a memory listing returns the 588 it asks
  for. Red with no cap (email and memory each).
- `search_with_a_large_limit_finds_a_match_by_meaning` (unit, model loaded): limits 10, 512, 513,
  2,000 and 4,000, plain and with `subject:`, all find the match by meaning alone; the plain search
  also finds a match by meaning whose subject is another, and the `subject:` search leaves it out.
  Red with no cap, with the cap at 1,024, and with the field filter skipped past limit 10. `memory_search_with_a_large_limit_finds_a_match_by_meaning` does the
  same for memory; red with no memory cap.
- `search_with_no_vector_within_the_dates_answers_by_keyword` and
  `memory_search_with_no_vector_within_the_dates_answers_by_keyword` (unit, model loaded: 20
  keyword matches within the dates without vectors, 5 messages with vectors outside them): all 20
  returned. Red with the fallback checked before the date filter (none returned).
- `vec_candidates_within_dates_keeps_those_dated_within_them` (unit, no model): both bounds, each
  alone, each inclusive, order kept, a candidate with no metadata dropped, everything kept without
  dates. Red with either bound exclusive and with the filter only when both are given.
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
- `test_searches_at_the_callers_limit_are_hybrid` (process, model loaded): email search at limits
  2,000 and 4,000, one at 2,000 with dates, `subject:` searches at 10 and 4,000, and memory
  searches at 2,000 and 4,000 each find the match by meaning alone; the dated one leaves out the
  nearer message outside the dates, and the `subject:` ones the match by meaning in another
  subject. Red with no k limit (the first version's evidence) and with the field filter skipped
  past limit 10.
