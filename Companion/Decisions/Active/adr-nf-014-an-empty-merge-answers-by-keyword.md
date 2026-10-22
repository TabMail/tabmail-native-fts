# ADR-NF-014: A Hybrid Search Whose Merge Keeps Nothing Answers by Keyword

**Status:** Accepted (2026-10-21, issue #28). Fixes the second known limitation of ADR-NF-013. The
iOS app makes the same change in step (tabmail-ios#240; `HybridMerge` mirrors `hybrid::merge_results`).

## Context

A hybrid search (email `search`, `memorySearch`) gives each candidate
`0.7 x vector score + 0.3 x keyword score` and keeps those at `MIN_SCORE` (0.1) or above. The
keyword score is bm25 turned into `r / (1 + r)`, an absolute number. SQLite FTS5 clamps the IDF of
a term found in at least half the rows (of an email year shard, or of the memory table) to about 0
(`nHit >= nRow / 2` in `fts5_aux.c`, SQLite 3.46.0 bundled by libsqlite3-sys 0.30.1),
so such a word scores about 0 by keyword in every row. A vector score under
`VECTOR_SCORE_THRESHOLD` (0.45) counts as 0, and a one-word query's vector matches are often that
weak. Then every merged score falls under `MIN_SCORE` and the search returns nothing, while the
keyword leg found every row with the word (review of #25: 600 rows with "budget", the query
`budget` returned 0).

## Decision

When the merge keeps nothing and the keyword leg found matches, the search answers by keyword only,
as it already does with an empty vector table and with no vector candidate within the dates
(ADR-NF-013 item 5): `search_fts_only` for email (newest first, as before), `memory_search_fts_only`
for memory (best keyword match first). It logs that it did. Nothing else changes: whenever the
merge keeps at least one result, that result list is the answer, scored as before (owner
2026-10-21, after the study below: "do the recommendations. Great how it works right now").

## Study (owner's real databases, 2026-10-21)

Copies of the owner's Thunderbird and iPhone databases, read by an uncommitted harness that ran
production's candidate queries and merge next to the alternatives. Only aggregate numbers were kept;
the copies were deleted afterwards. 100 results, 400 candidates per leg. Query sets: the most common
words, words in 5 to 20% of rows, rare words, generic phrases, and known items (the subject of a
random message, or six consecutive words of a random memory entry; a hit is that message's subject,
or that entry). Methods: current (`merge_results`), keyword rank alone, weighted reciprocal-rank
fusion (k = 60, with and without the vector threshold), per-query min-max normalisation, and
query-length gating (one word: keyword only).

Known-item MRR@10, with the number of the known-item queries whose item is in the top 10 in
brackets (out of 150, 25 for Voice; queries the keyword leg matched at all: 91, 81, 132, 133, 21).
RRF is weighted reciprocal-rank fusion with the vector threshold; "RRF, all" is without it.

| Database | current | keyword only | RRF | RRF, all | min-max |
|---|---|---|---|---|---|
| Thunderbird email, about 300,000 messages | **0.833** (137) | 0.444 (77) | 0.790 (132) | 0.790 (132) | 0.808 (133) |
| iPhone email, about 250,000 messages | **0.673** (117) | 0.405 (67) | 0.652 (112) | 0.654 (112) | 0.648 (111) |
| Thunderbird memory, about 1,300 sessions | 0.546 (103) | **0.550** (100) | 0.478 (94) | 0.401 (80) | 0.416 (86) |
| iPhone memory, about 800 turns | **0.912** (143) | 0.862 (133) | 0.820 (139) | 0.799 (134) | 0.797 (135) |
| Voice memory, 25 turns | 0.820 (22) | 0.780 (21) | 0.790 (22) | **0.887** (25) | 0.784 (24) |

- Current scoring is best in three databases and within 0.004 of keyword-only on Thunderbird memory.
  On the 25-turn Voice database "RRF, all" scores higher: without a threshold it returns all 25
  turns for every query, so the known item is always somewhere in the page, while 87% of its top 10
  are turns without the query's words. RRF without the threshold and min-max return a full page for
  every query and fill 90 to 97% of the top 10 for a rare word with rows that do not contain it.
  Gating one-word queries changes nothing the guard does not.
- Empty answers with keyword matches, current scoring: none in the two email databases (no sampled
  word is in half of a year shard); Thunderbird memory 5 of 15 common words; Voice memory 12 of 15
  (with 25 rows nearly every word is in at least half). With the guard: none anywhere, and every
  other number unchanged.

## Consequences

- A search for a common word returns its keyword matches instead of nothing. A small or new memory
  database, where most words are in at least half the rows, meets this most.
- Such an answer is ranked by keyword (email: newest first), with no match by meaning alone among
  it, as with an empty vector table.

## Rejected

- Weighted reciprocal-rank fusion, per-query min-max normalisation, query-length gating: each scored
  under the current merge in four of the five databases, and RRF without the threshold and min-max
  put mostly non-matching rows first; the one higher score (RRF without the threshold on 25 Voice
  turns) comes from returning every row for every query.
- Lowering `MIN_SCORE` or raising the keyword weight: either changes every search's ranking to fix
  the case where the merge keeps nothing; the study shows the ranking is right when it keeps
  something.

## Evidence

- `search_whose_merge_keeps_nothing_answers_by_keyword` (unit, model loaded: 30 messages with
  "budget" an hour apart and 10 without, all with vectors): the query `budget` returns the 30; within
  dates covering 10 of them, those 10; with limit 5, the newest 5, newest first. Red without the guard
  (none returned), with the fallback ignoring the dates, and with it returning 4 times the limit.
- `memory_search_whose_merge_keeps_nothing_answers_by_keyword` (unit, model loaded: 30 turns naming
  "budget" once in a long sentence, an hour apart, and 10 without): the same three searches and the
  same three reds.
- `search_whose_merge_keeps_results_answers_by_meaning_too` and
  `memory_search_whose_merge_keeps_results_answers_by_meaning_too` (unit, model loaded: one row with
  the words of `plan the vacation itinerary`, one about a vacation itinerary without "plan"): both are
  returned. Red with the guard firing whenever the keyword leg matched (only the row with the words).
