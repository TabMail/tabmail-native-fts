# ADR-NF-023: A Date-Sorted Search Walks Date Windows

**Status:** Accepted (2026-10-28, issue #47). Amends ADR-NF-017 (the date sort's single pool). The iOS app
ports the same rules in its own index (tabmail-ios#294).

## Context

A date-sorted search (`search` with `sort` `date_desc` or `date_asc`, ADR-NF-017) took 4,096 candidates per
leg (`DATE_SORT_POOL`, sqlite-vec's maximum k) and sorted them by date: the keyword leg's best 4,096 by
rank, the vector leg's nearest 4,096. With more matches than that in a leg, the pool's matches were sorted,
not all of them. The #46 study found the first result was the truly newest match for 63% of generic
phrases and 33% of common words without dates. The `email_search` tool description asked the agent to
narrow such searches by dates.

## Decision

1. A date sort walks the search's dates in windows, from the end the sort starts at (the newest for
   `date_desc`, the oldest for `date_asc`). Every match in a window comes before every match in the next,
   so the walk stops once `limit` matches are found. Relevance is unchanged (one pool).
2. What counts as a match is unchanged: every keyword match, and a match by meaning alone at `MIN_SCORE`
   (ADR-NF-020). A window is merged only when both legs hold all of its candidates:
   - Keyword: the next `DATE_SORT_POOL` keyword matches in sort order (date, then rank), no longer the
     best by rank. The window ends where they end; the matches at their last date are left to the next
     window, since they may go on past the pool. So a window never splits a date.
   - Vector: sqlite-vec's `DATE_SORT_POOL` nearest within the window (`rowid IN`, ADR-NF-016) hold every
     match by meaning alone in it: fewer were answered, or the farthest answered scores under
     `MIN_SCORE` by meaning alone. Otherwise every one of them is a match, and every vector's distance
     within the window is read in one pass over its rows (`vec_distance_cosine` by rowid, ADR-NF-016's
     method D: slower than a search past tens of thousands of rows, but not bounded by its k). More
     than the limit match, so the walk ends there.
3. Scores do not depend on the window. bm25 uses its year shard's statistics whatever dates the query
   names. A vector-only candidate counts only at `MIN_SCORE`, and the pool holds every such one. A
   keyword match counts its meaning however weak: when a full pool's farthest scores above
   `VECTOR_SCORE_THRESHOLD`, the window's keyword matches missing from the pool are read for their own
   vector distances (one `rowid IN` search over at most a pool of rows); a window read in one pass
   holds every vector's own distance. So every candidate's score is its own.
4. Fallbacks as before: no vector within the dates (or the field) answers by keyword only. When the
   windows walked hold no vector, one k = 1 vector search over the whole dates decides it. A failed
   vector search also answers by keyword only.
5. More than a pool of keyword matches at one millisecond cannot be split: the search sorts that pool
   and logs a warning. Not seen in real mail. (A pool of vectors at one millisecond is read whole.)

Windows are not year shards: the keyword pool and the vector pool choose them, so a window holds about a
pool of matches whatever the year. Typically one window: the newest 4,096 keyword matches, and a vector
pool that is complete within their dates.

## Study (a real Thunderbird archive, 2026-10-28)

A copy of a real Thunderbird index (300,783 messages, 17 year shards), read by an uncommitted harness
through the production read-only opener and `search`. Only aggregate numbers were kept; the copy is
deleted. The #46 query sets (24 generic phrases; 15 index words in 2-20% and 15 in 0.2-2% of messages),
and 15 words each in 0.02-0.2% and 0.003-0.03% (a median of 120 and 28 matches without dates: fewer
matches than the limit), no dates and the last 365 days, limit 100. The truth is every keyword match
and every vector within the dates, merged and sorted. Base is main before this change (0.12.1), run
once; the walk ran twice. Each search was warmed once, then timed. The machine was shared (load average
5 to 15), so p95 over 15 or 24 queries (about the slowest query) is noisy.

| Dates | Queries | first right, base / walk | all 100 exact, base / walk | p50 ms, base / walk | p95 ms, base / walk |
|---|---|---|---|---|---|
| no dates | generic phrases | 62-88% / 100% | 46-50% / 100% | 1,405-1,413 / 1,345-1,418 | 3,418-3,442 / 3,233-3,421 |
| no dates | words in 2-20% | 40-60% / 100% | 0-7% / 100% | 2,360-2,367 / 2,130-2,191 | 6,876-6,897 / 5,438-8,895 |
| no dates | words in 0.2-2% | 93-100% / 100% | 87% / 100% | 720-812 / 718-747 | 1,444-1,457 / 1,443-1,474 |
| no dates | words in 0.02-0.2% | 100% / 100% | 100% / 100% | 589-594 / 590-597 | 676 / 678-719 |
| no dates | words in 0.003-0.03% | 100% / 100% | 100% / 100% | 582-583 / 579-592 | 690-691 / 690-700 |
| last 365 days | generic phrases | 100% / 100% | 96% / 100% | 315-318 / 314-337 | 557-558 / 563-576 |
| last 365 days | words in 2-20% | 93-100% / 100% | 80% / 100% | 368-389 / 361-371 | 846-885 / 745-892 |
| last 365 days | words in 0.2-2% | 100% / 100% | 100% / 100% | 265-268 / 263-273 | 337-339 / 335-345 |
| last 365 days | words in 0.02-0.2% | 100% / 100% | 100% / 100% | 257-258 / 258-260 | 267-285 / 265-289 |
| last 365 days | words in 0.003-0.03% | 100% / 100% | 100% / 100% | 255 / 256-260 | 270-278 / 267-338 |

Ranges are over `date_desc` and `date_asc` (and the walk's two runs). The 8,895 ms p95 is one query in
the first walk run, at a load average of 14; the second run's was 7,355. Relevance, unchanged, for
scale: no dates 1.2 to 1.5 s median. The first version of the walk (completeness at the threshold, see
Rejected) measured the same on this archive: its weak-neighbour band is not dense enough here to cost
it more windows.

The walk is exact in every set and costs about what today's single date-sorted query costs: medians
within about 10% either way (lower for common words without dates), p95 within the noise between
runs. So every date sort walks; there is no separate interactive path. No interactive caller sends
`sort` (Thunderbird's only caller is the agent's `email_search`; Voice does not search email), and
relevance search is unchanged.

## Consequences

- A date-sorted search answers the true newest (oldest) matches, in order, however many there are. The
  `email_search` tool's advice to narrow a common-word date sort by dates is no longer needed for
  correctness (still useful to the agent for scope).
- A date sort reads at least one keyword query and one vector search, as before. A window whose
  pool is full reads the vectors once more: its keyword matches' own distances when the pool
  reaches above the threshold, or every vector in the window in one pass when the whole pool matches
  by meaning, which ends the walk. So no window reads the vectors more than twice, and only the
  last is ever read whole (linear in its rows, at most the archive). Weak neighbours that are no
  matches cost nothing more however many there are, nor do strong ones however their dates lie.
- Wire and tools unchanged; no capability (a client cannot tell an exact sort from a pooled one, and needs
  no different request).

## Rejected

- Year shards as windows (the issue's first idea): a year holds from a few to tens of thousands of matches,
  so it still needs splitting, and a year with few matches costs a vector search for little.
- Halving windows by date: blind to where the matches are; the pool's own dates are not (not measured).
- Completeness at `VECTOR_SCORE_THRESHOLD` (the first version of this change): exact without the
  keyword distances read, but a window then holds at most a pool of vectors above the threshold, so a
  walk through many weak neighbours that are no matches, to a few matches past them, costs a window
  each 4,096 of them, each with a full vector search or several. Quadratic in the archive: on a
  synthetic 160,000-message fixture with every vector in that band, 18 s against 0.23 s for the single
  pool (found in review). This decision answers it in 0.25 s: one window, two vector searches.
- Narrowing a window full of matches by meaning to the dates of the first 1,024 of them and
  searching again (the second version): when the nearer matches lie farther from the sort's start,
  each search drops only about 3,072 of them, so the walk costs a full vector search per 3,072
  matches. On a synthetic 160,000-message fixture with every vector a match and the older nearer,
  `date_desc` took 52 searches and about 5.8 s against 0.22 s for the single pool (found in
  review). Narrowing by halves would bound it at about log2 of the window over 1,024 searches,
  still several; one pass over the window reads every distance exactly and deletes the narrowing.
- A separate interactive single-query path: the walk measured no slower, and no interactive caller sorts.

## Evidence

Unit tests, model loaded (red on 0.12.1 except the termination guard):

- `a_date_sorted_search_is_exact_past_the_keyword_pool`: 5,000 strong matches and a weak one newest
  (oldest), past the best 4,096 by keyword: the same list as a search by keyword only, and hybrid scores
  although the one vector is outside the first window.
- `a_date_sorted_search_is_exact_past_the_vector_pool`: 5,000 near vectors and a far match newest
  (oldest): the exact list; scores equal to those with the dates narrowed to one message.
- `a_date_window_holds_every_match_at_its_edge_date`: 10 matches, 4,091 at one date, 100 after; the limit
  across two windows, the edge date's best match by keyword and meaning together first, found by keyword.
- `a_date_sorted_search_scores_a_weak_meaning_past_the_vector_pool`: synthetic vectors at similarity 0.50
  (above the threshold, under `MIN_SCORE`) and a keyword match at 0.48 past them: its score counts its
  meaning, as when the dates hold only it.
- `a_date_sorted_search_is_exact_past_a_pool_of_weak_matches_by_meaning`: 5,000 matches by meaning alone,
  each only just at `MIN_SCORE`: the true newest (oldest) 10, in order.
- `a_date_sort_with_a_pool_of_matches_at_one_date_answers`: 4,100 vectors at one millisecond; terminates.
- `a_date_sort_with_a_pool_of_keyword_matches_at_one_date_answers`: 4,100 keyword matches at one
  millisecond; terminates, the older matches first in `date_asc`.
- `a_date_sort_stopping_before_the_end_with_no_vector_within_the_dates_answers_by_keyword`: the k = 1
  check over the dates finds no vector; the keyword-only answer.
- `search_whose_vector_leg_fails_answers_by_keyword` and
  `a_search_whose_date_lookup_fails_fails_and_the_next_is_answered`: with date sorts too.
- `search_with_no_vector_within_the_dates_answers_by_keyword`: a date sort with no vector within the dates
  is the keyword-only answer, scores and all.
- Mutants (the review's mutation ledger), on the final code: 19 detected (keyword pool by rank, the edge
  date kept partial or its rows not kept, a full keyword pool counted strictly, the keyword pool never
  fetched again, a millisecond skipped between windows, completeness always or loosened, no own-distance
  read, no existence check either way or its empty answer kept hybrid, merge limit per window, one
  window only, no break or no forced progress at one date, a keyword pool at one date not kept, a
  vector search failure or a date lookup failure handled otherwise); 2 equivalent (completeness at the
  threshold, the first version: the same answers at the cost under Rejected; narrowing to the pool's
  farthest date: more vector searches, the same answers).
