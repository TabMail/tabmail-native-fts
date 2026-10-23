# ADR-NF-017: A Search Sorts Before It Takes Its Limit

**Status:** Accepted (2026-10-22, issue #46). Amends ADR-NF-013 (Thunderbird's own date sort) and
ADR-NF-014 (the email keyword-only answer's order). The iOS app makes the same change in its own
index (tabmail-ios#265); the callers then pass `sort` instead of sorting (tabmail-thunderbird#150,
tabmail-ios#264).

## Context

`search` returned its best `limit` matches by the combined score, and the caller sorted them by date
(Thunderbird `email_search`, iOS `EmailSearchTool`, default newest first). So a date-sorted search
showed the newest of the best matches, not the newest match. With more matches than the limit, the
newest or oldest one was often missing. A query of only a field (`from:boss@example.com`) has nothing
for the vector leg to mean (it compares the text `from:...` with each email), so which of the
sender's emails were kept was close to random.

## Decision (owner 2026-10-22)

1. `search` takes `sort`: `relevance` (absent or null), `date_desc` or `date_asc`. Any other value is
   refused with an error, so a misspelt sort is not answered in another order (`SearchSort`).
2. The helper sorts, then takes `limit`. Relevance: the combined score, ties newer first. A date sort:
   the date, ties the better match first, in both directions. Equal in both: rowid order, so a search
   always answers in one order. The order is in the shared `merge_results`, which now takes each
   candidate's date: keyword candidates carry theirs, vector-only ones are read in one `json_each`
   query (`fetch_dates`). A candidate without a date (its row is gone) is left out, as its row could
   not be answered anyway.
3. What counts as a match is the same in every sort: what the relevance search keeps (`MIN_SCORE`,
   or the keyword-only answer). A date sort takes it from a larger pool, `DATE_SORT_POOL` (4,096,
   sqlite-vec's maximum k) candidates per leg instead of `limit` x `CANDIDATE_MULTIPLIER`, and sorts
   every kept candidate by date.
4. A query of only field terms (`from:`, `to:`, `cc:`, `bcc:`, `subject:`, `body:`, with operators
   between them and no free word, phrase or group: `query_is_field_only`) skips the vector leg and
   answers by keyword over every match, in sort order. For relevance its matches are treated as tied
   (a field names which messages, not how well they match: their keyword scores, from shard
   statistics, field length and how often the value is in it, are no measure of relevance): newest
   first. So "the latest from X" is exact under the default sort, and "the oldest from X" with
   `date_asc`.
5. Every keyword-only answer (no model, no vector candidate within the dates or the field, an empty
   merge, a query of only fields) orders every match by the sort in SQL before its `LIMIT`:
   relevance `rank ASC, dateMs DESC`; `date_desc` `dateMs DESC, rank ASC`; `date_asc`
   `dateMs ASC, rank ASC`. This changes the email relevance answer, which was newest first
   (ADR-NF-014), to the best keyword match first, as memory's already was.
6. Memory search has no `sort`: its merge breaks ties newer first too.

## Study (a real Thunderbird archive, 2026-10-22)

A copy of a real Thunderbird index (300,357 messages, 17 year shards), read by an uncommitted
harness; only aggregate numbers were kept and the copy was deleted. 69 queries: 24 generic phrases and
45 words from the index's own vocabulary by document frequency (15 each at 2-20%, 0.2-2% and
0.02-0.2% of messages). No dates, the last 365 days and the last 30 days; newest and oldest first. The
truth is the date order of every match (every keyword match and every vector within the dates,
merged). "Right" means the first result is the truly newest (oldest) match; recall is the share of the
true first 100 returned. "Today" is the relevance search's best 100 sorted by date.

| Dates | Queries | today | 1,024 | 2,048 | 4,096 |
|---|---|---|---|---|---|
| last 30 days | all sets | 67-100% right | 100% | 100% | 100% |
| last 365 days | generic phrases | 38% right, recall 0.41 | 83%, 0.91 | 92%, 0.95 | 100%, 1.00 |
| last 365 days | words in 2-20% | 20%, 0.16 | 80%, 0.83 | 100%, 0.94 | 100%, 0.95 |
| no dates | words in 0.2-2% | 13%, 0.15 | 80%, 0.71 | 93%, 0.87 | 100%, 0.99 |
| no dates | generic phrases | 4%, 0.07 | 46%, 0.40 | 50%, 0.57 | 63%, 0.72 |
| no dates | words in 2-20% | 7%, 0.05 | 20%, 0.22 | 27%, 0.31 | 33%, 0.45 |

Newest first; oldest first is similar or better; rare words are right at every pool. Median time per
search is about the same at every pool: 200 to 300 ms with dates, 1.1 to 1.9 s without (today's
relevance search takes 1.1 to 1.4 s without dates on this archive). The p95 of undated common-word
searches is noisy (4 to 14 s over 15 queries).

4,096 is right in every dated set and best without dates, at no measurable median cost over 2,048.

## Consequences

- A date-sorted search returns the newest (oldest) matches, exactly whenever a leg's matches fit in
  the pool. A generic phrase or common word without dates has thousands of matches, more than any
  pool: the pool's matches are sorted. The `email_search` tool description says so and tells the
  agent to narrow such a search with dates (owner: "dates can narrow").
- A date sort reads up to 4,096 candidates per leg and their dates. Median cost about equal to a
  relevance search on the measured archive.
- A field-only search no longer finds a match by meaning alone (there is no meaning to match).
- Callers that still sort after the fetch keep working: without `sort` they get the relevance order,
  as before (with ties now newer first).

## Rejected

- Sorting only the best `limit` (the old behaviour): the newest match is usually missing (table).
- A pool of 1,024 or 2,048: worse in dated and undated sets, no cheaper at the median.
- A date sort over every keyword match with no vector leg: loses matches by meaning, which the
  relevance search keeps, so the same query would find different matches in different sorts.
- A silent default for an unknown `sort`: a misspelling would answer in an order the caller did not
  ask for.

## Evidence

Unit tests (model loaded where named):

- `hybrid::tests::relevance_ties_go_newer_first`, `date_sorts_order_by_date_and_ties_better_first`,
  `equal_score_and_date_come_in_rowid_order`, `a_candidate_without_a_date_is_left_out`,
  `sort_is_one_of_three_names`.
- `db::tests::keyword_search_sorts_every_match_before_the_limit` (no model: the three orders, an exact
  rank tie, a date tie), `search_refuses_an_unknown_sort`, `a_query_of_only_fields_is_field_only`.
- `a_search_of_only_fields_sorts_every_match_by_date` (with and without the model: the newest and
  oldest from a sender score worst, by a longer From, and are returned first),
  `a_date_sorted_search_finds_the_newest_and_oldest_match` (model: weak newest and oldest matches
  outside the best 3 are first), `a_date_sorted_search_orders_a_match_by_meaning_by_its_date`,
  `a_hybrid_search_puts_equal_matches_newer_first`, `memory_search_puts_equal_matches_newer_first`,
  `memory_search_puts_the_best_match_before_a_newer_weaker_one` (red with memory's merge by date);
  the date sort through each keyword fallback in `search_whose_merge_keeps_nothing_answers_by_keyword`
  and `search_with_no_vector_within_the_dates_answers_by_keyword`.
- `a_date_sort_reads_4096_keyword_candidates` and `a_date_sort_reads_4096_vector_candidates` (model:
  exactly 4,096 matches in one leg, the newest or oldest the worst of them, found first in both
  directions; red with a pool of 4,095, or with either leg alone at 2,048).
- `a_search_whose_date_lookup_fails_fails_and_the_next_is_answered` (model: a refused date lookup
  fails the email and the memory search, ends the read transaction, and the next search answers;
  red when either caller swallows the error).
