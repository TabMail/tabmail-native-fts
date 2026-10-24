# ADR-NF-020: Every Keyword Match Is a Hybrid Result

**Status:** Accepted (2026-10-24, issue #51; release blocker for native-fts 0.12.0). Supersedes the
fallback of ADR-NF-014. The iOS app makes the same change (paired tabmail-ios issue; `HybridMerge`
mirrors `hybrid::merge_results`).

## Context

A hybrid search (email `search`, `memorySearch`) gives each candidate
`0.7 x vector score + 0.3 x keyword score` and kept only those at `MIN_SCORE` (0.1) or above. FTS5
clamps the IDF of a word in at least half the rows to about 0 (ADR-NF-014), so such a word scores
about 0 by keyword in every row, and only the rows the vector leg lifted over `MIN_SCORE` were kept.
ADR-NF-014 answered by keyword when the merge kept NOTHING; when it kept one row, that one row was the
answer. The release smoke test (PLAN_RELEASE_1_10): 8 messages, each "we talked about <topic> and the
meeting notes"; `meeting` returned 1 by hybrid and 8 by keyword only, and the same for memory.

Owner invariant (2026-10-24): if hybrid search gives fewer results than keyword-only search,
something is wrong.

## Decision

1. Every keyword candidate is a result, however low it scores. `MIN_SCORE` gates only candidates found
   by meaning alone (`HybridCandidate.keyword_match`). Ranking is unchanged: the same final score, the
   same sort and tie-breaks (ADR-NF-017).
2. The ADR-NF-014 fallback (an empty merge with keyword matches answers by keyword) is deleted: the
   merge cannot be empty while the keyword leg found something. The other keyword-only paths stay: no
   vector candidate (empty vector table, none within the dates or the field), a field-only query
   (ADR-NF-017), a query that keeps a `NOT` (ADR-NF-019).
3. The keyword candidate pools (`search_fts_candidates`, `memory_search_fts_candidates`) order equal
   ranks newer first (`ORDER BY rank ASC, dateMs DESC`), as the keyword-only paths do. With every
   candidate kept, the pool decides which tied matches are answered; without it FTS5's own order
   (rowid) picked them.

Result: a hybrid search returns at least as many rows as the same search by keyword only, and every
keyword-only result when the limit has room for them (the candidate pool is `limit` x
`CANDIDATE_MULTIPLIER`, or `DATE_SORT_POOL` for a date sort). Within the limit, a match by meaning
may still rank above a keyword match, as before.

## Rejected

- Running hybrid and keyword-only searches in parallel and answering with the larger set (owner's
  alternative): two rankings for one query, and the choice flips with the counts; the merge already
  holds every keyword candidate.
- A threshold relative to the best score: it still drops keyword matches whenever one row scores well
  by meaning, the case this issue is about.
- Keeping the ADR-NF-014 fallback as well: unreachable once no keyword match is dropped.

## Consequences

- A common word returns its keyword matches, ranked by meaning among them (they tie by keyword), and
  then the newer first.
- Identical rows tied by keyword are ranked by the vector leg's support; sqlite-vec returns any `k` of
  equal distances, so which tied rows come first by relevance is not pinned (date sorts are).

## Evidence

- `search_returns_every_keyword_match_of_a_word_in_most_messages`,
  `memory_search_returns_every_keyword_match_of_a_word_in_most_entries` (unit, model loaded, the smoke
  test's rows): for `meeting`, `notes`, `talked` in three sorts, hybrid count >= keyword-only count, and
  a superset with the limit having room (all, dated, limit 3). Red on main (1 of 8).
- `search_ranks_keyword_matches_scoring_about_0_after_a_match_by_meaning` and its memory twin (were
  `..._keeps_one_result_answers_by_it`): the match by meaning first, then the 30 keyword matches.
- `every_keyword_match_is_kept_and_meaning_alone_needs_min_score` (hybrid unit): keyword candidates at
  about 0 kept, a weak vector-only one dropped, a strong one kept.
- `keyword_candidates_of_equal_rank_are_the_newest` and its memory twin: 12 tied rows indexed oldest
  first, a pool of 4 holds the 4 newest.
- `search_returns_a_keyword_match_that_scores_0` and its memory twin: a query matching only the
  msgId / memId (columns weighted 0, rank exactly 0) is a result with the model loaded.
- `memory_search_keeps_the_best_keyword_match_over_newer_weaker_ones_at_a_small_limit`: 13 matches,
  the best one older, limit 1: the pool takes rank before date, so the best match is answered.
- Mutants (ledger `PLAN_NF_COMMON_WORD/reviews/mutation-ledger.jsonl`): keyword gated by `MIN_SCORE`,
  vector-only ungated, the vector leg clearing the flag, a vector-only candidate counted as keyword,
  each pool without the date tie-break: all detected.
