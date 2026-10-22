# ADR-NF-016: sqlite-vec Searches Only Within a Search's Dates and Field

**Status:** Accepted (2026-10-22, issue #26). Fixes the first known limitation of ADR-NF-013 and the
one ADR-NF-012 left open for memory. The iOS app follows in one port with tabmail-ios#242.

## Context

A hybrid search (email `search`, `memorySearch`) asked sqlite-vec for the `k` nearest vectors over
the whole archive (`limit` x `CANDIDATE_MULTIPLIER`, `FIELD_VEC_MULTIPLIER` (2) times that when an
email query names a field), then kept those within the dates (`vec_candidates_within_dates`) and,
for a field, among the field's matches. When `k` or more rows outside them were nearer the query, a
match inside them by meaning alone was missed, and when none was left the search answered by keyword
only (ADR-NF-013 item 5).

sqlite-vec 0.1.9 accepts `rowid IN (...)` in a KNN query: it reads only the listed rowids from each
chunk (a bitmap per chunk, `VEC0_IDXSTR_KIND_KNN_ROWID_IN` in `sqlite-vec.c`), so the `k` it answers
are the nearest among them. vec0 metadata columns also take range constraints in a KNN query, but
need the date stored in the vector table.

## Decision

1. With dates (either bound alone, or both, each inclusive) or a field, `search_vec_candidates` asks
   sqlite-vec for the `k` nearest among the rows of the metadata table (`message_meta`,
   `memory_meta`) dated within the dates and, for a field, among the field's matching rowids (the
   existing `fetch_eligible_rowids`, passed as a JSON list): `rowid IN (SELECT rowid FROM meta WHERE
   dateMs >= ? AND dateMs <= ? AND (? IS NULL OR rowid IN (SELECT value FROM json_each(?))))`.
   Without either, the query is unchanged. `k` is `limit` x `CANDIDATE_MULTIPLIER`, field or not.
2. `vec_candidates_within_dates`, the field filter after the vector search and
   `FIELD_VEC_MULTIPLIER` are deleted. `SEARCH_LIMIT_MAX` stays 512, the owner's one cap for every
   search (ADR-NF-013), now a number with a compile-time check that its candidates stay within
   `VEC_K_MAX`, instead of derived from the deleted multiplier (which would have made it 1,024).
3. No schema change: the vector tables, the metadata tables and their writers are as they were.

## Study (owner's Thunderbird databases, 2026-10-22)

Copies of the owner's Thunderbird `fts.db` (about 300,000 messages over 16 years) and `memory.db`
(about 1,300 entries), read by an uncommitted harness: vectors and dates only, never a message or an entry's text.
48 queries (24 generic phrases, embedded, and 24 stored vectors). The truth is the exact nearest `k`
within the dates (and field), computed in memory; a result counts when it is in range and no farther
than the k-th true match (ties). Methods: A, the old global KNN then filter; B, `rowid IN` from the
metadata table (this decision); B with an index on `dateMs`; C, a vec0 table rebuilt with `dateMs`
as a metadata column; D, `vec_distance_cosine` over the rows within the dates. Only aggregates were
kept; the copies are deleted.

Email, mean recall of the vector candidates (A / B), and median time in ms (A / B):

| Window (rows within it) | limit 50 | limit 100 | limit 512 | ms, limit 100 |
|---|---|---|---|---|
| last 7 days (about 900) | 0.002 / 1.000 | 0.002 / 1.000 | 0.006 / 1.000 | 386 / 183 |
| last 30 days (about 3,500) | 0.010 / 1.000 | 0.010 / 1.000 | 0.010 / 1.000 | 378 / 182 |
| last 365 days (about 23,000) | 0.079 / 1.000 | 0.082 / 1.000 | 0.083 / 1.000 | 387 / 214 |
| 30 days, 3 years back (about 2,800) | 0.007 / 1.000 | 0.008 / 1.000 | 0.010 / 1.000 | 388 / 188 |
| from 2 years back (about 43,000) | 0.158 / 1.000 | 0.158 / 1.000 | 0.154 / 1.000 | 391 / 237 |
| no dates (all) | 1.000 / (A) | 1.000 / (A) | 1.000 / (A) | 390 |

- With dates, A kept no vector candidate at all (the search fell to keyword only) for 37 of 48
  queries over the last 7 days at limit 50, 23 over the last 30 days, 7 over a year.
- A field (random 1% and 10% of rows standing in for a field's matches; A with its doubled `k`):
  A's recall 0.02 and 0.20 without dates, under 0.02 within the last 30 days; B 1.000 everywhere,
  median 170 to 370 ms (A 400 to 1,300).
- Memory: A's recall 0.15 to 0.31 within 1, 7 or 30 days at limits 50 and 100, 1.000 at 512 (k over
  all the entries); B 1.000, under 1 ms.
- B is faster than A: sqlite-vec still reads every chunk, but keeps the top `k` of fewer rows.
  An index on `dateMs` did not change B's times (B reads the dates in `message_meta` once per search). C was about 40% faster than B but needs every vector copied into a new table (about a minute for
  300,000 rows) and the date kept in step on every write; D is fast for narrow dates and slower than
  A for wide ones (about 43,000 rows within them).

## Consequences

- A search with dates or a field finds its nearest matches by meaning inside them, however many
  nearer rows lie outside; the keyword-only answer for "no vector within the dates" now happens only
  when no row within them has a vector.
- A dated or field search reads the metadata table's dates once (a scan without an index on
  `dateMs`), measured above as faster overall than before. Memory's dates are indexed since
  ADR-NF-015, so a dated memory search reads only the dates within its range.
- A field's matching rowids go to SQLite as one JSON list (as many as the field matches).

## Rejected

- C (`dateMs` as a vec0 metadata column): faster by about 80 ms per dated search at 300,000
  messages, but a migration copying every vector, a second copy of each date kept in step by every
  writer (index, re-embed, delete, clear), and no help for a field.
- D (`vec_distance_cosine` over the rows within the dates): reads each vector by rowid; faster than B
  for a few thousand rows within the dates, slower than A at about 43,000.
- A larger `k` for dated searches: still misses whenever more nearer rows lie outside, and runs into
  `VEC_K_MAX`.
- Raising `SEARCH_LIMIT_MAX` to 1,024 now that a field no longer doubles `k`: a change to what every
  search returns, not part of this fix.

## Evidence

Each test below was checked against a mutated build. "Red" means it fails there.

- `search_vec_candidates_searches_only_within_the_dates_and_the_field` (unit, no model: 20 vectors
  farther from the query the later they are dated, one dated far before 1970 and one far in the
  future, and a nearest one with no metadata row): without
  dates or field, the nearest of all; with both bounds, each bound alone (inclusive), a field's
  rowids, and a field with a bound, the nearest among those only; an empty field, nothing. Red with
  either bound exclusive, the dates ignored, the field ignored, a field alone or one bound alone
  taking the unfiltered query, and a missing bound standing for 0, -10^12 or now instead of no limit.
- `search_finds_a_match_by_meaning_past_nearer_messages_outside_the_dates_or_the_field` (unit, model
  loaded: one match by meaning alone inside the dates, from alice; 40 nearer messages outside the
  dates and 20 nearer ones from someone else, more than twice the vector candidates asked for, checked
  in the test): the dated search and the `from:alice` search each return the match. Red on main
  (fcae1d8), for the dates and for the field; red with the search passing no dates, or no field, to
  the vector search.
- `memory_search_finds_a_match_by_meaning_past_nearer_entries_outside_the_dates` (unit, model
  loaded): the same for memory. Red on main, and with the memory search passing no dates.
- `search_with_a_large_limit_finds_a_match_by_meaning`, `search_with_dates_keeps_the_matches_within_them`
  (ADR-NF-013) also go red under the field and the date mutants above.
- `search_whose_vector_leg_fails_answers_by_keyword` (unit, model loaded, reads of the vector table
  refused): an email search without dates or a field, with dates, and with `subject:`, and a memory
  search without and with dates, each answer by keyword with only the rows in scope, and the next
  search is answered. Red with the filtered query's or the unfiltered query's failure turned into a
  panic.
