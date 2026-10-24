# ADR-NF-019: A Search That Excludes Answers by Keyword; an OR Filters Each Alternative

**Status:** Accepted (2026-10-24, issue #21). The iOS app follows with tabmail-ios#301.

## Context

`build_fts_match` keeps a capital NOT between two terms as the FTS5 operator (ADR-NF-011), so
`lunch NOT from:erin@example.com` and `vacation NOT itinerary` exclude by keyword. With the
embedding model loaded, the search is hybrid, and the vector leg cannot exclude:

- An email search with a field built its vector filter (`extract_column_scope_filter`) from the
  field terms alone, joined with a space, dropping the NOT. For `lunch NOT from:erin@example.com`
  the vector leg searched only erin's mail and returned it (issue #21, in 0.11.8 too).
- A NOT before a word, a phrase or a group (email and memory) never reached the vector leg, which
  returned the nearest by meaning, the excluded mail and entries among them.
- The same filter joined every field with AND, ignoring OR: `from:a OR from:b plan the trip` let the
  vector leg search only mail from both a and b (none), and `subject:launch OR plan the trip` only
  mail with "launch" in its subject, so a match by meaning for the other alternative was lost
  (recall only; issue #21's comments).

## Decision

1. A search whose processed FTS5 query keeps a NOT (`query_excludes`: a bare `NOT` token, outside
   quotes and groups, in `split_column_scope`'s scan) answers by keyword only, as it does without
   the model: email `search` (`search_fts_only`, in the sort asked for) and `memorySearch`
   (`memory_search_fts_only`). A query of only fields still takes the field-only path first
   (ADR-NF-017), which is by keyword too.
2. A NOT that `build_fts_match` drops (first, last, after another operator), quoted, lower-case or
   a field's value excludes nothing and changes nothing.
3. The vector filter (`extract_column_scope_filter`) is the rows the query's fields allow, its free
   terms matching anything. `split_column_scope` splits the fields at each OR outside a group
   (FTS5's OR binds loosest); each alternative keeps the AND of its own fields, and the
   alternatives are joined with OR (`from_:"a" OR from_:"b"`). An alternative with no field allows
   every row, so there is no filter, as for a query without fields. A query with no OR gets the
   filter it had.

## Consequences

- A search never returns mail or an entry its query excludes.
- A search with an OR finds by meaning within what each alternative allows.
- A search with a NOT finds no match by meaning alone. NOT is reached only by a capital NOT between
  two terms, which no tool description documents; models write it Gmail-style.
- The vector leg is skipped for such a search, so it costs no embedding.

## Rejected

- Carrying the NOT into the field filter: FTS5's NOT is binary, so `NOT from_:"erin@example.com"`
  alone is no query, and the eligible set would be every message but the excluded sender's (about
  300,000 rowids as one JSON list on the owner's archive). It also leaves a NOT before a word.
- Dropping the vector candidates that match an excluded term after the vector search: fewer than
  `k` remain, by as many as the exclusion removes, and it needs a MATCH per excluded term for each
  search.
- Excluding only a negated field (the issue's narrower fix): the NOT before a word returns excluded
  mail the same way.

## Evidence

Each test was checked against a mutated build ("red" = fails there).

- `search_excluding_a_sender_or_a_word_returns_none_of_it` (unit, model loaded): `lunch NOT
  from:erin@example.com` (and frank's, and a longer query) returns only the other sender's lunch;
  `vacation NOT itinerary` only the message without "itinerary"; `vacation NOT from:erin@example.com`
  nothing; an AND or OR after the NOT (`lunch NOT from:erin@example.com AND lunch`, `... OR dinner`)
  still excludes; each in relevance, date_desc and date_asc order. Red on main (599b8eb: erin's mail for
  `NOT from:erin`), with the email search ignoring the NOT, and with only a NOT before a field
  counted (red at `vacation NOT itinerary`).
- `memory_search_excluding_a_word_returns_none_of_it` (unit, model loaded): `vacation NOT itinerary`
  returns only the entry without "itinerary", and so do `... AND summer` and `... OR summer`. Red on
  main, with the memory search ignoring the NOT, and with the scan forgetting a NOT at a later AND or
  OR (review r1).
- `search_with_an_or_finds_by_meaning_within_what_each_alternative_allows` (unit, model loaded: two
  matches by meaning alone, from b and from c): `from:a@example.com OR from:b@example.com plan the
  vacation itinerary` returns b's only; `subject:launch OR plan the vacation itinerary` both. Red with
  the fields not split at OR (main's filter), with an alternative without a field still filtered, and
  with the alternatives joined by AND.
- `test_extract_column_scope_filter_keeps_each_alternative_between_ors` (unit): the filters above,
  explicit AND, and an OR inside a group. Red under the same three mutations.
- `a_query_that_keeps_a_not_excludes` (unit): a kept NOT before a field, a word, a phrase, a
  synonym group or a spelling group excludes; a dropped, quoted, lower-case NOT or a field's value
  does not. Red with `query_excludes` always false, with any `NOT` substring counted, and with any
  operator counted.
