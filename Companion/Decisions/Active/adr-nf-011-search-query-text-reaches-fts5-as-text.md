# ADR-NF-011: Search Query Text Reaches FTS5 as Text

**Status:** Accepted (2026-10-18). Callers: Thunderbird (email search tool, memory search tool,
Chat History search), TabMail Voice (memory search). No wire change.

## Context

`fts::query::build_fts_match` turns the query a user typed or a model wrote into an FTS5 MATCH
expression. It put most words in bare, quoting only words with `- @ : + .`, so:

- Any other punctuation FTS5 reads as syntax failed the whole search with an `fts5: syntax error`:
  `50% off`, `hello!`, `a/b`, `#launch`, `$100`, `budget, plan`, `R&D`, `(budget`, `budget)`.
- `AND`, `OR`, `NOT` typed in capitals were passed through as operators, and failed where FTS5
  does not accept them (`NOT budget`, `invoice OR`, `AND`).
- Any `word:` was read as a column filter: `note: launch` or `re:budget` failed with
  `no such column`. Memory search applied the mail field aliases too (`from:` became `from_:`),
  which the memory table does not have.
- The alias and `field:"value"` scanners copied the query byte by byte into chars, so non-ASCII
  text was corrupted: `café` searched for `cafÃ©` and found nothing.
- FTS5 rejects an implicit AND beside a parenthesised group, so a quoted phrase next to a word
  with synonyms (`meeting "budget plan"`) was a syntax error.

In Thunderbird a failed search reaches the chat tools as an error, and Chat History shows no results.

## Decision

1. A NUL in the query becomes a space (FTS5 reads a NUL as the end of the query, leaving a string
   open; the tokenizer treats it as a separator). The query is then scanned into words (no
   whitespace, no `"`) and `"phrases"` (an unclosed quote runs to the end), on `&str` so text stays
   whole.
2. Every word and phrase is written as an FTS5 string (`"word"`, with a prefix `*` for a user `*`,
   a word with punctuation, or a plain word of 4+ bytes when there are no synonym groups in the
   query; the old builder decided the last per quote-delimited segment, now it is query-wide). Apart from
   the operators of point 4, no query text is ever FTS5 syntax. A word or phrase with no letter or digit is dropped (an empty
   string inside an AND matches nothing).
3. A word expanding to synonyms becomes the generated `(a OR b …)` group; when any group is
   present every term is joined with an explicit `AND`. Whether a
   group is present is read from the terms as written (only a group starts with `(`), so it can't
   disagree with how fields took their values (a separate pre-scan did, for runs of empty fields
   like `from: to: meeting`).
4. A capital `AND`, `OR` or `NOT` (FTS5's operators are case-sensitive) standing between two
   written terms stays that operator, in place of the join: `from:"a@example.com" OR
   to:"a@example.com"`, `"budget plan" OR greetings`, `alice NOT carol`. One that is first, last,
   after another operator, or left with no written term after it is dropped (`NOT budget` searches
   `budget`), so it can never be a syntax error. Lowercase, quoted or a field's value, it is text.
   The hybrid column filter keeps joining the scoped terms with a space, as before.
5. Column scoping is the caller's list: `build_fts_match(q, columns, …)`. Mail search passes
   `QUERY_COLUMNS` (the documented `subject from to cc bcc body`, plus `from_ to_`), matched
   case-insensitively and written as the shard column; `name:word`, `name:"phrase"` and
   `name: word` scope. Every listed column is in `COLUMN_SCOPE_PREFIXES`, so a scoped hybrid
   search still filters its vector candidates. Memory search passes none: there `from:` is text.
   `from :` (space before the colon) no longer scopes.

## Rationale

- Quoting everything is the one rule that makes every input valid; the synonym groups are
  generated from a fixed word list, so they are the only syntax left to trust.
- Operators were never documented (the tool descriptions offer plain language, `"phrases"` and
  the six fields), but the old builder passed them through, and a model writes Gmail-style
  `from:X OR to:X`. That worked whenever no word beside it had synonyms. The review of the first
  candidate (2026-10-21) measured it returning nothing once `OR` was searched as a word, so
  between two terms they stay operators; only the placements that made FTS5 fail are dropped.

## Consequences

- Tests: `db::tests::test_search_reads_any_query_text_as_text` and
  `memory_db::tests::memory_search_reads_any_query_text_as_text` run the queries above, runs of empty
  fields, NULs, and words in several scripts through `search` / `memory_search` on real tables (red
  on the old builder: 16 errors, `café` empty, the phrase-and-synonym query an error);
  `db::tests::test_no_query_text_makes_search_fail` and the memory test run 3,000 seeded random
  queries each (`random_queries`), asserting none fails; `query::tests` pin the written
  expressions; `db::tests::test_each_documented_field_scopes_to_its_column` checks each documented
  field against a list written in the test, by a two-sided search and in the hybrid filter;
  `db::tests::test_search_operators_separators_and_text_less_words` and its memory twin pin the
  operators both ways, tabs / no-break / ideographic spaces as separators, and that a word with no
  letter or digit beside a synonym group hides no match; `test_rust_process_parity` `test_scoped_search_with_embeddings_finds_only_the_field` checks the
  same with the embedding model loaded.
- `role:` / `content:` no longer scope in memory search, and `msgId:` / `attachmentNames:` no longer
  scope in mail search (all undocumented and unused by any client).
- Both behaviour changes (`from :` with a space before the colon no longer scopes; memory search has
  no fields) were approved by the owner on 2026-10-18.
