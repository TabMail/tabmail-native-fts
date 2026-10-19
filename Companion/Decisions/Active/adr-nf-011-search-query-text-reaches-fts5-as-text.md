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

1. The query is scanned into words (no whitespace, no `"`) and `"phrases"` (an unclosed quote runs
   to the end), on `&str` so text stays whole.
2. Every word and phrase is written as an FTS5 string (`"word"`, with the prefix `*` rules as
   before: a user `*`, a word with punctuation, or a plain word of 4+ bytes when there are no
   synonym groups). No query text is ever FTS5 syntax. A word or phrase with no letter or digit is
   dropped (an empty string inside an AND matches nothing).
3. A word expanding to synonyms becomes the generated `(a OR b …)` group, the only operators in
   the result; when any group is present every term is joined with an explicit `AND`.
4. Column scoping is the caller's list: `build_fts_match(q, columns, …)`. Mail search passes
   `QUERY_COLUMNS` (the documented `subject from to cc bcc body`, plus `from_ to_`), matched
   case-insensitively and written as the shard column; `name:word`, `name:"phrase"` and
   `name: word` scope. Every listed column is in `COLUMN_SCOPE_PREFIXES`, so a scoped hybrid
   search still filters its vector candidates. Memory search passes none: there `from:` is text.
   `from :` (space before the colon) no longer scopes.

## Rationale

- Quoting everything is the one rule that makes every input valid; the synonym groups are
  generated from a fixed word list, so they are the only syntax left to trust.
- Operators typed by a user or model were never documented (the tool descriptions offer plain
  language, `"phrases"` and the six fields), and as passed through they failed more often than
  they worked; searched as words they behave like their lowercase forms always did.

## Consequences

- Tests: `db::tests::test_search_reads_any_query_text_as_text` and
  `memory_db::tests::memory_search_reads_any_query_text_as_text` run the queries above through
  `search` / `memory_search` on real tables (red on the old builder: 16 errors, `café` empty, the
  phrase-and-synonym query an error); `query::tests` pin the written expressions;
  `db::tests::test_every_query_field_reaches_the_column_scope_filter` ties the field list to the
  hybrid filter.
- `role:` / `content:` no longer scope in memory search (undocumented, unused).
