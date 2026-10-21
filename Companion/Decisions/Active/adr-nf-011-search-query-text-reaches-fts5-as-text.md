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
- Apostrophes were deleted from every word, so `O'Brien` searched `obrien` and `don't` searched
  `dont`, while the tokenizer indexes the text as `o brien` and `don t`: such a word never found
  the mail it was written in, and found mail that spelled it without one. (A possessive worked by
  accident: `Sarah's` searched `sarahs`, which the porter stemmer reduces to `sarah`.)
- FTS5 rejects an implicit AND beside a parenthesised group, so a quoted phrase next to a word
  with synonyms (`meeting "budget plan"`) was a syntax error.

In Thunderbird a failed search reaches the chat tools as an error, and Chat History shows no results.

## Decision

1. A NUL in the query becomes a space (FTS5 reads a NUL as the end of the query, leaving a string
   open; the tokenizer treats it as a separator). The query is then scanned into words (no
   whitespace, no `"`) and `"phrases"` (an unclosed quote runs to the end), on `&str` so text stays
   whole.
2. Every word and phrase is written as an FTS5 string (`"word"`, with a prefix `*` for a user `*`,
   a word with punctuation, or a plain word of 4+ bytes when no word in its run of words between
   quoted phrases has synonyms, as before). Apart from
   the operators of point 4, no query text is ever FTS5 syntax. A word or phrase with nothing the tokenizer indexes is dropped (an
   empty string inside an AND matches nothing). That follows unicode61's rule, letters, numbers
   and private-use characters (`L* N* Co`), read from the `unicode_categories` crate (already a
   dependency of `tokenizers`): Rust's `is_alphanumeric` also counts marks and enclosed-letter
   symbols (Ⓜ, 🅿) the tokenizer does not index, and misses private-use characters it does. It is
   close, not exact: unicode61 uses Unicode 6.1 data, so a few code points classified since then
   are read differently (see Consequences). A word keeps its apostrophes, except quote marks at
   either end (`'budget'`) and one possessive `'s` (straight or typographic), which are removed:
   quoted, `O'Brien` is the phrase `o brien` the tokenizer indexed, and `John's` searches `John`,
   as 0.11.8 effectively did (only one trailing `*`, then trailing `/` and `?`, then the quote
   marks, then the possessive are removed from a word). A possessive does not expand to synonyms
   (0.11.8 searched `client's` as `clients`, which has none), so it does not take the prefix star
   off the words beside it.
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
   The hybrid column filter keeps joining the scoped terms with a space, as before, so a kept
   `NOT` is not applied to hybrid vector candidates: `budget NOT from:bob@example.com` (an FTS5
   error in 0.11.8, as `budget` has synonyms) returns bob's mail through the vector side, as
   `numbers NOT from:bob@example.com` already did in 0.11.8. Tracked as native-fts #21, to land
   before this helper is released.
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
  `from:X OR to:X`. That worked whenever no word beside it had synonyms, and returns nothing once
  `OR` is searched as a word, so between two terms they stay operators; only the placements that
  made FTS5 fail are dropped.

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
  letter or digit beside a synonym group hides no match;
  `db::tests::test_search_finds_words_with_apostrophes` and its memory twin find `O'Brien` and
  `don't forget` in the mail that has them and not in the one spelling `OBrien` / `Dont` (red
  with the apostrophe strip restored: each finds only the other row), and `Sarah's`,
  `Sarah’s invoice`, `John's flight`, `team's budget` find what they name (red without the
  possessive strip), and `'budget'` still expands;
  `db::tests::test_search_prefix_star_is_decided_per_run_of_words` and the memory twin (red with
  a query-wide star rule), including `client's notes` finding "Notesheet for the client" and not
  "Notes for the customer" (red when a possessive expands) and a field's value beside a group
  staying exact (`meeting from:alex` does not find alexandra); an operator replaces one join only
  (`café OR greetings budget`); the text-less tests include enclosed letters and
  a lone mark, and `test_search_keeps_private_use_characters` is two-sided (red with
  `is_alphanumeric`); `test_rust_process_parity` `test_scoped_search_with_embeddings_finds_only_the_field` checks the
  same with the embedding model loaded.
- A differential against the released 0.11.8 helper (Thunderbird-shaped `search` and
  `memorySearch`, with and without the embedding model, 2026-10-21) found no query that newly
  fails, and no common query shape that finds less, apart from the intended apostrophe change
  above (`O'Brien`, `don't`).
- Accepted limitations, approved by the owner on 2026-10-21 (common query shapes must find what
  0.11.8 found; a recall difference on rare input is acceptable, and synonym expansion stays):
  - Rare input: a few code points are read differently from unicode61's Unicode 6.1 data (code
    points unassigned in 6.1, which it indexes, are dropped; New Tai Lue vowel signs, letters
    today but marks in 6.1, are kept and match nothing). A `*` inside a word (`budg*et`) is part
    of the word, not a prefix marker. A field with a space before its value (`from: "jane doe"`)
    takes a phrase without starting a new run of words.
  - Pre-existing in 0.11.8, tracked separately: a kept `NOT` is not applied to hybrid vector
    candidates (#21); a query with more than 256 kept `NOT`s exceeds FTS5's expression depth and
    fails (#22).
- `role:` / `content:` no longer scope in memory search, and `msgId:` / `attachmentNames:` no longer
  scope in mail search (all undocumented and unused by any client).
- Both behaviour changes (`from :` with a space before the colon no longer scopes; memory search has
  no fields) were approved by the owner on 2026-10-18.
