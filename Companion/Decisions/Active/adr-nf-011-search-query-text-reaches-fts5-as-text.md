# ADR-NF-011: Search Query Text Reaches FTS5 as Text

**Status:** Accepted (2026-10-18; both apostrophe spellings added 2026-10-22, native-fts #31). Callers: Thunderbird (email search tool, memory search tool,
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
   marks, then the possessive are removed from a word; the possessive is found at the end of the
   word's text, so `Sarah's.` and `(Sarah's)` read as `Sarah's`). A possessive looks up its
   synonyms with the `s`, as 0.11.8 did: `comment's` gets the group of `comments`, and `client's`
   (`clients` has none) stays a word, so it does not take the prefix star off the words beside it.
   Its removed `s` counts toward the 4 bytes (`Tom's` searches
   `"Tom"*`, as 0.11.8's `Toms*` did).
   A word whose only punctuation is an inner apostrophe (straight `'` or typographic `’`) searches
   both spellings, as a group: `("don't" OR "dont"*)`, so `Don't` finds "Don't" and "Dont" (#31).
   The typed spelling is a prefix, as any word with punctuation, unless its last part is one
   letter: `t*` has no prefix index (`FTS_PREFIXES` is 2 3 4), so it merges every indexed word
   starting with t (32 to 106 ms median on a 40,000-message archive), and `"don't"*` finds
   "Don took". The joined spelling is a plain word (`("we'll"* OR "well"*)`, `("I'm" OR "Im")`).
   A spelling group is not a synonym group: it leaves its neighbours' prefix stars alone
   (`don't forg` finds "forget"). A field's value keeps one spelling, as typed.
3. A word expanding to synonyms becomes the generated `(a OR b …)` group; when any group
   (synonyms or spellings) is present every term is joined with an explicit `AND`. Whether a
   group is present is read from the terms as written (only a group starts with `(`; the builder
   records which are synonym groups), so it can't disagree with how fields took their values (a
   separate pre-scan did, for runs of empty fields like `from: to: meeting`).
4. A capital `AND`, `OR` or `NOT` (FTS5's operators are case-sensitive) standing between two
   written terms stays that operator, in place of the join: `from:"a@example.com" OR
   to:"a@example.com"`, `"budget plan" OR greetings`, `alice NOT carol`. One that is first, last,
   after another operator, or left with no written term after it is dropped (`NOT budget` searches
   `budget`), so it can never be a syntax error. Lowercase, quoted or a field's value, it is text.
   At most `SEARCH_MAX_KEPT_NOTS` (100) capital `NOT`s are kept: FTS5 nests each one and refuses
   an expression deeper than 256, and a spelling group adds a level, so `don't` followed by 256
   `NOT`s failed where the #14 builder answered. An exclusion past the cap is left out (the `NOT`
   and the term after it), so the query answers with the first 100 exclusions applied (#22).
   The word after a kept `NOT` searches what was typed, without its synonyms: expanded, it would
   also exclude the left side's (`invoice NOT receipt` found nothing). Both spellings are what
   was typed, so `lunch NOT don't` excludes "Dont" too. FTS5's grammar decides what
   a kept `NOT` excludes: without a group (synonyms or spellings), the rest of the run of words
   (`alice NOT carol zebra` is alice NOT (carol zebra), as in 0.11.8); with one, the explicit
   `AND` leaves it only the next term.
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
  `db::tests::test_search_finds_words_with_apostrophes` and its memory twin find `O'Brien`,
  `O’Brien`, `don't forget`, `Don't` and `Don’t` in the text spelling them either way, never
  "Don took" (one-letter last part), with a neighbour's prefix star kept (`don't forg`), both
  spellings excluded by `lunch NOT don't`, and `subject:O'Brien` exact (each red on the #14
  builder or under its #31 mutant: no spellings, typed spelling always starred, spelling group
  treated as a synonym group, NOT operand without spellings, `name:value` field value with
  spellings, no AND join); `query::tests::a_word_with_an_inner_apostrophe_searches_both_spellings`
  pins the written groups, including the `name: value` field site (a group there would cut the
  hybrid column filter and answer an FTS5 error), a user's star on both spellings, the one-letter
  rule counting characters (`c'è`), and a possessive counting toward the 4 bytes; the
  `QUERY_FRAGMENTS` fuzz holds `don't`, `O’Brien` and `I'm`, so both seeded fuzz tests build
  spelling groups beside every operator, field and punctuation (red without the AND join), and
  both hybrid any-text tests send contractions, scoped and under a kept `NOT`, and 300 kept `NOT`s;
  both apostrophe tests find "Létude" for `l'ét` (a joined spelling's 4 bytes counted in bytes)
  and answer `don't NOT obrien` followed by 255 to 300 more `NOT`s with the first exclusion applied
  (red without the `NOT` cap); `query::tests::operators_between_terms_stay_operators` pins the
  cap; and `Sarah's`,
  `Sarah’s invoice`, `John's flight`, `team's budget` find what they name (red without the
  possessive strip), `Tom's` finds "Tommy" (red without the `s` counted), `Sarah's.`, `Sarah's...`,
  `Sarah's,` and `(Sarah's)` find Sarah (red with the possessive read at the end of the word), and `'budget'` still
  expands;
  `db::tests::test_search_prefix_star_is_decided_per_run_of_words` and the memory twin (red with
  a query-wide star rule), including `client's notes` finding "Notesheet for the client" and not
  "Notes for the customer" (red when a possessive looks up its synonyms without the `s`);
  `db::tests::test_search_possessive_of_a_plural_synonym_key_expands` and the memory twin find
  the feedback text for `comment's` in both apostrophes and both cases (red when a possessive
  never expands, or when one of the four endings is dropped) and a field's value beside a group
  staying exact (`meeting from:alex` does not find alexandra); an operator replaces one join only
  (`café OR greetings budget`); `db::tests::test_search_not_excludes_only_the_word_typed` finds
  `invoice NOT receipt` and `March NOT receipt` (red when the word after `NOT` expands) and that
  the word after a kept `AND` or `OR` still expands; the text-less tests include enclosed letters and
  a lone mark, and `test_search_keeps_private_use_characters` is two-sided (red with
  `is_alphanumeric`); `test_rust_process_parity` `test_scoped_search_with_embeddings_finds_only_the_field` checks the
  same with the embedding model loaded; `hybrid_search_reads_any_query_text_as_text` and its
  memory twin run text that leaves no keyword term or an empty column filter through the hybrid
  search (red without its empty-input guards); `test_search_finds_short_partial_words_in_any_script`
  and its memory twin find `会議`, `При` and `Zür` (red when the star counts characters, not
  bytes); `test_search_field_value_may_hold_a_colon` scopes `subject:Re: budget` and
  `body:https://example.com/x` (red when the field ends at the last colon).
- A differential against the released 0.11.8 helper (Thunderbird-shaped `search` and
  `memorySearch`, with and without the embedding model, 2026-10-21) found no query that newly
  fails, and no common query shape that finds less, apart from the apostrophe change of the
  next item (since replaced by both spellings, #31) and the possessive of a short word ending in `ie` (`Tie's` searches
  `tie*`, not 0.11.8's `Ties*`, and misses mail saying only "ties"), a variant form the owner's
  rule in the next item leaves to the vector side. Two differences find fewer rows on purpose: a
  capitalized field (`Subject:Invoice`) now filters the hybrid search's vector candidates, which
  0.11.8's case-sensitive check missed, so vector neighbours outside the field are no longer
  returned; and a typographic possessive (`comment’s`) now expands like the straight one, so its
  run of words loses the automatic prefix star.
- The apostrophe trade approved by the owner on 2026-10-21 (a word with an inner apostrophe
  finds the text as typed and no longer the spelling without it: `Don't` missed "Dont") was
  replaced on 2026-10-22 by both spellings (#31; the owner: "shouldn't don't find dont in
  keywords still?"). Accepted precision cost, the same as 0.11.8's: `we'll` also finds "well",
  `you're` also finds "your". A kept `NOT` beside a spelling group binds as beside a synonym
  group: the explicit `AND` leaves it only the next term. The owner's rule, 2026-10-21: a keyword miss of a variant or misspelled form
  of the typed word is acceptable, as those are typos the vector side of hybrid search finds.
- Accepted limitations, approved by the owner on 2026-10-21 (common query shapes must find what
  0.11.8 found; a recall difference on rare input is acceptable, and synonym expansion stays):
  - Rare input: a few code points are read differently from unicode61's Unicode 6.1 data (code
    points unassigned in 6.1, which it indexes, are dropped; New Tai Lue vowel signs, letters
    today but marks in 6.1, are kept and match nothing). A `*` inside a word (`budg*et`) is part
    of the word, not a prefix marker. A field followed by a phrase (`from: "jane doe"`, or
    `from:"jane doe"`) takes it without starting a new run of words. Typed parentheses are text, not grouping
    (`(budget OR plan) review`; 0.11.8 failed on them).
  - Pre-existing in 0.11.8, tracked separately: a kept `NOT` is not applied to hybrid vector
    candidates (#21). A long chain of kept `NOT`s (about 256) exceeded FTS5's expression depth and
    failed (#22); fixed with #31 by keeping at most 100 (Decision 4).
- `role:` / `content:` no longer scope in memory search, and `msgId:` / `attachmentNames:` no longer
  scope in mail search (all undocumented and unused by any client).
- Both behaviour changes (`from :` with a space before the colon no longer scopes; memory search has
  no fields) were approved by the owner on 2026-10-18.
