/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use crate::fts::synonyms::SynonymLookup;
use unicode_categories::UnicodeCategories;

/// Builds the FTS5 MATCH expression for a search query a user typed or a model wrote.
///
/// Every word and every `"quoted phrase"` reaches FTS5 as a string, so nothing typed breaks the
/// query: parentheses and punctuation are searched as text, and non-ASCII text is kept whole.
/// A capital AND, OR or NOT between two terms stays the operator; anywhere else it is dropped.
/// `name:word`, `name:"phrase"` and `name: word` scope to a column only when `columns` names it
/// (typed name, case-insensitive → table column); any other `x:` is text.
pub fn build_fts_match(
    q: Option<&str>,
    columns: &[(&str, &str)],
    use_synonyms: bool,
    synonyms: &SynonymLookup,
) -> String {
    let Some(q) = q else { return String::new() };
    // FTS5 reads a NUL as the end of the query, leaving a string open; the tokenizer treats it
    // as a separator anyway.
    let q = q.replace('\0', " ");
    let items = scan(&q);

    // A word with synonyms becomes an OR group; FTS5 then needs an explicit AND between all
    // terms. As before, no word in the same run of words (between quoted phrases) as a group gets
    // an automatic prefix star. Which runs have a group is read from the terms written, so it
    // can't disagree with how they were written.
    // A capital AND, OR or NOT standing between two terms stays the FTS5 operator, as the old
    // builder passed it through (`from:"a@example.com" OR to:"a@example.com"`); anywhere else
    // (first, last, after another operator) it is dropped.
    let render = |grouped_runs: &[bool]| -> Vec<(Option<&'static str>, String, usize)> {
        let mut out: Vec<(Option<&'static str>, String, usize)> = vec![];
        let mut operator: Option<&'static str> = None;
        let mut run = 0usize;
        let mut i = 0usize;
        while i < items.len() {
            let item = items[i];
            i += 1;
            if let Item::Word(w) = item {
                if let Some(op) = OPERATORS.iter().find(|op| **op == w) {
                    if operator.is_none() {
                        operator = Some(*op);
                    }
                    continue;
                }
            }
            let has_or_groups = grouped_runs.get(run).copied().unwrap_or(false);
            let term = match item {
                Item::Phrase(p) => {
                    run += 1;
                    phrase(p)
                }
                Item::Word(w) => match scoped_column(w, columns) {
                    // name:"phrase" and name: word take the next item as the value.
                    Some((col, "")) => {
                        let value = items.get(i).copied();
                        i += 1;
                        match value {
                            Some(Item::Phrase(p)) => phrase(p).map(|v| format!("{col}:{v}")),
                            Some(Item::Word(w)) => word(w, false, has_or_groups, synonyms).map(|v| format!("{col}:{v}")),
                            None => None,
                        }
                    }
                    Some((col, value)) => word(value, false, has_or_groups, synonyms).map(|v| format!("{col}:{v}")),
                    None => word(w, use_synonyms, has_or_groups, synonyms),
                },
            };
            if let Some(term) = term {
                out.push((operator.take(), term, run));
            }
        }
        out
    };
    let join = |terms: Vec<(Option<&str>, String, usize)>, joiner: &str| -> String {
        let mut s = String::new();
        for (n, (op, term, _)) in terms.into_iter().enumerate() {
            if n > 0 {
                match op {
                    Some(op) => {
                        s.push(' ');
                        s.push_str(op);
                        s.push(' ');
                    }
                    None => s.push_str(joiner),
                }
            }
            s.push_str(&term);
        }
        s
    };

    let terms = render(&[]);
    // Only a synonym group starts with "(": words and phrases start with a quote, scoped terms
    // with their column.
    let mut grouped_runs = vec![false; terms.last().map_or(0, |(_, _, run)| run + 1)];
    for (_, term, run) in &terms {
        if term.starts_with('(') {
            grouped_runs[*run] = true;
        }
    }
    if grouped_runs.contains(&true) {
        join(render(&grouped_runs), " AND ")
    } else {
        join(terms, " ")
    }
}

/// FTS5's binary operators, which are case-sensitive.
const OPERATORS: [&str; 3] = ["AND", "OR", "NOT"];

#[derive(Clone, Copy)]
enum Item<'a> {
    /// Text between double quotes (an unclosed quote runs to the end).
    Phrase(&'a str),
    /// A run of characters without whitespace or double quotes.
    Word(&'a str),
}

fn scan(q: &str) -> Vec<Item<'_>> {
    let mut items = vec![];
    let mut rest = q;
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            return items;
        }
        if let Some(after) = rest.strip_prefix('"') {
            let end = after.find('"').unwrap_or(after.len());
            items.push(Item::Phrase(&after[..end]));
            rest = after.get(end + 1..).unwrap_or("");
        } else {
            let end = rest.find(|c: char| c.is_whitespace() || c == '"').unwrap_or(rest.len());
            items.push(Item::Word(&rest[..end]));
            rest = &rest[end..];
        }
    }
}

/// `(table column, value)` when `w` is `name:value` with `name` one of `columns`.
fn scoped_column<'a>(w: &'a str, columns: &[(&str, &'a str)]) -> Option<(&'a str, &'a str)> {
    let (name, value) = w.split_once(':')?;
    columns
        .iter()
        .find(|(typed, _)| typed.eq_ignore_ascii_case(name))
        .map(|(_, col)| (*col, value))
}

/// Whether text has a character the tokenizer indexes (a term with none matches nothing).
/// unicode61 indexes letters, numbers and private-use characters (`L* N* Co`); Rust's
/// alphanumeric also counts the marks and enclosed-letter symbols (Ⓜ, 🅿) that have the
/// Alphabetic property, which unicode61 does not index.
fn has_text(s: &str) -> bool {
    s.chars().any(|c| (c.is_alphanumeric() && !c.is_mark() && !c.is_symbol()) || c.is_other_private_use())
}

fn phrase(p: &str) -> Option<String> {
    has_text(p).then(|| format!("\"{p}\""))
}

/// A word's searched text, whether the user ended it with `*`, and whether a possessive was
/// removed: one trailing `*`, then trailing `/` and `?`, then quote marks at either end
/// (`'budget'`), then one possessive `'s` are removed. Any other apostrophe stays: the tokenizer
/// splits `O'Brien` into `o` and `brien`, and the quoted word searches them as a phrase.
/// `John's` searches `John`, which (stemmed) also finds "John's".
fn prepare_word(w: &str) -> Option<(String, bool, bool)> {
    let (w, star) = match w.strip_suffix('*') {
        Some(rest) => (rest, true),
        None => (w, false),
    };
    let w = w.trim_end_matches(['/', '?']).trim_matches(['\'', '\u{2018}', '\u{2019}']);
    let stripped = POSSESSIVES.iter().find_map(|p| w.strip_suffix(p));
    let core = stripped.unwrap_or(w).to_string();
    has_text(&core).then_some((core, star, stripped.is_some()))
}

/// A possessive ending, with a straight or a typographic apostrophe.
const POSSESSIVES: [&str; 4] = ["'s", "'S", "\u{2019}s", "\u{2019}S"];

/// Only a word the user did not star expands to its synonyms (every synonym is plain letters).
fn expands(core: &str, star: bool, synonyms: &SynonymLookup) -> bool {
    !star && synonyms.expand(core) != core
}

fn has_punctuation(core: &str) -> bool {
    core.chars().any(|c| !c.is_alphanumeric() && c != '_' && !c.is_other_private_use())
}

fn word(w: &str, use_synonyms: bool, has_or_groups: bool, synonyms: &SynonymLookup) -> Option<String> {
    let (core, star, possessive) = prepare_word(w)?;
    // A possessive searches the word it names, never its synonyms: 0.11.8 searched `client's`
    // as `clients`, which has none, and a group would take the prefix star off its neighbours.
    if use_synonyms && !possessive && expands(&core, star, synonyms) {
        return Some(synonyms.expand(&core));
    }
    // A word with punctuation is a phrase of its parts ("dmarc-supp" → [dmarc, supp]); the star
    // keeps a partial address matching while it is typed. Plain words of 4+ bytes get one too,
    // unless synonym groups are present; a possessive's `s` counts, as 0.11.8 searched `Tom's`
    // as `Toms*`.
    let prefix = star || has_punctuation(&core) || (core.len() + usize::from(possessive) >= 4 && !has_or_groups);
    Some(format!("\"{core}\"{}", if prefix { "*" } else { "" }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const COLUMNS: &[(&str, &str)] = &[("from", "from_"), ("subject", "subject")];

    fn build(q: &str, columns: &[(&str, &str)]) -> String {
        build_fts_match(Some(q), columns, true, &SynonymLookup::new())
    }

    #[test]
    fn words_and_phrases_are_strings() {
        assert_eq!(build("café", &[]), r#""café"*"#);
        assert_eq!(build("50% off", &[]), r#""50%"* "off""#);
        assert_eq!(build("NOT (budget", &[]), r#""(budget"*"#);
        assert_eq!(build(r#"say "hi there"#, &[]), r#""say" "hi there""#);
        // One possessive 's is removed; any other apostrophe stays.
        assert_eq!(build("it's gone?", &[]), r#""it" "gone"*"#);
        assert_eq!(build("Sarah\u{2019}s O'Brien's don't", &[]), r#""Sarah"* "O'Brien"* "don't"*"#);
        // A possessive never expands, so its neighbours keep their stars.
        assert_eq!(build("client's notes", &[]), r#""client"* "notes"*"#);
        // A possessive's `s` counts toward the automatic star's 4 bytes.
        assert_eq!(build("Tom's it's", &[]), r#""Tom"* "it""#);
        // Quote marks around a word are not part of it.
        assert_eq!(build("'budget'", &[]), build("budget", &[]));
        assert_eq!(build("\u{2018}abcd\u{2019}", &[]), r#""abcd"*"#);
        assert_eq!(build("budg*", &[]), r#""budg"*"#);
        // A user's star keeps a short word a prefix, and a starred word never expands.
        assert_eq!(build("ab*", &[]), r#""ab"*"#);
        assert_eq!(build("meeting*", &[]), r#""meeting"*"#);
        // An underscore is part of a word, not punctuation: no star on a short one.
        assert_eq!(build("a_b", &[]), r#""a_b""#);
        // A NUL separates words.
        assert_eq!(build("ab\u{0}cd", &[]), r#""ab" "cd""#);
        assert_eq!(build(r#"-- "" _ ?"#, &[]), "");
        // Text the tokenizer does not index is dropped: enclosed-letter symbols and lone marks.
        // Private-use characters are indexed, so they stay.
        assert_eq!(build("\u{24d0} \u{1f17f}\u{fe0f} \u{0301} \u{93e}", &[]), "");
        assert_eq!(build("\u{e000}", &[]), "\"\u{e000}\"");
    }

    #[test]
    fn only_named_columns_scope() {
        assert_eq!(build("From:alice", COLUMNS), r#"from_:"alice"*"#);
        assert_eq!(build(r#"from: "a b" x"#, COLUMNS), r#"from_:"a b" "x""#);
        assert_eq!(build("subject: budget", COLUMNS), r#"subject:"budget"*"#);
        assert_eq!(build("subject:", COLUMNS), "");
        assert_eq!(build("note:launch", COLUMNS), r#""note:launch"*"#);
        assert_eq!(build("from:alice", &[]), r#""from:alice"*"#);
    }

    #[test]
    fn operators_between_terms_stay_operators() {
        assert_eq!(build("from:alice OR from:bob", COLUMNS), r#"from_:"alice"* OR from_:"bob""#);
        assert_eq!(build(r#""a b" NOT zebra"#, &[]), r#""a b" NOT "zebra"*"#);
        assert_eq!(build("abcd AND efgh", &[]), r#""abcd"* AND "efgh"*"#);
        // An operator replaces one join only.
        assert_eq!(build("abcd OR efgh ijkl", &[]), r#""abcd"* OR "efgh"* "ijkl"*"#);
        // First, last, doubled, or left with no term after it: dropped.
        assert_eq!(build("OR abcd", &[]), r#""abcd"*"#);
        assert_eq!(build("abcd NOT", &[]), r#""abcd"*"#);
        assert_eq!(build("abcd OR NOT efgh", &[]), r#""abcd"* OR "efgh"*"#);
        assert_eq!(build("abcd OR -- efgh", &[]), r#""abcd"* OR "efgh"*"#);
        assert_eq!(build("OR AND NOT", &[]), "");
        // Lowercase, quoted, or a field's value: text.
        assert_eq!(build("abcd or efgh", &[]), r#""abcd"* "or" "efgh"*"#);
        assert_eq!(build(r#"abcd "OR" efgh"#, &[]), r#""abcd"* "OR" "efgh"*"#);
        assert_eq!(build("subject: OR", COLUMNS), r#"subject:"OR""#);
        // Beside a synonym group, an operator replaces the AND join.
        let q = build("meeting OR zebra", COLUMNS);
        assert!(q.starts_with('(') && q.ends_with(r#") OR "zebra""#), "{q}");
    }

    #[test]
    fn synonym_groups_are_joined_with_and() {
        let q = build(r#"meeting "budget plan" from:alice"#, COLUMNS);
        assert!(q.starts_with('(') && q.contains(" OR meeting OR "), "{q}");
        // No word in a group's run of words (between quoted phrases) gets an automatic star;
        // a word in another run does.
        assert!(q.ends_with(r#") AND "budget plan" AND from_:"alice"*"#), "{q}");
        let q = build(r#"notes "Jane Doe" roadmapping meeting"#, COLUMNS);
        assert!(q.starts_with(r#""notes"* AND "Jane Doe" AND "roadmapping" AND ("#), "{q}");
        // The user's star survives beside a group.
        let q = build("meeting budg*", COLUMNS);
        assert!(q.starts_with('(') && q.ends_with(r#") AND "budg"*"#), "{q}");
        // Fields with no value: each takes the next item, and the joiner follows what is written.
        let q = build("from: subject: meeting", COLUMNS);
        assert!(q.starts_with(r#"from_:"subject:"* AND ("#), "{q}");
        // A field's value beside a group gets no automatic star either.
        for q in ["meeting from:alex", "meeting from: alex"] {
            let q = build(q, COLUMNS);
            assert!(q.ends_with(r#") AND from_:"alex""#), "{q}");
        }
        // A parenthesis inside a word is text: it is not a group and changes no join or star.
        assert_eq!(build("f(x) budg", &[]), r#""f(x)"* "budg"*"#);
        // A field's value is never expanded, and does not cause groups.
        assert_eq!(build("subject:meeting zebra", COLUMNS), r#"subject:"meeting"* "zebra"*"#);
    }
}
