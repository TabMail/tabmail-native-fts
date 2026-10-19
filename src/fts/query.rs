/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use crate::fts::synonyms::SynonymLookup;

/// Builds the FTS5 MATCH expression for a search query a user typed or a model wrote.
///
/// Every word and every `"quoted phrase"` reaches FTS5 as a string, so nothing typed is read
/// as FTS5 syntax: operator words, parentheses and punctuation are searched as text, and
/// non-ASCII text is kept whole. `name:word`, `name:"phrase"` and `name: word` scope to a
/// column only when `columns` names it (typed name, case-insensitive → table column); any
/// other `x:` is text. The synonym groups added here are the only operators in the result.
pub fn build_fts_match(
    q: Option<&str>,
    columns: &[(&str, &str)],
    use_synonyms: bool,
    synonyms: &SynonymLookup,
) -> String {
    let Some(q) = q else { return String::new() };
    let items = scan(q);

    // A word with synonyms becomes an OR group; FTS5 then needs an explicit AND between all
    // terms, and no word gets an automatic prefix star (as before).
    let has_or_groups = use_synonyms
        && items.iter().enumerate().any(|(i, item)| {
            matches!(item, Item::Word(w) if scoped_column(w, columns).is_none()
                && !follows_bare_field(&items, i, columns)
                && prepare_word(w).is_some_and(|(core, star)| expands(&core, star, synonyms)))
        });

    let mut out: Vec<String> = vec![];
    let mut i = 0usize;
    while i < items.len() {
        let item = items[i];
        i += 1;
        let term = match item {
            Item::Phrase(p) => phrase(p),
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
        out.extend(term);
    }

    out.join(if has_or_groups { " AND " } else { " " })
}

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

/// Whether item `i` is the value of a bare `name:` before it.
fn follows_bare_field(items: &[Item], i: usize, columns: &[(&str, &str)]) -> bool {
    i > 0 && matches!(items[i - 1], Item::Word(w) if matches!(scoped_column(w, columns), Some((_, ""))))
}

/// Whether text has anything a tokenizer would index (a term with none matches nothing).
fn has_text(s: &str) -> bool {
    s.chars().any(char::is_alphanumeric)
}

fn phrase(p: &str) -> Option<String> {
    has_text(p).then(|| format!("\"{p}\""))
}

/// A word's searched text and whether the user ended it with `*`: one trailing `*`, then
/// trailing `/` and `?`, are removed, and apostrophes are dropped.
fn prepare_word(w: &str) -> Option<(String, bool)> {
    let (w, star) = match w.strip_suffix('*') {
        Some(rest) => (rest, true),
        None => (w, false),
    };
    let core = w.trim_end_matches(['/', '?']).replace('\'', "");
    has_text(&core).then_some((core, star))
}

/// Only a plain word the user did not star expands to its synonyms.
fn expands(core: &str, star: bool, synonyms: &SynonymLookup) -> bool {
    !star && !has_punctuation(core) && synonyms.expand(core) != core
}

fn has_punctuation(core: &str) -> bool {
    core.chars().any(|c| !c.is_alphanumeric() && c != '_')
}

fn word(w: &str, use_synonyms: bool, has_or_groups: bool, synonyms: &SynonymLookup) -> Option<String> {
    let (core, star) = prepare_word(w)?;
    if use_synonyms && expands(&core, star, synonyms) {
        return Some(synonyms.expand(&core));
    }
    // A word with punctuation is a phrase of its parts ("dmarc-supp" → [dmarc, supp]); the star
    // keeps a partial address matching while it is typed. Plain words of 4+ bytes get one too,
    // unless synonym groups are present.
    let prefix = star || has_punctuation(&core) || (core.len() >= 4 && !has_or_groups);
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
        assert_eq!(build("NOT (budget", &[]), r#""NOT" "(budget"*"#);
        assert_eq!(build(r#"say "hi there"#, &[]), r#""say" "hi there""#);
        assert_eq!(build("it's gone?", &[]), r#""its" "gone"*"#);
        assert_eq!(build("budg*", &[]), r#""budg"*"#);
        assert_eq!(build(r#"-- "" _ ?"#, &[]), "");
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
    fn synonym_groups_are_joined_with_and() {
        let q = build(r#"meeting "budget plan" from:alice"#, COLUMNS);
        assert!(q.starts_with('(') && q.contains(" OR meeting OR "), "{q}");
        // With groups present no word gets an automatic star.
        assert!(q.ends_with(r#") AND "budget plan" AND from_:"alice""#), "{q}");
        // A field's value is never expanded, and does not cause groups.
        assert_eq!(build("subject:meeting zebra", COLUMNS), r#"subject:"meeting"* "zebra"*"#);
    }
}
