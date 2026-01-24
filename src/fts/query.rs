use crate::fts::synonyms::SynonymLookup;

// FTS5 query builder with email-specific syntax handling.
pub fn build_fts_match(q: Option<&str>, use_synonyms: bool, synonyms: &SynonymLookup) -> String {
    let Some(q) = q else { return String::new() };
    let q = q.trim();
    if q.is_empty() {
        return String::new();
    }

    // First translate field aliases in raw query (from: -> from_:, to: -> to_:).
    let mut q = translate_aliases(q);

    // Extract field:"quoted value" segments before splitting by quotes.
    let mut field_quoted_matches: Vec<(String, String)> = vec![];
    q = extract_field_quoted(&q, &mut field_quoted_matches);

    // Split by quotes to preserve quoted phrases.
    let parts: Vec<&str> = q.split('"').collect();
    let mut out: Vec<String> = vec![];

    for (idx, part) in parts.iter().enumerate() {
        let is_quoted = idx % 2 == 1;
        if is_quoted {
            out.push(format!("\"{}\"", part));
            continue;
        }

        let tokens: Vec<&str> = part.split_whitespace().filter(|t| !t.is_empty()).collect();
        let mut mapped: Vec<String> = vec![];

        let will_have_or_groups = if use_synonyms {
            tokens.iter().any(|tok| will_expand_to_or_group(tok, synonyms))
        } else {
            false
        };

        for tok in tokens {
            if let Some((field, val)) = placeholder_field_quoted(tok, &field_quoted_matches) {
                mapped.push(format!("{field}:\"{val}\""));
                continue;
            }

            if is_pure_punctuation(tok) {
                continue;
            }

            let (field, mut value) = split_field(tok);

            // If value already quoted, preserve as-is (no wildcarding / synonym expansion).
            if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
                let mut fv = String::new();
                if let Some(f) = field {
                    fv.push_str(f);
                    fv.push(':');
                }
                fv.push_str(value);
                mapped.push(fv);
                continue;
            }

            // Remove trailing punctuation like '?' or '/' before optional wildcard.
            let has_wildcard = value.ends_with('*');
            if has_wildcard {
                value = &value[..value.len() - 1];
            }
            let core = trim_trailing_slash_question(value);

            // Remove apostrophes for FTS5 compatibility.
            let mut escaped_core = core.replace('\'', "");

            // Handle naked wildcard "*": convert to "." (python uses "." so then adds "*" back).
            if escaped_core.is_empty() && has_wildcard {
                escaped_core.push('.');
            }

            let needs_quote = has_special_chars_requiring_quotes(&escaped_core);

            let final_token = if needs_quote {
                format!("\"{}\"", escaped_core.replace('"', "\"\""))
            } else {
                // Auto-add wildcard for tokens >= 4 chars, but avoid if OR groups exist.
                if !has_wildcard
                    && escaped_core.len() >= 4
                    && !will_have_or_groups
                {
                    format!("{escaped_core}*")
                } else if has_wildcard {
                    format!("{escaped_core}*")
                } else {
                    escaped_core
                }
            };

            if let Some(f) = field {
                mapped.push(format!("{f}:{final_token}"));
            } else if use_synonyms && !has_wildcard && !needs_quote && !final_token.is_empty() {
                let expanded = synonyms.expand(&final_token);
                if expanded != final_token {
                    mapped.push(expanded);
                } else {
                    mapped.push(final_token);
                }
            } else {
                mapped.push(final_token);
            }
        }

        if !mapped.is_empty() {
            let has_or_groups = mapped.iter().any(|t| t.contains("(") && t.contains(" OR "));
            if has_or_groups {
                out.push(mapped.join(" AND "));
            } else {
                out.push(mapped.join(" "));
            }
        }
    }

    out.join(" ").trim().to_string()
}

fn translate_aliases(q: &str) -> String {
    // Equivalent to Python regex: r'\b(from|to)\s*:' -> from_:/to_:
    // We'll do a small manual scanner to avoid regex deps.
    let mut out = String::with_capacity(q.len());
    let bytes = q.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        // check word boundary for "from" or "to" then optional spaces then ':'
        if starts_word_at(bytes, i, b"from") {
            let end = i + 4;
            let mut j = end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b':' {
                out.push_str("from_:");
                i = j + 1;
                continue;
            }
        }
        if starts_word_at(bytes, i, b"to") {
            let end = i + 2;
            let mut j = end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b':' {
                out.push_str("to_:");
                i = j + 1;
                continue;
            }
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

fn starts_word_at(haystack: &[u8], i: usize, needle: &[u8]) -> bool {
    if i + needle.len() > haystack.len() {
        return false;
    }
    // word boundary: previous is not alnum/_ and current starts with needle (case-insensitive)
    if i > 0 {
        let prev = haystack[i - 1];
        if (prev as char).is_ascii_alphanumeric() || prev == b'_' {
            return false;
        }
    }
    for (k, nb) in needle.iter().enumerate() {
        let hb = haystack[i + k];
        if hb.to_ascii_lowercase() != nb.to_ascii_lowercase() {
            return false;
        }
    }
    true
}

fn extract_field_quoted(q: &str, store: &mut Vec<(String, String)>) -> String {
    // Pattern: field_name:"quoted value" where field is [A-Za-z_][A-Za-z0-9_]*
    // We'll do a simple scan, not a full regex engine.
    let mut out = String::with_capacity(q.len());
    let bytes = q.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        // parse identifier
        if is_ident_start(bytes[i]) {
            let start_ident = i;
            let mut j = i + 1;
            while j < bytes.len() && is_ident_char(bytes[j]) {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b':' && (j + 1) < bytes.len() && bytes[j + 1] == b'"' {
                // find closing quote
                let mut k = j + 2;
                while k < bytes.len() && bytes[k] != b'"' {
                    k += 1;
                }
                if k < bytes.len() && bytes[k] == b'"' {
                    let field = String::from_utf8_lossy(&bytes[start_ident..j]).to_string();
                    let val = String::from_utf8_lossy(&bytes[(j + 2)..k]).to_string();
                    let placeholder = format!("__FQ{}__", store.len());
                    store.push((field, val));
                    out.push_str(&placeholder);
                    i = k + 1;
                    continue;
                }
            }
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

fn is_ident_start(b: u8) -> bool {
    (b as char).is_ascii_alphabetic() || b == b'_'
}

fn is_ident_char(b: u8) -> bool {
    (b as char).is_ascii_alphanumeric() || b == b'_'
}

fn placeholder_field_quoted<'a>(tok: &'a str, store: &'a [(String, String)]) -> Option<(&'a str, &'a str)> {
    if let Some(idx) = parse_placeholder(tok) {
        if idx < store.len() {
            let (field, val) = &store[idx];
            return Some((field.as_str(), val.as_str()));
        }
    }
    None
}

fn parse_placeholder(tok: &str) -> Option<usize> {
    // "__FQ{n}__"
    if !tok.starts_with("__FQ") || !tok.ends_with("__") {
        return None;
    }
    let inner = &tok[4..tok.len() - 2];
    inner.parse::<usize>().ok()
}

fn is_pure_punctuation(tok: &str) -> bool {
    tok.chars().all(|c| !c.is_alphanumeric() && c != '_')
}

fn split_field(tok: &str) -> (Option<&str>, &str) {
    if let Some(pos) = tok.find(':') {
        let (field, rest) = tok.split_at(pos);
        let rest = &rest[1..];
        if !field.is_empty() && field.chars().next().unwrap().is_ascii_alphabetic() || field.starts_with('_') {
            return (Some(field), rest);
        }
    }
    (None, tok)
}

fn trim_trailing_slash_question(s: &str) -> String {
    let mut end = s.len();
    while end > 0 {
        let c = s.as_bytes()[end - 1];
        if c == b'/' || c == b'?' {
            end -= 1;
            continue;
        }
        break;
    }
    s[..end].to_string()
}

fn has_special_chars_requiring_quotes(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '-' | '@' | ':' | '+' | '.'))
}

fn will_expand_to_or_group(tok: &str, synonyms: &SynonymLookup) -> bool {
    // Skip placeholders / pure punctuation.
    if parse_placeholder(tok).is_some() || is_pure_punctuation(tok) {
        return false;
    }

    let (field, mut value) = split_field(tok);
    if field.is_some() {
        return false;
    }

    // If already quoted, no.
    if value.starts_with('"') && value.ends_with('"') {
        return false;
    }

    let has_wildcard = value.ends_with('*');
    if has_wildcard {
        value = &value[..value.len() - 1];
    }
    let core = trim_trailing_slash_question(value);
    let escaped = core.replace('\'', "");
    if escaped.is_empty() {
        return false;
    }

    let needs_quote = has_special_chars_requiring_quotes(&escaped);
    if has_wildcard || needs_quote {
        return false;
    }

    let expanded = synonyms.expand(&escaped);
    expanded != escaped
}


