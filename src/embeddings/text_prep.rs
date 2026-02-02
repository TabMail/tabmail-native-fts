// text_prep.rs — Text preparation for embedding generation.
//
// Constructs embedding input text from structured fields (email headers, body, etc.).
// Truncates to fit the model's context window (256 word-piece tokens for all-MiniLM-L6-v2).

/// Prepare embedding text for an email message.
///
/// Strategy:
/// - Subject repeated for emphasis (mirrors BM25 5.0x column weight)
/// - From/To headers included for sender/recipient context
/// - Body truncated to fit within model context window
///
/// The total text is kept to ~200 words to stay within the 256 token limit
/// after word-piece tokenization (which expands words into subwords).
pub fn prepare_email_text(subject: &str, from: &str, to: &str, body: &str) -> String {
    let subject = subject.trim();
    let from = from.trim();
    let to = to.trim();
    let body = body.trim();

    // Header portion: subject (repeated) + from + to
    // This takes ~20-40 tokens, leaving ~200 tokens for body.
    let mut parts = Vec::with_capacity(4);
    if !subject.is_empty() {
        parts.push(format!("Subject: {subject}"));
        parts.push(format!("Subject: {subject}"));
    }
    if !from.is_empty() {
        parts.push(format!("From: {from}"));
    }
    if !to.is_empty() {
        parts.push(format!("To: {to}"));
    }

    let header = parts.join("\n");

    // Body: take first ~150 words to leave room for headers.
    // Word-piece tokenization typically expands by ~1.3x, so 150 words ≈ 195 tokens.
    let body_truncated = truncate_words(body, 150);

    if body_truncated.is_empty() {
        header
    } else if header.is_empty() {
        body_truncated
    } else {
        format!("{header}\n\n{body_truncated}")
    }
}

/// Prepare embedding text for a memory/chat entry.
///
/// Memory entries are shorter than emails and usually fit within the context window.
pub fn prepare_memory_text(role: &str, content: &str) -> String {
    let role = role.trim();
    let content = content.trim();

    // Take first ~200 words (memory entries are typically short)
    let content_truncated = truncate_words(content, 200);

    if role.is_empty() {
        content_truncated
    } else {
        format!("{role}: {content_truncated}")
    }
}

/// Truncate text to at most `max_words` words, preserving word boundaries.
fn truncate_words(text: &str, max_words: usize) -> String {
    let mut words = 0;
    let mut end = 0;

    for (i, c) in text.char_indices() {
        if c.is_whitespace() {
            words += 1;
            if words >= max_words {
                end = i;
                break;
            }
        }
        end = i + c.len_utf8();
    }

    text[..end].trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_email_text_basic() {
        let text = prepare_email_text("Budget Review", "alice@example.com", "bob@example.com", "Please review the attached budget.");
        assert!(text.contains("Subject: Budget Review"));
        assert!(text.contains("From: alice@example.com"));
        assert!(text.contains("To: bob@example.com"));
        assert!(text.contains("Please review"));
        // Subject should appear twice (emphasis)
        assert_eq!(text.matches("Subject: Budget Review").count(), 2);
    }

    #[test]
    fn test_prepare_email_text_empty_fields() {
        let text = prepare_email_text("", "", "", "Just a body");
        assert_eq!(text, "Just a body");
    }

    #[test]
    fn test_prepare_memory_text() {
        let text = prepare_memory_text("user", "What's the weather like?");
        assert_eq!(text, "user: What's the weather like?");
    }

    #[test]
    fn test_truncate_words() {
        let text = "one two three four five six seven eight nine ten";
        assert_eq!(truncate_words(text, 5), "one two three four five");
        assert_eq!(truncate_words(text, 100), text);
        assert_eq!(truncate_words("", 5), "");
    }
}
