/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// hybrid.rs — Score normalization and result merging for hybrid FTS5 + vector search.
//
// Following OpenClaw's weighted score merge approach:
// 1. Normalize BM25 rank to 0..1
// 2. Normalize cosine distance to 0..1
// 3. Union results by rowid
// 4. Compute final score = vectorWeight * vectorScore + textWeight * textScore
// 5. Sort (by final score DESC, or by date), return top N

use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::Value;

use crate::config;

/// The order a search sorts its matches in before it takes its `limit` (ADR-NF-017): the best match
/// first (the default, ties newer first), or newest or oldest first (ties the better match first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchSort {
    Relevance,
    DateDesc,
    DateAsc,
}

impl SearchSort {
    /// A request's `sort`: absent (or null) is `relevance`; any value but the three names is refused,
    /// so a misspelt sort is not answered in another order.
    pub fn from_params(params: &Value) -> anyhow::Result<Self> {
        match params.get("sort") {
            None | Some(Value::Null) => Ok(Self::Relevance),
            Some(Value::String(s)) if s == "relevance" => Ok(Self::Relevance),
            Some(Value::String(s)) if s == "date_desc" => Ok(Self::DateDesc),
            Some(Value::String(s)) if s == "date_asc" => Ok(Self::DateAsc),
            Some(other) => anyhow::bail!("Unknown sort {other}: expected \"relevance\", \"date_desc\" or \"date_asc\""),
        }
    }
}

/// A candidate result from one of the search engines.
#[derive(Debug, Clone)]
pub struct HybridCandidate {
    pub rowid: i64,
    pub text_score: f64,
    pub vector_score: f64,
    /// Found by keyword: a result whatever its score (#51).
    pub keyword_match: bool,
}

/// A merged result with final combined score.
#[derive(Debug, Clone)]
pub struct HybridResult {
    pub rowid: i64,
    pub final_score: f64,
    pub date_ms: i64,
}

/// Convert FTS5 BM25 rank to 0..1 score.
/// BM25 rank from SQLite is negative (lower = better match).
/// rank=0 → 1.0 (perfect), rank=-10 → score depends on normalization.
///
/// Note: SQLite's bm25() returns negative values where more negative = better.
/// We negate first: -rank gives us a positive value where higher = better.
pub fn bm25_rank_to_score(rank: f64) -> f64 {
    // rank is negative from SQLite bm25(), so -rank is positive.
    // More negative rank = better match = higher score.
    let positive_rank = if rank.is_finite() { (-rank).max(0.0) } else { 0.0 };
    // Normalize: high positive_rank → score near 1.0
    // Using: score = 1 - 1/(1 + positive_rank)  = positive_rank / (1 + positive_rank)
    // This maps 0 → 0, infinity → 1, with diminishing returns.
    positive_rank / (1.0 + positive_rank)
}

/// Convert cosine distance to 0..1 score.
/// distance=0 → 1.0 (identical vectors), distance=1 → 0.0 (orthogonal).
pub fn cosine_distance_to_score(distance: f64) -> f64 {
    (1.0 - distance).max(0.0)
}

/// A vector score rescaled for the merge: scores below `VECTOR_SCORE_THRESHOLD` map to 0, above it
/// to 0..1.
pub fn adjusted_vector_score(vector_score: f64) -> f64 {
    let th = config::hybrid::VECTOR_SCORE_THRESHOLD;
    ((vector_score - th) / (1.0 - th)).max(0.0)
}

/// Merge FTS5 and vector search results into a single list in `sort` order.
///
/// `text_results`: (rowid, bm25_rank) from FTS5 search
/// `vector_results`: (rowid, cosine_distance) from vector search
/// `dates`: each candidate's dateMs; a candidate without one (its row is gone) is left out
/// `vector_weight`: weight for semantic score (0.0..1.0)
/// `text_weight`: weight for keyword score (0.0..1.0)
/// `sort`: the order of every keyword match and every other candidate at `MIN_SCORE` or above,
/// before `limit` is taken
/// `limit`: maximum number of results to return
pub fn merge_results(
    text_results: &[(i64, f64)],
    vector_results: &[(i64, f64)],
    dates: &HashMap<i64, i64>,
    vector_weight: f64,
    text_weight: f64,
    sort: SearchSort,
    limit: usize,
) -> Vec<HybridResult> {
    let mut candidates: HashMap<i64, HybridCandidate> = HashMap::new();

    // Add text results
    for &(rowid, rank) in text_results {
        let score = bm25_rank_to_score(rank);
        candidates
            .entry(rowid)
            .and_modify(|c| c.text_score = score)
            .or_insert(HybridCandidate {
                rowid,
                text_score: score,
                vector_score: 0.0,
                keyword_match: true,
            });
    }

    // Add vector results
    for &(rowid, distance) in vector_results {
        let score = cosine_distance_to_score(distance);
        candidates
            .entry(rowid)
            .and_modify(|c| c.vector_score = score)
            .or_insert(HybridCandidate {
                rowid,
                text_score: 0.0,
                vector_score: score,
                keyword_match: false,
            });
    }

    // Compute final scores and filter. Every keyword match is a result, however low it scores: a word
    // in at least half the rows scores about 0 by keyword (FTS5 clamps its IDF), and dropping those
    // returned fewer matches than a keyword-only search (#51). MIN_SCORE gates only the candidates
    // found by meaning alone.
    let mut results: Vec<HybridResult> = candidates
        .into_values()
        .filter_map(|c| {
            let final_score = vector_weight * adjusted_vector_score(c.vector_score) + text_weight * c.text_score;
            if !c.keyword_match && final_score < config::hybrid::MIN_SCORE {
                return None;
            }
            Some(HybridResult {
                rowid: c.rowid,
                final_score,
                date_ms: *dates.get(&c.rowid)?,
            })
        })
        .collect();

    // Sort, then a last tie-break by rowid so equal results come back in one order.
    let better = |a: &HybridResult, b: &HybridResult| b.final_score.total_cmp(&a.final_score);
    let newer = |a: &HybridResult, b: &HybridResult| b.date_ms.cmp(&a.date_ms);
    results.sort_by(|a, b| {
        let order: Ordering = match sort {
            SearchSort::Relevance => better(a, b).then_with(|| newer(a, b)),
            SearchSort::DateDesc => newer(a, b).then_with(|| better(a, b)),
            SearchSort::DateAsc => a.date_ms.cmp(&b.date_ms).then_with(|| better(a, b)),
        };
        order.then_with(|| a.rowid.cmp(&b.rowid))
    });

    // Truncate to limit
    results.truncate(limit);

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_rank_to_score() {
        // rank=0 (no match) → score=0
        assert!((bm25_rank_to_score(0.0) - 0.0).abs() < 1e-10);

        // More negative rank = better match = higher score
        let score_good = bm25_rank_to_score(-10.0);
        let score_great = bm25_rank_to_score(-50.0);
        assert!(score_good > 0.0);
        assert!(score_great > score_good);
        assert!(score_great < 1.0);
    }

    #[test]
    fn test_cosine_distance_to_score() {
        assert!((cosine_distance_to_score(0.0) - 1.0).abs() < 1e-10);
        assert!((cosine_distance_to_score(1.0) - 0.0).abs() < 1e-10);
        assert!((cosine_distance_to_score(0.5) - 0.5).abs() < 1e-10);
        // Clamp negative distances
        assert!((cosine_distance_to_score(1.5) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_merge_results_basic() {
        let text = vec![(1, -10.0), (2, -5.0)];
        let vector = vec![(1, 0.2), (3, 0.1)];

        let dates = HashMap::from([(1, 0), (2, 0), (3, 0)]);
        let merged = merge_results(&text, &vector, &dates, 0.7, 0.3, SearchSort::Relevance, 10);

        // rowid=1 should have both scores, rowid=2 text only, rowid=3 vector only
        assert!(merged.len() <= 3);

        // rowid=1 should rank highest (has both scores)
        if !merged.is_empty() {
            assert_eq!(merged[0].rowid, 1);
        }
    }

    fn rowids(results: &[HybridResult]) -> Vec<i64> {
        results.iter().map(|r| r.rowid).collect()
    }

    /// `sort` absent or null is relevance; the three names are read; any other value is refused.
    #[test]
    fn sort_is_one_of_three_names() {
        use serde_json::json;
        assert_eq!(SearchSort::from_params(&json!({})).unwrap(), SearchSort::Relevance);
        assert_eq!(SearchSort::from_params(&json!({ "sort": null })).unwrap(), SearchSort::Relevance);
        for (name, sort) in [("relevance", SearchSort::Relevance), ("date_desc", SearchSort::DateDesc), ("date_asc", SearchSort::DateAsc)] {
            assert_eq!(SearchSort::from_params(&json!({ "sort": name })).unwrap(), sort, "{name}");
        }
        for bad in [json!("date"), json!("DATE_DESC"), json!("relevance "), json!(""), json!(1), json!(true), json!(["date_asc"])] {
            assert!(SearchSort::from_params(&json!({ "sort": bad })).is_err(), "{bad}");
        }
    }

    /// Relevance: the best score first; equal scores newer first, whatever their rowids.
    #[test]
    fn relevance_ties_go_newer_first() {
        let text = vec![(1, -4.0), (2, -4.0), (3, -4.0), (4, -4.0), (5, -9.0)];
        let dates = HashMap::from([(1, 30), (2, 10), (3, 40), (4, 20), (5, 0)]);
        let merged = merge_results(&text, &[], &dates, 0.7, 0.3, SearchSort::Relevance, 10);
        assert_eq!(rowids(&merged), [5, 3, 1, 4, 2]);
        // The four are an exact tie.
        assert!(merged[1..].iter().all(|r| r.final_score == merged[1].final_score));
        assert!(merged[0].final_score > merged[1].final_score);
        // The limit is taken after the sort.
        assert_eq!(rowids(&merge_results(&text, &[], &dates, 0.7, 0.3, SearchSort::Relevance, 2)), [5, 3]);
    }

    /// A date sort orders every kept candidate by date, newest or oldest first, a date's ties the
    /// better match first in both directions, then takes the limit.
    #[test]
    fn date_sorts_order_by_date_and_ties_better_first() {
        let text = vec![(1, -10.0), (2, -2.0), (3, -5.0), (4, -10.0), (5, -3.0)];
        let dates = HashMap::from([(1, 10), (2, 30), (3, 20), (4, 30), (5, 30)]);
        let merge = |sort, limit| rowids(&merge_results(&text, &[], &dates, 0.7, 0.3, sort, limit));
        assert_eq!(merge(SearchSort::DateDesc, 10), [4, 5, 2, 3, 1]);
        assert_eq!(merge(SearchSort::DateAsc, 10), [1, 3, 4, 5, 2]);
        assert_eq!(merge(SearchSort::DateDesc, 2), [4, 5]);
        assert_eq!(merge(SearchSort::DateAsc, 1), [1]);
        // By relevance the same candidates: 1 and 4 tie, the newer first.
        assert_eq!(merge(SearchSort::Relevance, 2), [4, 1]);
        // A vector-only candidate sorts by its date too.
        let vector = vec![(6, 0.0)];
        let dates = HashMap::from([(1, 10), (6, 5)]);
        let merged = merge_results(&[(1, -10.0)], &vector, &dates, 0.7, 0.3, SearchSort::DateAsc, 10);
        assert_eq!(rowids(&merged), [6, 1]);
    }

    /// Candidates equal in score and date come in rowid order, the same in every search.
    #[test]
    fn equal_score_and_date_come_in_rowid_order() {
        let text: Vec<(i64, f64)> = [4, 1, 6, 3, 5, 2].iter().map(|&r| (r, -4.0)).collect();
        let dates: HashMap<i64, i64> = (1..=6).map(|r| (r, 10)).collect();
        for sort in [SearchSort::Relevance, SearchSort::DateDesc, SearchSort::DateAsc] {
            assert_eq!(rowids(&merge_results(&text, &[], &dates, 0.7, 0.3, sort, 10)), [1, 2, 3, 4, 5, 6], "{sort:?}");
        }
    }

    /// Every keyword match is a result, however low it scores (a word in most rows scores about 0
    /// by keyword); a candidate found by meaning alone is kept only at MIN_SCORE or above (#51).
    #[test]
    fn every_keyword_match_is_kept_and_meaning_alone_needs_min_score() {
        // rank -0.000001: about 0 by keyword; rank 0 (a match only in a column weighted 0): exactly 0.
        // Distance 1.0: 0 by meaning.
        let text = vec![(1, -0.000001), (2, 0.0), (3, -0.000001)];
        // 3 also found by meaning, weakly; 4 by meaning alone, weakly; 5 by meaning alone, strongly.
        let vector = vec![(3, 1.0), (4, 1.0), (5, 0.0)];
        let dates: HashMap<i64, i64> = (1..=5).map(|r| (r, 10 * r)).collect();
        for sort in [SearchSort::Relevance, SearchSort::DateDesc, SearchSort::DateAsc] {
            let merged = merge_results(&text, &vector, &dates, 0.7, 0.3, sort, 10);
            let mut kept = rowids(&merged);
            kept.sort();
            assert_eq!(kept, [1, 2, 3, 5], "{sort:?}");
            assert!(merged.iter().filter(|r| r.rowid != 5).all(|r| r.final_score < config::hybrid::MIN_SCORE), "{sort:?}");
        }
        // By relevance the match by meaning comes first.
        assert_eq!(rowids(&merge_results(&text, &vector, &dates, 0.7, 0.3, SearchSort::Relevance, 1)), [5]);
    }

    /// A candidate with no date (its row is gone) is left out, as its row could not be answered.
    #[test]
    fn a_candidate_without_a_date_is_left_out() {
        let dates = HashMap::from([(1, 10)]);
        for sort in [SearchSort::Relevance, SearchSort::DateDesc, SearchSort::DateAsc] {
            let merged = merge_results(&[(1, -10.0), (2, -10.0)], &[(3, 0.0)], &dates, 0.7, 0.3, sort, 10);
            assert_eq!(rowids(&merged), [1], "{sort:?}");
        }
    }
}
