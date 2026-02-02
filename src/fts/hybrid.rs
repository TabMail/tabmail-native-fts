// hybrid.rs — Score normalization and result merging for hybrid FTS5 + vector search.
//
// Following OpenClaw's weighted score merge approach:
// 1. Normalize BM25 rank to 0..1
// 2. Normalize cosine distance to 0..1
// 3. Union results by rowid
// 4. Compute final score = vectorWeight * vectorScore + textWeight * textScore
// 5. Sort by final score DESC, return top N

use std::collections::HashMap;

use crate::config;

/// A candidate result from one of the search engines.
#[derive(Debug, Clone)]
pub struct HybridCandidate {
    pub rowid: i64,
    pub text_score: f64,
    pub vector_score: f64,
}

/// A merged result with final combined score.
#[derive(Debug, Clone)]
pub struct HybridResult {
    pub rowid: i64,
    pub final_score: f64,
    pub text_score: f64,
    pub vector_score: f64,
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

/// Merge FTS5 and vector search results into a single ranked list.
///
/// `text_results`: (rowid, bm25_rank) from FTS5 search
/// `vector_results`: (rowid, cosine_distance) from vector search
/// `vector_weight`: weight for semantic score (0.0..1.0)
/// `text_weight`: weight for keyword score (0.0..1.0)
/// `limit`: maximum number of results to return
pub fn merge_results(
    text_results: &[(i64, f64)],
    vector_results: &[(i64, f64)],
    vector_weight: f64,
    text_weight: f64,
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
            });
    }

    // Compute final scores and filter
    let mut results: Vec<HybridResult> = candidates
        .into_values()
        .map(|c| {
            let final_score = vector_weight * c.vector_score + text_weight * c.text_score;
            HybridResult {
                rowid: c.rowid,
                final_score,
                text_score: c.text_score,
                vector_score: c.vector_score,
            }
        })
        .filter(|r| r.final_score >= config::hybrid::MIN_SCORE)
        .collect();

    // Sort by final score DESC
    results.sort_by(|a, b| b.final_score.partial_cmp(&a.final_score).unwrap_or(std::cmp::Ordering::Equal));

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

        let merged = merge_results(&text, &vector, 0.7, 0.3, 10);

        // rowid=1 should have both scores, rowid=2 text only, rowid=3 vector only
        assert!(merged.len() <= 3);

        // rowid=1 should rank highest (has both scores)
        if !merged.is_empty() {
            assert_eq!(merged[0].rowid, 1);
        }
    }
}
