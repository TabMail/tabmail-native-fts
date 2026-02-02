// embeddings/ — Local sentence embedding engine using candle (pure Rust).
//
// Provides:
// - Model download + SHA256 verification
// - BERT inference with mean pooling
// - Text preparation for email and memory entries

pub mod download;
pub mod engine;
pub mod text_prep;
