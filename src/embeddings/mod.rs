/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// embeddings/ — Local sentence embedding engine using candle (pure Rust).
//
// Provides:
// - Model download + SHA256 verification
// - BERT inference with mean pooling
// - Text preparation for email and memory entries

pub mod download;
pub mod engine;
pub mod text_prep;
