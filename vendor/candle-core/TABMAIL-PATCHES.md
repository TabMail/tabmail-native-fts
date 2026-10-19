# TabMail changes to this copy of candle-core

This is candle-core 0.8.4 as published on crates.io (upstream commit
`468d1d525fe206a35d6962c02cfa7b9918b31076`, see `.cargo_vcs_info.json`), licensed under Apache 2.0
or MIT (`Cargo.toml`'s `license`; the crate ships only the Apache text, in `LICENSE`, and the MIT
text is at https://opensource.org/license/mit). Native FTS uses it through `[patch.crates-io]` in its own `Cargo.toml`.

Why it is here: candle-core 0.8.4 requires Zip 1.x, and Zip releases before 9.0.0 carry the
archive-extraction advisories RUSTSEC-2026-0367, 0368 and 0369. No candle release depends on Zip 9
yet, and moving to a newer candle would change the embedding engine. This copy keeps candle-core
0.8.4 and moves it to Zip 9, so the helper ships with no known advisories and the same behaviour.

Changes, and nothing else:

- `Cargo.toml`: requires `zip` 9.0.5 instead of 1.1.1.
- `src/npy.rs`, `src/pickle.rs`: Zip 9 returns the entry name as a `Result`; the error is passed
  on, as in the open upstream fix for Zip 9 (huggingface/candle#4041).
- `src/layout.rs`, `src/quantized/mod.rs`, `src/safetensors.rs`, `src/tensor.rs`: the elided
  lifetimes are written out (`'_`) so the crate builds without warnings on current Rust. Signatures
  only.
- The crate's own `Cargo.lock` and Cargo's extraction marker were left out; Cargo does not read
  them for a dependency.

Each changed source file starts with a one-line notice. Remove this copy, and the patch entry, once
a candle release that Native FTS moves to depends on Zip 9 or later.
