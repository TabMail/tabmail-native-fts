# ADR-NF-010: Candle-core 0.8.4 Is Vendored and Moved to Zip 9

**Status:** Accepted (2026-10-19). Callers: none change; the wire protocol and every reply are the same.

## Context

`cargo audit` reports RUSTSEC-2026-0367, 0368 and 0369 against Zip 1.1.4, which the helper gets only
through candle-core 0.8.4 (`zip ^1.1.1`). All three concern archive extraction (privileged
permission bits, symlinks); the fix is Zip 9.0.0 or later. The helper never extracts an archive:
the embedding engine loads safetensors, and candle-core's Zip readers serve its NumPy (`.npz`) and
PyTorch (`.pth`) loaders, which the helper does not call. So the advisories are unreachable, but the
audit is not clean, and the helper is shipped to users.

No candle release depends on Zip 9: candle 0.11.0 still requires Zip 8. Moving to a newer candle
would also change the embedding engine, which the compatible-only rule for this helper rules out.

## Decision

Keep candle-core 0.8.4 and move it to Zip 9, in a copy in `vendor/candle-core` used through
`[patch.crates-io]` in `Cargo.toml`.

- The copy is the crates.io release (upstream commit in `.cargo_vcs_info.json`, `LICENSE` kept).
- Changes: `zip` 9.0.5 instead of 1.1.1; Zip 9 returns an entry's name as a `Result`, and the
  NumPy and PyTorch loaders pass its error on (as the open upstream fix, huggingface/candle#4041,
  does); elided lifetimes written out so the crate builds without warnings. Each changed file
  carries a one-line notice, and `vendor/candle-core/TABMAIL-PATCHES.md` lists every change.
- The copy's own `Cargo.lock` is left out: Cargo does not read it, and dependency scanners would
  report the old versions it names.
- `Cargo.lock` changes only by Zip 9 and its dependencies: `zip` 9.0.5, `typed-path`, `nix` and
  `cfg_aliases` in; Zip 1's `arbitrary`, `num_enum`, `proc-macro-crate` and `toml_*` out.

**Remove the copy** (and the patch entry) once Native FTS moves to a candle release that depends on
Zip 9 or later.

**Builders:** pass `--remap-path-prefix=$HOME=~` in `RUSTFLAGS` for a shipped build. Without it a
binary carries the builder's home folder (its user name) in every dependency's source path; Cargo's
`trim-paths` profile setting, which would do this from `Cargo.toml`, is not stable yet.

## Verification

- `cargo audit`: no vulnerabilities; one allowed warning, Paste unmaintained (RUSTSEC-2024-0436),
  through candle's `gemm`, used at compile time only. It stays until candle drops it.
- `cargo test --release --locked`: 132 passed. `tests/run_tests.py`: 44 ran, OK (2 platform skips).
- The released 0.11.7 against this build on 1,161 recorded Thunderbird 1.9.1 requests: no content
  differences.
- With the remap, the macOS binary holds no home-folder path (350 before).

## Rejected

- **Candle 0.11 or later:** still Zip 8, and a new embedding engine.
- **Leaving the advisories, recorded as unreachable:** the audit is the release check, and an
  audit that always fails stops being read.
- **A fork on GitHub:** a second repository to keep for three small changes; the copy keeps them
  reviewable here.
