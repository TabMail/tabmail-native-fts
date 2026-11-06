# ADR-NF-024: One Generated Notices File Ships With Every Form of the Helper

**Status:** Accepted · **Date:** 2026-11-06 · **Issue:** #61

## Context

The helper links about 230 crates and compiles four pieces of C code:
- SQLite, bundled by `libsqlite3-sys`;
- Oniguruma, bundled by `onig_sys`;
- sqlite-vec;
- the BoringSSL and OpenSSL code in `ring`.

It also downloads the all-MiniLM-L6-v2 model. No distributed form carried their notices. Those forms are:
- the macOS and Windows installers;
- the Linux binary;
- the bare update binaries on the CDN;
- Voice's `voice-memory`.

`THIRD_PARTY_LICENSES.md` named three components and was not shipped. BSD-2/3, Unicode-3.0 and CDLA-Permissive-2.0 explicitly require the notice or text with binary copies. MIT, ISC and Apache-2.0 §4 require it with copies.

## Decision

- **Generation.** `THIRD_PARTY_NOTICES.txt` is generated from `Cargo.lock` by cargo-about 0.9.3, pinned, through `scripts/generate-third-party-notices.sh`. It uses `about.toml` and `about.hbs` and is committed.
  - It covers the union of every release target: macOS, Windows and Linux, x64 and arm64.
  - It leaves out build and dev dependencies.
  - Where a crate offers a choice of licenses, the first match in `about.toml`'s `accepted` order is reproduced (MIT first).
- **Clarifications.** Some crates ship no license file, or one the scanner misreads:
  - none: candle-nn, candle-transformers, ug, sqlite-vec;
  - misread: indented windows `license-mit`, chrono's and libm's combined files, curve25519-dalek;
  - combined C: ring, onig_sys with Oniguruma's `COPYING`, libsqlite3-sys with SQLite's blessing.

  Each of these gets an `about.toml` clarification that pins its real file by SHA-256. Files missing from the crate come from the crate's repository at the published commit or tag. The vendored candle-core carries a copy of candle's `LICENSE-MIT`.
- **Hand-written part.** Part 1 of the template credits what is not a crate:
  - the Rust standard library;
  - the model, its MIT base model (Microsoft MiniLM) and BERT's vocabulary;
  - the source offer for the helper itself (MPL-2.0 §3.2).
- **Check.** `scripts/check-third-party-notices.py` (stdlib; `cargo tree --offline --locked` over about.toml's targets) fails when the file:
  - misses a shipped crate at its exact version;
  - holds cargo-about's generic MIT/BSD placeholders.

  It runs in the tests, and the release scripts run it before building.
- **Shipping.** The file ships next to the helper in every installer and is uploaded next to every update binary. Voice ships it from its pinned checkout as-is.

## Rejected

- **Embedding the text in the binary behind a flag.** That changes the helper, and the file next to it is the standard form.
- **A hand-maintained list.** It drifts with every `cargo update`.
- **Generating at release time.** That needs cargo-about and the network in each CI job, and makes the text unreviewed. Generating in the repo makes every change reviewable in a diff.
- **Leaving proc-macros out.** cargo-about has no filter for them, and crediting them too is harmless.
