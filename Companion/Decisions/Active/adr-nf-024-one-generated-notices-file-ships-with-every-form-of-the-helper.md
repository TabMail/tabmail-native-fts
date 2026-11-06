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
  - combined C: ring (with fiat-crypto's `third_party/fiat/LICENSE`), onig_sys with Oniguruma's `COPYING`, libsqlite3-sys with SQLite's blessing;
  - per-file ISC notices: ring's files from BoringSSL and other contributors (Google, Intel, Simon Sapin, Trent Clarke, David Judd) and two rustls-webpki files (Daniel McCarney, Rafael Fernández López) carry their own copyright line; one slice (`start`/`end`) per distinct notice;
  - a nested license: regex-syntax's `src/unicode_tables/LICENSE-UNICODE` (Unicode-DFS-2016) for its compiled-in Unicode tables.

  Each of these gets an `about.toml` clarification that pins its real file by SHA-256. Files missing from the crate come from the crate's repository at the published commit or tag. The vendored candle-core carries a copy of candle's `LICENSE-MIT`.
- **Hand-written part.** Part 1 of the template credits what is not a crate:
  - the Rust standard library;
  - the model, its MIT base model (Microsoft MiniLM) and BERT's vocabulary;
  - the source offer for the helper itself (MPL-2.0 §3.2).
- **Check.** `scripts/check-third-party-notices.py` (stdlib; `cargo tree --locked` over all of about.toml's targets at once) fails when the file:
  - misses a shipped crate at its exact version;
  - holds cargo-about's generic MIT/BSD placeholders;
  - has a license section with no text;
  - does not credit a clarified crate under every license its pinned files name, and in as many sections as it has pinned files (each file has its own text).

  The last rule exists because cargo-about does not fail on a changed pinned file: it warns, ignores that clarification and falls back to the crate's own files. For onig_sys that drops Oniguruma's BSD-2 notice and for libsqlite3-sys the SQLite blessing, while the crate is still credited (under MIT). Each section heading carries its SPDX id so the checker can map licenses to sections.

  The query is not `--offline`: a release builder has cached only its own platform's crates, so an offline query over all six targets fails on a fresh cache. Cargo fetches the rest.

  It runs in the tests and in the generator. Every release flow runs it after building and before it uploads or packages anything, so a stale file stops the release (on macOS, after notarization). Every version bump needs a regeneration, because the file credits the helper itself at its version; `maintenance.sh --bump`/`--release` does it.
- **Shipping.** The file ships next to the helper in every installer package (the macOS `.pkg`, the Windows `Setup.exe`, the full TabMail macOS and Windows installers) and is uploaded to the CDN next to every update binary, before the update manifest. Voice is to ship it from its pinned checkout as-is, in its own change once this one merges. Two forms follow in later changes, because each needs the file on the CDN first or a helper change:
  - `scripts/install.sh` and `scripts/install.ps1` are served live from `main`, so they fetch the notices only once a release has put them on the CDN (#61);
  - self-update replaces only the binary; installing the new version's notices beside it is #62.

  A consumer of the CDN takes the version the update manifest names and fetches that version's binary and notices: the `-latest` copies are replaced one at a time. Manifests published before the first release with notices name a version that has none, so a consumer that requires the notices (the full Windows TabMail installer build) refuses until that release: release native-fts before the TabMail installers.

## Rejected

- **Embedding the text in the binary behind a flag.** That changes the helper, and the file next to it is the standard form.
- **A hand-maintained list.** It drifts with every `cargo update`.
- **Generating at release time.** That needs cargo-about and the network in each CI job, and makes the text unreviewed. Generating in the repo makes every change reviewable in a diff.
- **Leaving proc-macros out.** cargo-about has no filter for them, and crediting them too is harmless.
