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
  - `scripts/install.sh` and `scripts/install.ps1` are served live from `main`, so they fetch the notices only once a release has put them on the CDN (#61). Done once 0.12.3's notices were on the CDN for all three platforms (macOS's by an upload after the release): each script reads `latest.version` from its platform's `update-manifest.json`, refusing anything but `X.Y.Z`, downloads that release's binary (`fts_helper-<ver>`, `.exe` on Windows) and `fts_helper-<ver>-THIRD_PARTY_NOTICES.txt` to `.download` files beside the install, then moves the binary into place and then the notices, so a failed download leaves an earlier install as it was. The binary goes first because Windows refuses to replace a running helper's file: that refusal then leaves the earlier pair, where notices-first left the new notices beside the old binary (and a helper before 0.12.3 never repairs them). If the notices move fails after the binary's, the script exits non-zero, and the new binary puts its own notices there on its next `hello` (the amendment below) once that file can be replaced; a notices file that cannot be replaced at all (a file the system refuses to change) keeps the old notices until the user clears it, which an installer the user runs reports at once. `tests/test_install_scripts.py` runs `install.sh` against stand-in `curl`, `wget` and a `mv` that refuses the binary, and pins `install.ps1`'s steps in its text (no PowerShell on the build machines; the script is run on Windows by hand before a change merges);
  - self-update replaced only the binary; it now installs the new build's notices with it (#62, see the amendment below).

  A consumer of the CDN takes the version the update manifest names and fetches that version's binary and notices: the `-latest` copies are replaced one at a time. Manifests published before the first release with notices name a version that has none, so a consumer that requires the notices (the full Windows TabMail installer build) refuses until that release: release native-fts before the TabMail installers.

## Rejected

- **Embedding the text in the binary behind a flag.** That changes the helper, and the file next to it is the standard form. (#62 does compile the text in, so an updater can take a downloaded build's notices from the build itself; the file next to the helper stays the form users get.)
- **A hand-maintained list.** It drifts with every `cargo update`.
- **Generating at release time.** That needs cargo-about and the network in each CI job, and makes the text unreviewed. Generating in the repo makes every change reviewable in a diff.
- **Leaving proc-macros out.** cargo-about has no filter for them, and crediting them too is harmless.

## Amendment 2026-11-06 (#62): an update installs the new build's notices with its binary

Self-update and the user-local migration replaced only the binary, so the notices beside it described an older build. Now the notices beside an installed helper belong to the installed binary.

- **The binary carries its notices.** `self_update::THIRD_PARTY_NOTICES` is the repository's checked `THIRD_PARTY_NOTICES.txt`, compiled in with `include_str!`. `fts_helper --print-third-party-notices` prints them and exits, before logging and without touching any file.
- **An update installs them as a pair.** After the signature and SHA-256 checks, the updater runs the staged binary with that argument (`notices_of`, bounded by `NOTICES_READ_TIMEOUT_SECS`). The printed notices must credit the target version on their `- tabmail-native-fts <version>` line, which the release check pins to the build. They are staged beside the install. `commit_pair` keeps the installed notices as a backup, renames the staged notices into place, then swaps the binary. If the swap fails, it puts the old notices back (or removes the new ones when there were none). The folder therefore holds the old pair or the new pair. On Windows the apply-update process does the commit after the old helper exits, given `--staged-notices`. A failure before the commit removes both staged files and leaves the installed pair alone, so the update is refused and retried later.
- **A failed swap keeps the binary.** The Windows apply-update process used to move the installed binary aside, then move the staged one in. When the second rename failed, nothing was left at the installed path. A first fix put the binary back, but one refused restore still left the path empty for good, because later attempts saw no installed binary to restore. Now the staged binary replaces the installed one in a single rename (`std::fs::rename`, which replaces an existing file), retried within the existing bound, so the installed path holds a runnable helper whichever attempt fails. Cost: if the old helper is still running when every attempt has been made, the update is refused and retried at the next check. Renaming the running file aside would have let the update succeed, at the risk of an empty path.
- **The migration copies with notices first.** `migrate_from` writes this build's notices beside the user-local copy, then copies the binary, then writes the native-messaging manifests. A failure stops before the later writes, and the next `hello` retries.
- **`hello` makes the notices the installed binary's (amended 2026-11-06).** At startup the helper records which binary file it runs from (`remember_started_binary`: size, modification time, and device and inode on Unix). On `hello`, `reconcile_notices` replaces the notices beside it with this build's when they differ, but only while the file at the helper's path is still that binary. This repairs every state in which the two differ: an install from before the notices existed, an update applied by an updater from before #62 (it swaps only the binary and leaves the previous build's notices), and an update whose rollback could not finish (it also removes the notices backup such an update leaves). A process whose binary has been replaced since it started writes nothing, so an older process still running from the folder after an update never puts its notices back over the installed build's. The write goes to a file only this process uses and is then renamed into place. It writes nothing when the helper is not named `fts_helper`/`fts_helper.exe`: a client that bundles the helper (Voice's `voice-memory`) ships its notices under its own name inside a signed app, and never sends `hello` (ADR-NF-008). A failure is logged and does not fail `hello`.
  - *Why not create-only (the first form of this amendment):* a create-only `hello` could not repair an existing file. So an older updater's update, or a rollback that could not finish, left the binary and its notices different on every later start.
  - *Why no cross-process lock:* there is one remaining window. An old process can check its binary, and then a second process can install an update before the old process renames its notices into place. Only then are the new build's notices overwritten. The installed binary's next `hello` repairs it, and on Unix that is the reconnect that follows every update. A lock shared by the updater, the Windows apply-update process and `hello` would close that window, but it adds a lock file, contention handling and stale-lock recovery to the update path. That is not worth it for a file that heals on the next start (simplicity with eventual correctness).
- **The Windows handoff is one contract.** `apply_update_command` builds the apply-update process's command and `parse_apply_update_args` reads it back; `main` uses the parser, so the launcher and the process cannot drift apart unnoticed. `--staged-notices` is required: the apply-update process is always a copy of the updater that starts it, so an updater from before #62 runs its own old binary, never this parser. Where the swap happens is a parameter of `install_downloaded` (`Apply::Here` on Unix, `Apply::AfterExit` on Windows, chosen in `update_request`), so tests can run the Windows handoff on any platform.
- **Why the update keeps the pair although `hello` repairs (2026-11-06).** Repair on `hello` alone would end every update with a short mismatch, healed by the reconnect. But a notices file that cannot be written (for example, a file the system refuses to change) would leave the new binary next to the old notices for good. With the pair, the binary is not replaced unless its notices went in first. That is the owner's requirement that a failure leaves no half-applied update, and it is the design the owner approved.

Why this and not a download:
- **Integrity.** The notices are part of the signed, SHA-256-checked binary, so they are exactly as trustworthy as it is.
- **No release-side change.** A separate download would need a new manifest field and a new signed-message format, with old helpers still verifying the old one. The update manifest is unchanged here, and the release flows already refuse a stale notices file before building anything they ship.

Running the downloaded build before the swap adds one check to the update: a build that cannot print notices crediting its own version is not installed. Cost: the binary grows by the size of the file (about 240 KB).
