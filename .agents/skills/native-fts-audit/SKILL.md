---
name: native-fts-audit
description: Run the Rust dependency audit for tabmail-native-fts (`maintenance.sh --audit` / `cargo audit`), but layered with a supply-chain preflight — web-check for active crates.io / RustSec campaigns, cross-reference any compromised crate names against `Cargo.lock`, verify the fix-target crate's maintainer + publish-date legitimacy, then patch via `maintenance.sh --update` (or escalate to a full release via the `tabmail-release` skill if shipping is needed). Use when the user asks to "audit native-fts", "run maintenance.sh --audit", "cargo audit returned X", "check rust deps for vulns", "patch native-fts deps", or similar. Does NOT cut a release — for that, hand off to `tabmail-release`. This skill is the npm/CF-worker counterpart `deps-supply-chain-audit` but for the Rust side.
---

# TabMail Native-FTS Dependency Audit (supply-chain-aware)

This skill is the **Rust analog** of `deps-supply-chain-audit` (which covers the npm/Cloudflare-worker side). Both share the same supply-chain preflight discipline: never trust `cargo update` blindly during an active registry-compromise campaign.

The single repo this skill operates on is `tabmail-native-fts/` (Rust crate that ships the FTS+semantic search binary used by the Thunderbird add-on; not linked into iOS — iOS has its own Swift-native FTS). The script that orchestrates the audit is `tabmail-release-helpers/tb-native-fts/maintenance.sh`.

**When to defer to a sibling skill instead:**
- User wants to **ship a fix** (release a new native-fts version to the CDN + GH Actions Linux/Windows builds) → `tabmail-release` skill. That skill already runs `maintenance.sh --release` end-to-end. This skill stops short of releasing.
- User wants to audit npm/CF-worker deps → `deps-supply-chain-audit` skill.

---

## Stage 0 — Run the audit

```bash
cd tabmail-release-helpers/tb-native-fts
./maintenance.sh --audit       # explicit
# or:
./maintenance.sh               # audit is the default with no flags
```

Behind the scenes this runs `cargo audit` inside `tabmail-native-fts/` against `Cargo.lock`. (The script auto-installs `cargo-audit` via `cargo install cargo-audit` if missing.)

Each finding gives:
- **RUSTSEC-YYYY-NNNN** advisory ID (link: `https://rustsec.org/advisories/RUSTSEC-YYYY-NNNN`)
- Vulnerable crate + version range
- Patched version range
- Severity / scope (informational warnings vs. unmaintained vs. actual CVE)

Group by **distinct vulnerable crate**, mirroring the npm flow. Often the same transitive crate (e.g., `paste`, `time`, `chrono`, an `openssl-sys` line) appears across multiple direct deps.

### Distinguish three classes of finding

1. **Vulnerability (`vulnerability:`)** — a real CVE. Needs a patch.
2. **Unmaintained (`unmaintained:`)** — informational. The `paste 1.0.15` warning is the canonical example — transitive via `tokenizers` / `candle` / `gemm`, upstream problem, safe to ignore (per `tabmail-release` skill Gotchas).
3. **Yanked version** — a release the maintainer pulled from crates.io. Usually means the lockfile pins a yanked version; `cargo update` will move past it.

Only class (1) requires patching urgency. Surface (2) and (3) to the user but don't block on them.

---

## Stage 1 — Supply-chain preflight (MANDATORY before any `cargo update`)

The Rust ecosystem has historically seen fewer worm-style attacks than npm, but typosquats and account takeovers do happen — `rustdecimal` (Aug 2022) and the recurring `serde_*` typosquats are reminders. Always preflight before updating.

### 1a. Web-search for recent crates.io / Rust ecosystem attacks

```
"crates.io supply chain attack <current month> <current year>"
"RustSec advisory <year> malicious crate typosquat"
"rust supply chain compromise <year>"
```

Authoritative sources:
- **RustSec advisory DB** (`rustsec.org/advisories/`) — official source.
- **GitHub Security Advisories — Rust** (`github.com/advisories?query=ecosystem%3Acargo`).
- **Wiz / Snyk / StepSecurity / Socket.dev** — broader supply-chain reporting; coverage of Rust attacks is lighter than npm but still present.
- **rust-lang security blog** (`blog.rust-lang.org/inside-rust/`) — formal disclosures.

For any campaign in the last ~30 days, **WebFetch the post** and extract the named-compromised crate list (scoped or not — Rust has no scope namespace, all crate names are unscoped).

### 1b. Cross-reference against Cargo.lock

```bash
cd tabmail-native-fts
grep -E 'name = "(<compromised-name-1>|<compromised-name-2>|...)"' Cargo.lock
```

If **any** match → STOP. Surface to the user. Treat this as an incident, not a routine patch:
- Which compromised crate, which version is pinned
- Build artifacts produced since the install may already be tainted (esp. if it ships in the user's `cargo install`-ed tooling)
- Don't rotate / re-run anything that could exfiltrate creds until the user sequences a response

If **no match** → proceed to 1c.

### 1c. Verify the fix-target's legitimacy

For each crate that `cargo audit` recommends upgrading, sanity-check the patched version on crates.io. `cargo` has no native equivalent of the npm registry JSON dump, but you can `curl`:

```bash
curl -s "https://crates.io/api/v1/crates/<pkg>" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('latest:', d['crate']['max_stable_version'])
print('updated:', d['crate']['updated_at'])
# Top recent versions:
for v in d['versions'][:8]:
    print(f\"{v['num']}: published {v['created_at']} by id={v['published_by']['id'] if v['published_by'] else None}\")
"
```

Verify:
1. **Recent versions all published by the historical maintainer set.** If a brand-new publisher appears in the last 30 days alongside the version `cargo audit` wants you to bump to, that's a red flag.
2. **Patched version published BEFORE the most-recent named attack date.** A version published mid-attack (and especially the *immediate* next-version after a known-compromised crate) is suspect.
3. **No unusual publish bursts** — if a crate normally publishes monthly and just shipped 4 versions in 6 hours, investigate.

If any check trips → STOP. Pin to a known-clean older version via Cargo.toml constraint instead (`crate = "=1.2.3"`), and surface to the user.

---

## Stage 2 — Patch (only after preflight is clean)

The minimal action — `cargo update` — refreshes `Cargo.lock` within the semver constraints already in `Cargo.toml`:

```bash
cd tabmail-release-helpers/tb-native-fts
./maintenance.sh --update
```

This script runs (in order):
1. `cargo update` (lockfile refresh)
2. `cargo audit` (verify the bump cleared the advisory)
3. `cargo check` (build sanity)

If `cargo audit` still reports the vuln after `cargo update`, that means the constraint in `Cargo.toml` doesn't allow the patched version — a **breaking** upgrade is needed:

```bash
# Edit Cargo.toml manually, bump the constraint, e.g. "1.2" -> "1.3" or "~1.2.5"
# Then:
cd tabmail-native-fts
cargo update
cargo audit
cargo check
```

For breaking upgrades, `cargo test` is also strongly advised since semver-breaking changes can shift behavior:

```bash
cd tabmail-native-fts && cargo test
```

---

## Stage 3 — Release

**Cargo.lock is tracked** (committed in `6cc973a`, 2026-07-07 — no longer gitignored). The fix in Cargo.lock is the canonical dep snapshot for the release, and `maintenance.sh --release` now stages it alongside Cargo.toml and src/config.rs.

**Default action: run the release end-to-end from this skill.** Request scoped escalated execution for operations that need host keychain access or network access: commit signing, push, macOS notarization, GitHub release creation, and GitHub Actions triggers. There is no manual step unless approval is denied or the required signing key is unavailable.

```bash
cd /Users/kwang/Work/GitData/tabmail/tabmail-release-helpers/tb-native-fts
./maintenance.sh --release
```
Run with scoped escalated execution after obtaining approval. This script is the single source of truth — don't hand-roll any of its steps.

### Decision matrix

| Situation | Action |
|-----------|--------|
| Audit clean after `--update`, no Cargo.toml change, warnings only (unmaintained/unsound) | Note it; warnings don't ship exploitable code. **Still run the release** to ship the Cargo.lock bump — it's quick and keeps distributed binaries current. |
| Audit clean after `--update`, **Cargo.toml constraint bumped** | Release as usual; the constraint change is already staged by maintenance.sh. |
| Audit clean after `--update`, **real CVE (vulnerability:)** | Release. Any real CVE warrants shipping. |
| Audit still red after `--update` AND a breaking Cargo.toml change | Patch the source for any breakage, run `cargo test`, then release. |
| The vuln is in an unmaintained-but-still-functional crate with no patched version | Open an issue upstream / consider replacing the crate. Don't ship a half-fix. |

### What `--release` does end-to-end (do NOT replicate manually)

1. `cargo update` + `cargo audit`
2. Bump patch version in Cargo.toml + src/config.rs
3. `cargo check`
4. `git add Cargo.toml src/config.rs Cargo.lock` → `git commit -s` → `git push origin main`
5. `build-mac-local.sh` — universal macOS binary (~100s)
6. `release-macos-universal-local.sh` — sign, notarize, upload binary + .pkg to CDN
7. GH CLI triggers Linux + Windows GH Actions builds; waits for completion

---

## Stage 4 — Commit + push (only if Cargo.toml changed outside of a release)

This only applies to the unusual case where you need to commit a Cargo.toml constraint bump WITHOUT running the full release (e.g. the user explicitly says "don't release yet").

```bash
cd tabmail-native-fts
git status --short
```

Expected: `M Cargo.toml` (and optionally `M src/config.rs` if version was bumped, `M Cargo.lock` since it's now tracked). If there's unexpected WIP, surface it and don't bundle it.

```bash
git add Cargo.toml Cargo.lock      # and src/config.rs if version was also bumped
git commit -s -m "<see message below>"
env -u DISPLAY git push origin main    # sandbox OFF for push
```

Commit message style:

```
fix(deps): bump <crate> constraint to address <advisory-id>

Tighten Cargo.toml constraint from <old> to <new> to ensure the patched
version of <crate> is selected. <crate> is a [direct|transitive] dep
[via <parent-crate>]. Verified upstream crate is uncompromised by the
<YYYY-MM-DD> <campaign-name> crates.io campaign (<crate> not in scope;
maintainer-set unchanged; <patched-version> published <date>).

```

Commits: `-s` always (DCO); `env -u DISPLAY` for push (ssh UseKeychain); request scoped escalated execution for signing and network operations.

---

## Stage 5 — Wrap-up to user

Brief summary:
- Vulns found (advisory IDs + crates).
- Supply-chain preflight findings: named campaigns in last 30 days, whether anything in our `Cargo.lock` was on a compromised list.
- What was patched (`cargo update` lockfile-only, or Cargo.toml constraint bump too).
- Disposition: deferred (next routine release), committed standalone, or escalated to `tabmail-release` skill.
- Any unmaintained-but-unfixed advisories (carry-forward known issues).

Format: tight bullet list, no preamble.

---

## Anti-patterns to avoid

- **Blind `cargo update` mid-campaign.** Same hazard as `npm audit fix` — pulls whatever satisfies the constraint, even a hijacked patch. Always preflight.
- **Trusting `cargo audit` exclusively.** It reads RustSec, which lags real-time disclosure. Cross-reference web sources for the last 30 days.
- **Re-running `./maintenance.sh --release` after a partial failure.** It re-bumps the version on each invocation. Resume manually instead — see tabmail-release skill Gotchas.
- **Forgetting scoped escalation on commands that push/sign.** Commit signing needs the ed25519 key in the ssh-agent; push needs ssh UseKeychain; both can fail in the default sandbox. Request approval for git commit/push and the release script when host access is required.
- **Using `timeout`/`gtimeout` wrappers around maintenance.sh.** GNU timeout severs the Mach bootstrap port, breaking keychain access for notarization. Let maintenance.sh run to completion in a persistent terminal session and poll it if needed.
- **Patching a crate to fix one CVE and ignoring the same crate's other open advisories.** `cargo audit` lists all findings — read the whole report, not just the first one.
- **Tee'd command exit codes.** `./maintenance.sh --update 2>&1 | tee log` reports tee's exit, not the script's. Always verify with `git status` / re-running `cargo audit` afterwards.
- **Treating "unmaintained" the same as "vulnerable".** Unmaintained is a planning concern (find a replacement before the crate goes away); vulnerable is a security concern (patch now).

---

## Worked example skeleton (no real run yet)

```
maintenance.sh --audit
→ 1 advisory: RUSTSEC-2026-XXXX in crate <foo> 0.4.x, patched in 0.4.7

Stage 1 — preflight
→ No active crates.io campaigns in last 30 days (web search clean)
→ Cargo.lock contains <foo> 0.4.3; not on any compromised list
→ crates.io: <foo> 0.4.7 published 2026-XX-XX by historical maintainer

Stage 2 — patch
→ ./maintenance.sh --update
→ cargo update bumped <foo> to 0.4.8
→ cargo audit: clean
→ cargo check: ok

Stage 3 — decide
→ Cargo.toml unchanged (constraint already allowed 0.4.x)
→ Cargo.lock gitignored, nothing to commit
→ Disposition: defer to next release (next maintenance.sh --release run will pick this up)
```

---

## See also

- **`deps-supply-chain-audit`** skill — same workflow for npm / Cloudflare workers.
- **`tabmail-release`** skill — the full ship-it path (`maintenance.sh --release` end-to-end).
- **`tabmail-release-helpers/tb-native-fts/maintenance.sh`** — the orchestrator script. Read it before doing anything non-obvious; it's the source of truth on what the audit / update / release flows actually do.
