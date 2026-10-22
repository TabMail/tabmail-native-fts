# October 21, 2026 TLS dependency audit

The weekly scan of the clean v0.11.8 checkout found two RustSec advisories for
one CVE, CVE-2026-111943. They are vulnerabilities, with no CVSS score supplied
in the RustSec records; do not describe them as two critical CVEs.

- [RUSTSEC-2026-0382](https://rustsec.org/advisories/RUSTSEC-2026-0382.html):
  `rustls-webpki` 0.103.15; patched stable version is 0.103.16.
- [RUSTSEC-2026-0381](https://rustsec.org/advisories/RUSTSEC-2026-0381.html):
  `webpki-roots` 1.0.9; patched version is 1.0.10. The separately resolved
  0.26.11 compatibility wrapper does not fix an affected 1.x dependency.

Both concern trust in distrusted emSign intermediates whose private keys are
publicly known. `cargo tree --locked -i rustls-webpki` places the dependency
under `rustls` / `ureq`, so this is the helper's HTTP/TLS dependency path.
The tracked lockfile matches HEAD; GitHub's latest release metadata is v0.11.8.

The registry lists the patched versions as published October 20 by `ctz` and
October 19 by `cpu`, respectively; both names also published historical
versions. The campaign-name cross-check found no matches in Cargo.lock.
The existing Paste unmaintained warning remains separate.

The security PR updates only these two packages and the compatibility wrapper's
resolved reference in Cargo.lock. The same 151 release-mode tests pass before and
after; cargo audit then reports zero vulnerabilities and the existing Paste
unmaintained warning. A signed release through the canonical maintenance/release
workflow is still needed to ship the fix to installed users.
