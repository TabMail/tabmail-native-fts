# Candle/Zip dependency maintenance and security constraint

At the initial 2026-10-16 check, `./maintenance.sh --update` failed because Candle 0.8 requires `zip ^1.1.1` and the allowed versions were yanked. crates.io subsequently un-yanked Zip 1.x; that resolver blocker no longer applies. The tracked lockfile selects Zip 1.1.4.

RustSec then published [RUSTSEC-2026-0367](https://rustsec.org/advisories/RUSTSEC-2026-0367.html), [0368](https://rustsec.org/advisories/RUSTSEC-2026-0368.html), and [0369](https://rustsec.org/advisories/RUSTSEC-2026-0369.html) on 2026-10-17 UTC (2026-10-16 Pacific). They concern privileged permission bits and symlink handling during archive extraction; all require Zip >=9.0.0. The current audit therefore reports three Zip vulnerabilities plus the unmaintained Paste warning. An in-range Zip 1.x update cannot clear them. Even Candle 0.11.0 requires Zip ^8.6.0, below the patched major; a clean audit needs an upstream dependency change or a separately validated compatibility migration.

No archive-extraction calls were found in TabMail's native source or Candle 0.8.4. The embedding engine loads safetensors; Candle's Zip readers serve its unused pickle/NumPy loaders. This is a reachability assessment, not an advisory exemption: retain the findings in audit reports and recheck if model-loading paths change.

The independent Rustls 0.23.46 security update changes only its lockfile version and checksum and clears RUSTSEC-2026-0346 through 0349. All 106 native unit tests pass before and after that update.
