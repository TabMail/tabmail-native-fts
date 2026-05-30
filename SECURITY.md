# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in the TabMail Native FTS Helper,
please report it privately. **Do not open a public issue.**

Email **security@tabmail.ai** with:

- A description of the vulnerability and its impact
- Steps to reproduce (proof-of-concept if possible)
- The affected version (`fts_helper` reports its version on startup; see
  `HOST_VERSION` in `src/config.rs`)

We aim to acknowledge reports within 72 hours and to provide a remediation
timeline after triage.

## Scope

This repository covers the native-messaging FTS host binary only. Vulnerabilities
in the Thunderbird add-on, the iOS app, or the backend service should be reported
through their respective repositories or to the same address with the component
named.

## Disclosure

We follow coordinated disclosure. Please give us a reasonable window to ship a
fix before any public disclosure. There is no paid bug-bounty program at this
time; we are grateful for responsible reports and will credit reporters who wish
to be acknowledged.
