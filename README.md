# TabMail Native FTS Helper

Local **hybrid search** (keyword + meaning) for TabMail, in one self-contained Rust binary per OS.

- **Keyword search:** bundled SQLite with FTS5, Porter stemming and email-aware synonyms.
- **Meaning search:** sentence embeddings computed on your computer with `all-MiniLM-L6-v2`
  (candle, pure Rust), stored with sqlite-vec.
- **Private by design:** your mail and chat history are indexed and searched locally. Nothing is
  sent anywhere to search it.

## Used by

| App | What it searches | How it ships the helper |
|-----|------------------|-------------------------|
| **[TabMail for Thunderbird](https://github.com/TabMail/tabmail-thunderbird)** | Your email, and the assistant's chat history (memory) | Installed next to Thunderbird as a native-messaging host; updates itself |
| **[TabMail Voice](https://github.com/TabMail/tabmail-voice)** | Agent mode's earlier requests (memory) | Built from this repository at a pinned commit and bundled inside the app as `voice-memory`; updated with the app |

The two never share files. TabMail Voice runs its own copy with its own database, model and log
folders (see [Caller-named locations](#caller-named-locations)), and never touches Thunderbird's
copy or its data.

## Contents

- [Architecture](#architecture)
- [Installation](#installation)
- [Building from source](#building-from-source)
- [Testing](#testing)
- [Native messaging manifest](#native-messaging-manifest)
- [Installation paths](#installation-paths)
- [Database storage](#database-storage)
- [Protocol notes](#protocol-notes)
- [Logs](#logs)
- [Self-update](#self-update)
- [Search quality](#search-quality)
- [Performance tuning](#performance-tuning)
- [License](#license) · [Contributing](#contributing) · [Trademarks](#trademarks)

---

## Architecture

```
TabMail for Thunderbird (JavaScript)      TabMail Voice (Electron)
        ↕ Native Messaging API                   ↕ same wire format, own process
          (stdin/stdout, 4-byte length + JSON)
                    Rust helper binary (`fts_helper`)
                        ↕ direct SQLite access
          Email database (`fts.db`) and memory database (`memory.db`)
```

Each caller starts its own helper process and gets its own databases.

## Installation

**For end users:** use the TabMail installer (macOS `.pkg`, Windows `.exe`) or the install scripts
below. The helper is installed automatically and moves itself to a user-local folder on first run,
so it can update itself. TabMail Voice users need nothing extra: the app ships its own copy.

**Quick install (no admin required):**

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/TabMail/tabmail-native-fts/main/scripts/install.sh | bash

# Windows (PowerShell)
irm https://raw.githubusercontent.com/TabMail/tabmail-native-fts/main/scripts/install.ps1 | iex
```

The install scripts live in this repo at [`scripts/install.sh`](scripts/install.sh) and
[`scripts/install.ps1`](scripts/install.ps1). Read them before piping to a shell. Each downloads
the prebuilt `fts_helper` binary for your platform from the CDN and registers it as a Thunderbird
native-messaging host.

## Building from source

```bash
# Install the Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build the release binary
cd tabmail-native-fts
cargo build --release

# The binary is at ./target/release/fts_helper
```

## Testing

```bash
# Build the Rust binary first
cargo build --release

# Run all tests
python3 tests/run_tests.py

# Rust helper tests only
python3 tests/run_tests.py rust

# Update mechanism tests only
python3 tests/run_tests.py update

# Verbose output
python3 tests/run_tests.py -v
```

## Third-party notices

`THIRD_PARTY_NOTICES.txt` is generated from `Cargo.lock` by
[cargo-about](https://github.com/EmbarkStudios/cargo-about) (`about.toml`, `about.hbs`). After any
dependency change, generate it again and commit it:

```bash
cargo install cargo-about --version 0.9.3 --locked --features cli   # once
scripts/generate-third-party-notices.sh
```

`scripts/check-third-party-notices.py` (also run by the tests) fails if a crate the helper is built
from is missing from the file, or if a crate's entry fell back to a license template with no
copyright line. A crate that ships no license file, or one the scanner cannot read, gets a
clarification in `about.toml` that pins its real file by checksum. If that file changes, cargo-about
ignores the clarification (it only warns), and the checker fails because the crate is no longer
credited under every license the clarification names: read the new file, then update the checksum.

## Native messaging manifest

The manifest tells Thunderbird where to find the helper binary. Each platform's installer writes
it with the correct path. (TabMail Voice registers no manifest; it starts its bundled copy
directly.)

```json
{
  "name": "tabmail_fts",
  "description": "TabMail FTS Native Helper",
  "path": "/path/to/fts_helper",
  "type": "stdio",
  "allowed_extensions": ["thunderbird@tabmail.ai"]
}
```

| Platform | User-local manifest path |
|----------|--------------------------|
| macOS | `~/Library/Application Support/Mozilla/NativeMessagingHosts/tabmail_fts.json` |
| Linux | `~/.mozilla/native-messaging-hosts/tabmail_fts.json` |
| Windows | Registry: `HKCU\Software\Mozilla\NativeMessagingHosts\tabmail_fts` → path to JSON |

## Installation paths

| | macOS | Linux | Windows |
|-|-------|-------|---------|
| **User-local** (auto-update enabled) | `~/Library/Application Support/TabMail/native/fts_helper` | `~/.local/share/tabmail/native/fts_helper` | `%LOCALAPPDATA%\TabMail\native\fts_helper.exe` |
| **System-wide** (installer location, fallback only) | `/Applications/TabMail.app/Contents/Resources/fts_helper` | `/opt/tabmail/fts_helper` | `C:\Program Files\TabMail\native\fts_helper.exe` |

## Database storage

For Thunderbird, the databases live in the **Thunderbird profile's extension data folder**. Each
Thunderbird profile gets its own isolated email and memory databases.

| Platform | Folder |
|----------|--------|
| macOS | `~/Library/Thunderbird/Profiles/<profile>/browser-extension-data/thunderbird@tabmail.ai/tabmail_fts/` |
| Linux | `~/.thunderbird/<profile>/browser-extension-data/thunderbird@tabmail.ai/tabmail_fts/` |
| Windows | `%APPDATA%\Thunderbird\Profiles\<profile>\browser-extension-data\thunderbird@tabmail.ai\tabmail_fts\` |

- **`fts.db`**: the email index.
- **`memory.db`**: chat history for the assistant's memory features:
  - **Memory search:** find past conversations by keyword (FTS5 with stemming and synonyms) and
    by meaning.
  - **Memory read:** retrieve full chat sessions by timestamp, for context continuity.

TabMail Voice keeps only a memory database, in its own data folder (`memory/database`), and its
model in `memory/models`.

*Note: the helper automatically migrates databases from the old location
(`<profile>/tabmail_fts/`) to the new one on first run.*

## Protocol notes

### Memory search previews

`memorySearch` takes an optional `previewChars` (a positive integer): each result's `content` and
`snippet` then come back cut to that many characters, so a reply of long entries stays small
(Thunderbird refuses a native message over 1 MiB). The stored entries are unchanged. See ADR-NF-012
in `DECISIONS.md`. Helpers with this support advertise `capabilities.memoryPreviewV1` in `hello`,
and those whose dated memory search filters by date before taking its limit advertise
`capabilities.memoryFilterBeforeLimitV1` (ADR-NF-018).

### Result limits

A `search` or `memorySearch` with a query returns at most 512 results, whatever `limit` it is
given, so it stays a hybrid (keyword and meaning) search; narrow it by dates instead. A memory
listing (no query) returns the page asked for. See ADR-NF-013.

### Sorting

`search` takes an optional `sort`: `relevance` (the default), `date_desc` or `date_asc`; any
other value is refused. The helper sorts the matches, then takes `limit`: the best matches first
(equal ones newer first), or the newest or oldest matches (equal dates the better match first). A
date sort walks the dates in windows from the end it starts at, so it is exact however many matches
there are (ADR-NF-023). A query of only fields (`from:"alice@example.com"`) is answered by keyword
over every match, newest first by default. Memory search orders equal matches newer first. See
ADR-NF-017. Helpers with `sort` advertise `capabilities.searchSortV1` in `hello`.

### Memory size limit

A client can bound the memory database: `memoryIndexBatch` takes an optional `maxBytes`, and
`memoryTrim { maxBytes }` applies a changed limit.

- A trim stops asking for more work after about 20 s, and can finish the step under way (at 5 GiB
  a call ran at most about 2 s past that). One cut short reports `withinLimit: false`, and the helper
  finishes it while no request is waiting, in steps of about 1 s, so later requests wait for at most
  one step. A later write with a different limit, or without one, or `memoryClear`, replaces it.
- The limit counts the database's pages in use; the write-ahead log can add to the files on disk.
- The oldest entries are removed first. The space they took is reused by new entries, and the file
  keeps its largest size.
- `memoryStats` reports `usedBytes`, the size the limit counts (pages in use).
- Without `maxBytes` there is no limit.

Helpers with this support advertise `capabilities.memorySizeLimitV1` in `hello`.

### Caller-named locations

A client that ships its own copy of the helper (TabMail Voice does) can keep every file apart from
Thunderbird's: the database folder is `init` `profilePath`, the model folder is `init` `modelDir`,
and the log folder is the `--log-dir` start-up argument (all absolute paths). See ADR-NF-008 in
`DECISIONS.md`.

### Folder membership

`fts.db` keeps the historical `msgId` primary key for compatibility and stores opaque folder
identities in the additive `message_folder_membership` relation. Helpers advertising
`capabilities.folderMembershipV1` support exact folder equality operations:

- `listFolderMembership { folderId, afterMsgId?, limit? }`
- `listFolderMembershipState { afterMsgId?, limit? }`
- `assignFolderMembershipBatch { assignments: [{ msgId, folderId }] }` returns
  `{ ok, assigned, alreadyAssigned, missing }`

Helpers advertising `capabilities.folderMembershipSummaryV1` also answer
`folderMembershipSummary { folderIds, trustedAccountIds }` with
`{ ok, ownerlessRows, strayTrustedRows, strayUntrustedRows }`: rows with no folder relation, and
relation rows whose folder is not in `folderIds`, split by whether the msgId's account prefix is
trusted. It is one read snapshot with a fixed-size reply. `removeBatch` returns `removedFolderIds`
(distinct owners of the deleted rows) and `removedOwnerless` beside `count`.

`indexBatch` accepts `folderId` on each row. Existing rows with no relation can adopt one;
attempts to reassign a row from one non-empty folder identity to a different one fail closed.
Paging and assignment batches are bounded per call, but callers may continue until `done`, so
archive reconciliation is unbounded overall without requiring a single long-running native request.
Clients compute folder digests incrementally from `listFolderMembership` pages. The global state
page applies its limit to `message_ids` before joining folder membership, so even a terminal
migration page cannot scan the whole archive in one call. Assignments for live messages outside the
native FTS policy window are counted in `missing` and otherwise ignored.

## Logs

Logs are written to `~/.tabmail/logs/fts_helper.log`, or to the folder given with
`--log-dir <absolute path>`. `--log-level <off|error|warn|info|debug>` sets the level (debug by
default); `off` writes no log file. TabMail Voice starts its copy with `--log-level off`.

- Logs rotate automatically at **10 MB**
- **5 backup files** are kept
- Maximum disk use: **about 50 MB**

```bash
# View the latest log
tail -f ~/.tabmail/logs/fts_helper.log
```

## Self-update

The Thunderbird copy of the helper updates itself:

1. **Version negotiation:** the add-on sends `hello`; the helper answers with its version and
   capabilities.
2. **Update check:** the add-on fetches `update-manifest.json` from the CDN.
3. **Self-update:** the helper downloads the new version, verifies the Ed25519 signature and the
   SHA-256 hash, and swaps the files atomically.
4. **Auto-migration:** on first run from the system location, the helper copies itself to the
   user-local folder for future updates.
5. **Notices:** the helper carries its own third-party notices
   (`fts_helper --print-third-party-notices`). An update installs the new build's notices next to
   it as `fts_helper-THIRD_PARTY_NOTICES.txt` together with the binary, and puts the old ones back
   if the swap fails. The migration copies them too. On `hello`, while the file at its path is
   still the binary it started from, the helper replaces missing or different notices with its
   own, which repairs an update applied by an older updater.

TabMail Voice's copy never updates itself (it never sends `hello` or the update requests); a new
helper arrives with a new version of the app.

**Security:**

- All downloads over HTTPS from `cdn.tabmail.ai`
- Ed25519 signature verification on update manifests
- SHA-256 hash verification on downloaded binaries
- A backup is made before each update and restored automatically on failure

## Search quality

### Porter stemmer

FTS5 uses Porter stemming for English:

- "running" matches "run", "runs", "runner"
- "emails" matches "email", "emailing"

### Email-specific synonym expansion

About 100 curated synonym groups for common email terms, for example:

| Search term | Also matches |
|-------------|--------------|
| `meeting` | call, sync, standup, huddle, appointment |
| `urgent` | asap, immediately, priority, critical |
| `invoice` | bill, payment, receipt, statement |
| `attachment` | attached, file, document, enclosed |

### Query text

Every word and `"quoted phrase"` in a query is searched as text, so punctuation never causes a
syntax error. A capitalized `AND`, `OR` or `NOT` between two terms is an operator
(`from:"alice@example.com" OR to:"alice@example.com"`); anywhere else it is dropped. Mail search
scopes `subject:`, `from:`, `to:`, `cc:`, `bcc:` and `body:` (for example
`from:"alice@example.com" budget`); any other `word:` is text, and memory search has no fields.

### BM25 column weights

Results are ranked with column-specific weights:

| Column | Weight | Meaning |
|--------|--------|---------|
| subject | 5.0 | Subject matches rank highest |
| from_ | 3.0 | Sender matches are important |
| to_ | 2.0 | Recipient matches are useful |
| body | 1.0 | Body matches are common |

## Performance tuning

The helper uses conservative, safe SQLite defaults:

```sql
PRAGMA journal_mode = WAL          -- Write-Ahead Logging
PRAGMA synchronous = NORMAL        -- Balance safety vs speed
PRAGMA cache_size = -64000         -- 64MB cache
PRAGMA mmap_size = 268435456       -- 256MB memory-mapped I/O
PRAGMA busy_timeout = 2000         -- 2s wait for locks
```

These defaults are fast for most users. For very large mailboxes (over 100k messages), see the Rust
source for tuning options.

---

## License

This project is open source under the
[Mozilla Public License 2.0](https://www.mozilla.org/MPL/2.0/) (MPL-2.0).

- Free to use, modify, and distribute, including commercially.
- MPL is a weak-copyleft license at file granularity: modifications to MPL-licensed files must
  stay open, but you may combine them with proprietary code in larger works.

Copyright © Lisem AI Ltd.

See:

- [LICENSE](./LICENSE)
- [THIRD_PARTY_NOTICES.txt](./THIRD_PARTY_NOTICES.txt): the copyright notices and license texts of
  the open-source software the helper is built with, and of the embedding model. The installer
  packages ship this file next to the helper, and each release uploads it next to the update
  binaries, and self-update installs the new build's copy with its binary. The install scripts will
  install it too (#61).

## Contributing

We welcome contributions! This project uses the
[Developer Certificate of Origin (DCO)](https://developercertificate.org/): sign off every commit
with `git commit -s`.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for details.

## Trademarks

"TabMail" is a trademark of Lisem AI Ltd. This license does not grant permission to use the
TabMail name or branding.

See [TRADEMARKS.md](./TRADEMARKS.md).
