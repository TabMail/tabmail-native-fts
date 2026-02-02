# TabMail Native FTS Helper

Rust-based native messaging host for **hybrid semantic + keyword search** using bundled SQLite + FTS5 + sqlite-vec (single self-contained binary per OS). Generates sentence embeddings locally via candle (pure Rust) with `all-MiniLM-L6-v2`.

This is the native helper component for the **[TabMail Thunderbird Add-on](https://github.com/TabMail/tabmail-thunderbird)**.

## Architecture

```
Thunderbird Extension (JavaScript)
    ↕ Native Messaging API (stdin/stdout)
Rust Native Host Binary (`fts_helper`)
    ↕ Direct SQLite access
FTS Database (per-profile)
```

## Installation

**For end users:** Use the TabMail installer (macOS .pkg, Windows .exe) or the curl/PowerShell install scripts. The native FTS helper is installed automatically and migrates to user-local on first run for auto-updates.

**Quick install (no admin required):**

```bash
# macOS / Linux
curl -fsSL https://cdn.tabmail.ai/releases/native-fts/install.sh | bash

# Windows (PowerShell)
irm https://cdn.tabmail.ai/releases/native-fts/install.ps1 | iex
```

## Building from Source

```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build release binary
cd tabmail-native-fts
cargo build --release

# Binary is at: ./target/release/fts_helper
```

## Testing

```bash
# Build the Rust binary first
cargo build --release

# Run all tests
python3 tests/run_tests.py

# Run Rust helper tests only
python3 tests/run_tests.py rust

# Run update mechanism tests only
python3 tests/run_tests.py update

# Verbose output
python3 tests/run_tests.py -v
```

## Native Messaging Manifest

The native messaging manifest tells Thunderbird where to find the helper binary. Each platform's installer generates this dynamically with the correct path.

**Manifest format:**

```json
{
  "name": "tabmail_fts",
  "description": "TabMail FTS Native Helper",
  "path": "/path/to/fts_helper",
  "type": "stdio",
  "allowed_extensions": ["thunderbird@tabmail.ai"]
}
```

**Manifest locations:**

| Platform | User-local manifest path |
|----------|-------------------------|
| macOS | `~/Library/Application Support/Mozilla/NativeMessagingHosts/tabmail_fts.json` |
| Linux | `~/.mozilla/native-messaging-hosts/tabmail_fts.json` |
| Windows | Registry: `HKCU\Software\Mozilla\NativeMessagingHosts\tabmail_fts` → path to JSON |

## Installation Paths

**User-Local (auto-update enabled):**
- macOS: `~/Library/Application Support/TabMail/native/fts_helper`
- Linux: `~/.local/share/tabmail/native/fts_helper`
- Windows: `%LOCALAPPDATA%\TabMail\native\fts_helper.exe`

**System-Wide (installer location, fallback only):**
- macOS: `/Applications/TabMail.app/Contents/Resources/fts_helper`
- Linux: `/opt/tabmail/fts_helper`
- Windows: `C:\Program Files\TabMail\native\fts_helper.exe`

## Database Storage

The FTS databases are stored in the **Thunderbird profile's extension data directory**:

### Email database (`fts.db`)

- **macOS**: `~/Library/Thunderbird/Profiles/<profile>/browser-extension-data/thunderbird@tabmail.ai/tabmail_fts/fts.db`
- **Linux**: `~/.thunderbird/<profile>/browser-extension-data/thunderbird@tabmail.ai/tabmail_fts/fts.db`
- **Windows**: `%APPDATA%\Thunderbird\Profiles\<profile>\browser-extension-data\thunderbird@tabmail.ai\tabmail_fts\fts.db`

### Memory database (`memory.db`)

A separate database stores chat history for the agent's memory features:

- **macOS**: `~/Library/Thunderbird/Profiles/<profile>/browser-extension-data/thunderbird@tabmail.ai/tabmail_fts/memory.db`
- **Linux**: `~/.thunderbird/<profile>/browser-extension-data/thunderbird@tabmail.ai/tabmail_fts/memory.db`
- **Windows**: `%APPDATA%\Thunderbird\Profiles\<profile>\browser-extension-data\thunderbird@tabmail.ai\tabmail_fts\memory.db`

The memory database enables:
- **Memory search** — Find past conversations by keyword using FTS5 with stemming and synonyms
- **Memory read** — Retrieve full chat sessions by timestamp for context continuity

Each Thunderbird profile gets its own isolated FTS and memory databases.

*Note: The helper automatically migrates databases from the old location (`<profile>/tabmail_fts/`) to the new location on first run.*

## Logs

Logs are written to: `~/.tabmail/logs/fts_helper.log`

- Logs automatically rotate at **10MB**
- Keeps **5 backup files**
- Maximum disk usage: **~50MB**

```bash
# View latest log
tail -f ~/.tabmail/logs/fts_helper.log
```

## Self-Update Mechanism

The native helper can automatically update itself:

1. **Version Negotiation** — Extension sends `hello`, helper responds with version and capabilities
2. **Update Check** — Extension fetches `update-manifest.json` from CDN
3. **Self-Update** — Helper downloads new version, verifies Ed25519 signature + SHA256 hash, atomically swaps files
4. **Auto-Migration** — On first run from system location, helper copies itself to user-local for future auto-updates

**Security:**
- All downloads over HTTPS from `cdn.tabmail.ai`
- Ed25519 signature verification on update manifests
- SHA256 hash verification on downloaded binaries
- Backup created before update (auto-restored on failure)

## Search Quality Features

### Porter Stemmer

FTS5 uses Porter stemming for English:
- "running" matches "run", "runs", "runner"
- "emails" matches "email", "emailing"

### Email-Specific Synonym Expansion

~100 curated synonym groups for common email terms:

| Search Term | Also Matches |
|-------------|--------------|
| `meeting` | call, sync, standup, huddle, appointment |
| `urgent` | asap, immediately, priority, critical |
| `invoice` | bill, payment, receipt, statement |
| `attachment` | attached, file, document, enclosed |

### BM25 Column Weights

Search results are ranked with column-specific weights:

| Column | Weight | Meaning |
|--------|--------|---------|
| subject | 5.0 | Subject matches rank highest |
| from_ | 3.0 | Sender matches are important |
| to_ | 2.0 | Recipient matches are useful |
| body | 1.0 | Body matches are common |

## Performance Tuning

The helper uses conservative, safe defaults for SQLite:

```sql
PRAGMA journal_mode = WAL          -- Write-Ahead Logging
PRAGMA synchronous = NORMAL        -- Balance safety vs speed
PRAGMA cache_size = -64000         -- 64MB cache
PRAGMA mmap_size = 268435456       -- 256MB memory-mapped I/O
PRAGMA busy_timeout = 2000         -- 2s wait for locks
```

These defaults are fast for most users. For very large mailboxes (>100k messages), see the Rust source for tuning options.

---

## License

This project is source-available and licensed under the
[PolyForm Noncommercial License 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/).

- Free for non-commercial use
- Commercial use is not permitted without a separate license

Commercial licenses are available from Lisem AI Ltd.

See:
- [LICENSE](./LICENSE)
- [COMMERCIAL-LICENSE.md](./COMMERCIAL-LICENSE.md)
- [THIRD_PARTY_LICENSES.md](./THIRD_PARTY_LICENSES.md) — Third-party model and library licenses

---

## Contributing

We welcome contributions! By submitting a pull request or other contribution,
you agree to our Contributor License Agreement.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for details.

---

## Trademarks

"TabMail" is a trademark of Lisem AI Ltd.
This license does not grant permission to use the TabMail name or branding.

See [TRADEMARKS.md](./TRADEMARKS.md).
