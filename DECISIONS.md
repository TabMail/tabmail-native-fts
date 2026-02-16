# TabMail Native FTS - Architectural Decisions

> **Check this file before proposing alternatives.** For cross-cutting decisions, see `../DECISIONS.md`.

---

## ADR-018: SchemaVersion + Reader/Writer Thread Split for Native FTS

**Context:** The native FTS helper processed all operations single-threaded. Heavy write operations blocked read operations for seconds to minutes. Additionally, the JS side compared host version strings to decide reindex triggers, meaning any version bump would trigger a full ~45 minute reindex.

**Decision:**
1. Add integer `SCHEMA_VERSION` constant (starting at 1) that bumps only when DB schema, FTS5 tokenizer config, or embedding model actually changes.
2. After `init`, split processing into three threads: main (stdin reader + dispatcher), reader (read-only DB ops), writer (write/destructive DB ops).
3. `EmbeddingEngine` and `SynonymLookup` shared via `Arc`. Each thread opens its own `rusqlite::Connection` (Connection is `!Send`).
4. Responses written through `Arc<Mutex<Stdout>>`. Native messaging protocol correlates by `id` field, not by order.
5. `clear`/`memoryClear` in writer signals reader via `AtomicBool` to reopen its connection.
6. No new crate dependencies.

**Rationale:**
- SchemaVersion prevents unnecessary full reindex on non-schema host updates
- Reader/writer split eliminates the primary UX problem: search blocked during indexing
- SQLite WAL mode already supports concurrent readers + single writer
- Compiler-enforced thread safety via Rust ownership + Send/Sync traits

**Consequences:**
- Reader connections cache ~64MB each, total memory increase ~128MB
- Clear operations have a brief window where reader may fail
- Pre-init messages remain single-threaded
- JS side needs no changes (already handles out-of-order responses)

---

## Template for New Decisions

```markdown
## ADR-XXX: [Title]

**Context:** [What situation led to this decision?]

**Decision:** [What did we decide?]

**Rationale:** [Why?]

**Consequences:**
- [Trade-offs, both positive and negative]
```
