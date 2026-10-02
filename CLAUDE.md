# TabMail Native FTS - Claude Code Rules

> **STOP. Before answering, I must follow the root `../CLAUDE.md` § Companion Routing: load the ALWAYS-LOADED set in full, then `rg -ni` the SEARCH-ONLY indexes and `Companion/` trees for the task's terms and read every hit in full. I must update the routed document when I discover durable knowledge. This is mandatory for every task, every time — no exceptions.**

## Companion Files

**Loading rule lives in the root `../CLAUDE.md` § Companion Routing (owner 2026-09-09).** ALWAYS LOADED in full: `../CLAUDE.md`, `../PROJECT_STRUCTURE.md`, this file, and this project's `PROJECT_STRUCTURE.md`. SEARCH-ONLY (`rg -ni`, read every hit in full, never read whole): the `PROJECT_MEMORY.md`, `DECISIONS.md`, `MISTAKES.md` indexes at root and here, and every `Companion/` tree. Update the routed detail plus its index line when you learn something durable.

**Global (parent directory):**
- **`../CLAUDE.md`** — Global rules that apply to all subprojects.
- **`../PROJECT_STRUCTURE.md`** — Monorepo layout, tech stack, component relationships.
- **`../PROJECT_MEMORY.md`** — Cross-cutting knowledge and workflows.
- **`../DECISIONS.md`** — Cross-cutting architectural decisions.

**This project:**
- **`PROJECT_STRUCTURE.md`** — Directory tree, entry points, sub-component map.
- **`PROJECT_MEMORY.md`** — Native FTS specific knowledge, patterns, quirks.
- **`DECISIONS.md`** — Native FTS specific architectural decisions.

**Search the indexes before every task; read every hit in full. Update them when you discover something new.**

---

## Development Rules

1. **Rust** — All code in Rust. No FFI unless absolutely necessary.
2. **No new crate dependencies without justification** — Keep binary size and audit surface small.
3. **Thread safety via ownership** — Use Rust's type system (`Send`, `Sync`, `Arc`, `Mutex`) for thread safety. No unsafe unless absolutely necessary.
4. **Native messaging protocol** — Communication with Thunderbird via stdin/stdout JSON messages. Responses correlated by `id` field, not by order.
5. **SQLite WAL mode** — Read and write connections are separate. Reader/writer thread split.
