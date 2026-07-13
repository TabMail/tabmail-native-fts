# TabMail Native FTS - Claude Code Rules

> **STOP. Before answering, I must read ALL companion files listed below — both global and project-specific. I must also update them when I discover something new. This is mandatory for every task, every time — no exceptions.**

## Companion Files (READ BEFORE EVERY TASK)

Before starting any task in this project, read these files and update them when you learn something new:

**Global (parent directory):**
- **`../CLAUDE.md`** — Global rules that apply to all subprojects.
- **`../PROJECT_STRUCTURE.md`** — Monorepo layout, tech stack, component relationships.
- **`../PROJECT_MEMORY.md`** — Cross-cutting knowledge and workflows.
- **`../DECISIONS.md`** — Cross-cutting architectural decisions.

**This project:**
- **`PROJECT_STRUCTURE.md`** — Directory tree, entry points, sub-component map.
- **`PROJECT_MEMORY.md`** — Native FTS specific knowledge, patterns, quirks.
- **`DECISIONS.md`** — Native FTS specific architectural decisions.

**You MUST read all companion files before every task. Update them when you discover something new.**

---

## Development Rules

1. **Rust** — All code in Rust. No FFI unless absolutely necessary.
2. **No new crate dependencies without justification** — Keep binary size and audit surface small.
3. **Thread safety via ownership** — Use Rust's type system (`Send`, `Sync`, `Arc`, `Mutex`) for thread safety. No unsafe unless absolutely necessary.
4. **Native messaging protocol** — Communication with Thunderbird via stdin/stdout JSON messages. Responses correlated by `id` field, not by order.
5. **SQLite WAL mode** — Read and write connections are separate. Reader/writer thread split.
