# ADR-NF-007: Move or Remove Indexes the Profile Guess Left Where No Profile Reads Them

**Status:** Accepted (2026-10-05). Thunderbird counterpart: the add-on names its data
directory in `init` (Thunderbird ADR-025).

## Context

Without `profilePath`, `handle_init` guesses the profile with `find_thunderbird_profile_dir`:
the most recently modified non-hidden directory under the platform's profiles directory
(`~/Library/Thunderbird/Profiles`, `~/.thunderbird`, `%APPDATA%/Thunderbird/Profiles`),
or `~/.tabmail` when there is none. A wrong guess put a profile's index into another
directory. Once the add-on sends `profilePath` (Thunderbird ADR-025), the helper never
looks at the guessed directory again, so an index that landed where no profile reads it
stays on disk for good:

- `~/.tabmail/browser-extension-data/<addon id>/tabmail_fts`, when the profiles directory
  was missing or empty (a profile at a custom or portable path, or a sandboxed install);
- a non-profile directory beside the profiles, such as `~/.thunderbird/Crash Reports`
  on Linux;
- a profile that does not have the add-on installed.

These hold a copy of the user's mail index (`fts.db`) and chat memory index (`memory.db`) that nothing reads.

## Decision

On `init` with `profilePath`, the helper runs `adopt_or_remove_orphaned_indexes` over every
directory the guess could return (`guessed_profile_dirs`: `~/.tabmail` plus the non-hidden
directories under the profiles directory). An index there,
`<dir>/browser-extension-data/<addon id>/tabmail_fts`, is an orphan unless:

- it is this profile's own data directory (compared after resolving symlinks), or
- `<dir>/extensions/<addon id>.xpi` exists: a profile with the add-on installed reads
  that index, so it is that profile's own, even if another profile's mail was written
  into it before ADR-025. That contamination is accepted, not cleaned up.

The index must physically live in the directory: if `browser-extension-data`, the add-on's
directory or `tabmail_fts` is a symlink, it is not an orphan and nothing is followed,
because a symlink could lead into another profile that has the add-on installed. The
add-on id is the last component of `profilePath`.

**Move first (owner, 2026-10-05: "move and fix", not delete).** When this profile has no
`tabmail_fts` yet, the orphan whose `fts.db` was written most recently (the one the old
helper used last) is renamed into this profile's data directory. Its mail index and chat
memory (`memory.db`) carry over. The add-on's startup reconciliation then repairs the mail
index against this profile's folders (ADR-022/ADR-024 in the add-on): missing mail is
indexed and rows for mail that is gone are removed. Rows of accounts this profile does not
have stay as unloaded-account rows, and `memory.db` is not reconciled (accepted).

**Then remove the rest.** Every other orphan, and every orphan when this profile already
has an index, is removed. A move that fails, typically because the orphan is on another
volume, is not retried as a copy: a multi-gigabyte copy inside `init` could outlast the
add-on's init RPC and get the helper killed mid-copy on every start (the ADR-NF-005
lesson), so that orphan is removed and the add-on rebuilds the index.

After a move or removal, the add-on's data directory and `browser-extension-data` above it
are removed only if they are now empty. Every step is best effort and logged; it never
fails `init`. It runs on every `init` with `profilePath`, and on a machine with nothing to
do it costs one directory listing and a few `stat` calls.

`find_thunderbird_profile_dir` keeps its behaviour for add-on releases that do not send
`profilePath`; its profiles-directory, fallback and candidate logic is shared with
`guessed_profile_dirs`.

## Consequences

- A user whose index sat in a guessed directory keeps it: on the first `init` after the
  helper update it moves into this profile, chat memory included, and reconciliation
  repairs it. Nothing unread stays on disk.
- Where several orphans exist, only the newest moves; the others, including their
  `memory.db`, are removed. A moved orphan may hold another profile's rows (two profiles
  that shared a guessed directory); they stay as accepted contamination.
- **Release order: this helper before (or with) the add-on release carrying ADR-025.** The
  add-on updates the helper before `init`, so a user who gets both at once moves the
  orphan. If the add-on ships first, the old helper creates an empty index in this
  profile on its first `init` with `profilePath`; when this helper arrives later, the
  orphan is removed instead of moved and its chat memory is lost (the mail index has
  already been rebuilt).
- An orphan on another volume than the profile is removed instead of moved; that user's
  index is rebuilt and older chat memory is lost.
- An add-on release older than ADR-025, running in another profile at the same time and
  still using a guessed directory without the add-on installed (for example
  `~/.tabmail`), loses that index; it rebuilds after the add-on updates.
- A profile that loads the add-on without an installed `.xpi` (a temporary developer
  install, or a proxy file named after the add-on id) and is not the profile running
  `init` loses its index if another profile's helper runs `init`; it rebuilds.
- While Thunderbird updates the add-on, the old `.xpi` is moved away before the new one
  is moved in. An `init` from another profile in that instant can remove the updating
  profile's index; it rebuilds.
- Tests: unit tests in `src/main.rs` (moved: the newest orphan into a profile without an
  index; never moved: an index a profile reads; removed: the rest, an orphan that cannot
  be moved; kept: own via a symlinked candidate, another profile with the add-on,
  unrelated files, a symlinked index or parent, an index that cannot be removed) and
  `tests/test_profile_path_cleanup.py` (the helper binary with a temporary HOME: a real
  index moved with its rows, removal when this profile has an index, and the guess
  without `profilePath`).
