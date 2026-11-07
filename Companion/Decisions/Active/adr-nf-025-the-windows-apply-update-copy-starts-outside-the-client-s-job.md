# ADR-NF-025: The Windows Apply-Update Copy Starts Outside the Client's Job

**Status:** Accepted (2026-11-06, issue #65). Applies to the Windows handoff of ADR-NF-024.

## Context

On Windows the helper cannot replace its own running file. After a verified `updateRequest` it copies
itself to a temporary folder, starts the copy in apply-update mode, answers success and exits; the copy
waits for it to exit and swaps the staged binary in.

Measured on Windows 11 with Thunderbird 157, with the helper registered as a native-messaging host and run
by a standard (UAC) user, this never applied an update:

1. **The copy did not start.** It was named `fts_helper_apply_update.exe`. Windows UAC treats an executable
   with no elevation manifest whose name contains an installer word ("update", "setup", "install", "patch")
   as an installer, and `CreateProcess` refuses it for a standard user with os error 740 ("The requested
   operation requires elevation"). Microsoft documents this installer detection for 32-bit executables;
   it was measured with the x64 build running under emulation on ARM64 Windows. Elsewhere the old name
   may start, and the copy then fails at the next step. The same binary under a neutral name starts. Run as SYSTEM, both start,
   which is why tests outside a user session never saw it.
2. **With a neutral name, the copy was killed.** Thunderbird (Gecko's `subprocess` module) starts a native
   host inside a job object and terminates the job when the host exits. The copy is in that job by default,
   so it ended with Gecko's termination code (0x7F) before its first log line.
3. **With `CREATE_BREAKAWAY_FROM_JOB` alone, the copy failed to initialize** (0xC0000142): it was still
   attached to the host's console, which goes away with the job.

## Decision

- The copy is named `fts_helper_apply.exe` (`config::update::APPLY_COPY_FILE_NAME`), a name with no installer
  word. A unit test checks the name, and the launcher test checks the copy is started under it.
- On Windows the copy is started with `CREATE_BREAKAWAY_FROM_JOB | DETACHED_PROCESS`: outside the client's job,
  with no console. Thunderbird's job allows breakaway (`JOB_OBJECT_LIMIT_BREAKAWAY_OK`).
- The copy's stdin, stdout and stderr are null on every platform. It outlives the helper, so none of the
  client's pipes is its stdio. On Unix it holds no other descriptor of them either. On Windows it may inherit
  other inheritable handles of the helper's, which can include the original pipe handles. In the measured run
  Thunderbird still saw the disconnect 21 ms after the response, while the copy ran for about 1 s.

With all three, the same Thunderbird run applied the update: the new binary and its notices were installed.

## Consequences

- If the client's own job does not allow breakaway, `CreateProcess` fails, the update is answered with an
  error (#64), and it is retried at the next connection. Nothing is installed in that case. With nested jobs
  the copy breaks away up the chain until it reaches a job that does not allow breakaway, which then keeps the
  copy, and creation does not fail (Microsoft, "Nested Jobs"). In the measured Thunderbird run the copy still sat in a
  job and the update applied.
- A second process running the same helper (a second Thunderbird profile) still makes every swap attempt fail
  after the success answer, as ADR-NF-024 describes. The client limits how often it asks for the same version.
- A helper released before this change runs updates with the old launch, so it cannot update itself into this
  one on Windows. It is replaced by reinstalling.
- Thunderbird reconnects 1 s after the helper exits. If it has started the old helper again before the copy's
  swap, the swap cannot replace the running file and the update fails after its success answer (ADR-NF-024);
  the next automatic attempt is in the next Thunderbird session. Whether this race is lost in practice is
  measured in #69.

## Alternatives

- **Embed an `asInvoker` manifest instead of renaming.** That is the documented way to exempt an executable from
  installer detection, whatever its name, but it adds a linker step to the build. The rename was measured to be
  enough.
- **Drop Windows self-update.** Users would update only by reinstalling.
