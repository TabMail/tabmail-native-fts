# ADR-NF-018: Capabilities for the Behaviour 0.11.8 Lacks

**Status:** Accepted (2026-10-24). Amends ADR-NF-012 item 4 ("no capability flag").

## Context

Thunderbird's next add-on release relies on three helper changes made after 0.11.8:
`memorySearch` `previewChars` (ADR-NF-012), the dated hybrid memory search filtering before its
limit (ADR-NF-012 amendment, ADR-NF-016), and `search` `sort` (ADR-NF-017). An add-on can meet a
0.11.8 helper, for example when the helper's update check cannot reach the network. Its release
review measured what happens then:

- a dated memory search at limit 100 returned 0 of 6 matches inside the dates;
- a listing of six whole sessions came back as one 1.25 MB message, over Thunderbird's 1 MiB cap, and
  Thunderbird disconnected the helper.

Helper main also reports version 0.11.8 until its release commit, so the add-on cannot tell them
apart by version.

## Decision

`hello` advertises three more capabilities, each `true`:

- `memoryPreviewV1`: `memorySearch` honours `previewChars`.
- `memoryFilterBeforeLimitV1`: a dated `memorySearch` filters its candidates by date before it
  takes its limit.
- `searchSortV1`: `search` honours `sort` (tabmail-thunderbird#164).

A caller that needs one refuses the request when the connection that would receive it does not
advertise it, instead of trusting the version. The flags are additive. TabMail Voice builds its
helper from a pinned revision and reads no capabilities, so it is unaffected.

## Evidence

`test_hello_advertises_folder_membership_v1` asserts every advertised capability.
