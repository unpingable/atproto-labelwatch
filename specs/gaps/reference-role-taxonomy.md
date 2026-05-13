# Candidate: Reference Role Taxonomy

**Status**: candidate / non-binding. Filed as a handle, not authorization to build.

## Problem

`labelers.is_reference` (bool) and `Config.reference_dids` (list) are too coarse. After the 2026-05-13 patch, `Config.flaky_reference_dids` was added as a parallel list to demote known-flaky popular references from system-wide CRITICAL-driving status. This is correct as a localized fix, but it's the second flag describing the same axis. A third would mean the axis wants a name.

A given labeler can be any combination of:
- historically high-volume / popular
- currently observed as a label source
- reachable at its declared endpoint
- a published reference (e.g. listed by labeler-lists)
- a calibration anchor we trust for system-wide health
- recurrently flaky / quiet
- excluded from health anchoring for any of several reasons (flaky, deprecated, under investigation)

These are not the same property. Today they're collapsed into one bool plus ad-hoc config lists.

## Keeper

> **Popularity is not standing.**
>
> A popular noisy source can be a subject of interest without being a source of calibration authority.

The 05-13 incident: hailey.at had `is_reference=1` because it's a popular labeler people care about. The system promoted "important" to "health authority" and let one flaky reference hijack the platform verdict every time it went quiet. Same claim-boundary bad-promotion pattern that shows up in other costumes.

## Current patch

`Config.flaky_reference_dids` separates known-flaky popular references from strict health anchors. Their gone_dark/degrading state routes to `flaky_reference_quiet` (advisory) instead of `reference_issues` (CRITICAL-driving). Per-labeler info dicts now carry `known_flaky: bool`.

This is correct as a patch. It's also a hint that the underlying model wants to be a role taxonomy, not parallel bool/list flags.

## Possible roles (sketch — not a schema proposal)

- `observed_subject` — we ingest events from it; baseline tracked
- `popular_subject` — historically high-volume; interesting but not authoritative
- `strict_reference_anchor` — its silence is platform-critical
- `advisory_reference` — its silence is reportable but not CRITICAL-driving
- `known_flaky_reference` — recurrent quiet/dark behavior; silence is annotation
- `excluded_from_system_health` — under investigation, deprecated, or otherwise unsuited as an anchor

A labeler can hold multiple roles. CRITICAL fires only when a `strict_reference_anchor` is in trouble — or when aggregate degradation gates fire independent of reference roles.

## Rule

A known-flaky popular source may be reported as interesting, but must not be allowed to drive system-wide CRITICAL alone. The reference set must tolerate one flaky popular member going quiet without declaring the whole system critical.

## Not building this now

YAGNI applies. The current patch addresses the live incident with the minimum change. This note exists so:
1. The next time something on this axis needs a tweak, the third flag's worth of accumulated config triggers the consolidation.
2. The vocabulary ("strict anchor" vs "advisory reference" vs "flaky reference") is available for review and ratification before it's load-bearing.

If/when this gets ratified, candidates for migration:
- `labelers.is_reference: int` → `labelers.reference_role: text` (or roles bitfield)
- `Config.reference_dids` + `Config.flaky_reference_dids` → role-keyed config or a `reference_roster` config block
- `signal_health_snapshot` verdict logic decouples from `is_reference` and reads role directly
