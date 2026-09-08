# Acquisition custody, fair scheduling, and homepage query budget

Date: 2026-09-08
Baseline: `1200308783b51f46b1117277b7f7371f7996c6d2` (`v0.1.0-rc.7`)
Status at authoring: locally qualified; deployment evidence is a separate dated receipt.

## What the observations corrected

The previous deployment left 25 active sources without durable pagination
cursors and two existing cursors without recent observation metadata. A bounded
read of eight recent outcomes per currently active source found 129 sources,
all recently attempted. Among the 25 cursorless sources, 19 repeatedly returned
an `empty` acquisition outcome, four repeatedly returned `success`, and two
repeatedly returned `error`. This does not establish that these sources should
return a pagination cursor. Outcome history does not retain response cursor
presence, and a successful empty response must not manufacture one.

The two existing unobserved cursors had repeated successful nonempty responses.
Implementation inspection and a failing regression identified why: a terminal
page with no next cursor overwrote the local variable with `None`, suppressing
the observation update despite successful use of an existing durable cursor.
The repair re-observes the durable cursor after a successful acquisition using
the existing helper, which refuses to create a missing cursor. Cursor advancement
and the `cursor_continuity/v2` question keep their established meanings.

Production evidence also corrected the earlier unfinished-report concern:
a natural report completed at 16:22 UTC and derive at 16:52 UTC. Later derive
passes deferred under measured memory pressure. Completion on those occasions
does not establish sustained report or derive freshness.

## Scheduling defect and bounded repair

Present production outcomes did not demonstrate starvation. A deterministic
budget fixture did: three runs with a reopened database each attempted the first
source and never reached the other two. This is a latent scheduling defect under
short budgets, distinct from the observed cursor classification.

Multi-ingest now orders eligible endpoint sources by DID and resumes after
`meta['multi_ingest:last_attempted_did']`. A completed successful, empty, or failed
attempt records this position with its outcome. Removing the previous source
does not reset progress; lexicographic rotation continues to its successor.
A crash before completion may repeat an attempt. This metadata is compatible
with the preceding runtime, which ignores the additional key; no schema migration
or new dependency is needed.

## Homepage work and its limits

A loopback homepage request took 12.6 seconds. Query timing showed that the
optional network-weather strip requested the entire detailed boundary report,
including sorting and materializing edges, to use only its moderation-edge
count. A grouped count now uses the same window, family version, edge type,
classification, and NULL treatment. Local boundary fixtures and a bounded
same-snapshot production comparison agreed (nine edges). The latter ran the
aggregate first in 5.335 seconds and the old summary second in 1.539 seconds;
cache and order confound performance, so this is parity evidence, not proof
of a speedup.

The optional strip therefore also has a two-second SQLite execution budget on
a request-owned readonly connection. Expiry or an escaping computation error
renders “temporarily unavailable” with no current weather conclusion. It does
not turn missing computation into calm, zero, or healthy. The connection's
progress handler is cleared and the connection closed. This SQLite budget is
not a hard real-time bound on filesystem stalls or connection setup.

The homepage remains useful when optional weather is unavailable; that outcome
still leaves weather computation degraded. Production acceptance separately
measures homepage latency (five-second target), weather availability, acquisition,
scan/derive/report cadence, and discovery continuity.

## Reproduction and evidence boundaries

Run from this revision with the project's existing test environment:

```sh
python -m pytest -q tests/test_acquisition_rotation.py tests/test_multi_ingest.py tests/test_cursor_persistence.py tests/test_frontdoor.py tests/test_observation_adequacy.py tests/test_boundary.py
python scripts/demo_offline.py --output /explicit/new/synthetic-demo-directory
```

The 122 focused tests pass, including HTTP fixtures, restart fairness, terminal
cursor observation, aggregate parity at both time boundaries, old family versions,
NULL families, query interruption, explicit unavailability, and genuine zero.
The first two regressions fail against the baseline source. The offline demo
still ingests 37 synthetic events and detects the expected `flip_flop` finding.

Private campaign `2026-09-08-observatory-correctness-recovery`, lane `labelwatch`,
retains timestamped classification, query profiles, baseline failures, qualified
test output, and demo receipts. Source identifiers, operational paths, and live
data are excluded from this account. Exact deployment outcomes will be appended
in a separate repository history receipt after production observation.
