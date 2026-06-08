# Operator-maturity scan — summary (2026-06-08)

One-page TL;DR of the operator-maturity findings. Full page:
[`../index.md`](../index.md).

## Headline

> Label emission, declared consumer semantics, operational liveness,
> and moderation authority are separate properties.
>
> All three adjacent implication failures appear at population scale
> in 150 observed ATProto labelers.

| Property observed | Does not imply |
|---|---|
| Label emission | Declared consumer semantics |
| Declared consumer semantics | Operational liveness |
| Operational liveness | Moderation authority |

## Numbers

- **150** observed labelers in scope (recent activity OR ever-ingested
  service record)
- **14** high-volume emitters publish ZERO `labelValueDefinitions`
  (F-007 — top: `antiantiai.bsky.social` and `labeler.plural.host`
  at ~908,000 events/30d each, same volume as moderation.bsky.app)
- **65** abandoned labelers (43% — service record on file, zero events
  in last 30d)
- **28** of those 65 retain substantial declared scope (≥6
  `labelValueDefinitions`) despite operational silence (F-008 — top:
  `sonasky.app` with 684 definitions, all silent)
- **1** labeler (`vocalabeller.kanshen.click`) republished its
  service record 106,000 times with one definition and zero events
  (pathological churn-without-output)
- **0** sampled production clients hardcode any third-party labeler
  as a default (consumer-conversion census; 7/7 clients checked)

## Class histogram (heuristic, see D-002)

```
abandoned                  65  (43%)
unknown                    26  (17%)
experimental               24  (16%)
community-service          13   (9%)
personal/reputational      11   (7%)
moderation-infrastructure  10   (7%)
platform-root               1   (<1%)
```

## Refusal hooks

- **emission ≠ declaration** — do not treat an observed label stream
  as subscribable moderation infrastructure unless the labeler also
  declares the consumed label values in its service record. (F-007)
- **declaration ≠ liveness** — do not treat the presence of a service
  record as evidence of a live moderation service. (F-008)
- **liveness ≠ authority** — do not assume any production client
  converts a live third-party labeler's labels into default
  visibility behavior. (consumer-conversion census + F-001 + F-004)

## Provenance

- **Snapshot:** `operator-maturity-scan-2026-06-08.json` (in this
  artifacts directory) — 150 rows × 23 columns + scan metadata
- **CSV mirror:** `operator-maturity-scan-2026-06-08.csv`
- **Scanner:** `docs/analysis/tools/operator_maturity_scan.py` v1
- **Source:** `/var/lib/labelwatch/labelwatch.db` (prod, 192.46.223.21)
- **Scanned at:** 2026-06-08 (UTC; exact timestamp in
  `scan_meta.scanned_at` field of the JSON)
- **Regression check:** `../regression/test_findings_regression.py`
  asserts headline numbers against this snapshot

## What this artifact does NOT do

- Does not classify labelers normatively. Heuristic categories only.
- Does not include `has_contact_or_appeal_path` (deferred — needs
  appview profile fetches + manual classification).
- Does not cover closed-source Bluesky clients (see
  consumer-conversion census' sampling caveats).
- Does not assert numbers will remain stable; ecosystem evolves.
  Re-run scanner before citing in time-sensitive context.
