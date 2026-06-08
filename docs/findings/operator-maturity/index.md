# Label emission is not moderation infrastructure

> *Labeler admissibility findings — operator-maturity scan, 2026-06-08*

## Summary

A scan of 150 observed ATProto labelers shows that **label emission,
declared consumer semantics, operational liveness, and moderation
authority are separate properties.** All three of the adjacent
implication failures between them appear at population scale in the
current Bluesky labeler ecosystem.

**The central object:**

| Property observed | Does not imply |
|---|---|
| Label emission | Declared consumer semantics |
| Declared consumer semantics | Operational liveness |
| Operational liveness | Moderation authority |

Each row is a real-data finding with receipts; see the F-NNN entries
below. The chain is one-directional adjacent non-entailment — not a
claim that *every* pair of these properties has been separately
disproven, but a claim that the *adjacent* implications cannot be
silently assumed.

## What was measured

For each labeler with either recent label-event activity
(`events_30d > 0`) OR an ever-ingested service record on file, the
scan computed:

- **emission proxies:** `label_count_30d`, `label_count_7d`,
  `distinct_targets_30d`
- **declaration proxies:** `latest_label_def_count` (count of
  `labelValueDefinitions` in the most recent observed service
  record), `explains_labels` (at least one definition has locale
  text), `user_visible_consequence_known` (declared with a
  `defaultSetting` in `{hide, warn, ignore}`)
- **liveness proxies:** `active_recently` (any events in last
  30d), `has_stable_service_record` (1 ≤ revisions ≤ 5),
  `service_record_revisions` total
- **authority proxies:** see the consumer-conversion census; here,
  only the `is_reference` and `platform-root` flags appear directly

Source: `/var/lib/labelwatch/labelwatch.db` on prod (192.46.223.21),
scanned 2026-06-08 via
`docs/analysis/tools/operator_maturity_scan.py`.

150 rows returned.

## Core doctrine

The canonical anti-laundering hook (frozen in
[`docs/specimens/DISAGREEMENTS.md`](../../specimens/DISAGREEMENTS.md)):

> **Do not let "labeler exists" silently convert into "moderation
> service exists."** Label emission, declared semantics, and
> operational liveness are separate properties. The observed ATProto
> labeler ecosystem contains all three failure modes at scale.

The three adjacent non-entailments, each receipted:

  - **emission ≠ declaration** — see F-007 below
  - **declaration ≠ liveness** — see F-008 below
  - **liveness ≠ authority** — see F-001, F-004, and the consumer-
    conversion census

Longer-distance pairs (emission ≠ authority, declaration ≠
authority) are derivable from the chain but argued separately
elsewhere; they are not direct outputs of this scan.

## Operator-maturity scan

Class histogram (heuristic; see [Limits](#limits--classifier-caveats)):

```
abandoned                  65  (43%)
unknown                    26  (17%)
experimental               24  (16%)
community-service          13   (9%)
personal/reputational      11   (7%)
moderation-infrastructure  10   (7%)
platform-root               1   (<1%)
─────────────────────────────────
total                     150
```

The scanner sources, full per-row data, and reproduction instructions
live in [Receipts](#receipts--generated-data).

## F-007 — emission without declaration

**14 of 150 observed labelers emit at high volume (>1000 events in
the last 30 days) while publishing zero `labelValueDefinitions`** in
their service record. Top of the cohort:

| labeler | events / 30d | declared defs |
|---|---:|---:|
| `antiantiai.bsky.social` | 908,000 | 0 |
| `labeler.plural.host` | 906,500 | 0 |
| `oracle.posters.rip` | 698,575 | 0 |
| `uspol-labeler.bsky.social` | 446,585 | 0 |
| `bottags.bsky.social` | 121,267 | 0 |
| (… 9 more from 12k to 73k events/30d) | | |

Two of these emit at moderation.bsky.app's own volume (~908k/30d
each).

**Why this matters.** Under ATProto's stackable-moderation
subscription model, client-side honoring of labels via `labelersPref`
depends on a declared label vocabulary in the labeler's service
record. **A labeler with no `labelValueDefinitions` presents no
standard consumer-side semantic surface to subscribe to**, regardless
of how many labels it emits.

Consumers, auditors, and downstream tools cannot infer that
high-volume emitted labels correspond to an admissible moderation
surface. Emission volume is therefore not evidence of usable
protocol participation. A label-events firehose view of the
ecosystem systematically overstates the size of subscribable
moderation infrastructure.

**Diagnostic class** — four distinct gaps, none of which is "bug in
labeler":

1. **Protocol affordance gap** — emitting labels is operationally
   easier than declaring usable consumer semantics.
2. **Client reality gap** — clients may rely on definitions; emitters
   may not provide them.
3. **Ecosystem measurement gap** — raw label volume overstates
   meaningful moderation infrastructure.
4. **Anthropology gap** — the "federated moderation ecosystem" is
   mostly not a mature operator field.

**Generalization.** F-007 generalizes F-006 (the
`moderation.bsky.app/needs-review` specimen) from a single instance
into an ecosystem pattern. Label publication is operationally
separable from declared consumer semantics, and the separation
occurs at non-trivial scale across many independent operators.

## F-008 — declaration without liveness

**65 of 150 observed labelers are abandoned** (43%) — had a service
record on file, zero events in the last 30 days.

**Of those 65, 28 retain substantial declared scope** (≥ 6
`labelValueDefinitions`) despite operational silence. Top of the
substantial-scope subset:

| labeler | declared defs |
|---|---:|
| `sonasky.app` | 684 |
| `stemlabels.xyz` | 461 |
| `pokemon.sonasky.app` | 161 |
| `label.wol.blue` | 119 |
| `cons.fyi` | 108 |
| (… 23 more, 6–75 defs each) | |

**Why this matters.** A Bluesky user who subscribes to any of these
labelers via `labelersPref` would receive zero label events from
that subscription. The labeler is discoverable via its service
record and may even appear in client UIs as available, but the
subscription is operationally a no-op.

F-008 is the **mirror image** of F-007: declared consumer semantics
without operational label emission. The same admissibility hook
applies in both directions — declaration and liveness travel
separately.

**Pathological subcase — definition churn without emission.** Some
of the "abandoned" labelers are not "stopped" but "stuck":

| labeler | service-record revisions | declared defs | events / 30d |
|---|---:|---:|---:|
| `vocalabeller.kanshen.click` | **106,000** | 1 | 0 |
| `cons.fyi` | 1,034 | 108 | 0 |
| `labeler-bot-tan.suibari.com` | 866 | 18 | 0 |
| `moderation.blueat.net` | 99 | 20 | 0 |

`vocalabeller.kanshen.click` republished its service record 106,000
times with a single labelValueDefinition and zero events. The mirror
of F-007's "shout into the void" is F-008's "republish without
output." Both fail the consumer-protocol contract from different
sides.

## Limits / classifier caveats

This page is **descriptive aggregate signal**. Not measurement, not
judgment, not accusation.

**The `maturity_class` field is heuristic.** Class boundaries are
defensible but arbitrary (`events_30d ≥ 100`, `events_30d ≥ 10000`,
etc.). Threshold tuning will move boundary cases. **Do not cite an
individual labeler's `maturity_class` as a normative judgment about
that labeler.** Cite the table as aggregate signal — the F-007 +
F-008 cohort sizes and patterns. See
[D-002](../../specimens/DISAGREEMENTS.md) for the discipline note.

**`has_contact_or_appeal_path` is uniformly `unknown_v1`.**
Populating it would require appview profile fetches plus manual
classification of description text; deferred.

**Known classifier debt (T-001):** the pre-existing
`labelers.likely_test_dev` field flags `xblock.aendra.dev` (908k
events/30d, 13 declared defs — the labeler our specimens-track cites
as a real third-party adoption case) and `recordcollector.edavis.dev`
(50k/30d, 67 defs) as test/dev. The maturity classifier uses
`likely_test_dev` as an override and downgrades both to
"experimental." The cohort framing in F-007/F-008 does not depend
on these two specific classifications; the misflag is noise at the
high-volume tail. See T-001 for the review item.

**Sampling scope.** Only labelers Labelwatch has discovered are in
scope (`labelers_total = 501` ever-discovered; 150 with recent
activity or ingested service record). Labelers we have never
observed are out of scope by construction. The
consumer-conversion census also did not cover ~40 of ~47 clients
in the Bluesky showcase; see that page for client-side sampling
caveats.

**Time window.** Counts are "last 30 days" (`events_30d`) at scan
time. Abandonment status is volatile — an "abandoned" labeler that
resumes will move classes in the next scan. Regenerate the snapshot
before citing these numbers in any time-sensitive context.

**No per-account dossiers.** Per labelwatch's `CLAUDE.md`:
aggregate-first, descriptive language, no accusation-shaped outputs.
This page operates per-labeler, not per-account.

## Why this matters

The protocol model of stackable moderation imagines independent
labelers publishing declared semantics, users opting in, and
clients applying scoped moderation. The observed ecosystem, viewed
through this scan, contains all three adjacent-implication failures
of that model:

- High-volume labelers emit testimony their service records do not
  declare. Clients cannot subscribe to these labelers via the
  standard protocol mechanism — there is nothing to subscribe TO.
  (F-007)
- Labelers with rich declared scope go operationally silent. Users
  who subscribe to them get zero label events; the subscription is
  a no-op. (F-008)
- Even live, well-declared third-party labelers are not converted by
  default by any production client we sampled. Adoption is opt-in;
  the consumer-side conversion edge is not promoted from labeler
  publication. (consumer-conversion census + F-001 + F-004)

These are not bugs in any specific labeler. They are properties of
the ecosystem at the current stage of its development. The shape
matters most for downstream tools and policy discussions that
silently equate "a labeler exists for X" with "moderation is
happening to X." This page exists so the conflation has somewhere to
be refused.

## Receipts / generated data

This page's headline numbers are computed from a fixed snapshot;
re-running the scanner against a fresh database may produce
different cohort sizes (the ecosystem evolves).

**Snapshot artifact** (pinned by this page):
- [`artifacts/operator-maturity-scan-2026-06-08.json`](artifacts/operator-maturity-scan-2026-06-08.json) —
  full per-labeler data, 150 rows × 23 columns + scan metadata
- [`artifacts/operator-maturity-scan-2026-06-08.csv`](artifacts/operator-maturity-scan-2026-06-08.csv) —
  flattened CSV for spreadsheet / research use
- [`artifacts/operator-maturity-summary-2026-06-08.md`](artifacts/operator-maturity-summary-2026-06-08.md) —
  one-page TL;DR

**Scanner source:**
- [`docs/analysis/tools/operator_maturity_scan.py`](../../analysis/tools/operator_maturity_scan.py) —
  scan; reproduction:
  ```bash
  sudo -u labelwatch python3 operator_maturity_scan.py \
      --db /var/lib/labelwatch/labelwatch.db \
      --out /tmp/operator_maturity.json
  ```

**Regression check** (asserts the snapshot still produces the cited
headline numbers — F-007 cohort = 14, F-008 abandoned = 65, F-008
substantial-scope subset = 28, vocalabeller specimen = 106k
revisions / 1 def / 0 events, T-001 mis-flags present, full class
histogram):
- [`regression/test_findings_regression.py`](regression/test_findings_regression.py)
  ```bash
  cd docs/findings/operator-maturity/
  python3 regression/test_findings_regression.py
  # PASS (150 rows; all 7 regression checks green)
  ```
  This does NOT assert the numbers must stay stable forever — the
  point is that the SCANNER must explain when they change. If the
  regression fails, investigate why before updating the page.

**Methodology + analysis writeup:**
- [`docs/analysis/labeler-operator-maturity-001.md`](../../analysis/labeler-operator-maturity-001.md) —
  fuller analysis with all four notable patterns

**Adjacent findings cited above:**
- [F-001](../../specimens/DISAGREEMENTS.md) — reference-labeler status
  does not imply default-client conversion
- [F-004](../../specimens/DISAGREEMENTS.md) — third-party labelers
  publish `labelValueDefinitions`; opt-in consumers honor them
- [F-006](../../specimens/DISAGREEMENTS.md) — `needs-review` is
  emitter-undeclared, not ingestion-gap-shaped (the F-007
  single-specimen progenitor)
- [F-007](../../specimens/DISAGREEMENTS.md) — population-scale
  publish-without-declare (this page)
- [F-008](../../specimens/DISAGREEMENTS.md) — stale-service /
  abandoned declared-scope no-op subscriptions (this page)
- [D-002](../../specimens/DISAGREEMENTS.md) — operator-maturity
  taxonomy is heuristic, not normative
- [T-001](../../specimens/DISAGREEMENTS.md) — `likely_test_dev`
  mis-flags xblock + recordcollector

**Consumer-side context:**
- [`docs/analysis/consumer-conversion-census.md`](../../analysis/consumer-conversion-census.md) —
  the "liveness ≠ authority" half of the chain. No sampled client
  hardcodes a non-mod.bsky labeler as a default.
