# Labeler operator-maturity 001

> **Question this answers:** Treating each labeler like a service, would
> a Bluesky user — or a client developer deciding what to expose — trust
> this thing enough to subscribe to it?
>
> **Headline:** 150 observed labelers (in last 30d activity or ever-
> ingested service record). **65 are abandoned** (43%). **14 high-volume
> emitters publish zero declared scope** — labels firing at up to 908k
> events/30d with no service record at all, meaning no Bluesky client can
> actually subscribe to them via the protocol's standard
> `labelersPref` mechanism. They're shouting into a hallway with no
> doors. F-006 is not an isolated curiosity; it is a population-scale
> shape.

## Method

Scan `labelwatch.db.labelers` + `discovery_events` for any labeler
with EITHER recent events (`events_30d > 0`) OR an ever-ingested
service record. For each, derive heuristic flags + a maturity_class
per the schema in `labelwatch-next-questions.md § A1`.

Scanner: `docs/analysis/tools/operator_maturity_scan.py`
Output: `docs/analysis/data/operator_maturity_001.json` (150 rows)
Run: 2026-06-08; scanned via `sudo -u labelwatch python3` on prod.

Per-row columns:
```
labeler_did | handle | display_name | labeler_class | is_reference
| active_recently | declares_scope | explains_labels
| has_contact_or_appeal_path  ← UNKNOWN in v1 (requires appview
                                profile fetch + manual classification)
| has_stable_service_record   ← 1 ≤ revisions ≤ 5
| label_count_30d | distinct_targets_30d | latest_label_def_count
| user_visible_consequence_known  ← declares with hide/warn/ignore
| service_record_revisions | first_record_at | last_record_at
| last_seen | regime_state | endpoint_status | visibility_class
| auditability | likely_test_dev | maturity_class
```

Heuristic maturity classes — **NOT measurement**:

| class | criteria |
|---|---|
| `platform-root` | `did:plc:ar7c4by46qjdydhdevvrndac` (mod.bsky) |
| `abandoned` | had service record, `events_30d = 0` |
| `experimental` | sparse activity (`events_30d ∈ [1, 10)`) OR `likely_test_dev=1` |
| `personal/reputational` | `events_30d ∈ [10, 100)`; declares some scope |
| `community-service` | `events_30d ∈ [100, 10000)`; declares scope AND explains labels |
| `moderation-infrastructure` | `events_30d ≥ 10000`; declared scope |
| `unknown` | insufficient signal (no record, no events, or contradictory) |

## Class histogram

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

## Top 25 by `label_count_30d` (boring-table headline)

| # | maturity_class | handle | 30d | defs | expl | stable | vis | rev |
|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `platform-root` | moderation.bsky.app | 908000 | 18 | ✓ | ✓ | ✓ | 1 |
| 2 | `unknown` | **antiantiai.bsky.social** | 908000 | 0 | – | – | – | 0 |
| 3 | `experimental` | xblock.aendra.dev | 908000 | 13 | ✓ | ✓ | ✓ | 2 |
| 4 | `unknown` | **labeler.plural.host** | 906500 | 0 | – | – | – | 0 |
| 5 | `moderation-infrastructure` | label.haus | 708300 | 2 | ✓ | ✓ | ✓ | 1 |
| 6 | `unknown` | **oracle.posters.rip** | 698575 | 0 | – | – | – | 0 |
| 7 | `moderation-infrastructure` | snubstack.boo | 650332 | 3 | ✓ | ✓ | – | 1 |
| 8 | `moderation-infrastructure` | labeler.antisubstack.fyi | 493800 | 1 | ✓ | – | ✓ | 6 |
| 9 | `unknown` | **uspol-labeler.bsky.social** | 446585 | 0 | – | – | – | 0 |
| 10 | `moderation-infrastructure` | fucks.supply | 416885 | 1 | ✓ | ✓ | ✓ | 4 |
| 11 | `moderation-infrastructure` | pds.labeler.tny.im | 357956 | 102 | ✓ | – | ✓ | 112 |
| 12 | `moderation-infrastructure` | skywatch.blue | 262475 | 35 | ✓ | ✓ | ✓ | 4 |
| 13 | `unknown` | **bottags.bsky.social** | 121267 | 0 | – | – | – | 0 |
| 14 | `moderation-infrastructure` | kys.makersfield.co | 101450 | 1 | ✓ | ✓ | ✓ | 1 |
| 15 | `unknown` | **mediardaire.bsky.social** | 73403 | 0 | – | – | – | 0 |
| 16 | `unknown` | **facelabels.bsky.social** | 58779 | 0 | – | – | – | 0 |
| 17 | `moderation-infrastructure` | stechlab-labels.bsky.social | 57785 | 26 | ✓ | – | ✓ | 33 |
| 18 | `moderation-infrastructure` | atls.city | 56796 | 5 | ✓ | ✓ | ✓ | 5 |
| 19 | `unknown` | **labeler.nunnybabbit.com** | 54410 | 0 | – | – | – | 0 |
| 20 | `experimental` | recordcollector.edavis.dev | 50727 | 67 | ✓ | ✓ | ✓ | 5 |
| 21 | `unknown` | **mod.shawn.party** | 49224 | 0 | – | – | – | 0 |
| 22 | `unknown` | **moderation.plyr.fm** | 29008 | 0 | – | – | – | 0 |
| 23 | `unknown` | **iraqwarmongers.com** | 28928 | 0 | – | – | – | 0 |
| 24 | `moderation-infrastructure` | alt-text-labeler.bsky.social | 13000 | 2 | ✓ | – | ✓ | **17911** |
| 25 | `unknown` | **activitylabeler.certified.one** | 12663 | 0 | – | – | – | 0 |

`30d` = events in last 30 days. `defs` = labelValueDefinitions in latest
service record. `expl` = at least one definition has locale text.
`stable` = 1 ≤ revisions ≤ 5. `vis` = `user_visible_consequence_known`.
`rev` = service-record revisions ever observed.

**Bold handles** in the table are high-volume emitters with **zero
declared scope** — the F-006 pattern at scale.

## Notable patterns

### Pattern 1 — High-volume "shout into the void" emitters (14 labelers)

Labelers emitting >1k events/30d with `latest_label_def_count = 0`:

| handle | 30d events |
|---|---:|
| antiantiai.bsky.social | 908000 |
| labeler.plural.host | 906500 |
| oracle.posters.rip | 698575 |
| uspol-labeler.bsky.social | 446585 |
| bottags.bsky.social | 121267 |
| mediardaire.bsky.social | 73403 |
| facelabels.bsky.social | 58779 |
| labeler.nunnybabbit.com | 54410 |
| mod.shawn.party | 49224 |
| moderation.plyr.fm | 29008 |
| iraqwarmongers.com | 28928 |
| activitylabeler.certified.one | 12663 |
| (… 2 more under 12k) | |

**What this means.** ATProto's stackable-moderation framework requires
a labeler to publish an `app.bsky.labeler.service` record with
`labelValueDefinitions` for a Bluesky client to honor its labels via
`labelersPref`. **These 14 labelers cannot be subscribed to via the
standard protocol mechanism — they have nothing to subscribe TO.**
Their testimony is published into the protocol but the consumer-side
door is locked.

Two of them (`antiantiai`, `labeler.plural.host`) emit at
**moderation.bsky.app's same volume** (~908k events/30d each). The
ecosystem has high-volume labelers whose testimony enters the
firehose with no consumer-facing semantics whatsoever.

F-006 was one case (`needs-review` from mod.bsky). At population
scale, the same shape applies to 14+ labelers and accounts for
substantial event volume.

### Pattern 2 — Service-record churn outliers

Labelers with > 10 service-record revisions:

| handle | revisions | defs | events_30d | maturity_class |
|---|---:|---:|---:|---|
| **vocalabeller.kanshen.click** | **106,000** | 1 | 0 | abandoned |
| **alt-text-labeler.bsky.social** | **17,911** | 2 | 13,000 | moderation-infrastructure |
| cons.fyi | 1,034 | 108 | 0 | abandoned |
| labeler-bot-tan.suibari.com | 866 | 18 | 0 | abandoned |
| pds.labeler.tny.im | 112 | 102 | 357,956 | moderation-infrastructure |
| moderation.blueat.net | 99 | 20 | 0 | abandoned |
| mushroom-labeler.bsky.social | 85 | 1 | 0 | abandoned |
| art.tartgames.com | 34 | 7 | 0 | abandoned |
| stechlab-labels.bsky.social | 33 | 26 | 57,785 | moderation-infrastructure |
| awacs.prtgn.org | 19 | 7 | 0 | abandoned |

**`vocalabeller.kanshen.click` published 106,000 revisions of its
service record** with one labelValueDefinition and zero events in the
last 30 days. That's not a normal failure mode — it's something
operationally pathological (likely a stuck redeploy loop). The
labeler is currently inert but its discovery footprint dominates.

`alt-text-labeler.bsky.social` (17,911 revisions, 2 defs, active)
likely has similar update churn but currently produces labels.

### Pattern 3 — Abandoned with substantial declared scope (28 labelers, defs ≥ 6)

| handle | defs | last_seen |
|---|---:|---|
| sonasky.app | 684 | 2026-06-08 |
| stemlabels.xyz | 461 | 2026-06-08 |
| pokemon.sonasky.app | 161 | 2026-06-08 |
| label.wol.blue | 119 | 2026-06-08 |
| cons.fyi | 108 | 2026-06-08 |
| hsd.lilacparty.us | 75 | 2026-06-08 |
| pronounsinb.io | 68 | 2026-06-08 |
| demonslayerfeeds.bsky.social | 46 | 2026-06-08 |
| 283labeler.mp0.jp | 44 | 2026-06-08 |
| (… 19 more 6–44 defs) | | |

Significant labeler operator effort went into publishing rich
service records — some with hundreds of definitions — and then no
labels emitted in the last 30 days. Subscribing to any of these
would currently be a no-op. This is the "stale service" failure
mode the maturity column is built to surface.

`last_seen` is the labeler-record-last-seen timestamp, not last-
emission — most show 2026-06-08 because the discovery sweep updated
their record-last-seen recently. None has emitted labels recently.

### Pattern 4 — `likely_test_dev` heuristic flags real labelers

The pre-existing `likely_test_dev` column flagged 2 high-volume
labelers as test/dev:

- `xblock.aendra.dev` — 908k events/30d, 13 declared defs, the very
  labeler Bundle G's specimen-003 cites as a real third-party
  consumer-adoption case
- `recordcollector.edavis.dev` — 50k events/30d, 67 declared defs

Neither looks experimental in behavior. The pre-existing
`likely_test_dev` heuristic is probably matching on handle/domain
patterns (`*.bsky.social` not preferred? `*.dev` flagged?) but
treating these as experimental for maturity classification loses
signal. This is itself a candidate finding for the existing
labelers-table's classification logic: the test_dev signal
overrides every other dimension in our maturity classifier and
silently downgrades two real labelers. Worth a review pass.

## What this enables / doesn't enable

This is product/research/SRE-style profiling. It IS useful for:

- A user deciding whether to subscribe to a labeler — the boring
  table answers most of the relevant questions in one glance.
- A client developer wondering which third-party labelers are even
  worth surfacing in a discovery UI (skip the 65 abandoned ones).
- A researcher characterizing the Bluesky labeling ecosystem at a
  population level.
- A labeler operator self-auditing where their service stands in
  the ecosystem distribution.

It does NOT:

- Make any admissibility claim. The boring table is descriptive.
- Classify labelers morally. "abandoned" / "experimental" /
  "community-service" are operational categories.
- Cover closed-source clients' subscription behavior.
- Establish `has_contact_or_appeal_path` (the column exists but is
  uniformly `unknown_v1` because populating it needs appview
  profile fetches + a manual classification pass).

## Findings worth tracking forward

- **F-007 candidate:** the population-scale "publish without
  declare" shape (14 labelers, up to 908k events/30d each, no
  service record) is operationally interesting beyond F-006's
  single instance. Could deserve its own finding entry, framed as a
  consumer-protocol observation: **substantial label-event volume
  enters the firehose with no consumer-side subscription path
  defined by the emitter.** Not added to DISAGREEMENTS.md in this
  commit; left for explicit decision.
- **Pre-existing `likely_test_dev` heuristic warrants review.**
  It's overriding correct classification for xblock and
  recordcollector.
- **Definition-drift analysis (B1)** is now more clearly motivated
  — Pattern 2 shows that service-record churn is real and varied.
  When that scan runs, the diff stream is non-empty.

## Caveats

- Maturity classes are heuristic, not measurement. Cutoffs are
  defensible but arbitrary (e.g., why 10000 not 5000?). Boundary
  cases will shift on threshold tuning.
- `has_contact_or_appeal_path` is uniformly `unknown_v1`.
- Last-seen timestamps look uniform (2026-06-08) because of recent
  discovery sweep; semantically these don't equal last-emission
  time.
- Scope is "labelers active or discovered." Labelers we've never
  seen are out of scope by construction.
- Per labelwatch CLAUDE.md: aggregate-first, no per-account
  dossiers, no accusation-shaped outputs. This table is
  per-labeler not per-account.

## Provenance

- **Scanned:** 2026-06-08
- **Source:** `/var/lib/labelwatch/labelwatch.db` on prod
  (192.46.223.21)
- **Scanner:** `docs/analysis/tools/operator_maturity_scan.py`
  v1
- **Raw output:** `docs/analysis/data/operator_maturity_001.json`
  (150 rows, full data)
