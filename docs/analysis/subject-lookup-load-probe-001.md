# Subject-lookup load-probe — gap spec, 2026-06-10

> **Status: named gap, not yet built.** Filed during the
> `whatsonme.frontdoor.v0` rendering slice. The shape audit
> (`labelwatch.index_audit.v1`) verdicted admissible against the live
> 40M-row DB on 2026-06-10 using the all-zeros sentinel probe DID. The
> sentinel probe answers a shape question — "does the planner use the
> right index?" — not a load question. The frontdoor rendering slice is
> unblocked on shape; broad promotion of the surface should not happen
> until load is characterized against a real subject.

## Why this exists

`labelwatch.index_audit.v1` is a per-query shape audit. The receipt
captures `EXPLAIN QUERY PLAN`, observed runtime, and index coverage
against a synthetic probe DID (`did:plc:000…0`) that matches zero rows.
That measurement is a **floor**: it shows the planner can find the
index, but it does NOT show the cost of the GROUP BY work, the
read-amplification of fanning across labelers, or the worst-case
contention against the live writer.

For most subjects this floor is the operating cost. For some subjects
— popular accounts with hundreds of distinct labels across many
labelers — the per-subject result set is large enough that the
post-fetch GROUP BY / sentence generation / authority-effect classify
loop becomes the binding cost, not the index probe.

The frontdoor is structurally safe (admissible shape). Broad
promotion — Bluesky announcement, sharing on social, sustained homepage
traffic — should be gated on a separate load characterization.

## What the load probe must answer

Per-bucket runtime: given subjects in the **top decile of label volume**
(by `unique_targets_30d` / per-DID event count, however we sample), what
do Q2 / Q3 / Q8 cost end-to-end?

Concretely:

1. **Identify a sample of high-volume subjects.** Top 100 DIDs by
   `COUNT(*) GROUP BY target_did` over the last 30 days. The query that
   selects this sample is itself a `SCAN label_events` and is therefore
   audit-refused — the load probe is a one-shot offline measurement,
   not a piece of the live surface.
2. **Run `lookup_subject(conn, did, audit_receipt=admissible)` against
   each sample subject.** Measure:
   - wall time end-to-end
   - per-query SQLite time (Q2, Q3, Q8)
   - python-side time (authority-effect classify, sentence generation,
     HTML render)
   - peak memory delta
   - number of labelers in the result, number of label values
3. **Bucket the results** by (number of distinct labelers touching
   subject, total events against subject). Surface the 50th/90th/99th
   percentile.
4. **Emit a receipt** (`labelwatch.load_probe.v1`?) describing the
   sample, the percentiles, and a verdict:
   - `admissible_for_publication` — p99 wall time < 500ms, no
     unbounded query path observed
   - `admissible_with_debt` — p99 wall time 500ms–5s, surface ships
     but accepts that some subjects render slowly
   - `refused_unbounded` — p99 > 5s OR any single subject hung,
     surface gated until remediation

## What it must NOT do

- **Must not be wired into the live frontdoor.** This is offline /
  one-shot characterization. The frontdoor depends on the shape audit
  (`labelwatch.index_audit.v1`), not the load probe.
- **Must not adjudicate or rank labelers / subjects.** The load probe
  describes the cost of looking up a subject; it does not say anything
  about the subject. Same publication discipline as the frontdoor.
- **Must not be run against the live DB during peak ingest** without
  first checking writer-thread health. The probe is read-only but its
  fanned reads can pressure the WAL.

## Sequencing

Per chatty (2026-06-10): "Rendering is unblocked by admissible query
shape; promotion is still pending real-subject load characterization."

The rendering slice ships first. The load probe is a follow-up before
broad promotion (Bluesky thread, RSS, Substack). The threshold for
running the probe is "we're about to point traffic at this," not "we
have downtime."

## Composes with

- `docs/analysis/subject-lookup-frontdoor-001.md` — the surface
  contract this probe characterizes.
- `docs/analysis/receipts/labelwatch.index_audit.whatsonme.frontdoor.v0.20260610T010254Z.json`
  — the shape audit. The load probe is a separate axis (load, not
  shape).
- The "weather, never verdict" doctrine — load percentiles describe
  the surface, not the subjects sampled.
