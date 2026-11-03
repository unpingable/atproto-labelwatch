# Labelwatch ordinary operational closeout — November 3, 2026

The October 12 production cutover remains independently accepted at runtime `aef40df17c17ef9746e4d03cd080ad096808945c`,
with presentation `f693a067e2957b395f970e2cdeb72b01e3945eee`. The original failed October 5 retention occurrence remains
failed and superseded; it was never a successful rerun. Issues #6 and #7 remain open for their existing seven-day
ordinary operational observation milestone.

Read-only November 3 observations found authentic daily maintenance advancing to generation 22, fresh collection
and public facts, retained daily storage/reserve observations, and accepted Slack receipts. They also found the
gap table at 128/128 rows: 121 `source_unavailable`, six maintenance and one unknown interval, with overflow from
October 19 onward. Public facts truthfully disclose unknown coverage and `gap_overflow=true`. An all-ok watcher
report and elapsed seven days do not satisfy the no-overflow acceptance condition.

The collector classified each discovery outage as a store-wide `source_unavailable` interval even while the
sticky roster continued to collect. The bounded correction records discovery availability in one fixed metadata
record, preserves its continuation and roster, and leaves collection gaps to actual collection interruptions.
Missing, failed, stale or incomplete discovery keeps aggregate coverage unknown. The existing watcher gains
explicit gap-overflow and sustained-discovery-refusal observations through its existing Slack route.
Current discovery can include a new scan in progress when its last successful complete scan is still within
600 seconds and the latest attempt succeeded; normal pagination must not make coverage oscillate.

The 306 unstarted sources in this observation all had a current endpoint failure, a saturated bounded failure
counter and zero bootstrap probes. No pending bootstrap source had a successful page newer than its recorded
failure. This is observed external unavailability; it does not demonstrate scheduler starvation or successful
bootstrap completion for every reachable source.

## Review and deployment boundary

The correction is a source candidate, with no production mutation or manual notification send. It preserves the
deployed runtime/presentation behavior and the already deployed monitor changes `d7a16b4` and `145296e`. The combined
candidate also includes the existing predecessor/total-loss line (`cefb3cb`, `3412438`) and public main `22592dc`.
The accepted account/API presentation, discovery status/currentness, predecessor disclosure and held-on-lost-store
publication are retained together. These source integrations do not change the selected production release.

Runtime changes are `src/labelwatch/recent_collector.py`, `tools/segmented_qualification/recent_storage.py`,
`recent_publish.py` and `recent_product.py`. One fixed metadata key is added within the existing v8 schema and caps;
there is no event/index migration or change to the 30-day window. The watcher script has a new obligations digest.
The public overview schema is unchanged; discovery details remain operator-only, and affect the existing coverage
verdict conservatively.

A reviewed deployment must use the existing release/rollback procedure, preserve the selected presentation and
each installed unit override, and update the selected product script deliberately: the production product unit
currently executes a separate presentation tree. Replacing the runtime package alone does not update that tree.
Install the reviewed watcher through its existing unit and Slack configuration; reconcile its new script hash
and obligations identity in Cartography. Existing state/observations/intents must remain in place. No old failed
producer, retired timer or qualification campaign is restarted.
The first scheduled evaluation of the corrected watcher is expected to produce one L12 notice for the existing
true overflow. That is ordinary detection through the established route, not a new manual notification drill.

The prior runtime ignores the additive discovery key and resumes the old discovery-gap behavior. It also lacks
the combined predecessor and held-on-lost-store behavior. After recovery has initialized a new epoch or recorded
loss/unknown predecessor metadata, do not expose that state through the old predecessor-blind presentation:
keep the product held and select a release that preserves the combined recovery contract. No downgrade acceptance
for that state is claimed. A watcher rollback must not be reported as continued L12/L13 coverage. Existing gap rows, overflow bounds,
archives, failed occurrences and acceptance records must not be rewritten on deployment or rollback. Overflow
can clear only through the accepted ordinary expiry/recovery path. The candidate therefore does not itself meet
the operational milestone: after deployment, observe actual discovery/collection, normal maintenance, coverage,
storage/reserves and existing Slack transitions, then obtain independent acceptance under the original criteria.

## Formalization consideration

The proposition is scoped to the exact collector/store/publisher/watcher transition: a failed discovery request
preserves known-source continuation and produces bounded, currentness-qualified discovery evidence rather than
asserting collection loss. Overflow and discovery incidents persist through unknown/held evidence and resolve only
on explicit fresh observations. Real-store regressions and finite watcher transition tests are appropriate for
this bounded correction. They do not prove continuous service, provider reachability, historical completeness,
measured full-window capacity or the operational observation milestone. The existing acceptance owner decides
that milestone from actual retained evidence.
