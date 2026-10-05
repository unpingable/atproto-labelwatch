# Labelwatch custody decision — October 5

`CUSTODY_CONTRACT_QUALIFIED`

Qualified source: `1fdfd028fd0dde2ca96210fc3cb5d450117efb89`.
Independent acceptance `0e64d667-20c4-4b91-874a-32bde31ada33` confirms the bounded
shared contract. [Qualification](LABELWATCH-CUSTODY-QUALIFICATION.md) contains
measurements, exact receipt hashes, assumptions and executable commands.

| Acceptance property | Evidence |
|---|---|
| One canonical observable source record, one acceptance | Shared single-file/segmented transaction; duplicate workers and crash retries |
| Authored clock does not expire identity | Future/ancient archive replay remains duplicate after hot key removal |
| Verification has no current mutation authority | Existing quarantine unchanged; unknown below-floor input refuses with cursor preserved |
| Receipt follows committed custody | Before/during/after publication cuts; uncommitted receipt refused; conflicting receipt preserved |
| Archive custody is exclusive | Overlap refused; orphan excluded; canonical authority required for retirement |
| Restoration cannot invent absence | Authoritative JSON checkpoint succeeds; stale or incomplete restoration refuses |
| Mutable identity need not retain all historical payloads | Owner-based transfer to immutable exact indexes; 12-owner hot keys zero |
| Qualification detects the known bad designs | Three original classes detected; three first-repair counterexamples preserved and corrected |

This is a contract decision. The segmented architecture remains unadopted.
Production remains unenrolled; mutations **NONE**. Legacy identities require an
explicit reconciliation/fence proof before deployment; unavailable canonical
fields cannot be manufactured from older receipts or ingestion-clock timestamps.

Existing [Labelwatch #7](https://github.com/unpingable/atproto-labelwatch/issues/7)
owns exactly the next qualification gates:

1. Complete 40+7-day global-state capacity: all tables, cold identity/index bytes,
   acceptance admission and whole-owner expiry/WAL/scratch at current volume.
2. Required current-volume queries over the complete 40-day historical horizon,
   retaining the exact historical-coverage regression.
3. Long-history catalog/index admission bounds, including cold membership work;
   no rescan of all historical payloads to admit today's owner.

Use fresh aggregate storage admission and the existing machinery. Prior rollover
and reclamation need not repeat unless an identified custody change affects their
property; prior throughput/expiry claims do not automatically transfer.
If those three gates pass, adopt the existing candidate and stop architecture
qualification. No new engine comparison, migration or production activation is
part of this closure. Incident #6, public API #1 and Monitor #18 remain separate.
