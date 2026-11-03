# Labelwatch data contract

Labelwatch is an observatory, not a system of record. It may lose history. It
must never claim continuity or coverage it cannot substantiate.

**Historical observation data is not required for service recovery unless an
explicitly identified capability depends on it.** No capability in the rolling
observatory currently depends on it.

## What it observes

Labelwatch watches public ATProto labelers and records the label application
and removal events it observes, timed by its own acceptance clock. Source-authored
label dates are never treated as observation times. It does not know whether a
label is currently active.

For a source with no saved cursor (every source in a new epoch), Labelwatch
locates the source's current head with a bounded number of probes and collects
forward from there. It does not traverse the source's history. This is a choice:
the observatory reports what it saw happen, not what a source says happened
earlier.

## The window

Observations are kept for a rolling window of 30 days. Older observations
expire and are not served; requests outside the window are refused explicitly.

## Three independent guarantees

These are separate promises. Holding one says nothing about the others.

- **Retention.** Within the current epoch, an accepted observation is kept until
  it leaves the 30-day window, then it expires. There is no backup and no
  archive that can be restored into the product.
- **Observation continuity.** Within one epoch, every known interruption
  (source unavailable, collection paused, cursor reset, maintenance, unknown)
  is recorded as a gap with its reason. No recorded gaps does not prove
  complete coverage. Continuity never carries across epochs.
- **Recovery.** The service can always be restored by initialising a new, empty
  store. Recovery does not restore history; it starts a new epoch.

## What happens on loss

When the store is missing, emptied or unreadable:

- The collector refuses to run until a store is initialised. Readers refuse
  without creating anything in the store directory, so they cannot block
  re-initialisation.
- Overview publication replaces the last `overview.json` with an explicit held
  envelope (`hold: true`, no readings) and exits non-zero. A stale reading is not
  left standing. Pages report the summary as unavailable, which is absence of
  evidence, not evidence that nothing happened.
- The operator runs `recent_init.py` into an empty root. This starts a new epoch
  with a new acquisition generation id and start time. Everything in the window
  before that start is shown as "not observed".
- The default is uncertainty: continuity before the new epoch is recorded as
  unknown unless the predecessor is discovered or declared. Init discovers it from
  the last published `overview.json` (in `$LABELWATCH_RECENT_PUBDIR`, or
  `--published-overview PATH`): when that names a generation, the new store
  records it as lost, and the interval from its start to the new epoch start is
  shown as "observed, then lost" (the overview is a snapshot, so its last
  observation time is shown as the last known one, not the end). An absent,
  unreadable or held overview, or one without a generation, records unknown
  continuity, never a first install.
- The operator can declare instead, which skips discovery: `--continuity-break`
  (lost, extent unknown), `--predecessor-receipt` with the lost store's init
  receipt, `overview.json` or `STATUS.json` (lost, with its recorded bounds), or
  `--first-install`, the only way to record that no previous epoch exists.
  Conflicting flags are refused. A lost predecessor's interval is shown as
  "observed, then lost": with its recorded bounds when known, otherwise as the
  widest interval it could cover, labelled as such. After a break, or with
  continuity unknown, the pages do not say that no gaps are recorded.
- The previous-epoch text on `/about` and `/previous-epoch` comes only from the
  store's recorded predecessor. With none recorded, it says continuity before
  this epoch is unknown. Stores created before predecessor records existed, which have no
  record and started before 2026-10-15, keep the one real earlier epoch: it was
  withdrawn and ended on 2026-10-06.

## State whose loss affects continuity or identity

- **Epoch identity**: the acquisition generation id, its start time, and the
  recorded predecessor. Losing these ends the epoch.
- **Window position and gap record**: the retention frontier, the recorded gaps
  and their reasons. Losing these loses the ability to disclose interruptions.
- **Per-source collection position**: each source's endpoint, cursor, bootstrap
  phase and last observation. Losing these forces every source to re-establish
  its head, with coverage unknown until it does.
- **Retained observations**: the event records for the window. Losing these
  loses history, but not the ability to serve.
- **Records kept outside the store**: the init receipt and the last published
  overview or status. After a total loss these are the only evidence of the lost
  epoch's bounds; without them the loss can still be declared, with unknown
  extent.

The source roster seed only decides which labelers are enrolled; it can be
re-applied and carries no observation history.

## What it never claims

- "Not observed" is never shown as quiet, and no observations never means no
  labels exist.
- No recorded gaps is never presented as complete coverage.
- Observations from an earlier epoch are never presented as observed by the
  current one.
- A previous epoch's end date is never stated unless it was recorded.
- Current label state is always unknown.

## Known limitations

- A loss that is neither discovered nor declared at re-initialisation is shown
  as "not observed" before the new epoch start, with continuity before it
  unknown. Discovery only sees the last overview left in the publish directory;
  a wrongly declared `--first-install` hides a loss.
- A predecessor taken from an init receipt alone has no last-observation time;
  the lost interval then runs to the new epoch start.
- A `reader.lock` left in an emptied store directory by an older release still
  blocks re-initialisation; remove it by hand.
- The facts API that republishes `overview.json` lives outside this repository.
  It has been reported to serve the overview's age without a freshness verdict;
  that is not verified or changed here. Consumers should check `generated_at`
  and `hold` themselves.
