-- T-003 data fix: split labeler_class from is_reference.
-- One-shot SQL to correct rows already in the labelers table.
-- Idempotent: re-running has no effect once the rows are correct.
--
-- The bug: discover.py's _classify_labeler() set labeler_class
-- ='official_platform' for every DID in config.reference_dids,
-- conflating institutional authority with calibration role.
--
-- The fix in code (discover.py): only mod.bsky is official_platform;
-- other reference DIDs stay third_party with is_reference=1.
--
-- This script applies the same correction to existing rows. Reference
-- labelers other than mod.bsky get labeler_class='third_party' while
-- keeping is_reference=1.
--
-- mod.bsky (did:plc:ar7c4by46qjdydhdevvrndac) is intentionally
-- excluded — it IS official_platform AND is_reference.

UPDATE labelers
   SET labeler_class = 'third_party'
 WHERE labeler_class = 'official_platform'
   AND labeler_did <> 'did:plc:ar7c4by46qjdydhdevvrndac';

-- Sanity SELECT (run after the UPDATE to verify):
--   SELECT labeler_did, handle, labeler_class, is_reference
--   FROM labelers
--   WHERE labeler_class = 'official_platform' OR is_reference = 1
--   ORDER BY labeler_class, handle;
--
-- Expected after fix:
--   official_platform + is_reference=1:  moderation.bsky.app (1 row)
--   third_party + is_reference=1:        skywatch.blue, label.haus,
--                                         and any other reference DIDs
