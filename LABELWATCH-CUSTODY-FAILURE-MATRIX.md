# Labelwatch custody failure matrix

Source: `1fdfd028fd0dde2ca96210fc3cb5d450117efb89`. Isolated contract fixtures; no production mutation. Full per-case records: private campaign `runtime/contract-2a3a1e4c-accc-476a-8f94-1499587d89f4/RESULT.json`.

| Qualification case | Expected / actual | Durable state / visible receipt / retry |
|---|---|---|
| canonical-repeat-and-transport | SAME_IDENTITY / SAME_IDENTITY | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| later-authored-source-record | DISTINCT_IDENTITY / DISTINCT_IDENTITY | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| same-time-distinct-legitimate-record | DISTINCT_IDENTITY / DISTINCT_IDENTITY | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| missing-authored-time | REFUSED / REFUSED | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| conflicting-ts-alias | REFUSED / REFUSED | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| single-file-iterable-verification | ACCEPTED_ONCE / ACCEPTED_ONCE | {"global_delta":"NONE","retry_inserted":0} |
| single-file-poll-cursor | ACCEPTED_ONCE / ACCEPTED_ONCE | {"cursor":"opaque-position","no_network":true} |
| serialized-authoritative-checkpoint | VALIDATED / VALIDATED | {"checkpoint_roundtrip":true} |
| history-only-restore-inactive | REFUSED / REFUSED | {"restore_rows":2} |
| stale-complete-backup | REFUSED / REFUSED | {"authoritative_checkpoint":{"archives":[["single-file","c9ea9bebbd3a28f6145575f81893ea956f53a267910ee8004876f214b9193530"]],"cursors":{"ingest_cursor:https://fixture.invalid":"opaque-position"},"generation":"6","lineage":"/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-global-lifetime-custody-20261005/runtime/contract-2a3a1e4c-accc-476a-8f94-1499587d89f4/single"}} |
| single-file-archive-identity-transfer | ACCEPTED_ONCE / ACCEPTED_ONCE | {"duplicate_archive_replay":0,"quarantine_delta":0,"retired_keys":2} |
| success-before-acceptance-commit | REFUSED / REFUSED | {"accepted_after_rollback":0,"external_receipt":false} |
| overlapping-archive-ownership | REFUSED / REFUSED | {"committed_owners":1,"sealed_owner_reuse":"REFUSED"} |
| receipt-conflict-and-unique-ids | REFUSED / REFUSED | {"first_receipt_ids":[1],"original_preserved":true} |
| replay-after-additive-schema-change | ACCEPTED_ONCE / ACCEPTED_ONCE | {"identity_and_quarantine_delta":"NONE"} |
| single-after_effect_before_identity | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"retry_inserted":1,"state_before_retry":"NOT_ACCEPTED"} |
| single-before_global_commit | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"retry_inserted":1,"state_before_retry":"NOT_ACCEPTED"} |
| single-after_global_commit | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"retry_inserted":0,"state_before_retry":"ACCEPTED_ONCE"} |
| segmented-after_effect_before_identity | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"retry_inserted":1,"state_before_retry":"NOT_ACCEPTED"} |
| segmented-before_global_commit | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"retry_inserted":1,"state_before_retry":"NOT_ACCEPTED"} |
| segmented-after_global_commit | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"retry_inserted":0,"state_before_retry":"ACCEPTED_ONCE"} |
| before_receipt | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"receipt_ids":[1],"retry_inserted":0} |
| during_receipt | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"receipt_ids":[1],"retry_inserted":0} |
| after_receipt_before_ack | ACCEPTED_ONCE / ACCEPTED_ONCE | {"process_exit":73,"receipt_ids":[1],"retry_inserted":0} |
| duplicate-worker-invocation | ACCEPTED_ONCE / ACCEPTED_ONCE | {"workers":2} |
| archive-conversion_killed | ACCEPTED_ONCE / ACCEPTED_ONCE | {"intermediate_read":"ONE_OWNER","quarantine_delta":0,"retired_hot_keys":0,"retry_inserted":0} |
| archive-verification_killed | ACCEPTED_ONCE / ACCEPTED_ONCE | {"intermediate_read":"ONE_OWNER","quarantine_delta":0,"retired_hot_keys":0,"retry_inserted":0} |
| archive-after_parquet_before_checkpoint | ACCEPTED_ONCE / ACCEPTED_ONCE | {"intermediate_read":"ONE_OWNER","quarantine_delta":0,"retired_hot_keys":0,"retry_inserted":0} |
| archive-before_archive_commit | ACCEPTED_ONCE / ACCEPTED_ONCE | {"intermediate_read":"ONE_OWNER","quarantine_delta":0,"retired_hot_keys":0,"retry_inserted":0} |
| archive-after_archive_commit_before_receipt | ACCEPTED_ONCE / ACCEPTED_ONCE | {"intermediate_read":"EXPLICIT_REFUSAL","quarantine_delta":0,"retired_hot_keys":0,"retry_inserted":0} |
| archive-after_receipt_before_ack | ACCEPTED_ONCE / ACCEPTED_ONCE | {"intermediate_read":"ONE_OWNER","quarantine_delta":0,"retired_hot_keys":0,"retry_inserted":0} |
| reprocessing | REFUSED / REFUSED | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| unauthorized-recovery | REFUSED / REFUSED | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| future-and-ancient-archive-lifetime | ACCEPTED_ONCE / ACCEPTED_ONCE | {"accepted":3,"history_count":3,"hot_keys":0,"quarantine_delta":0} |
| missing-identity-index | REFUSED / REFUSED | {"cursor_preserved":true} |
| same-identity-changed-record-content | REFUSED / REFUSED | {"state_delta":"Identity-only check: no durable writes; hash equality/difference or explicit refusal"} |
| mutant-verification-global-quarantine | DETECTED / DETECTED | {"disposition":"Intentionally incorrect fixture retained; qualification detects the regression","global_quarantine_rows":1} |
| mutant-authored-expiry-without-archive-membership | DETECTED / DETECTED | {"actual_count":4,"disposition":"Intentionally incorrect fixture retained; qualification detects the regression"} |
| mutant-receipt-discovery-before-custody | DETECTED / DETECTED | {"disposition":"Intentionally incorrect fixture retained; qualification detects the regression","fixed_count":1,"old_discovery_count":2,"transition":"exact pre-commit durable-state substitution"} |
| bounded-mutable-lifetime | BOUNDED_HOT_KEYS / BOUNDED_HOT_KEYS | {"claim":"structural only; no current-volume horizon capacity claim"} |
| historical-coverage-regression | COMPLETE_OR_REFUSED / COMPLETE_OR_REFUSED | {"receipt":"/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-global-lifetime-custody-20261005/runtime/contract-2a3a1e4c-accc-476a-8f94-1499587d89f4/coverage/RESULT.json"} |

Crash prefixes before acceptance commit are NOT_ACCEPTED. After commit they are ACCEPTED_ONCE even with absent/partial receipt. Missing authoritative identity is INDETERMINATE with acceptance/cursor refusal. Every retry in the matrix preserves one accepted effect. No ACCEPTED_TWICE outcome is permitted.

Known-bad controls deliberately restore: historical verification writing full global quarantine; acceptance without surviving archive membership after hot-key expiry; discovery admitting a receipt-backed object before global custody commit. All are detected. The last control is a deterministic durable-state substitution, not an actual pre-commit process-death claim.

The independently found first-repair counterexamples are immutable under run `eae274c1-c0da-4317-bf18-c17f85ac4d98`: uncommitted success, cross-owner archive overlap, JSON checkpoint tuple/list mismatch. Corrected regressions remain executable in `tools/segmented_qualification/custody_qualify.py`.

Independent successor evidence and limits are linked from [qualification](LABELWATCH-CUSTODY-QUALIFICATION.md). Verification/reprocessing never receive implicit current mutation authority. Full horizon throughput/capacity is outside this matrix.
