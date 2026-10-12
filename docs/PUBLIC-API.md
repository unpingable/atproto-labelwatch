# Public API and account data

[Website guide](https://labelwatch.neutral.zone/api) ·
[Facts API reference](https://api.neutral.zone/atproto/) ·
[OpenAPI](https://api.neutral.zone/atproto/openapi.json)

The two surfaces have different scopes. The Facts API publishes anonymous,
read-only **aggregate** observations. Labelwatch account pages and bounded
exports contain subject-specific **public label observations**. Neither requires
a login; account data is not available through the aggregate Facts API.
Operational health and private source diagnostics are not public routes.

## Aggregate facts

```sh
curl --fail-with-body https://api.neutral.zone/atproto/v1/labelwatch/overview
curl --fail-with-body https://api.neutral.zone/atproto/v1/instruments
```

The overview envelope contains `contract`, `instrument`, `generated_at`,
`retrieved_at`, `age_seconds`, `freshness`, `facts`, `caveats` and `provenance`.
`facts.counts` contains observed event totals; `facts.coverage` describes gaps,
warm-up and the observed interval; `facts.acquisition` identifies the epoch.
`facts.current_label_state` is `unknown`. Null alert counts mean unknown, not zero.

Fresh publication does not imply complete coverage. The `freshness` object
reports publication and, when evaluated, collection age with their bounds.
Labelwatch serves HTTP 200 only for `fresh` or `warming_up` observations.
Stale, held or unavailable observations return HTTP 503 and an `error` object,
not a fabricated zero. A missing source can omit freshness detail. The index's
`PRESENT` means readable; check `freshness_state` before treating it as servable.
See the Facts reference for the complete schema, caching and CORS contract.

## Account views

`GET /account?did=…&days=7` returns HTML. Account-DID label observations appear
first, followed by post labels and, when present, other record labels. Each
section is newest-observed first and preserves labeler, subject, action and both
observation/source timestamps. Counts are events, not currently active labels.

`days` accepts only `7` or `30`, clipped to the available observation frontier.
Alternatively supply UTC `start` and `end` timestamps: start inclusive, end
exclusive. Requests outside the rolling window are refused.
Optional exact filters are `labeler` (DID), `value`, `action`
(`application_observed`, `removal_observed`, `unknown_action`) and `target_kind`
(`account`, `post`).

`/lookup?account=…&days=7` redirects a DID to its account view. Handle lookup
requires the operator's existing handle-resolution option; it is not enabled
on every installation. DID lookup does not require that option.

## Bounded JSON exports

```sh
# Replace the placeholder with an actual account DID.
DID='did:plc:YOUR_ACCOUNT_DID'
curl --get 'https://labelwatch.neutral.zone/exports' \
  --data-urlencode "did=$DID" --data-urlencode 'days=7'

# Use the opaque cursor returned by creation; repeat with next_cursor.
CURSOR='COPY_CURSOR_FROM_RESPONSE'
curl --get 'https://labelwatch.neutral.zone/exports' \
  --data-urlencode "cursor=$CURSOR"
```

Creation returns a `manifest` and `cursor`. Pages contain `rows`, `manifest`,
`next_cursor` and `terminal`. Follow `next_cursor` until `terminal` is true.
Keep the manifest with all pages: it describes the interval, filters, coverage,
frontier and expiry. Rows contain `labeler_did`, `uri`, `value`, `action`,
`source_timestamp`, `observed_at` and a recorded public `sig` when available.
Signatures are exposed as recorded, not represented as verified.

The account HTML links `/download?cursor=…` for its own complete bounded
snapshot. **Account-download and paginated-export cursors are separate.** Do not
send an export cursor to `/download`. Cursors expire and can become unavailable
after a product-service restart; they are not durable identifiers.

| HTTP | Meaning and next action |
|---|---|
| 400 | Invalid/missing request fields; correct the request. |
| 409 | Explicit refusal, including an invalid interval/filter/cursor or an oversized export. Read `error` and any suggested intervals; never assume truncation succeeded. |
| 410 | Snapshot expired or unavailable; start a new export. |
| 503 | Store busy or snapshot unavailable; busy responses include `Retry-After`. Retry within the cursor deadline or begin again. |

JSON refusals include `error` and `complete: false`. Browser requests may receive
an HTML explanation. Large exports return suggested narrower intervals when
available. An interrupted download or a subset of pages is incomplete.

## Limits of interpretation

The fixed rolling window is 30 days. The current epoch does not reconstruct
previous history. Source-authored timestamps are not observation timestamps.
No observations does not mean no labels exist, and no recorded gaps does not
prove complete source coverage. Applications/removals do not establish current
label state. The API does not provide write operations, moderation decisions,
private profile data or an operator interface.
