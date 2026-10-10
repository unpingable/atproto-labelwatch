# DRAFT / OFFPRODUCTION formalization consideration

Decision owner: Codex /root; independent acceptance remains pending. Base repository revision at copy: 078596c936a7c3f142ebbeaa00d331dadbc1d6ef

Source draft identities:
{
  "labelwatch-recent-watch.py": "255045af99b7780c0b269bcb552ee15f59612d9483db6c6c8a3b94c293bf69c9",
  "README.md": "09a580b6127ed9834185b49acea913d86fa24f14a29b98a604ecfedf604544a3",
  "labelwatch-recent-watch.service": "df7a6d7ac153b95851f3f3b11e61d2f29279af30402a15540bafe649f19a52e2",
  "labelwatch-recent-watch.timer": "c607974bab964e48165e7d93e2b700afb383ccadaf38328702aae5be322da098",
  "labelwatch-recent-control.service": "0904884b4211693e0f48b16e3acf3ae5e9696b0b0a7b4a67ffab13a230ca6b53",
  "labelwatch-recent-control.timer": "e88c57db346f098314dbf1821e4b6c27b41fa4a0ce695b0b11237085366826e7",
  "tests/test_watch.py": "8e9b59948dbdc5548eb3fe48a00f5c9cf9f0858e454ed52e01c7c34ac48b47ac"
}

Propositions: journal reason attributed only to exact valid maintenance InvocationID; window advancement requires prior generation baseline. Scope: existing sampled evaluator, fixtures and current draft; no production enrollment, native Monitor integration, exactly-once delivery or provider recovery claim. Practical command-match and cold-start/day-transition regression cases suffice for these bounded corrections. No new formal model or runtime. Result source identities:
{
  "README.md": "41d2d4ec23ba4ca3fdab6cecc0ef21ff219b48f36dcc5dc980d01f8bbcfda8c7",
  "labelwatch-recent-control.service": "0904884b4211693e0f48b16e3acf3ae5e9696b0b0a7b4a67ffab13a230ca6b53",
  "labelwatch-recent-control.timer": "e88c57db346f098314dbf1821e4b6c27b41fa4a0ce695b0b11237085366826e7",
  "labelwatch-recent-watch.py": "bf89d73508480d83931c2abce41e9dce8307a81624ff7a16c3f58a6281e5842d",
  "labelwatch-recent-watch.service": "df7a6d7ac153b95851f3f3b11e61d2f29279af30402a15540bafe649f19a52e2",
  "labelwatch-recent-watch.timer": "c607974bab964e48165e7d93e2b700afb383ccadaf38328702aae5be322da098",
  "tests/test_watch.py": "16f33a6760dd859b9c64b4cf00eef0f4c09f2749ee13ec2178b6a5bf826aa48e"
}

Qualification: 37 existing and regression fixture tests passed (exit 0) with existing locked venv and pytest, cache/bytecode disabled. Provider transport mocked; CLI refused-input uses only loopback port 9. Earlier sandbox run had 2 environment failures (EPERM instead of refusal); approved local run outside sandbox passed. No services installed or provider delivery. Regression cases bind current journal InvocationID and reject missing identity; cold-start baseline remains unknown until a later generation. Independent review pending.

Reviewer correction against commit 353ea6f: absent/malformed generation formerly became inferred zero. Current and prior generation must now be observed nonnegative integers (bool excluded); otherwise L4 unknown and no advancement claim. Two-day absence/malformed regressions plus invalid-current case qualify this same bounded baseline proposition. 44 fixture tests passed exit 0, mocked delivery and loopback refusal only. Independent rereview pending. Exact corrected source/test identities:
{
  "labelwatch-recent-watch.py": "e0d9b9e32855ba2f1d9b0ee63b48a75e3f432fe02e11b70998a2d57df1f8ad1f",
  "tests/test_watch.py": "9d7b403530766d495726c841b7ae0f38203632204f76f5982285aa71004f7b41"
}

## Authorized predictive-capacity and retention-health addendum

Decision owner Codex /root approved bounded design under user steering; base commit 20de9bc06d57fb322a279e19dea0d2bd8bba4f7b. Current fixed30-day contract and activation boundary preserved. Exact propositions: comparable positive net allocated-rate baseline gives a conditional linear30-day footprint estimate; insufficient/nonpositive/reset baseline remains unknown; actual admitted-store/WAL/reserve exhaustion has a distinct condition; journal reasons bind exact current unit occurrence; scheduled deadlines preceding acquisition.started_at are not missed occurrences. Tests cover estimate outside lookahead, urgent crossing, store/WAL/reserve/inode exhaustion, cold/short/nonpositive/mismatched/stale baseline, roster/counter/generation reset, persisted observations/intents, overwritten round, collector-vs-maintenance invocation and fresh enrollment.

Assumptions/unknowns: stat inventory is bounded sampled metadata under concurrent writes, not coherent sealed allocation; file and directory inode blocks included, hardlinks counted once within inventory. It cannot establish ownership outside configured store or gross bytes/ingested row. Historicalbaseline resets after maintenance or acquisition/roster/policy changes. Accepted-row velocity requires full matching enrolled roster. Forecast repeats observed recent net rate without crediting future unobserved retirement; shared-filesystem depletion includes other tenants. A forecast is not measured full-window capacity or acceptance of30-day service; no adaptive retention or autoactuation. Budgets unset until explicitly admitted. Replica2GiBstore/256MiBaggregateWAL scoped values; production policy remains owner action. Existing original replica NC producer/artifacts not modified or credited to new observation.

Formalization decision: practical bounded deterministic transition fixtures and explicit arithmetic assumptions suffice for this observational change; no stronger storage-growth bound or provider/recovery theorem is claimed. A future model would need actual workload/retirement correspondence and concurrent-filesystem assumptions, separately scoped. Qualification:70fixture tests passed in0.93s, exit0, mocked provider transport and local loopback refusal fixtures. Independent review pending. Exact source/test/document result identities:
{
  "labelwatch-recent-watch.py": "ffc06bf8b31d8613a49bf058bb6eabd238d15b872567d01e313e3a5026f7c93c",
  "tests/test_watch.py": "72ec00f81951067f68a4f9895089c3dacc8741ceb1ceaff0fa95542f43d76740",
  "README.md": "66760d44746268a28ee3de01d36c74ef8ecfb8bbb7985777e24fe36b7a934596"
}

Independent counterexamples against598aa0e corrected: empty/uncovered reserve observations, invalid store/mount identities, individually decreasing source counters masked by aggregate growth, and future-generated STATUS cannot yield an ok forecast. Filesystem reserve crossing retains its known urgency under nonpositive store growth, with store component explicitly unknown. Visible namespace allocation excludes open-unlinked scratch/outside-root allocation; qualified peak and admission headroom remain separate.86fixture tests passed exit0; exact follow-up result identities:
{
  "labelwatch-recent-watch.py": "cdf6142ae70181f57b74c94c180b2969f9a789345d363243cd2c715a11f0d21f",
  "tests/test_watch.py": "bce8df478ad1ac9bd980724b0603e0cea2f87e89f6cf8b0654968a5dfb4c7fb7",
  "README.md": "df8c55759b05c929352308312dfef958395da81a76dd5f9375d83eb123b7a85f"
}
