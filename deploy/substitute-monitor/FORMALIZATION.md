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
