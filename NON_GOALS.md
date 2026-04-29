# Non-goals

- No truth adjudication.
- No content moderation.
- No “this labeler is bad” verdicts.
- No LLM-in-the-loop decisions.
- No inference about motives.
- No blocking or enforcement—only observables + receipts.
- No poster dossiers. No per-DID behavioral forecast, volatility score, risk class, or "discourse weather" read on an account. Weather is aggregate system state, not individual behavioral telemetry.

The forbidden shape is `GET /poster/{did}/weather` or any equivalent per-handle behavioral forecast surface. The existing `/v1/climate/{did}` is receiving-end accounting (what labelers did to a DID), not behavioral forecasting on the DID — that distinction is load-bearing.

If the tables can answer dossier-shaped questions, the API still must not.
