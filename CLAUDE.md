# CLAUDE.md

labelwatch is an ATProto label behavior monitor. It polls `com.atproto.label.queryLabels` for configured labeler DIDs, stores normalized label events in SQLite, runs detection rules (rate spikes, flip-flop patterns), and produces auditable alerts with SHA-256 receipt hashes. It observes labeler behavior — it does not moderate content, judge truth, or emit labels.

## Architecture rules

- **Observation only** — no content moderation, no truth adjudication, no "bad labeler" verdicts.
- **Append-only events** — `label_events` rows are never updated or deleted.
- **Receipt hashing** — every alert includes a SHA-256 receipt hash over (rule_id, labeler_did, ts, inputs, evidence_hashes, config_hash) for audit trail integrity.

## File structure

```
src/labelwatch/
  config.py     — Config dataclass, TOML loader
  db.py         — SQLite schema, connect, init, migrations, CRUD
  ingest.py     — HTTP polling via queryLabels, event normalization
  rules.py      — Detection rules: label_rate_spike, flip_flop
  scan.py       — Orchestrator: runs rules, computes receipts, writes alerts
  report.py     — Static HTML + JSON report generator
  receipts.py   — config_hash and receipt_hash helpers
  runner.py     — Continuous ingest/scan loop for docker
  cli.py        — argparse CLI: ingest, scan, report, export, run
  utils.py      — Timestamps, hashing, git commit detection
tests/
  test_ingest.py, test_rules.py, test_receipts.py, test_report.py
```

## Commands

```bash
pytest tests/ -v          # run all tests
labelwatch --help         # CLI usage
labelwatch ingest --config config.toml
labelwatch scan --config config.toml
labelwatch report --format html --out report --now max
```

## Governed development

This project uses agent_gov for governed development. Governor state lives in `.governor/` (git-ignored).
