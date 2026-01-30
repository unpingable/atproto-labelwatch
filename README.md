# labelwatch

This project monitors labeler behavior over time and flags integrity-risk patterns (rate spikes, drift, synchronized activity). It does not judge content or truth; it produces inspectable receipts about governance infrastructure behavior.

## Quick start

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
labelwatch ingest --config config.toml
labelwatch scan --config config.toml
labelwatch report --alerts --since 24h
labelwatch report --format html --out report --now max
```

## What it does (MVP)

- Ingests label events from `com.atproto.label.queryLabels` for configured labeler DIDs.
- Stores normalized label events and labeler profiles in SQLite.
- Scans for conservative behavior patterns and writes receipts to `alerts`.

## What it does not do

See `NON_GOALS.md`.

## Config

Create a `config.toml` like:

```toml
db_path = "labelwatch.db"
service_url = "https://bsky.social"
labeler_dids = ["did:plc:example1", "did:plc:example2"]

window_minutes = 15
baseline_hours = 24
spike_k = 10.0
min_current_count = 50
flip_flop_window_hours = 24
max_events_per_scan = 200000
```

## CLI

- `labelwatch ingest`
- `labelwatch scan`
- `labelwatch report --labeler DID`
- `labelwatch report --alerts --since 24h`
- `labelwatch export --format json`
- `labelwatch report --format html --out report/ --now max`
- `labelwatch run --config config.toml --db labelwatch.db --ingest-interval 120 --scan-interval 300 --report-out report/`

## Static reports

Generate a static HTML + JSON bundle:

```bash
labelwatch report --format html --out report --now max
```

Open `report/index.html` in a browser or host the `report/` directory anywhere. Reports include build signatures plus clock-skew and timestamp-assumption diagnostics for traceability.

## Docker

Copy and edit the example config:

```bash
cp config/config.toml.example config/config.toml
```

Then run:

```bash
docker compose up --build
```
