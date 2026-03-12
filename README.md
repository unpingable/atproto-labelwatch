# labelwatch

An observatory for ATProto's labeling infrastructure. Monitors labeler behavior
over time and flags integrity-risk patterns (rate spikes, drift, synchronized
activity, boundary instability). It does not judge content or truth; it produces
inspectable receipts about governance infrastructure behavior.

## What it does

**Discovers labelers** via batch enumeration (`listReposByCollection`), a
Jetstream sidecar that watches `app.bsky.labeler.service` records in real time,
and a backstop scrape of curated labeler lists. All three channels feed into a
single registry with evidence-based classification.

**Ingests label events** from `com.atproto.label.queryLabels` across all
discovered labelers. Events are normalized, hashed (SHA-256), and stored in
SQLite. Multi-ingest handles labelers that run their own endpoints.

**Detects anomalies** with four rules (rate spike, flip-flop, target
concentration, churn index), all with warm-up gating to suppress false positives
during labeler startup. Alerts include receipt hashes for auditability.

**Derives labeler state** with four independent signals: regime state
(warming_up / stable / bursty / degraded / ...), auditability risk (0-100),
inference risk (0-100), and temporal coherence (0-100). Four dials, not one
trust score.

**Analyzes boundary instability** between labelers: label family normalization,
JSD divergence, contradiction edges, shared-target overlap. Domain
classification (moderation / metadata / novelty / political) filters real
conflict from badge-ecosystem orthogonality.

**Generates reports** as static HTML + JSON sites: census, triage views
(Active/Alerts/New/Opaque/All), per-labeler pages with evidence expanders,
volume badges, discovery health cards, and boundary analysis.

**Serves label climate** via HTTP: per-DID reporting showing which labelers
apply what labels, daily time series, top values, and example posts. Rate
limited, disk cached, concurrency gated.

## Quick start

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .

# Configure
cp config/config.toml.example config.toml
# Edit config.toml with your settings

# One-shot commands
labelwatch ingest --config config.toml
labelwatch scan --config config.toml
labelwatch report --format html --out report/ --now max

# Continuous operation
labelwatch run --config config.toml --db labelwatch.db
```

## Architecture

Three systemd services, one SQLite database (WAL mode):

```
                                 ┌──────────────────────┐
                                 │  Jetstream            │
                                 │  (labeler.service     │
                                 │   records)            │
                                 └──────────┬───────────┘
                                            │
┌──────────────────┐            ┌───────────▼───────────┐
│  ATProto Service │            │  Discovery Stream     │
│  (queryLabels)   │            │  (discovery_stream.py) │
└────────┬─────────┘            │  + backstop scrape    │
         │                      └───────────┬───────────┘
         │ HTTP polling                     │
         ▼                                  ▼
┌──────────────────┐    ┌───────────────────────────────┐
│  Ingest          │───▶│  SQLite DB (schema v19, WAL)  │
│  (ingest.py)     │    │                               │
│  multi-ingest    │    │  label_events   labelers      │
└──────────────────┘    │  alerts         evidence      │
                        │  discovery_events              │
┌──────────────────┐    │  boundary_edges/targets        │
│  Rules + Scan    │───▶│  derived_author_day            │
│  (rules.py,      │    │  derived_author_labeler_day   │
│   scan.py)       │    └───────────────┬───────────────┘
│  receipted alerts│                    │
└──────────────────┘                    │
                                        ▼
┌──────────────────┐    ┌───────────────────────────────┐
│  Derive          │    │  Report        │  Climate API │
│  (derive.py)     │    │  (report.py)   │  (server.py) │
│  regime state    │    │  HTML + JSON   │  /v1/climate │
│  risk scores     │    │  static site   │  rate limited│
│  coherence       │    └────────────────┴──────────────┘
└──────────────────┘
```

### Services

| Service | Purpose | Resources |
|---------|---------|-----------|
| `labelwatch.service` | Main loop: ingest, scan, derive, report | 2GB / 50% CPU |
| `labelwatch-discovery.service` | Jetstream sidecar for real-time labeler discovery | 256MB / 10% CPU |
| `labelwatch-api.service` | HTTP API: climate, whatsonme (`/v1/*`) | 512MB / 25% CPU |

## CLI

```bash
# Ingestion & scanning
labelwatch ingest --config config.toml       # Fetch label events
labelwatch scan --config config.toml         # Run detection rules
labelwatch run --config config.toml          # Continuous loop (all of the above)

# Discovery
labelwatch discover --config config.toml     # Batch labeler discovery
labelwatch discover --backstop              # Scrape labeler-lists.bsky.social
labelwatch discover-stream                   # Jetstream sidecar (runs continuously)

# Reporting
labelwatch report --format html --out report/    # Static HTML site
labelwatch report --alerts --since 24h           # Recent alerts
labelwatch report --labeler did:plc:...          # Single labeler

# Climate & account labels
labelwatch climate --did did:plc:...         # Generate climate report (CLI)
labelwatch whatsonme did:plc:...             # Account labels via queryLabels
labelwatch whatsonme @alice.bsky.social      # Also accepts @handles
labelwatch serve --port 8423                 # Start HTTP server

# Inspection
labelwatch labelers                          # List discovered labelers
labelwatch labelers --class declared         # Filter by visibility class
labelwatch census                            # Classification census
labelwatch coverage-delta                    # Upstream vs registry comparison
labelwatch reclassify --dry-run              # Preview reclassification

# Maintenance
labelwatch db-optimize                       # Run ANALYZE + query planner
```

## API

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Health check |
| `GET /v1/climate/{did_or_handle}` | Label climate report (local ingest data) |
| `GET /v1/whatsonme/{did_or_handle}` | Account-level labels via network queryLabels |

Both `/v1/climate/` and `/v1/whatsonme/` accept DIDs or `@handle`s. Query params:
`format=json|html`, `window=N` (climate only), `sources=did1,did2` (whatsonme only).

Rate limited, disk cached (climate), concurrency gated. Kill switch via
`CLIMATE_API_DISABLED=1`.

## Configuration

Create a `config.toml` (see `config/config.toml.example`):

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

discovery_enabled = true
discovery_interval_hours = 24
boundary_enabled = true
```

## Detection rules

| Rule | What it detects |
|------|----------------|
| `label_rate_spike` | Label rate exceeds baseline by spike_k (default 10x) |
| `flip_flop` | Apply → negate → re-apply on same (uri, val) within window |
| `target_concentration` | HHI on target distribution indicates fixation on few targets |
| `churn_index` | Jaccard distance of target sets across adjacent windows |

All rules include warm-up gating and collect evidence hashes for auditability.

## Labeler classification

Three-axis classification from structured evidence:

- **Visibility**: declared / protocol_public / observed_only / unresolved
- **Reachability**: accessible / auth_required / down / unknown
- **Auditability**: high / medium / low

Sticky evidence fields (observed_as_src, has_labeler_service, etc.) are never
downgraded by transient probe failures.

## Schema

SQLite with WAL mode. Current version: v19. Key tables:

| Table | Purpose |
|-------|---------|
| `label_events` | Append-only ingested labels (SHA-256 deduped) |
| `labelers` | Registry with classification, regime state, risk scores, volume stats |
| `alerts` | Detection results with receipt hashes |
| `labeler_evidence` | Append-only classification evidence |
| `discovery_events` | Jetstream/batch/backstop discovery audit trail |
| `boundary_edges` | Cross-labeler contradiction/divergence edges |
| `derived_author_day` | Rollup: label counts per author per day |
| `derived_author_labeler_day` | Rollup: label counts per author/labeler/day |

## Related projects

- [driftwatch](https://github.com/unpingable/atproto-driftwatch) — reference
  ATProto labeler with drift detection, longitudinal tracking, and a decision
  ledger. Labelwatch watches labeler behavior; driftwatch watches information
  drift. Same observatory family.

## Design constraints

- Aggregate-first, NOT profile-first
- Observation only — does not moderate content, judge truth, or emit labels
- No ML classifiers, no LLM-in-the-loop
- Receipt hashing for auditability (SHA-256, not cryptographic signing)
- Four independent risk dials, not one collapsed trust score

## License

Unless otherwise noted, this repository is licensed under MIT OR Apache-2.0,
at your option. Contributions are accepted under the same terms.
