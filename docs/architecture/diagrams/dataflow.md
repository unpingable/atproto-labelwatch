# labelwatch — Dataflow

```mermaid
flowchart LR
    EXT["External:<br/>ATProto<br/>Jetstream<br/>PLC"]

    DISC["1. Discovery<br/>(3 channels)"]
    ING["2. Ingest<br/>(poll queryLabels)"]
    SCAN["3. Scan<br/>(detection rules)"]
    DER["4. Derive<br/>(regime/risk/coherence)"]
    BND["Boundary<br/>(JSD edges)"]
    ROLL["5. Rollups<br/>(author_day,<br/>author_labeler_day)"]

    REP["6. Report<br/>(static)"]
    CLIM["7. Climate API<br/>(on-demand)"]

    EXT --> DISC
    EXT --> ING
    DISC --> ING
    ING --> SCAN
    ING --> DER
    SCAN --> DER
    DER --> BND
    DER --> ROLL
    SCAN --> REP
    DER --> REP
    BND --> REP
    ROLL --> CLIM
```

## Stage gates

Each stage has its own gates that can suppress signals from advancing:

| Stage | Gates |
|-------|-------|
| 1. Discovery | DB write failure → crash (no silent loss) |
| 2. Ingest | Cursor persistence + event_hash dedup as safety net |
| 3. Scan | Warmup gate; sparse gate (rate rules suppressed below volume threshold) |
| 4. Derive | Hysteresis (N consecutive passes for state change) |
| 5. Rollups | None (deterministic aggregation) |
| 6. Report | Cooldown filter; fight gate (≥2 shared targets) |
| 7. Climate API | Per-IP rate limit; concurrency semaphore; generation timeout; payload whitelist; kill switch |

See `../DATAFLOW.md` for stage detail and `../FAILURE_MODES.md` for what each gate prevents.
