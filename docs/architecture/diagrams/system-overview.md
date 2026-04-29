# labelwatch — System overview

```mermaid
flowchart TD
    subgraph external["External"]
        AT["ATProto<br/>(queryLabels HTTP)"]
        JS["Jetstream"]
        PLC["PLC Directory"]
    end

    subgraph services["Three systemd services"]
        MAIN["labelwatch.service<br/>ingest · scan · derive · report"]
        DISC["labelwatch-discovery.service<br/>Jetstream sidecar"]
        API["labelwatch-api.service<br/>climate HTTP API"]
    end

    DB[("SQLite WAL<br/>schema v19")]

    subgraph public["Public surfaces"]
        REP["Static report<br/>(HTML + JSON)"]
        CLIM["/v1/climate/{did}<br/>(receiving-end)"]
        REG["/v1/registry"]
        HEALTH["/health"]
    end

    AT --> MAIN
    PLC --> MAIN
    JS --> DISC
    AT --> DISC

    MAIN <--> DB
    DISC --> DB
    DB --> API

    MAIN -->|atomic dir swap| REP
    API --> CLIM
    API --> REG
    API --> HEALTH
```

## Notes

- Three services share one SQLite database (WAL mode). Subsystem isolation: each subsystem in the main service is wrapped in try/except so one crash doesn't kill the others.
- Climate API binds loopback only; Caddy reverse proxy handles TLS and external access.
- See `../OVERVIEW.md` for invariants and component inventory.
