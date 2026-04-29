# labelwatch — Publication boundary

The decision tree for whether a proposed surface may be published.

```mermaid
flowchart TD
    Q["New published surface?"]
    Q --> Q1{"Aggregate or<br/>per-DID?"}
    Q1 -->|Aggregate| OK1["✓ Permitted (default)<br/>e.g., census,<br/>boundary edges,<br/>label distribution"]
    Q1 -->|Per-DID| Q2{"Receiving-end<br/>or behavioral-end?"}
    Q2 -->|Receiving-end<br/>(what was done<br/>TO the DID)| OK2["✓ Permitted with gates<br/>rate limit · cache ·<br/>kill switch · payload whitelist<br/><br/>Example:<br/>/v1/climate/{did}"]
    Q2 -->|Behavioral-end<br/>(what the DID<br/>did or will do)| FORBID["✗ FORBIDDEN<br/>This is dossier production"]

    FORBID --> FORBA["Forbidden shapes:<br/>GET /poster/{did}/weather<br/>GET /poster/{did}/volatility<br/>GET /poster/{did}/risk_class<br/><br/>Rule:<br/>if the tables can answer it,<br/>the API still must not."]

    classDef ok fill:#d4edda,stroke:#155724,color:#155724
    classDef no fill:#f8d7da,stroke:#721c24,color:#721c24
    classDef detail fill:#e2e3e5,stroke:#383d41,color:#383d41

    class OK1,OK2 ok
    class FORBID no
    class FORBA detail
```

## The load-bearing rule

> **If the tables can answer dossier-shaped questions, the API still must not.**

Storage is permissive — derive passes, rollups, internal joins all need access to per-DID rows. The publishable contract is *not* "everything storage can compute." See `../PUBLIC_SURFACES.md` for the full doctrine.

## What the climate API is and isn't

The climate API (`/v1/climate/{did_or_handle}`) is **receiving-end accounting**:

- ✓ "what labelers labeled this DID" (label history)
- ✓ "which labelers are most active on this DID"
- ✗ "what this DID is likely to post next"
- ✗ "this DID's posting risk class"
- ✗ "this DID's discourse-weather forecast"

The first two describe public observable actions taken by *labelers*, indexed by their *target*. The last three would describe inferred properties of the *target*. Different surfaces, even though both are keyed on `{did}`.
