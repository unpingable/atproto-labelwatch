# Diagrams

Mermaid diagrams supplementing the architecture docs. GitHub renders `mermaid` code blocks in `.md` files inline.

## Files

| File | Purpose |
|------|---------|
| `system-overview.md` | External systems, three services, storage, public surfaces |
| `dataflow.md` | 7-stage pipeline with stage gates |
| `publication-boundary.md` | Decision tree: aggregate / per-DID receiving-end / per-DID behavioral-end |

## Editing

For interactive editing, paste the mermaid block into the [Mermaid Live Editor](https://mermaid.live).

For local rendering, use `mmdc` from `@mermaid-js/mermaid-cli`.

## Cross-reference

These diagrams are the visual companions to:
- `../OVERVIEW.md` (system-overview)
- `../DATAFLOW.md` (dataflow)
- `../PUBLIC_SURFACES.md` (publication-boundary)
