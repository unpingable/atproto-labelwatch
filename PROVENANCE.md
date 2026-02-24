# Provenance

This project is human-directed and AI-assisted. Final design authority,
acceptance criteria, and editorial control rest with the human author.
AI contributions were material and are categorized below by function.

## Human authorship

The author defined the project direction, requirements, and design intent —
including the observation-only posture, receipt-based auditability model,
evidence-based classification taxonomy, and deployment decisions. AI systems
contributed proposals, drafts, implementation, and critique under author
supervision; they did not independently determine project goals or
deployment decisions. The author reviewed, revised, or rejected AI-generated
output throughout development.

## AI-assisted collaboration

### Architectural design

Lead collaboration: ChatGPT (OpenAI). Heavy involvement in the evidence-based
classification model, visibility taxonomy (declared / protocol_public /
observed_only / unresolved), auditability framework, warm-up gating design,
and the multi-surface evidence approach. Architectural requirements and
design constraints produced in ChatGPT sessions were then distilled into
the implementation plan that guided coding.

### Implementation, tests, and integration

Lead collaboration: Claude (Anthropic) via Claude Code. The multi-phase
implementation plan was assembled from architectural decisions made in
ChatGPT sessions and executed across six phases: schema v4, pure classifier,
discovery/ingest wiring, warm-up gating, report overhaul, and CLI/docs.
Heavy contributions to source code, test suites, CLI wiring, module
integration, schema migrations, and report generation.

## Development context

This project was not developed under governor-in-the-loop governance
(unlike its sibling project agent_gov). Development used Claude Code
directly with human review at each phase boundary.

## Provenance basis and limits

This document is a functional attribution record based on commit history,
co-author trailers (where present), and working sessions. It is not a
complete forensic account of all contributions.

Some AI contributions (especially design critique, rejected alternatives,
and footguns avoided) may not appear in repository artifacts or commit
metadata.

Model names/tools are recorded at the platform level (e.g., ChatGPT,
Claude Code); exact model versions may vary across sessions and are not
exhaustively reconstructed here.

## What this document does not claim

- No exact proportional attribution. Contributions are categorized by
  function, not quantified by token count or lines of code.
- Design and implementation were not cleanly sequential. Architecture
  informed code, code revealed design gaps, and the feedback loop was
  continuous.
- "Footguns avoided" and "ideas that didn't ship" are real contributions
  that leave no artifact. This document cannot fully account for them.

---

This document reflects the project state as of 2026-02-24 and may be revised.
