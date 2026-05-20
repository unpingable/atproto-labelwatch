# Non-Sovereign Perimeter Doctrine — candidate

**Status:** doctrine + vocabulary candidate, parked. Filed 2026-05-15.

**This is not authorization to build.** It is a doctrine stub and a parked vocabulary list, kept here so future Labelwatch extensions in this neighborhood have a handle for review instead of being reinvented from scratch.

## Origin

Extracted from a bad-idea review of "Perimeter View" / "CoSo-style hardened perimeter" framings. The verdict on the framing itself: **do not build Perimeter View as a unified atproto product.** The useful isotope that survived quarantine is the doctrine below.

The doctrine generalizes a discipline that Labelwatch already practices implicitly (aggregate-first, observation-only, no verdicts) into a portable rule for any system that interprets-and-presents the moderation layer to downstream users.

## Core doctrine

> The lens advises. The user/community chooses. The custodian leaves receipts.

Three roles, never collapsed:

- **Lens** — interprets signal, surfaces context. Advisory. Replaceable.
- **User/community** — decides what to do with the advice. Holds final authority over their own surface.
- **Custodian** — operates the lens. Bound to leave receipts of what was emitted, when, and why; bound by its own published expiry/refusal rules.

The doctrine is enforced by what the system *cannot* do, not by what it promises.

## Five doctrine pieces

### 1. Receipted interpretation

Emission receipt ≠ interpretation receipt.

- "Labeler X emitted label Y for target Z at time T" is an emission receipt.
- "Downstream surface S interpreted Y as warning/filter/feed-treatment/risk-context" is an interpretation receipt.

A lens that presents moderation state must record how *it* rendered the upstream signal, not just what the signal was. Otherwise the gap between what labelers do and what users see is unmeasurable, and the lens can drift silently.

### 2. Pressure-shape vocabulary, not moral-verdict vocabulary

Candidate vocabulary for context signals — **parked, not adopted**:

| Candidate term | Shape |
|---|---|
| `possible-automation` | behavior-level, hedged |
| `handle-churn-risk` | observable, time-bounded |
| `coordinated-reply-burst` | aggregate behavior, episodic |
| `new-account-high-volume` | structural, no judgment |
| `impersonation-proximity` | relational, descriptive |

Vocabulary to **avoid** if Labelwatch ever publishes context signals:

- `bad-actor` — identity-level moral verdict
- `troll` — character claim, not behavior
- `verified-good` — permanent positive label, equally invasive

The rule: terms describe *pressure observed on the surface*, not *what kind of person produced it*.

### 3. Advisory + time-limited by default

Any risk/context signal:

- expires by default unless explicitly renewed against fresh evidence
- never binds downstream action — clients/communities opt in to act
- never persists as a "scarlet letter" — a person's signal yesterday is not a person's signal today

Compare and contrast with the labeler ecosystem as it stands: many labels are de-facto permanent because there's no expiry contract. Any Labelwatch context-surface would have to fix this in its own emissions, not inherit the upstream defect.

### 4. Subscribe-and-swap posture

The lens must be replaceable. Concretely:

- the surface exposes which lens it's using
- the lens is swappable for another lens at the user/community level
- the lens publishes inspectable refusal rules (what it won't testify about, and why)
- no architectural lock-in to "the hardened perimeter"

If switching lenses is hard, the lens has become a sovereign by accident, and the doctrine has failed.

### 5. Labelwatch fit

This is not "Labelwatch should run a labeler." It is a possible future *lane* for Labelwatch: watching custodians and lenses — exposing interpretation drift, expiration behavior, pressure-signal vocabulary, and cannot-testify boundaries.

Useful as **doctrine and vocabulary first, not product.** Implementation only when an existing Labelwatch problem forces it.

## Review gate (Bad Idea Review Board)

Before any concrete Labelwatch extension that touches this neighborhood lands, the proposal must pass all five:

1. **Is this actually different from running a labeler?** (If no → it's a labeler, build it as one or don't.)
2. **Can it stay a lens, not a sovereign?** (If no → CoSo-in-a-trenchcoat. Don't build.)
3. **Does it create value without becoming CoSo-in-a-trenchcoat?** (If no → see #2.)
4. **Can it be useful for a polity of one?** (If no → it's a centralization play, not a tool.)
5. **Does it dogfood Labelwatch / NQ / Wicket discipline: receipts, expiry, advisory state, cannot-testify boundaries, inspectable refusal?** (If no → it's exempt from the rules it claims to enforce.)

**A negative on any one means do not build.**

## Smallest sane next action

Park this note. Park the candidate vocabulary. Do not implement.

If a future Labelwatch problem makes this neighborhood load-bearing (e.g., users start asking what their feed treatment means and we can't answer because no interpretation receipt exists), this doc becomes the starting point for the review gate.

## Composes with

- Labelwatch architecture rules (observation-only, aggregate-first, no verdicts) — same family
- Co-presence is not corroboration (workspace doctrine) — interpretation receipts must not be treated as evidence of the underlying emission
- Operationally Accountable Interface Surfaces (`~/git/gnat/good_actually/operationally-accountable-interface-surfaces.md`) — any lens UI is bound by the five-question pixel challenge; an interpretation receipt is one of the five legitimate things a pixel can do
- Tolerability horizon doctrine — advisory-by-default + expiry-by-default are the implementation shape of "sometimes bad is tolerable"
