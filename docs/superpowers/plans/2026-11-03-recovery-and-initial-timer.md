# Recovery and initial timer implementation plan

> **For agentic workers:** Execute inline with superpowers:executing-plans; root owns independent whole-branch review.

**Goal:** Refuse missing-state writers without obstructing init and avoid premature initial timer notices.

**Architecture:** Preserve the store lock and use explicit create authority for state. Use existing L2 facts, obligations and transition flow with a fixed initial deadline.

**Tech Stack:** Python, SQLite, fcntl, systemd facts, pytest.

**Spec:** `docs/superpowers/specs/2026-11-03-recovery-and-initial-timer-design.md`

## Global constraints

No production effects. Preserve original evidence and 600-second discovery policy.
Private test dependencies and artifacts remain below 512 MiB; base environment is read-only.

## Review focus

State disappears between lock admission and SQLite open: no implicit state creation.
An existing lock must continue fencing concurrent writers.
A restarted timer must not resolve an already open incident.
A moving future due must not postpone the initial deadline indefinitely.
Malformed, absent or future timestamps must not imply observed scheduling success.

### Task 1: Missing-state writers

Files: `tools/segmented_qualification/storage.py`, `tests/test_recent_missing_state.py`, `deploy/RUNBOOK-recent.md`.

- [ ] Reproduce empty-store refusal through real Collector and direct writer/open; assert unchanged empty root and same-root init succeeds. Add state-disappearance and writer-lock fencing controls.
- [ ] Run new tests and retain expected failures.
- [ ] Require existing state for every store lock helper call and make state opens noncreating; Store.create explicitly opts in.
- [ ] Run new tests plus recent init/storage/acquisition/total-loss and segmented consumers. Update operator guidance and commit.

### Task 2: Initial collection timer

Files: existing watcher, `deploy/substitute-monitor/tests/test_initial_timer.py`, watcher README.

- [ ] Add real evaluate/transition tests for initial wait, due/deadline boundaries, moving due, unknown inputs, inactive timer, restart with open incident and real-trigger recovery; exercise gather's exact systemctl property request.
- [ ] Run tests and retain expected failures.
- [ ] Read active-entry and valid next due, skip initial wait and notify after 600 seconds from active entry. Update obligation text/digest and docs.
- [ ] Run unchanged full watcher suite with loopback permission and commit.

### Task 3: Discovery bound and review handoff

Files: `deploy/RUNBOOK-recent.md`, discovery regression tests.

- [ ] Reproduce the seven-page two-minute scan through real store/publisher, retaining current=false after 600 seconds until completion.
- [ ] Document observed 14-minute limitation without policy/cadence change.
- [ ] Run complete recent/segmented/watcher checks, inspect diff, record immutable lane completion with hashes and custody, and hand branch/commit to root for independent review.
