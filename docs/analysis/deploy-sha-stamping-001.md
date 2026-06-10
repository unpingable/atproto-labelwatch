# Receipt stamping — pass deploy SHA into VM, 2026-06-10

> **Status: tiny hygiene issue, not blocking.** Filed during the
> frontdoor rendering slice.

## Symptom

The first live `labelwatch.index_audit.v1` receipt
(`docs/analysis/receipts/labelwatch.index_audit.whatsonme.frontdoor.v0.20260610T010254Z.json`)
has `git_commit: null`. The audit's `get_git_commit()` helper reads
`.git/HEAD` from the current working directory; on the VM there's no
`.git/` (per deploy doctrine "No git on VM"). So the field is null.

## Why it matters (mildly)

Receipts are supposed to be reproducible. "Audit ran against DB X
with code at commit Y" lets future-us re-run the audit, compare
verdicts, and decide whether a regression came from the DB shape
changing or the audit logic changing. With `git_commit: null` the
provenance is observation-time-only.

Not load-bearing for the current admissible verdict. Load-bearing if
a later receipt diverges and we want to know whether the audit logic
moved between runs.

## Fix shape (no code yet)

Two minimal patches:

1. **Deploy-side:** pass `LABELWATCH_GIT_SHA` as an env var to the
   rsync + restart commands. Set it locally before deploying:

   ```bash
   GIT_SHA="$(git rev-parse --short HEAD)" rsync … && \
     ssh … "LABELWATCH_GIT_SHA=$GIT_SHA systemctl restart labelwatch …"
   ```

   For the audit (one-shot, not a service), pass it via the SSH
   environment for that command. Already partially encoded in the
   driftwatch deploy command (see MEMORY.md).

2. **Code-side:** modify `utils.get_git_commit()` to prefer an
   environment variable (`LABELWATCH_GIT_SHA`) when set, falling back
   to `.git/HEAD` for local invocations.

   ```python
   def get_git_commit() -> Optional[str]:
       env_sha = os.environ.get("LABELWATCH_GIT_SHA")
       if env_sha:
           return env_sha.strip() or None
       # ... existing .git/HEAD logic ...
   ```

## Sequencing

After: real-subject load probe (`subject-lookup-load-probe-001.md`).
Before: any receipt comparison across deploy versions.

Strictly small. Don't let it intercept the main path. Filed so future-
us doesn't re-derive it.
