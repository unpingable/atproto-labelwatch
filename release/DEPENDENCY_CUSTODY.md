# Dependency custody and synthetic recovery

Target: Linux x86_64 with CPython 3.12 (`cp312`). The campaign wheelhouse
contains the Labelwatch wheel and the compatible binary wheels named in
`requirements-cp312-linux-x86_64.lock`. Every dependency is pinned with the
SHA-256 digest of the captured wheel.

From the repository root, after copying the reviewed wheelhouse to
`WHEELHOUSE`, the complete offline qualification is:

```sh
python3 -m venv /tmp/labelwatch-recovery-venv
/tmp/labelwatch-recovery-venv/bin/python -m pip install \
  --no-index --find-links WHEELHOUSE labelwatch==0.1.0rc6
/tmp/labelwatch-recovery-venv/bin/python scripts/demo_offline.py \
  --output /tmp/labelwatch-demo
/tmp/labelwatch-recovery-venv/bin/python scripts/recover_synthetic.py \
  --output /tmp/labelwatch-recovery
```

Both output directories must be new. The recovery command accepts no source
database path: it creates fixture state, performs an online SQLite backup,
restores it to a second database, invokes the installed status command, and
asserts cursor continuity, 37 events, and the expected flip-flop finding. Its
`result.json` is the machine-readable receipt. This is synthetic
application/data recovery, not source-worktree reconstruction, production
recovery, or a rehearsed rollback.

The wheelhouse proves dependency closure and network-independent installation
for the declared target. It does not prove that third-party wheels can be
rebuilt byte-for-byte from source. Package license metadata still requires
human review before redistribution.
