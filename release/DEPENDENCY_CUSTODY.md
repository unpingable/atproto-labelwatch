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
  --no-index --find-links WHEELHOUSE labelwatch==0.1.0rc7
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

## 2026-10-08: recent-observatory extra (`recent`)

The clean-start recent store and product need the `recent` extra in
`pyproject.toml`. Two pins were added to the lock; no other pin changed:

- `duckdb==1.4.3`, wheel
  `duckdb-1.4.3-cp312-cp312-manylinux_2_26_x86_64.manylinux_2_28_x86_64.whl`,
  sha256 `4f868a7e6d9b37274a1aa34849ea92aa964e9bd59a5237d6c17e8540533a1e4f`
- `pyarrow==22.0.0`, wheel
  `pyarrow-22.0.0-cp312-cp312-manylinux_2_28_x86_64.whl`,
  sha256 `c6c791b09c57ed76a18b03f2631753a4960eefbbca80f846da8baefc6491fcfe`

Verified on crow: both wheels fetched with `pip download --no-deps
--only-binary=:all:` for cp312/manylinux x86_64; the local sha256 equals the
digest PyPI publishes for that filename; every hashed file in the clean-start
test venv's installed RECORD matches the wheel's RECORD (duckdb 51, pyarrow
748, no mismatch); neither wheel declares a non-extra runtime dependency.

Not verified: these wheels are not yet in the reviewed campaign wheelhouse,
the offline install above has not been rerun with them, the hash is of the
PyPI artifact (not a rebuild from source), and glibc ≥ 2.28 on the target
host is assumed from the manylinux tag, not checked on the host. License
metadata (MIT for duckdb, Apache-2.0 for pyarrow, plus bundled native
libraries) still needs the same human review as the other wheels.
