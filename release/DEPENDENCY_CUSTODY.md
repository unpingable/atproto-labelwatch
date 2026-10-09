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

## 2026-10-08: clean-start wheelhouse and locked venv

Wheelhouse: `labelwatch-segmented-horizon-capacity-20261005/runtime/wheelhouse-clean-start-20261008/`
(23 wheels, one per lock line). 21 are copied byte-for-byte from the reviewed
wheelhouse `2026-09-07-overnight-completion/lanes/dependency-recovery/labelwatch/wheelhouse/`
(its `labelwatch-0.1.0rc2` wheel is excluded); the 0bada713 packaging run
(`runtime-packaging-20261006/caecf073-*/WHEELS.json`) drew its seven HTTP
wheels from that same directory. duckdb and pyarrow were downloaded once from
PyPI (see above). Every file's sha256 was checked against its lock line before
use, and no file is unpinned.

`release/build-venv.sh` builds a venv from exactly this lock (`--no-index
--require-hashes --no-deps --find-links`), checks the venv equals the lock,
and runs the collect unit's dependency gate. Built at
`runtime/clean-start-venv-locked/venv` with /usr/bin/python3.12 (3.12.3): gate
passed with no pin change. The earlier `clean-start-venv` used for lane tests
was not the lock (e.g. idna 3.20); test tools (pytest 9.1.1, pytest-asyncio
1.4.0, pluggy, iniconfig, packaging, Pygments) are kept outside the locked
venv in `clean-start-venv-locked/test-tools` and added only via PYTHONPATH
for tests.

## 2026-10-09: cp310 lock for the production host

The production host (zone, Ubuntu 22.04.5) runs CPython 3.10.12 with glibc
2.35 and has no 3.12 interpreter; the cp312 lock cannot be installed there.
`requirements-cp310-linux-x86_64.lock` pins the same 23 distributions at the
same versions with one exception, `websockets==16.1.1` instead of `17.1`
(websockets 17.x requires Python >= 3.11), plus one addition,
`exceptiongroup==1.3.1`, which anyio 4.15.1 requires on Python < 3.11 (the
first replica build failed `pip check` without it). The recent observatory
(`src/labelwatch/recent_*.py`, `tools/segmented_qualification/recent_*.py`,
`storage.py`) imports neither websockets nor atproto; websockets is a
dependency of the legacy discovery stream only. The collect unit's
`ExecStartPre` dependency gate names the nine distributions the successor
actually imports, and those pins are identical in both locks.

Wheelhouse: `runtime/wheelhouse-clean-start-cp310-20261009/` (24 wheels;
cp310, cp39-abi3 or pure-Python). Each wheel was fetched once from PyPI with
`pip download --no-deps --only-binary=:all: --python-version 3.10`, its local
sha256 compared with the digest PyPI publishes for that filename (24/24
equal; receipt `clean-start-20261008/evidence/cp310-lock-*.json`), and the
lock line carries that digest. Native wheels are manylinux_2_17/2_28 x86_64,
within glibc 2.35. `release/build-venv.sh` selects the lock from the
interpreter's ABI tag (cp310 or cp312) and refuses any other interpreter.
