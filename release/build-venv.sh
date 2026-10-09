#!/usr/bin/env bash
# Build <release_dir>/venv from exactly the hash-locked set, offline.
#
#   WHEELHOUSE=<dir> [PYTHON=/usr/bin/python3] release/build-venv.sh <release_dir>
#
# <release_dir>/source must already hold the release tree (git archive of the
# release sha). The lock is selected by the interpreter's ABI tag: CPython
# 3.12 uses requirements-cp312-linux-x86_64.lock, CPython 3.10 (the
# production host, Ubuntu 22.04) uses requirements-cp310-linux-x86_64.lock.
# Any other interpreter is refused because no reviewed lock exists for it.
#
# Labelwatch itself is NOT installed into the venv: units run it
# from source via PYTHONPATH=<release_dir>/source/src:<release_dir>/source, as
# deploy/README-recent.md documents. The script finishes by running the
# collect unit's ExecStartPre dependency gate against the new venv.
set -euo pipefail
release=${1:?usage: WHEELHOUSE=<dir> build-venv.sh <release_dir>}
wheelhouse=${WHEELHOUSE:?WHEELHOUSE must name the reviewed wheelhouse}
python=${PYTHON:-/usr/bin/python3}
source_dir=$release/source
unit=$source_dir/deploy/labelwatch-recent-collect.service

[ -d "$source_dir" ] || { echo "missing $source_dir" >&2; exit 2; }
abi=$("$python" -c 'import sys, platform; sys.exit("CPython required") if platform.python_implementation() != "CPython" else print("cp%d%d" % sys.version_info[:2])')
case "$abi" in cp310|cp312) ;; *) echo "no reviewed lock for $abi ($python)" >&2; exit 2;; esac
lock=$source_dir/release/requirements-$abi-linux-x86_64.lock
[ -f "$lock" ] && [ -f "$unit" ] || { echo "release tree lacks lock or unit" >&2; exit 2; }
[ ! -e "$release/venv" ] || { echo "$release/venv exists; refusing to modify" >&2; exit 2; }
echo "lock $(basename "$lock")"

"$python" -m venv "$release/venv"
venv_py=$release/venv/bin/python
"$venv_py" -m pip install --quiet --no-index --require-hashes --no-deps \
  --find-links "$wheelhouse" -r "$lock"
"$venv_py" -m pip check

# Exactly the lock: same names and versions, nothing more (pip itself aside).
"$venv_py" - "$lock" <<'PY'
import importlib.metadata as m, re, sys
norm = lambda n: re.sub(r'[-_.]+', '-', n).lower()
want = {norm(l.split('==')[0]): l.split('==')[1].split()[0] for l in open(sys.argv[1]) if l.strip()}
have = {norm(d.metadata['Name']): d.version for d in m.distributions() if norm(d.metadata['Name']) not in ('pip', 'setuptools')}
if have != want:
    sys.exit('venv differs from lock: missing/changed %s extra %s' % (
        sorted(set(want.items()) - set(have.items())), sorted(set(have.items()) - set(want.items()))))
print('lock_exact', len(want))
PY

PYTHONPATH=$source_dir/src:$source_dir "$venv_py" -c 'import labelwatch, labelwatch.recent_collector, duckdb, pyarrow; print("source_import_ok")'

gate=$(sed -n 's/^ExecStartPre=[^ ]* -c "\(.*\)"$/\1/p' "$unit")
[ -n "$gate" ] || { echo "no ExecStartPre gate in $unit" >&2; exit 2; }
"$venv_py" -c "$gate"
echo dependency_gate_ok
