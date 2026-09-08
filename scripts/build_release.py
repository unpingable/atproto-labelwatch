#!/usr/bin/env python3
"""Build a Python release wheel twice from one committed Git ref."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def run(*argv: str, cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    return subprocess.run(argv, cwd=cwd, env=env, check=True, text=True,
                          stdout=subprocess.PIPE).stdout.strip()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    commit = run("git", "rev-parse", f"{args.ref}^{{commit}}", cwd=repo)
    tree = run("git", "rev-parse", f"{commit}^{{tree}}", cwd=repo)
    epoch = run("git", "show", "-s", "--format=%ct", commit, cwd=repo)
    archive = subprocess.run(
        ["git", "archive", "--format=tar", commit], cwd=repo, check=True,
        stdout=subprocess.PIPE,
    ).stdout
    output = args.output.resolve()
    runtime = output / "runtime"
    evidence = output / "evidence"
    runtime.mkdir(parents=True, exist_ok=True)
    evidence.mkdir(parents=True, exist_ok=True)
    builds: list[dict[str, str]] = []
    with tempfile.TemporaryDirectory(prefix="atproto-release-") as owner:
        root = Path(owner)
        for number in (1, 2):
            source = root / f"source-{number}"
            wheel_dir = root / f"wheel-{number}"
            source.mkdir()
            wheel_dir.mkdir()
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as bundle:
                bundle.extractall(source, filter="data")
            env = os.environ.copy()
            env.update({"SOURCE_DATE_EPOCH": epoch, "PYTHONHASHSEED": "0"})
            run(sys.executable, "-m", "pip", "wheel", "--no-deps",
                "--no-build-isolation", "--wheel-dir", str(wheel_dir), ".",
                cwd=source, env=env)
            wheels = sorted(wheel_dir.glob("*.whl"))
            if len(wheels) != 1:
                raise SystemExit(f"build {number}: expected one wheel, found {len(wheels)}")
            builds.append({"name": wheels[0].name, "sha256": digest(wheels[0])})
            if number == 1:
                shutil.copy2(wheels[0], runtime / wheels[0].name)
    reproducible = builds[0] == builds[1]
    receipt = {
        "schema": "atproto.ops.reproducible-build-receipt/v1",
        "source": {"commit": commit, "tree": tree, "dirty": False},
        "target": f"linux-{platform.machine()}/cpython-{platform.python_version()}",
        "toolchain": {
            "python": platform.python_version(),
            "pip": run(sys.executable, "-m", "pip", "--version").split()[1],
        },
        "source_date_epoch": int(epoch),
        "runtime_artifacts_compared": builds,
        "evidence_excluded_from_runtime_comparison": ["build-receipt.json"],
        "reproducible": reproducible,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    (evidence / "build-receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(receipt, sort_keys=True))
    return 0 if reproducible else 1


if __name__ == "__main__":
    raise SystemExit(main())
