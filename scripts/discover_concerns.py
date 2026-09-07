#!/usr/bin/env python3
"""Discover repository-owned concern manifests without project knowledge."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


SKIP_PARTS = {".git", ".venv", "node_modules", "target", "__pycache__"}


def manifests(roots: list[Path]):
    seen = set()
    for root in roots:
        candidates = [root / ".ops" / "concerns.toml"]
        candidates.extend(root.glob("*/.ops/concerns.toml"))
        for path in sorted(candidates):
            resolved = path.resolve()
            if resolved in seen or not resolved.is_file() or any(part in SKIP_PARTS for part in resolved.parts):
                continue
            seen.add(resolved)
            yield resolved


def load(path: Path) -> dict:
    with path.open("rb") as handle:
        value = tomllib.load(handle)
    if value.get("schema") != "project.concerns/v1":
        raise ValueError(f"{path}: unsupported schema")
    ids = [item.get("id") for item in value.get("concerns", [])]
    if not ids or any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError(f"{path}: concern IDs must be present and unique")
    return value


def load_binding(repo: Path) -> dict:
    path = repo / ".ops" / "observation.toml"
    with path.open("rb") as handle:
        value = tomllib.load(handle)
    if value.get("schema") != "project.observation-binding/v1":
        raise ValueError(f"{path}: unsupported binding schema")
    return value


def run_status(repo: Path, status: dict, timeout: float) -> dict:
    command = status.get("argv")
    if not isinstance(command, list) or not command or not all(isinstance(part, str) and part for part in command):
        return {"available": False, "error": "manifest has no valid argv status command"}
    env = {"PATH": os.environ.get("PATH", "")}
    for key, value in (status.get("environment") or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            return {"available": False, "error": "status environment must be string-to-string"}
        env[key] = str((repo / value).resolve()) if key.endswith("PATH") and not Path(value).is_absolute() else value
    try:
        completed = subprocess.run(command, cwd=repo, env=env, text=True, capture_output=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"available": False, "error": str(exc)}
    if completed.returncode:
        return {"available": False, "exit_code": completed.returncode, "error": completed.stderr.strip()}
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        return {"available": False, "error": f"status output is not JSON: {exc}"}
    return {"available": True, "schema": payload.get("schema"), "payload": payload}


def discover(roots: list[Path], *, execute_status: bool = False, timeout: float = 10.0) -> dict:
    repositories = []
    for path in manifests(roots):
        manifest = load(path)
        repo = path.parent.parent
        binding = load_binding(repo)
        item = {
            "repository": str(repo),
            "manifest": str(path),
            "project": manifest.get("project"),
            "manifest_schema": manifest["schema"],
            "status_schema": binding.get("output_schema"),
            "required_concerns": [concern["id"] for concern in manifest["concerns"] if concern.get("required") is True],
            "status": {"available": False, "reason": "not requested"},
        }
        if execute_status:
            item["status"] = run_status(repo, binding, timeout)
        repositories.append(item)
    return {"schema": "project.concern_discovery/v1", "repositories": repositories}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("roots", nargs="+", help="repository or workspace roots")
    parser.add_argument("--run-status", action="store_true", help="invoke each acquisition-binding status command")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args(argv)
    try:
        result = discover([Path(root) for root in args.roots], execute_status=args.run_status, timeout=args.timeout)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
