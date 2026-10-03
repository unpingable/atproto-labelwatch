#!/usr/bin/env python3
"""Apply a verified archive/catalog extension on the Labelwatch host.

Run with the deployed interpreter, inside a durable maintenance unit. The
crow coordinator supplies freshly verified NFS custody and stages the same
bytes on Zone. Zone is temporary transfer/rollback storage; serving stays
on root. A pending trim always resumes with the same partition and catalog.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from labelwatch import db, retention
from labelwatch.frontdoor_archive import load_catalog
from labelwatch.trim import TrimRefused, run_trim, verify_partition

UNITS = ["labelwatch-lock-watcher", "labelwatch-api", "labelwatch-discovery", "labelwatch"]
RESERVE = 32 * 1024**3


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save(path, value):
    path = Path(path)
    tmp = path.with_suffix(".new")
    with tmp.open("w") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def command(*args):
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout


def custody_check(config, *, resuming=False):
    """No database/unit mutation is reachable without complete NFS custody."""
    custody = config.get("nfs_custody", {})
    if custody.get("result") != "NFS_ARCHIVE_CUSTODY_VERIFIED":
        raise TrimRefused("fresh verified NFS archive/catalog custody is required")
    try:
        verified_at = datetime.fromisoformat(custody["verified_at"])
        age = (datetime.now(timezone.utc) - verified_at).total_seconds()
        if not resuming and not 0 <= age <= 3600:
            raise ValueError("outside one-hour launch window")
    except (KeyError, TypeError, ValueError) as exc:
        raise TrimRefused("NFS custody must be freshly verified for this occurrence") from exc
    for kind in ("partition", "catalog", "old_catalog"):
        item = custody.get(kind, {})
        if not item.get("archive_path") or not item.get("sha256"):
            raise TrimRefused(f"missing verified NFS {kind} identity")
    if custody["old_catalog"]["sha256"] != config["old_catalog_sha256"]:
        raise TrimRefused("old NFS catalog custody differs from selected rollback identity")
    manifest = verify_partition(config["partition_manifest"])
    candidate = load_catalog(config["candidate_manifest"])
    if manifest["partition"]["sha256"] != custody["partition"]["sha256"]:
        raise TrimRefused("staged partition differs from verified NFS custody")
    if candidate.database_sha256 != custody["catalog"]["sha256"]:
        raise TrimRefused("staged catalog differs from verified NFS custody")
    if candidate.end_day_exclusive + "T00:00:00Z" != manifest["floor"]:
        raise TrimRefused("candidate catalog does not join the proposed floor")
    if candidate.source_event_id_upper_bound != manifest["selection"]["watermark"]:
        raise TrimRefused("candidate catalog watermark differs from partition")
    return manifest, candidate


def completed_trim(conn, manifest, manifest_path):
    """Recover the committed database receipt after a lost file receipt."""
    if retention.live_floor(conn) != manifest["floor"] or db.get_meta(conn, "retention:trim_pending"):
        return None
    result = json.loads(db.get_meta(conn, "retention:last_trim") or "{}")
    if result.get("manifest_sha256") != sha(manifest_path) or result.get("floor") != manifest["floor"]:
        raise TrimRefused("completed floor belongs to another trim occurrence")
    return result


def apply(config):
    receipt = Path(config["receipt_dir"])
    checkpoint = receipt / "checkpoint.json"
    state = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    config_sha = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    if state and state["config_sha256"] != config_sha:
        raise TrimRefused("resume configuration identity changed")
    manifest, candidate = custody_check(config, resuming=state.get("custody_admitted", False))
    if os.geteuid() != 0:
        raise PermissionError("host maintenance requires root")
    receipt.mkdir(mode=0o700, parents=True, exist_ok=True)
    if state.get("phase") == "accepted":
        return json.loads((receipt / "trim.json").read_text())
    old_manifest = Path(config["old_manifest"])
    old_db = old_manifest.parent / json.loads(old_manifest.read_text())["catalog"]["file"] if old_manifest.exists() else None
    new_dir = Path("/var/lib/labelwatch/cold") / ("catalog-" + candidate.database_sha256[:16])
    new_manifest = new_dir / Path(config["candidate_manifest"]).name
    rollback = Path(config["rollback_dir"])
    rollback.mkdir(mode=0o700, parents=True, exist_ok=True)

    def phase(name, **extra):
        state.update(config_sha256=config_sha, phase=name, time=time.time(), **extra)
        save(checkpoint, state)
        print(json.dumps(state), flush=True)

    def restore():
        # Called only before a floor commit. Restore immutable bytes first.
        command("systemctl", "stop", *UNITS)
        if new_dir.exists():
            expected = {Path(config["candidate_manifest"]).name, candidate.database_path.name}
            if not {p.name for p in new_dir.iterdir()} <= expected:
                raise RuntimeError("unexpected files in occurrence-owned new catalog directory")
            for path in new_dir.iterdir():
                path.unlink()
            new_dir.rmdir()
        old_manifest.parent.mkdir(parents=True, exist_ok=True)
        for source in rollback.iterdir():
            shutil.copy2(source, old_manifest.parent / source.name)
        for unit in ("labelwatch", "labelwatch-api"):
            dropin = Path(f"/etc/systemd/system/{unit}.service.d/zz-atproto-release.conf")
            shutil.copy2(receipt / (unit + ".conf"), dropin)
        command("systemctl", "daemon-reload")
        command("systemctl", "start", *reversed(UNITS))

    conn = db.connect(config["database"])
    try:
        current = retention.live_floor(conn)
        if current not in (manifest["previous_floor"], manifest["floor"]):
            raise TrimRefused("live floor differs from this occurrence")
        if not state:
            phase("custody-admitted", custody_admitted=True)
        result = completed_trim(conn, manifest, config["partition_manifest"])
        already_complete = result is not None
        if already_complete:
            save(receipt / "trim.json", result)
        if current == manifest["previous_floor"] and not state.get("preservation_ready"):
            old = load_catalog(old_manifest)
            if old.database_sha256 != config["old_catalog_sha256"]:
                raise TrimRefused("selected old catalog identity changed")
            save(receipt / "pre-meta.json", dict(conn.execute("SELECT key,value FROM meta WHERE key LIKE 'retention:%'")))
            save(receipt / "pre-health.json", json.loads(command("curl", "-fsS", "--max-time", "30", "http://127.0.0.1:8423/health")))
            # Real installed old coverage must refuse the proposed next floor.
            try:
                old_custody = retention.load_catalog_custody(str(old_manifest))
                if old_custody.load_status != "loaded":
                    raise RuntimeError("old catalog custody failed to load")
                run_trim(conn, manifest["floor"], config["partition_manifest"], catalog_custody=old_custody, dry_run=True)
            except TrimRefused as exc:
                expected = f"installed catalog ends at {old.end_day_exclusive}, not at the new floor {manifest['floor']}"
                if str(exc) != expected:
                    raise RuntimeError("next-floor negative control refused for another precondition") from exc
                save(receipt / "gap-refusal.json", {"refused": str(exc), "floor_unchanged": retention.live_floor(conn)})
            else:
                raise RuntimeError("old catalog unexpectedly authorized next floor")
            for source in (old_db, old_manifest):
                target = rollback / source.name
                if not target.exists():
                    if shutil.disk_usage(rollback).free - source.stat().st_size < RESERVE:
                        raise RuntimeError("Zone32GiB reserve would fail during rollback copy")
                    shutil.copy2(source, target)
                if sha(source) != sha(target):
                    raise RuntimeError("old Zone rollback copy differs")
            for unit in ("labelwatch", "labelwatch-api"):
                shutil.copy2(f"/etc/systemd/system/{unit}.service.d/zz-atproto-release.conf", receipt / (unit + ".conf"))
            phase("old-custody-verified", preservation_ready=True, old_catalog_sha256=old.database_sha256, old_manifest_sha256=sha(old_manifest))
        if not already_complete:
            command("systemctl", "stop", *UNITS)
            states = command("systemctl", "show", *UNITS, "-p", "Id", "-p", "ActiveState", "-p", "MainPID")
            (receipt / "stopped-units.txt").write_text(states)
            if "ActiveState=active" in states or any(line.startswith("MainPID=") and line != "MainPID=0" for line in states.splitlines()):
                raise RuntimeError("cohort stop did not establish inactive consumers")
            phase("cohort-stopped")
        if current == manifest["previous_floor"]:
            # Stop all consumers before retiring the root rollback copy.
            if old_manifest.exists():
                for process in Path("/proc").glob("[0-9]*"):
                    for fd in (process / "fd").glob("*"):
                        try:
                            if os.readlink(fd).startswith(str(old_manifest.parent) + "/"):
                                raise RuntimeError(f"open old-catalog descriptor: {fd}")
                        except FileNotFoundError:
                            pass
                names = {p.name for p in old_manifest.parent.iterdir()}
                if names != {old_manifest.name, old_db.name}:
                    raise RuntimeError("unexpected files in old root catalog directory")
                for path in (old_db, old_manifest):
                    if path.stat().st_nlink != 1 or sha(path) != sha(rollback / path.name):
                        raise RuntimeError("old root copy custody/link gate failed")
                phase("retiring-old-root-copy")
                old_db.unlink()
                old_manifest.unlink()
                old_manifest.parent.rmdir()
            new_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
            stage_manifest = Path(config["candidate_manifest"])
            stage_db = stage_manifest.parent / json.loads(stage_manifest.read_text())["catalog"]["file"]
            for source in (stage_db, stage_manifest):
                target = new_dir / source.name
                if not target.exists() or sha(target) != sha(source):
                    if shutil.disk_usage("/").free - source.stat().st_size < RESERVE:
                        raise RuntimeError("root32GiB reserve would fail during catalog copy")
                    shutil.copy2(source, target)
                    with target.open("rb") as handle:
                        os.fsync(handle.fileno())
                target.chmod(0o644)
                if sha(target) != sha(source):
                    raise RuntimeError("new root catalog copy differs")
            phase("new-root-catalog-verified", new_manifest=str(new_manifest), new_manifest_sha256=sha(new_manifest))
            for unit in ("labelwatch", "labelwatch-api"):
                path = Path(f"/etc/systemd/system/{unit}.service.d/zz-atproto-release.conf")
                original = (receipt / (unit + ".conf")).read_text()
                if str(old_manifest) not in original:
                    raise RuntimeError("old catalog missing from preserved release selection")
                path.write_text(original.replace(str(old_manifest), str(new_manifest)))
            command("systemctl", "daemon-reload")
        installed = retention.load_catalog_custody(str(new_manifest))
        if installed.load_status != "loaded" or installed.catalog.database_sha256 != candidate.database_sha256:
            raise RuntimeError("installed catalog identity differs")
        # Immutable files and paused collector/discovery serialize the checks
        # with the floor commit, delete batches, and late-row quarantine.
        if shutil.disk_usage("/").free < RESERVE:
            raise RuntimeError("root32GiB reserve unavailable before trim")
        if not already_complete:
            pre_free = conn.execute("PRAGMA freelist_count").fetchone()[0]
            run_trim(conn, manifest["floor"], config["partition_manifest"], catalog_custody=installed, archive_ref=config["nfs_custody"]["partition"]["archive_path"], dry_run=True)
            phase("trim-preconditions-accepted", freelist_before=pre_free)
            result = run_trim(conn, manifest["floor"], config["partition_manifest"], catalog_custody=installed, archive_ref=config["nfs_custody"]["partition"]["archive_path"], on_batch=lambda n: phase("trim-batch", batch=n))
            result["freelist_before"] = pre_free
            result["freelist_after"] = conn.execute("PRAGMA freelist_count").fetchone()[0]
            save(receipt / "trim.json", result)
            phase("trim-complete")
    except BaseException:
        if retention.live_floor(conn) == manifest["previous_floor"]:
            if state.get("preservation_ready"):
                restore()
            phase("blocked-or-rolled-back-before-floor")
        else:
            phase("resume-required-keep-new-catalog")
        raise
    finally:
        conn.close()
    command("systemctl", "start", *reversed(UNITS))
    for _ in range(240):
        try:
            health = json.loads(command("curl", "-fsS", "--max-time", "5", "http://127.0.0.1:8423/health"))
            if health["ok"] and health["frontdoor"]["ready"] and health["retention"]["coverage"] == "complete" and health["retention"]["live_floor"] == manifest["floor"]:
                save(receipt / "post-health.json", health)
                phase("accepted")
                return result
        except (subprocess.CalledProcessError, KeyError):
            pass
        time.sleep(3)
    raise RuntimeError("post-trim complete API health deadline exceeded; preserve new catalog")


def retire_staging(config):
    """Retire exact duplicated transfer files after an accepted floor move."""
    receipt = Path(config["receipt_dir"])
    if os.geteuid() != 0:
        raise PermissionError("host staging retirement requires root")
    state = json.loads((receipt / "checkpoint.json").read_text())
    config_sha = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    if state["config_sha256"] != config_sha:
        raise TrimRefused("staging retirement configuration identity changed")
    if state["phase"] != "accepted":
        raise TrimRefused("staging still has an unfinished transition dependency")
    if (receipt / "storage-closeout.json").exists():
        return json.loads((receipt / "storage-closeout.json").read_text())
    if (receipt / "storage-retirement-inventory.json").exists():
        raise TrimRefused("interrupted staging closeout: reconcile exact sealed inventory before retry or successor")
    manifest, candidate = custody_check(config, resuming=True)
    installed = load_catalog(state["new_manifest"])
    conn = db.connect(config["database"], readonly=True)
    try:
        if retention.live_floor(conn) != manifest["floor"] or db.get_meta(conn, "retention:trim_pending"):
            raise TrimRefused("floor/pending state does not permit staging retirement")
    finally:
        conn.close()
    if installed.database_sha256 != candidate.database_sha256:
        raise TrimRefused("serving root catalog differs from staged copy")
    paths = {}
    partition_manifest = Path(config["partition_manifest"])
    paths[partition_manifest] = sha(partition_manifest)
    paths[partition_manifest.parent / manifest["partition"]["file"]] = manifest["partition"]["sha256"]
    stage_manifest = Path(config["candidate_manifest"])
    paths[stage_manifest] = state["new_manifest_sha256"]
    paths[candidate.database_path] = candidate.database_sha256
    backup = Path(config["rollback_dir"])
    old_name = Path(config["old_manifest"]).name
    old_manifest = backup / old_name
    old_data = json.loads(old_manifest.read_text())
    paths[old_manifest] = state["old_manifest_sha256"]
    paths[backup / old_data["catalog"]["file"]] = state["old_catalog_sha256"]
    for directory in {p.parent for p in paths}:
        if set(directory.iterdir()) != {p for p in paths if p.parent == directory}:
            raise RuntimeError("unexpected staging files; retain for ownership review")
    for process in Path("/proc").glob("[0-9]*"):
        for fd in (process / "fd").glob("*"):
            try:
                if os.readlink(fd) in {str(p) for p in paths}:
                    raise RuntimeError(f"open staging descriptor: {fd}")
            except FileNotFoundError:
                pass
    before = shutil.disk_usage(backup).free
    allocated = 0
    for path, expected in paths.items():
        stat = path.stat()
        if stat.st_nlink != 1 or sha(path) != expected:
            raise RuntimeError("staging link/identity gate failed; retain")
        allocated += stat.st_blocks * 512
    save(receipt / "storage-retirement-inventory.json", {"paths": {str(p): h for p, h in paths.items()}, "allocated_bytes": allocated, "zone_free_before": before, "config_sha256": config_sha, "root_manifest": state["new_manifest"], "floor": manifest["floor"], "next_action": "If interrupted before storage-closeout.json, inspect remaining exact files/digests/fds/links and verified NFS/root custody; retire only named remaining duplicates and seal closeout. Never rerun floor transition."})
    for path in paths:
        path.unlink()
    for directory in {p.parent for p in paths}:
        directory.rmdir()
    result = {"result": "EXACT_STAGING_RETIRED", "paths": list(map(str, paths)), "allocated_bytes_reclaimed": allocated, "zone_free_before": before, "zone_free_after": shutil.disk_usage(backup.parent).free, "custody": "same bytes verified on NFS; new selected catalog on root; old catalog cannot authorize committed new floor"}
    save(receipt / "storage-closeout.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--retire-staging", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    print(json.dumps(retire_staging(config) if args.retire_staging else apply(config), indent=2))


if __name__ == "__main__":
    main()
