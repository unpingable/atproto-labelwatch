#!/usr/bin/env python3
"""Weekly crow coordinator: verified NFS archive -> catalog -> root trim.

Use a durable systemd service and a private JSON config. An unfinished
occurrence blocks a successor; --resume names that exact occurrence. No key
generation, provider provisioning, or generation replacement is performed.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from labelwatch.trim import TrimRefused, verify_partition
from tools.label_events_frontdoor_archive import SummaryRef, verify_summary
from tools.label_events_retention_apply import save, sha


def run(argv, **kwargs):
    return subprocess.run(list(map(str, argv)), check=True, text=True, **kwargs)


def publish(source, target):
    """An interrupted NFS copy never publishes a final artifact name."""
    source, target = Path(source), Path(target)
    if target.exists():
        if sha(source) != sha(target):
            raise RuntimeError("published archive identity differs; preserve and reconcile")
        return
    tmp = target.with_name(target.name + ".incomplete")
    shutil.copy2(source, tmp)
    if sha(source) != sha(tmp):
        raise RuntimeError("archive copy readback differs")
    with tmp.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(tmp, target)
    directory_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


class Cycle:
    def __init__(self, config, occurrence, lock_fd):
        self.config, self.path = config, Path(occurrence)
        self.lock_fd = lock_fd
        self.path.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.ssh = ["ssh", "-i", config["ssh_key"], "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "AddKeysToAgent=no", "-o", "ForwardAgent=no", "-o", "BatchMode=yes", config["host"]]
        self.scp = ["scp", "-C", "-i", config["ssh_key"], "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "AddKeysToAgent=no", "-o", "ForwardAgent=no", "-o", "BatchMode=yes"]
        self.state = json.loads((self.path / "checkpoint.json").read_text()) if (self.path / "checkpoint.json").exists() else {}

    def remote(self, *args):
        return run(self.ssh + [shlex.join(list(map(str, args)))], capture_output=True).stdout

    def pull(self, remote, target, expected_sha=None):
        target = Path(target)
        tmp = target.with_name(target.name + ".incomplete")
        run(self.scp + [self.config["host"] + ":" + remote, tmp])
        if expected_sha is not None and sha(tmp) != expected_sha:
            raise RuntimeError("downloaded archive partition differs; preserve incomplete copy")
        if str(target).endswith(".json"):
            json.loads(tmp.read_text())
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def dispatch(self, unit, script, terminal, log, timeout, cpu, memory):
        # Inspect a dispatch gap before reissuing the exact launch.
        code = run(self.ssh + [shlex.join(["systemctl", "show", unit, "-p", "LoadState", "-p", "ActiveState"])], capture_output=True)
        if "LoadState=not-found" in code.stdout:
            probe = "import json;from pathlib import Path; t=Path(" + repr(terminal) + ");print(json.dumps({'terminal':t.read_text().strip() if t.exists() else None,'log_exists':Path(" + repr(log) + ").exists()}))"
            prior = json.loads(self.remote(self.config["host_python"], "-c", probe))
            if prior["terminal"] == "exit=0":
                return
            if prior["terminal"] is not None or prior["log_exists"]:
                raise RuntimeError("original producer evidence exists without unit; reconcile preserved occurrence before retry")
            self.remote("systemd-run", "--unit", unit, "--property=Type=oneshot", f"--property=TimeoutStartSec={timeout}", f"--property=CPUQuota={cpu}", f"--property=MemoryMax={memory}", "--no-block", "/bin/bash", script)

    def phase(self, name, **extra):
        self.state.update(phase=name, time=datetime.now(timezone.utc).isoformat(), **extra)
        save(self.path / "checkpoint.json", self.state)
        print(json.dumps(self.state), flush=True)

    def mounted_archive(self):
        archive = Path(self.config["archive_root"])
        archive.stat()  # activate the configured automount before inspection
        rows = run(["findmnt", "-n", "-o", "FSTYPE,SOURCE,OPTIONS", "-T", archive], capture_output=True).stdout.strip().splitlines()
        # findmnt emits both the automount and its overmounted NFS mount.
        fields = rows[-1].split() if rows else []
        if len(fields) != 3 or fields[0] not in ("nfs", "nfs4") or "rw" not in fields[2].split(","):
            raise TrimRefused("writable NFS backing mount unavailable; no trim")
        if self.config.get("archive_source") and fields[1] != self.config["archive_source"]:
            raise TrimRefused("NFS source differs from admitted archive identity")
        if shutil.disk_usage(archive).free < 20 * 1024**3:
            raise TrimRefused("verified NFS mount/capacity unavailable; no trim")

    def admit_apply_memory(self):
        # Catalog verification uses a bounded reclaimable file cache. Do not
        # infer today's host capacity for a later scheduled occurrence.
        available = int(self.remote(self.config["host_python"], "-c",
            "from pathlib import Path; print(next(int(row.split()[1])*1024 for row in Path('/proc/meminfo').read_text().splitlines() if row.startswith('MemAvailable:')))"))
        if available < 5 * 1024**3:
            raise TrimRefused("host5GiB available-memory gate failed before apply")
        self.phase("apply-memory-admitted", host_available_bytes=available, apply_memory_max_bytes=4 * 1024**3)

    def poll(self, unit, terminal, deadline, root_reserve=True):
        until = time.monotonic() + deadline
        while time.monotonic() < until:
            status = self.remote("systemctl", "show", unit, "-p", "ActiveState", "-p", "SubState", "-p", "MainPID", "-p", "Result", "-p", "LoadState")
            self.phase("monitor-" + unit, unit=unit, status=status)
            if "ActiveState=activating" not in status and "ActiveState=active" not in status:
                terminal_text = self.remote("cat", terminal).strip()
                if terminal_text != "exit=0" or ("LoadState=not-found" not in status and "Result=success" not in status):
                    raise RuntimeError(f"original producer failed: {unit}: {terminal_text}")
                return
            if root_reserve:
                available = int(self.remote("df", "-B1", "--output=avail", "/").splitlines()[-1])
                if available < 32 * 1024**3:
                    self.remote("systemctl", "stop", unit)
                    raise RuntimeError("original producer stopped at root32GiB reserve boundary")
            time.sleep(30)
        raise RuntimeError(f"supervision deadline: inspect original {unit}, do not duplicate")

    def execute(self):
        c = self.config
        self.mounted_archive()
        identity = run(["git", "-C", c["source"], "rev-parse", "HEAD"], capture_output=True).stdout.strip()
        if identity != c["tools_revision"]:
            raise TrimRefused("coordinator source identity differs from admitted config")
        facts_code = "import sqlite3,json; c=sqlite3.connect('file:/var/lib/labelwatch/labelwatch.db?mode=ro',uri=True); print(json.dumps(dict(c.execute(\"select key,value from meta where key like 'retention:%'\"))))"
        facts = json.loads(self.remote(c["host_python"], "-c", facts_code))
        if (self.path / "config.json").exists() and json.loads((self.path / "config.json").read_text()) != c:
            raise TrimRefused("resume must use the exact original coordinator configuration")
        if not self.state:
            if facts.get("retention:trim_pending"):
                raise TrimRefused("host has a pending trim; reconcile its original occurrence")
            old_floor = datetime.fromisoformat(facts["retention:live_floor"].replace("Z", "+00:00"))
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            new_floor = min(today - timedelta(days=c.get("live_days", 40)), old_floor + timedelta(days=c.get("max_delta_days", 7)))
            if new_floor <= old_floor:
                self.phase("accepted-no-floor-move", result="working set already inside retained window")
                return
            health = json.loads(self.remote("curl", "-fsS", "--max-time", "30", "http://127.0.0.1:8423/health"))
            if health["retention"]["coverage"] != "complete":
                raise TrimRefused("current API coverage is incomplete; reconcile before successor")
            self.phase("planned", old_floor=old_floor.strftime("%Y-%m-%dT00:00:00Z"), floor=new_floor.strftime("%Y-%m-%dT00:00:00Z"), old_catalog_sha256=health["retention"]["catalog"]["sha256"], source_revision=identity, host=c["host"], execution_identity=os.environ.get("INVOCATION_ID", "manual durable resume"))
            save(self.path / "config.json", c)
        name = self.path.name
        remote_dir = c["host_work_root"] + "/" + name
        zone = c["zone_work_root"] + "/" + name
        archive = Path(c["archive_root"]) / name
        archive.mkdir(mode=0o700, exist_ok=True)
        partition_name = "label-events-lt-" + self.state["floor"][:10] + ".manifest.json"
        partition = archive / partition_name
        if not partition.exists():
            self.remote("mkdir", "-p", remote_dir, zone + "/export")
            unit = "labelwatch-delta-export-" + name
            script = self.path / "export.sh"
            script.write_text("#!/bin/bash\nset -euo pipefail\nexec >" + shlex.quote(remote_dir + "/export.log") + " 2>&1\ntrap 'printf \"exit=%s\\n\" \"$?\" > " + shlex.quote(remote_dir + "/EXPORT-TERMINAL") + "' EXIT\n" + shlex.join([c["host_python"], c["host_source"] + "/tools/label_events_trim_export.py", "--db", "/var/lib/labelwatch/labelwatch.db", "--floor", self.state["floor"], "--out-dir", zone + "/export", "--verify"]) + "\n")
            # Persist identity before launching. Resume supervises this unit,
            # rather than reissuing export into an existing destination.
            if not self.state.get("export_launched"):
                free = int(self.remote("df", "-B1", "--output=avail", c["zone_work_root"]).splitlines()[-1])
                if free < 32 * 1024**3 + 15 * 1024**3:
                    raise TrimRefused("Zone transient export/catalog/rollback envelope unavailable")
                run(self.scp + [script, c["host"] + ":" + remote_dir + "/export.sh"])
                self.phase("export-launch-recorded", export_launched=True, export_unit=unit, export_terminal=remote_dir + "/EXPORT-TERMINAL")
            self.dispatch(unit, remote_dir + "/export.sh", remote_dir + "/EXPORT-TERMINAL", remote_dir + "/export.log", 3600, "50%", "512M")
            self.poll(unit, remote_dir + "/EXPORT-TERMINAL", 3700)
            self.pull(zone + "/export/" + partition_name, partition)
            pm = json.loads(partition.read_text())
            self.pull(zone + "/export/" + pm["partition"]["file"], archive / pm["partition"]["file"], pm["partition"]["sha256"])
        elif not (archive / json.loads(partition.read_text())["partition"]["file"]).exists():
            # Resume an interrupted copy from the original completed export.
            pm = json.loads(partition.read_text())
            self.pull(zone + "/export/" + pm["partition"]["file"], archive / pm["partition"]["file"], pm["partition"]["sha256"])
        pm = verify_partition(partition)
        self.phase("archive-partition-verified", manifest_sha256=sha(partition), watermark=pm["selection"]["watermark"], rows=pm["rows"]["count"])
        current = json.loads(Path(c["current_catalog_record"]).read_text())
        base_manifest = Path(self.state.get("base_manifest", current["manifest"]))
        summaries = Path(c["summaries_root"])
        output = self.path / "catalog"
        new_manifest = output / "frontdoor-cold-catalog.sqlite.manifest.json"
        if not new_manifest.exists():
            if shutil.disk_usage(self.path).free < 240 * 1024**3:
                raise TrimRefused("crow200GiB reserve plus40GiB merge budget unavailable")
            self.phase("catalog-extension", base_manifest=str(base_manifest), log=str(self.path / "catalog.log"))
            if output.exists():
                # Only after the previous durable coordinator exited and its
                # inherited file lock is free. Keep the failed occurrence.
                preserved = output.with_name("catalog.failed-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
                os.rename(output, preserved)
                self.phase("failed-catalog-preserved", preserved=str(preserved))
            with (self.path / "catalog.log").open("a") as log:
                child = subprocess.Popen(list(map(str, [sys.executable, Path(c["source"]) / "tools/label_events_catalog_extend.py", "--base-manifest", base_manifest, "--summaries-dir", summaries, "--partition-manifest", partition, "--out-dir", output])), stdout=log, stderr=log, pass_fds=(self.lock_fd,))
                self.phase("catalog-producer-running", child_pid=child.pid, child_start_stat=Path(f"/proc/{child.pid}/stat").read_text(), coordinator_pid=os.getpid())
                while child.poll() is None:
                    allocated = int(run(["du", "-s", "-B1", self.path], capture_output=True).stdout.split()[0])
                    if allocated > 40 * 1024**3 or shutil.disk_usage(self.path).free < 200 * 1024**3:
                        child.terminate()
                        child.wait(timeout=30)
                        raise RuntimeError("catalog child stopped at admitted storage boundary; preserve occurrence")
                    time.sleep(30)
                if child.returncode:
                    raise RuntimeError(f"catalog producer exit={child.returncode}; failed output retained")
        nm = json.loads(new_manifest.read_text())
        for source in (output / nm["catalog"]["file"], new_manifest):
            target = archive / source.name
            publish(source, target)
            if sha(source) != sha(target):
                raise RuntimeError("NFS catalog readback differs")
        for source in (output / "summaries").iterdir():
            target = summaries / source.name
            publish(source, target)
            if sha(source) != sha(target):
                raise RuntimeError("NFS new summary readback differs")
        for src in nm["sources"]:
            verify_summary(SummaryRef(summaries / ("date=" + src["day"] + ".sqlite"), src["summary_sha256"]))
        old = json.loads(base_manifest.read_text())
        old_db = base_manifest.parent / old["catalog"]["file"]
        if sha(old_db) != self.state["old_catalog_sha256"]:
            raise TrimRefused("old NFS catalog differs from installed predecessor")
        self.mounted_archive()
        verify_partition(partition)
        archived_new_db = archive / nm["catalog"]["file"]
        if sha(archived_new_db) != nm["catalog"]["sha256"]:
            raise TrimRefused("new NFS catalog custody changed")
        custody = {"result": "NFS_ARCHIVE_CUSTODY_VERIFIED", "verified_at": datetime.now(timezone.utc).isoformat(), "partition": {"archive_path": str(partition), "sha256": pm["partition"]["sha256"]}, "catalog": {"archive_path": str(archive / new_manifest.name), "sha256": nm["catalog"]["sha256"]}, "old_catalog": {"archive_path": str(base_manifest), "sha256": old["catalog"]["sha256"]}}
        save(self.path / "nfs-custody.json", custody)
        self.phase("archive-catalog-verified", catalog_sha256=nm["catalog"]["sha256"])
        unit = "labelwatch-delta-apply-" + name
        script = self.path / "apply.sh"
        if not self.state.get("apply_launched"):
            self.admit_apply_memory()
            self.remote("mkdir", "-p", zone + "/catalog", remote_dir)
            for source in (archived_new_db, archive / new_manifest.name):
                run(self.scp + [source, c["host"] + ":" + zone + "/catalog/"])
            old_remote_manifest = self.remote("systemctl", "show", "labelwatch-api", "-p", "Environment", "--value")
            selections = shlex.split(old_remote_manifest)
            old_remote_manifest = next(s.split("=", 1)[1] for s in selections if s.startswith("LABELWATCH_FRONTDOOR_COLD_CATALOG="))
            apply_config = {"database": "/var/lib/labelwatch/labelwatch.db", "receipt_dir": "/opt/labelwatch/deployment-receipts/" + name, "old_manifest": old_remote_manifest, "old_catalog_sha256": self.state["old_catalog_sha256"], "candidate_manifest": zone + "/catalog/" + new_manifest.name, "partition_manifest": zone + "/export/" + partition.name, "rollback_dir": zone + "/rollback", "nfs_custody": custody}
            save(self.path / "apply-config.json", apply_config)
            run(self.scp + [self.path / "apply-config.json", Path(c["source"]) / "tools/label_events_retention_apply.py", c["host"] + ":" + remote_dir + "/"])
            script.write_text("#!/bin/bash\nset -euo pipefail\nexec >" + shlex.quote(remote_dir + "/apply.log") + " 2>&1\ntrap 'printf \"exit=%s\\n\" \"$?\" > " + shlex.quote(remote_dir + "/APPLY-TERMINAL") + "' EXIT\n" + shlex.join([c["host_python"], remote_dir + "/label_events_retention_apply.py", "--config", remote_dir + "/apply-config.json"]) + "\n")
            run(self.scp + [script, c["host"] + ":" + remote_dir + "/apply.sh"])
            self.admit_apply_memory()
            self.phase("apply-launch-recorded", apply_launched=True, apply_unit=unit, apply_terminal=remote_dir + "/APPLY-TERMINAL")
        self.dispatch(unit, remote_dir + "/apply.sh", remote_dir + "/APPLY-TERMINAL", remote_dir + "/apply.log", 7200, "100%", "4G")
        self.poll(unit, remote_dir + "/APPLY-TERMINAL", 7300, root_reserve=False)
        run(self.scp + [c["host"] + ":/opt/labelwatch/deployment-receipts/" + name + "/trim.json", self.path])
        result = json.loads((self.path / "trim.json").read_text())
        if result["floor"] != pm["floor"] or result["catalog"]["sha256"] != nm["catalog"]["sha256"]:
            raise RuntimeError("host terminal receipt identity mismatch")
        save(c["current_catalog_record"], {"manifest": str(archive / new_manifest.name), "previous_manifest": str(base_manifest), "floor": pm["floor"], "watermark": pm["selection"]["watermark"]})
        # Reverify archive custody before retiring transfer-only copies. The
        # old Zone catalog cannot roll back a committed newer live floor.
        self.mounted_archive()
        verify_partition(partition)
        if sha(archived_new_db) != nm["catalog"]["sha256"] or sha(old_db) != old["catalog"]["sha256"]:
            raise RuntimeError("archive custody changed before staging retirement")
        self.phase("accepted-floor-staging-closeout", trim=result)
        cleanup = json.loads(self.remote(c["host_python"], remote_dir + "/label_events_retention_apply.py", "--config", remote_dir + "/apply-config.json", "--retire-staging"))
        save(self.path / "host-storage-closeout.json", cleanup)
        self.phase("accepted", result="LABELWATCH_SUSTAINABLE_DELTA_TRIM_ACCEPTED", trim=result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--resume", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if config.get("live_days", 40) < 31 or not 1 <= config.get("max_delta_days", 7) <= 7:
        raise TrimRefused("retain>=31days and bounded1..7day delta required")
    root = Path(config["occurrence_root"])
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (root / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        active = root / "ACTIVE.json"
        if active.exists() and not args.resume:
            prior = json.loads(active.read_text())
            checkpoint = Path(prior["occurrence"]) / "checkpoint.json"
            if not checkpoint.exists() or json.loads(checkpoint.read_text())["phase"] not in ("accepted", "accepted-no-floor-move"):
                raise TrimRefused("unfinished occurrence; inspect and explicitly --resume its exact path")
        if args.resume:
            if args.resume.resolve().parent != root.resolve() or not active.exists() or Path(json.loads(active.read_text())["occurrence"]).resolve() != args.resume.resolve():
                raise TrimRefused("resume must name the exact registered occurrence under configured root")
        occurrence = args.resume or root / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
        save(active, {"occurrence": str(occurrence), "host": config["host"]})
        Cycle(config, occurrence, lock.fileno()).execute()


if __name__ == "__main__":
    main()
