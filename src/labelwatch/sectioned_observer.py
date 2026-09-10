"""Bounded, sectioned, read-only Labelwatch operational observation.

This module owns observation only.  It does not connect to SQLite, alter a
service, authorize an effect, or retry an occurrence.  Raw file content is
read only when a configured section requests a SHA-256 digest; it is never
placed in the result.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence


COMPLETE = "COMPLETE"
REFUSED = "REFUSED"
NOT_OBSERVED = "NOT_OBSERVED"


@dataclass(frozen=True)
class SectionLimits:
    entries: int
    path_bytes: int
    content_bytes: int
    file_bytes: int
    output_bytes: int
    seconds: float


@dataclass(frozen=True)
class ObserverConfig:
    database: str
    backup_destination: str
    release_root: str
    configuration_files: tuple[str, ...] = ()
    unit_files: tuple[str, ...] = ()
    output_bytes: int = 4 * 1024 * 1024
    required_seconds: float = 15.0
    required_content_bytes_per_cut: int = 2 * 1024 * 1024
    release: SectionLimits = SectionLimits(
        8192, 512 * 1024, 256 * 1024 * 1024, 64 * 1024 * 1024,
        3 * 1024 * 1024, 60.0,
    )
    configuration: SectionLimits = SectionLimits(
        64, 64 * 1024, 16 * 1024 * 1024, 4 * 1024 * 1024,
        256 * 1024, 20.0,
    )
    unit_files_limits: SectionLimits = SectionLimits(
        128, 128 * 1024, 16 * 1024 * 1024, 4 * 1024 * 1024,
        512 * 1024, 20.0,
    )
    process_entries: int = 1024
    descriptor_entries: int = 20_000
    descriptor_matches: int = 1024
    process_output_bytes: int = 512 * 1024
    process_seconds: float = 20.0
    process_content_bytes: int = 128 * 1024 * 1024
    argv_bytes_per_process: int = 32 * 1024
    maps_bytes_per_process: int = 1024 * 1024


class SectionRefusal(Exception):
    def __init__(self, reason: str, observations: Mapping[str, object] | None = None):
        super().__init__(reason)
        self.reason = reason
        self.observations = dict(observations or {})


@dataclass
class ReadBudget:
    limit: int
    consumed: int = 0

    def read(self, path: Path, maximum: int) -> bytes:
        allowance = min(maximum, self.limit - self.consumed)
        if allowance <= 0:
            raise SectionRefusal("CONTENT_BYTE_LIMIT_EXCEEDED", {"content_bytes_read": self.consumed})
        try:
            with path.open("rb") as stream:
                content = stream.read(allowance)
        except PermissionError as exc:
            raise SectionRefusal("PERMISSION_DENIED", {"content_bytes_read": self.consumed}) from exc
        self.consumed += len(content)
        # No one-byte probe crosses the declared ceiling. Filling the complete
        # allowance is conservatively refused because EOF was not observed.
        if len(content) == allowance:
            raise SectionRefusal("CONTENT_BOUND_REACHED_WITHOUT_EOF", {"content_bytes_read": self.consumed})
        return content


def _deadline(seconds: float) -> int:
    return time.monotonic_ns() + int(seconds * 1_000_000_000)


def _check_time(deadline_ns: int, observations: Mapping[str, object]) -> None:
    if time.monotonic_ns() > deadline_ns:
        raise SectionRefusal("TIME_LIMIT_EXCEEDED", observations)


def _sha256_file(path: Path, byte_limit: int, deadline_ns: int) -> tuple[str, int]:
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise SectionRefusal("NOT_A_REGULAR_FILE")
    if before.st_size > byte_limit:
        raise SectionRefusal("FILE_BYTE_LIMIT_EXCEEDED")
    digest = hashlib.sha256()
    consumed = 0
    # O_NOFOLLOW closes the pathname-substitution interval for the final open.
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if (opened.st_dev, opened.st_ino, opened.st_size) != (
            before.st_dev, before.st_ino, before.st_size
        ):
            raise SectionRefusal("PATH_IDENTITY_CHANGED_BEFORE_READ")
        while consumed < opened.st_size:
            _check_time(deadline_ns, {"content_bytes_read": consumed})
            chunk = os.read(fd, min(1024 * 1024, opened.st_size - consumed))
            if not chunk:
                break
            consumed += len(chunk)
            digest.update(chunk)
        if consumed != opened.st_size:
            raise SectionRefusal("SHORT_CONTENT_READ", {"content_bytes_read": consumed})
        after = os.fstat(fd)
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
            opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns
        ):
            raise SectionRefusal(
                "CONTENT_MUTATED_DURING_READ", {"content_bytes_read": consumed}
            )
    finally:
        os.close(fd)
    return digest.hexdigest(), consumed


def _path_record(path: Path) -> dict[str, object]:
    try:
        observed = path.lstat()
    except FileNotFoundError:
        return {"path": str(path), "state": "ABSENT"}
    except PermissionError:
        return {"path": str(path), "state": "UNAVAILABLE", "reason": "PERMISSION_DENIED"}
    if stat.S_ISLNK(observed.st_mode):
        return {
            "path": str(path),
            "state": "SYMLINK_REFUSED",
            "device": observed.st_dev,
            "inode": observed.st_ino,
        }
    return {
        "path": str(path),
        "state": "PRESENT",
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "mode": stat.S_IMODE(observed.st_mode),
        "type": stat.S_IFMT(observed.st_mode),
        "logical_bytes": observed.st_size,
        "allocated_bytes": observed.st_blocks * 512,
        "mtime_ns": observed.st_mtime_ns,
    }


def _unescape_mount(value: str) -> str:
    for encoded, decoded in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
        value = value.replace(encoded, decoded)
    return value


def parse_mountinfo(text: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for line in text.splitlines():
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        lhs, rhs = left.split(), right.split()
        if len(lhs) < 6 or len(rhs) < 2:
            continue
        records.append({
            "major_minor": lhs[2],
            "mount_root": _unescape_mount(lhs[3]),
            "mount_point": _unescape_mount(lhs[4]),
            "filesystem_type": rhs[0],
            "mount_source": _unescape_mount(rhs[1]),
        })
    return records


def _mount_for(path: Path, mountinfo: Sequence[Mapping[str, str]]) -> dict[str, str]:
    absolute = str(path.resolve(strict=False))
    matches = [m for m in mountinfo if absolute == m["mount_point"] or absolute.startswith(m["mount_point"].rstrip("/") + "/")]
    if not matches:
        raise SectionRefusal("MOUNT_NOT_OBSERVABLE")
    return dict(max(matches, key=lambda item: len(item["mount_point"])))


def _statvfs(path: Path) -> dict[str, int]:
    probe = path if path.exists() else path.parent
    values = os.statvfs(probe)
    return {
        "block_size": values.f_frsize,
        "filesystem_bytes": values.f_blocks * values.f_frsize,
        "available_bytes": values.f_bavail * values.f_frsize,
    }


def _filesystem_uuid(mount_source: str, maximum_entries: int = 256) -> str:
    try:
        source_stat = os.stat(mount_source)
        source_identity = source_stat.st_rdev
        entries = sorted(Path("/dev/disk/by-uuid").iterdir(), key=lambda p: p.name)
    except (FileNotFoundError, PermissionError, OSError):
        return NOT_OBSERVED
    if len(entries) > maximum_entries:
        return NOT_OBSERVED
    for entry in entries:
        try:
            if entry.stat().st_rdev == source_identity:
                return entry.name
        except (FileNotFoundError, PermissionError, OSError):
            continue
    return NOT_OBSERVED


def _read_small(path: Path, maximum: int = 4096, budget: ReadBudget | None = None) -> str | None:
    try:
        raw = budget.read(path, maximum) if budget else path.read_bytes()[: maximum + 1]
        if len(raw) > maximum:
            return None
        return raw.decode("ascii", "strict").strip()
    except (FileNotFoundError, PermissionError, UnicodeDecodeError, OSError):
        return None


def _device_chain(major_minor: str, budget: ReadBudget, maximum_depth: int = 8) -> list[dict[str, object]]:
    chain: list[dict[str, object]] = []
    current = Path("/sys/dev/block") / major_minor
    seen: set[str] = set()
    for _ in range(maximum_depth):
        try:
            resolved = current.resolve(strict=True)
            st = current.stat()
        except (FileNotFoundError, PermissionError, OSError):
            if not chain:
                chain.append({"major_minor": major_minor, "state": "NOT_OBSERVED"})
            break
        name = resolved.name
        if name in seen:
            chain.append({"kernel_name": name, "state": "REFUSED", "reason": "DEVICE_ANCESTRY_CYCLE"})
            break
        seen.add(name)
        dev_text = _read_small(resolved / "dev", budget=budget) or major_minor
        canonical = f"/dev/{name}"
        record: dict[str, object] = {
            "major_minor": dev_text,
            "kernel_name": name,
            "canonical_device_locator": canonical,
            "locator_device": st.st_dev,
            "device_type": "partition" if (resolved / "partition").exists() else "disk",
            "size_sectors": _read_small(resolved / "size", budget=budget),
            "logical_sector_bytes": _read_small(resolved / "queue/logical_block_size", budget=budget),
            "partition_start_sectors": _read_small(resolved / "start", budget=budget),
        }
        try:
            locator_stat = os.stat(canonical)
            locator_major_minor = f"{os.major(locator_stat.st_rdev)}:{os.minor(locator_stat.st_rdev)}"
            record["locator_major_minor"] = locator_major_minor
            record["locator_matches_sysfs_identity"] = locator_major_minor == dev_text
            if locator_major_minor != dev_text:
                record["state"] = "REFUSED"
                record["reason"] = "DEVICE_LOCATOR_IDENTITY_MISMATCH"
        except (FileNotFoundError, PermissionError, OSError):
            record["locator_major_minor"] = "NOT_OBSERVED"
            record["locator_matches_sysfs_identity"] = "NOT_OBSERVED"
        size = record["size_sectors"]
        if isinstance(size, str) and size.isdigit():
            record["device_bytes"] = int(size) * 512
        chain.append(record)
        if record["device_type"] != "partition":
            break
        parent = resolved.parent
        parent_dev = _read_small(parent / "dev", budget=budget)
        if not parent_dev:
            chain.append({"state": "NOT_OBSERVED", "reason": "PARENT_DEVICE_IDENTITY_UNAVAILABLE"})
            break
        current = Path("/sys/dev/block") / parent_dev
    return chain


def take_required_cut(config: ObserverConfig) -> dict[str, object]:
    deadline_ns = _deadline(config.required_seconds)
    read_budget = ReadBudget(config.required_content_bytes_per_cut)
    database = Path(config.database)
    paths = {
        "database": database,
        "wal": Path(config.database + "-wal"),
        "shm": Path(config.database + "-shm"),
        "database_parent": database.parent,
        "backup_destination": Path(config.backup_destination),
        "release_root": Path(config.release_root),
    }
    try:
        mountinfo = parse_mountinfo(read_budget.read(Path("/proc/self/mountinfo"), 1024 * 1024).decode("utf-8"))
    except (OSError, UnicodeError, SectionRefusal) as exc:
        raise SectionRefusal("MOUNTINFO_UNAVAILABLE", {"error_class": type(exc).__name__})
    records: dict[str, object] = {}
    mounts: dict[str, object] = {}
    devices: dict[str, object] = {}
    for name, path in paths.items():
        _check_time(deadline_ns, {"completed_paths": len(records)})
        record = _path_record(path)
        records[name] = record
        if record["state"] == "SYMLINK_REFUSED":
            raise SectionRefusal("ENROLLED_PATH_IS_SYMLINK", {"paths": records})
        if record["state"] == "UNAVAILABLE":
            raise SectionRefusal("ENROLLED_PATH_UNAVAILABLE", {"paths": records})
        if name not in ("wal", "shm") and record["state"] != "PRESENT":
            raise SectionRefusal("REQUIRED_ENROLLED_PATH_ABSENT", {"paths": records})
        probe = path if record["state"] == "PRESENT" else path.parent
        try:
            mount = _mount_for(probe, mountinfo)
            mount.update(_statvfs(probe))
            mount["filesystem_uuid"] = _filesystem_uuid(mount["mount_source"])
        except (OSError, SectionRefusal) as exc:
            raise SectionRefusal("FILESYSTEM_TOPOLOGY_UNAVAILABLE", {"paths": records, "failed_path": str(path), "error_class": type(exc).__name__})
        mounts[name] = mount
        major_minor = mount["major_minor"]
        if major_minor not in devices:
            devices[major_minor] = _device_chain(major_minor, read_budget)
            if any(record.get("state") == REFUSED for record in devices[major_minor]):
                raise SectionRefusal("DEVICE_LOCATOR_IDENTITY_MISMATCH", {"paths": records, "filesystems": mounts, "device_chains": devices})
    return {
        "observed_monotonic_ns": time.monotonic_ns(),
        "content_read_accounting": {"bytes": read_budget.consumed, "limit": read_budget.limit},
        "paths": records,
        "filesystems": mounts,
        "device_chains": devices,
    }


def _stable_required(first: Mapping[str, object], second: Mapping[str, object]) -> tuple[bool, list[str]]:
    changed: list[str] = []
    for key in ("paths", "filesystems", "device_chains"):
        if first.get(key) != second.get(key):
            changed.append(key)
    return not changed, changed


def observe_required(config: ObserverConfig, cut: Callable[[ObserverConfig], dict[str, object]] = take_required_cut) -> dict[str, object]:
    try:
        first = cut(config)
        second = cut(config)
        stable, changed = _stable_required(first, second)
        return {
            "disposition": COMPLETE if stable else REFUSED,
            "reason": "STABLE_REQUIRED_CUT" if stable else "REQUIRED_CUT_CHANGED",
            "limits": {"seconds_per_cut": config.required_seconds, "cuts": 2},
            "first_cut": first,
            "second_cut": second,
            "changed_dimensions": changed,
        }
    except SectionRefusal as exc:
        return {"disposition": NOT_OBSERVED, "reason": exc.reason, "observations": exc.observations, "limits": {"seconds_per_cut": config.required_seconds, "cuts": 2}}


def observe_files(paths: Iterable[Path], limits: SectionLimits, *, relative_to: Path | None = None) -> dict[str, object]:
    deadline_ns = _deadline(limits.seconds)
    records: list[dict[str, object]] = []
    path_bytes = content_bytes = 0
    counters = {"entries": 0, "path_bytes": 0, "content_bytes_read": 0}
    try:
        for path in paths:
            _check_time(deadline_ns, counters)
            counters["entries"] += 1
            if counters["entries"] > limits.entries:
                raise SectionRefusal("ENTRY_LIMIT_EXCEEDED", counters)
            try:
                locator = str(path.relative_to(relative_to)) if relative_to else str(path)
            except ValueError:
                raise SectionRefusal("PATH_OUTSIDE_ENROLLED_ROOT", counters)
            path_bytes += len(locator.encode("utf-8"))
            counters["path_bytes"] = path_bytes
            if path_bytes > limits.path_bytes:
                raise SectionRefusal("PATH_BYTE_LIMIT_EXCEEDED", counters)
            observed = path.lstat()
            if stat.S_ISLNK(observed.st_mode):
                records.append({"path": locator, "state": "SYMLINK_EXCLUDED"})
                continue
            if stat.S_ISDIR(observed.st_mode):
                records.append({"path": locator, "state": "DIRECTORY_METADATA_ONLY"})
                continue
            if not stat.S_ISREG(observed.st_mode):
                records.append({"path": locator, "state": "NON_REGULAR_METADATA_ONLY"})
                continue
            remaining = limits.content_bytes - content_bytes
            if observed.st_size > remaining:
                raise SectionRefusal("CONTENT_BYTE_LIMIT_EXCEEDED", counters)
            digest, consumed = _sha256_file(path, min(limits.file_bytes, remaining), deadline_ns)
            content_bytes += consumed
            counters["content_bytes_read"] = content_bytes
            records.append({
                "path": locator,
                "state": "HASHED_CONTENT_NOT_RETAINED",
                "logical_bytes": observed.st_size,
                "content_bytes_read": consumed,
                "sha256": digest,
                "device": observed.st_dev,
                "inode": observed.st_ino,
            })
            if len(json.dumps(records, separators=(",", ":")).encode("utf-8")) > limits.output_bytes:
                raise SectionRefusal("SECTION_OUTPUT_LIMIT_EXCEEDED", counters)
    except KeyboardInterrupt:
        return {"disposition": REFUSED, "reason": "INTERRUPTED", "limits": asdict(limits), "counters": counters, "records": records}
    except PermissionError:
        return {"disposition": NOT_OBSERVED, "reason": "PERMISSION_DENIED", "limits": asdict(limits), "counters": counters, "records": records}
    except FileNotFoundError:
        return {"disposition": NOT_OBSERVED, "reason": "PATH_DISAPPEARED", "limits": asdict(limits), "counters": counters, "records": records}
    except SectionRefusal as exc:
        return {"disposition": REFUSED, "reason": exc.reason, "limits": asdict(limits), "counters": {**counters, **exc.observations}, "records": records}
    return {"disposition": COMPLETE, "reason": "BOUNDED_HASH_CENSUS_COMPLETE", "limits": asdict(limits), "counters": counters, "records": records}


def _tree_paths(root: Path, entry_limit: int) -> Iterable[Path]:
    # scandir does not follow directory symlinks. Discovery itself stops after
    # the one extra entry needed to produce a bounded refusal.
    pending = [root]
    yielded = 0
    while pending:
        current = pending.pop()
        yield current
        yielded += 1
        if yielded > entry_limit:
            return
        if current.is_symlink() or not current.is_dir():
            continue
        with os.scandir(current) as entries:
            for item in entries:
                pending.append(Path(item.path))
                if yielded + len(pending) > entry_limit:
                    break


def observe_release(config: ObserverConfig) -> dict[str, object]:
    return observe_files(_tree_paths(Path(config.release_root), config.release.entries), config.release, relative_to=Path(config.release_root))


def observe_sqlite_header(config: ObserverConfig) -> dict[str, object]:
    path = Path(config.database)
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            header = os.read(fd, 100)
        finally:
            os.close(fd)
    except (FileNotFoundError, PermissionError, OSError) as exc:
        return {"disposition": NOT_OBSERVED, "reason": type(exc).__name__, "content_bytes_read": 0, "raw_content_retained": False}
    if len(header) != 100:
        return {"disposition": REFUSED, "reason": "SHORT_SQLITE_HEADER", "content_bytes_read": len(header), "raw_content_retained": False}
    return {
        "disposition": COMPLETE,
        "reason": "EXACT_HEADER_READ",
        "content_bytes_read": 100,
        "raw_content_retained": False,
        "header_sha256": hashlib.sha256(header).hexdigest(),
        "magic_matches_sqlite3": header[:16] == b"SQLite format 3\x00",
        "claim_limit": "HEADER_FIELDS_ONLY_NOT_INTEGRITY_OR_RECOVERABILITY",
    }


def _proc_start_ticks(stat_bytes: bytes) -> str:
    # comm may contain spaces and ')'; fields after the last ')' are stable to split.
    suffix = stat_bytes.rsplit(b")", 1)
    if len(suffix) != 2:
        raise ValueError("malformed proc stat")
    fields = suffix[1].split()
    return fields[19].decode("ascii")  # field 22 overall; suffix starts at field 3


def _bounded_proc_digest(path: Path, maximum: int, budget: ReadBudget) -> tuple[str, int]:
    before = budget.consumed
    content = budget.read(path, maximum)
    if budget.consumed - before > maximum:
        raise SectionRefusal("PROCESS_FILE_BYTE_LIMIT_EXCEEDED")
    return hashlib.sha256(content).hexdigest(), budget.consumed - before


def observe_processes(config: ObserverConfig, proc_root: Path = Path("/proc")) -> dict[str, object]:
    deadline_ns = _deadline(config.process_seconds)
    counters = {
        "process_entries": 0,
        "descriptor_entries": 0,
        "descriptor_matches": 0,
        "unavailable_processes": 0,
        "content_bytes_read": 0,
    }
    records: list[dict[str, object]] = []
    read_budget = ReadBudget(config.process_content_bytes)
    database = Path(config.database)
    try:
        database_stat = database.stat(follow_symlinks=False)
        database_identity = (database_stat.st_dev, database_stat.st_ino)
    except (FileNotFoundError, PermissionError, OSError) as exc:
        return {"disposition": NOT_OBSERVED, "reason": type(exc).__name__, "counters": counters}
    try:
        pids = sorted((p for p in proc_root.iterdir() if p.name.isdigit()), key=lambda p: int(p.name))
        for process in pids:
            _check_time(deadline_ns, counters)
            counters["process_entries"] += 1
            if counters["process_entries"] > config.process_entries:
                raise SectionRefusal("PROCESS_ENTRY_LIMIT_EXCEEDED", counters)
            try:
                before = _proc_start_ticks(read_budget.read(process / "stat", 8192))
                argv_hash, argv_bytes = _bounded_proc_digest(process / "cmdline", config.argv_bytes_per_process, read_budget)
                maps_hash, maps_bytes = _bounded_proc_digest(process / "maps", config.maps_bytes_per_process, read_budget)
                counters["content_bytes_read"] = read_budget.consumed
                matches: list[dict[str, object]] = []
                for descriptor in sorted((process / "fd").iterdir(), key=lambda p: int(p.name)):
                    counters["descriptor_entries"] += 1
                    if counters["descriptor_entries"] > config.descriptor_entries:
                        raise SectionRefusal("DESCRIPTOR_ENTRY_LIMIT_EXCEEDED", counters)
                    try:
                        descriptor_stat = descriptor.stat()
                    except (FileNotFoundError, PermissionError, OSError):
                        continue
                    if (descriptor_stat.st_dev, descriptor_stat.st_ino) != database_identity:
                        continue
                    counters["descriptor_matches"] += 1
                    if counters["descriptor_matches"] > config.descriptor_matches:
                        raise SectionRefusal("DESCRIPTOR_MATCH_LIMIT_EXCEEDED", counters)
                    flags = _read_small(process / "fdinfo" / descriptor.name, budget=read_budget)
                    access = "NOT_OBSERVED"
                    if flags:
                        for line in flags.splitlines():
                            if line.startswith("flags:"):
                                try:
                                    mode = int(line.split(":", 1)[1].strip(), 8) & os.O_ACCMODE
                                    access = {os.O_RDONLY: "READ_ONLY", os.O_WRONLY: "WRITE_ONLY", os.O_RDWR: "READ_WRITE"}.get(mode, "NOT_OBSERVED")
                                except ValueError:
                                    pass
                    matches.append({"descriptor": descriptor.name, "access": access})
                after = _proc_start_ticks(read_budget.read(process / "stat", 8192))
                counters["content_bytes_read"] = read_budget.consumed
                if before != after:
                    raise SectionRefusal("PROCESS_IDENTITY_CHANGED", counters)
                if matches:
                    records.append({
                        "pid": int(process.name),
                        "start_ticks": before,
                        "argv_sha256": argv_hash,
                        "argv_bytes_read": argv_bytes,
                        "maps_sha256": maps_hash,
                        "maps_bytes_read": maps_bytes,
                        "database_descriptors": matches,
                        "raw_content_retained": False,
                    })
                    if len(json.dumps(records, separators=(",", ":")).encode()) > config.process_output_bytes:
                        raise SectionRefusal("PROCESS_OUTPUT_LIMIT_EXCEEDED", counters)
            except SectionRefusal:
                raise
            except (FileNotFoundError, ProcessLookupError):
                # Process exited during the bounded scan; this is explicit incomplete testimony.
                counters["unavailable_processes"] += 1
            except (PermissionError, OSError, UnicodeError, ValueError):
                counters["unavailable_processes"] += 1
    except KeyboardInterrupt:
        return {"disposition": REFUSED, "reason": "INTERRUPTED", "limits": _process_limits(config), "counters": counters, "records": records}
    except SectionRefusal as exc:
        return {"disposition": REFUSED, "reason": exc.reason, "limits": _process_limits(config), "counters": {**counters, **exc.observations}, "records": records}
    return {"disposition": COMPLETE, "reason": "BOUNDED_PROCESS_CENSUS_COMPLETE", "limits": _process_limits(config), "counters": counters, "records": records}


def _process_limits(config: ObserverConfig) -> dict[str, object]:
    return {
        "process_entries": config.process_entries,
        "descriptor_entries": config.descriptor_entries,
        "descriptor_matches": config.descriptor_matches,
        "output_bytes": config.process_output_bytes,
        "seconds": config.process_seconds,
        "argv_bytes_per_process": config.argv_bytes_per_process,
        "maps_bytes_per_process": config.maps_bytes_per_process,
        "content_bytes": config.process_content_bytes,
    }


def _not_observed(reason: str, limits: Mapping[str, object] | None = None) -> dict[str, object]:
    result: dict[str, object] = {"disposition": NOT_OBSERVED, "reason": reason}
    if limits:
        result["limits"] = dict(limits)
    return result


def _bound_output(record: dict[str, object], maximum: int) -> dict[str, object]:
    encoded = lambda: json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    def finalize(extra: Mapping[str, object] | None = None) -> bool:
        record["output"] = {"disposition": COMPLETE, "bytes": 0, "limit": maximum, **dict(extra or {})}
        for _ in range(4):
            record["output"]["bytes"] = len(encoded())
        if len(encoded()) <= maximum:
            return True
        record.pop("output", None)
        return False

    if finalize():
        return record
    # Required facts and all dispositions are retained. Optional record detail is discarded explicitly.
    for name in ("release", "configuration", "unit_files"):
        section = record["sections"].get(name)
        if isinstance(section, dict) and "records" in section:
            section["records"] = "OMITTED_TO_MEET_OVERALL_OUTPUT_BOUND"
            section["details_retained"] = False
            if finalize({"optional_details_omitted": True}):
                return record
    for name, section in tuple(record["sections"].items()):
        if name == "required_capacity_topology" or not isinstance(section, dict):
            continue
        summary = {key: section[key] for key in ("disposition", "reason", "limits", "counters", "matches_closed_required_cut") if key in section}
        summary["details_retained"] = False
        summary["detail_reason"] = "OMITTED_TO_MEET_OVERALL_OUTPUT_BOUND"
        record["sections"][name] = summary
    if finalize({"optional_details_omitted": True}):
        return record
    # An impossibly small limit is reported without violating its own bound.
    minimal = {
        "schema": record["schema"],
        "output": {
            "disposition": REFUSED,
            "reason": "OVERALL_OUTPUT_LIMIT_TOO_SMALL_FOR_REQUIRED_FACTS",
            "limit": maximum,
        },
        "required_facts_sha256": hashlib.sha256(
            json.dumps(record["sections"]["required_capacity_topology"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    for _ in range(4):
        minimal["output"]["bytes"] = len(json.dumps(minimal, sort_keys=True, separators=(",", ":")).encode())
    if len(json.dumps(minimal, sort_keys=True, separators=(",", ":")).encode()) > maximum:
        minimal = {"output": {"disposition": REFUSED, "reason": "OUTPUT_LIMIT_TOO_SMALL", "limit": maximum}}
        if len(json.dumps(minimal, sort_keys=True, separators=(",", ":")).encode()) > maximum:
            return {}
    return minimal


def run_observer(config: ObserverConfig, *, required_cut: Callable[[ObserverConfig], dict[str, object]] = take_required_cut) -> dict[str, object]:
    record: dict[str, object] = {
        "schema": "labelwatch.sectioned-operational-observation/v1",
        "contract": {
            "read_only": True,
            "sql_access": False,
            "service_actions": False,
            "production_effects": False,
            "automatic_retry": False,
            "raw_content_retained": False,
        },
        "sections": {},
    }
    required = observe_required(config, required_cut)
    record["sections"]["required_capacity_topology"] = required
    optional_names = ("release", "configuration", "unit_files", "processes", "sqlite_header", "post_optional_corroboration")
    if required["disposition"] != COMPLETE:
        for name in optional_names:
            record["sections"][name] = _not_observed("REQUIRED_SECTION_NOT_COMPLETE")
        return _bound_output(record, config.output_bytes)
    try:
        for name, operation in (
            ("release", lambda: observe_release(config)),
            ("configuration", lambda: observe_files((Path(p) for p in config.configuration_files), config.configuration)),
            ("unit_files", lambda: observe_files((Path(p) for p in config.unit_files), config.unit_files_limits)),
            ("processes", lambda: observe_processes(config)),
            ("sqlite_header", lambda: observe_sqlite_header(config)),
        ):
            try:
                record["sections"][name] = operation()
            except OSError as exc:
                record["sections"][name] = _not_observed(type(exc).__name__)
        try:
            post = required_cut(config)
            record["sections"]["post_optional_corroboration"] = {
                "disposition": COMPLETE,
                "reason": "CORROBORATING_CUT_RECORDED",
                "cut": post,
                "matches_closed_required_cut": _stable_required(required["second_cut"], post)[0],
            }
        except SectionRefusal as exc:
            record["sections"]["post_optional_corroboration"] = _not_observed(exc.reason)
    except KeyboardInterrupt:
        for name in optional_names:
            record["sections"].setdefault(name, _not_observed("INTERRUPTED_BEFORE_SECTION"))
    return _bound_output(record, config.output_bytes)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--backup-destination", required=True)
    parser.add_argument("--release-root", required=True)
    parser.add_argument("--configuration-file", action="append", default=[])
    parser.add_argument("--unit-file", action="append", default=[])
    args = parser.parse_args(argv)
    config = ObserverConfig(
        database=args.database,
        backup_destination=args.backup_destination,
        release_root=args.release_root,
        configuration_files=tuple(args.configuration_file),
        unit_files=tuple(args.unit_file),
    )
    print(json.dumps(run_observer(config), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
