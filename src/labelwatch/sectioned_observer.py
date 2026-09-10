"""Bounded, sectioned, read-only Labelwatch operational observation.

This module owns observation only.  It does not connect to SQLite, alter a
service, authorize an effect, or retry an occurrence.  Bounded file content is
read for declared SHA-256 digests and for the explicitly declared mountinfo,
sysfs, process, descriptor and SQLite-header observations.  Raw content is
never placed in the result.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
import argparse
import sys
import signal
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

    def __post_init__(self) -> None:
        if self.output_bytes < 512:
            raise ValueError("section output_bytes must be at least 512")


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
    header_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.output_bytes < 128:
            raise ValueError("output_bytes must be at least 128 to retain a disposition")
        if self.process_output_bytes < 512:
            raise ValueError("process_output_bytes must be at least 512")


class SectionRefusal(Exception):
    def __init__(self, reason: str, observations: Mapping[str, object] | None = None):
        super().__init__(reason)
        self.reason = reason
        self.observations = dict(observations or {})


class SectionUnavailable(Exception):
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
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            descriptor = os.open(path, flags)
        except PermissionError as exc:
            raise SectionRefusal("PERMISSION_DENIED", {"content_bytes_read": self.consumed}) from exc
        chunks: list[bytes] = []
        observed = 0
        try:
            while observed < allowance:
                chunk = os.read(descriptor, allowance - observed)
                if not chunk:
                    break
                chunks.append(chunk)
                observed += len(chunk)
                self.consumed += len(chunk)
        finally:
            os.close(descriptor)
        # No one-byte probe crosses the declared ceiling. Filling the complete
        # allowance is conservatively refused because EOF was not observed.
        if observed == allowance:
            raise SectionRefusal("CONTENT_BOUND_REACHED_WITHOUT_EOF", {"content_bytes_read": self.consumed})
        return b"".join(chunks)


def _deadline(seconds: float) -> int:
    return time.monotonic_ns() + int(seconds * 1_000_000_000)


def _check_time(deadline_ns: int, observations: Mapping[str, object]) -> None:
    if time.monotonic_ns() > deadline_ns:
        raise SectionRefusal("TIME_LIMIT_EXCEEDED", observations)


def _descriptor_within(fd: int, root: Path) -> bool:
    try:
        target = Path(f"/proc/self/fd/{fd}").resolve(strict=True)
        target.relative_to(root)
        return True
    except (FileNotFoundError, OSError, ValueError):
        return False


def _root_identity(root: Path) -> tuple[int, int]:
    observed = root.lstat()
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise SectionRefusal("RELEASE_ROOT_DIRECTORY_REQUIRED")
    return observed.st_dev, observed.st_ino


def _check_root_identity(root: Path, expected: tuple[int, int]) -> None:
    if _root_identity(root) != expected:
        raise SectionRefusal("RELEASE_ROOT_IDENTITY_CHANGED")


def _sha256_file(
    path: Path,
    byte_limit: int,
    deadline_ns: int,
    physical_root: Path | None = None,
    physical_root_identity: tuple[int, int] | None = None,
) -> tuple[str, int, os.stat_result]:
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
        if physical_root is not None and physical_root_identity is not None:
            _check_root_identity(physical_root, physical_root_identity)
        opened = os.fstat(fd)
        if physical_root is not None and not _descriptor_within(fd, physical_root):
            raise SectionRefusal("OPENED_FILE_OUTSIDE_ENROLLED_ROOT")
        if (opened.st_dev, opened.st_ino, opened.st_size) != (
            before.st_dev, before.st_ino, before.st_size
        ):
            raise SectionRefusal("PATH_IDENTITY_CHANGED_BEFORE_READ")
        if opened.st_size == 0 and byte_limit <= 0:
            raise SectionRefusal(
                "ZERO_SIZE_FILE_UNVERIFIABLE_AT_CONTENT_BOUND",
                {"content_bytes_read": consumed},
            )
        if opened.st_size == 0:
            probe = os.read(fd, 1)
            consumed += len(probe)
            if probe:
                raise SectionRefusal("ZERO_SIZE_FILE_HAS_CONTENT", {"content_bytes_read": consumed})
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
        _check_time(deadline_ns, {"content_bytes_read": consumed})
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
            opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns
        ):
            raise SectionRefusal(
                "CONTENT_MUTATED_DURING_READ", {"content_bytes_read": consumed}
            )
        if physical_root is not None and physical_root_identity is not None:
            _check_root_identity(physical_root, physical_root_identity)
    except SectionRefusal as exc:
        try:
            os.close(fd)
        except BaseException:
            pass
        observations = dict(exc.observations)
        observations.setdefault("content_bytes_read", consumed)
        raise SectionRefusal(exc.reason, observations) from exc
    except KeyboardInterrupt as exc:
        try:
            os.close(fd)
        except BaseException:
            pass
        raise SectionRefusal("INTERRUPTED", {"content_bytes_read": consumed}) from exc
    except OSError as exc:
        try:
            os.close(fd)
        except BaseException:
            pass
        raise SectionUnavailable(type(exc).__name__, {"content_bytes_read": consumed}) from exc
    except BaseException:
        try:
            os.close(fd)
        except BaseException:
            pass
        raise
    try:
        os.close(fd)
    except KeyboardInterrupt as exc:
        raise SectionRefusal("INTERRUPTED", {"content_bytes_read": consumed}) from exc
    except OSError as exc:
        raise SectionUnavailable(type(exc).__name__, {"content_bytes_read": consumed}) from exc
    return digest.hexdigest(), consumed, after


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


def _filesystem_uuid(mount_source: str, maximum_entries: int = 256) -> tuple[str, int]:
    try:
        source_stat = os.stat(mount_source)
        source_identity = source_stat.st_rdev
        entries = os.scandir("/dev/disk/by-uuid")
    except (FileNotFoundError, PermissionError, OSError):
        return NOT_OBSERVED, 0
    observed = 0
    with entries:
        for entry in entries:
            observed += 1
            if observed > maximum_entries:
                return NOT_OBSERVED, observed
            try:
                if entry.stat().st_rdev == source_identity:
                    return entry.name, observed
            except (FileNotFoundError, PermissionError, OSError):
                continue
    return NOT_OBSERVED, observed


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
    records: dict[str, object] = {}
    mounts: dict[str, object] = {}
    devices: dict[str, object] = {}
    try:
        mountinfo = parse_mountinfo(read_budget.read(Path("/proc/self/mountinfo"), 1024 * 1024).decode("utf-8"))
        for name, path in paths.items():
            _check_time(deadline_ns, {"completed_paths": len(records)})
            record = _path_record(path)
            records[name] = record
            if record["state"] == "SYMLINK_REFUSED":
                raise SectionRefusal("ENROLLED_PATH_IS_SYMLINK")
            if record["state"] == "UNAVAILABLE":
                raise SectionRefusal("ENROLLED_PATH_UNAVAILABLE")
            if name not in ("wal", "shm") and record["state"] != "PRESENT":
                raise SectionRefusal("REQUIRED_ENROLLED_PATH_ABSENT")
            probe = path if record["state"] == "PRESENT" else path.parent
            try:
                mount = _mount_for(probe, mountinfo)
                mount.update(_statvfs(probe))
                mount["filesystem_uuid"], mount["uuid_entries_observed"] = _filesystem_uuid(mount["mount_source"])
            except (OSError, SectionRefusal) as exc:
                raise SectionRefusal("FILESYSTEM_TOPOLOGY_UNAVAILABLE", {"failed_path": str(path), "error_class": type(exc).__name__}) from exc
            mounts[name] = mount
            major_minor = mount["major_minor"]
            if major_minor not in devices:
                devices[major_minor] = _device_chain(major_minor, read_budget)
                if any(item.get("state") == REFUSED for item in devices[major_minor]):
                    raise SectionRefusal("DEVICE_LOCATOR_IDENTITY_MISMATCH")
        _check_time(deadline_ns, {"completed_paths": len(records)})
    except KeyboardInterrupt as exc:
        raise SectionRefusal("INTERRUPTED", {
            "content_read_accounting": {"bytes": read_budget.consumed, "limit": read_budget.limit},
            "paths": records,
            "filesystems": mounts,
            "device_chains": devices,
        }) from exc
    except (OSError, UnicodeError, SectionRefusal) as exc:
        reason = exc.reason if isinstance(exc, SectionRefusal) else "REQUIRED_CUT_UNAVAILABLE"
        prior = exc.observations if isinstance(exc, SectionRefusal) else {"error_class": type(exc).__name__}
        raise SectionRefusal(reason, {
            **prior,
            "content_read_accounting": {"bytes": read_budget.consumed, "limit": read_budget.limit},
            "paths": records,
            "filesystems": mounts,
            "device_chains": devices,
        }) from exc
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
    except KeyboardInterrupt:
        return {"disposition": NOT_OBSERVED, "reason": "INTERRUPTED", "first_cut_partial": {}, "limits": {"seconds_per_cut": config.required_seconds, "cuts": 2}}
    except SectionRefusal as exc:
        return {"disposition": NOT_OBSERVED, "reason": exc.reason, "observations": exc.observations, "limits": {"seconds_per_cut": config.required_seconds, "cuts": 2}}
    try:
        second = cut(config)
    except KeyboardInterrupt:
        return {"disposition": NOT_OBSERVED, "reason": "INTERRUPTED", "first_cut": first, "second_cut_partial": {}, "limits": {"seconds_per_cut": config.required_seconds, "cuts": 2}}
    except SectionRefusal as exc:
        return {"disposition": NOT_OBSERVED, "reason": exc.reason, "first_cut": first, "second_cut_partial": exc.observations, "limits": {"seconds_per_cut": config.required_seconds, "cuts": 2}}
    stable, changed = _stable_required(first, second)
    return {
        "disposition": COMPLETE if stable else REFUSED,
        "reason": "STABLE_REQUIRED_CUT" if stable else "REQUIRED_CUT_CHANGED",
        "limits": {"seconds_per_cut": config.required_seconds, "cuts": 2},
        "first_cut": first,
        "second_cut": second,
        "changed_dimensions": changed,
    }


def observe_files(paths: Iterable[Path], limits: SectionLimits, *, relative_to: Path | None = None) -> dict[str, object]:
    deadline_ns = _deadline(limits.seconds)
    records: list[dict[str, object]] = []
    path_bytes = content_bytes = 0
    counters = {"entries": 0, "entry_probes": 0, "path_bytes": 0, "probe_path_bytes": 0, "content_bytes_read": 0}

    def result(disposition: str, reason: str) -> dict[str, object]:
        value: dict[str, object] = {"disposition": disposition, "reason": reason, "limits": asdict(limits), "counters": counters, "records": records.copy()}
        if len(json.dumps(value, separators=(",", ":")).encode()) > limits.output_bytes:
            value["records"] = "OMITTED_TO_MEET_SECTION_OUTPUT_BOUND"
            value["details_retained"] = False
        return value

    def check_record_budget() -> None:
        _check_time(deadline_ns, counters)
        if len(json.dumps(result(REFUSED, "SECTION_OUTPUT_LIMIT_EXCEEDED"), separators=(",", ":")).encode("utf-8")) > limits.output_bytes or result(REFUSED, "SECTION_OUTPUT_LIMIT_EXCEEDED").get("details_retained") is False:
            records.pop()
            raise SectionRefusal("SECTION_OUTPUT_LIMIT_EXCEEDED", counters)

    try:
        physical_root_identity = _root_identity(relative_to) if relative_to else None
        physical_root = relative_to.resolve(strict=True) if relative_to else None
        if relative_to and physical_root_identity:
            _check_root_identity(relative_to, physical_root_identity)
    except SectionRefusal as exc:
        counters.update(exc.observations)
        return result(REFUSED, exc.reason)
    except (FileNotFoundError, PermissionError, OSError) as exc:
        return result(NOT_OBSERVED, type(exc).__name__)

    try:
        for path in paths:
            _check_time(deadline_ns, counters)
            if relative_to and physical_root_identity:
                _check_root_identity(relative_to, physical_root_identity)
            counters["entries"] += 1
            if counters["entries"] > limits.entries:
                counters["entry_probes"] = 1
                try:
                    probe_locator = str(path.relative_to(relative_to)) if relative_to else str(path)
                except ValueError:
                    probe_locator = str(path)
                counters["probe_path_bytes"] = len(probe_locator.encode("utf-8"))
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
                check_record_budget()
                continue
            if stat.S_ISDIR(observed.st_mode):
                records.append({"path": locator, "state": "DIRECTORY_METADATA_ONLY"})
                check_record_budget()
                continue
            if not stat.S_ISREG(observed.st_mode):
                records.append({"path": locator, "state": "NON_REGULAR_METADATA_ONLY"})
                check_record_budget()
                continue
            remaining = limits.content_bytes - content_bytes
            if observed.st_size > remaining:
                raise SectionRefusal("CONTENT_BYTE_LIMIT_EXCEEDED", counters)
            try:
                digest, consumed, hashed_stat = _sha256_file(
                    path,
                    min(limits.file_bytes, remaining),
                    deadline_ns,
                    physical_root,
                    physical_root_identity,
                )
            except SectionRefusal as exc:
                content_bytes += int(exc.observations.get("content_bytes_read", 0))
                counters["content_bytes_read"] = content_bytes
                raise SectionRefusal(exc.reason, counters) from exc
            except SectionUnavailable as exc:
                content_bytes += int(exc.observations.get("content_bytes_read", 0))
                counters["content_bytes_read"] = content_bytes
                raise SectionUnavailable(exc.reason, counters) from exc
            content_bytes += consumed
            counters["content_bytes_read"] = content_bytes
            records.append({
                "path": locator,
                "state": "HASHED_CONTENT_NOT_RETAINED",
                "logical_bytes": hashed_stat.st_size,
                "content_bytes_read": consumed,
                "sha256": digest,
                "device": hashed_stat.st_dev,
                "inode": hashed_stat.st_ino,
            })
            check_record_budget()
        _check_time(deadline_ns, counters)
        if relative_to and physical_root_identity:
            _check_root_identity(relative_to, physical_root_identity)
    except KeyboardInterrupt:
        return result(REFUSED, "INTERRUPTED")
    except PermissionError:
        return result(NOT_OBSERVED, "PERMISSION_DENIED")
    except FileNotFoundError:
        return result(NOT_OBSERVED, "PATH_DISAPPEARED")
    except OSError as exc:
        return result(NOT_OBSERVED, type(exc).__name__)
    except SectionUnavailable as exc:
        counters.update(exc.observations)
        return result(NOT_OBSERVED, exc.reason)
    except SectionRefusal as exc:
        counters.update(exc.observations)
        return result(REFUSED, exc.reason)
    return result(COMPLETE, "BOUNDED_HASH_CENSUS_COMPLETE")


def _tree_paths(root: Path, entry_limit: int) -> Iterable[Path]:
    # scandir does not follow directory symlinks. Discovery itself stops after
    # the one extra entry needed to produce a bounded refusal.
    pending = [root]
    yielded = 0
    discovered = 1
    root_stat = root.lstat()
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise SectionRefusal("RELEASE_ROOT_DIRECTORY_REQUIRED")
    root_resolved = root.resolve(strict=True)
    while pending:
        current = pending.pop()
        yield current
        yielded += 1
        if yielded > entry_limit:
            return
        current_stat = current.lstat()
        if stat.S_ISLNK(current_stat.st_mode) or not stat.S_ISDIR(current_stat.st_mode):
            continue
        if discovered >= entry_limit + 1:
            continue
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_DIRECTORY", 0)
        descriptor = os.open(current, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (current_stat.st_dev, current_stat.st_ino):
                raise SectionRefusal("DIRECTORY_IDENTITY_CHANGED_BEFORE_TRAVERSAL")
            current_root = root.lstat()
            if (current_root.st_dev, current_root.st_ino) != (root_stat.st_dev, root_stat.st_ino):
                raise SectionRefusal("RELEASE_ROOT_IDENTITY_CHANGED")
            if not _descriptor_within(descriptor, root_resolved):
                raise SectionRefusal("DIRECTORY_OUTSIDE_ENROLLED_ROOT")
            with os.scandir(descriptor) as entries:
                for item in entries:
                    pending.append(current / item.name)
                    discovered += 1
                    if discovered >= entry_limit + 1:
                        break
        finally:
            os.close(descriptor)


def observe_release(config: ObserverConfig) -> dict[str, object]:
    return observe_files(_tree_paths(Path(config.release_root), config.release.entries), config.release, relative_to=Path(config.release_root))


def observe_sqlite_header(config: ObserverConfig, expected_identity: tuple[int, int] | None = None) -> dict[str, object]:
    path = Path(config.database)
    deadline_ns = _deadline(config.header_seconds)
    consumed = 0
    try:
        path_stat = path.lstat()
        if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
            raise SectionRefusal("SQLITE_HEADER_REGULAR_FILE_REQUIRED")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(path, flags)
        try:
            before = os.fstat(fd)
            if (before.st_dev, before.st_ino) != (path_stat.st_dev, path_stat.st_ino):
                raise SectionRefusal("SQLITE_HEADER_PATH_IDENTITY_CHANGED")
            if expected_identity and (before.st_dev, before.st_ino) != expected_identity:
                raise SectionRefusal("SQLITE_HEADER_REQUIRED_IDENTITY_MISMATCH")
            header = os.read(fd, 100)
            consumed = len(header)
            after = os.fstat(fd)
            _check_time(deadline_ns, {"content_bytes_read": consumed})
            if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
                raise SectionRefusal("SQLITE_HEADER_CONTENT_MUTATED", {"content_bytes_read": len(header)})
        finally:
            os.close(fd)
    except SectionRefusal as exc:
        return {"disposition": REFUSED, "reason": exc.reason, "content_bytes_read": exc.observations.get("content_bytes_read", consumed), "raw_content_retained": False}
    except KeyboardInterrupt:
        return {"disposition": REFUSED, "reason": "INTERRUPTED", "content_bytes_read": consumed, "raw_content_retained": False}
    except (FileNotFoundError, PermissionError, OSError) as exc:
        return {"disposition": NOT_OBSERVED, "reason": type(exc).__name__, "content_bytes_read": consumed, "raw_content_retained": False}
    if len(header) != 100:
        return {"disposition": REFUSED, "reason": "SHORT_SQLITE_HEADER", "content_bytes_read": len(header), "raw_content_retained": False}
    return {
        "disposition": COMPLETE,
        "reason": "EXACT_HEADER_READ",
        "content_bytes_read": 100,
        "raw_content_retained": False,
        "header_sha256": hashlib.sha256(header).hexdigest(),
        "device": after.st_dev,
        "inode": after.st_ino,
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


def _bounded_scandir(
    root: Path,
    counters: dict[str, int],
    counter: str,
    limit: int,
    refusal: str,
) -> Iterable[os.DirEntry[str]]:
    """Lazily enumerate at most limit entries plus one refusal probe."""
    with os.scandir(root) as entries:
        for entry in entries:
            counters[counter] += 1
            if counters[counter] > limit:
                raise SectionRefusal(refusal, counters)
            yield entry


def _process_result(
    config: ObserverConfig,
    disposition: str,
    reason: str,
    counters: Mapping[str, int],
    records: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    value: dict[str, object] = {
        "disposition": disposition,
        "reason": reason,
        "limits": _process_limits(config),
        "counters": dict(counters),
        "records": list(records),
    }
    encoded = lambda: len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
    if encoded() <= config.process_output_bytes:
        return value
    value["records"] = "OMITTED_TO_MEET_PROCESS_OUTPUT_BOUND"
    value["details_retained"] = False
    if encoded() <= config.process_output_bytes:
        return value
    # This compact envelope is bounded even at the supported 512-byte floor.
    return {
        "disposition": disposition,
        "reason": reason,
        "limits": {"output_bytes": config.process_output_bytes},
        "counters": dict(counters),
        "details_retained": False,
    }


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
        for process_entry in _bounded_scandir(
            proc_root,
            counters,
            "process_entries",
            config.process_entries,
            "PROCESS_ENTRY_LIMIT_EXCEEDED",
        ):
            _check_time(deadline_ns, counters)
            if not process_entry.name.isdigit():
                continue
            process = proc_root / process_entry.name
            try:
                before = _proc_start_ticks(read_budget.read(process / "stat", 8192))
                matches: list[dict[str, object]] = []
                for descriptor_entry in _bounded_scandir(
                    process / "fd",
                    counters,
                    "descriptor_entries",
                    config.descriptor_entries,
                    "DESCRIPTOR_ENTRY_LIMIT_EXCEEDED",
                ):
                    _check_time(deadline_ns, counters)
                    descriptor = process / "fd" / descriptor_entry.name
                    try:
                        descriptor_stat = descriptor.stat()
                    except (FileNotFoundError, PermissionError, OSError):
                        continue
                    if (descriptor_stat.st_dev, descriptor_stat.st_ino) != database_identity:
                        continue
                    counters["descriptor_matches"] += 1
                    if counters["descriptor_matches"] > config.descriptor_matches:
                        raise SectionRefusal("DESCRIPTOR_MATCH_LIMIT_EXCEEDED", counters)
                    flags = _read_small(process / "fdinfo" / descriptor_entry.name, budget=read_budget)
                    access = "NOT_OBSERVED"
                    if flags:
                        for line in flags.splitlines():
                            if line.startswith("flags:"):
                                try:
                                    mode = int(line.split(":", 1)[1].strip(), 8) & os.O_ACCMODE
                                    access = {os.O_RDONLY: "READ_ONLY", os.O_WRONLY: "WRITE_ONLY", os.O_RDWR: "READ_WRITE"}.get(mode, "NOT_OBSERVED")
                                except ValueError:
                                    pass
                    matches.append({"descriptor": descriptor_entry.name, "access": access})
                _check_time(deadline_ns, counters)
                if not matches:
                    continue
                argv_hash, argv_bytes = _bounded_proc_digest(process / "cmdline", config.argv_bytes_per_process, read_budget)
                maps_hash, maps_bytes = _bounded_proc_digest(process / "maps", config.maps_bytes_per_process, read_budget)
                after = _proc_start_ticks(read_budget.read(process / "stat", 8192))
                counters["content_bytes_read"] = read_budget.consumed
                _check_time(deadline_ns, counters)
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
                    candidate = _process_result(
                        config, COMPLETE, "BOUNDED_PROCESS_CENSUS_COMPLETE", counters, records
                    )
                    if candidate.get("details_retained") is False:
                        records.pop()
                        raise SectionRefusal("PROCESS_OUTPUT_LIMIT_EXCEEDED", counters)
            except SectionRefusal:
                raise
            except (FileNotFoundError, ProcessLookupError):
                # Process exited during the bounded scan; this is explicit incomplete testimony.
                counters["unavailable_processes"] += 1
            except (PermissionError, OSError, UnicodeError, ValueError):
                counters["unavailable_processes"] += 1
            finally:
                counters["content_bytes_read"] = read_budget.consumed
    except KeyboardInterrupt:
        counters["content_bytes_read"] = read_budget.consumed
        return _process_result(config, REFUSED, "INTERRUPTED", counters, records)
    except SectionRefusal as exc:
        counters["content_bytes_read"] = read_budget.consumed
        merged = {**exc.observations, **counters}
        return _process_result(config, REFUSED, exc.reason, merged, records)
    except OSError as exc:
        counters["content_bytes_read"] = read_budget.consumed
        return _process_result(config, NOT_OBSERVED, type(exc).__name__, counters, records)
    counters["content_bytes_read"] = read_budget.consumed
    try:
        _check_time(deadline_ns, counters)
    except SectionRefusal as exc:
        return _process_result(config, REFUSED, exc.reason, counters, records)
    complete = _process_result(config, COMPLETE, "BOUNDED_PROCESS_CENSUS_COMPLETE", counters, records)
    if complete.get("details_retained") is False:
        return _process_result(config, REFUSED, "PROCESS_OUTPUT_LIMIT_EXCEEDED", counters, records)
    return complete


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


def _required_database_identity(required: Mapping[str, object]) -> tuple[int, int] | None:
    try:
        database = required["second_cut"]["paths"]["database"]
        return int(database["device"]), int(database["inode"])
    except (KeyError, TypeError, ValueError):
        return None


def _bound_output(record: dict[str, object], maximum: int) -> dict[str, object]:
    if maximum < 128:
        raise ValueError("output limit cannot retain a disposition")
    encoded = lambda: json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
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
        minimal["output"]["bytes"] = len(json.dumps(minimal, sort_keys=True, separators=(",", ":")).encode()) + 1
    if len(json.dumps(minimal, sort_keys=True, separators=(",", ":")).encode()) + 1 > maximum:
        minimal = {"output": {"disposition": REFUSED, "reason": "OUTPUT_LIMIT_TOO_SMALL", "limit": maximum}}
        if len(json.dumps(minimal, sort_keys=True, separators=(",", ":")).encode()) + 1 > maximum:
            raise ValueError("output limit cannot retain a disposition")
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
            ("sqlite_header", lambda: observe_sqlite_header(config, _required_database_identity(required))),
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
        except (SectionRefusal, OSError, KeyboardInterrupt) as exc:
            reason = exc.reason if isinstance(exc, SectionRefusal) else ("INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else type(exc).__name__)
            record["sections"]["post_optional_corroboration"] = _not_observed(reason)
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
    signal.signal(signal.SIGTERM, lambda _signum, _frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    sys.stdout.write(json.dumps(run_observer(config), sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
