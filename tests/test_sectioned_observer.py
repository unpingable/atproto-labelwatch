from __future__ import annotations

import inspect
import json
from dataclasses import replace
from pathlib import Path

import pytest

from labelwatch import sectioned_observer as observer


def _limits(**changes):
    base = observer.SectionLimits(
        entries=16,
        path_bytes=4096,
        content_bytes=4096,
        file_bytes=4096,
        output_bytes=65536,
        seconds=5,
    )
    return replace(base, **changes)


def _config(tmp_path: Path, **changes):
    database = tmp_path / "data" / "labelwatch.sqlite"
    database.parent.mkdir()
    database.write_bytes(b"SQLite format 3\x00" + b"\x00" * 84)
    backup = tmp_path / "backup"
    backup.mkdir()
    release = tmp_path / "release"
    release.mkdir()
    base = observer.ObserverConfig(
        database=str(database),
        backup_destination=str(backup),
        release_root=str(release),
        release=_limits(),
        configuration=_limits(),
        unit_files_limits=_limits(),
        output_bytes=128 * 1024,
    )
    return replace(base, **changes)


def _cut(_config):
    return {
        "observed_monotonic_ns": 1,
        "paths": {"database": {"device": 7, "inode": 8, "logical_bytes": 100}},
        "filesystems": {"database": {"major_minor": "7:0", "available_bytes": 1000}},
        "device_chains": {"7:0": [{"kernel_name": "fixture"}]},
    }


def test_required_closes_before_optional_configuration_refusal(tmp_path):
    config_path = tmp_path / "oversize.conf"
    config_path.write_bytes(b"x" * 9)
    config = _config(
        tmp_path,
        configuration_files=(str(config_path),),
        configuration=_limits(content_bytes=8, file_bytes=8),
    )

    result = observer.run_observer(config, required_cut=_cut)

    assert result["sections"]["required_capacity_topology"]["disposition"] == "COMPLETE"
    assert result["sections"]["configuration"]["disposition"] == "REFUSED"
    assert result["sections"]["configuration"]["reason"] == "CONTENT_BYTE_LIMIT_EXCEEDED"
    assert result["sections"]["sqlite_header"]["disposition"] == "COMPLETE"
    assert result["sections"]["post_optional_corroboration"]["disposition"] == "COMPLETE"


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"entries": 1}, "ENTRY_LIMIT_EXCEEDED"),
        ({"path_bytes": 1}, "PATH_BYTE_LIMIT_EXCEEDED"),
        ({"content_bytes": 2}, "CONTENT_BYTE_LIMIT_EXCEEDED"),
        ({"file_bytes": 2}, "FILE_BYTE_LIMIT_EXCEEDED"),
    ],
)
def test_file_census_bounds(tmp_path, change, reason):
    path = tmp_path / "abc"
    path.write_bytes(b"content")
    limits = _limits(**change)
    paths = [path, path] if reason == "ENTRY_LIMIT_EXCEEDED" else [path]
    result = observer.observe_files(paths, limits)
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == reason


def test_time_bound_retains_partial_counters(tmp_path, monkeypatch):
    path = tmp_path / "a"
    path.write_bytes(b"a")
    ticks = iter((0, 2_000_000_000))
    monkeypatch.setattr(observer.time, "monotonic_ns", lambda: next(ticks))
    result = observer.observe_files([path], _limits(seconds=1))
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "TIME_LIMIT_EXCEEDED"
    assert result["counters"]["entries"] == 0


def test_permission_failure_is_not_absence(tmp_path, monkeypatch):
    path = tmp_path / "private"
    path.write_bytes(b"x")
    monkeypatch.setattr(observer, "_sha256_file", lambda *args: (_ for _ in ()).throw(PermissionError()))
    result = observer.observe_files([path], _limits())
    assert result["disposition"] == "NOT_OBSERVED"
    assert result["reason"] == "PERMISSION_DENIED"


def test_interruption_retains_completed_records(tmp_path):
    first = tmp_path / "first"
    first.write_bytes(b"one")

    def paths():
        yield first
        raise KeyboardInterrupt

    result = observer.observe_files(paths(), _limits())
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "INTERRUPTED"
    assert result["records"][0]["state"] == "HASHED_CONTENT_NOT_RETAINED"


def test_symlink_is_metadata_only_and_target_content_unread(tmp_path):
    target = tmp_path / "target"
    target.write_bytes(b"secret")
    link = tmp_path / "link"
    link.symlink_to(target)
    result = observer.observe_files([link], _limits())
    assert result["disposition"] == "COMPLETE"
    assert result["records"] == [{"path": str(link), "state": "SYMLINK_EXCLUDED"}]
    assert result["counters"]["content_bytes_read"] == 0


def test_enrolled_required_symlink_fails_closed(tmp_path):
    actual = tmp_path / "actual.sqlite"
    actual.write_bytes(b"x")
    linked = tmp_path / "linked.sqlite"
    linked.symlink_to(actual)
    backup = tmp_path / "backup"
    backup.mkdir()
    release = tmp_path / "release"
    release.mkdir()
    config = observer.ObserverConfig(str(linked), str(backup), str(release))
    result = observer.observe_required(config)
    assert result["disposition"] == "NOT_OBSERVED"
    assert result["reason"] == "ENROLLED_PATH_IS_SYMLINK"


def test_content_accounting_matches_bytes_and_never_retains_content(tmp_path):
    path = tmp_path / "value"
    path.write_bytes(b"abcdef")
    result = observer.observe_files([path], _limits())
    assert result["counters"]["content_bytes_read"] == 6
    encoded = json.dumps(result)
    assert "abcdef" not in encoded
    assert result["records"][0]["state"] == "HASHED_CONTENT_NOT_RETAINED"


def test_pathname_replacement_before_open_refuses(tmp_path, monkeypatch):
    path = tmp_path / "subject"
    replacement = tmp_path / "replacement"
    path.write_bytes(b"first")
    replacement.write_bytes(b"second")
    real_open = observer.os.open

    def replacing_open(open_path, flags):
        path.unlink()
        replacement.rename(path)
        return real_open(open_path, flags)

    monkeypatch.setattr(observer.os, "open", replacing_open)
    result = observer.observe_files([path], _limits())
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "PATH_IDENTITY_CHANGED_BEFORE_READ"


def test_changed_required_cut_refuses_and_retains_both_cuts(tmp_path):
    calls = 0

    def changing(config):
        nonlocal calls
        calls += 1
        result = _cut(config)
        result["filesystems"]["database"]["available_bytes"] += calls
        return result

    result = observer.observe_required(_config(tmp_path), changing)
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "REQUIRED_CUT_CHANGED"
    assert result["changed_dimensions"] == ["filesystems"]
    assert result["first_cut"] != result["second_cut"]


def test_required_failure_prevents_optional_reads(tmp_path):
    config = _config(tmp_path)

    def unavailable(_config):
        raise observer.SectionRefusal("FIXTURE_UNAVAILABLE")

    result = observer.run_observer(config, required_cut=unavailable)
    assert result["sections"]["required_capacity_topology"]["disposition"] == "NOT_OBSERVED"
    assert all(
        section["reason"] == "REQUIRED_SECTION_NOT_COMPLETE"
        for name, section in result["sections"].items()
        if name != "required_capacity_topology"
    )


def test_post_optional_change_does_not_reopen_closed_required(tmp_path):
    calls = 0

    def later_change(config):
        nonlocal calls
        calls += 1
        result = _cut(config)
        if calls == 3:
            result["paths"]["database"]["logical_bytes"] = 101
        return result

    result = observer.run_observer(_config(tmp_path), required_cut=later_change)
    assert result["sections"]["required_capacity_topology"]["disposition"] == "COMPLETE"
    assert result["sections"]["post_optional_corroboration"]["disposition"] == "COMPLETE"
    assert result["sections"]["post_optional_corroboration"]["matches_closed_required_cut"] is False


def test_output_bound_preserves_required_and_all_section_dispositions(tmp_path):
    initial = _config(tmp_path)
    for index in range(20):
        (Path(initial.release_root) / f"file-{index:02}").write_bytes(b"x" * 20)
    config = replace(initial, output_bytes=3600)
    result = observer.run_observer(config, required_cut=_cut)
    assert "first_cut" in result["sections"]["required_capacity_topology"]
    assert set(result["sections"]) == {
        "required_capacity_topology", "release", "configuration", "unit_files",
        "processes", "sqlite_header", "post_optional_corroboration",
    }
    assert result["output"]["bytes"] <= 3600
    assert result["output"]["bytes"] == len(json.dumps(result, sort_keys=True, separators=(",", ":")).encode())


def test_impossibly_small_output_bound_emits_only_bounded_refusal(tmp_path):
    result = observer.run_observer(replace(_config(tmp_path), output_bytes=300), required_cut=_cut)
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    assert len(encoded) <= 300
    assert result["output"]["disposition"] == "REFUSED"
    assert result["output"]["reason"] == "OVERALL_OUTPUT_LIMIT_TOO_SMALL_FOR_REQUIRED_FACTS"


def test_source_has_no_sql_or_service_mutation_path():
    source = inspect.getsource(observer)
    assert "import sqlite3" not in source
    assert "subprocess" not in source
    assert "systemctl" not in source
    assert "os.remove" not in source
    assert "unlink(" not in source
    assert "rename(" not in source


def _proc_stat(pid: int, start_ticks: int) -> bytes:
    # Fields 3..21: state plus 18 numeric placeholders, then field 22 starttime.
    return f"{pid} (fixture worker) S {' '.join(['0'] * 18)} {start_ticks} 0\n".encode()


def test_process_census_binds_identity_and_descriptor_access(tmp_path):
    config = _config(tmp_path)
    proc = tmp_path / "proc"
    process = proc / "42"
    (process / "fd").mkdir(parents=True)
    (process / "fdinfo").mkdir()
    (process / "stat").write_bytes(_proc_stat(42, 9001))
    (process / "cmdline").write_bytes(b"python\x00worker.py\x00")
    (process / "maps").write_bytes(b"fixture maps")
    (process / "fd" / "7").symlink_to(config.database)
    (process / "fdinfo" / "7").write_text("flags:\t0100002\n")
    result = observer.observe_processes(config, proc)
    assert result["disposition"] == "COMPLETE"
    assert result["counters"]["descriptor_matches"] == 1
    assert result["records"][0]["start_ticks"] == "9001"
    assert result["records"][0]["database_descriptors"] == [{"descriptor": "7", "access": "READ_WRITE"}]
    assert "python" not in json.dumps(result)


def test_process_descriptor_bound_refuses_with_partial_counts(tmp_path):
    config = replace(_config(tmp_path), descriptor_entries=0)
    proc = tmp_path / "proc"
    process = proc / "42"
    (process / "fd").mkdir(parents=True)
    (process / "fdinfo").mkdir()
    (process / "stat").write_bytes(_proc_stat(42, 5))
    (process / "cmdline").write_bytes(b"")
    (process / "maps").write_bytes(b"")
    (process / "fd" / "7").symlink_to(config.database)
    result = observer.observe_processes(config, proc)
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "DESCRIPTOR_ENTRY_LIMIT_EXCEEDED"
    assert result["counters"]["descriptor_entries"] == 1
