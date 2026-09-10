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


def _cut(config):
    identity = Path(config.database).stat() if config is not None else None
    return {
        "observed_monotonic_ns": 1,
        "paths": {"database": {"device": identity.st_dev if identity else 7, "inode": identity.st_ino if identity else 8, "logical_bytes": 100}},
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


def test_content_probe_never_reads_past_aggregate_ceiling(tmp_path):
    path = tmp_path / "larger"
    path.write_bytes(b"abcd")
    budget = observer.ReadBudget(3)
    with pytest.raises(observer.SectionRefusal) as refused:
        budget.read(path, 10)
    assert refused.value.reason == "CONTENT_BOUND_REACHED_WITHOUT_EOF"
    assert budget.consumed == 3


def test_regular_file_oversize_refuses_before_content_open(tmp_path, monkeypatch):
    path = tmp_path / "oversize"
    path.write_bytes(b"abcd")
    monkeypatch.setattr(observer.os, "open", lambda *args: pytest.fail("content open crossed pre-read size refusal"))
    result = observer.observe_files([path], _limits(file_bytes=3))
    assert result["disposition"] == "REFUSED"
    assert result["counters"]["content_bytes_read"] == 0


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


def test_record_identity_is_from_the_file_that_was_hashed(tmp_path, monkeypatch):
    path = tmp_path / "subject"
    replacement = tmp_path / "replacement"
    path.write_bytes(b"old")
    replacement.write_bytes(b"new bytes")
    replacement_inode = replacement.stat().st_ino
    real_hash = observer._sha256_file

    def replace_before_hash(*args, **kwargs):
        replacement.replace(path)
        return real_hash(*args, **kwargs)

    monkeypatch.setattr(observer, "_sha256_file", replace_before_hash)
    result = observer.observe_files([path], _limits())
    assert result["disposition"] == "COMPLETE"
    assert result["records"][0]["inode"] == replacement_inode
    assert result["records"][0]["logical_bytes"] == len(b"new bytes")


def test_refusal_content_count_is_added_to_prior_files(tmp_path, monkeypatch):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.write_bytes(b"12345")
    second.write_bytes(b"123")
    real_hash = observer._sha256_file

    def fail_second(path, *args, **kwargs):
        if path == second:
            raise observer.SectionRefusal("CONTENT_MUTATED_DURING_READ", {"content_bytes_read": 3})
        return real_hash(path, *args, **kwargs)

    monkeypatch.setattr(observer, "_sha256_file", fail_second)
    result = observer.observe_files([first, second], _limits())
    assert result["disposition"] == "REFUSED"
    assert result["counters"]["content_bytes_read"] == 8


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


def test_second_required_cut_failure_retains_first_and_partial_second(tmp_path):
    calls = 0
    config = _config(tmp_path)

    def partial(config):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise observer.SectionRefusal("SECOND_CUT_UNAVAILABLE", {"paths": {"database": {"state": "PRESENT"}}})
        return _cut(config)

    result = observer.observe_required(config, partial)
    assert result["disposition"] == "NOT_OBSERVED"
    assert result["first_cut"] == _cut(config)
    assert result["second_cut_partial"]["paths"]["database"]["state"] == "PRESENT"


def test_optional_oserror_is_section_local_and_required_cut_survives(tmp_path, monkeypatch):
    monkeypatch.setattr(observer, "observe_release", lambda config: (_ for _ in ()).throw(OSError("fixture")))
    result = observer.run_observer(_config(tmp_path), required_cut=_cut)
    assert result["sections"]["required_capacity_topology"]["disposition"] == "COMPLETE"
    assert result["sections"]["release"] == {"disposition": "NOT_OBSERVED", "reason": "OSError"}
    assert result["sections"]["sqlite_header"]["disposition"] == "COMPLETE"


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
    assert result["output"]["bytes"] == len(json.dumps(result, sort_keys=True, separators=(",", ":")).encode()) + 1


def test_impossibly_small_output_bound_emits_only_bounded_refusal(tmp_path):
    result = observer.run_observer(replace(_config(tmp_path), output_bytes=300), required_cut=_cut)
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    assert len(encoded) + 1 <= 300
    assert result["output"]["disposition"] == "REFUSED"
    assert result["output"]["reason"] == "OVERALL_OUTPUT_LIMIT_TOO_SMALL_FOR_REQUIRED_FACTS"


def test_unrepresentable_output_limit_is_rejected_before_observation(tmp_path):
    with pytest.raises(ValueError, match="retain a disposition"):
        replace(_config(tmp_path), output_bytes=1)


def test_output_metadata_itself_is_included_in_limit():
    record = {"schema": "x", "sections": {"required_capacity_topology": {"disposition": "COMPLETE", "padding": "x" * 100}}}
    without_metadata = len(json.dumps(record, sort_keys=True, separators=(",", ":")).encode())
    result = observer._bound_output(record, without_metadata + 1)
    assert len(json.dumps(result, sort_keys=True, separators=(",", ":")).encode()) + 1 <= without_metadata + 1


def test_tree_enumeration_stops_after_one_bounded_probe(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    for index in range(100):
        (root / str(index)).write_bytes(b"")
    observed = list(observer._tree_paths(root, 2))
    assert len(observed) == 3  # two admitted entries plus one explicit refusal probe


def test_release_refusal_accounts_for_probe_metadata(tmp_path):
    config = _config(tmp_path)
    root = Path(config.release_root)
    (root / "first").write_bytes(b"")
    (root / "second-probe").write_bytes(b"")
    result = observer.observe_release(replace(config, release=replace(config.release, entries=2)))
    assert result["reason"] == "ENTRY_LIMIT_EXCEEDED"
    assert result["counters"]["entry_probes"] == 1
    assert result["counters"]["probe_path_bytes"] > 0


def test_directory_symlink_substitution_is_not_followed(tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "content").write_text("must not be traversed")
    moved = tmp_path / "moved"
    real_open = observer.os.open
    substituted = False

    def replace_before_directory_open(path, flags):
        nonlocal substituted
        if Path(path) == root and not substituted:
            substituted = True
            root.rename(moved)
            root.symlink_to(outside, target_is_directory=True)
        return real_open(path, flags)

    monkeypatch.setattr(observer.os, "open", replace_before_directory_open)
    result = observer.observe_files(observer._tree_paths(root, 10), _limits(), relative_to=root)
    assert result["disposition"] == "NOT_OBSERVED"
    assert all(record.get("path") != "content" for record in result["records"])


def test_metadata_only_records_enforce_section_output_limit(tmp_path):
    paths = []
    for index in range(10):
        path = tmp_path / (("long-directory-name-" * 8) + str(index))
        path.mkdir()
        paths.append(path)
    result = observer.observe_files(paths, _limits(output_bytes=512))
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "SECTION_OUTPUT_LIMIT_EXCEEDED"
    assert len(json.dumps(result, separators=(",", ":")).encode()) <= 512


def test_final_deadline_check_catches_expiry_after_last_read(tmp_path, monkeypatch):
    path = tmp_path / "subject"
    path.write_bytes(b"x")
    ticks = iter((0, 0, 0, 2_000_000_000))
    monkeypatch.setattr(observer.time, "monotonic_ns", lambda: next(ticks))
    result = observer.observe_files([path], _limits(seconds=1))
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "TIME_LIMIT_EXCEEDED"
    assert result["counters"]["content_bytes_read"] == 1


def test_corroboration_oserror_is_section_local(tmp_path):
    config = _config(tmp_path)
    calls = 0

    def cut(subject):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("fixture")
        return _cut(subject)

    result = observer.run_observer(config, required_cut=cut)
    assert result["sections"]["required_capacity_topology"]["disposition"] == "COMPLETE"
    assert result["sections"]["post_optional_corroboration"] == {"disposition": "NOT_OBSERVED", "reason": "OSError"}


def test_sqlite_header_refuses_required_identity_mismatch(tmp_path):
    config = _config(tmp_path)
    actual = Path(config.database).stat()
    result = observer.observe_sqlite_header(config, (actual.st_dev, actual.st_ino + 1))
    assert result["disposition"] == "REFUSED"
    assert result["reason"] == "SQLITE_HEADER_REQUIRED_IDENTITY_MISMATCH"


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


def test_process_unavailable_after_stat_retains_read_count(tmp_path):
    config = _config(tmp_path)
    proc = tmp_path / "proc"
    process = proc / "42"
    process.mkdir(parents=True)
    stat_bytes = _proc_stat(42, 5)
    (process / "stat").write_bytes(stat_bytes)
    result = observer.observe_processes(config, proc)
    assert result["disposition"] == "COMPLETE"
    assert result["counters"]["unavailable_processes"] == 1
    assert result["counters"]["content_bytes_read"] == len(stat_bytes)
