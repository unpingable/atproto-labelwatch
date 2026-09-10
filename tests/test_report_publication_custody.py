from contextlib import contextmanager
from pathlib import Path

import pytest

from labelwatch import report


def _tree(path: Path, marker: str) -> None:
    path.mkdir()
    (path / "marker").write_text(marker, encoding="utf-8")


def _marker(path: Path) -> str:
    return (path / "marker").read_text(encoding="utf-8")


@contextmanager
def _publication(out: Path, staging: Path):
    lock_fd, lock_path, lock_identity = report._publication_lock(str(out))
    value = {
        "state": "STAGING",
        "lock_fd": lock_fd,
        "lock_path": lock_path,
        "lock_identity": lock_identity,
        "stage_identity": report._path_identity(str(staging)),
    }
    try:
        yield value
    finally:
        report.os.close(lock_fd)


def test_generation_failure_clears_only_unchanged_staging_bytes(tmp_path, monkeypatch):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    _tree(out, "live")
    _tree(previous, "previous")

    def fail(_conn, staging, *_args, **_kwargs):
        Path(staging, "partial").write_text("partial", encoding="utf-8")
        raise RuntimeError("generation stopped")

    monkeypatch.setattr(report, "_generate_report_into_staging", fail)
    with pytest.raises(RuntimeError, match="generation stopped"):
        report.generate_report(object(), str(out))
    assert _marker(out) == "live"
    assert _marker(previous) == "previous"
    stages = list(tmp_path.glob(".report-tmp-*"))
    assert len(stages) == 1 and list(stages[0].iterdir()) == []


def test_generation_failure_does_not_clear_pathname_substitution(tmp_path, monkeypatch):
    out = tmp_path / "report"

    def substitute(_conn, staging, *_args, **_kwargs):
        staging_path = Path(staging)
        staging_path.rename(staging_path.with_name("displaced"))
        _tree(staging_path, "substitute")
        raise RuntimeError("generation stopped")

    monkeypatch.setattr(report, "_generate_report_into_staging", substitute)
    with pytest.raises(RuntimeError, match="generation stopped"):
        report.generate_report(object(), str(out))
    assert _marker(next(tmp_path.glob(".report-tmp-*"))) == "substitute"
    assert (tmp_path / "displaced").is_dir()


def test_stage_creation_never_adopts_uuid_collision(tmp_path, monkeypatch):
    out = tmp_path / "report"
    collision = tmp_path / ".report-tmp-fixed"
    _tree(collision, "preexisting")
    monkeypatch.setattr(report.uuid, "uuid4", lambda: type("U", (), {"hex": "fixed"})())
    with pytest.raises(FileExistsError):
        report.generate_report(object(), str(out))
    assert _marker(collision) == "preexisting"


def test_success_keeps_immediate_rollback_and_clears_only_older_bytes(tmp_path):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    staging = tmp_path / ".report-tmp-fixed"
    _tree(out, "live")
    _tree(previous, "old-previous")
    _tree(staging, "new")
    with _publication(out, staging) as publication:
        report._commit_out_dir(str(staging), str(out), publication)
    assert publication["state"] == "COMPLETE"
    assert _marker(out) == "new"
    assert _marker(previous) == "live"
    recovery = Path(publication["recovery"])
    assert recovery.is_dir() and list(recovery.iterdir()) == []


@pytest.mark.parametrize("failed_replace", [1, 2, 3])
def test_rename_failures_retain_every_transition_object(tmp_path, monkeypatch, failed_replace):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    staging = tmp_path / ".report-tmp-fixed"
    _tree(out, "live")
    _tree(previous, "old-previous")
    _tree(staging, "new")
    real_replace = report.os.replace
    calls = 0

    def fail_at(source, destination):
        nonlocal calls
        calls += 1
        if calls == failed_replace:
            raise OSError(f"replace {failed_replace} stopped")
        return real_replace(source, destination)

    monkeypatch.setattr(report.os, "replace", fail_at)
    with _publication(out, staging) as publication:
        with pytest.raises(OSError, match=f"replace {failed_replace} stopped"):
            report._commit_out_dir(str(staging), str(out), publication)
    assert publication["state"] == "COMMIT_STARTED"
    recovery = Path(publication["recovery"])
    if failed_replace == 1:
        assert (_marker(out), _marker(previous), _marker(staging)) == ("live", "old-previous", "new")
    elif failed_replace == 2:
        assert (_marker(out), _marker(recovery), _marker(staging)) == ("live", "old-previous", "new")
    else:
        assert (_marker(previous), _marker(recovery), _marker(staging)) == ("live", "old-previous", "new")


def test_substituted_stage_is_not_accepted_as_live(tmp_path):
    out = tmp_path / "report"
    staging = tmp_path / ".report-tmp-fixed"
    displaced = tmp_path / "displaced-stage"
    _tree(out, "live")
    _tree(staging, "generated")
    with _publication(out, staging) as publication:
        staging.rename(displaced)
        _tree(staging, "substitute")
        with pytest.raises(RuntimeError, match="staging identity changed"):
            report._commit_out_dir(str(staging), str(out), publication)
    assert (_marker(out), _marker(staging), _marker(displaced)) == ("live", "substitute", "generated")


def test_live_symlink_inserted_during_rename_is_reversed(tmp_path, monkeypatch):
    out = tmp_path / "report"
    staging = tmp_path / ".report-tmp-fixed"
    displaced = tmp_path / "displaced-live"
    target = tmp_path / "target"
    _tree(out, "live")
    _tree(staging, "new")
    _tree(target, "target")
    original = report.os.replace
    inserted = False

    def insert(source, destination):
        nonlocal inserted
        if Path(source) == out and not inserted:
            inserted = True
            out.rename(displaced)
            out.symlink_to(target, target_is_directory=True)
        return original(source, destination)

    monkeypatch.setattr(report.os, "replace", insert)
    with _publication(out, staging) as publication:
        with pytest.raises(RuntimeError, match="changed during rename"):
            report._commit_out_dir(str(staging), str(out), publication)
    assert out.is_symlink()
    assert _marker(displaced) == "live"
    assert _marker(staging) == "new"


def test_recovery_path_substitution_is_never_cleared(tmp_path, monkeypatch):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    staging = tmp_path / ".report-tmp-fixed"
    _tree(out, "live")
    _tree(previous, "old-previous")
    _tree(staging, "new")
    original = report._clear_owned_tree

    def substitute(path, identity):
        recovery = Path(path)
        recovery.rename(tmp_path / "displaced-recovery")
        _tree(recovery, "substitute")
        return original(path, identity)

    monkeypatch.setattr(report, "_clear_owned_tree", substitute)
    with _publication(out, staging) as publication:
        report._commit_out_dir(str(staging), str(out), publication)
    assert publication["state"] == "COMPLETE_WITH_RETAINED_RECOVERY"
    assert _marker(Path(publication["recovery"])) == "substitute"
    assert _marker(tmp_path / "displaced-recovery") == "old-previous"
    assert (_marker(out), _marker(previous)) == ("new", "live")


def test_second_publisher_is_refused_while_first_holds_lock(tmp_path):
    out = tmp_path / "report"
    first, _path, _identity = report._publication_lock(str(out))
    try:
        with pytest.raises(RuntimeError, match="already active"):
            report._publication_lock(str(out))
    finally:
        report.os.close(first)


def test_interrupted_layout_is_refused_without_touching_rollback(tmp_path):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    _tree(previous, "immediate-rollback")
    with pytest.raises(RuntimeError, match="ambiguous interrupted publication layout"):
        report.generate_report(object(), str(out))
    assert _marker(previous) == "immediate-rollback"
    assert not out.exists()


def test_symlink_destinations_are_refused_before_publication(tmp_path):
    real = tmp_path / "real"
    staging = tmp_path / ".report-tmp-fixed"
    out = tmp_path / "report"
    _tree(real, "live")
    _tree(staging, "new")
    out.symlink_to(real, target_is_directory=True)
    with _publication(out, staging) as publication:
        with pytest.raises(RuntimeError, match="non-directory publication path"):
            report._commit_out_dir(str(staging), str(out), publication)
    assert publication["state"] == "STAGING"
    assert out.is_symlink()
    assert _marker(real) == "live"
    assert _marker(staging) == "new"
