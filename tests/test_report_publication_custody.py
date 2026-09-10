from pathlib import Path

import pytest

from labelwatch import report


def _tree(path: Path, marker: str) -> None:
    path.mkdir()
    (path / "marker").write_text(marker, encoding="utf-8")


def _marker(path: Path) -> str:
    return (path / "marker").read_text(encoding="utf-8")


def test_generation_failure_removes_only_unchanged_staging(tmp_path, monkeypatch):
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
    assert not list(tmp_path.glob(".report-tmp-*"))


def test_generation_failure_retains_pathname_substitution(tmp_path, monkeypatch):
    out = tmp_path / "report"
    substituted = None

    def substitute(_conn, staging, *_args, **_kwargs):
        nonlocal substituted
        staging_path = Path(staging)
        staging_path.rename(staging_path.with_name("displaced"))
        staging_path.mkdir()
        substituted = staging_path
        (staging_path / "foreign").write_text("retain", encoding="utf-8")
        raise RuntimeError("generation stopped")

    monkeypatch.setattr(report, "_generate_report_into_staging", substitute)
    with pytest.raises(RuntimeError, match="generation stopped"):
        report.generate_report(object(), str(out))

    assert substituted is not None
    assert (substituted / "foreign").read_text(encoding="utf-8") == "retain"
    assert (tmp_path / "displaced").is_dir()


def test_success_keeps_immediate_rollback_and_discards_older_one(tmp_path):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    staging = tmp_path / ".report-tmp-fixed"
    _tree(out, "live")
    _tree(previous, "old-previous")
    _tree(staging, "new")
    publication = {"state": "STAGING"}

    report._commit_out_dir(str(staging), str(out), publication)

    assert publication["state"] == "COMPLETE"
    assert _marker(out) == "new"
    assert _marker(previous) == "live"
    assert not staging.exists()
    assert not list(tmp_path.glob(".report-recovery-*"))


@pytest.mark.parametrize("failed_replace", [1, 2, 3])
def test_rename_failures_retain_every_transition_object(
    tmp_path, monkeypatch, failed_replace
):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    staging = tmp_path / ".report-tmp-fixed"
    _tree(out, "live")
    _tree(previous, "old-previous")
    _tree(staging, "new")
    publication = {"state": "STAGING"}
    real_replace = report.os.replace
    calls = 0

    def fail_at(source, destination):
        nonlocal calls
        calls += 1
        if calls == failed_replace:
            raise OSError(f"replace {failed_replace} stopped")
        return real_replace(source, destination)

    monkeypatch.setattr(report.os, "replace", fail_at)
    with pytest.raises(OSError, match=f"replace {failed_replace} stopped"):
        report._commit_out_dir(str(staging), str(out), publication)

    assert publication["state"] == "COMMIT_STARTED"
    recovery = Path(publication["recovery"])
    if failed_replace == 1:
        assert _marker(out) == "live"
        assert _marker(previous) == "old-previous"
        assert _marker(staging) == "new"
        assert not recovery.exists()
    elif failed_replace == 2:
        assert _marker(out) == "live"
        assert not previous.exists()
        assert _marker(staging) == "new"
        assert _marker(recovery) == "old-previous"
    else:
        assert not out.exists()
        assert _marker(previous) == "live"
        assert _marker(staging) == "new"
        assert _marker(recovery) == "old-previous"


def test_cleanup_failure_after_live_replacement_retains_recovery(
    tmp_path, monkeypatch
):
    out = tmp_path / "report"
    previous = tmp_path / "report.prev"
    staging = tmp_path / ".report-tmp-fixed"
    _tree(out, "live")
    _tree(previous, "old-previous")
    _tree(staging, "new")
    publication = {"state": "STAGING"}

    def fail_cleanup(_path):
        raise OSError("cleanup stopped")

    monkeypatch.setattr(report.shutil, "rmtree", fail_cleanup)
    report._commit_out_dir(str(staging), str(out), publication)

    assert publication["state"] == "COMPLETE_WITH_RETAINED_RECOVERY"
    assert _marker(out) == "new"
    assert _marker(previous) == "live"
    recovery = Path(publication["recovery"])
    assert _marker(recovery) == "old-previous"


def test_symlink_destinations_are_refused_before_publication(tmp_path):
    real = tmp_path / "real"
    staging = tmp_path / ".report-tmp-fixed"
    out = tmp_path / "report"
    _tree(real, "live")
    _tree(staging, "new")
    out.symlink_to(real, target_is_directory=True)
    publication = {"state": "STAGING"}

    with pytest.raises(RuntimeError, match="non-directory publication path"):
        report._commit_out_dir(str(staging), str(out), publication)

    assert publication["state"] == "STAGING"
    assert out.is_symlink()
    assert _marker(real) == "live"
    assert _marker(staging) == "new"
