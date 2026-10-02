from pathlib import Path

import pytest

from labelwatch import report


def test_generate_report_removes_staging_tree_after_failure(monkeypatch, tmp_path):
    out_dir = tmp_path / "public"
    observed = {}

    def fail_report(conn, requested_out, **kwargs):
        staging = Path(kwargs["tmp_dir"])
        observed["staging"] = staging
        assert requested_out == out_dir
        (staging / "partial.json").write_text("partial", encoding="utf-8")
        raise RuntimeError("qualification failure")

    monkeypatch.setattr(report, "_generate_report", fail_report)

    with pytest.raises(RuntimeError, match="qualification failure"):
        report.generate_report(object(), out_dir)

    assert not observed["staging"].exists()
    assert list(tmp_path.glob(".report-tmp-*")) == []


def test_generate_report_releases_staging_tree_after_commit(monkeypatch, tmp_path):
    out_dir = tmp_path / "public"
    observed = {}

    def commit_report(conn, requested_out, **kwargs):
        staging = Path(kwargs["tmp_dir"])
        observed["staging"] = staging
        (staging / "index.html").write_text("complete", encoding="utf-8")
        report._commit_out_dir(str(staging), str(requested_out))

    monkeypatch.setattr(report, "_generate_report", commit_report)

    report.generate_report(object(), out_dir)

    assert not observed["staging"].exists()
    assert (out_dir / "index.html").read_text(encoding="utf-8") == "complete"
    assert list(tmp_path.glob(".report-tmp-*")) == []
