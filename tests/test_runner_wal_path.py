from pathlib import Path

import pytest

from labelwatch import runner


def test_wal_size_resolves_database_alias(tmp_path):
    real = tmp_path / 'state' / 'labels.db'
    real.parent.mkdir()
    real.touch()
    alias = tmp_path / 'labels.db'
    alias.symlink_to(real)
    with Path(str(real) + '-wal').open('wb') as stream:
        stream.truncate(81 * 1024 * 1024)
    assert not Path(str(alias) + '-wal').exists()
    assert runner._wal_size_mb(str(alias)) == 81
    assert runner._wal_size_mb(str(real)) == 81


def test_missing_wal_preserves_existing_zero_result(tmp_path):
    real = tmp_path / 'labels.db'
    real.touch()
    assert runner._wal_size_mb(str(real)) == 0


def test_unexpected_wal_stat_error_is_not_absent_wal(monkeypatch):
    def inaccessible(_):
        raise PermissionError('fixture')
    monkeypatch.setattr(runner.os.path, 'getsize', inaccessible)
    with pytest.raises(PermissionError):
        runner._wal_size_mb('labels.db')


def test_report_guard_rejects_resolved_wal_over_existing_80_mib(tmp_path, monkeypatch):
    real = tmp_path / 'state.db'
    real.touch()
    alias = tmp_path / 'alias.db'
    alias.symlink_to(real)
    with Path(str(real) + '-wal').open('wb') as stream:
        stream.truncate(81 * 1024 * 1024)
    opened = []
    monkeypatch.setattr(runner.db, 'connect', lambda *a, **k: opened.append(True))
    class StopFixture(Exception):
        pass
    def stop(interval):
        assert interval == 60
        raise StopFixture
    monkeypatch.setattr(runner.time, 'sleep', stop)
    with pytest.raises(StopFixture):
        runner._report_loop(str(alias), str(tmp_path / 'report'), 3600)
    assert opened == []
