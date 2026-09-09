import sqlite3

import pytest

from labelwatch.maintenance_artifacts import identity
from labelwatch.maintenance_diagnosis import diagnose, require_entry
from labelwatch.maintenance_manifest import VerificationRefused


def test_actual_page_statistics_and_closed_need(tmp_path):
    source = tmp_path / 'source.sqlite'
    connection = sqlite3.connect(source)
    connection.execute('CREATE TABLE rows(value BLOB)')
    connection.executemany('INSERT INTO rows VALUES (?)', [(b'x' * 4096,)] * 64)
    connection.commit()
    connection.execute('DELETE FROM rows')
    connection.commit()
    connection.close()
    before = identity(source)
    found = diagnose(source, minimum_freelist_pages=32, pressure_floor_bytes=0)
    require_entry(found, before)
    assert found['freelist_bloat'] == 'ESTABLISHED'
    assert found['filesystem_pressure'] == 'NOT_NEEDED'
    assert found['facts']['sqlite']['freelist_count'] >= 32
    assert found['facts']['wal']['state'] == found['facts']['shm']['state'] == 'OBSERVED_ABSENT'
    assert identity(source) == before
    absent_need = diagnose(source, minimum_freelist_pages=100000, pressure_floor_bytes=0)
    assert absent_need['entry_disposition'] == 'NOT_NEEDED'
    with pytest.raises(VerificationRefused):
        require_entry(absent_need, before)
    # Observed pressure alone does not establish that compaction is warranted.
    pressure_only = diagnose(source, minimum_freelist_pages=100000, pressure_floor_bytes=2**63)
    assert pressure_only['filesystem_pressure'] == 'ESTABLISHED'
    assert pressure_only['entry_disposition'] == 'NOT_NEEDED'
    sidecar = tmp_path / 'source.sqlite-wal'
    sidecar.write_bytes(b'unknown sidecar contents')
    unknown = diagnose(source, minimum_freelist_pages=32, pressure_floor_bytes=0)
    assert unknown['entry_disposition'] == 'NOT_OBSERVABLE'
    with pytest.raises(VerificationRefused):
        require_entry(unknown, before)
    assert sidecar.read_bytes() == b'unknown sidecar contents'
    assert identity(source) == before


def test_missing_main_is_observed_absent_not_qualified(tmp_path):
    result = diagnose(tmp_path / 'absent.sqlite', minimum_freelist_pages=1, pressure_floor_bytes=0)
    assert result['facts']['main']['state'] == 'OBSERVED_ABSENT'
    assert result['entry_disposition'] == 'NOT_OBSERVABLE'
    assert not (tmp_path / 'absent.sqlite').exists()
    with pytest.raises(VerificationRefused):
        require_entry(result, {})
