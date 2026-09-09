from pathlib import Path

from labelwatch.maintenance_observation import observe
from labelwatch.maintenance_step import digest
from test_maintenance_artifacts import source_cut
from test_maintenance_manifest import REV


def test_observer_reads_actual_database_not_an_enactment_claim(tmp_path):
    source, expected = source_cut(tmp_path)
    result = observe(operation='fixture', source=source, original=tmp_path / 'absent.sqlite',
                     revision=REV, expected_verification_sha256=digest(expected), writer_identities={})
    assert result['database']['value']['matches_declared_cut'] is True
    assert result['filesystem']['value']['free_bytes'] >= 0
    assert result['original_presence']['value']['present'] is False
    assert result['writers']['state'] == 'NOT_OBSERVABLE'
    # A local completed-record assertion does not substitute for missing source.
    (tmp_path / 'enactment.json').write_text('{"success":true}')
    source.unlink()
    unknown = observe(operation='fixture', source=source, original=tmp_path / 'absent.sqlite',
                      revision=REV, expected_verification_sha256=digest(expected), writer_identities={})
    assert unknown['database']['state'] == 'NOT_OBSERVABLE'
    assert unknown['database']['value'] is None
