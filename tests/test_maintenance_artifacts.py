import sqlite3

import pytest

from labelwatch.maintenance_artifacts import (
    copy_restore_verify, compact_verified, identity, space_prerequisites, verify_closed,
)
from labelwatch.maintenance_manifest import offline_application_verify, VerificationRefused
from test_maintenance_manifest import fixture, REV


def source_cut(tmp_path):
    path = tmp_path / 'source.sqlite'
    conn = fixture(path)
    conn.execute('CREATE TABLE bloat (value BLOB)')
    conn.executemany('INSERT INTO bloat VALUES (?)', [(b'x' * 4096,)] * 64)
    conn.commit()
    conn.execute('DELETE FROM bloat')
    conn.commit()
    expected = offline_application_verify(conn, application_revision=REV)
    conn.close()
    return path, expected


def test_real_restore_and_compaction_preserve_source_and_all_rows(tmp_path):
    source, expected = source_cut(tmp_path)
    before = identity(source)
    result = copy_restore_verify(source, tmp_path / 'backup.sqlite', tmp_path / 'restored.sqlite',
                                 revision=REV, expected=expected)
    assert result['cleanup_authorized'] is False
    assert result['separate_filesystem'] is False
    stage = compact_verified(source, tmp_path / 'stage.sqlite', revision=REV, expected=expected)
    assert stage['staging']['identity']['bytes'] < before['bytes']
    assert identity(source) == before
    with pytest.raises(FileExistsError):
        compact_verified(source, tmp_path / 'stage.sqlite', revision=REV, expected=expected)


def test_copy_success_is_not_restore_application_acceptance(tmp_path):
    source, expected = source_cut(tmp_path)
    expected['rule_ids'] = ['not-the-application-result']
    with pytest.raises(VerificationRefused, match='verification differs'):
        copy_restore_verify(source, tmp_path / 'backup.sqlite', tmp_path / 'restore.sqlite',
                            revision=REV, expected=expected)
    assert (tmp_path / 'backup.sqlite').exists()
    assert not (tmp_path / 'restore.sqlite').exists()


def test_same_counts_changed_row_refuses_reopen(tmp_path):
    source, expected = source_cut(tmp_path)
    with sqlite3.connect(source) as conn:
        conn.execute("UPDATE maintenance_types SET value='changed' WHERE key='text'")
    with pytest.raises(VerificationRefused, match='verification differs'):
        verify_closed(source, revision=REV, expected=expected)


def test_backup_same_filesystem_refuses_before_mutation(tmp_path):
    source, _ = source_cut(tmp_path)
    with pytest.raises(VerificationRefused, match='separately identified'):
        space_prerequisites(source, tmp_path, operating_margin=4096)


def test_symlink_and_duplicate_destination_refuse(tmp_path):
    source, expected = source_cut(tmp_path)
    alias = tmp_path / 'alias.sqlite'
    alias.symlink_to(source)
    with pytest.raises(OSError):
        identity(alias)
    with pytest.raises(FileExistsError):
        copy_restore_verify(source, source, tmp_path / 'restore.sqlite', revision=REV, expected=expected)
