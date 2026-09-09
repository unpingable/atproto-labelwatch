import sqlite3

import pytest

from labelwatch import db
from labelwatch.maintenance_manifest import logical_manifest, offline_application_verify, VerificationRefused

REV = 'a' * 40


def fixture(path):
    conn = db.connect(str(path))
    db.init_db(conn)
    conn.execute('CREATE TABLE maintenance_types (key TEXT PRIMARY KEY, value)')
    conn.executemany('INSERT INTO maintenance_types VALUES (?, ?)',
                     [('null', None), ('int', 7), ('real', 7.25),
                      ('text', 'a\0b'), ('blob', b'a\0b')])
    conn.commit()
    return conn


def test_full_rows_survive_real_backup_restore_and_vacuum(tmp_path):
    source = fixture(tmp_path / 'source.sqlite')
    expected = offline_application_verify(source, application_revision=REV)
    backup = sqlite3.connect(tmp_path / 'backup.sqlite')
    source.backup(backup)
    restored = sqlite3.connect(tmp_path / 'restored.sqlite')
    backup.backup(restored)
    restored.execute('VACUUM')
    assert offline_application_verify(restored, application_revision=REV) == expected
    restored.close()
    backup.close()
    source.close()


@pytest.mark.parametrize('value', ['different', b'a\0b', 7, None])
def test_same_counts_and_cursors_do_not_hide_changed_contents(tmp_path, value):
    source = fixture(tmp_path / 'source.sqlite')
    before = logical_manifest(source, application_revision=REV)
    source.execute("UPDATE maintenance_types SET value=? WHERE key='text'", (value,))
    source.commit()
    after = logical_manifest(source, application_revision=REV)
    assert [t['row_count'] for t in before['tables']] == [t['row_count'] for t in after['tables']]
    assert before != after
    source.close()


def test_wrong_schema_is_not_silently_migrated(tmp_path):
    source = fixture(tmp_path / 'source.sqlite')
    source.execute("UPDATE meta SET value='999' WHERE key='schema_version'")
    with pytest.raises(VerificationRefused, match='schema'):
        logical_manifest(source, application_revision=REV)
    source.close()


def test_virtual_table_is_not_silently_excluded(tmp_path):
    source = fixture(tmp_path / 'source.sqlite')
    source.execute('CREATE VIRTUAL TABLE searchable USING fts5(text)')
    with pytest.raises(VerificationRefused, match='virtual'):
        logical_manifest(source, application_revision=REV)
    source.close()


def test_numeric_storage_class_ties_have_stable_logical_order(tmp_path):
    source = fixture(tmp_path / 'source.sqlite')
    source.execute('CREATE TABLE ties (value)')
    source.executemany('INSERT INTO ties VALUES (?)', [(7,), (7.0,)])
    before = logical_manifest(source, application_revision=REV)
    source.execute('DELETE FROM ties')
    source.executemany('INSERT INTO ties VALUES (?)', [(7.0,), (7,)])
    assert logical_manifest(source, application_revision=REV) == before
    source.close()


def test_read_only_verification_does_not_migrate_or_write(tmp_path):
    path = tmp_path / 'source.sqlite'
    source = fixture(path)
    source.close()
    readonly = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    readonly.execute('PRAGMA query_only=ON')
    offline_application_verify(readonly, application_revision=REV)
    readonly.close()
