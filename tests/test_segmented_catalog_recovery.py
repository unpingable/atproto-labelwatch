"""Recovery and coverage controls for the retained indexed catalog candidate."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools/segmented_qualification'))
from storage import Store, connect
from qualify import event
from tier import VerifiedCatalog, TierSession


@pytest.fixture
def archived(tmp_path):
    # --basetemp must be within the existing campaign-owned runtime tree.
    store = Store.create(tmp_path / 'store')
    store.ingest([event()], 'fixture-source', '1')
    store.rotate('2026-10-05')
    archive = tmp_path / 'archive'
    archive.mkdir()
    store.archive('2026-09-28', archive)
    store.retire('2026-09-28')
    return store, archive, VerifiedCatalog(archive, store=store)


def count(store, archive, catalog):
    with TierSession(store, archive, catalog=catalog) as reader:
        return reader.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]


def test_missing_derived_entry_refuses_then_explicit_readmission_repairs(archived):
    store, archive, catalog = archived
    with catalog.connection() as c:
        c.execute('DELETE FROM entries')
    with pytest.raises(RuntimeError, match='coverage unavailable'):
        count(store, archive, catalog)
    with connect(store.state) as authority:
        assert catalog.admit(authority, '2026-09-28')['payload_hash_calls'] == 1
    assert count(store, archive, catalog) == 1


@pytest.mark.parametrize('phase', ['before_catalog_commit', 'after_catalog_commit'])
def test_catalog_commit_interruption_retry(archived, phase):
    store, archive, catalog = archived
    with catalog.connection() as c:
        c.execute('DELETE FROM entries')
    def interrupted(point):
        if point == phase:
            raise RuntimeError('qualification interruption')
    with connect(store.state) as authority:
        with pytest.raises(RuntimeError, match='qualification interruption'):
            catalog.admit(authority, '2026-09-28', death=interrupted)
    restarted = VerifiedCatalog(archive, store=store)
    assert (restarted.lookup('2026-09-28') is not None) == (phase == 'after_catalog_commit')
    with connect(store.state) as authority:
        restarted.admit(authority, '2026-09-28')
    assert count(store, archive, restarted) == 1


def test_corrupt_derived_binding_never_changes_custody(archived):
    store, archive, catalog = archived
    with catalog.connection() as c:
        c.execute("UPDATE entries SET parquet_sha256='wrong'")
    with pytest.raises(RuntimeError, match='binding'):
        count(store, archive, catalog)
    with connect(store.state) as authority:
        with pytest.raises(RuntimeError, match='conflicts'):
            catalog.admit(authority, '2026-09-28')


def test_orphan_publication_is_not_history_authority(archived):
    store, archive, catalog = archived
    (archive / 'orphan.receipt.json').write_text(json.dumps({'identity': 'orphan'}))
    assert count(store, archive, catalog) == 1


def test_half_open_range_and_missing_payload(archived):
    store, archive, catalog = archived
    assert len(catalog.files(start='2026-09-28T12:00:00Z', end='2026-09-29T00:00:00Z')) == 1
    assert catalog.files(end='2026-09-28T12:00:00Z') == []
    (archive / '2026-09-28.parquet').rename(archive / 'retained-negative-control.parquet')
    with pytest.raises((RuntimeError, FileNotFoundError)):
        count(store, archive, catalog)
