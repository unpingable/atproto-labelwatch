import os
from pathlib import Path
import sqlite3
import tempfile

import pytest

from labelwatch.maintenance_observation import observe_cleanup
from labelwatch.maintenance_step import digest
from test_maintenance_step import enrolled, invoke


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_independent_copy_observer_reads_real_changed_rows_and_missing_restore(tmp_path):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-copy-observe-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        invoke(tmp_path, step, 'stage')
        arguments = dict(backup=Path(step['backup']), restore=Path(step['restore']),
            operation=step['operation'], source=Path(step['source']), original=Path(step['original']),
            revision=step['revision'], expected_verification_sha256=digest(step['expected']),
            writer_identities={})
        positive = observe_cleanup(**arguments)
        assert positive['backup']['value']['verification_sha256'] == digest(step['expected'])
        assert positive['restore']['value']['verification_sha256'] == digest(step['expected'])
        # Equal row counts and valid integrity are not an unchanged-content claim.
        with sqlite3.connect(step['backup']) as conn:
            conn.execute("UPDATE maintenance_types SET value='different' WHERE key='text'")
        conn.close()
        changed = observe_cleanup(**arguments)
        assert changed['backup']['value']['integrity'] == 'ok'
        assert changed['backup']['value']['verification_sha256'] != digest(step['expected'])
        arguments['restore'] = Path(temporary) / 'not-present.sqlite'
        absent = observe_cleanup(**arguments)
        assert absent['restore'] == {'state': 'NOT_OBSERVABLE', 'value': None}
        assert {'slot': 'restore', 'reason': 'FileNotFoundError'} in absent['unknowns']
