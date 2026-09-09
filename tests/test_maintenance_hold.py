import hashlib
import json

import pytest

from labelwatch import db
from labelwatch.maintenance_hold import active_hold, paths, MaintenanceHeld


def install(path):
    hold, release = paths(str(path))
    hold.parent.mkdir()
    value = {'schema': 'labelwatch.maintenance-hold/v1', 'operation': 'fixture-operation',
             'database': str(path), 'manifest_sha256': 'a' * 64, 'application_revision': 'a' * 40}
    hold.write_text(json.dumps(value))
    return value, release


def test_persistent_hold_refuses_writer_and_wrong_release(tmp_path):
    path = tmp_path / 'database.sqlite'
    conn = db.connect(str(path))
    db.init_db(conn)
    conn.close()
    hold, release = install(path)
    for _ in range(2):
        with pytest.raises(MaintenanceHeld):
            db.connect(str(path))
    release.write_text('{}')
    with pytest.raises(MaintenanceHeld, match='exact hold'):
        active_hold(str(path))
    release.write_text(json.dumps({'schema': 'labelwatch.maintenance-release/v1',
        'operation': hold['operation'], 'hold_sha256': hashlib.sha256(
            json.dumps(hold, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        'release_receipt': 'fixture-external-custody-not-authenticated-here'}))
    assert active_hold(str(path)) is None
    db.connect(str(path)).close()


def test_interrupted_hold_installation_stays_closed(tmp_path):
    path = tmp_path / 'database.sqlite'
    hold, _ = paths(str(path))
    hold.parent.mkdir()
    with pytest.raises(MaintenanceHeld, match='without complete hold'):
        db.connect(str(path))
    assert not path.exists()
