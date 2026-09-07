from datetime import datetime, timedelta, timezone

from labelwatch import db, ops_status


NOW = datetime(2026, 8, 25, 16, 0, tzinfo=timezone.utc)


def _status(tmp_path):
    path = tmp_path / "labelwatch.sqlite"
    conn = db.connect(str(path))
    db.init_db(conn)
    return path, conn


def _by_id(status):
    return {item["id"]: item["observation"] for item in status["concerns"]}


def _set(conn, key, value):
    db.set_meta(conn, key, value)
    conn.commit()


def test_clean_repository_exposes_required_absence(tmp_path):
    path = tmp_path / "missing.sqlite"
    status = ops_status.build_status(path, now=NOW)
    items = _by_id(status)
    assert status["schema"] == "project.ops.status/v1"
    assert all(item["required"] for item in status["concerns"])
    assert items["labelwatch.ingest.poll_coverage"]["local_state"] == "ABSENT"
    assert items["labelwatch.ingest.poll_coverage"]["observation_present"] is False
    assert items["labelwatch.persistence.sqlite_continuity"]["local_state"] == "ABSENT"


def test_successful_empty_poll_is_coverage_not_blindness(tmp_path):
    path, conn = _status(tmp_path)
    db.insert_ingest_outcome(
        conn, "did:plc:quiet", NOW.isoformat(), "attempt", "empty", 0,
        None, 4, None, None, "multi",
    )
    conn.commit()
    observed = _by_id(ops_status.build_status(path, now=NOW))["labelwatch.ingest.poll_coverage"]
    assert observed["local_state"] == "PRESENT"
    assert observed["facts"]["outcomes"] == {"empty": 1}


def test_observer_silence_is_not_source_silence(tmp_path):
    path, conn = _status(tmp_path)
    _set(conn, "ops:discovery:observed_at", NOW.isoformat())
    _set(conn, "ops:discovery:session_id", "session-new")
    _set(conn, "ops:discovery:connected", "false")
    observed = _by_id(ops_status.build_status(path, now=NOW))["labelwatch.discovery.stream_coverage"]
    assert observed["local_state"] == "DEGRADED"
    assert observed["facts"]["connected"] is False


def test_live_stream_does_not_hide_drain_shedding(tmp_path):
    path, conn = _status(tmp_path)
    for key, value in {
        "ops:discovery:observed_at": NOW.isoformat(),
        "ops:discovery:session_id": "session",
        "ops:discovery:connected": "true",
        "ops:discovery:queue_dropped": "3",
        "ops:discovery:queue_depth": "50",
        "ops:discovery:queue_capacity": "50",
    }.items():
        _set(conn, key, value)
    items = _by_id(ops_status.build_status(path, now=NOW))
    assert items["labelwatch.discovery.stream_coverage"]["local_state"] == "DEGRADED"
    assert items["labelwatch.discovery.drain"]["local_state"] == "DEGRADED"


def test_live_stream_does_not_hide_saturated_drain(tmp_path):
    path, conn = _status(tmp_path)
    for key, value in {
        "ops:discovery:observed_at": NOW.isoformat(),
        "ops:discovery:session_id": "session",
        "ops:discovery:connected": "true",
        "ops:discovery:queue_depth": "50",
        "ops:discovery:queue_capacity": "50",
    }.items():
        _set(conn, key, value)
    items = _by_id(ops_status.build_status(path, now=NOW))
    assert items["labelwatch.discovery.stream_coverage"]["local_state"] == "PRESENT"
    assert items["labelwatch.discovery.drain"]["local_state"] == "DEGRADED"
    assert "saturated" in items["labelwatch.discovery.drain"]["reason"]


def test_malformed_stream_input_is_visible_loss(tmp_path):
    path, conn = _status(tmp_path)
    for key, value in {
        "ops:discovery:observed_at": NOW.isoformat(),
        "ops:discovery:session_id": "session",
        "ops:discovery:connected": "true",
        "ops:discovery:parse_failures": "1",
    }.items():
        _set(conn, key, value)
    observed = _by_id(ops_status.build_status(path, now=NOW))["labelwatch.discovery.stream_coverage"]
    assert observed["local_state"] == "DEGRADED"
    assert observed["facts"]["parse_failures"] == 1


def test_historical_cursor_and_output_become_stale(tmp_path):
    path, conn = _status(tmp_path)
    old = (NOW - timedelta(hours=2)).isoformat()
    _set(conn, "ingest_cursor:did:plc:a", "cursor-a")
    _set(conn, "ops:cursor:observed_at:did:plc:a", old)
    _set(conn, "last_scan_ok_ts", old)
    _set(conn, "last_report_ok_ts", old)
    items = _by_id(ops_status.build_status(path, now=NOW))
    assert items["labelwatch.ingest.cursor_continuity"]["local_state"] == "STALE"
    assert items["labelwatch.processing.scan_freshness"]["local_state"] == "STALE"
    assert items["labelwatch.output.report_freshness"]["local_state"] == "STALE"


def test_cursor_observation_and_advancement_are_distinct(tmp_path):
    path, conn = _status(tmp_path)
    db.set_cursor(conn, "did:plc:a", "same")
    first_advance = db.get_meta(conn, "ops:cursor:advanced_at:did:plc:a")
    db.set_cursor(conn, "did:plc:a", "same")
    assert db.get_meta(conn, "ops:cursor:observed_at:did:plc:a")
    assert db.get_meta(conn, "ops:cursor:advanced_at:did:plc:a") == first_advance


def test_cursor_reobservation_does_not_manufacture_advancement(tmp_path):
    path, conn = _status(tmp_path)
    db.set_cursor(conn, "did:plc:a", "same")
    first_advance = db.get_meta(conn, "ops:cursor:advanced_at:did:plc:a")
    db.set_cursor(conn, "did:plc:a", "same")
    observed = _by_id(ops_status.build_status(path, now=datetime.now(timezone.utc)))[
        "labelwatch.ingest.cursor_continuity"
    ]
    assert observed["local_state"] == "PRESENT"
    assert observed["facts"]["advanced_at_by_source"]["did:plc:a"] == first_advance


def test_unmapped_required_concern_is_explicitly_missing(tmp_path):
    path, conn = _status(tmp_path)
    conn.close()
    manifest = tmp_path / "concerns.toml"
    manifest.write_text(
        'schema = "project.concerns/v1"\nproject = "labelwatch"\n'
        '[[concerns]]\nid = "labelwatch.test.uninstrumented"\n'
        'question = "labelwatch.question.uninstrumented/v1"\n'
        'profile = "labelwatch.profile.local_status/v1"\nrequired = true\n'
        'description = "An intentionally uninstrumented required concern."\n',
        encoding="utf-8",
    )
    item = ops_status.build_status(path, now=NOW, manifest_path=manifest)["concerns"][0]
    assert item["observation"]["local_state"] == "ABSENT"
    assert item["observation"]["observation_present"] is False


def test_sqlite_and_capacity_are_narrow_independent_probes(tmp_path, monkeypatch):
    path, conn = _status(tmp_path)
    conn.close()
    monkeypatch.setattr(
        ops_status.shutil,
        "disk_usage",
        lambda _path: type("Usage", (), {"total": 100, "used": 99, "free": 1})(),
    )
    items = _by_id(ops_status.build_status(path, now=NOW))
    assert items["labelwatch.persistence.sqlite_continuity"]["local_state"] == "PRESENT"
    assert items["labelwatch.persistence.volume_capacity"]["local_state"] == "DEGRADED"
    assert "freelist_count" in items["labelwatch.persistence.sqlite_freelist"]["facts"]


def test_freelist_pathology_requires_both_existing_gates():
    page_size = 4096
    assert ops_status._freelist_pathological(page_size, 2_000_000, 500_000)
    assert not ops_status._freelist_pathological(page_size, 10_000_000, 500_000)
    assert not ops_status._freelist_pathological(page_size, 100_000, 25_000)
