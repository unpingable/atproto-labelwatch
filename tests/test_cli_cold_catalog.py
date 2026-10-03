from types import SimpleNamespace

import pytest

from labelwatch import cli, db, retention, scan


def _run(monkeypatch, tmp_path, coverage_status):
    configured = object()
    connection = object()
    observed = []
    monkeypatch.setenv(
        "LABELWATCH_FRONTDOOR_COLD_CATALOG", str(tmp_path / "catalog.json")
    )
    custody = retention.CatalogCustody(str(tmp_path / "catalog.json"), "loaded",
                                       None, configured, (0,))
    monkeypatch.setattr(cli, "load_config", lambda _path: SimpleNamespace(db_path="db"))
    monkeypatch.setattr(db, "connect", lambda _path: connection)
    monkeypatch.setattr(db, "init_db", lambda _conn: None)
    monkeypatch.setattr(cli, "_resolve_now", lambda _conn, _now: "fixed-now")
    monkeypatch.setattr(retention, "load_catalog_custody", lambda: custody)
    monkeypatch.setattr(
        retention, "catalog_coverage",
        lambda conn, catalog: {"status": coverage_status, "reason": None},
    )
    monkeypatch.setattr(
        scan,
        "run_scan",
        lambda conn, cfg, *, now, cold_catalog: observed.append(
            ("scan", conn, cfg, now, cold_catalog)
        ) or 7,
    )
    monkeypatch.setattr(
        scan,
        "run_derive",
        lambda conn, cfg, *, now, cold_catalog: observed.append(
            ("derive", conn, cfg, now, cold_catalog)
        ),
    )

    cli.cmd_scan(SimpleNamespace(
        config=None, db_path=None, window_minutes=None,
        baseline_hours=None, now="max",
    ))

    assert [item[0] for item in observed] == ["scan", "derive"]
    assert all(item[1] is connection for item in observed)
    assert all(item[3] == "fixed-now" for item in observed)
    return configured, [item[4] for item in observed]


def test_scan_cli_passes_complete_cold_catalog_to_both_consumers(monkeypatch, tmp_path):
    configured, passed = _run(monkeypatch, tmp_path, "complete")
    assert all(item is configured for item in passed)


@pytest.mark.parametrize("status", ["gap", "stale", "absent", "tampered"])
def test_scan_cli_withholds_incomplete_cold_catalog(monkeypatch, tmp_path, status):
    _, passed = _run(monkeypatch, tmp_path, status)
    assert passed == [None, None]


@pytest.mark.parametrize("coverage_case", ["complete", "absent", "gap", "stale", "tampered", "live_only", "quarantined", "other_quarantined"])
def test_labeler_report_lifetime_total_or_refusal(monkeypatch, tmp_path, capsys, coverage_case):
    import json
    from _cold_catalog_fixture import build_test_catalog

    labeler = "did:plc:labeler"
    path = tmp_path / "live.db"
    conn = db.connect(str(path))
    db.init_db(conn)
    # An owned cold duplicate still present live must not be counted twice.
    rows = [(labeler, "did:plc:subject", "did:plc:subject", None, "warn", 0,
             None, None, ts, f"event-{i}", "did:plc:subject")
            for i, ts in enumerate(["2026-08-11T01:00:00Z", "2026-08-20T01:00:00Z"])]
    db.insert_label_events(conn, rows)
    conn.execute("INSERT OR IGNORE INTO labelers(labeler_did,first_seen,last_seen) VALUES(?,?,?)",
                 (labeler, "2026-08-11T01:00:00Z", "2026-08-20T01:00:00Z"))
    if coverage_case != "live_only":
        retention.set_retention_floor(conn, "2026-08-14T00:00:00Z", "archive://test")
        db.set_meta(conn, retention.RETENTION_HISTORY_START_KEY,
                    "2026-08-09T00:00:00Z" if coverage_case == "gap" else "2026-08-10T00:00:00Z")
    conn.commit()
    if coverage_case in {"quarantined", "other_quarantined"}:
        late = list(rows[0])
        late[0] = labeler if coverage_case == "quarantined" else "did:plc:other"
        late[9] = "late-new-event"
        from labelwatch import ingest
        assert ingest.store_label_events(conn, [tuple(late)]) == 0
        assert conn.execute("SELECT COUNT(*) FROM quarantined_events WHERE labeler_did=?", (late[0],)).fetchone()[0] == 1
        conn.commit()
    conn.close()
    monkeypatch.delenv("LABELWATCH_RETENTION_FLOOR", raising=False)
    monkeypatch.delenv("LABELWATCH_FRONTDOOR_COLD_CATALOG", raising=False)
    if coverage_case not in {"absent", "live_only"}:
        manifest = build_test_catalog(tmp_path / "cold", [
            (labeler, "did:plc:subject", "warn", 0, "2026-08-11T01:00:00Z", "did:plc:subject"),
            (labeler, "https://example.invalid", "warn", 0, "2026-08-12T01:00:00Z", None),
        ], start_day="2026-08-10", end_day_exclusive="2026-08-13" if coverage_case == "stale" else "2026-08-14", watermark=1000)
        monkeypatch.setenv("LABELWATCH_FRONTDOOR_COLD_CATALOG", str(manifest))
        if coverage_case == "tampered":
            (manifest.parent / "catalog.sqlite").write_bytes(b"changed")
    monkeypatch.setattr(cli, "load_config", lambda _: SimpleNamespace(db_path=str(path)))
    args = SimpleNamespace(config=None, db_path=None, format="json", now=None,
                           alerts=False, labeler=labeler)
    if coverage_case in {"complete", "live_only", "other_quarantined"}:
        cli.cmd_report(args)
        output = json.loads(capsys.readouterr().out)
        assert output["total_events"] == (2 if coverage_case == "live_only" else 3)
        if coverage_case != "live_only":
            assert output["cold_history"]["status"] == "catalog"
    else:
        with pytest.raises(SystemExit) as refusal:
            cli.cmd_report(args)
        assert refusal.value.code == 2
        output = json.loads(capsys.readouterr().out)
        assert output["error"] == "cold_history_unavailable"
        assert "total_events" not in output
