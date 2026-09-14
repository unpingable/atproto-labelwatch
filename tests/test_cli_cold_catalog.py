from types import SimpleNamespace

from labelwatch import cli, db, frontdoor_archive, scan


def test_scan_cli_passes_configured_cold_catalog_to_both_consumers(
    monkeypatch, tmp_path
):
    configured = object()
    connection = object()
    observed = []
    monkeypatch.setenv(
        "LABELWATCH_FRONTDOOR_COLD_CATALOG", str(tmp_path / "catalog.json")
    )
    monkeypatch.setattr(cli, "load_config", lambda _path: SimpleNamespace(db_path="db"))
    monkeypatch.setattr(db, "connect", lambda _path: connection)
    monkeypatch.setattr(db, "init_db", lambda _conn: None)
    monkeypatch.setattr(cli, "_resolve_now", lambda _conn, _now: "fixed-now")
    monkeypatch.setattr(frontdoor_archive, "load_catalog", lambda _path: configured)
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
    assert all(item[4] is configured for item in observed)
