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
