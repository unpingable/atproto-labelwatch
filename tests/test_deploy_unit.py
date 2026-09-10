"""Deployment template qualifications that do not require systemd."""
from __future__ import annotations

from pathlib import Path


def test_main_unit_places_global_options_before_run_subcommand():
    unit = (Path(__file__).resolve().parents[1] / "deploy/labelwatch.service").read_text()
    logical = unit.replace("\\\n", " ")
    exec_start = next(line for line in logical.splitlines()
                      if line.startswith("ExecStart="))
    assert exec_start.index("--config") < exec_start.index(" run ")
    assert exec_start.index("--db") < exec_start.index(" run ")
    assert "--report-out /var/www/labelwatch" in exec_start


def test_api_unit_pins_external_frontdoor_receipts():
    unit = (Path(__file__).resolve().parents[1] / "deploy/labelwatch-api.service").read_text()
    assert (
        "Environment=LABELWATCH_AUDIT_RECEIPTS_DIR="
        "/var/lib/labelwatch/receipts/frontdoor"
    ) in unit
    assert "ReadOnlyPaths=/var/lib/labelwatch" in unit
