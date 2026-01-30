from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
import sys
from typing import Optional

from . import db, ingest, scan
from . import report as report_mod
from . import runner
from .config import load_config
from .utils import format_ts, parse_ts


def _parse_duration(value: str) -> timedelta:
    value = value.strip().lower()
    if value.endswith("h"):
        return timedelta(hours=float(value[:-1]))
    if value.endswith("d"):
        return timedelta(days=float(value[:-1]))
    if value.endswith("m"):
        return timedelta(minutes=float(value[:-1]))
    raise ValueError("duration must end with m, h, or d")


def cmd_ingest(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    if args.fixture:
        total = ingest.ingest_from_fixture(conn, args.fixture)
    else:
        if not cfg.labeler_dids:
            raise SystemExit("labeler_dids must be configured for ingest")
        total = ingest.ingest_from_service(conn, cfg, limit=args.limit, max_pages=args.pages)
    print(json.dumps({"ingested": total}))


def _resolve_now(conn, now_arg: str | None, table: str = "label_events") -> datetime | None:
    if not now_arg:
        return None
    if now_arg == "max":
        if table not in {"label_events", "alerts"}:
            raise SystemExit("invalid table for --now max resolution")
        row = conn.execute(f"SELECT MAX(ts) AS ts FROM {table}").fetchone()
        if not row or not row["ts"]:
            raise SystemExit(f"no {table} found to resolve --now max")
        dt = parse_ts(row["ts"])
    else:
        dt = parse_ts(now_arg)
    if dt.tzinfo is None:
        print("warning: --now is naive; assuming UTC", file=sys.stderr)
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def cmd_scan(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    if args.window_minutes is not None:
        cfg.window_minutes = args.window_minutes
    if args.baseline_hours is not None:
        cfg.baseline_hours = args.baseline_hours
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    now = _resolve_now(conn, args.now)
    total = scan.run_scan(conn, cfg, now=now)
    print(json.dumps({"alerts": total}))


def cmd_report(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    if args.format == "html":
        if args.now == "max":
            max_alert = conn.execute("SELECT MAX(ts) AS ts FROM alerts").fetchone()["ts"]
            max_label = conn.execute("SELECT MAX(ts) AS ts FROM label_events").fetchone()["ts"]
            max_ts = max(x for x in [max_alert, max_label] if x) if (max_alert or max_label) else None
            now = parse_ts(max_ts) if max_ts else None
        else:
            now = _resolve_now(conn, args.now)
        if now is not None and now.tzinfo is None:
            print("warning: --now is naive; assuming UTC", file=sys.stderr)
            now = now.replace(tzinfo=timezone.utc)
        out_dir = args.out or "report"
        report_mod.generate_report(conn, out_dir, now=now)
        print(json.dumps({"report_dir": out_dir}))
        return

    now_table = "alerts" if args.alerts else "label_events"
    now = _resolve_now(conn, args.now, table=now_table)

    if args.labeler:
        row = conn.execute("SELECT * FROM labelers WHERE labeler_did=?", (args.labeler,)).fetchone()
        if not row:
            print(json.dumps({"error": "labeler not found"}))
            return
        total_events = conn.execute(
            "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=?", (args.labeler,)
        ).fetchone()["c"]
        total_alerts = conn.execute(
            "SELECT COUNT(*) AS c FROM alerts WHERE labeler_did=?", (args.labeler,)
        ).fetchone()["c"]
        output = {
            "labeler_did": row["labeler_did"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "total_events": total_events,
            "total_alerts": total_alerts,
        }
        print(json.dumps(output, indent=2))
        return

    if args.alerts:
        since_ts = None
        if args.since:
            delta = _parse_duration(args.since)
            base_now = now if now is not None else datetime.now(timezone.utc)
            since_ts = format_ts(base_now - delta)
        if since_ts:
            if now is not None:
                rows = conn.execute(
                    "SELECT * FROM alerts WHERE ts>=? AND ts<=? ORDER BY ts DESC LIMIT ?",
                    (since_ts, format_ts(now), args.limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM alerts WHERE ts>=? ORDER BY ts DESC LIMIT ?",
                    (since_ts, args.limit),
                ).fetchall()
        else:
            if now is not None:
                rows = conn.execute(
                    "SELECT * FROM alerts WHERE ts<=? ORDER BY ts DESC LIMIT ?",
                    (format_ts(now), args.limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM alerts ORDER BY ts DESC LIMIT ?",
                    (args.limit,),
                ).fetchall()
        output = [dict(r) for r in rows]
        print(json.dumps(output, indent=2))
        return

    raise SystemExit("report requires --labeler or --alerts")


def cmd_export(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    rows = conn.execute("SELECT * FROM alerts ORDER BY ts DESC").fetchall()
    output = [dict(r) for r in rows]
    print(json.dumps(output))


def cmd_run(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    if args.window_minutes is not None:
        cfg.window_minutes = args.window_minutes
    if args.baseline_hours is not None:
        cfg.baseline_hours = args.baseline_hours
    runner.run_loop(
        cfg,
        ingest_interval=args.ingest_interval,
        scan_interval=args.scan_interval,
        report_out=args.report_out,
    )


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(prog="labelwatch")
    parser.add_argument("--config", help="Path to config.toml")
    parser.add_argument("--db-path", "--db", dest="db_path", help="Override db_path")

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ingest = sub.add_parser("ingest", help="Ingest label events")
    p_ingest.add_argument("--limit", type=int, default=100, help="Page size")
    p_ingest.add_argument("--pages", type=int, default=10, help="Max pages")
    p_ingest.add_argument("--fixture", help="Ingest from fixture JSONL")
    p_ingest.set_defaults(func=cmd_ingest)

    p_scan = sub.add_parser("scan", help="Run rules scan")
    p_scan.add_argument("--now", help="ISO-8601 timestamp or 'max'")
    p_scan.add_argument("--window-minutes", type=int, help="Override window minutes")
    p_scan.add_argument("--baseline-hours", type=int, help="Override baseline hours")
    p_scan.set_defaults(func=cmd_scan)

    p_report = sub.add_parser("report", help="Report on labelers/alerts")
    p_report.add_argument("--labeler", help="Labeler DID")
    p_report.add_argument("--alerts", action="store_true")
    p_report.add_argument("--since", help="Duration like 24h, 7d")
    p_report.add_argument("--now", help="ISO-8601 timestamp or 'max'")
    p_report.add_argument("--format", choices=["json", "html"], default="json")
    p_report.add_argument("--out", help="Output directory for HTML report")
    p_report.add_argument("--limit", type=int, default=50)
    p_report.set_defaults(func=cmd_report)

    p_export = sub.add_parser("export", help="Export alerts")
    p_export.add_argument("--format", choices=["json"], default="json")
    p_export.set_defaults(func=cmd_export)

    p_run = sub.add_parser("run", help="Run ingest/scan loop")
    p_run.add_argument("--ingest-interval", type=int, default=120, help="Seconds between ingest runs")
    p_run.add_argument("--scan-interval", type=int, default=300, help="Seconds between scan runs")
    p_run.add_argument("--report-out", help="Output directory for HTML report")
    p_run.add_argument("--window-minutes", type=int, help="Override window minutes")
    p_run.add_argument("--baseline-hours", type=int, help="Override baseline hours")
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
