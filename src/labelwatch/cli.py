from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
import sys
from typing import Optional

from . import climate as climate_mod
from . import db, discover, ingest, scan
from . import discovery_stream
from . import report as report_mod
from . import runner
from . import server as server_mod
from . import provenance as provenance_mod
from . import whatsonme as whatsonme_mod
from .classify import EvidenceDict, classify_labeler, CLASSIFIER_VERSION
from .config import load_config
from .utils import format_ts, now_utc, parse_ts


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
    scan.run_derive(conn, cfg, now=now)
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


def cmd_discover(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    if args.backstop:
        summary = discover.backstop_from_lists(conn)
    else:
        summary = discover.run_discovery(conn, cfg)
    print(json.dumps(summary, indent=2))


def cmd_discover_stream(args) -> None:
    import asyncio
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    if not os.path.exists(cfg.db_path):
        raise SystemExit(f"Database not found: {cfg.db_path}")
    asyncio.run(discovery_stream.run(cfg.db_path, args.backstop_interval))


def cmd_labelers(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    query = (
        "SELECT labeler_did, handle, display_name, labeler_class, is_reference, "
        "endpoint_status, service_endpoint, first_seen, last_seen, "
        "visibility_class, reachability_state, auditability, classification_confidence, "
        "likely_test_dev "
        "FROM labelers WHERE 1=1"
    )
    params: list = []

    if args.visibility_class:
        query += " AND visibility_class=?"
        params.append(args.visibility_class)
    if not args.include_test_dev:
        query += " AND (likely_test_dev=0 OR likely_test_dev IS NULL)"

    query += " ORDER BY is_reference DESC, labeler_class, labeler_did"
    rows = conn.execute(query, params).fetchall()
    output = [dict(r) for r in rows]
    print(json.dumps(output, indent=2))


def cmd_census(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    result = {}
    for field in ("visibility_class", "reachability_state", "classification_confidence", "auditability"):
        rows = conn.execute(
            f"SELECT COALESCE({field}, 'unknown') AS val, COUNT(*) AS c FROM labelers GROUP BY val"
        ).fetchall()
        result[field] = {r["val"]: r["c"] for r in rows}

    total = conn.execute("SELECT COUNT(*) AS c FROM labelers").fetchone()["c"]
    test_dev = conn.execute("SELECT COUNT(*) AS c FROM labelers WHERE likely_test_dev=1").fetchone()["c"]
    result["total"] = total
    result["test_dev_count"] = test_dev

    print(json.dumps(result, indent=2))


def cmd_climate(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    payload = climate_mod.generate_climate(
        conn, target_did=args.did, window_days=args.window,
        out_dir=args.out, fmt=args.out_format,
    )
    if args.out_format == "json":
        print(json.dumps(payload, indent=2))


def cmd_provenance(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path

    labeler_did = args.did

    # Derive observed metrics from local DB
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    observed = provenance_mod.derive_observed_metrics(conn, labeler_did)
    conn.close()

    # Build snapshot (fetches from network)
    try:
        snap = provenance_mod.snapshot_for_did(labeler_did, observed)
    except Exception as e:
        print(json.dumps({"error": str(e)}, indent=2), file=sys.stderr)
        raise SystemExit(1)

    print(json.dumps(snap.to_dict(), indent=2))


def cmd_whatsonme(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    identifier = args.identifier
    sources = args.sources.split(",") if args.sources else None
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    payload = whatsonme_mod.generate_whatsonme(identifier, sources=sources, conn=conn)
    conn.close()

    if payload.get("error"):
        print(json.dumps(payload, indent=2), file=sys.stderr)
        raise SystemExit(1)

    if args.out_format == "json":
        print(json.dumps(payload, indent=2))
    elif args.out_format == "html":
        html_str = whatsonme_mod._render_whatsonme_html(payload)
        out_path = os.path.join(args.out, "whatsonme.html")
        os.makedirs(args.out, exist_ok=True)
        with open(out_path, "w") as f:
            f.write(html_str)
        print(json.dumps({"wrote": out_path}))
    else:
        # both
        print(json.dumps(payload, indent=2))
        html_str = whatsonme_mod._render_whatsonme_html(payload)
        out_path = os.path.join(args.out, "whatsonme.html")
        os.makedirs(args.out, exist_ok=True)
        with open(out_path, "w") as f:
            f.write(html_str)


def cmd_serve(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    if not os.path.exists(cfg.db_path):
        raise SystemExit(f"Database not found: {cfg.db_path}")
    server_mod.run_server(
        db_path=cfg.db_path,
        port=args.port,
        cache_dir=args.cache_dir,
        max_concurrent=args.max_concurrent,
        rate_limit=args.rate_limit,
        bind=args.bind,
    )


def cmd_reclassify(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    rows = conn.execute("SELECT * FROM labelers").fetchall()
    changes = []
    ts = format_ts(now_utc())

    for row in rows:
        evidence = EvidenceDict(
            declared_record_present=bool(row["declared_record"]),
            did_doc_labeler_service_present=bool(row["has_labeler_service"]),
            did_doc_label_key_present=bool(row["has_label_key"]),
            observed_label_src=bool(row["observed_as_src"]),
            probe_result=row["reachability_state"] if row["reachability_state"] != "unknown" else None,
        )
        cls = classify_labeler(evidence)

        old = {
            "visibility_class": row["visibility_class"],
            "reachability_state": row["reachability_state"],
            "auditability": row["auditability"],
            "classification_confidence": row["classification_confidence"],
        }
        new = {
            "visibility_class": cls.visibility_class,
            "reachability_state": cls.reachability_state,
            "auditability": cls.auditability,
            "classification_confidence": cls.classification_confidence,
        }

        if old != new:
            changes.append({
                "labeler_did": row["labeler_did"],
                "old": old,
                "new": new,
                "reason": cls.reason,
            })

            if not args.dry_run:
                conn.execute(
                    "UPDATE labelers SET visibility_class=?, reachability_state=?, "
                    "auditability=?, classification_confidence=?, classification_reason=?, "
                    "classification_version=?, classified_at=? WHERE labeler_did=?",
                    (cls.visibility_class, cls.reachability_state, cls.auditability,
                     cls.classification_confidence, cls.reason, cls.version, ts,
                     row["labeler_did"]),
                )

    if not args.dry_run and changes:
        conn.commit()

    output = {
        "dry_run": args.dry_run,
        "total_labelers": len(rows),
        "changed": len(changes),
        "classifier_version": CLASSIFIER_VERSION,
        "changes": changes,
    }
    print(json.dumps(output, indent=2))


def cmd_coverage_delta(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    result = discover.coverage_delta(conn)
    print(json.dumps(result, indent=2))


def cmd_post(args) -> None:
    from .posting import BlueskyConfig, BlueskyPublisher, LinkCard

    app_password = args.app_password or os.environ.get("LABELWATCH_APP_PASSWORD")
    if not app_password:
        raise SystemExit(
            "App password required: --app-password or LABELWATCH_APP_PASSWORD env var"
        )
    cfg = BlueskyConfig(
        handle=args.handle,
        app_password=app_password,
        dry_run=args.dry_run,
    )
    publisher = BlueskyPublisher(cfg)

    if args.link_url:
        card = LinkCard(
            url=args.link_url,
            title=args.link_title or "",
            description=args.link_description or "",
        )
        result = publisher.post_link_card(args.text, card)
    else:
        result = publisher.post_text(args.text)

    print(json.dumps(result if isinstance(result, dict) else {"uri": str(result.uri), "cid": str(result.cid)}, indent=2))


def cmd_db_optimize(args) -> None:
    cfg = load_config(args.config)
    if args.db_path:
        cfg.db_path = args.db_path
    conn = db.connect(cfg.db_path)
    db.init_db(conn)
    result = db.optimize_db(conn)
    print(json.dumps(result, indent=2))


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

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

    p_discover = sub.add_parser("discover", help="Run labeler discovery")
    p_discover.add_argument("--backstop", action="store_true",
                            help="Run backstop discovery from labeler-lists")
    p_discover.set_defaults(func=cmd_discover)

    p_ds = sub.add_parser("discover-stream", help="Run Jetstream discovery listener")
    p_ds.add_argument("--backstop-interval", type=int, default=6,
                      help="Hours between labeler-lists backstop checks")
    p_ds.set_defaults(func=cmd_discover_stream)

    p_labelers = sub.add_parser("labelers", help="List discovered labelers")
    p_labelers.add_argument("--visibility-class", choices=["declared", "protocol_public", "observed_only", "unresolved"],
                            help="Filter by visibility class")
    p_labelers.add_argument("--include-test-dev", action="store_true", help="Include test/dev labelers")
    p_labelers.set_defaults(func=cmd_labelers)

    p_census = sub.add_parser("census", help="Show labeler classification census")
    p_census.set_defaults(func=cmd_census)

    p_climate = sub.add_parser("climate", help="Generate label climate for a DID")
    p_climate.add_argument("--did", required=True, help="Target DID")
    p_climate.add_argument("--window", type=int, default=30, help="Window in days (max 60)")
    p_climate.add_argument("--out", default=".", help="Output directory")
    p_climate.add_argument("--format", choices=["json", "html", "both"], default="both",
                           dest="out_format", help="Output format")
    p_climate.set_defaults(func=cmd_climate)

    p_whatsonme = sub.add_parser("whatsonme", help="Look up account labels for a DID or @handle")
    p_whatsonme.add_argument("identifier", help="DID or @handle to look up")
    p_whatsonme.add_argument("--sources", help="Comma-separated source DIDs to filter by")
    p_whatsonme.add_argument("--out", default=".", help="Output directory for HTML")
    p_whatsonme.add_argument("--format", choices=["json", "html", "both"], default="json",
                             dest="out_format", help="Output format")
    p_whatsonme.set_defaults(func=cmd_whatsonme)

    p_prov = sub.add_parser("provenance", help="Generate provenance scorecard for a labeler")
    p_prov.add_argument("did", help="Labeler DID to score")
    p_prov.set_defaults(func=cmd_provenance)

    p_reclass = sub.add_parser("reclassify", help="Recompute classifications from evidence")
    p_reclass.add_argument("--dry-run", action="store_true", help="Show diff without writing")
    p_reclass.set_defaults(func=cmd_reclassify)

    p_serve = sub.add_parser("serve", help="Run climate HTTP server")
    p_serve.add_argument("--port", type=int, default=8423)
    p_serve.add_argument("--bind", default="127.0.0.1", help="Bind address (default: loopback)")
    p_serve.add_argument("--cache-dir", default="cache")
    p_serve.add_argument("--max-concurrent", type=int, default=2)
    p_serve.add_argument("--rate-limit", type=int, default=30, help="Max requests/min")
    p_serve.set_defaults(func=cmd_serve)

    p_covdelta = sub.add_parser("coverage-delta",
                                help="Compare upstream labeler list vs registry")
    p_covdelta.set_defaults(func=cmd_coverage_delta)

    p_dbopt = sub.add_parser("db-optimize", help="Run ANALYZE and query planner optimization")
    p_dbopt.set_defaults(func=cmd_db_optimize)

    p_post = sub.add_parser("post", help="Post to Bluesky via labelwatch account")
    p_post.add_argument("text", help="Post text (max 300 graphemes)")
    p_post.add_argument("--link-url", help="External link card URL")
    p_post.add_argument("--link-title", help="Link card title")
    p_post.add_argument("--link-description", help="Link card description")
    p_post.add_argument("--dry-run", action="store_true", help="Log payload without posting")
    p_post.add_argument("--handle", default="labelwatch.bsky.social", help="Account handle")
    p_post.add_argument("--app-password", help="App password (or set LABELWATCH_APP_PASSWORD)")
    p_post.set_defaults(func=cmd_post)

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
