#!/usr/bin/env python3
"""Explicit successor preview: loopback account HTML and bounded JSON exports.

Run only on an admitted off-production RecentStore:
  PYTHONPATH=src:. python tools/segmented_qualification/recent_product.py --store PATH
No existing service, ingestion task or production configuration is changed.
"""
from __future__ import annotations

import argparse
import html
import json
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlencode

from labelwatch.recent_observations import Limits, RecentObservations, Refused, account_html, timestamp, iso


def dense_account_html(args):
    link='/exports?'+urlencode({key:value for key,value in args.items() if key not in ('cursor','days')})
    return ("<!doctype html><html lang='en'><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
            "<title>Export this observation period</title><main><h1>This period exceeds the account preview budget</h1>"
            "<p>No partial account view was published. Many observations can share one timestamp, so a shorter period may not help.</p>"
            "<p><a href='"+html.escape(link,quote=True)+"'>Open the bounded paginated export for this period</a></p>"
            "<p>The export uses a fixed acquisition frontier and expires after its stated deadline. Current label state remains unknown.</p></main></html>")


def interval(product, days=30):
    if days not in (7, 30):
        raise Refused("invalid_period")
    frontier = product.provider.store.frontier()
    now = product.now()
    start = max(now - timedelta(days=days), timestamp(frontier["start"]))
    end = min(now, timestamp(frontier["end"]))
    if start >= end:
        raise Refused("observation_window_unavailable")
    return iso(start), iso(end)


def handler(product):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(2)

        def log_message(self, *args):
            pass  # No account/cursor values in ordinary request logs.

        def respond(self, status, body, content_type="application/json"):
            data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type + "; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(data)

        def dispatch(self, path, args):
            if path == "/":
                start, end = interval(product)
                return ("<!doctype html><meta charset='utf-8'><title>Recent account observations</title>"
                        "<h1>Recent account observations</h1><p>Explore recorded public labels for an account. "
                        "The candidate window is 30 days. A new store has a warm-up gap before acquisition began; "
                        "no observations does not mean no labels. Current label state is unknown.</p>"
                        "<form action='/account'><label>Account DID <input name='did' required></label>"
                        "<label>Start (UTC ISO timestamp) <input name='start' value='" + html.escape(start, quote=True) + "' required></label>"
                        "<label>End, exclusive (UTC ISO timestamp) <input name='end' value='" + html.escape(end, quote=True) + "' required></label>"
                        "<button>View this period</button><button name='days' value='7'>Last 7 days</button>"
                        "<button name='days' value='30'>Full candidate window</button></form>"
                        "<p>Interval ends at the available source frontier, which may be earlier than now. "
                        "Exports are finite consistent snapshots. "
                        "Exports use bounded pages even when many observations share one timestamp. Account previews have a finite row budget; no truncated result is published.</p>"), True
            if path == "/exports" and "cursor" in args:
                return product.page(args["cursor"]), False
            if path in ("/account", "/exports"):
                if "days" in args:
                    args["start"], args["end"] = interval(product, int(args["days"]))
                create = product.create_export if path == "/exports" else product.create
                created = create(args["did"], args["start"], args["end"],
                                         labeler=args.get("labeler") or None,
                                         value=args.get("value") or None,
                                         action=args.get("action") or None,
                                         target_kind=args.get("target_kind") or None)
                if path == "/account":
                    return account_html(product.account(created["cursor"])), True
                return created, False
            raise Refused("unknown_route")

        def do_GET(self):
            try:
                if len(self.path) > 8192:
                    raise Refused("request_too_large")
                request = urlsplit(self.path)
                query = parse_qs(request.query, keep_blank_values=True, max_num_fields=9)
                if any(len(values) != 1 for values in query.values()):
                    raise Refused("duplicate_parameter")
                args = {key: value[0] for key, value in query.items()}
                if set(args) - {"did", "start", "end", "labeler", "value", "cursor", "action", "target_kind", "days"}:
                    raise Refused("unknown_parameter")
                result, is_html = self.dispatch(request.path, args)
                self.respond(200, result, "text/html" if is_html else "application/json")
            except Refused as exc:
                body = {"error": str(exc), "complete": False}
                if str(exc) in ("snapshot_row_limit", "snapshot_byte_limit"):
                    body["next_action"] = "Use the bounded paginated export for this period; reducing time may not separate equal-timestamp observations."
                    body["export_url"] = '/exports?'+urlencode(args)
                    if request.path == '/account':
                        self.respond(409,dense_account_html(args),'text/html')
                        return
                self.respond(410 if str(exc) == "snapshot_expired_or_unavailable" else 409, body)
            except (KeyError, ValueError):
                self.respond(400, {"error": "invalid_request", "complete": False})
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass
            except Exception as exc:
                if request.path == '/account' and str(exc) == 'recent query byte ceiling':
                    self.respond(409,dense_account_html(args),'text/html')
                    return
                self.respond(503, {"error": "observation_snapshot_unavailable", "complete": False})
    return Handler


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8431)
    parser.add_argument("--window-days", type=int, default=30, choices=(30,),
                        help="Only the 30-day candidate is implemented; longer guarantees require qualification.")
    args = parser.parse_args(argv)
    # This adapter is deliberately explicit and separately qualified. No
    # fallback to live databases or fabricated observation timestamps.
    from recent_provider import RecentProvider
    provider = RecentProvider(args.store)
    product = RecentObservations(provider, Limits(days=args.window_days))
    server = HTTPServer(("127.0.0.1", args.port), handler(product))
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
