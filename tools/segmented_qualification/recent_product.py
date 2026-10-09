#!/usr/bin/env python3
"""Rolling-observatory site: landing, about, account HTML, bounded JSON exports.

Run only on an admitted RecentStore:
  PYTHONPATH=src:. python tools/segmented_qualification/recent_product.py --store PATH
Routes: / (landing), /about, /previous-epoch, /health (operator JSON with source
DIDs: loopback/operator-only, never routed publicly),
/lookup (DID or handle -> /account), /account, /exports, /download,
/instruments/fonts/<file>. Binds 127.0.0.1 only.
"""
from __future__ import annotations

import argparse
import html
import json
import re
import sys
import threading
import time
from contextlib import contextmanager
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlencode

from labelwatch.recent_observations import (Limits, RecentObservations, Refused, account_html, timestamp, iso,
                                            landing_html, about_html, previous_epoch_html, lookup_refusal_html,
                                            refusal_html, busy)

_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)  # once, for the sibling recent_publish module

FONTS = Path(__file__).resolve().parents[2] / "src" / "labelwatch" / "_instruments" / "fonts"
FONT_FILES = {"IBMPlexSans-Regular.woff2", "IBMPlexSans-SemiBold.woff2",
              "IBMPlexMono-Regular.woff2", "SourceSerif4-Bold.woff2"}
HANDLE = re.compile(r"^@?([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$")


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


@contextmanager
def one_clock(product):
    """Pin the product clock for one request.

    A "last N days" request derives its interval from now() and create() checks
    the window against now() again; with a running clock the second reading is
    later, so the full-window start always fell just outside it (409). Pinning
    reads the clock once; create()/export semantics are unchanged.
    """
    with product._lock:
        fixed = product.now()
        original = product.now
        product.now = lambda: fixed
        try:
            yield fixed
        finally:
            product.now = original


class SiteStatus:
    """Short-lived cache of the cheap store summary (no payload reads).

    The figures come from the same aggregate reader as the published overview,
    so the landing page and overview.json cannot disagree in definition.
    """
    def __init__(self, product, ttl=15.0, stale_after=900):
        self.product, self.ttl, self.stale_after = product, ttl, stale_after
        self._lock = threading.Lock()
        self._cached = None

    def read(self):
        with self._lock:
            tick = time.monotonic()
            if self._cached and tick - self._cached[0] < self.ttl:
                return self._cached[1]
            now = self.product.now()
            try:
                import recent_publish
                from labelwatch import recent_overview
                stats = recent_publish.read_store(self.product.provider.store, now, timeout=2.0, budget=5.0, labelers=False)
                view = recent_overview.overview(stats, now)
                result = {"view": view, "maintenance": stats["last_maintenance"], "error": None, "now": now,
                          "refused_sources": stats["refused_sources"], "operator_sources": stats["operator_sources"]}
            except Exception as exc:  # summary absence is disclosed, never shown as zero
                result = {"view": None, "maintenance": None, "error": type(exc).__name__, "now": now}
            self._cached = (tick, result)
            return result

    HEALTH_LIST_CAP = 50

    def health(self, full=False):
        """Operator JSON. Per-source lists are capped at 50 (+ totals) unless full."""
        status = self.read()
        view = status["view"]
        if view is None:
            return 503, {"status": "unavailable", "reason": status["error"], "window_days": self.product.limits.days}
        cov = view["coverage"]
        through = cov["observed_through"]
        age = (status["now"] - timestamp(through)).total_seconds() if through else None
        # observed_through is the latest accepted collector page; null until one is accepted.
        state = "unknown" if age is None else "fresh" if age <= self.stale_after else "stale"
        overall = "ok" if state == "fresh" else "warming_up" if state == "unknown" and cov["warmup"] is not None else "degraded"
        return 200, {"status": overall,
                     "window_days": view["window_days"], "acquisition": view["acquisition"],
                     "coverage": {"status": cov["status"], "observed_days": cov["observed_days"], "warmup": cov["warmup"],
                                  "gaps_count": len(cov["gaps"]), "gap_overflow": cov["gap_overflow"], "sources": cov["sources"],
                                  # Operator detail only; never in the public overview artifact.
                                  **self._operator_lists(status, full)},
                     "last_maintenance": status["maintenance"],
                     "freshness": {"state": state, "basis": "last_accepted_observation", "observed_through": through,
                                   "age_seconds": None if age is None else round(age, 3), "stale_after_seconds": self.stale_after},
                     "generated_at": view["generated_at"]}


    def _operator_lists(self, status, full):
        lists = {"refused_sources": status["refused_sources"], **status["operator_sources"]}
        established = lists.pop("established_at", None)
        if not full:
            lists.pop("source_status", None)  # the per-source table only with ?full=1
        out = {}
        for key, rows in lists.items():
            rows = list(rows or [])
            out[key] = rows if full else rows[:self.HEALTH_LIST_CAP]
            out[key + "_total"] = len(rows)
        out["lists_truncated"] = not full and any(out[k + "_total"] > len(out[k]) for k in lists)
        out["full_lists"] = "/health?full=1"
        out["established_at"] = established or {"earliest": None, "latest": None}
        return out


def handler(product, *, resolver=None, status=None):
    site = status or SiteStatus(product)
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(2)

        def log_message(self, *args):
            pass  # No account/cursor values in ordinary request logs.

        def respond(self, status, body, content_type="application/json", attachment=False, retry_after=None):
            data = body.encode() if isinstance(body, str) else json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type + "; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            if attachment:
                self.send_header("Content-Disposition", 'attachment; filename="labelwatch-observations.json"')
            self.send_header("Cache-Control", "no-store")
            if retry_after is not None:
                self.send_header("Retry-After", str(retry_after))
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; font-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(data)

        def respond_bytes(self, data, content_type):
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(data)

        def busy(self):
            """Explicit, retryable refusal when the store lease or a query deadline is exhausted."""
            body = {"error": "store_busy", "complete": False, "retry_after_seconds": 5}
            if self.wants_html():
                self.respond(503, refusal_html("store_busy"), "text/html", retry_after=5)
            else:
                self.respond(503, body, retry_after=5)

        def wants_html(self):
            """Browsers get a small HTML refusal; API clients (Accept: application/json) keep JSON."""
            accept = (getattr(self, "headers", None) or {}).get("Accept") or ""
            return "text/html" in accept and "application/json" not in accept

        def redirect(self, location):
            self.send_response(303)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()

        def lookup(self, args):
            """DID or handle -> the unchanged /account route; handles resolve only when enabled."""
            if set(args) - {"account", "days"} or "account" not in args or args.get("days", "30") not in ("7", "30"):
                raise Refused("invalid_lookup_request")
            account, days = args["account"].strip(), args.get("days", "30")
            if len(account) > 512:
                raise Refused("invalid_lookup_request")
            if account.startswith("did:"):
                return self.redirect("/account?" + urlencode({"did": account, "days": days}))
            if not HANDLE.match(account):
                return self.respond(409, lookup_refusal_html("That is not an account DID or handle."), "text/html")
            if resolver is None:
                return self.respond(409, lookup_refusal_html("Handle lookup is not enabled on this service."), "text/html")
            try:
                did = resolver(account.lstrip("@"))
            except Exception:
                did = None
            if not isinstance(did, str) or not did.startswith("did:") or len(did) > 512 or any(c in did for c in "/?#\r\n"):
                return self.respond(409, lookup_refusal_html("That handle could not be resolved to an account DID."), "text/html")
            return self.redirect("/account?" + urlencode({"did": did, "days": days}))

        def dispatch(self, path, args):
            if path == "/":
                try:
                    window = interval(product)
                except Exception:
                    window = None
                current = site.read()
                return landing_html(current["view"], window, current["error"]), True
            if path in ("/about", "/previous-epoch"):
                if path == "/previous-epoch":
                    return previous_epoch_html(), True
                view = site.read()["view"]
                return about_html(view["acquisition"]["started_at"] if view else None, product.limits.days), True
            if path == "/download":
                if set(args)!={"cursor"}:raise Refused("invalid_download_request")
                return product.download(args["cursor"]),False
            if path == "/exports" and "cursor" in args:
                return product.page(args["cursor"]), False
            if path in ("/account", "/exports"):
                create = product.create_export if path == "/exports" else product.create
                with one_clock(product):
                    if "days" in args:
                        args["start"], args["end"] = interval(product, int(args["days"]))
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
                if request.path == "/health":
                    if args not in ({}, {"full": "1"}):
                        raise Refused("unknown_parameter")
                    code, body = site.health(full=args == {"full": "1"})
                    self.respond(code, body)
                    return
                if request.path.startswith("/instruments/fonts/"):
                    name = request.path.rsplit("/", 1)[1]
                    if name not in FONT_FILES or args:
                        raise Refused("unknown_route")
                    self.respond_bytes((FONTS / name).read_bytes(), "font/woff2")
                    return
                if request.path == "/lookup":
                    self.lookup(args)
                    return
                if set(args) - {"did", "start", "end", "labeler", "value", "cursor", "action", "target_kind", "days"}:
                    raise Refused("unknown_parameter")
                result, is_html = self.dispatch(request.path, args)
                if request.path == "/download":
                    self.respond(200,result,"application/json",True)
                else:
                    self.respond(200, result, "text/html" if is_html else "application/json")
            except Refused as exc:
                if str(exc) == "store_busy":
                    self.busy()
                    return
                body = {"error": str(exc), "complete": False}
                details = getattr(exc, "details", None)
                if str(exc) == "export_too_large":
                    body["next_action"] = "Request a narrower interval; a bounded export must finish within its stated deadline cap."
                if details:
                    details = json.loads(json.dumps(details))
                    keep = {k: v for k, v in args.items() if k not in ("start", "end", "days", "cursor")}
                    for item in details.get("suggested_intervals") or []:
                        if item.get("status") != "interval_too_dense":
                            item["export_url"] = "/exports?" + urlencode({**keep, "start": item["start"], "end": item["end_exclusive"]})
                    body.update(details)
                if str(exc) in ("snapshot_row_limit", "snapshot_byte_limit"):
                    body["next_action"] = "Use the bounded paginated export for this period; reducing time may not separate equal-timestamp observations."
                    body["export_url"] = '/exports?'+urlencode(args)
                    if request.path == '/account':
                        self.respond(409,dense_account_html(args),'text/html')
                        return
                code = 410 if str(exc) == "snapshot_expired_or_unavailable" else 409
                if self.wants_html():
                    self.respond(code, refusal_html(str(exc), details), "text/html")
                    return
                self.respond(code, body)
            except (KeyError, ValueError):
                self.respond(400, {"error": "invalid_request", "complete": False})
            except (BrokenPipeError, ConnectionResetError, TimeoutError) as exc:
                if busy(exc):  # a lease/deadline error, not the client socket
                    self.busy()
            except Exception as exc:
                if busy(exc):
                    self.busy()
                    return
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
    parser.add_argument("--resolve-handles", action="store_true",
                        help="Resolve handles in /lookup via com.atproto.identity.resolveHandle (outbound HTTPS).")
    parser.add_argument("--stale-after-seconds", type=int, default=900,
                        help="/health reports collection as stale when the last accepted observation is older.")
    args = parser.parse_args(argv)
    # This adapter is deliberately explicit and separately qualified. No
    # fallback to live databases or fabricated observation timestamps.
    from recent_provider import RecentProvider
    provider = RecentProvider(args.store)
    product = RecentObservations(provider, Limits(days=args.window_days))
    resolver = None
    if args.resolve_handles:
        from labelwatch.resolve import resolve_handle_to_did
        resolver = lambda handle: resolve_handle_to_did(handle, timeout=3)  # noqa: E731
    site = SiteStatus(product, stale_after=args.stale_after_seconds)
    server = HTTPServer(("127.0.0.1", args.port), handler(product, resolver=resolver, status=site))
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
