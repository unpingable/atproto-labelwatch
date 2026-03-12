"""Climate HTTP server — stdlib only, no external dependencies.

Routes:
    GET /v1/climate/{did}  — label climate report (json or html)
    GET /health            — health check

Rate-limited, disk-cached, concurrency-gated. Designed to not melt.
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

from . import db
from .climate import generate_climate, public_climate_payload, _render_html
from .registry import generate_registry, render_registry_html
from .report import _did_slug

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token bucket rate limiter
# ---------------------------------------------------------------------------

class _TokenBucket:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, capacity: int, refill_rate: float):
        self._capacity = capacity
        self._tokens = float(capacity)
        self._refill_rate = refill_rate  # tokens per second
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume(self) -> float:
        """Try to consume one token. Returns 0 if ok, or seconds to wait."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
            self._last_refill = now
            if self._tokens >= 1:
                self._tokens -= 1
                return 0.0
            return (1 - self._tokens) / self._refill_rate


# ---------------------------------------------------------------------------
# Request stats (per-minute logging)
# ---------------------------------------------------------------------------

class _Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.requests = 0
        self.status_4xx = 0
        self.status_429 = 0
        self.status_503 = 0
        self.cache_hits = 0
        self.last_flush = time.monotonic()

    def record(self, status: int, cache_hit: bool = False):
        with self._lock:
            self.requests += 1
            if cache_hit:
                self.cache_hits += 1
            if 400 <= status < 500:
                self.status_4xx += 1
            if status == 429:
                self.status_429 += 1
            if status == 503:
                self.status_503 += 1

    def flush_if_due(self) -> Optional[str]:
        with self._lock:
            now = time.monotonic()
            if now - self.last_flush < 60:
                return None
            msg = (
                f"STATS req={self.requests} 4xx={self.status_4xx} "
                f"429={self.status_429} 503={self.status_503} "
                f"cache_hit={self.cache_hits}"
            )
            self.requests = 0
            self.status_4xx = 0
            self.status_429 = 0
            self.status_503 = 0
            self.cache_hits = 0
            self.last_flush = now
            return msg


# ---------------------------------------------------------------------------
# Disk cache
# ---------------------------------------------------------------------------

class _DiskCache:
    def __init__(self, cache_dir: str, ttl: int = 300):
        self._cache_dir = os.path.realpath(cache_dir)
        self._ttl = ttl

    def _path(self, did: str, window: int, fmt: str) -> str:
        slug = _did_slug(did)
        ext = "json" if fmt == "json" else "html"
        p = os.path.realpath(os.path.join(self._cache_dir, "climate", slug, f"w{window}.{ext}"))
        # Path traversal check
        if not p.startswith(self._cache_dir + os.sep):
            raise ValueError("path traversal")
        return p

    def get(self, did: str, window: int, fmt: str) -> Optional[bytes]:
        try:
            p = self._path(did, window, fmt)
        except ValueError:
            return None
        try:
            mtime = os.path.getmtime(p)
            if time.time() - mtime > self._ttl:
                return None
            with open(p, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def put(self, did: str, window: int, fmt: str, data: bytes) -> None:
        try:
            p = self._path(did, window, fmt)
        except ValueError:
            return
        d = os.path.dirname(p)
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        try:
            os.write(fd, data)
            os.close(fd)
            os.replace(tmp, p)
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise


# ---------------------------------------------------------------------------
# DID validation
# ---------------------------------------------------------------------------

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _validate_did(raw: str) -> Optional[str]:
    """Validate and clean a DID string. Returns error message or None."""
    if not raw.startswith("did:"):
        return "Invalid DID"
    if len(raw) > 256:
        return "Invalid DID"
    if "/" in raw or _CONTROL_RE.search(raw):
        return "Invalid DID"
    return None


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

class ClimateHandler(BaseHTTPRequestHandler):
    # Set by configure()
    db_path: str = ""
    cache: Optional[_DiskCache] = None
    bucket: Optional[_TokenBucket] = None
    semaphore: Optional[threading.Semaphore] = None
    stats: Optional[_Stats] = None
    disabled: bool = False
    generation_timeout: int = 10

    def log_message(self, format, *args):
        # Suppress default stderr logging — we do our own
        pass

    def _send_json(self, status: int, obj: dict, extra_headers: Optional[dict] = None,
                   cache_hit: bool = False):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Content-Type-Options", "nosniff")
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        if self.stats:
            self.stats.record(status, cache_hit)

    def _send_html(self, status: int, html_bytes: bytes, extra_headers: Optional[dict] = None,
                   cache_hit: bool = False):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("X-Content-Type-Options", "nosniff")
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)
        if self.stats:
            self.stats.record(status, cache_hit)

    def _send_error(self, status: int, msg: str, extra_headers: Optional[dict] = None):
        self._send_json(status, {"error": msg}, extra_headers)

    def do_GET(self):
        start = time.monotonic()
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/")
        query = urllib.parse.parse_qs(parsed.query)

        try:
            if path == "/health":
                self._handle_health()
            elif path == "/v1/registry":
                self._handle_registry(query)
            elif "/v1/climate/" in path:
                self._handle_climate(path, query)
            else:
                self._send_error(404, "Not found")
        except Exception:
            logger.exception("Unhandled error")
            self._send_error(500, "Internal server error")

        elapsed_ms = (time.monotonic() - start) * 1000
        did_trunc = ""
        if "/v1/climate/" in path:
            parts = path.split("/v1/climate/", 1)
            if len(parts) > 1:
                raw = urllib.parse.unquote(parts[1]).strip()
                did_trunc = raw[:20]
        logger.info("%-4s %3d %6.1fms %s", self.command,
                     getattr(self, '_last_status', 0), elapsed_ms,
                     did_trunc or path[:50])

        if self.stats:
            msg = self.stats.flush_if_due()
            if msg:
                logger.info(msg)

    def _handle_health(self):
        from .read_health import get_tracker
        from .signal_health import signal_health_snapshot

        reads = get_tracker().snapshot()

        # Signal health (per-labeler EPS baseline)
        signals = {"verdict": "NO_DATA"}
        try:
            conn = db.connect(self.db_path, readonly=True)
            try:
                signals = signal_health_snapshot(conn)
            finally:
                conn.close()
        except Exception:
            logger.debug("Signal health query failed", exc_info=True)

        self._last_status = 200
        self._send_json(200, {
            "ok": True,
            "reads": reads,
            "reads_degraded": reads["verdict"] in ("DEGRADED",),
            "signals": {
                "verdict": signals["verdict"],
                "classifications": signals.get("classifications"),
                "total_observed": signals.get("total_observed"),
                "overall_7d_30d_ratio": signals.get("overall_7d_30d_ratio"),
                "gone_dark_count": len(signals.get("gone_dark", [])),
                "degrading_count": len(signals.get("degrading", [])),
                "reference_issues": signals.get("reference_issues", []),
            },
            "signals_degraded": signals["verdict"] in ("CRITICAL", "DEGRADED"),
        }, {"Cache-Control": "no-store"})

    def _handle_registry(self, query: dict):
        """Handle /v1/registry requests."""
        if self.disabled:
            self._last_status = 503
            self._send_error(503, "Service disabled")
            return

        fmt = query.get("format", ["html"])[0]
        if fmt not in ("json", "html"):
            fmt = "html"

        # Rate limit
        if self.bucket:
            wait = self.bucket.consume()
            if wait > 0:
                self._last_status = 429
                self._send_error(429, "Rate limited",
                                 {"Retry-After": str(int(wait) + 1),
                                  "Cache-Control": "no-store"})
                return

        # Concurrency gate
        if self.semaphore and not self.semaphore.acquire(blocking=False):
            self._last_status = 503
            self._send_error(503, "Server busy")
            return

        try:
            self._generate_registry(fmt)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _generate_registry(self, fmt: str):
        """Generate and respond with registry data."""
        result: dict = {}
        error = [None]
        done_event = threading.Event()

        def _generate():
            try:
                conn = db.connect(self.db_path, readonly=True)
                try:
                    result["payload"] = generate_registry(conn)
                finally:
                    conn.close()
            except Exception as e:
                error[0] = e
            finally:
                done_event.set()

        t = threading.Thread(target=_generate, daemon=True)
        t.start()
        done_event.wait(timeout=self.generation_timeout)

        if not done_event.is_set():
            self._last_status = 503
            self._send_error(503, "Generation timeout")
            return

        if error[0] is not None:
            logger.error("Registry generation error: %s", error[0])
            self._last_status = 500
            self._send_error(500, "Internal server error")
            return

        payload = result["payload"]
        headers = {"Cache-Control": "private, max-age=300"}

        if fmt == "json":
            self._last_status = 200
            self._send_json(200, payload, headers)
        else:
            html_str = render_registry_html(payload)
            self._last_status = 200
            self._send_html(200, html_str.encode("utf-8"), headers)

    def _handle_climate(self, path: str, query: dict):
        # Parse DID from path
        parts = path.split("/v1/climate/", 1)
        if len(parts) < 2 or not parts[1]:
            self._last_status = 400
            self._send_error(400, "Invalid DID")
            return

        remainder = parts[1]
        # Extra path segments → 404
        if "/" in remainder:
            self._last_status = 404
            self._send_error(404, "Not found")
            return

        raw_input = urllib.parse.unquote(remainder).strip()

        # Handle resolution: accept @handle or bare handle
        if raw_input.startswith("did:"):
            did = raw_input
        else:
            from .resolve import resolve_handle_to_did
            handle_input = raw_input.lstrip("@")
            did = resolve_handle_to_did(handle_input)
            if not did:
                self._last_status = 404
                self._send_error(404, f"Could not resolve handle: {raw_input}")
                return

        # Validate
        err = _validate_did(did)
        if err:
            self._last_status = 400
            self._send_error(400, err)
            return

        # Panic switch
        if self.disabled:
            self._last_status = 503
            self._send_error(503, "Service disabled")
            return

        # Parse query params
        fmt = query.get("format", ["html"])[0]
        if fmt not in ("json", "html"):
            fmt = "html"
        try:
            window = int(query.get("window", ["30"])[0])
        except (ValueError, IndexError):
            window = 30
        window = max(1, min(window, 60))

        # Rate limit (before cache)
        if self.bucket:
            wait = self.bucket.consume()
            if wait > 0:
                self._last_status = 429
                self._send_error(429, "Rate limited",
                                 {"Retry-After": str(int(wait) + 1),
                                  "Cache-Control": "no-store"})
                return

        # Cache check
        if self.cache:
            cached = self.cache.get(did, window, fmt)
            if cached is not None:
                self._last_status = 200
                headers = {"Cache-Control": "private, max-age=300"}
                if fmt == "json":
                    self._send_json(200, json.loads(cached), headers, cache_hit=True)
                else:
                    self._send_html(200, cached, headers, cache_hit=True)
                return

        # Concurrency gate
        if self.semaphore and not self.semaphore.acquire(blocking=False):
            self._last_status = 503
            self._send_error(503, "Server busy")
            return

        handle = query.get("handle", [None])[0]
        # If we resolved from a handle, pass it along
        if not handle and raw_input != did:
            handle = raw_input.lstrip("@")

        try:
            self._generate_and_respond(did, window, fmt, handle=handle)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _generate_and_respond(self, did: str, window: int, fmt: str,
                              handle: str = None):
        # Generation with timeout
        result: Dict[str, Any] = {}
        error = [None]
        done_event = threading.Event()

        def _generate():
            try:
                conn = db.connect(self.db_path, readonly=True)
                try:
                    # Use a temp dir for file output (we discard it)
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        payload = generate_climate(
                            conn, target_did=did, window_days=window,
                            out_dir=tmp_dir, fmt="json",
                        )
                    result["payload"] = payload
                except Exception as e:
                    error[0] = e
                finally:
                    conn.close()
            except Exception as e:
                error[0] = e
            finally:
                done_event.set()

        t = threading.Thread(target=_generate, daemon=True)
        t.start()
        done_event.wait(timeout=self.generation_timeout)

        if not done_event.is_set():
            self._last_status = 503
            self._send_error(503, "Generation timeout")
            return

        if error[0] is not None:
            logger.error("Generation error: %s", error[0])
            self._last_status = 500
            self._send_error(500, "Internal server error")
            return

        payload = result["payload"]
        public = public_climate_payload(payload)
        if handle:
            public["target_handle"] = handle

        headers = {"Cache-Control": "private, max-age=300"}

        if fmt == "json":
            body = json.dumps(public, indent=2).encode("utf-8")
            if self.cache:
                self.cache.put(did, window, "json", body)
            self._last_status = 200
            self._send_json(200, public, headers)
        else:
            html_str = _render_html(public, did, window)
            body = html_str.encode("utf-8")
            if self.cache:
                self.cache.put(did, window, "html", body)
            self._last_status = 200
            self._send_html(200, body, headers)


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------

def configure_handler(db_path: str, cache_dir: str, max_concurrent: int = 2,
                      rate_limit: int = 30, cache_ttl: int = 300,
                      generation_timeout: int = 10) -> type:
    """Create a configured handler class."""
    # Refill rate: rate_limit per minute → rate_limit/60 per second
    handler = type("ConfiguredClimateHandler", (ClimateHandler,), {
        "db_path": db_path,
        "cache": _DiskCache(cache_dir, cache_ttl),
        "bucket": _TokenBucket(rate_limit, rate_limit / 60.0),
        "semaphore": threading.Semaphore(max_concurrent),
        "stats": _Stats(),
        "disabled": os.environ.get("CLIMATE_API_DISABLED", "") == "1",
        "generation_timeout": generation_timeout,
    })
    return handler


def run_server(db_path: str, port: int = 8423, cache_dir: str = "cache",
               max_concurrent: int = 2, rate_limit: int = 30,
               bind: str = "127.0.0.1"):
    """Start the climate HTTP server."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Verify DB exists
    if not os.path.exists(db_path):
        raise SystemExit(f"Database not found: {db_path}")

    handler_cls = configure_handler(db_path, cache_dir, max_concurrent, rate_limit)

    server = ThreadingHTTPServer((bind, port), handler_cls)
    logger.info("Climate server starting on port %d (db=%s, cache=%s)",
                port, db_path, cache_dir)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        server.shutdown()
