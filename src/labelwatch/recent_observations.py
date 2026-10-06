"""Bounded recent-observation views; independent of the selected live service.

Providers hold a consistent read lease only while materializing a finite public
snapshot. Pagination never retains a database connection or generation lease.
All effective/current-state claims remain unknown: these are observed events.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import html
import json
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode


class Refused(ValueError):
    """Explicit public refusal, never a successful empty result."""


def timestamp(value):
    if not isinstance(value, str):
        raise Refused("invalid_timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Refused("invalid_timestamp") from exc
    if result.tzinfo is None:
        raise Refused("timestamp_requires_timezone")
    return result.astimezone(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@dataclass(frozen=True)
class Limits:
    days: int = 30
    snapshots: int = 4
    rows: int = 1000
    snapshot_bytes: int = 1024 * 1024
    page_rows: int = 100
    ttl_seconds: int = 120
    page_requests: int = 64

    def __post_init__(self):
        if not 30 <= self.days <= 45 or any(
            value <= 0 for value in (self.snapshots, self.rows, self.snapshot_bytes,
                                     self.page_rows, self.ttl_seconds, self.page_requests)
        ):
            raise ValueError("invalid finite product limits")


@dataclass
class _Snapshot:
    manifest: bytes
    rows: tuple[bytes, ...]
    deadline: float
    requests: int = 0


class RecentObservations:
    """Single-process finite public snapshots over a context-managed provider.

    Provider.snapshot(did,start,end,max_rows) yields rows, frontier and coverage.
    Rows must include trusted observed_at; ts is the source-authored timestamp.
    Provider errors are not converted to complete/empty data. Admission is
    serialized, bounding simultaneously materialized exports. Process restart
    intentionally expires every cursor; this is not durable download hosting.
    """
    def __init__(self, provider, limits=Limits(), *, now=None, monotonic=time.monotonic):
        self.provider, self.limits = provider, limits
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic
        self._key = secrets.token_bytes(32)
        self._snapshots = {}
        self._lock = threading.RLock()

    def _purge(self):
        for key in list(self._snapshots):
            if self.monotonic() >= self._snapshots[key].deadline:
                del self._snapshots[key]

    def _cursor(self, identity, offset):
        body = f"{identity}:{offset}".encode()
        mac = hmac.new(self._key, body, hashlib.sha256).hexdigest()
        return base64.urlsafe_b64encode(body + b":" + mac.encode()).decode()

    def _decode(self, token):
        try:
            if len(token) > 256:
                raise ValueError()
            body, mac = base64.b64decode(token, altchars=b"-_", validate=True).rsplit(b":", 1)
            expected = hmac.new(self._key, body, hashlib.sha256).hexdigest().encode()
            if not hmac.compare_digest(mac, expected):
                raise ValueError()
            identity, offset = body.decode().split(":")
            offset = int(offset)
            if offset < 0:
                raise ValueError()
            return identity, offset
        except (ValueError, UnicodeError, TypeError) as exc:
            raise Refused("invalid_cursor") from exc

    @staticmethod
    def _row(row, did, start, end):
        # Do not export whole mappings: signatures, probes and operational
        # evidence can coexist with these public event columns in a provider.
        required = ("labeler_did", "uri", "val", "ts", "observed_at")
        if any(not isinstance(row.get(k), str) or not row[k] for k in required):
            raise Refused("public_event_schema_unavailable")
        uri = row["uri"]
        if uri != did and not uri.startswith("at://" + did + "/"):
            raise Refused("provider_subject_mismatch")
        observed = timestamp(row["observed_at"])
        if not start <= observed < end:
            raise Refused("provider_window_mismatch")
        timestamp(row["ts"])
        neg = row.get("neg")
        if neg is not None and (type(neg) not in (int, bool) or neg not in (0, 1)):
            raise Refused("invalid_event_action")
        answer = {"labeler_did": row["labeler_did"], "uri": uri,
                  "value": row["val"], "source_timestamp": row["ts"],
                  "observed_at": iso(observed),
                  "action": "removal_observed" if neg == 1 else
                            "application_observed" if neg == 0 else "unknown_action"}
        for name in ("src", "cid", "exp"):
            if row.get(name) is not None:
                if not isinstance(row[name], str):
                    raise Refused("invalid_public_event_field")
                answer[name] = row[name]
        return answer

    @staticmethod
    def _coverage(value, start, end):
        if not isinstance(value, dict) or value.get("status") not in ("observed", "gapped", "unknown"):
            raise Refused("coverage_unavailable")
        gaps = []
        for gap in value.get("gaps", []):
            if len(gaps) >= 128:
                raise Refused("coverage_budget_exceeded")
            lo, hi = timestamp(gap["start"]), timestamp(gap["end"])
            if not start <= lo < hi <= end:
                raise Refused("invalid_coverage_gap")
            reason = gap.get("reason")
            if reason not in ("not_observed", "source_unavailable", "unknown"):
                reason = "unknown"
            gaps.append({"start": iso(lo), "end": iso(hi), "reason": reason})
        if value["status"] == "observed" and gaps:
            raise Refused("conflicting_coverage")
        return {"status": value["status"], "gaps": gaps,
                "scope": "recorded_observations_only; not universal source coverage"}

    def create(self, did, start, end, *, labeler=None, value=None, action=None, target_kind=None):
        with self._lock:
            self._purge()
            if not isinstance(did, str) or not did.startswith("did:") or len(did) > 512 or any(c in did for c in "/?#\r\n"):
                raise Refused("invalid_account_did")
            for item in (labeler, value):
                if item is not None and (not isinstance(item, str) or len(item.encode()) > 512):
                    raise Refused("invalid_filter")
            if action not in (None, "application_observed", "removal_observed", "unknown_action"):
                raise Refused("invalid_action_filter")
            if target_kind not in (None, "account", "post"):
                raise Refused("invalid_target_filter")
            lo, hi, now = timestamp(start), timestamp(end), self.now()
            if not now - timedelta(days=self.limits.days) <= lo < hi <= now:
                raise Refused("out_of_window")
            if len(self._snapshots) >= self.limits.snapshots:
                raise Refused("snapshot_concurrency_limit")
            created_tick = self.monotonic()
            deadline = created_tick + self.limits.ttl_seconds
            rows, row_bytes, examined = [], 0, 0
            # No lease survives exit from this provider context.
            with self.provider.snapshot(did, iso(lo), iso(hi), self.limits.rows + 1,
                                        timeout_seconds=min(10, self.limits.ttl_seconds),
                                        max_bytes=self.limits.snapshot_bytes) as source:
                coverage = self._coverage(source["coverage"], lo, hi)
                frontier = source["frontier"]
                if not isinstance(frontier, str) or not frontier or len(frontier) > 256:
                    raise Refused("frontier_unavailable")
                for raw in source["rows"]:
                    examined += 1
                    if examined > self.limits.rows:
                        raise Refused("snapshot_row_limit")
                    if self.monotonic() >= deadline:
                        raise Refused("snapshot_expired")
                    # Bound serialization of each public value before building
                    # a duplicate JSON buffer; internal fields are never copied.
                    for field in ("labeler_did", "uri", "val", "ts", "observed_at", "src", "cid", "exp"):
                        text = raw.get(field)
                        if isinstance(text, str) and len(text) > self.limits.snapshot_bytes:
                            raise Refused("snapshot_byte_limit")
                    public = self._row(raw, did, lo, hi)
                    if labeler is not None and public["labeler_did"] != labeler:
                        continue
                    if value is not None and public["value"] != value:
                        continue
                    if action is not None and public["action"] != action:
                        continue
                    if target_kind == "account" and public["uri"] != did:
                        continue
                    if target_kind == "post" and not public["uri"].startswith("at://" + did + "/app.bsky.feed.post/"):
                        continue
                    blob = encoded(public)
                    row_bytes += len(blob)
                    if row_bytes > self.limits.snapshot_bytes:
                        raise Refused("snapshot_byte_limit")
                    rows.append(blob)
            rows.sort(key=lambda blob: (json.loads(blob)["observed_at"], blob))
            manifest = {"schema": "labelwatch.public-observations.v1", "account": did,
                        "interval": {"start": iso(lo), "end": iso(hi), "end_exclusive": True},
                        "window_days": self.limits.days, "coverage": coverage,
                        "frontier": frontier, "filters": {"labeler": labeler, "value": value,
                                                         "action": action, "target_kind": target_kind},
                        "count": len(rows), "current_state": "unknown",
                        "completeness": "all matching recorded rows in this snapshot; gaps disclosed",
                        "expires_at": iso(now + timedelta(seconds=self.limits.ttl_seconds))}
            # Identity covers both the public semantics and exact exported rows.
            manifest["content_sha256"] = hashlib.sha256(encoded(manifest) + b"\n" + b"\n".join(rows)).hexdigest()
            if row_bytes + len(encoded(manifest)) > self.limits.snapshot_bytes:
                raise Refused("snapshot_byte_limit")
            if self.monotonic() >= deadline:
                raise Refused("snapshot_expired")
            identity = secrets.token_hex(16)
            self._snapshots[identity] = _Snapshot(encoded(manifest), tuple(rows), deadline)
            return {"manifest": manifest, "cursor": self._cursor(identity, 0)}

    def page(self, cursor):
        with self._lock:
            identity, offset = self._decode(cursor)
            self._purge()
            snapshot = self._snapshots.get(identity)
            if snapshot is None:
                raise Refused("snapshot_expired_or_unavailable")
            if snapshot.requests >= self.limits.page_requests:
                del self._snapshots[identity]
                raise Refused("snapshot_retry_limit")
            if offset > len(snapshot.rows) or offset % self.limits.page_rows:
                raise Refused("invalid_cursor_offset")
            snapshot.requests += 1
            end = min(offset + self.limits.page_rows, len(snapshot.rows))
            terminal = end == len(snapshot.rows)
            return {"manifest": json.loads(snapshot.manifest), "offset": offset,
                    "rows": [json.loads(row) for row in snapshot.rows[offset:end]],
                    "terminal": terminal,
                    "next_cursor": None if terminal else self._cursor(identity, end)}

    def account(self, cursor):
        """View all already-bounded rows, without consuming a page/retry lease."""
        with self._lock:
            identity, _ = self._decode(cursor)
            self._purge()
            snapshot = self._snapshots.get(identity)
            if snapshot is None:
                raise Refused("snapshot_expired_or_unavailable")
            rows = [json.loads(row) for row in snapshot.rows]
            return {"manifest": json.loads(snapshot.manifest), "timeline": rows,
                    "summary": {"observed_events": len(rows),
                                "applications_observed": sum(r["action"] == "application_observed" for r in rows),
                                "removals_observed": sum(r["action"] == "removal_observed" for r in rows),
                                "current_state": "unknown"}, "export_cursor": cursor}


def account_html(account):
    """Account surface sharing the exact export snapshot and coverage."""
    esc = lambda value: html.escape(str(value), quote=True)
    manifest, summary = account["manifest"], account["summary"]
    events = "".join("<tr><td>" + "</td><td>".join(esc(row[k]) for k in
                    ("observed_at", "action", "value", "labeler_did", "uri")) + "</td></tr>"
                    for row in account["timeline"])
    export = "/exports?" + urlencode({"cursor": account["export_cursor"]})
    def select(name, options):
        return "<select name='" + name + "'>" + "".join(
            "<option value='" + val + "'" + (" selected" if manifest["filters"].get(name) == (val or None) else "") +
            ">" + label + "</option>" for val, label in options) + "</select>"
    return ("<!doctype html><html><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
            "<title>Recent observations</title><style>body{font:16px system-ui,sans-serif;max-width:1100px;"
            "margin:2rem auto;padding:0 1rem;color:#1e293b;background:#f8fafc}h1{font-size:1.7rem}"
            "table{border-collapse:collapse;width:100%;font-size:.9rem}th,td{text-align:left;padding:.6rem;"
            "border-bottom:1px solid #cbd5e1;overflow-wrap:anywhere}pre{white-space:pre-wrap;overflow-wrap:anywhere}"
            "input,select,button{padding:.55rem;margin:.2rem;border:1px solid #94a3b8;border-radius:.3rem}"
            "a{color:#075985}details{margin-top:1.5rem}caption{text-align:left;font-weight:600;margin:1rem 0}</style>"
            "<h1>Recent observations for " + esc(manifest["account"]) + "</h1>"
            "<p>" + esc(manifest["interval"]["start"]) + " to " + esc(manifest["interval"]["end"]) +
            " (end exclusive). Current label state: unknown.</p><p>" +
            esc(summary["applications_observed"]) + " applications observed; " +
            esc(summary["removals_observed"]) + " removals observed. Missing removal does not mean active.</p>"
            "<p>Coverage: " + esc(manifest["coverage"]["status"]) + ". Observations may contain gaps.</p>"
            "<pre>" + esc(json.dumps(manifest["coverage"]["gaps"])) + "</pre>"
            "<form action='/account'><input name='did' value='" + esc(manifest["account"]) + "'>"
            "<input name='start' value='" + esc(manifest["interval"]["start"]) + "'>"
            "<input name='end' value='" + esc(manifest["interval"]["end"]) + "'>"
            "<input name='labeler' placeholder='Labeler DID' value='" + esc(manifest["filters"]["labeler"] or "") + "'>"
            "<input name='value' placeholder='Label value' value='" + esc(manifest["filters"]["value"] or "") + "'>"
            + select("action", [("", "All observations"), ("application_observed", "Applications"),
                                 ("removal_observed", "Removals"), ("unknown_action", "Unknown event action")])
            + select("target_kind", [("", "Account and records"), ("account", "Account only"), ("post", "Posts only")]) +
            "<button>Filter observations</button></form><p><a href='" + esc(export) + "'>Export this period</a>"
            " — snapshot expires " + esc(manifest["expires_at"]) + ".</p>"
            "<table><caption>Observed timeline and attribution</caption><thead><tr>"
            "<th>Observed at</th><th>Event</th><th>Label</th><th>Labeler</th><th>Subject</th>"
            "</tr></thead><tbody>" + events + "</tbody></table><details><summary>Expert event rows</summary><pre>" +
            esc(json.dumps(account["timeline"], ensure_ascii=False, indent=2)) + "</pre></details></html>")
