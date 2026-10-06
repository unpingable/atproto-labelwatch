"""Finite collector adapter; no network transport or service activation.

Transport.request(operation, params, *, max_bytes, timeout_seconds) must enforce
both limits while reading, and return bytes. ``discover`` returns an explicitly
resolved {sources:[{did,endpoint}],cursor?} page: endpoint/DID resolution belongs
to the transport, never a guessed endpoint. Store methods are an atomic adapter
contract, not a claim that legacy ingestion is wired to this collector.
"""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import json
import time
from urllib.parse import urlsplit

from .ingest import normalize_label, _is_valid_did
from .db import parse_target_did


class Refused(ValueError):
    pass


@dataclass(frozen=True)
class Limits:
    response_bytes: int = 1024 * 1024
    page_rows: int = 100
    pages: int = 128
    sources: int = 128
    seconds: float = 10

    def __post_init__(self):
        for value, maximum in ((self.response_bytes, 1024*1024), (self.page_rows, 1000),
                               (self.pages, 128), (self.sources, 1024)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise Refused('invalid finite collector limit')
        if not 0 < self.seconds <= 30:
            raise Refused('invalid collector deadline')


def cursor(value):
    if value is not None and (not isinstance(value, str) or not value or len(value.encode()) > 4096):
        raise Refused('invalid opaque cursor')
    return value


def source(value):
    if not isinstance(value, dict) or not isinstance(value.get('did'), str) or not _is_valid_did(value['did']):
        raise Refused('invalid source DID')
    endpoint = value.get('endpoint')
    if not isinstance(endpoint, str) or len(endpoint.encode()) > 2048:
        raise Refused('invalid source endpoint')
    url = urlsplit(endpoint)
    if url.scheme != 'https' or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise Refused('invalid source endpoint')
    return {'did': value['did'], 'endpoint': endpoint.rstrip('/')}


class Collector:
    def __init__(self, store, transport, limits=Limits(), monotonic=time.monotonic,
                 now=lambda: datetime.now(timezone.utc)):
        self.store, self.transport, self.limits = store, transport, limits
        self.monotonic, self.now = monotonic, now
        self.offset = 0

    def _request(self, operation, params, deadline):
        remaining = deadline-self.monotonic()
        if remaining <= 0:
            raise Refused('collector deadline exceeded')
        data = self.transport.request(operation, params, max_bytes=self.limits.response_bytes,
                                      timeout_seconds=remaining)
        if self.monotonic() >= deadline:
            raise Refused('collector deadline exceeded')
        if not isinstance(data, bytes) or len(data) > self.limits.response_bytes:
            raise Refused('response byte ceiling')
        try:
            value = json.loads(data)
        except (ValueError, UnicodeError) as exc:
            raise Refused('invalid response JSON') from exc
        if not isinstance(value, dict):
            raise Refused('response object required')
        return value

    def discover(self, continuation=None):
        """One bounded discovery page; caller keeps returned opaque continuation.

        An incomplete/refused discovery never replaces the sticky source roster.
        Discovery is not evidence that a source is reachable or fully observed.
        """
        continuation = cursor(continuation)
        value = self._request('discover', {'cursor': continuation, 'limit': self.limits.page_rows},
                              self.monotonic()+self.limits.seconds)
        rows = value.get('sources')
        if not isinstance(rows, list) or len(rows) > min(self.limits.page_rows, self.limits.sources):
            raise Refused('discovery page ceiling or missing sources')
        rows = [source(row) for row in rows]
        if len({row['did'] for row in rows}) != len(rows):
            raise Refused('duplicate discovered source')
        next_cursor = cursor(value.get('cursor'))
        if next_cursor is not None and next_cursor == continuation:
            raise Refused('discovery continuation did not advance')
        self.store.remember_sources(rows, limit=self.limits.sources)
        return {'discovered': len(rows), 'cursor': next_cursor, 'terminal': next_cursor is None}

    def tick(self):
        """One page per source per invocation; rotating start preserves fairness.

        Store.accept_page must atomically compare request_cursor, accept records,
        and persist next_cursor; absent next_cursor preserves the prior position.
        It must permit later changed terminal pages and deduplicate event identity.
        No transport failure advances continuation. Gaps are conservative intervals,
        not a claim that records were known to be missing in that interval.
        """
        rows = self.store.collector_sources(limit=self.limits.sources+1)
        if len(rows) > self.limits.sources:
            raise Refused('source roster ceiling')
        rows = sorted((source(row) for row in rows), key=lambda row: row['did'])
        if len({row['did'] for row in rows}) != len(rows):
            raise Refused('duplicate roster source')
        if not rows:
            return {'attempted': 0, 'results': [], 'coverage': 'unknown'}
        start = self.offset % len(rows)
        ordered = rows[start:]+rows[:start]
        deadline = self.monotonic()+self.limits.seconds
        results = []
        for item in ordered[:self.limits.pages]:
            if self.monotonic() >= deadline:
                break
            self.offset = (self.offset+1) % len(rows)
            stamp = self.now()
            if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
                raise Refused('UTC observation clock required')
            try:
                position = cursor(self.store.collector_cursor(item['did']))
                page = self._request('labels', {**item, 'cursor': position, 'limit': self.limits.page_rows}, deadline)
                labels = page.get('labels')
                if not isinstance(labels, list) or len(labels) > self.limits.page_rows:
                    raise Refused('label page ceiling or missing labels')
                next_cursor = cursor(page.get('cursor'))
                if next_cursor is not None and next_cursor == position:
                    raise Refused('label continuation did not advance')
                normalized = []
                for raw in labels:
                    if not isinstance(raw, dict) or raw.get('src') != item['did'] or raw.get('labeler_did', item['did']) != item['did']:
                        raise Refused('source attribution mismatch')
                    if not isinstance(raw.get('uri'), str) or not isinstance(raw.get('val'), str):
                        raise Refused('invalid label fields')
                    if 'neg' in raw and type(raw['neg']) is not bool:
                        raise Refused('invalid label action')
                    r = normalize_label(raw, strict_identity=True)
                    normalized.append((r.labeler_did,r.src,r.uri,r.cid,r.val,r.neg,r.exp,r.sig,r.ts,r.event_hash,parse_target_did(r.uri)))
                if self.monotonic() >= deadline:
                    raise Refused('collector deadline exceeded before commit')
                accepted_at = self.now()
                if accepted_at.tzinfo is None or accepted_at.utcoffset() != timedelta(0) or accepted_at < stamp:
                    raise Refused('observation clock regression')
                result = self.store.accept_page(normalized, source=item['did'], request_cursor=position,
                                                next_cursor=next_cursor, observed_at=accepted_at.isoformat())
                results.append({'source': item['did'], 'status': 'accepted', 'rows': len(normalized),
                                'terminal': next_cursor is None, 'result': result})
            except Exception as exc:
                # Gap persistence failure is fatal: never report a recorded gap that was lost.
                end = max(self.now(), stamp+timedelta(microseconds=1))
                self.store.record_gap(stamp.isoformat(), end.isoformat(), 'source_unavailable')
                results.append({'source': item['did'], 'status': 'refused', 'reason': type(exc).__name__})
        return {'attempted': len(results), 'deferred': len(rows)-len(results), 'results': results, 'coverage': 'unknown'}
