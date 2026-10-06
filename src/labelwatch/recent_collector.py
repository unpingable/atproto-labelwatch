"""Finite collector adapter and explicit HTTP transport; no service activation.

Transport.request(operation, params, *, max_bytes, timeout_seconds) must enforce
both limits while reading, and return bytes. ``discover`` returns an explicitly
{sources:[{did,endpoint}],cursor?} page (endpoint may be unresolved/None): resolution belongs
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
    sources: int = 2048
    seconds: float = 10

    def __post_init__(self):
        for value, maximum in ((self.response_bytes, 1024*1024), (self.page_rows, 100),
                               (self.pages, 128), (self.sources, 2048)):
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
    if endpoint is None:
        return {'did': value['did'], 'endpoint': None}
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

    def discover(self):
        """One bounded discovery page; store atomically checkpoints its continuation.

        An incomplete/refused discovery never replaces the sticky source roster.
        Discovery is not evidence that a source is reachable or fully observed.
        """
        continuation = cursor(self.store.collector_discovery_cursor())
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
        self.store.remember_sources(rows, limit=self.limits.sources,
                                    request_cursor=continuation, next_cursor=next_cursor)
        return {'discovered': len(rows), 'cursor': next_cursor, 'terminal': next_cursor is None}

    def tick(self):
        """Finite pages per invocation; persisted per-attempt claims preserve fairness.

        Store.collector_claim_source atomically advances the bounded scheduling
        position before each attempt, including failed pages and process loss.
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
        ordered = rows
        deadline = self.monotonic()+self.limits.seconds
        results = []
        for _ in ordered[:self.limits.pages]:
            if self.monotonic() >= deadline:
                break
            item = self.store.collector_claim_source(limit=self.limits.sources)
            if item is None:
                break
            item = source(item)
            stamp = self.now()
            if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
                raise Refused('UTC observation clock required')
            attempt = self.store.begin_source_attempt(item['did'], stamp.isoformat())
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
                                                next_cursor=next_cursor, observed_at=accepted_at.isoformat(), attempt_token=attempt)
                results.append({'source': item['did'], 'status': 'accepted', 'rows': len(normalized),
                                'terminal': next_cursor is None, 'result': result})
            except Exception as exc:
                # Gap persistence failure is fatal: never report a recorded gap that was lost.
                end = max(self.now(), stamp+timedelta(microseconds=1))
                self.store.record_gap(stamp.isoformat(), end.isoformat(), 'source_unavailable')
                self.store.finish_source_attempt(item['did'], attempt)
                results.append({'source': item['did'], 'status': 'refused', 'reason': str(exc)[:160] if isinstance(exc, Refused) else type(exc).__name__})
        return {'attempted': len(results), 'deferred': len(rows)-len(results), 'results': results, 'coverage': 'unknown'}


class HTTPTransport:
    """Explicit synchronous entry to bounded async HTTP; never started on import.

    ``http_transport`` accepts an httpx async test transport. An async deadline
    includes resolution and streamed body reads. OS resolver/executor shutdown
    can outlive cancellation: the invoking service must also bound process life.
    No retries/redirects or ambient proxy credentials. Discovery retains unresolved DIDs, resolved on collection.
    """
    def __init__(self, http_transport=None):
        self.http_transport = http_transport

    def request(self, operation, params, *, max_bytes, timeout_seconds):
        import asyncio
        return asyncio.run(asyncio.wait_for(self._request(operation, params, max_bytes, timeout_seconds),
                                             timeout=timeout_seconds))

    async def _request(self, operation, params, max_bytes, timeout_seconds):
        import httpx
        from urllib.parse import quote, unquote
        from .resolve import resolve_service_endpoint
        remaining = max_bytes
        async with httpx.AsyncClient(transport=self.http_transport, trust_env=False,
                                     follow_redirects=False, timeout=timeout_seconds) as client:
            async def fetch(url, query=None):
                nonlocal remaining
                async with client.stream('GET', url, params=query, headers={'Accept': 'application/json',
                                                                         'Accept-Encoding': 'identity'}) as response:
                    response.raise_for_status()
                    if response.headers.get('content-encoding', 'identity') != 'identity':
                        raise Refused('encoded response refused')
                    body = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        remaining -= len(chunk)
                        if remaining < 0:
                            raise Refused('aggregate response byte ceiling')
                        body.extend(chunk)
                value = json.loads(body)
                if not isinstance(value, dict):
                    raise Refused('HTTP JSON object required')
                return value

            if operation == 'discover':
                query = {'collection': 'app.bsky.labeler.service', 'limit': params['limit']}
                if params['cursor'] is not None:
                    query['cursor'] = params['cursor']
                page = await fetch('https://bsky.network/xrpc/com.atproto.sync.listReposByCollection', query)
                repos = page.get('repos')
                if not isinstance(repos, list) or len(repos) > params['limit']:
                    raise Refused('discovery repository page ceiling')
                result = {'sources': [{'did': r['did'], 'endpoint': None} for r in repos],
                          'cursor': page.get('cursor')}
            elif operation == 'labels':
                did, endpoint = params['did'], params['endpoint']
                if endpoint is None:
                    if did.startswith('did:plc:'):
                        url = 'https://plc.directory/'+quote(did, safe=':')
                    elif did.startswith('did:web:'):
                        parts = did[len('did:web:'):].split(':')
                        authority = unquote(parts[0])
                        parsed = urlsplit('https://'+authority)
                        if parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment or not parsed.hostname:
                            raise Refused('invalid did:web authority')
                        path = '/'.join(quote(unquote(part), safe='') for part in parts[1:])
                        url = 'https://'+authority+('/'+path if path else '/.well-known')+'/did.json'
                    else:
                        raise Refused('unsupported DID method')
                    doc = await fetch(url)
                    if doc.get('id') != did:
                        raise Refused('DID document identity mismatch')
                    endpoint = source({'did': did, 'endpoint': resolve_service_endpoint(doc)})['endpoint']
                    if endpoint is None:
                        raise Refused('labeler endpoint unresolved')
                query = {'sources': did, 'uriPatterns': '*', 'limit': params['limit']}
                if params['cursor'] is not None:
                    query['cursor'] = params['cursor']
                result = await fetch(endpoint+'/xrpc/com.atproto.label.queryLabels', query)
            else:
                raise Refused('unknown transport operation')
            encoded = json.dumps(result, separators=(',', ':')).encode()
            if len(encoded) > max_bytes:
                raise Refused('adapter result byte ceiling')
            return encoded
