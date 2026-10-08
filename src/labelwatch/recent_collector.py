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
import re
import time
from urllib.parse import urlsplit

from .ingest import normalize_label, _is_valid_did
from .db import parse_target_did


class Refused(ValueError):
    pass


# Bounded head establishment for cursorless sources (tools/segmented_qualification/BOOTSTRAP.md).
# Verified 2026-10-08 against Ozone and skyware-style labelers: decimal id cursors,
# next > request on every non-empty page, empty page beyond the head.
PROBE_ROWS = 50
TOTAL_PROBES = 72          # >= 1 + ceil(53/4) gallop + 53 bisection steps over [1, 2**53]
CURSOR_MAX = 2**53
LATE_ALLOWANCE = timedelta(hours=48)
STORE_HOLD = 'rotate daily observation owner before ingestion'
WAL_PINNED = 'wal_budget_pinned'
STORE_CONDITIONS = (STORE_HOLD, WAL_PINNED)
ENDPOINT_TTL = timedelta(hours=24)
FUTURE_ALLOWANCE = timedelta(seconds=300)
# Verified id-ordered providers advance about one id per row (mod.bsky.app 50-61 ids
# per 50 rows, skyware 50 per 50); urbanism.plus jumped 1728 ids for 50 rows.
CURSOR_JUMP = 2
JUMP_MIN_ROWS = 10      # any page this size or larger, whatever the provider's page cap
REJECT_STATUSES = {'400', '401', '403', '404', '410'}


def _deterministic(reason):
    """A provider answer that will not change by retrying (count-bounded), else None."""
    match = re.fullmatch(r'provider status (\d{3})', reason or '')
    if match and match.group(1) in REJECT_STATUSES:
        return match.group(1)
    if reason == 'source attribution mismatch':
        return 'attribution'
    return None
DECIMAL = re.compile(r'(0|[1-9][0-9]{0,15})')


def _decimal(value):
    if value is None or not DECIMAL.fullmatch(value) or int(value) > CURSOR_MAX:
        return None
    return int(value)


def _source_time(value):
    """Source-authored time is used only to refuse pre-epoch rows, never as acquisition time."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class Limits:
    response_bytes: int = 1024 * 1024
    page_rows: int = 100
    pages: int = 128
    sources: int = 2048
    seconds: float = 10
    request_seconds: float = 1
    bootstrap_probes: int = 8

    def __post_init__(self):
        for value, maximum in ((self.response_bytes, 1024*1024), (self.page_rows, 100),
                               (self.pages, 128), (self.sources, 2048), (self.bootstrap_probes, 16)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise Refused('invalid finite collector limit')
        if not 0 < self.seconds <= 30 or not 0 < self.request_seconds <= min(10,self.seconds):
            raise Refused('invalid collector deadline')


def cursor(value):
    if value is not None and (not isinstance(value, str) or not value or len(value.encode()) > 4096):
        raise Refused('invalid opaque cursor')
    return value


PRIVATE_NAMES = ('localhost',)
PRIVATE_SUFFIXES = ('.localhost', '.local', '.internal')


def _private_address(text):
    """True for loopback, private, link-local, unique-local, reserved or unspecified IPs."""
    import ipaddress
    try:
        address = ipaddress.ip_address(text.strip('[]').split('%')[0])
    except ValueError:
        return None          # not a literal IP
    if getattr(address, 'ipv4_mapped', None):
        address = address.ipv4_mapped
    return (address.is_private or address.is_loopback or address.is_link_local or address.is_reserved
            or address.is_multicast or address.is_unspecified)


def public_host(host):
    """Refuse hosts that would point a collector request at the collector's own network.

    Literal IPs in loopback/private/link-local/ULA ranges and local-only names are
    refused here; names are also checked after resolution (HTTPTransport). The unit's
    IPAddressDeny is the second layer against DNS rebinding.
    """
    if not host:
        return False
    name = host.lower().rstrip('.')
    private = _private_address(name)
    if private is not None:
        return not private
    return name not in PRIVATE_NAMES and not name.endswith(PRIVATE_SUFFIXES)


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
    if not public_host(url.hostname):
        raise Refused('non-public source endpoint refused')
    return {'did': value['did'], 'endpoint': endpoint.rstrip('/')}


class Collector:
    def __init__(self, store, transport, limits=Limits(), monotonic=time.monotonic,
                 now=lambda: datetime.now(timezone.utc), sleep=None, stale_seconds=60):
        self.store, self.transport, self.limits = store, transport, limits
        # sleep=None: a round ends when nothing is due; the service passes time.sleep
        # so a round can wait for per-source spacing inside its own deadline.
        self.monotonic, self.now, self.sleep = monotonic, now, sleep
        # Attempts older than the unit's stop deadline (45 s + grace) were lost
        # with a killed process; they are disclosed and cleared at round start.
        self.stale_seconds = stale_seconds

    def _request(self, operation, params, deadline):
        resolutions = getattr(self.transport, 'resolutions', None)
        if isinstance(resolutions, dict):
            resolutions.clear()
        started=self.monotonic()
        remaining = deadline-started
        if remaining <= 0:
            raise Refused('collector deadline exceeded')
        allowance=min(remaining,self.limits.request_seconds)
        request_deadline=started+allowance
        data = self.transport.request(operation, params, max_bytes=self.limits.response_bytes,
                                      timeout_seconds=allowance)
        if self.monotonic() >= request_deadline:
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

    def run_round(self, discover=False):
        """A discovery outage does not suppress already-enrolled ready work."""
        discovery=None
        if discover:
            started=self.now()
            try:discovery={'status':'accepted',**self.discover()}
            except Exception as exc:
                ended=max(self.now(),started+timedelta(microseconds=1))
                self.store.record_gap(started.isoformat(),ended.isoformat(),'source_unavailable')
                discovery={'status':'refused','reason':str(exc)[:160] if isinstance(exc,Refused) else type(exc).__name__}
        return {'discovery':discovery,'collection':self.tick()}

    def tick(self):
        """Finite pages per invocation; persisted per-attempt claims preserve fairness.

        Store.collector_claim_source atomically advances the bounded scheduling
        position before each attempt, including failed pages and process loss.
        Store.accept_page must atomically compare request_cursor, accept records,
        and persist next_cursor; absent next_cursor preserves the prior position.
        It must permit later changed terminal pages and deduplicate event identity.
        No transport failure advances continuation. Gaps are conservative intervals,
        not a claim that records were known to be missing in that interval.

        A cursorless source is never traversed from its history: it is first
        bootstrapped (bounded probes, one request per claim, at most
        Limits.bootstrap_probes per source per tick, TOTAL_PROBES ever).
        Probes share the tick's page budget and deadline with ready sources.
        """
        rows = self.store.collector_sources(limit=self.limits.sources+1)
        if len(rows) > self.limits.sources:
            raise Refused('source roster ceiling')
        rows = sorted((source(row) for row in rows), key=lambda row: row['did'])
        if len({row['did'] for row in rows}) != len(rows):
            raise Refused('duplicate roster source')
        if not rows:
            return {'attempted': 0, 'results': [], 'coverage': 'unknown'}
        floor = _source_time(self.store.collector_epoch())
        if floor is None:
            raise Refused('acquisition epoch unavailable')
        floor -= LATE_ALLOWANCE
        deadline = self.monotonic()+self.limits.seconds
        sweep = getattr(self.store, 'collector_sweep_attempts', None)
        lost = sweep(self.now().isoformat(), self.stale_seconds) if sweep else []
        results, probes, capped, held = [], {}, 0, None
        # Reserved share: at least ceil(attempts/3) of a tick's attempts are claimed
        # for bootstrapping sources. The store orders that claim in-progress first
        # (most probes used), then unstarted round-robin, so a source mid-search
        # keeps its priority across ticks. Only share claims count toward the share.
        # Sources that hit the per-tick probe cap or failed this tick are excluded.
        attempts, probe_attempts, excluded = 0, 0, set()
        slots, waits = self.limits.pages, 0
        while slots > 0:
            # A wait for spacing does not use a page slot; waits are bounded too.
            slots -= 1
            if self.monotonic() >= deadline:
                if not results:
                    held = 'collector deadline exceeded before any attempt'
                break
            stamp = self.now()
            if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
                raise Refused('UTC observation clock required')
            admission = getattr(self.store, 'collector_admission', None)
            held = admission(stamp.isoformat()) if admission else None
            if held:
                self._maintenance_gap(stamp, reason=held)
                break
            item, shared = None, False
            extra = {'exclude': sorted(excluded)} if excluded else {}
            if probe_attempts*3 < attempts+1:
                item = self.store.collector_claim_source(limit=self.limits.sources, observed_at=stamp.isoformat(),
                                                         probing=True, **extra)
                shared = item is not None
            if item is None:
                item = self.store.collector_claim_source(limit=self.limits.sources,observed_at=stamp.isoformat(), **extra)
            if item is None:
                # Nothing due (per-source spacing, quiet backoff): wait for the next
                # due source if it falls inside this round's deadline, else stop.
                next_due = getattr(self.store, 'collector_next_due', None)
                due = _source_time(next_due()) if next_due else None
                wait = (due-stamp).total_seconds() if due is not None else None
                if self.sleep is None or wait is None or wait <= 0 or self.monotonic()+wait >= deadline \
                        or waits >= self.limits.pages:
                    break
                self.sleep(wait)
                slots, waits = slots+1, waits+1
                continue
            item = source(item)
            state = self.store.collector_source_state(item['did'])
            resolved = _source_time(state.get('endpoint_resolved_at'))
            if resolved is not None and stamp-resolved >= ENDPOINT_TTL:
                item = {**item, 'endpoint': None}     # re-resolve a cached endpoint daily
            booting = state['cursor'] is None and state['phase'] in ('unstarted', 'probing')
            if booting and probes.get(item['did'], 0) >= self.limits.bootstrap_probes:
                excluded.add(item['did'])
                capped += 1   # Per-tick probe share spent; yield the slot, never loop.
                if capped > len(rows):
                    break
                continue
            capped = 0
            attempts += 1
            if shared:
                probe_attempts += 1
            if booting:
                probes[item['did']] = probes.get(item['did'], 0)+1
                if probes[item['did']] >= self.limits.bootstrap_probes:
                    excluded.add(item['did'])
            attempt = self.store.begin_source_attempt(item['did'], stamp.isoformat())
            try:
                if booting:
                    results.append(self._bootstrap(item, state, attempt, stamp, deadline, floor))
                    continue
                position = cursor(state['cursor'])
                page = self._request('labels', {**item, 'cursor': position, 'limit': self.limits.page_rows}, deadline)
                normalized, next_cursor = self._page(item, page, self.limits.page_rows)
                if self._endpoint_changed(item):
                    self.store.finish_source_attempt(item['did'], attempt, observed_at=self.now().isoformat(), failure=False)
                    results.append({'source': item['did'], 'status': 'endpoint_changed'})
                    continue
                results.append(self._accept(item, normalized, position, next_cursor, attempt, stamp, deadline, floor,
                                            self.limits.page_rows))
            except Exception as exc:
                end = max(self.now(), stamp+timedelta(microseconds=1))
                reason = str(exc)[:160] if isinstance(exc, Refused) else type(exc).__name__
                condition = next((c for c in STORE_CONDITIONS if isinstance(exc, RuntimeError) and c in str(exc)), None)
                if condition:
                    # The store, not the source, refused (UTC rotation window, or a WAL
                    # pinned by readers): one store-level maintenance gap, round stops.
                    self._maintenance_gap(stamp, end, reason=condition)
                    self.store.finish_source_attempt(item['did'], attempt, observed_at=end.isoformat(), failure=False)
                    held = condition
                    break
                # One source's failure is per-source state, never a store-wide gap.
                # Our own resolution is kept even when the labels request failed.
                self._endpoint_changed(item)
                excluded.add(item['did'])     # backoff applies; never re-claimed this tick
                rejected = _deterministic(reason)
                extra = {'rejected': rejected} if rejected else {}
                self.store.finish_source_attempt(item['did'], attempt, observed_at=end.isoformat(), reason=reason, **extra)
                results.append({'source': item['did'], 'status': 'refused', 'reason': reason})
        summary = getattr(self.store, 'collector_bootstrap_summary', None)
        if held:
            results.append({'source': None, 'status': 'refused', 'scope': 'store', 'reason': held[:160]})
        attempted = [row for row in results if row['source'] is not None]
        round_status = 'refused' if held and not attempted else 'ran'
        return {'attempted': len(attempted), 'deferred': len(rows)-len({row['source'] for row in attempted}), 'results': results,
                'round': {'status': round_status, 'store_hold': held, 'lost_attempts': len(lost)}, 'refused_sources': [row['source'] for row in attempted if row['status'] == 'refused'],
                'bootstrap': summary() if summary else None, 'coverage': 'unknown'}

    def _endpoint_changed(self, item):
        """Persist our own DID resolution; True when the provider namespace changed.

        The resolution arrives on the transport's side channel, never in a
        provider-authored body: a page cannot redirect a source or reset its budget.
        """
        resolutions = getattr(self.transport, 'resolutions', None)
        endpoint = resolutions.pop(item['did'], None) if isinstance(resolutions, dict) else None
        persist = getattr(self.store, 'collector_resolved_endpoint', None)
        if endpoint is None or persist is None:
            return False
        return persist(item['did'], source({'did': item['did'], 'endpoint': endpoint})['endpoint'],
                       self.now().isoformat())['changed']

    def _maintenance_gap(self, stamp, end=None, reason=STORE_HOLD):
        """One store-wide interval per hold; overlapping rounds merge in the store.

        The daily-owner hold has refused ingestion since midnight UTC, so the
        interval starts there and every round of the hold extends one row.
        """
        start = stamp.replace(hour=0, minute=0, second=0, microsecond=0) if reason == STORE_HOLD else stamp
        end = max(end or stamp, start+timedelta(microseconds=1))
        self.store.record_gap(start.isoformat(), end.isoformat(), 'maintenance')

    def _page(self, item, page, rows):
        labels = page.get('labels')
        if not isinstance(labels, list) or len(labels) > rows:
            raise Refused('label page ceiling or missing labels')
        next_cursor = cursor(page.get('cursor'))
        if not labels:
            # Verified: skyware-style labelers answer an empty page with cursor "0";
            # following it would restart the whole history. An empty page never moves.
            next_cursor = None
        normalized = []
        for raw in labels:
            if not isinstance(raw, dict) or raw.get('src') != item['did'] or raw.get('labeler_did', item['did']) != item['did']:
                raise Refused('source attribution mismatch')
            if not isinstance(raw.get('uri'), str) or not isinstance(raw.get('val'), str):
                raise Refused('invalid label fields')
            if 'neg' in raw and type(raw['neg']) is not bool:
                raise Refused('invalid label action')
            if not isinstance(raw.get('cts'), str) or not raw['cts']:
                # No source time: no identity and no epoch decision. Skip this row
                # (counted undated), never refuse the page that carries it.
                normalized.append(None)
                continue
            r = normalize_label(raw, strict_identity=True)
            normalized.append((r.labeler_did,r.src,r.uri,r.cid,r.val,r.neg,r.exp,r.sig,r.ts,r.event_hash,parse_target_did(r.uri)))
        return normalized, next_cursor

    def _accept(self, item, normalized, position, next_cursor, attempt, stamp, deadline, floor, limit):
        if next_cursor is not None and next_cursor == position:
            raise Refused('label continuation did not advance')
        before, after = _decimal(position), _decimal(next_cursor)
        if before is not None and after is not None and after <= before:
            raise Refused('label continuation regressed')
        if self.monotonic() >= deadline:
            raise Refused('collector deadline exceeded before commit')
        accepted_at = self.now()
        if accepted_at.tzinfo is None or accepted_at.utcoffset() != timedelta(0) or accepted_at < stamp:
            raise Refused('observation clock regression')
        # Bounded late arrival: the allowance applies at acceptance time as well
        # as at the epoch, so a source first reached late never backfills history.
        late_floor = max(floor, accepted_at-LATE_ALLOWANCE)
        kept, pre_epoch, late, undated, future = [], 0, 0, 0, 0
        for row in normalized:
            authored = None if row is None else _source_time(row[8])
            if authored is None:
                undated += 1
            elif authored < floor:
                pre_epoch += 1      # Epoch-only: never a silent historical backfill.
            elif authored < late_floor:
                late += 1
            elif authored > accepted_at+FUTURE_ALLOWANCE:
                future += 1
            else:
                kept.append(row)
        skipped = {k: v for k, v in (('pre_epoch', pre_epoch), ('undated', undated), ('late', late), ('future', future)) if v}
        extra = {'skipped': skipped} if skipped else {}
        if normalized and not kept and next_cursor is not None:
            # Any page (any size) with zero accepted rows and an advancing cursor
            # counts toward the history cap; one undated row does not reset it.
            extra['history'] = True
        if len(normalized) >= JUMP_MIN_ROWS and before is not None and after is not None and after-before > CURSOR_JUMP*len(normalized):
            # Disclosure heuristic, not a loss detector: ids skipped by the provider.
            extra['order_evidence'] = {'kind': 'cursor_jump', 'request': position, 'next': next_cursor,
                                       'rows': len(normalized)}
        result = self.store.accept_page(kept, source=item['did'], request_cursor=position,
                                        next_cursor=next_cursor, observed_at=accepted_at.isoformat(), attempt_token=attempt, **extra)
        return {'source': item['did'], 'status': 'accepted', 'rows': len(kept), 'pre_epoch_skipped': pre_epoch,
                'late_skipped': late, 'undated_skipped': undated, 'future_skipped': future, 'terminal': next_cursor is None, 'result': result}

    def _bootstrap(self, item, state, attempt, stamp, deadline, floor):
        """One probe: gallop x16 then bisect on the numeric cursor until a confirmed short page.

        Invariant: lo is a provider cursor at or below the head when it was seen
        (a returned id, or a probe that still had a full page beyond it), so
        starting at lo never skips a record created later; hi had nothing beyond
        it when probed. Every outcome is persisted before the next request.

        A short page is the head only once confirmed: the probe at its numeric
        continuation must return fewer rows than it did (review r3). A
        continuation that returns as many rows (>= 10) reveals a provider page
        cap; that cap then defines a full page for the rest of the search.
        """
        expect = {k: state.get(k) for k in ('phase', 'probes', 'lo', 'hi', 'confirm')}
        conf = json.loads(state['confirm']) if state.get('confirm') else {}
        cap = conf.get('cap')
        lo = _decimal(state['lo']) if state['lo'] is not None else None
        hi = _decimal(state['hi']) if state['hi'] is not None else None
        if state['phase'] == 'probing' and ((lo is None and 'next' not in conf) or (state['hi'] is not None and hi is None)):
            raise RuntimeError('persisted bootstrap bracket invalid')
        step = lambda **kw: self.store.bootstrap_step(item['did'], attempt, self.now().isoformat(), expect, **kw)
        report = lambda value, **kw: {'source': item['did'], 'status': 'bootstrap', 'phase': value['phase'],
                                      'probes': value['probes'], **kw}
        keep = lambda extra=None: json.dumps({**({'cap': cap} if cap else {}), **(extra or {})}, sort_keys=True) \
            if cap or extra else None
        rows = min(PROBE_ROWS, self.limits.page_rows)
        confirming = 'next' in conf
        if confirming:
            x = _decimal(conf['next'])
            if x is None:
                raise RuntimeError('persisted bootstrap confirmation invalid')
        elif state['phase'] == 'unstarted':
            x = None
        elif hi is not None and hi-lo <= 1:
            # Restart after a persisted converged bracket: no request needed.
            return report(step(phase='established', lo=str(lo), hi=str(hi), established=str(lo)))
        elif lo >= CURSOR_MAX:
            return report(step(phase='refused', lo=str(lo), reason='cursor_out_of_range'))
        else:
            x = min(max(lo*16, lo+1), CURSOR_MAX) if hi is None else (lo+hi)//2
        position = None if x is None else str(x)
        page = self._request('labels', {**item, 'cursor': position, 'limit': rows}, deadline)
        normalized, next_cursor = self._page(item, page, rows)
        if self._endpoint_changed(item):
            raise Refused('resolved endpoint changed; bootstrap restarts')
        lo_s, hi_s = state['lo'], state['hi']
        if confirming:
            served, before = len(normalized), conf['rows']
            if served < before:
                # Fewer rows (or none) beyond the continuation: the short page was the head.
                established = conf['at']
                if established is not None and _decimal(established) is None:
                    raise RuntimeError('persisted bootstrap confirmation invalid')
                return report(step(phase='established', lo=lo_s, hi=hi_s, established=established, confirm=None),
                              confirmed=True)
            if served < rows:
                cap = max(cap or 0, served)   # adopt the larger size as the provider page cap
        full = cap or rows
        if not normalized and x is not None:
            hi = x
        elif len(normalized) < full or next_cursor is None:
            following = _decimal(next_cursor)
            if following is not None and (x is None or following > x):
                # Unconfirmed short page: confirm at its continuation before trusting it.
                value = step(phase='probing', lo=lo_s, hi=hi_s,
                             confirm=keep({'at': position, 'next': next_cursor, 'rows': len(normalized)}))
                return report(value, probe=position, rows=len(normalized), confirming=True)
            # Terminal or opaque continuation: everything after `position` is in hand.
            value = step(phase='established', lo=lo_s, hi=hi_s, established=position, finish=False)
            return {**self._accept(item, normalized, position, next_cursor, attempt, stamp, deadline, floor, full),
                    'bootstrap': value['phase'], 'probes': value['probes']}
        else:
            # Further probing needs a verified numeric, advancing cursor; anything
            # else is a deterministic refusal (coverage unknown), never a traversal.
            following = _decimal(next_cursor)
            if following is None:
                reason = 'cursor_out_of_range' if DECIMAL.fullmatch(next_cursor) else 'opaque_cursor'
                return report(step(phase='refused', lo=lo_s, hi=hi_s, reason=reason))
            if x is not None and following <= x:
                return report(step(phase='refused', lo=lo_s, hi=hi_s, reason='cursor_regression'))
            lo = x if x is not None else following
        if lo is None:
            raise Refused('bootstrap bracket unavailable')
        if hi is not None and hi-lo <= 1:
            if x == lo:   # The full page for lo is in hand: it is the first collected page.
                value = step(phase='established', lo=str(lo), hi=str(hi), established=str(lo), finish=False)
                return {**self._accept(item, normalized, position, next_cursor, attempt, stamp, deadline, floor, full),
                        'bootstrap': value['phase'], 'probes': value['probes']}
            return report(step(phase='established', lo=str(lo), hi=str(hi), established=str(lo)))
        return report(step(phase='probing', lo=str(lo), hi=None if hi is None else str(hi), confirm=keep()),
                      probe=position, rows=len(normalized))


class HTTPTransport:
    """Explicit synchronous entry to bounded async HTTP; never started on import.

    ``http_transport`` accepts an httpx async test transport. An async deadline
    includes resolution and streamed body reads. OS resolver/executor shutdown
    can outlive cancellation: the invoking service must also bound process life.
    A context spans one finite invocation, so a request timeout does not join
    resolver workers before other sources run. Four resolver workers bound memory;
    exhaustion by uncancellable DNS still limits progress. No retries/redirects
    or ambient proxy credentials. Discovery retains unresolved DIDs.
    """
    def __init__(self, http_transport=None):
        self.http_transport = http_transport
        self._loop=None
        self._executor=None
        self._client=None   # one client per invocation (tick), reused across requests
        self.resolutions={}  # did -> endpoint from our own DID resolution (never from a body)

    def __enter__(self):
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        if self._loop is not None:raise RuntimeError('transport invocation already open')
        self._loop=asyncio.new_event_loop()
        self._executor=ThreadPoolExecutor(max_workers=4,thread_name_prefix='labelwatch-resolver')
        self._loop.set_default_executor(self._executor)
        return self

    def __exit__(self,*exc):
        # A timed-out resolver must not force executor shutdown before the next
        # enrolled source is attempted. Join only after this finite invocation.
        # OS DNS may still stall here; the enclosing process deadline is required.
        try:
            if self._client is not None:
                self._loop.run_until_complete(self._client.aclose())
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._executor.shutdown(wait=True,cancel_futures=True)
        finally:
            self._loop.close();self._loop=None;self._executor=None;self._client=None

    def request(self, operation, params, *, max_bytes, timeout_seconds):
        import asyncio
        if self._loop is None:
            with self:
                return self.request(operation,params,max_bytes=max_bytes,timeout_seconds=timeout_seconds)
        return self._loop.run_until_complete(asyncio.wait_for(
            self._request(operation,params,max_bytes,timeout_seconds),timeout=timeout_seconds))

    async def _public_resolution(self, url):
        """Refuse a name that resolves to a non-public address (best effort; see public_host)."""
        import asyncio
        if not public_host(url.hostname):
            raise Refused('non-public host refused')
        if _private_address(url.hostname) is not None:
            return
        infos = await asyncio.get_running_loop().getaddrinfo(url.hostname, url.port or 443)
        if any(_private_address(info[4][0]) for info in infos):
            raise Refused('host resolves to a non-public address')

    async def _request(self, operation, params, max_bytes, timeout_seconds):
        import httpx
        from urllib.parse import quote, unquote
        from .resolve import resolve_service_endpoint
        remaining = max_bytes
        if self._client is None:
            self._client = httpx.AsyncClient(transport=self.http_transport, trust_env=False,
                                             follow_redirects=False, timeout=timeout_seconds)
        client = self._client
        async def fetch(url, query=None):
            nonlocal remaining
            if self.http_transport is None:
                await self._public_resolution(urlsplit(url))
            async with client.stream('GET', url, params=query, timeout=timeout_seconds,
                                     headers={'Accept': 'application/json', 'Accept-Encoding': 'identity'}) as response:
                if response.status_code >= 400:
                    # Deterministic 4xx answers are count-bounded by the store.
                    raise Refused(f'provider status {response.status_code}')
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
                    if not public_host(parsed.hostname):
                        raise Refused('non-public did:web authority refused')
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
                resolved = endpoint
                # Our own resolution is reported before the labels request, so it is
                # kept even if that request fails (side channel, never the body).
                self.resolutions[did] = resolved
            else:
                resolved = None
            query = {'sources': did, 'uriPatterns': '*', 'limit': params['limit']}
            if params['cursor'] is not None:
                query['cursor'] = params['cursor']
            result = await fetch(endpoint+'/xrpc/com.atproto.label.queryLabels', query)
            result.pop('_resolved_endpoint', None)   # provider bodies never carry our state
        else:
            raise Refused('unknown transport operation')
        encoded = json.dumps(result, separators=(',', ':')).encode()
        if len(encoded) > max_bytes:
            raise Refused('adapter result byte ceiling')
        return encoded
