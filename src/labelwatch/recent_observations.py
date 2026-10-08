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
import uuid
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
        self._exports = {}
        self._lock = threading.RLock()

    def _purge(self):
        for key in list(self._snapshots):
            if self.monotonic() >= self._snapshots[key].deadline:
                del self._snapshots[key]

        for key in list(self._exports):
            if self.monotonic() >= self._exports[key]['deadline']:
                del self._exports[key]

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
        # Export the declared public observation projection, including a recorded
        # label signature; exclude internal probes and operational metadata.
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
        for name in ("src", "cid", "exp", "sig"):
            if row.get(name) is not None:
                if not isinstance(row[name], str):
                    raise Refused("invalid_public_event_field")
                answer[name] = row[name]
        return answer

    @staticmethod
    def _coverage(value, start, end):
        if not isinstance(value, dict) or value.get("status") not in ("observed", "gapped", "unknown"):
            raise Refused("coverage_unavailable")
        acquisition=value.get('acquisition')
        try:
            if not isinstance(acquisition,dict) or set(acquisition)!={'schema','generation_id','started_at','basis'}:
                raise ValueError('acquisition fields')
            if acquisition['schema']!='labelwatch.acquisition.v1' or acquisition['basis']!='local_acceptance_clock':
                raise ValueError('acquisition schema or basis')
            identity=uuid.UUID(acquisition['generation_id'])
            if identity.version!=4 or str(identity)!=acquisition['generation_id']:raise ValueError('acquisition identity')
            acquired=timestamp(acquisition['started_at'])
        except (ValueError,TypeError,AttributeError,KeyError) as exc:
            raise Refused('acquisition_metadata_unavailable') from exc
        acquisition={**acquisition,'started_at':iso(acquired)}
        warmup={'start':iso(start),'end':iso(min(end,acquired)),'reason':'not_observed'} if start<acquired else None
        gaps = []
        for gap in value.get("gaps", []):
            if len(gaps) >= 128:
                raise Refused("coverage_budget_exceeded")
            lo, hi = timestamp(gap["start"]), timestamp(gap["end"])
            if not start <= lo < hi <= end:
                raise Refused("invalid_coverage_gap")
            reason = gap.get("reason")
            if reason not in ("not_observed", "source_unavailable", "unknown", "ingest_paused", "cursor_reset", "maintenance", "warmup"):
                reason = "unknown"
            gaps.append({"start": iso(lo), "end": iso(hi), "reason": reason})
        if value["status"] == "observed" and (gaps or warmup):
            raise Refused("conflicting_coverage")
        return {"status": value["status"], "gaps": gaps, "acquisition":acquisition, "warmup":warmup,
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
            if len(self._snapshots)+len(self._exports) >= self.limits.snapshots:
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
                    for field in ("labeler_did", "uri", "val", "ts", "observed_at", "src", "cid", "exp", "sig"):
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
            if row_bytes + len(encoded(manifest)) + len(rows) + 128 > self.limits.snapshot_bytes:
                raise Refused("snapshot_byte_limit")
            if self.monotonic() >= deadline:
                raise Refused("snapshot_expired")
            identity = secrets.token_hex(16)
            self._snapshots[identity] = _Snapshot(encoded(manifest), tuple(rows), deadline)
            return {"manifest": manifest, "cursor": self._cursor(identity, 0)}

    def create_export(self, did, start, end, *, labeler=None, value=None, action=None, target_kind=None):
        """Fixed-ID acquisition frontier; finite pages without a retained lease."""
        with self._lock:
            self._purge()
            if not isinstance(did, str) or not did.startswith('did:') or len(did)>512 or any(c in did for c in '/?#\r\n'):
                raise Refused('invalid_account_did')
            if any(x is not None and (not isinstance(x,str) or len(x.encode())>512) for x in (labeler,value)):
                raise Refused('invalid_filter')
            if action not in (None,'application_observed','removal_observed','unknown_action') or target_kind not in (None,'account','post'):
                raise Refused('invalid_filter')
            lo,hi,now=timestamp(start),timestamp(end),self.now()
            if not now-timedelta(days=self.limits.days)<=lo<hi<=now:raise Refused('out_of_window')
            if len(self._snapshots)+len(self._exports)>=self.limits.snapshots:raise Refused('snapshot_concurrency_limit')
            identity=secrets.token_hex(16)
            state={'deadline':self.monotonic()+self.limits.ttl_seconds,'query':(did,iso(lo),iso(hi)),
                   'filters':{'labeler':labeler,'value':value,'action':action,'target_kind':target_kind},
                   'after':0,'upper':None,'next_index':0,'cache_index':-1,'cache':None,'retries':0,'count':0}
            prepared=self._export_read(state)
            state['upper']=prepared['upper_id']
            coverage=self._coverage(prepared['coverage'],lo,hi)
            state['acquisition']=coverage['acquisition']
            manifest={'schema':'labelwatch.public-observations.v1','account':did,
                      'interval':{'start':iso(lo),'end':iso(hi),'end_exclusive':True},
                      'window_days':self.limits.days,'coverage':coverage,'frontier':prepared['frontier'],
                      'filters':state['filters'],'count':None,'current_state':'unknown',
                      'completeness':'fixed acquisition frontier; complete only after terminal page; gaps disclosed',
                      'pagination':'stable event-ID keyset; no source lease between pages',
                      'expires_at':iso(now+timedelta(seconds=self.limits.ttl_seconds))}
            state['manifest']=encoded(manifest);state['digest']=hashlib.sha256(state['manifest'])
            state['prepared']=prepared
            if self.monotonic()>=state['deadline']:raise Refused('snapshot_expired_or_unavailable')
            self._exports[identity]=state
            return {'manifest':manifest,'cursor':self._cursor(identity,0),
                    'instructions':'Request /exports?cursor=<cursor>, then follow next_cursor until terminal=true. An expired or interrupted download is incomplete; the deadline is fixed.'}

    def _export_read(self,state):
        did,start,end=state['query']
        try:
            with self.provider.snapshot(did,start,end,self.limits.page_rows,
                    timeout_seconds=min(10,max(.001,state['deadline']-self.monotonic())),
                    max_bytes=max(1,self.limits.snapshot_bytes//2),
                    after_id=state['after'],upper_id=state['upper']) as source:
                result=dict(source);result['rows']=[];used=0
                for raw in source['rows']:
                    if len(result['rows'])>=self.limits.page_rows:raise Refused('provider_keyset_unavailable')
                    for field in ('labeler_did','uri','val','ts','observed_at','src','cid','exp','sig'):
                        value=raw.get(field)
                        if isinstance(value,str):
                            if len(value)>self.limits.snapshot_bytes:raise Refused('snapshot_byte_limit')
                            used+=len(value.encode())
                    if used>self.limits.snapshot_bytes:raise Refused('snapshot_byte_limit')
                    result['rows'].append(raw)
        except Refused:
            raise
        except Exception as exc:
            raise Refused('snapshot_expired_or_unavailable') from exc
        coverage=self._coverage(result['coverage'],timestamp(start),timestamp(end))
        if state.get('acquisition') is not None and coverage['acquisition']!=state['acquisition']:
            raise Refused('acquisition_generation_changed')
        upper,next_id,more=result.get('upper_id'),result.get('next_after_id'),result.get('has_more')
        if type(upper) is not int or type(next_id) is not int or type(more) is not bool or not state['after']<=next_id<=upper:
            raise Refused('provider_keyset_unavailable')
        if state['upper'] is not None and upper!=state['upper']:raise Refused('provider_frontier_changed')
        if more and next_id==state['after']:raise Refused('provider_keyset_not_advanced')
        ids=[row.get('id') for row in result['rows']]
        if len(ids)>self.limits.page_rows or any(type(x) is not int for x in ids) or ids!=sorted(set(ids)):
            raise Refused('provider_keyset_unavailable')
        if ids and (ids[0]<=state['after'] or ids[-1]!=next_id):raise Refused('provider_keyset_unavailable')
        return result

    def _export_page(self,identity,index,state):
        if index==state['cache_index']:
            state['retries']+=1
            if state['retries']>self.limits.page_requests:
                del self._exports[identity];raise Refused('snapshot_retry_limit')
            return json.loads(state['cache'])
        if index!=state['next_index'] or (state['cache'] is not None and json.loads(state['cache'])['terminal']):
            raise Refused('invalid_cursor_offset')
        try:
            source=state.pop('prepared',None)
            if source is None:source=self._export_read(state)
            did,start,end=state['query'];lo,hi=timestamp(start),timestamp(end)
            rows=[]
            for raw in source['rows']:
                public=self._row(raw,did,lo,hi);filters=state['filters']
                if filters['labeler'] is not None and public['labeler_did']!=filters['labeler']:continue
                if filters['value'] is not None and public['value']!=filters['value']:continue
                if filters['action'] is not None and public['action']!=filters['action']:continue
                if filters['target_kind']=='account' and public['uri']!=did:continue
                if filters['target_kind']=='post' and not public['uri'].startswith('at://'+did+'/app.bsky.feed.post/'):continue
                rows.append(public)
            terminal=not source['has_more']
            response={'manifest':json.loads(state['manifest']),'offset':index,'rows':rows,'terminal':terminal,
                      'next_cursor':None if terminal else self._cursor(identity,index+1)}
            digest=state['digest'].copy()
            for row in rows:digest.update(b'\n'+encoded(row))
            if terminal:response.update(count=state['count']+len(rows),content_sha256=digest.hexdigest())
            blob=encoded(response)
            if len(blob)>self.limits.snapshot_bytes:raise Refused('snapshot_byte_limit')
            if self.monotonic()>=state['deadline']:raise Refused('snapshot_expired_or_unavailable')
            state.update(after=source['next_after_id'],cache_index=index,next_index=index+1,cache=blob,
                         digest=digest,count=state['count']+len(rows))
            return response
        except Exception:
            del self._exports[identity]
            raise

    def page(self, cursor):
        with self._lock:
            identity, offset = self._decode(cursor)
            self._purge()
            if identity in self._exports:
                return self._export_page(identity,offset,self._exports[identity])
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

    def download(self, cursor):
        """Whole already-materialized account snapshot; no provider access."""
        with self._lock:
            identity,_=self._decode(cursor)
            self._purge()
            snapshot=self._snapshots.get(identity)
            if snapshot is None:raise Refused('snapshot_expired_or_unavailable')
            if snapshot.requests>=self.limits.page_requests:raise Refused('snapshot_retry_limit')
            answer={'manifest':json.loads(snapshot.manifest),'rows':[json.loads(row) for row in snapshot.rows],
                    'terminal':True,'count':len(snapshot.rows)}
            if len(encoded(answer))>self.limits.snapshot_bytes:raise Refused('snapshot_byte_limit')
            snapshot.requests+=1
            return answer

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


# Public gap-reason wording, shared by account views and the rolling-observatory pages.
GAP_REASONS={'not_observed':'Acquisition had not begun; this interval was not observed.',
             'source_unavailable':'A source was unavailable during this interval.',
             'ingest_paused':'Collection was stopped during this interval.',
             'cursor_reset':'Source continuation was reset; continuity is not established.',
             'maintenance':'Collection coverage was interrupted by maintenance.',
             'warmup':'The collector was warming up; observation coverage is not established.',
             'unknown':'Observation coverage is not established for this interval.'}


def account_html(account):
    """Human account view over the same bounded, normalized public snapshot."""
    esc=lambda value:html.escape(str(value),quote=True)
    manifest,summary=account['manifest'],account['summary']
    rows=account['timeline']
    def when(value):
        stamp=timestamp(value)
        return stamp.strftime('%b %d, %Y at %H:%M:%S')+(f'.{stamp.microsecond:06d}' if stamp.microsecond else '')+' UTC'
    export='/download?'+urlencode({'cursor':account['export_cursor']})
    cards=[('Applications observed',summary['applications_observed']),('Removals observed',summary['removals_observed']),
           ('Labelers',len({r['labeler_did'] for r in rows})),('Affected subjects',len({r['uri'] for r in rows}))]
    cards=''.join('<div class="card"><strong>'+esc(value)+'</strong><span>'+esc(label)+'</span></div>' for label,value in cards)
    timeline=[]
    for row in sorted(rows,key=lambda r:timestamp(r['observed_at']),reverse=True):
        action={'application_observed':'Application observed','removal_observed':'Removal observed'}.get(row['action'],'Action not established')
        kind='Account' if row['uri']==manifest['account'] else 'Post' if '/app.bsky.feed.post/' in row['uri'] else 'Record'
        timeline.append('<li><article><div class="event-head"><strong>'+esc(action)+'</strong><span class="kind">'+kind+'</span></div>'
                        '<p class="value">'+esc(row['value'])+'</p><p>From <code>'+esc(row['labeler_did'])+'</code></p>'
                        '<p class="muted">Observed '+esc(when(row['observed_at']))+' · Source authored '+esc(when(row['source_timestamp']))+'</p>'
                        '<p class="subject">'+esc(row['uri'])+'</p></article></li>')
    reasons=GAP_REASONS
    gaps=''.join('<li>'+esc(when(gap['start']))+' to '+esc(when(gap['end']))+': '+esc(reasons.get(gap['reason'],reasons['unknown']))+'</li>'
                 for gap in manifest['coverage']['gaps'])
    gap_content='<ul>'+gaps+'</ul>' if gaps else '<p>No specific gaps are recorded. This does not establish complete source coverage.</p>'
    acquisition=manifest['coverage']['acquisition']
    acquisition_content='<p>Local acquisition began '+esc(when(acquisition['started_at']))+'. Earlier source-authored dates are not historical observation times.</p>'
    warmup=manifest['coverage']['warmup']
    if warmup:acquisition_content+='<p>Warm-up: '+esc(when(warmup['start']))+' to '+esc(when(warmup['end']))+' was not observed by this acquisition generation.</p>'
    def field(name,label,value):
        return '<label>'+label+'<input name="'+name+'" value="'+esc(value)+'"></label>'
    def select(name,label,options):
        return '<label>'+label+'<select name="'+name+'">'+''.join('<option value="'+value+'"'+(' selected' if manifest['filters'].get(name)==(value or None) else '')+'>'+text+'</option>' for value,text in options)+'</select></label>'
    filters=field('did','Account DID',manifest['account'])+field('start','Interval start (UTC)',manifest['interval']['start'])+field('end','Interval end, exclusive (UTC)',manifest['interval']['end'])
    filters+=field('labeler','Labeler DID',manifest['filters']['labeler'] or '')+field('value','Label value',manifest['filters']['value'] or '')
    filters+=select('action','Observation type',[('','All observations'),('application_observed','Applications'),('removal_observed','Removals'),('unknown_action','Unknown action')])
    filters+=select('target_kind','Subject type',[('','All subjects'),('account','Account'),('post','Posts')])
    expert=''.join('<tr>'+''.join('<td>'+esc(row.get(k,''))+'</td>' for k in ('observed_at','action','value','labeler_did','uri','sig'))+'</tr>' for row in rows)
    empty='<p>No observations were recorded in this interval. This does not establish that no labels exist.</p>'
    return ('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
            '<title>Recent label observations</title><style>body{font:16px system-ui,sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem;color:#1e293b;background:#f8fafc}'
            'h1{font-size:1.8rem}h2{font-size:1.3rem}.account,.subject,code,td,pre{overflow-wrap:anywhere}.muted{color:#475569;font-size:.9rem}'
            '.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:1rem}.card,article,.coverage{background:white;border:1px solid #cbd5e1;border-radius:.6rem;padding:1rem}'
            '.card strong{display:block;font-size:2rem}.card span{font-size:.9rem}.coverage{margin:1rem 0}.timeline{list-style:none;padding:0}.timeline li{margin:1rem 0}'
            '.event-head{display:flex;justify-content:space-between;gap:1rem}.kind{background:#e2e8f0;padding:.2rem .6rem;border-radius:1rem;font-size:.85rem}'
            '.value{font-size:1.2rem;font-weight:600}.subject{font-size:.85rem}form{display:flex;flex-wrap:wrap;gap:.8rem}label{display:flex;flex-direction:column;font-size:.85rem;gap:.3rem}'
            'input,select,button{padding:.6rem;border:1px solid #94a3b8;border-radius:.3rem}a{color:#075985}table{border-collapse:collapse;width:100%;font-size:.85rem}'
            'th,td{text-align:left;padding:.5rem;border-bottom:1px solid #cbd5e1}details{margin:1.5rem 0}pre{white-space:pre-wrap}.table-wrap{overflow:auto}</style>'
            '<h1>Recent label observations</h1><p class="account">'+esc(manifest['account'])+'</p><p>'+esc(when(manifest['interval']['start']))+' to '+esc(when(manifest['interval']['end']))+' (end exclusive).</p>'
            '<div class="cards">'+cards+'</div><p><strong>Current label state is unknown.</strong> A recorded application or removal does not establish whether a label is currently active.</p>'
            '<section class="coverage"><h2>Observation coverage</h2><p>These are recorded observations, not a complete view of every source.</p>'+acquisition_content+gap_content+'</section>'
            '<h2>Filter observations</h2><form action="/account">'+filters+'<button>Apply filters</button></form>'
            '<p><a href="'+esc(export)+'">Export this period</a> · Expires '+esc(when(manifest['expires_at']))+'.</p>'
            '<h2>Timeline · newest first</h2>'+('<ol class="timeline">'+''.join(timeline)+'</ol>' if timeline else empty)+
            '<details><summary>Expert observation rows (normalized projection)</summary><p>Public label-record signatures are included when recorded; this view does not verify them. Internal probes and operational metadata are excluded.</p>'
            '<div class="table-wrap"><table><thead><tr><th>Observed</th><th>Action</th><th>Label</th><th>Labeler</th><th>Subject</th><th>Public signature</th></tr></thead><tbody>'+expert+'</tbody></table></div>'
            '<pre>'+esc(json.dumps(rows,ensure_ascii=False,indent=2))+'</pre></details></html>')


# ---------------------------------------------------------------------------
# Rolling-observatory pages. Presentation only: every figure comes from the
# aggregate overview envelope (labelwatch.recent-overview.v1); nothing here
# reads storage or changes account/export semantics.
# ---------------------------------------------------------------------------

PREVIOUS_EPOCH_END = '2026-10-06'
EPOCH_GAP_NOTES = {'pre_epoch': 'Source-authored before this epoch began (beyond the late allowance); skipped, not stored, and counted per source.'}


def _neutral_css():
    from pathlib import Path
    css = (Path(__file__).resolve().parent / '_instruments' / 'neutral.css').read_text(encoding='utf-8')
    return css.replace('url("fonts/', 'url("/instruments/fonts/')


_LAYOUT = ('body{margin:0}.nz-wrap{max-width:960px;margin:0 auto;padding:0 16px}main{padding:1.5rem 0 2rem}'
           'h1{font-size:clamp(2rem,6vw,3.4rem);margin:.4rem 0}h2{font-size:1.25rem;margin-top:2rem}'
           '.nz-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:1rem;margin:1rem 0}'
           '.nz-cards .nz-panel strong{display:block;font:700 2rem/1.1 var(--nz-serif)}.nz-cards .nz-panel span{color:var(--nz-muted);font-size:.9rem}'
           '.nz-muted{color:var(--nz-muted)}form{display:flex;flex-wrap:wrap;gap:.8rem;align-items:end}label{display:flex;flex-direction:column;gap:.3rem;font-size:.9rem}'
           'code,.nz-wrap td{overflow-wrap:anywhere}table{border-collapse:collapse;width:100%}td,th{text-align:left;padding:.4rem;border-bottom:1px solid var(--nz-rule)}'
           '.nz-state{border-left:4px solid var(--nz-signal);padding-left:1rem}')


def _esc(value):
    return html.escape(str(value), quote=True)


def _when(value):
    stamp = timestamp(value)
    return stamp.strftime('%b %d, %Y at %H:%M') + ' UTC'


def page_html(title, body):
    """Shared shell; the neutral-instruments stylesheet is reused verbatim."""
    nav = ('<nav><a href="/">Overview</a><a href="/#account">Account lookup</a>'
           '<a href="/about">About coverage</a><a href="/previous-epoch">Previous epoch</a></nav>')
    return ('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>' + _esc(title) + '</title><style>' + _neutral_css() + _LAYOUT + '</style>'
            '<body><div class="nz-wrap"><header class="nz-masthead"><span class="nz-family">Neutral Zone</span>'
            '<span class="nz-product">Labelwatch</span>' + nav + '</header><main>' + body + '</main>'
            '<footer class="nz-footer"><p>Labelwatch records observed label application and removal events. '
            'Current label state is unknown. Observations older than 30 days expire.</p></footer></div></body></html>')


def coverage_statement(started_at=None, window_days=30):
    """The public coverage statement (proposal Part 4, verbatim with N and the epoch start filled in)."""
    began = _esc(_when(started_at)) if started_at else 'a time that is currently unavailable'
    return (f'Labelwatch keeps a rolling window of the most recent {int(window_days)} days of public label observations. '
            'Older observations expire and are not served. '
            f'The current observation epoch began on {began}. '
            'Anything before that time was not observed by this epoch; it is shown as &quot;not observed&quot;, never as quiet. '
            'Source-authored dates on labels are not observation times. '
            'Inside the window, recorded gaps are disclosed with their reason. '
            'No recorded gaps does not prove complete source coverage, and no observations does not mean no labels exist. '
            'Labelwatch records observed application and removal events; it does not know whether a label is currently active. '
            f'Observations from the previous epoch, which ended {PREVIOUS_EPOCH_END}, are not part of this service. '
            'Requests outside the window are refused explicitly.')


def _epoch_day(elapsed_days, window):
    """Elapsed-day ordinal since epoch start (not observed coverage)."""
    return min(int(elapsed_days) + 1, int(window))


def _number(value):
    return 'unknown' if value is None else f'{value:,}'


def _account_form(interval=None):
    form = ('<section id="account" class="nz-panel"><h2>Look up an account</h2>'
            '<form action="/lookup"><label>Account DID or handle<input name="account" required '
            'placeholder="did:plc:… or name.bsky.social" size="34"></label>'
            '<button name="days" value="7">Last 7 days</button>'
            '<button class="secondary" name="days" value="30">Full 30-day window</button></form>'
            '<p class="nz-muted">Shows observed label events for that account inside the window. '
            'No observations does not mean no labels exist. Current label state is unknown.</p>')
    if interval:
        start, end = interval
        form += ('<details><summary>Choose an exact interval</summary><form action="/account">'
                 '<label>Account DID<input name="did" required></label>'
                 '<label>Start (UTC ISO timestamp)<input name="start" value="' + _esc(start) + '" required></label>'
                 '<label>End, exclusive (UTC ISO timestamp)<input name="end" value="' + _esc(end) + '" required></label>'
                 '<button>View this period</button></form>'
                 '<p class="nz-muted">Interval ends at the available source frontier, which may be earlier than now. '
                 'Account previews have a finite row budget; no truncated result is published.</p></details>')
    return form + '</section>'


def _exports_panel():
    return ('<section class="nz-panel"><h2>Exports</h2><p>Every account view links a whole-period JSON download. '
            'For periods larger than the preview budget, <code>/exports?did=…&amp;days=30</code> opens a bounded, '
            'paginated export with a fixed acquisition frontier. Exports are finite consistent snapshots and expire '
            'after their stated deadline; an interrupted download is incomplete.</p></section>')


def landing_html(view, interval=None, error=None):
    """Landing page for the rolling observatory.

    view is a labelwatch.recent-overview.v1 envelope (top labelers optional),
    or None when the store summary is unavailable.
    """
    body = ['<p class="nz-eyebrow">Rolling observatory</p><h1>Recently observed labeling activity</h1>'
            '<p>Labelwatch watches public ATProto labelers and records the label application and removal events it '
            'observes. It keeps a rolling 30-day window: observations older than 30 days expire and are not served.</p>'
            '<p class="nz-state"><strong>Labelwatch records observed application/removal events; current label state is unknown.</strong> '
            'A recorded application does not establish that a label is still active.</p>']
    if view is None:
        body.append('<section class="nz-panel"><h2>Observation summary unavailable</h2><p>The current observation summary '
                    'could not be read' + (' (' + _esc(error) + ')' if error else '') + '. This is absence of evidence, '
                    'not evidence that nothing was observed.</p></section>')
    else:
        cov, counts, acq = view['coverage'], view['counts'], view['acquisition']
        window = view['window_days']
        rail = [('Epoch began', _when(acq['started_at'])),
                ('Window', f'{window} days, rolling'),
                ('Day', f"{_epoch_day(cov['observed_days'], window)} of {window} since epoch start"),
                ('Recorded gaps', 'overflow' if cov['gap_overflow'] else str(len(cov['gaps'])))]
        body.append('<div class="nz-status-rail">' + ''.join('<span>' + _esc(k) + ' <strong>' + _esc(v) + '</strong></span>' for k, v in rail) + '</div>')
        cards = [('Events observed, last 24 hours', counts.get('events_24h')),
                 ('Events observed, last 7 days', counts.get('events_7d')),
                 (f'Events observed in the {window}-day window', counts.get('events_window')),
                 ('Labelers active, last 7 days', counts.get('labelers_active_7d'))]
        body.append('<h2>Recently observed activity</h2><div class="nz-cards">' + ''.join(
            '<div class="nz-panel"><strong>' + _esc(_number(v)) + '</strong><span>' + _esc(k) + '</span></div>' for k, v in cards) + '</div>'
            '<p class="nz-muted">Counts are of observations recorded by this epoch, not of everything that happened. '
            'A labeler that could not be reached contributes nothing.</p>')
        coverage = ['<section class="nz-panel"><h2>Coverage of this epoch</h2>',
                    '<p>Local acquisition began ' + _esc(_when(acq['started_at'])) + '. Earlier source-authored dates are not historical observation times.</p>']
        if cov['warmup']:
            coverage.append('<p><strong>Day ' + _esc(_epoch_day(cov['observed_days'], window)) + ' of ' + _esc(window)
                            + ' since epoch start (' + _esc(_when(acq['started_at'])) + ').</strong> Warm-up: ' + _esc(_when(cov['warmup']['start'])) + ' to '
                            + _esc(_when(cov['warmup']['end'])) + ' was not observed by this acquisition generation.</p>')
        else:
            coverage.append('<p>The full ' + _esc(window) + '-day window lies inside this epoch.</p>')
        if cov['gaps']:
            coverage.append('<h3>Collection interruptions</h3><ul>' + ''.join(
                '<li>' + _esc(_when(g['start'])) + ' to ' + _esc(_when(g['end'])) + ': ' + _esc(GAP_REASONS.get(g['reason'], GAP_REASONS['unknown'])) + '</li>'
                for g in cov['gaps']) + '</ul>')
            if cov['gap_overflow']:
                coverage.append('<p>More interruptions were recorded than can be listed; the whole window is disclosed as not established.</p>')
        else:
            coverage.append('<p>No specific gaps are recorded. This does not establish complete source coverage.</p>')
        src = cov['sources']
        coverage.append('<h3>Source roster</h3><table><tbody>' + ''.join(
            '<tr><th>' + _esc(k) + '</th><td>' + _esc(_number(src.get(key))) + '</td></tr>' for k, key in (
                ('Labelers enrolled', 'seeded'), ('Not yet started', 'unstarted'), ('Probing for the current head', 'probing'),
                ('Established (collecting from the epoch forward)', 'established'),
                ('Refused (could not be safely initialised)', 'refused'),
                ('Rows skipped as authored before this epoch', 'pre_epoch_skipped'),
                ('Rows skipped without a usable authored date', 'undated_skipped'))) + '</tbody></table>'
            '<p>Sources that could not be safely initialised are reported as unknown coverage, never as quiet. '
            'Established means collection is positioned at the epoch, not that coverage is complete.</p>')
        coverage.append('<p class="nz-muted">Observations older than ' + _esc(window) + ' days expire. '
                        'Requests outside the window are refused explicitly.</p></section>')
        body.append(''.join(coverage))
        if view.get('top_labelers_7d'):
            body.append('<h2>Most active labelers, last 7 days</h2><table><thead><tr><th>Labeler service</th><th>Events observed</th></tr></thead><tbody>'
                        + ''.join('<tr><td><code>' + _esc(r['labeler_did']) + '</code></td><td>' + _esc(_number(r['count'])) + '</td></tr>'
                                  for r in view['top_labelers_7d']) + '</tbody></table>')
    body.append(_account_form(interval))
    body.append(_exports_panel())
    body.append('<p><a href="/about">How coverage works</a> · <a href="/previous-epoch">About the previous epoch</a></p>')
    return page_html('Labelwatch rolling observatory', ''.join(body))


def about_html(started_at=None, window_days=30):
    glossary = ''.join('<tr><th><code>' + _esc(k) + '</code></th><td>' + _esc(v) + '</td></tr>'
                       for k, v in list(GAP_REASONS.items()) + list(EPOCH_GAP_NOTES.items()))
    return page_html('About Labelwatch coverage',
                     '<p class="nz-eyebrow">About</p><h1>What this observatory covers</h1>'
                     '<section class="nz-panel"><p>' + coverage_statement(started_at, window_days) + '</p></section>'
                     '<h2>Gap reasons</h2><p>Each disclosed interval carries one of these reasons.</p>'
                     '<table><tbody>' + glossary + '</tbody></table>')


def previous_epoch_html():
    return page_html('Previous Labelwatch epoch',
                     '<p class="nz-eyebrow">Previous epoch</p><h1>The previous epoch is withdrawn</h1>'
                     '<section class="nz-panel"><p>The previous Labelwatch observation epoch ended on ' + PREVIOUS_EPOCH_END + '. '
                     'Its pages, census, labeler and alert reports are withdrawn and are not served by this service. '
                     'Its observations are not part of the current rolling window, and nothing from it is presented '
                     'as observed by the current epoch.</p></section>'
                     '<p><a href="/">Return to the current rolling observatory</a></p>')


def lookup_refusal_html(reason):
    return page_html('Account lookup', '<h1>Account lookup</h1><section class="nz-panel"><p>' + _esc(reason) + '</p>'
                     '<p>Enter the account DID (for example <code>did:plc:…</code>).</p></section>' + _account_form())


REFUSAL_TEXT = {
    'out_of_window': 'That interval is outside the rolling 30-day window. Older observations expire and are not served.',
    'invalid_account_did': 'That is not an account DID.',
    'snapshot_expired_or_unavailable': 'This view or export has expired or is no longer available. Start it again.',
    'observation_window_unavailable': 'No observation window is available yet.',
}


def refusal_html(code):
    """Small browser page for an explicit refusal; the JSON form is unchanged."""
    text = REFUSAL_TEXT.get(code, 'This request was refused.')
    return page_html('Request refused', '<h1>Request refused</h1><section class="nz-panel"><p>' + _esc(text) + '</p>'
                     '<p class="nz-muted">Refusal: <code>' + _esc(code) + '</code></p></section>'
                     '<p><a href="/">Return to the rolling observatory</a></p>')
