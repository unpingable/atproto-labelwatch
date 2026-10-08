"""Opt-in recent-observation candidate; new isolated stores only.

Thirty full UTC days, with one partial boundary owner. Source authored time is
never used as the observation clock. No lifetime deduplication or continuity
claim survives retirement. This is not an enrollment/migration tool.
"""
from __future__ import annotations
import datetime as dt
from contextlib import closing
import json
import hashlib
import sqlite3
import uuid
from pathlib import Path
from storage import Store, owned, connect, lock, db, custody, ingest, sha, syncdir

# Bounded head establishment (BOOTSTRAP.md). A cursorless new-epoch source
# probes numeric provider cursors; it never traverses history page by page.
# total_probes is the store cap (72); the collector's proven bound is 68
# (1 + ceil(53/4) gallop + 53 bisection steps). history_pages caps consecutive
# full all-skipped pages after establishment; reject_attempts caps consecutive
# deterministic provider rejections; endpoint_seconds re-resolves endpoints.
BOOTSTRAP_POLICY={'total_probes':72,'cursor_max':2**53,'history_pages':16,'reject_attempts':6,'endpoint_seconds':86400}
BOOTSTRAP_PHASES=('unstarted','probing','established','refused')
SCHEDULER_POLICY={'warm_seconds':1,'quiet_seconds':30,'failure_base_seconds':30,'failure_max_seconds':900,'failure_counter_max':6,'hot_streak_max':4}
_UNSET = object()
UTC = dt.timezone.utc
EPOCH = dt.datetime(1970,1,1,tzinfo=UTC)
TIMED = {'alerts':'ts', 'labeler_evidence':'ts', 'labeler_probe_history':'ts',
         'derived_receipts':'ts', 'ingest_outcomes':'ts', 'discovery_events':'discovered_at',
         'posted_findings':'posted_at', 'quarantined_events':'last_quarantined_at'}
DERIVED = {'derived_label_fp','derived_labeler_lag_7d','derived_labeler_reversal_7d',
           'derived_labeler_boundary_load_7d','derived_val_dist_day','derived_labeler_entropy_7d',
           'derived_author_day','derived_author_labeler_day','boundary_edges','boundary_targets'}
CONTROL = {'meta','label_events','labelers','provider_registry','sqlite_sequence','q_segments',
           'q_pending','q_transition','q_archive','q_hot_keys','custody_archives','q_recent_seen',
           'q_recent_transition','q_recent_protected','q_recent_counts','q_recent_gaps','q_recent_sources'}

LIMITS={'meta':4096,'provider_registry':10000,'labelers':100000,
        'q_recent_seen':100000000,'q_hot_keys':100000000,'custody_archives':32,
        'q_segments':33,'q_recent_protected':32,'q_recent_gaps':128,'q_recent_sources':2048}
LIMITS.update({name:10000000 for name in TIMED.keys() | DERIVED})


def clock(value):
    parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise ValueError('explicit UTC observation clock required')
    return parsed.astimezone(UTC)


def iso(value):
    return value.isoformat(timespec='microseconds').replace('+00:00', 'Z')


def micros(value):
    delta=clock(value)-EPOCH if isinstance(value,str) else value-EPOCH
    return (delta.days*86400+delta.seconds)*1000000+delta.microseconds


def from_micros(value):
    return iso(EPOCH+dt.timedelta(microseconds=value))


def day_number(owner):
    return (dt.date.fromisoformat(owner)-EPOCH.date()).days


def day_name(number):
    return (EPOCH.date()+dt.timedelta(days=number)).isoformat()


class RecentStore(Store):
    @classmethod
    def create(cls, root, now, days=30):
        if days != 30: raise ValueError('only 30-day candidate admitted; 45 is unqualified')
        stamp = clock(now)
        # Custody source-time floor remains separate from observation retention.
        store = super().create(root, period=stamp.date().isoformat(), floor='0001-01-01T00:00:00Z')
        (store.root/'archive').mkdir()
        with closing(connect(store.state)) as c:
            c.executescript('''
            CREATE TABLE q_recent_seen(event_id INTEGER PRIMARY KEY, owner_day INTEGER NOT NULL,
                observed_us INTEGER NOT NULL, target_did TEXT NOT NULL);
            CREATE INDEX recent_account_clock ON q_recent_seen(target_did,observed_us,event_id);
            CREATE INDEX recent_owner ON q_recent_seen(owner_day);
            CREATE TABLE q_recent_transition(singleton INTEGER PRIMARY KEY CHECK(singleton=1), body TEXT NOT NULL);
            CREATE TABLE q_recent_protected(owner TEXT PRIMARY KEY, dependency TEXT NOT NULL);
            CREATE TABLE q_recent_gaps(start TEXT NOT NULL,end TEXT NOT NULL,reason TEXT NOT NULL,
                PRIMARY KEY(start,end,reason));
            CREATE TABLE q_recent_counts(name TEXT PRIMARY KEY,n INTEGER NOT NULL,ceiling INTEGER NOT NULL);
            CREATE TABLE q_recent_sources(did TEXT PRIMARY KEY,endpoint TEXT,
                provider_cursor TEXT,last_page TEXT,last_observed TEXT,attempt_token TEXT,attempt_start TEXT,
                hot INTEGER NOT NULL DEFAULT 0 CHECK(hot IN (0,1,2)),next_due TEXT,
                failures INTEGER NOT NULL DEFAULT 0 CHECK(failures BETWEEN 0 AND 6),
                boot_phase TEXT NOT NULL DEFAULT 'unstarted' CHECK(boot_phase IN ('unstarted','probing','established','refused')),
                boot_probes INTEGER NOT NULL DEFAULT 0 CHECK(boot_probes BETWEEN 0 AND 72),
                boot_lo TEXT,boot_hi TEXT,boot_cursor TEXT,boot_started TEXT,boot_at TEXT,boot_reason TEXT,
                pre_epoch_skipped INTEGER NOT NULL DEFAULT 0 CHECK(pre_epoch_skipped>=0),
                undated_skipped INTEGER NOT NULL DEFAULT 0 CHECK(undated_skipped>=0),
                unavailable_since TEXT,last_failure_at TEXT,last_failure_reason TEXT CHECK(length(last_failure_reason)<=160),
                late_skipped INTEGER NOT NULL DEFAULT 0 CHECK(late_skipped>=0),
                accepted_rows INTEGER NOT NULL DEFAULT 0 CHECK(accepted_rows>=0),
                history_streak INTEGER NOT NULL DEFAULT 0 CHECK(history_streak BETWEEN 0 AND 16),
                cursor_order_unverified INTEGER NOT NULL DEFAULT 0 CHECK(cursor_order_unverified IN (0,1)),
                cursor_order_evidence TEXT CHECK(length(cursor_order_evidence)<=1024),
                provider_rejects INTEGER NOT NULL DEFAULT 0 CHECK(provider_rejects BETWEEN 0 AND 6),
                endpoint_resolved_at TEXT);
            ''')
            for key, value in {'version':'7','days':'30','start':iso(stamp.replace(hour=0,minute=0,second=0,microsecond=0)-dt.timedelta(days=29)),
                               'end':iso(stamp),'generation':'0','clock':iso(stamp),'events':'0'}.items():
                db.set_meta(c, 'q:recent_'+key, value)
            db.set_meta(c,'q:recent_start',iso(stamp-dt.timedelta(days=30)))
            db.set_meta(c,'q:recent_acquisition_start',iso(stamp))
            db.set_meta(c,'q:recent_acquisition',json.dumps({'schema':'labelwatch.acquisition.v1',
                'generation_id':str(uuid.uuid4()),'started_at':iso(stamp),'basis':'local_acceptance_clock'},sort_keys=True))
            db.set_meta(c,'q:recent_gap_overflow','0')
            for table,ceiling in LIMITS.items():
                count=c.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                c.execute('INSERT INTO q_recent_counts VALUES(?,?,?)',(table,count,ceiling))
                c.executescript(f'''CREATE TRIGGER recent_count_added_{table} AFTER INSERT ON "{table}" BEGIN
                    SELECT CASE WHEN (SELECT n>=ceiling FROM q_recent_counts WHERE name='{table}') THEN RAISE(ABORT,'recent row ceiling') END;
                    UPDATE q_recent_counts SET n=n+1 WHERE name='{table}'; END;
                CREATE TRIGGER recent_count_deleted_{table} AFTER DELETE ON "{table}" BEGIN
                    UPDATE q_recent_counts SET n=n-1 WHERE name='{table}'; END;''')
            c.commit()
        return cls(root)

    def require(self, c):
        if db.get_meta(c,'q:recent_version') != '7':
            raise RuntimeError('recent schema v7 required; no implicit conversion; old evidence preserved')
        self.acquisition(c)

    @staticmethod
    def acquisition(c):
        """Creation identity is independent of advancing retention generations.

        No migration fallback: missing metadata never manufactures a start time.
        This identifies local acceptance, not historical source completeness.
        """
        try:
            value=json.loads(db.get_meta(c,'q:recent_acquisition') or 'null')
            if not isinstance(value,dict) or set(value)!={'schema','generation_id','started_at','basis'}:
                raise ValueError('acquisition fields')
            if value['schema']!='labelwatch.acquisition.v1' or value['basis']!='local_acceptance_clock':
                raise ValueError('acquisition schema or basis')
            identity=uuid.UUID(value['generation_id'])
            if identity.version!=4 or str(identity)!=value['generation_id']:raise ValueError('acquisition identity')
            started=clock(value['started_at'])
            if iso(started)!=value['started_at'] or value['started_at']!=db.get_meta(c,'q:recent_acquisition_start'):
                raise ValueError('acquisition start binding')
            if started>clock(db.get_meta(c,'q:recent_end')) or started>clock(db.get_meta(c,'q:recent_clock')):
                raise ValueError('acquisition frontier ordering')
            return value
        except (ValueError,TypeError,AttributeError,KeyError) as exc:
            raise RuntimeError('trusted acquisition metadata unavailable') from exc

    def recover(self, c, death=None):
        self.require(c)
        if c.execute('SELECT 1 FROM q_recent_transition').fetchone():
            raise RuntimeError('recent maintenance recovery required before reads or writes')
        return super().recover(c, death)

    def policies(self, c):
        tables={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        unknown=tables-set(TIMED)-DERIVED-CONTROL
        if unknown: raise RuntimeError('unclassified global tables: '+','.join(sorted(unknown)))
        return {name: ('observation-window' if name=='q_recent_seen' else
                       'time-expiry' if name in TIMED else 'invalidate-on-maintenance' if name in DERIVED
                       else 'bounded-control-or-owner') for name in sorted(tables)}

    def caps(self,c,scan=True):
        self.policies(c)
        # Finite fail-closed limits; workload viability must be measured separately.
        for table,n,maximum in c.execute('SELECT name,n,ceiling FROM q_recent_counts'):
            if n<0 or n>maximum: raise RuntimeError('recent state row ceiling: '+table)
            if scan and c.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]!=n:
                raise RuntimeError('recent counter correspondence: '+table)
        if c.execute('SELECT 1 FROM meta WHERE length(value)>65536 LIMIT 1').fetchone():
            raise RuntimeError('recent control value ceiling')

    @staticmethod
    def validate_rows(rows):
        if len(rows)>10000: raise RuntimeError('bounded ingest page exceeded')
        if any(any(isinstance(value,str) and len(value.encode())>16384 for value in row) for row in rows):
            raise RuntimeError('recent event field byte ceiling')
        sizes=[len(json.dumps(row,ensure_ascii=False).encode()) for row in rows]
        if any(n>65536 for n in sizes) or sum(sizes)>16*1024**2:raise RuntimeError('recent event/batch byte ceiling')

    def _stage_observations(self,c,rows,stamp,death=None):
        if stamp<clock(db.get_meta(c,'q:recent_clock')):raise RuntimeError('observation clock regression')
        end=iso(stamp+dt.timedelta(microseconds=1))
        owner=db.get_meta(c,'q:active')
        if owner!=stamp.date().isoformat():raise RuntimeError('rotate daily observation owner before ingestion')
        accepted,duplicates=custody.stage(c,rows,owner,death)
        count=int(db.get_meta(c,'q:recent_events'))+len(accepted)
        if count>100000000:raise RuntimeError('recent observation cardinality ceiling')
        evidence=set()
        for event_id,row in accepted:
            c.execute('INSERT INTO q_pending VALUES (?,?)',(event_id,owner))
            target=row[2][5:].split('/')[0] if row[2].startswith('at://') else row[2]
            c.execute('INSERT INTO q_recent_seen VALUES (?,?,?,?)',(event_id,day_number(owner),micros(stamp),target))
            db.upsert_labeler(c,row[0],iso(stamp));ingest._track_observed_src(c,row[1] or row[0],iso(stamp),evidence)
        db.set_meta(c,'q:recent_clock',iso(stamp));db.set_meta(c,'q:recent_end',end)
        db.set_meta(c,'q:recent_events',str(count))
        return accepted,duplicates

    def ingest(self, rows, source, cursor, observed_at, death=None, injected=None):
        stamp=clock(observed_at)
        self.validate_rows(rows)
        if len(source)>1024 or len(str(cursor))>4096: raise RuntimeError('source continuation size ceiling')
        self.check_local(injected)
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c)
                if stamp<clock(db.get_meta(c,'q:recent_clock')): raise RuntimeError('observation clock regression')
                owner=db.get_meta(c,'q:active')
                if owner!=stamp.date().isoformat(): raise RuntimeError('rotate daily observation owner before ingestion')
                continuation='q:recent_source:'+hashlib.sha256(source.encode()).hexdigest()
                batch=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                previous=db.get_meta(c,continuation)
                if db.get_cursor(c,source)==cursor and previous:
                    saved=json.loads(previous)
                    if saved['batch']!=batch:raise RuntimeError('opaque continuation reused with different batch')
                    return {'inserted':0,'accepted_ids':[],'duplicate_ids':[],'cursor':cursor,'replayed_page':True}
                c.execute('BEGIN IMMEDIATE')
                accepted,duplicates=self._stage_observations(c,rows,stamp,death)
                db.set_meta(c,continuation,json.dumps({'batch':batch,'count':len(rows)}))
                self.caps(c,scan=False)
                if death: death('before_recent_ingest_commit')
                db.set_cursor(c,source,cursor)
                if death: death('after_recent_ingest_commit')
                self.flush(c)
                return {'inserted':len(accepted),'accepted_ids':[x[0] for x in accepted], 'duplicate_ids':duplicates,'cursor':cursor}
            finally:c.close()

    def remember_sources(self,rows,limit=2048,request_cursor=_UNSET,next_cursor=_UNSET):
        discovery=request_cursor is not _UNSET or next_cursor is not _UNSET
        if discovery:
            for value in (request_cursor,next_cursor):
                if value is not None and (not isinstance(value,str) or len(value.encode())>4096):raise ValueError("discovery cursor bound")
        if not 0<limit<=2048 or len(rows)>limit:raise RuntimeError('source enrollment ceiling')
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                if discovery and db.get_meta(c,'q:recent_discovery_cursor')!=request_cursor:raise RuntimeError('discovery continuation changed')
                for row in rows:
                    did,endpoint=row['did'],row.get('endpoint')
                    if not isinstance(did,str) or not did.startswith('did:') or len(did.encode())>1024:raise ValueError('invalid source DID')
                    if endpoint is not None and (not isinstance(endpoint,str) or len(endpoint.encode())>2048 or not endpoint.startswith('https://')):raise ValueError('invalid source endpoint')
                    old=c.execute('SELECT endpoint FROM q_recent_sources WHERE did=?',(did,)).fetchone()
                    if old and old[0] and endpoint and old[0]!=endpoint:raise RuntimeError('source endpoint change requires explicit cursor reset')
                    c.execute('INSERT INTO q_recent_sources(did,endpoint) VALUES (?,?) ON CONFLICT(did) DO UPDATE SET endpoint=COALESCE(q_recent_sources.endpoint,excluded.endpoint)',(did,endpoint))
                if c.execute('SELECT COUNT(*) FROM q_recent_sources').fetchone()[0]>limit:raise RuntimeError('source enrollment ceiling; no discovered source dropped')
                if discovery:
                    if next_cursor is None:c.execute("DELETE FROM meta WHERE key='q:recent_discovery_cursor'")
                    else:db.set_meta(c,'q:recent_discovery_cursor',next_cursor)
                self.caps(c,scan=False);c.commit()
            finally:c.close()

    def collector_discovery_cursor(self):
        with closing(connect(self.state,readonly=True)) as c:
            self.require(c)
            return db.get_meta(c,'q:recent_discovery_cursor')

    def begin_source_attempt(self,did,observed_at):
        stamp=iso(clock(observed_at));token=str(uuid.uuid4())
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                row=c.execute('SELECT attempt_token,attempt_start FROM q_recent_sources WHERE did=?',(did,)).fetchone()
                if row is None:raise RuntimeError('source is not enrolled')
                if stamp<db.get_meta(c,'q:recent_clock'):raise RuntimeError('observation clock regression')
                if row[0]:
                    if stamp<=row[1]:raise RuntimeError('pending attempt requires advancing recovery clock')
                    self._record_gap(c,row[1],stamp,'unknown')
                c.execute('UPDATE q_recent_sources SET attempt_token=?,attempt_start=? WHERE did=?',(token,stamp,did))
                self.caps(c,scan=False);c.commit();return token
            finally:c.close()

    def finish_source_attempt(self,did,attempt_token,observed_at=None,failure=True,reason=None,rejected=None):
        """A single source's failure is per-source state, never a store-wide gap.

        unavailable_since keeps the first failed attempt start until a success.
        """
        if reason is not None and (not isinstance(reason,str) or len(reason)>160):raise ValueError('failure reason bound')
        if rejected is not None and (not isinstance(rejected,str) or len(rejected)>32):raise ValueError('rejection bound')
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                row=c.execute('SELECT attempt_token,attempt_start,failures FROM q_recent_sources WHERE did=?',(did,)).fetchone()
                if row is None or not attempt_token or row[0]!=attempt_token:raise RuntimeError('source attempt changed')
                stamp=clock(observed_at or max(row[1],db.get_meta(c,'q:recent_clock')))
                if iso(stamp)<row[1]:raise RuntimeError('attempt clock regression')
                failures=min(SCHEDULER_POLICY['failure_counter_max'],row[2]+1) if failure else 0
                delay=min(SCHEDULER_POLICY['failure_max_seconds'],SCHEDULER_POLICY['failure_base_seconds']*2**(failures-1)) if failure else SCHEDULER_POLICY['quiet_seconds']
                due=iso(stamp+dt.timedelta(seconds=delay))
                c.execute('UPDATE q_recent_sources SET attempt_token=NULL,attempt_start=NULL,hot=0,next_due=?,failures=? WHERE did=?',(due,failures,did))
                if failure:
                    c.execute('UPDATE q_recent_sources SET unavailable_since=COALESCE(unavailable_since,?),last_failure_at=?,last_failure_reason=? WHERE did=?',
                              (row[1],iso(stamp),reason or 'unspecified',did))
                    # Deterministic provider rejections are count-bounded (structural refusal);
                    # transient failures keep the backoff and reset the count.
                    rejects=c.execute('SELECT provider_rejects FROM q_recent_sources WHERE did=?',(did,)).fetchone()[0]
                    rejects=min(BOOTSTRAP_POLICY['reject_attempts'],rejects+1) if rejected else 0
                    c.execute('UPDATE q_recent_sources SET provider_rejects=? WHERE did=?',(rejects,did))
                    if rejected and rejects>=BOOTSTRAP_POLICY['reject_attempts']:
                        c.execute("UPDATE q_recent_sources SET boot_phase='refused',boot_reason=? WHERE did=?",('bootstrap_refused:provider_rejects:'+rejected,did))
                c.commit()
            finally:c.close()

    def collector_sources(self,limit):
        if not 0<limit<=2049:raise ValueError('source listing bound')
        c=connect(self.state,readonly=True)
        try:
            self.require(c)
            return [dict(r) for r in c.execute('SELECT did,endpoint FROM q_recent_sources ORDER BY did LIMIT ?',(limit,))]
        finally:c.close()

    def collector_source_status(self,limit):
        """Per-source bootstrap disclosure rows for the product (bounded listing)."""
        if not 0<limit<=2048:raise ValueError('source listing bound')
        c=connect(self.state,readonly=True)
        try:
            self.require(c)
            return [dict(r) for r in c.execute('''SELECT did,boot_phase AS bootstrap,boot_probes AS bootstrap_probes,
                boot_started AS bootstrap_started_at,boot_at AS bootstrap_established_at,boot_cursor AS bootstrap_cursor,boot_reason AS bootstrap_reason,
                pre_epoch_skipped,undated_skipped,provider_cursor IS NOT NULL AS positioned,
                unavailable_since,last_failure_at,last_failure_reason,failures,
                late_skipped,accepted_rows,cursor_order_unverified,cursor_order_evidence,provider_rejects,
                undated_skipped>0 AND accepted_rows=0 AS undated_only,endpoint_resolved_at
                FROM q_recent_sources ORDER BY did LIMIT ?''',(limit,))]
        finally:c.close()

    def collector_admission(self,observed_at):
        """None when ingestion at observed_at is admitted; else a store-wide reason."""
        stamp=clock(observed_at)
        with closing(connect(self.state,readonly=True)) as c:
            self.require(c)
            if c.execute('SELECT 1 FROM q_recent_transition').fetchone():return 'recent maintenance recovery required'
            if db.get_meta(c,'q:active')!=stamp.date().isoformat():return 'rotate daily observation owner before ingestion'
            return None

    def collector_epoch(self):
        """Acquisition start: the epoch floor for source-authored rows (minus the late allowance)."""
        with closing(connect(self.state,readonly=True)) as c:
            self.require(c);return self.acquisition(c)['started_at']

    def collector_source_state(self,did):
        c=connect(self.state,readonly=True)
        try:
            self.require(c)
            row=c.execute('SELECT provider_cursor,boot_phase,boot_probes,boot_lo,boot_hi,endpoint_resolved_at FROM q_recent_sources WHERE did=?',(did,)).fetchone()
            if row is None:raise RuntimeError('source is not enrolled')
            return {'cursor':row[0],'phase':row[1],'probes':row[2],'lo':row[3],'hi':row[4],'endpoint_resolved_at':row[5]}
        finally:c.close()

    def collector_resolved_endpoint(self,did,endpoint,resolved_at):
        """Persist a transport-resolved endpoint (1 GET per request afterwards).

        A configured endpoint (never resolved here) is not replaced. A changed
        resolved endpoint is a different provider namespace: the source restarts
        bootstrap from that provider's head (never continues an old cursor).
        """
        if not isinstance(endpoint,str) or len(endpoint.encode())>2048 or not endpoint.startswith('https://'):raise ValueError('invalid source endpoint')
        stamp=iso(clock(resolved_at))
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                row=c.execute('SELECT endpoint,endpoint_resolved_at FROM q_recent_sources WHERE did=?',(did,)).fetchone()
                if row is None:raise RuntimeError('source is not enrolled')
                if row[0] is not None and row[1] is None:c.rollback();return {'changed':False,'configured':True}
                changed=row[0] is not None and row[0]!=endpoint
                c.execute('UPDATE q_recent_sources SET endpoint=?,endpoint_resolved_at=? WHERE did=?',(endpoint,stamp,did))
                if changed:
                    c.execute("""UPDATE q_recent_sources SET provider_cursor=NULL,last_page=NULL,boot_phase='unstarted',boot_probes=0,
                        boot_lo=NULL,boot_hi=NULL,boot_cursor=NULL,boot_at=NULL,boot_reason=NULL,history_streak=0,
                        last_failure_reason='endpoint_changed' WHERE did=? AND boot_phase!='refused'""",(did,))
                c.commit();return {'changed':changed}
            finally:c.close()

    def collector_bootstrap_summary(self):
        """Per-source acquisition disclosure; established is not completeness."""
        c=connect(self.state,readonly=True)
        try:
            self.require(c)
            phases={phase:0 for phase in BOOTSTRAP_PHASES}
            for phase,n in c.execute('SELECT boot_phase,COUNT(*) FROM q_recent_sources GROUP BY boot_phase'):phases[phase]=n
            # A positioned source predating this bootstrap is collected forward as before.
            positioned=c.execute("SELECT COUNT(*) FROM q_recent_sources WHERE boot_phase IN ('unstarted','probing') AND provider_cursor IS NOT NULL").fetchone()[0]
            skipped=c.execute('SELECT COALESCE(SUM(pre_epoch_skipped),0),COALESCE(SUM(undated_skipped),0),COALESCE(SUM(late_skipped),0) FROM q_recent_sources').fetchone()
            unordered=[{'did':r[0],'evidence':json.loads(r[1]) if r[1] else None} for r in c.execute(
                'SELECT did,cursor_order_evidence FROM q_recent_sources WHERE cursor_order_unverified=1 ORDER BY did LIMIT 2048')]
            undated_only=[r[0] for r in c.execute('SELECT did FROM q_recent_sources WHERE undated_skipped>0 AND accepted_rows=0 ORDER BY did LIMIT 2048')]
            rejecting=[{'did':r[0],'provider_rejects':r[1]} for r in c.execute('SELECT did,provider_rejects FROM q_recent_sources WHERE provider_rejects>0 ORDER BY did LIMIT 2048')]
            refused=[{'did':r[0],'reason':r[1]} for r in c.execute("SELECT did,boot_reason FROM q_recent_sources WHERE boot_phase='refused' ORDER BY did LIMIT 2048")]
            unavailable=[{'did':r[0],'since':r[1],'last_failure_at':r[2],'reason':r[3]} for r in c.execute(
                'SELECT did,unavailable_since,last_failure_at,last_failure_reason FROM q_recent_sources WHERE unavailable_since IS NOT NULL ORDER BY did LIMIT 2048')]
            return {**phases,'positioned_without_bootstrap':positioned,'pre_epoch_skipped':skipped[0],
                    'undated_skipped':skipped[1],'refused_sources':refused,
                    'unavailable':len(unavailable),'unavailable_sources':unavailable,'late_skipped':skipped[2],
                    'cursor_order_unverified':len(unordered),'cursor_order_unverified_sources':unordered,
                    'undated_only':len(undated_only),'undated_only_sources':undated_only,
                    'provider_rejects':len(rejecting),'provider_rejects_sources':rejecting,
                    'coverage':'unknown' if phases['unstarted']+phases['probing']+phases['refused']-positioned>0 or unavailable
                               or unordered or undated_only else 'established_sources_only'}
        finally:c.close()

    def bootstrap_step(self,did,attempt_token,observed_at,expect,phase,lo=None,hi=None,established=None,reason=None,finish=True):
        """Persist one probe outcome before the next request (compare-and-set).

        established sets provider_cursor (None = provider beginning) and keeps
        the attempt open when finish=False so the probe page is accepted next.
        A refused source is excluded from claims: its coverage stays unknown.
        """
        if phase not in ('probing','established','refused'):raise ValueError('bootstrap phase')
        for value in (lo,hi,established):
            if value is not None and (not isinstance(value,str) or not value.isdigit() or len(value)>16 or int(value)>BOOTSTRAP_POLICY['cursor_max']):
                raise ValueError('bootstrap cursor must be a bounded decimal')
        if reason is not None and (not isinstance(reason,str) or len(reason)>64):raise ValueError('bootstrap reason bound')
        stamp=iso(clock(observed_at))
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                row=c.execute('SELECT attempt_token,provider_cursor,boot_phase,boot_probes,boot_lo,boot_hi FROM q_recent_sources WHERE did=?',(did,)).fetchone()
                if row is None:raise RuntimeError('source is not enrolled')
                if not attempt_token or row[0]!=attempt_token:raise RuntimeError('source attempt changed')
                if row[1] is not None:raise RuntimeError('positioned source is not bootstrapped')
                if (row[2],row[3],row[4],row[5])!=(expect['phase'],expect['probes'],expect['lo'],expect['hi']):raise RuntimeError('bootstrap state changed')
                if row[2] not in ('unstarted','probing'):raise RuntimeError('bootstrap already decided')
                probes=row[3]+1
                if probes>BOOTSTRAP_POLICY['total_probes']:raise RuntimeError('bootstrap probe ceiling')
                if phase=='probing' and probes>=BOOTSTRAP_POLICY['total_probes']:
                    phase,reason='refused','probe_budget_exhausted'
                c.execute('UPDATE q_recent_sources SET boot_phase=?,boot_probes=?,boot_lo=?,boot_hi=?,boot_started=COALESCE(boot_started,?),unavailable_since=NULL,provider_rejects=0,history_streak=0 WHERE did=?',
                          (phase,probes,lo,hi,stamp,did))
                if phase=='established':
                    c.execute('UPDATE q_recent_sources SET provider_cursor=?,boot_cursor=?,boot_at=? WHERE did=?',(established,established,stamp,did))
                if phase=='refused':
                    c.execute('UPDATE q_recent_sources SET boot_reason=? WHERE did=?',('bootstrap_refused:'+(reason or 'unspecified'),did))
                if finish or phase!='established':
                    c.execute('UPDATE q_recent_sources SET attempt_token=NULL,attempt_start=NULL,hot=0,failures=0,next_due=? WHERE did=?',(stamp,did))
                self.caps(c,scan=False);c.commit()
                return {'phase':phase,'probes':probes,'lo':lo,'hi':hi,'cursor':established if phase=='established' else None,
                        'reason':'bootstrap_refused:'+reason if phase=='refused' else None}
            finally:c.close()

    def collector_cursor(self,did):
        c=connect(self.state,readonly=True)
        try:
            self.require(c);row=c.execute('SELECT provider_cursor FROM q_recent_sources WHERE did=?',(did,)).fetchone()
            if row is None:raise RuntimeError('source is not enrolled')
            return row[0]
        finally:c.close()

    def collector_claim_source(self,limit=2048,observed_at=None):
        if not 0<limit<=2048:raise ValueError('source claim bound')
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                if c.execute('SELECT COUNT(*) FROM q_recent_sources').fetchone()[0]>limit:raise RuntimeError('source enrollment ceiling')
                stamp=iso(clock(observed_at or db.get_meta(c,'q:recent_clock')))
                streak=int(db.get_meta(c,'q:recent_hot_streak') or '0')
                if not 0<=streak<=SCHEDULER_POLICY['hot_streak_max']:raise RuntimeError('scheduler streak bound')
                def candidate(hot):
                    key='q:recent_last_hot' if hot else 'q:recent_last_cold'
                    previous=db.get_meta(c,key) or ''
                    sql='SELECT did,endpoint FROM q_recent_sources WHERE '+('hot>0' if hot else 'hot=0')+" AND boot_phase!='refused' AND (next_due IS NULL OR next_due<=?)"
                    row=c.execute(sql+' AND did>? ORDER BY did LIMIT 1',(stamp,previous)).fetchone()
                    if row is None:row=c.execute(sql+' ORDER BY did LIMIT 1',(stamp,)).fetchone()
                    return row,key
                cold,cold_key=candidate(0);hot,hot_key=candidate(1)
                choose_hot=hot is not None and (streak<SCHEDULER_POLICY['hot_streak_max'] or cold is None)
                row,key=(hot,hot_key) if choose_hot else (cold,cold_key)
                if row is None:c.rollback();return None
                db.set_meta(c,key,row['did'])
                db.set_meta(c,'q:recent_hot_streak',str(min(SCHEDULER_POLICY['hot_streak_max'],streak+1) if choose_hot else 0))
                self.caps(c,scan=False);c.commit();return dict(row)
            finally:c.close()

    @staticmethod
    def _schedule_success(c,source,active,request_cursor,next_cursor,stamp,force_quiet=False):
        previous=c.execute('SELECT hot FROM q_recent_sources WHERE did=?',(source,)).fetchone()[0]
        progressing=bool(active) and next_cursor is not None and next_cursor!=request_cursor
        if force_quiet:hot=0
        elif progressing:hot=1
        elif active or previous==1:hot=2  # One bounded warm observation after activity.
        else:hot=0
        delay=0 if hot==1 else SCHEDULER_POLICY['warm_seconds'] if hot==2 else SCHEDULER_POLICY['quiet_seconds']
        due=iso(stamp+dt.timedelta(seconds=delay))
        c.execute('UPDATE q_recent_sources SET hot=?,next_due=?,failures=0 WHERE did=?',(hot,due,source))

    def accept_page(self,rows,*,source,request_cursor,next_cursor,observed_at,death=None,attempt_token=None,skipped=None,history=False):
        """Opaque provider continuation CAS; no manufactured upstream cursor.

        The last bounded page remembers its exact hashes after window expiry,
        so repeated terminal tails do not become newly observed historical data.
        This is not unbounded replay identity or proof of stream continuity.
        skipped={'pre_epoch':n,'undated':m} counts rows the collector refused to
        accept as source-dated before the epoch floor (or undatable); they are a
        disclosure, never observations and never a coverage gap. 'late' counts
        rows inside the epoch but older than acceptance minus the late allowance.
        history=True marks a full page whose rows were all skipped as too old;
        BOOTSTRAP_POLICY.history_pages consecutive ones re-probe the head.
        Overlap on a cursor-following page (rows already seen) marks the source
        cursor_order_unverified: rows are still accepted, continuity is not claimed.
        """
        skipped=dict(skipped or {})
        if set(skipped)-{'pre_epoch','undated','late'} or any(type(v) is not int or not 0<=v<=100 for v in skipped.values()):raise ValueError('skipped row accounting')
        pre_epoch,undated,late=skipped.get('pre_epoch',0),skipped.get('undated',0),skipped.get('late',0)
        if len(rows)+pre_epoch+undated+late>100:raise RuntimeError('collector page ceiling')
        for value in (request_cursor,next_cursor):
            if value is not None and (not isinstance(value,str) or len(value.encode())>4096):raise ValueError('opaque cursor bound')
        self.validate_rows(rows);stamp=clock(observed_at);self.check_local()
        page={'request':request_cursor,'next':next_cursor,'hashes':[r[9] for r in rows],
              'payloads':[custody.payload_digest(r) for r in rows]}
        if pre_epoch or undated or late:page['skipped']=[pre_epoch,undated,late]
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                current=c.execute('SELECT provider_cursor,last_page,attempt_token,attempt_start FROM q_recent_sources WHERE did=?',(source,)).fetchone()
                if current is None:raise RuntimeError('source is not enrolled')
                if current[2]!=attempt_token:raise RuntimeError('source attempt changed')
                if current[3] and iso(stamp)<current[3]:raise RuntimeError('attempt clock regression')
                previous=json.loads(current[1]) if current[1] else None
                if previous==page:
                    self._schedule_success(c,source,False,request_cursor,next_cursor,stamp,force_quiet=True)
                    c.execute('UPDATE q_recent_sources SET attempt_token=NULL,attempt_start=NULL,unavailable_since=NULL WHERE did=?',(source,))
                    c.commit();return {'inserted':0,'cursor':current[0],'replayed_page':True}
                if current[0]!=request_cursor:raise RuntimeError('provider continuation changed; page refused')
                # Guard the full payload digest as well as canonical source hash.
                # Within retained custody, normal identity lookup checks payload.
                tail=dict(zip(previous['hashes'],previous['payloads'])) if previous else {}
                for row in rows:
                    if row[9] in tail and tail[row[9]]!=custody.payload_digest(row):raise RuntimeError('retained terminal identity content conflict')
                fresh=[row for row in rows if row[9] not in tail]
                accepted,duplicates=self._stage_observations(c,fresh,stamp,death)
                position=next_cursor if next_cursor is not None else request_cursor
                self._schedule_success(c,source,bool(rows) or bool(pre_epoch+undated+late),request_cursor,next_cursor,stamp)
                c.execute('UPDATE q_recent_sources SET provider_cursor=?,last_page=?,last_observed=?,attempt_token=NULL,attempt_start=NULL,pre_epoch_skipped=pre_epoch_skipped+?,undated_skipped=undated_skipped+?,late_skipped=late_skipped+?,accepted_rows=accepted_rows+?,unavailable_since=NULL,provider_rejects=0 WHERE did=?',(position,json.dumps(page,sort_keys=True),iso(stamp),pre_epoch,undated,late,len(accepted),source))
                followed=previous is not None and previous.get('next') is not None and previous['next']==request_cursor and previous['next']!=previous['request']
                overlap=len(rows)-len(fresh)+len(duplicates)
                if followed and overlap:
                    evidence=json.dumps({'request':request_cursor,'previous_request':previous['request'],'overlap':overlap,'rows':len(rows),'at':iso(stamp)},sort_keys=True)
                    c.execute('UPDATE q_recent_sources SET cursor_order_unverified=1,cursor_order_evidence=COALESCE(cursor_order_evidence,?) WHERE did=?',(evidence[:1024],source))
                streak=c.execute('SELECT history_streak,boot_probes FROM q_recent_sources WHERE did=?',(source,)).fetchone()
                streak,probes=(streak[0]+1 if history else 0),streak[1]
                if streak>=BOOTSTRAP_POLICY['history_pages']:
                    # Premature head: re-probe from here (budget applies) or refuse.
                    numeric=isinstance(position,str) and position.isdigit() and len(position)<=16 and int(position)<=BOOTSTRAP_POLICY['cursor_max']
                    if numeric and probes<BOOTSTRAP_POLICY['total_probes']-1:
                        c.execute("UPDATE q_recent_sources SET provider_cursor=NULL,boot_phase='probing',boot_lo=?,boot_hi=NULL,history_streak=0,hot=0 WHERE did=?",(position,source))
                    else:
                        c.execute("UPDATE q_recent_sources SET boot_phase='refused',boot_reason='bootstrap_refused:premature_head',history_streak=0 WHERE did=?",(source,))
                else:c.execute('UPDATE q_recent_sources SET history_streak=? WHERE did=?',(streak,source))
                self.caps(c,scan=False)
                if death:death('before_recent_ingest_commit')
                c.commit()
                if death:death('after_recent_ingest_commit')
                self.flush(c)
                return {'inserted':len(accepted),'accepted_ids':[x[0] for x in accepted],'duplicate_ids':duplicates,'cursor':position}
            finally:c.close()

    def archive(self, identity, destination=None, **kwargs):
        dest=self.root/'archive'
        with closing(connect(self.state,readonly=True)) as c:self.require(c)
        if destination is not None and Path(destination).resolve()!=dest.resolve():
            raise RuntimeError('recent archive must be owned by this new store')
        return super().archive(identity,dest,lock_name='archive.lock',**kwargs)

    def retire(self, identity, **kwargs):
        with closing(connect(self.state,readonly=True)) as c:self.require(c)
        # Same archive lock domain prevents an archive retry reopening SQLite
        # while the verified retired owner's exact sidecars are discharged.
        with lock(self.root,'archive.lock'):
            allocated=super().retire(identity,**kwargs)
            with lock(self.root),lock(self.root,'reader.lock'):
                c=connect(self.state)
                try:
                    self.require(c)
                    row=c.execute('SELECT status FROM q_segments WHERE identity=?',(identity,)).fetchone()
                    source=self.root/(identity+'.sqlite')
                    if row is None or row[0]!='RETIRED' or source.exists():raise RuntimeError('sidecar retirement authority absent')
                    sidecars=[Path(str(source)+suffix) for suffix in ('-wal','-shm')]
                    # All supported readers honor reader.lock; direct SQLite
                    # consumers are outside this new-generation ownership rule.
                    import os,stat
                    for path in sidecars:
                        if not path.exists() and not path.is_symlink():continue
                        info=path.lstat()
                        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_dev!=self.root.stat().st_dev or os.path.ismount(path):
                            raise RuntimeError('retired sidecar ownership boundary')
                        if path.name.endswith('-wal') and info.st_size:
                            raise RuntimeError('nonempty retired WAL preserved')
                    for path in sidecars:
                        if path.exists():allocated+=path.stat().st_blocks*512;path.unlink()
                    syncdir(self.root)
                    c.execute('BEGIN IMMEDIATE');custody.expire_owner(c,identity);c.commit()
                finally:c.close()
            return allocated

    def frontier(self):
        c=connect(self.state,readonly=True)
        try:
            self.require(c)
            if c.execute('SELECT 1 FROM q_recent_transition').fetchone():raise RuntimeError('recent maintenance recovery required')
            return {**{k:db.get_meta(c,'q:recent_'+k) for k in ('start','end','generation')},
                    'acquisition':self.acquisition(c)}
        finally:c.close()

    def _record_gap(self,c,start,end,reason):
        start,end=iso(clock(start)),iso(clock(end))
        if start>=end or reason not in ('ingest_paused','source_unavailable','cursor_reset','maintenance','unknown','warmup'):raise ValueError('invalid bounded gap')
        start=max(start,db.get_meta(c,'q:recent_start'))
        if start>=end:return
        # Same-reason overlapping or touching intervals merge into one row.
        for old_start,old_end in list(c.execute('SELECT start,end FROM q_recent_gaps WHERE reason=? AND start<=? AND end>=?',(reason,end,start))):
            start,end=min(start,old_start),max(end,old_end)
            c.execute('DELETE FROM q_recent_gaps WHERE start=? AND end=? AND reason=?',(old_start,old_end,reason))
        if c.execute('SELECT COUNT(*) FROM q_recent_gaps').fetchone()[0]>=128:
            # Remember the dropped interval so overflow can be released later.
            db.set_meta(c,'q:recent_gap_overflow','1')
            lo,hi=db.get_meta(c,'q:recent_gap_overflow_start'),db.get_meta(c,'q:recent_gap_overflow_end')
            db.set_meta(c,'q:recent_gap_overflow_start',min(start,lo) if lo else start)
            db.set_meta(c,'q:recent_gap_overflow_end',max(end,hi) if hi else end)
        else:c.execute('INSERT INTO q_recent_gaps VALUES(?,?,?)',(start,end,reason))

    def _release_gap_overflow(self,c):
        """After expiry, overflow becomes one merged in-window 'unknown' gap (or nothing)."""
        if db.get_meta(c,'q:recent_gap_overflow')!='1':return
        if c.execute('SELECT COUNT(*) FROM q_recent_gaps').fetchone()[0]>=128:return
        lo,hi=db.get_meta(c,'q:recent_gap_overflow_start'),db.get_meta(c,'q:recent_gap_overflow_end')
        c.execute("DELETE FROM meta WHERE key IN ('q:recent_gap_overflow_start','q:recent_gap_overflow_end')")
        db.set_meta(c,'q:recent_gap_overflow','0')
        if lo and hi:self._record_gap(c,lo,hi,'unknown')

    def record_gap(self,start,end,reason):
        with lock(self.root):
            c=connect(self.state)
            try:
                self.recover(c);c.execute('BEGIN IMMEDIATE')
                self._record_gap(c,start,end,reason);c.commit()
            finally:c.close()

    def advance_day(self,now):
        """Finite daily orchestration; existing journals own each transition.

        A process interruption resumes those journals on the next invocation.
        This command does not enroll legacy data or authorize production paths.
        """
        import os
        stamp=clock(now);completed=[]
        with lock(self.root,'daily.lock'):
            with closing(connect(self.state,readonly=True)) as c:
                self.require(c)
                if stamp<clock(db.get_meta(c,'q:recent_clock')):raise RuntimeError('observation clock regression')
                pending=c.execute('SELECT 1 FROM q_recent_transition').fetchone()
            if pending:self.maintain(now)
            with lock(self.root):
                with closing(connect(self.state)) as c:self.recover(c)
            def drain():
                with closing(connect(self.state,readonly=True)) as c:
                    rows=list(c.execute("SELECT identity,status FROM q_segments WHERE status IN ('SEALED','ARCHIVED','RETIRED') ORDER BY identity LIMIT 34"))
                if len(rows)>33:raise RuntimeError('daily owner bound exceeded')
                for owner,status in rows:
                    if status=='RETIRED' and (self.root/'archive'/(owner+'.retired.json')).exists() and not any(Path(str(self.root/(owner+'.sqlite'))+suffix).exists() for suffix in ('-wal','-shm')):continue
                    if status in ('SEALED','ARCHIVED'):
                        source=self.root/(owner+'.sqlite')
                        need=source.stat().st_size*2+1048576
                        # Preserve every configured filesystem reserve after the
                        # archive temporary envelope lands on its filesystem.
                        self.admit_reserves(need,(self.root,self.root/'archive'),'daily archive allocation not admitted')
                        self.archive(owner)
                    self.retire(owner);completed.append(owner)
            drain()  # Free the two-owner queue before creating today's owner.
            self.rotate(stamp.date().isoformat())
            drain()
            result=self.maintain(now)
            return {'retired':completed,'maintenance':result}

    def maintain(self, now, archive=None, death=None):
        """Journal authority expiry before unlink; retry exact remaining files.

        All paths are new-fixture-owned. Existing campaign source/evidence is
        never enrolled. A protection dependency refuses the whole transition.
        """
        stamp=clock(now);dest=self.root/'archive'
        if archive is not None and Path(archive).resolve()!=dest.resolve():raise RuntimeError('archive mismatch')
        with lock(self.root),lock(self.root,'reader.lock'):
            c=connect(self.state)
            try:
                self.require(c)
                pending=c.execute('SELECT body FROM q_recent_transition').fetchone()
                if pending: plan=json.loads(pending[0])
                else:
                    super().recover(c)
                    if stamp<clock(db.get_meta(c,'q:recent_clock')):raise RuntimeError('observation clock regression')
                    if iso(stamp)==db.get_meta(c,'q:recent_clock') and db.get_meta(c,'q:recent_generation')!='0':
                        return {'expired_owners':[],'frontier':db.get_meta(c,'q:recent_start'),'no_change':True}
                    start=iso(stamp-dt.timedelta(days=30))
                    owners=[];files=[]
                    for owner,status in c.execute('SELECT identity,status FROM q_segments'):
                        if owner>=start[:10]:continue
                        if status!='RETIRED':raise RuntimeError('expired owner needs verified archive and source retirement first')
                        if c.execute('SELECT 1 FROM q_recent_protected WHERE owner=?',(owner,)).fetchone():raise RuntimeError('owner has retained evidence dependency')
                        row=c.execute('SELECT receipt_json FROM custody_archives WHERE identity=?',(owner,)).fetchone()
                        if not row:raise RuntimeError('expired owner custody absent')
                        receipt=json.loads(row[0])
                        if Path(receipt['archive_root']).resolve()!=dest.resolve():raise RuntimeError('foreign evidence namespace preserved')
                        members=[(dest/(owner+'.parquet'),receipt['parquet_sha256']),
                                 (dest/receipt['identity_index']['file'],receipt['identity_index']['sha256']),
                                 (dest/(owner+'.receipt.json'),None),(dest/(owner+'.retired.json'),None)]
                        for path,expected in members:
                            if path.parent.resolve()!=dest.resolve() or path.is_symlink() or path.stat().st_nlink!=1:raise RuntimeError('retirement path/link boundary')
                            digest=sha(path)
                            if expected and digest!=expected:raise RuntimeError('retirement custody corruption')
                            files.append({'name':path.name,'sha256':digest})
                        owners.append(owner)
                    self.policies(c)
                    plan={'start':start,'end':max(iso(stamp),db.get_meta(c,'q:recent_end')),'clock':iso(stamp),'owners':owners,'files':files}
                    c.execute('INSERT INTO q_recent_transition VALUES (1,?)',(json.dumps(plan,sort_keys=True),));c.commit()
                    if death:death('after_recent_plan')
                # Idempotent transaction removes all authoritative references.
                c.execute('BEGIN IMMEDIATE')
                for owner in plan['owners']:
                    c.execute('DELETE FROM q_recent_seen WHERE owner_day=?',(day_number(owner),))
                    for table,column in [('q_hot_keys','segment'),('custody_archives','identity'),('q_archive','identity'),('q_segments','identity')]:
                        c.execute(f'DELETE FROM {table} WHERE {column}=?',(owner,))
                for table,column in TIMED.items():c.execute(f'DELETE FROM {table} WHERE {column}<?',(plan['start'],))
                c.execute('DELETE FROM q_recent_seen WHERE observed_us<?',(micros(plan['start']),))
                c.execute('DELETE FROM q_recent_gaps WHERE end<=?',(plan['start'],))
                db.set_meta(c,'q:recent_start',plan['start']);self._release_gap_overflow(c)
                db.set_meta(c,'q:recent_events',str(c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]))
                for table in DERIVED:c.execute(f'DELETE FROM {table}')
                # Discovery and configured-source enrollment outlive observed
                # event expiry. Preserve their sticky endpoint/probe state;
                # the explicit 100k source cap refuses overflow. Retirement
                # of a source needs its own operator rule, never event absence.
                for key in ('start','end','clock'):db.set_meta(c,'q:recent_'+key,plan[key])
                c.commit()
                if death:death('after_recent_authority')
                # Derived catalog is repairable but must not retain missing entries.
                for path in self.root.glob('reader-catalog-*.sqlite'):
                    with sqlite3.connect(path) as cat:
                        cat.executemany('DELETE FROM entries WHERE identity=?',[(x,) for x in plan['owners']])
                        cat.commit()
                        cat.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                        cat.execute('VACUUM')
                for item in plan['files']:
                    path=dest/item['name']
                    if path.exists():
                        if path.is_symlink() or path.stat().st_nlink!=1 or sha(path)!=item['sha256']:raise RuntimeError('retirement candidate changed; preserve and reconcile')
                        path.unlink();syncdir(dest)
                    if death:death('during_recent_unlink')
                # Projection invalidation + row ceilings bound semantics; VACUUM
                # reclaims global high-water pages with a measured temporary peak.
                # This isolated adapter has one state writer and fenced readers.
                if c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]:raise RuntimeError('state checkpoint pinned')
                self.admit_reserves(self.state.stat().st_size*2,(self.root,),'global compaction allocation not admitted')
                c.execute('VACUUM')
                c.execute('DELETE FROM q_recent_transition')
                db.set_meta(c,'q:recent_generation',str(int(db.get_meta(c,'q:recent_generation'))+1));c.commit()
                self.caps(c)
                return {'expired_owners':plan['owners'],'frontier':plan['start'],'policies':self.policies(c)}
            finally:c.close()
