"""Bounded recent-state lifecycle, distinct from production migration acceptance."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[1]/'tools/segmented_qualification'))
import pytest
import storage
from storage import custody,connect
from recent_storage import RecentStore
from recent_provider import RecentProvider

@pytest.fixture
def store(tmp_path,monkeypatch):
    root=tmp_path/'runtime';root.mkdir()
    evidence=tmp_path/'evidence';evidence.mkdir()
    import json
    (evidence/'ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive':str(root/'store/archive')}))
    monkeypatch.setattr(storage,'ROOT',root)
    return RecentStore.create(root/'store','2026-09-01T00:00:00Z')

def event(value='test',authored='1999-01-01T00:00:00Z'):
    raw={'src':'did:plc:labeler','uri':'did:plc:subject','val':value,'cts':authored}
    return ('did:plc:labeler','did:plc:labeler','did:plc:subject',None,value,0,None,None,authored,custody.canonical_identity(raw),'did:plc:subject')

def seed(s):
    return s.ingest([event()],'source','opaque1','2026-09-01T00:01:00Z')

def archive(s):
    seed(s);s.rotate('2026-10-02');s.archive('2026-09-01');s.retire('2026-09-01')

def test_observation_clock_boundary_and_idempotence(store):
    seed(store)
    assert store.ingest([event()],'source','opaque1','2026-09-01T00:01:00Z')['inserted']==0
    with pytest.raises(RuntimeError,match='different batch'):
        store.ingest([event('changed')],'source','opaque1','2026-09-01T00:01:00Z')
    with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:01:00Z','2026-09-01T00:01:00.000001Z',10) as result:
        assert len(result['rows'])==1
        assert result['rows'][0]['ts']=='1999-01-01T00:00:00Z'
    with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:00:00Z','2026-09-01T00:01:00Z',10) as result:
        assert result['rows']==[]

def test_warmup_and_explicit_gap(store):
    seed(store);store.record_gap('2026-08-31T00:00:00Z','2026-09-01T00:00:00Z','source_unavailable')
    with RecentProvider(store).snapshot('did:plc:subject','2026-08-31T00:00:00Z','2026-09-01T00:01:00.000001Z',10) as result:
        assert result['coverage']['status']=='unknown'
        assert {g['reason'] for g in result['coverage']['gaps']}=={'not_observed','source_unavailable'}

@pytest.mark.parametrize('phase',['after_recent_plan','after_recent_authority','during_recent_unlink'])
def test_interrupted_retirement_refuses_then_recovers(store,phase):
    archive(store)
    def fail(here):
        if here==phase:raise RuntimeError('interrupted')
    with pytest.raises(RuntimeError,match='interrupted'):store.maintain('2026-10-02T00:00:00Z',death=fail)
    with pytest.raises(RuntimeError,match='recovery required'):store.frontier()
    assert store.maintain('2026-10-02T00:00:00Z')['expired_owners']==['2026-09-01']
    assert list((store.root/'archive').iterdir())==[]
    with connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM custody_archives').fetchone()[0]==0
        assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]==0
        store.caps(c)

def test_evidence_dependency_preserves_owner(store):
    archive(store)
    with connect(store.state) as c:c.execute('INSERT INTO q_recent_protected VALUES (?,?)',('2026-09-01','independent review'))
    with pytest.raises(RuntimeError,match='evidence dependency'):store.maintain('2026-10-02T00:00:00Z')
    assert (store.root/'archive/2026-09-01.parquet').exists()

def test_row_counts_replace_and_transaction_refusal(store):
    with connect(store.state) as c:
        for _ in range(2):c.execute('INSERT OR REPLACE INTO posted_findings VALUES (?,?,?,?)',('x','fixture',None,'2026-09-01T00:00:00Z'))
        assert c.execute("SELECT n FROM q_recent_counts WHERE name='posted_findings'").fetchone()[0]==1
        c.execute("UPDATE q_recent_counts SET ceiling=0 WHERE name='q_recent_seen'")
    with pytest.raises(Exception,match='recent row ceiling'):seed(store)
    with connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0]==0
        assert c.execute('SELECT COUNT(*) FROM q_pending').fetchone()[0]==0

def test_payload_byte_guard(store):
    with pytest.raises(RuntimeError,match='field byte'):store.ingest([event('x'*16385)],'source','opaque1','2026-09-01T00:01:00Z')
    seed(store)
    with pytest.raises(RuntimeError,match='byte ceiling'):
        with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:00:00Z','2026-09-01T00:01:00.000001Z',10,max_bytes=1):pass

def test_partial_owner_survives_full_thirty_day_cut(store):
    seed(store);store.rotate('2026-10-01');store.archive('2026-09-01');store.retire('2026-09-01')
    store.maintain('2026-10-01T00:00:00Z')
    assert store.frontier()['start']=='2026-09-01T00:00:00.000000Z'
    assert (store.root/'archive/2026-09-01.parquet').exists()
    with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:00:00Z','2026-09-01T00:02:00Z',10) as result:assert len(result['rows'])==1

def test_retirement_discharges_hot_identity_cache_preserving_replay(store):
    seed(store);store.rotate('2026-10-01');store.archive('2026-09-01');store.retire('2026-09-01')
    with connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0]==0
        assert c.execute("SELECT n FROM q_recent_counts WHERE name='q_hot_keys'").fetchone()[0]==0
        assert c.execute('SELECT COUNT(*) FROM custody_archives').fetchone()[0]==1
    result=store.ingest([event()],'source','opaque2','2026-10-01T00:00:00Z')
    assert result['inserted']==0 and result['duplicate_ids']==[1]
    # Verified idempotent retirement can resume a previously completed transfer.
    assert store.retire('2026-09-01')==0

def test_discovered_source_without_events_survives_event_expiry(store):
    archive(store)
    with connect(store.state) as c:
        storage.db.upsert_labeler(c,'did:plc:new-discovery','2026-09-01T00:00:00Z')
        c.execute("UPDATE labelers SET service_endpoint='https://fixture.invalid',declared_record=1 WHERE labeler_did='did:plc:new-discovery'")
    store.maintain('2026-10-02T00:00:00Z')
    with connect(store.state) as c:
        row=c.execute("SELECT service_endpoint,declared_record FROM labelers WHERE labeler_did='did:plc:new-discovery'").fetchone()
        assert tuple(row)==('https://fixture.invalid',1)
        assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]==0
        store.caps(c)

def test_v2_exact_integer_clock_and_v1_refusal(store):
    from recent_storage import micros,from_micros,day_number,day_name
    for value in ('1969-12-31T23:59:59.999999Z','2026-09-01T00:00:00.000001Z','9999-12-31T23:59:59.999999Z'):
        assert from_micros(micros(value))==value
    assert day_name(day_number('2026-09-01'))=='2026-09-01'
    with connect(store.state) as c:
        assert [r[1] for r in c.execute('PRAGMA table_info(q_recent_seen)')]==['event_id','owner_day','observed_us','target_did']
        storage.db.set_meta(c,'q:recent_version','1')
    with pytest.raises(RuntimeError,match='schema v4'):store.frontier()

def test_maximum_clock_refuses_without_acceptance(store):
    # Year9999 is representable, but no half-open exclusive successor exists.
    store.rotate('9999-12-31')
    with pytest.raises(OverflowError):store.ingest([event()],'source','opaque1','9999-12-31T23:59:59.999999Z')
    with connect(store.state) as c:assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]==0

def test_collector_opaque_pages_tail_and_endpoint_policy(store):
    did='did:plc:labeler'
    store.remember_sources([{'did':did,'endpoint':None}])
    store.remember_sources([{'did':did,'endpoint':'https://fixture.invalid'}])
    first=store.accept_page([event()],source=did,request_cursor=None,next_cursor='raw/opaque',observed_at='2026-09-01T00:01:00Z')
    assert first['inserted']==1 and store.collector_cursor(did)=='raw/opaque'
    # A successful commit can be retried with the original request position.
    assert store.accept_page([event()],source=did,request_cursor=None,next_cursor='raw/opaque',observed_at='2026-09-01T00:01:00Z')['inserted']==0
    terminal=store.accept_page([event(),event('second')],source=did,request_cursor='raw/opaque',next_cursor=None,observed_at='2026-09-01T00:02:00Z')
    assert terminal['inserted']==1 and store.collector_cursor(did)=='raw/opaque'
    # Polling a changed terminal tail does not invent an upstream cursor.
    assert store.accept_page([event('second'),event('third')],source=did,request_cursor='raw/opaque',next_cursor=None,observed_at='2026-09-01T00:03:00Z')['inserted']==1
    with pytest.raises(RuntimeError,match='continuation changed'):store.accept_page([],source=did,request_cursor='other',next_cursor=None,observed_at='2026-09-01T00:04:00Z')
    with pytest.raises(RuntimeError,match='endpoint change'):store.remember_sources([{'did':did,'endpoint':'https://changed.invalid'}])
    store.remember_sources([{'did':did,'endpoint':None}])
    assert store.collector_sources(2)==[{'did':did,'endpoint':'https://fixture.invalid'}]

def test_collector_claim_survives_restart_and_retained_tail_expiry(store):
    store.remember_sources([{'did':'did:plc:a','endpoint':None},{'did':'did:plc:b','endpoint':None}])
    assert store.collector_claim_source()['did']=='did:plc:a'
    reopened=RecentStore(store.root)
    assert reopened.collector_claim_source()['did']=='did:plc:b'
    assert reopened.collector_claim_source()['did']=='did:plc:a'
    store.accept_page([event()],source='did:plc:a',request_cursor=None,next_cursor=None,observed_at='2026-09-01T00:01:00Z')
    store.rotate('2026-10-02');store.archive('2026-09-01');store.retire('2026-09-01');store.maintain('2026-10-02T00:00:00Z')
    assert store.accept_page([event()],source='did:plc:a',request_cursor=None,next_cursor=None,observed_at='2026-10-02T00:01:00Z')['inserted']==0
    assert store.accept_page([event(),event('new')],source='did:plc:a',request_cursor=None,next_cursor=None,observed_at='2026-10-02T00:01:00Z')['inserted']==1

@pytest.mark.parametrize('phase',['before_recent_ingest_commit','after_recent_ingest_commit'])
def test_collector_atomic_page_recovery(store,phase):
    store.remember_sources([{'did':'did:plc:labeler','endpoint':'https://fixture.invalid'}])
    def fail(here):
        if here==phase:raise RuntimeError('interrupted')
    with pytest.raises(RuntimeError,match='interrupted'):
        store.accept_page([event()],source='did:plc:labeler',request_cursor=None,next_cursor='next',observed_at='2026-09-01T00:01:00Z',death=fail)
    result=store.accept_page([event()],source='did:plc:labeler',request_cursor=None,next_cursor='next',observed_at='2026-09-01T00:01:00Z')
    assert result['inserted']==(1 if phase=='before_recent_ingest_commit' else 0)
    with connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]==1
        assert c.execute('SELECT COUNT(*) FROM q_pending').fetchone()[0]==0
        assert store.collector_cursor('did:plc:labeler')=='next'

def test_attempt_restart_gap_and_obsolete_response_cas(store):
    did='did:plc:labeler';store.remember_sources([{'did':did}])
    old=store.begin_source_attempt(did,'2026-09-01T00:01:00Z')
    reopened=RecentStore(store.root)
    token=reopened.begin_source_attempt(did,'2026-09-01T00:02:00Z')
    with pytest.raises(RuntimeError,match='attempt changed'):store.finish_source_attempt(did,old)
    with pytest.raises(RuntimeError,match='attempt changed'):
        store.accept_page([event()],source=did,request_cursor=None,next_cursor=None,observed_at='2026-09-01T00:02:00Z',attempt_token=old)
    result=store.accept_page([event()],source=did,request_cursor=None,next_cursor=None,observed_at='2026-09-01T00:02:00Z',attempt_token=token)
    assert result['inserted']==1
    token=store.begin_source_attempt(did,'2026-09-01T00:03:00Z')
    assert store.accept_page([event()],source=did,request_cursor=None,next_cursor=None,observed_at='2026-09-01T00:03:00Z',attempt_token=token)['replayed_page']
    with connect(store.state) as c:
        assert tuple(c.execute('SELECT start,end,reason FROM q_recent_gaps').fetchone())==('2026-09-01T00:01:00.000000Z','2026-09-01T00:02:00.000000Z','unknown')
        assert c.execute('SELECT attempt_token FROM q_recent_sources').fetchone()[0] is None

def test_discovery_continuation_atomic_roster_and_terminal_reset(store):
    assert store.collector_discovery_cursor() is None
    store.remember_sources([{'did':'did:plc:a'}],request_cursor=None,next_cursor='opaque')
    with pytest.raises(RuntimeError,match='continuation changed'):
        store.remember_sources([{'did':'did:plc:b'}],request_cursor=None,next_cursor='bad')
    with pytest.raises(RuntimeError,match='enrollment ceiling'):
        store.remember_sources([{'did':'did:plc:b'}],limit=1,request_cursor='opaque',next_cursor=None)
    assert store.collector_discovery_cursor()=='opaque'
    assert store.collector_sources(2)==[{'did':'did:plc:a','endpoint':None}]
    store.remember_sources([{'did':'did:plc:b'}],request_cursor='opaque',next_cursor=None)
    assert store.collector_discovery_cursor() is None

def test_daily_driver_drains_full_queue_then_rotates_and_retries(store):
    seed(store);store.rotate('2026-09-02')
    result=store.advance_day('2026-09-03T00:00:00Z')
    assert result['retired']==['2026-09-01','2026-09-02']
    assert store.advance_day('2026-09-03T00:00:00Z')['retired']==[]
    with connect(store.state) as c:
        assert c.execute("SELECT identity FROM q_segments WHERE status='ACTIVE'").fetchone()[0]=='2026-09-03'
        assert c.execute("SELECT COUNT(*) FROM q_segments WHERE status!='RETIRED'").fetchone()[0]==1
    store.advance_day('2026-10-04T00:00:00Z')
    assert not (store.root/'archive/2026-09-01.parquet').exists()

def test_daily_driver_resumes_expiry_journal(store):
    archive(store)
    def fail(phase):
        if phase=='after_recent_authority':raise RuntimeError('interrupted')
    with pytest.raises(RuntimeError,match='interrupted'):store.maintain('2026-10-02T00:00:00Z',death=fail)
    result=RecentStore(store.root).advance_day('2026-10-02T00:00:00Z')
    assert result['maintenance']['no_change']
    assert not (store.root/'archive/2026-09-01.parquet').exists()

def test_daily_driver_refuses_archive_allocation_before_output(store,monkeypatch):
    import os
    from types import SimpleNamespace
    seed(store);store.rotate('2026-09-02')
    monkeypatch.setattr(os,'statvfs',lambda p:SimpleNamespace(f_bavail=64424509440,f_frsize=1,f_favail=100))
    with pytest.raises(RuntimeError,match='allocation not admitted'):store.advance_day('2026-09-03T00:00:00Z')
    assert (store.root/'2026-09-01.sqlite').exists()
    assert list((store.root/'archive').iterdir())==[]

@pytest.mark.parametrize('phase',['after_archive_commit_before_receipt','after_unlink_before_retired'])
def test_daily_recovers_publication_and_retirement_cuts(store,monkeypatch,phase):
    seed(store);store.rotate('2026-09-02')
    original=storage.cut
    def interrupt(requested,here):
        if here==phase:raise RuntimeError('interrupted')
    monkeypatch.setattr(storage,'cut',interrupt)
    with pytest.raises(RuntimeError,match='interrupted'):
        store.archive('2026-09-01')
        store.retire('2026-09-01')
    monkeypatch.setattr(storage,'cut',original)
    store.advance_day('2026-10-03T00:00:00Z')
    assert not (store.root/'archive/2026-09-01.parquet').exists()
    assert store.advance_day('2026-10-03T00:00:00Z')['retired']==[]

def test_fixed_archive_lock_domain_bounds_names_and_serializes_waiters(store,monkeypatch):
    import datetime as dt
    import threading
    import time
    entered=threading.Event();release=threading.Event();second=threading.Event()
    def operation(identity,*args):
        if identity=='2026-01-01':entered.set();assert release.wait(2)
        elif identity=='2026-01-02':second.set()
    monkeypatch.setattr(store,'_archive',operation)
    first=threading.Thread(target=lambda:store.archive('2026-01-01'))
    other=threading.Thread(target=lambda:store.archive('2026-01-02'))
    first.start();assert entered.wait(2);other.start();time.sleep(.03)
    assert not second.is_set();release.set();first.join(2);other.join(2)
    assert second.is_set() and not first.is_alive() and not other.is_alive()
    for day in range(1000):store.archive((dt.date(2020,1,1)+dt.timedelta(days=day)).isoformat())
    assert len(list(store.root.glob('archive*.lock')))==1
    assert (store.root/'archive.lock').exists()

def test_retired_sqlite_sidecars_discharge_and_nonempty_wal_refusal(store):
    seed(store);store.rotate('2026-09-02');store.archive('2026-09-01')
    store.retire('2026-09-01')
    source=store.root/'2026-09-01.sqlite'
    assert not source.exists()
    assert not Path(str(source)+'-wal').exists()
    assert not Path(str(source)+'-shm').exists()
    wal=Path(str(source)+'-wal');wal.write_bytes(b'pending')
    shm=Path(str(source)+'-shm');shm.write_bytes(b'metadata')
    with pytest.raises(RuntimeError,match='nonempty retired WAL'):store.retire('2026-09-01')
    assert wal.read_bytes()==b'pending' and shm.read_bytes()==b'metadata'

def test_retired_sqlite_sidecar_links_refused(store):
    import os
    seed(store);store.rotate('2026-09-02');store.archive('2026-09-01');store.retire('2026-09-01')
    exact=store.root/'2026-09-01.sqlite-shm';other=store.root/'preserved-fixture';other.write_bytes(b'identity')
    os.link(other,exact)
    with pytest.raises(RuntimeError,match='ownership boundary'):store.retire('2026-09-01')
    assert exact.exists() and other.read_bytes()==b'identity'

def test_old_profile_retirement_refuses_before_lock_or_file_effect(store,monkeypatch):
    import recent_storage
    seed(store);store.rotate('2026-09-02');store.archive('2026-09-01')
    with connect(store.state) as c:
        storage.db.set_meta(c,'q:recent_version','3')
        authority=[tuple(r) for r in c.execute('SELECT * FROM q_segments')]
    before={p.name:storage.sha(p) for p in store.root.iterdir() if p.is_file() and p.name.startswith('2026-09-01')}
    def forbid_lock(*args,**kwargs):raise AssertionError('old profile reached mutable lock acquisition')
    monkeypatch.setattr(recent_storage,'lock',forbid_lock)
    with pytest.raises(RuntimeError,match='schema v4'):store.retire('2026-09-01')
    assert before=={p.name:storage.sha(p) for p in store.root.iterdir() if p.is_file() and p.name.startswith('2026-09-01')}
    with connect(store.state) as c:assert authority==[tuple(r) for r in c.execute('SELECT * FROM q_segments')]
