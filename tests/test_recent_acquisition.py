"""New-generation provenance and warmup, never a legacy timestamp conversion."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).parents[1]/'tools/segmented_qualification'))
import storage
from storage import connect, db
from recent_storage import RecentStore
from recent_provider import RecentProvider
from labelwatch.recent_observations import RecentObservations, Limits, Refused, account_html, timestamp
from test_recent_storage import event

DID='did:plc:subject'
START=datetime(2026,9,1,tzinfo=timezone.utc)

@pytest.fixture
def store(tmp_path,monkeypatch):
    root=tmp_path/'runtime';root.mkdir()
    evidence=tmp_path/'evidence';evidence.mkdir()
    (evidence/'ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive':str(root/'store/archive')}))
    monkeypatch.setattr(storage,'ROOT',root)
    return RecentStore.create(root/'store',START.isoformat())

def product(store,now):
    return RecentObservations(RecentProvider(store),now=lambda:now)

def test_empty_nominal_window_html_and_export_share_explicit_acquisition(store):
    p=product(store,START)
    lo=(START-timedelta(days=30)).isoformat();hi=START.isoformat()
    account=p.create(DID,lo,hi);view=p.page(account['cursor'])
    exported=p.create_export(DID,lo,hi);download=p.page(exported['cursor'])
    assert view['rows']==download['rows']==[]
    assert view['manifest']['coverage']==download['manifest']['coverage']
    coverage=view['manifest']['coverage']
    assert coverage['status']=='unknown' and coverage['warmup']['start'].startswith('2026-08-02')
    assert coverage['warmup']['end']==coverage['acquisition']['started_at']
    assert store.frontier()['acquisition']['generation_id']==coverage['acquisition']['generation_id']
    assert timestamp(store.frontier()['acquisition']['started_at'])==timestamp(coverage['acquisition']['started_at'])
    html=account_html(p.account(account['cursor']))
    assert 'Local acquisition began' in html and 'Warm-up:' in html
    assert 'Current label state is unknown' in html

def test_first_event_exact_boundary_never_relabels_source_time(store):
    store.ingest([event(authored='1999-01-01T00:00:00Z')],'fixture','cursor',START.isoformat())
    end=START+timedelta(microseconds=1)
    p=product(store,end);answer=p.page(p.create(DID,START.isoformat(),end.isoformat())['cursor'])
    assert answer['manifest']['coverage']['warmup'] is None
    assert timestamp(answer['rows'][0]['observed_at'])==timestamp(store.frontier()['acquisition']['started_at'])
    assert answer['rows'][0]['source_timestamp']=='1999-01-01T00:00:00Z'

def test_stopped_collection_gap_survives_public_projection(store):
    end=START+timedelta(hours=1)
    store.ingest([], 'fixture','cursor',end.isoformat())
    store.record_gap(START.isoformat(),end.isoformat(),'ingest_paused')
    p=product(store,end);cursor=p.create(DID,START.isoformat(),end.isoformat())['cursor'];answer=p.page(cursor)
    assert answer['manifest']['coverage']['gaps']==[{'start':'2026-09-01T00:00:00Z','end':'2026-09-01T01:00:00Z','reason':'ingest_paused'}]
    assert 'Collection was stopped' in account_html(p.account(cursor))
    assert answer['manifest']['coverage']['status']=='unknown'

def test_restart_and_retention_preserve_acquisition_identity(store):
    acquisition=store.frontier()['acquisition']
    restarted=RecentStore(store.root)
    assert restarted.frontier()['acquisition']==acquisition
    restarted.advance_day('2026-10-03T00:00:00Z')
    assert restarted.frontier()['start']>acquisition['started_at']
    assert restarted.frontier()['acquisition']==acquisition
    with RecentProvider(restarted).snapshot(DID,'2026-10-02T00:00:00Z','2026-10-03T00:00:00Z',10) as result:
        assert not any(g['reason']=='not_observed' for g in result['coverage']['gaps'])
        assert result['coverage']['acquisition']==acquisition

@pytest.mark.parametrize('kind',['missing','unknown_schema','unknown_basis','invalid_id','missing_start','changed_start'])
def test_unknown_acquisition_metadata_refuses_before_public_empty_answer(store,kind):
    with connect(store.state) as c:
        value=json.loads(db.get_meta(c,'q:recent_acquisition'))
        if kind=='missing':c.execute("DELETE FROM meta WHERE key='q:recent_acquisition'")
        elif kind=='missing_start':c.execute("DELETE FROM meta WHERE key='q:recent_acquisition_start'")
        else:
            field,changed={'unknown_schema':('schema','unknown'),'unknown_basis':('basis','source_authored'),'invalid_id':('generation_id','unknown'),'changed_start':('started_at','2026-08-01T00:00:00.000000Z')}[kind]
            value[field]=changed;db.set_meta(c,'q:recent_acquisition',json.dumps(value))
    with pytest.raises(RuntimeError,match='acquisition metadata'):
        store.frontier()
    with pytest.raises(RuntimeError,match='acquisition metadata'):
        with RecentProvider(store).snapshot(DID,'2026-08-31T00:00:00Z',START.isoformat(),10):pass

def test_v5_has_no_implicit_identity_upgrade(store):
    with connect(store.state) as c:db.set_meta(c,'q:recent_version','5')
    with pytest.raises(RuntimeError,match='schema v6'):store.frontier()
    with connect(store.state) as c:assert db.get_meta(c,'q:recent_version')=='5'

def test_missing_public_acquisition_metadata_refuses():
    from test_recent_observations import make,START as LO,END as HI,DID as ACCOUNT
    p,source,_=make([]);source.coverage.pop('acquisition')
    with pytest.raises(Refused,match='acquisition_metadata'):p.create(ACCOUNT,LO,HI)

def test_dense_export_refuses_acquisition_generation_change():
    from test_recent_observations import make,event as observed,START as LO,END as HI,DID as ACCOUNT
    p,source,_=make([observed(0),observed(1)],page_rows=1)
    created=p.create_export(ACCOUNT,LO,HI);first=p.page(created['cursor'])
    source.coverage['acquisition']={**source.coverage['acquisition'],'generation_id':'87654321-4321-4321-8321-210987654321'}
    with pytest.raises(Refused,match='acquisition_generation_changed'):p.page(first['next_cursor'])
