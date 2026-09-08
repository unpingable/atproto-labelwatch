import json
import sqlite3
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from labelwatch import db, rules, scan, runner, emitter_classifier as ec
from labelwatch.config import Config


def database():
    conn = db.connect(':memory:')
    db.init_db(conn)
    return conn


def add_events(conn, did, count):
    conn.execute("INSERT INTO labelers(labeler_did,first_seen,scan_count) VALUES(?,'2020-01-01T00:00:00Z',20)", (did,))
    conn.executemany("INSERT INTO label_events(labeler_did,uri,val,ts,event_hash) VALUES(?,?,'test','2026-01-01T00:00:00Z',?)", ((did,f'at://{did}/app.bsky.feed.post/{i}',did+str(i)) for i in range(count)))
    conn.commit()


def test_threshold_cache_preserves_rule_decisions_and_bounds_work():
    conn=database()
    for count in (0,2,3,4,5,6,10000):
        add_events(conn, 'did:plc:n'+str(count), count)
    cfg=Config(warmup_min_events=3,confidence_min_events=5)
    exact=rules._build_event_count_cache(conn)
    bounded=rules._build_event_count_cache(conn,cap=5)
    for did in bounded:
        assert rules._confidence_tag(conn,cfg,did,bounded)==rules._confidence_tag(conn,cfg,did,exact)
        assert rules._warmup_state(conn,cfg,did,bounded)==rules._warmup_state(conn,cfg,did,exact)
    now=datetime(2026,1,2,tzinfo=timezone.utc)
    current=rules.run_rules(conn,cfg,now)
    with patch.object(rules,'_build_event_count_cache',return_value=exact):
        assert rules.run_rules(conn,cfg,now)==current
    assert set(rules._build_event_count_cache(conn,cap=0).values())=={0}
    counts=[]
    for cap in (None,5):
        ticks=[0]
        def progress():
            ticks[0]+=1
            return 0
        conn.set_progress_handler(progress,100)
        rules._build_event_count_cache(conn,cap=cap)
        counts.append(ticks[0])
    conn.set_progress_handler(None,0)
    assert counts[1] < counts[0]/10


def record(conn,ts,defs,operation='update'):
    conn.execute('INSERT INTO discovery_events(labeler_did,operation,source,record_json,discovered_at) VALUES(?,?,?,?,?)',
                 ('did:plc:fixture',operation,'fixture',json.dumps({'policies':{'labelValueDefinitions':defs}}),ts))


def test_batch_definitions_preserve_newest_history_ties_and_provenance():
    conn=database()
    record(conn,'2026-01-01T00:00:00Z',[{'identifier':'old','description':'historical'}])
    record(conn,'2026-01-02T00:00:00Z',[{'identifier':'new','description':'first'},{'identifier':'new','description':'second'}])
    record(conn,'2026-01-03T00:00:00Z',[{'identifier':'tie','description':'earlier id'}])
    record(conn,'2026-01-03T00:00:00Z',[{'identifier':'tie','description':'later id'}])
    record(conn,'2026-01-04T00:00:00Z',[{'identifier':'old','description':'deleted record excluded'}],'delete')
    record(conn,'2026-01-05T00:00:00Z',None)
    record(conn,'2026-01-06T00:00:00Z',[{'description':'missing identifier'}])
    requested={'old','new','tie','absent',None}
    expected={v:ec._find_any_emitter_definition(conn,v) for v in requested}
    actual=ec._find_emitter_definitions(conn,requested,chunk_rows=2)
    assert {v:actual.get(v) for v in requested}==expected
    values=[{'value':v} for v in ('old','old','new','tie','absent')]
    enriched=ec.enrich_top_vals_with_tier_classification(conn,values)
    assert [r['tier_classification'] for r in enriched]==[ec.classify_one(v['value'],expected[v['value']]) for v in values]
    assert conn.in_transaction  # fixture writes are still owned by caller


def test_batch_lookup_query_count_not_per_requested_value():
    conn=database()
    for i in range(20):
        record(conn,f'2026-01-{i+1:02d}T00:00:00Z',[{'identifier':f'v{i}'}])
    conn.commit()
    queries=[]
    conn.set_trace_callback(queries.append)
    result=ec._find_emitter_definitions(conn,{f'v{i}' for i in range(30)},chunk_rows=8)
    conn.set_trace_callback(None)
    assert len(result)==20
    assert len([q for q in queries if 'FROM discovery_events' in q])==5
    assert not conn.in_transaction


def test_malformed_definition_is_not_silent_absence():
    conn=database()
    conn.execute("INSERT INTO discovery_events(labeler_did,operation,source,record_json,discovered_at) VALUES('did:plc:x','update','fixture','not json','2026-01-01')")
    with pytest.raises(sqlite3.OperationalError):
        ec._find_any_emitter_definition(conn,'x')
    with pytest.raises(sqlite3.OperationalError):
        ec._find_emitter_definitions(conn,{'x'},chunk_rows=1)


def test_derive_fail_skip_pending_and_complete_do_not_share_success_tick(monkeypatch):
    conn=database()
    monkeypatch.setattr(scan,'_DERIVE_STEP_YIELD_SECONDS',0)
    db.set_meta(conn,'last_derive_ok_ts','prior')
    conn.commit()
    def fail():
        conn.execute("INSERT INTO meta(key,value) VALUES('should_rollback','1')")
        raise ValueError('fixture failure')
    failure=scan._derive_step(conn,'failed',fail)
    assert failure['state']=='FAILED'
    assert db.get_meta(conn,'should_rollback') is None
    assert scan._derive_step(conn,'skip',lambda:'SKIPPED_SOURCE_STALE')['state']=='SKIPPED'
    db.set_meta(conn,'update_author_day:backlog_active','1')
    assert scan._derive_step(conn,'update_author_day',lambda:None)['state']=='PENDING'
    monkeypatch.setattr(scan,'_DERIVE_DISABLED',True)
    assert scan.run_derive(conn,Config())['state']=='SKIPPED'
    for state in ('FAILED','SKIPPED','PENDING','INCOMPLETE'):
        assert runner._record_derive_outcome(conn,{'state':state}) is False
        assert db.get_meta(conn,'last_derive_ok_ts')=='prior'
    assert runner._record_derive_outcome(conn,{'state':'COMPLETE'}) is True
    assert db.get_meta(conn,'last_derive_ok_ts')!='prior'


def test_failed_middle_derive_step_keeps_other_commits(monkeypatch):
    conn=database()
    monkeypatch.setattr(scan,'_DERIVE_DISABLED',False)
    monkeypatch.setattr(scan,'_DERIVE_STEP_YIELD_SECONDS',0)
    def fail(*args):
        raise RuntimeError('fixture failure')
    monkeypatch.setattr(scan,'_update_coverage_columns',fail)
    outcome=scan.run_derive(conn,Config(),datetime(2026,1,2,tzinfo=timezone.utc))
    assert outcome['state']=='INCOMPLETE'
    assert outcome['steps']['update_coverage_columns']['state']=='FAILED'
    assert outcome['steps']['cleanup_ingest_outcomes']['state']=='COMPLETE'


def test_complete_derive_pass_has_explicit_complete_outcome(monkeypatch):
    conn=database()
    monkeypatch.setattr(scan,'_DERIVE_DISABLED',False)
    monkeypatch.setattr(scan,'_DERIVE_STEP_YIELD_SECONDS',0)
    outcome=scan.run_derive(conn,Config(),datetime(2026,1,2,tzinfo=timezone.utc))
    assert outcome['state']=='COMPLETE'
    assert outcome['steps']
    assert all(step['state']=='COMPLETE' for step in outcome['steps'].values())
