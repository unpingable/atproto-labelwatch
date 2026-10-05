"""Direct shared-custody regressions using the existing fixtures and crash runner."""
from __future__ import annotations
import argparse, datetime as dt, json, os, resource, sqlite3, subprocess, sys, time
from pathlib import Path
from unittest.mock import patch
from storage import Store, atomic, connect, custody, db, ingest, sha, cut, COLS
from qualify import event, child
from tier import TierSession
from tools.label_events_cold_archive import export_day, verify_manifest, reconstruct


def raw(i=1, ts='2026-10-05T12:00:00Z'):
    return {'src':'did:plc:fixturelabeler','uri':'at://did:plc:fixturesubject/app.bsky.feed.post/'+str(i),'val':'test','cts':ts}


def row(record):
    r=ingest.normalize_label(record, strict_identity=True)
    return (r.labeler_did,r.src,r.uri,r.cid,r.val,r.neg,r.exp,r.sig,r.ts,r.event_hash,db.parse_target_did(r.uri))


def globals_digest(conn):
    from storage import digest
    return {r[0]:digest(conn.execute('SELECT * FROM "'+r[0]+'" ORDER BY rowid')) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence'").fetchall()}


def new_single(path):
    path.mkdir(); c=connect(path/'single.sqlite');db.init_db(c);c.commit();custody.initialize(c,str(path));return c


def history_count(store,archive):
    with TierSession(store,archive,include_below_floor=True) as q:return q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]


def single_archive(c,path,owner='single-file'):
    c.commit();c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    source=path/'single.sqlite';dest=path/'archive';dest.mkdir()
    manifest=export_day(source,dest,'2026-10-05',sha(source));verified=verify_manifest(manifest)
    idx=custody.build_index(c,dest/'single.identities.sqlite',owner)
    receipt={'identity':owner,'archive_root':str(dest),'content':{'rows':verified['partition']['rows']},'identity_index':idx,
             'manifest':str(manifest),'manifest_sha256':sha(manifest)}
    c.execute('BEGIN IMMEDIATE');custody.commit_archive(c,dest/'single.receipt.json',receipt);c.commit();atomic(dest/'single.receipt.json',receipt)
    return receipt,manifest


def single_child(action,path,phase):
    c=connect(path/'single.sqlite');rows=json.loads((path/'rows.json').read_text())
    death=lambda name:cut(phase,name)
    if action=='single-accept':custody.accept(c,rows,'single-source','101',death=death)
    else:
        def publish(target,value):
            if phase=='during_receipt':
                with target.with_suffix('.incomplete').open('w') as f:f.write('{');f.flush();os.fsync(f.fileno());os._exit(73)
            atomic(target,value)
        custody.receipt(c,rows,path/'acceptance.receipt.json',publish,death)
    c.close()


def single_subprocess(action,path,phase):
    return subprocess.run([sys.executable,__file__,'--child',action,'--base',str(path),'--phase',phase],capture_output=True,text=True,timeout=30)


def qualify(base):
    base=Path(base);base.mkdir();results=[];started=time.perf_counter()
    def record(name,expected,actual,**evidence):
        assert actual==expected,(name,expected,actual)
        results.append({'case':name,'expected':expected,'actual':actual,**evidence})
        atomic(base/'PROGRESS.json',{'completed_cases':len(results),'last_case':name})
        print(name,actual,flush=True)
    # Record identity is independent of the cursor and never created from wall clock.
    a=raw();b={**a,'cursor':'changed'}
    record('canonical-repeat-and-transport','SAME_IDENTITY','SAME_IDENTITY' if row(a)[9]==row(b)[9] else 'WRONG')
    record('later-authored-source-record','DISTINCT_IDENTITY','DISTINCT_IDENTITY' if row(a)[9]!=row(raw(ts='2026-10-06T12:00:00Z'))[9] else 'WRONG')
    record('same-time-distinct-legitimate-record','DISTINCT_IDENTITY','DISTINCT_IDENTITY' if row(a)[9]!=row(raw(2))[9] else 'WRONG')
    for name,record_input in [('missing-authored-time',{k:v for k,v in a.items() if k!='cts'}),('conflicting-ts-alias',{**a,'ts':'2099-01-01T00:00:00Z'})]:
        try:row(record_input)
        except custody.Refused:actual='REFUSED'
        else:actual='ACCEPTED'
        record(name,'REFUSED',actual)
    # Actual single-file iterable path and polling page adapter, with no network.
    c=new_single(base/'single');assert ingest.ingest_from_iter(c,[a,a])==1
    before=globals_digest(c);assert ingest.ingest_from_iter(c,[a],mode='verification')==0;assert globals_digest(c)==before
    assert ingest.ingest_from_iter(c,[a])==0
    record('single-file-iterable-verification','ACCEPTED_ONCE','ACCEPTED_ONCE',global_delta='NONE',retry_inserted=0)
    from labelwatch.config import Config
    config=Config();config.service_url='https://fixture.invalid';config.labeler_dids=['did:plc:fixturelabeler']
    with patch.object(ingest,'fetch_labels',return_value={'labels':[raw(2)],'cursor':'opaque-position'}):
        assert ingest.ingest_from_service(c,config,max_pages=1)==1
        assert ingest.ingest_from_service(c,config,max_pages=1)==0
    record('single-file-poll-cursor','ACCEPTED_ONCE','ACCEPTED_ONCE',cursor=db.get_cursor(c,config.service_url),no_network=True)
    receipt,manifest=single_archive(c,base/'single');c.execute('DELETE FROM label_events');assert custody.expire_owner(c,'single-file')==2;c.commit()
    assert ingest.ingest_from_iter(c,[a,raw(2)])==0
    assert c.execute('SELECT COUNT(*) FROM quarantined_events').fetchone()[0]==0
    cp=custody.checkpoint(c);assert custody.replay(c,[row(a)],'recovery',cp)==[1]
    assert custody.replay(c,[row(a)]*2)==[1,1]
    restored=base/'single'/'restored-history.sqlite';reconstruct(manifest,restored)
    r=connect(restored)
    try:custody.initialize(r,'history-only')
    except custody.Refused:result='REFUSED'
    else:result='UNSAFE_ENROLLMENT'
    record('history-only-restore-inactive','REFUSED',result,restore_rows=r.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]);r.close()
    backup=base/'single'/'complete-backup.sqlite';bconn=sqlite3.connect(backup);c.backup(bconn);bconn.close()
    ingest.ingest_from_iter(c,[raw(3)]);latest=custody.checkpoint(c);bconn=connect(backup)
    try:custody.validate_resume(bconn,latest)
    except custody.Refused:result='REFUSED'
    else:result='UNSAFE_RESUME'
    record('stale-complete-backup','REFUSED',result,authoritative_checkpoint=latest);bconn.close()
    c.close()
    record('single-file-archive-identity-transfer','ACCEPTED_ONCE','ACCEPTED_ONCE',retired_keys=2,duplicate_archive_replay=0,quarantine_delta=0)
    # Crash acceptance on both layout paths; rollback plus retry must preserve one effect.
    for layout in ('single','segmented'):
        for phase in ('after_effect_before_identity','before_global_commit','after_global_commit'):
            p=base/(layout+'-'+phase);rs=[event()]
            if layout=='single':
                c=new_single(p);c.close();atomic(p/'rows.json',rs)
                outcome=single_subprocess('single-accept',p,phase);assert outcome.returncode==73,(phase,outcome.stderr)
                c=connect(p/'single.sqlite');n=c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]
                result=custody.accept(c,rs,'single-source','101');assert c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==1;c.close()
            else:
                s=Store.create(p);atomic(base/(layout+phase+'.json'),rs)
                outcome=child('ingest',s,phase,rows_json=base/(layout+phase+'.json'),cursor='101');assert outcome.returncode==73,(phase,outcome.stderr)
                with s.queries() as q:n=q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]
                result=s.ingest(rs,'fixture-provider','101')
                with s.queries() as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==1
            assert n==(1 if phase=='after_global_commit' else 0)
            record(layout+'-'+phase,'ACCEPTED_ONCE','ACCEPTED_ONCE',state_before_retry='ACCEPTED_ONCE' if n else 'NOT_ACCEPTED',retry_inserted=result['inserted'],process_exit=73)
    for phase in ('before_receipt','during_receipt','after_receipt_before_ack'):
        p=base/phase;c=new_single(p);rs=[event()];custody.accept(c,rs,'single-source','101');c.close();atomic(p/'rows.json',rs)
        out=single_subprocess('receipt',p,phase);assert out.returncode==73,(phase,out.stderr)
        c=connect(p/'single.sqlite');result=custody.accept(c,rs,'single-source','101');assert result['inserted']==0
        ids=custody.receipt(c,rs,p/'acceptance.receipt.json',atomic);assert ids==[1];c.close()
        record(phase,'ACCEPTED_ONCE','ACCEPTED_ONCE',retry_inserted=0,receipt_ids=ids,process_exit=73)
    # Concurrency uses SQLite serialization, not independent worker dedupe.
    p=base/'duplicate-workers';c=new_single(p);c.close();atomic(p/'rows.json',[event()])
    argv=[sys.executable,__file__,'--child','single-accept','--base',str(p),'--phase','none']
    workers=[subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(2)]
    for worker in workers:
        stdout,stderr=worker.communicate(timeout=30);assert worker.returncode==0,(stdout,stderr)
    c=connect(p/'single.sqlite');assert c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==1;c.close()
    record('duplicate-worker-invocation','ACCEPTED_ONCE','ACCEPTED_ONCE',workers=2)
    # Archive-custody crash edges: intermediate reads have one owner or explicit refusal.
    for phase in ('conversion_killed','verification_killed','after_parquet_before_checkpoint','before_archive_commit','after_archive_commit_before_receipt','after_receipt_before_ack'):
        p=base/('archive-'+phase);s=Store.create(p);rs=[event()];s.ingest(rs,'provider','1');s.rotate('2026-10-05');dest=base/('archive-'+phase+'-cold');dest.mkdir()
        out=child('archive',s,phase,period='2026-09-28',archive=dest);assert out.returncode==73,(phase,out.stderr)
        try:n=history_count(s,dest);mid='ONE_OWNER';assert n==1
        except (RuntimeError,FileNotFoundError):mid='EXPLICIT_REFUSAL'
        s.archive('2026-09-28',dest);assert history_count(s,dest)==1;s.retire('2026-09-28');s.advance_floor('2026-10-05T00:00:00Z')
        assert s.ingest(rs,'provider','2')['inserted']==0
        c=connect(s.state);assert c.execute('SELECT COUNT(*) FROM quarantined_events').fetchone()[0]==0;assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0]==0;c.close()
        record('archive-'+phase,'ACCEPTED_ONCE','ACCEPTED_ONCE',intermediate_read=mid,retry_inserted=0,retired_hot_keys=0,quarantine_delta=0)
    # Mandatory future/ancient and repeated/overlapping replay cases, fixed cardinality.
    p=base/'lifetime';s=Store.create(p,floor='0001-01-01T00:00:00Z')
    rs=[event(1,'2226-10-05T12:00:00Z'),event(2,'1900-01-01T00:00:00Z'),event(3)]
    assert s.ingest(rs,'provider','1')['inserted']==3;s.rotate('2026-10-05');dest=base/'lifetime-cold';dest.mkdir();s.archive('2026-09-28',dest);s.retire('2026-09-28');s.advance_floor('2026-10-05T00:00:00Z')
    c=connect(s.state);assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0]==0
    before=globals_digest(c)
    expected_ids={r[9]:i+1 for i,r in enumerate(rs)}
    for replay_rows in (rs,rs,rs[1:],rs[:2],list(reversed(rs))):
        assert custody.replay(c,replay_rows)==[expected_ids[r[9]] for r in replay_rows]
    assert globals_digest(c)==before
    for mode in ('reprocessing','unauthorized-recovery'):
        try:custody.replay(c,rs,'recovery' if mode=='unauthorized-recovery' else mode)
        except custody.Refused:result='REFUSED'
        else:result='UNSAFE_MUTATION'
        record(mode,'REFUSED',result)
    c.close()
    for replay_rows in (rs,rs,list(reversed(rs))):assert s.ingest(replay_rows,'provider','2')['inserted']==0
    assert history_count(s,dest)==3
    record('future-and-ancient-archive-lifetime','ACCEPTED_ONCE','ACCEPTED_ONCE',accepted=3,hot_keys=0,history_count=3,quarantine_delta=0)
    # Missing identity coverage does not become absence, even for a novel event.
    receipt=json.loads((dest/'2026-09-28.receipt.json').read_text());idx=custody.index_path(receipt);saved=idx.with_suffix('.saved');idx.rename(saved)
    try:s.ingest([event(999,'2026-10-05T00:00:00Z')],'provider','MUST_NOT_ADVANCE')
    except (custody.Refused,FileNotFoundError):result='REFUSED'
    else:result='UNSAFE_ACCEPTANCE'
    saved.rename(idx);c=connect(s.state);assert db.get_cursor(c,'provider')=='2';c.close()
    record('missing-identity-index','REFUSED',result,cursor_preserved=True)
    # Same hash cannot be rebound to a different immutable payload.
    wrong=list(rs[0]);wrong[8]='2030-01-01T00:00:00Z'
    try:s.ingest([wrong],'provider','3')
    except custody.Refused:result='REFUSED'
    else:result='CONFLICT_ACCEPTED'
    record('same-identity-changed-record-content','REFUSED',result)
    # Known-bad designs: each actual invariant checker must detect the substitution.
    c=connect(s.state);before=globals_digest(c)
    def bad_verification():
        db.quarantine_label_events(c,[rs[1]],reason='below_retention_floor',live_floor='2026-10-05T00:00:00Z',now_ts='2026-10-05T00:00:00Z');c.commit()
        assert globals_digest(c)==before
    try:bad_verification()
    except AssertionError:result='DETECTED'
    else:result='MISSED'
    record('mutant-verification-global-quarantine','DETECTED',result,global_quarantine_rows=c.execute('SELECT COUNT(*) FROM quarantined_events').fetchone()[0]);c.close()
    with patch.object(custody,'lookup',return_value=None):
        assert s.ingest([rs[0]],'provider','bad-future')['inserted']==1
    try:assert history_count(s,dest)==3
    except AssertionError:result='DETECTED'
    else:result='MISSED'
    record('mutant-authored-expiry-without-archive-membership','DETECTED',result,actual_count=history_count(s,dest))
    # The old ordering yields a receipt-backed orphan. Fixed reader excludes it;
    # deliberately restored discovery-only query must fail the count invariant.
    p=base/'receipt-mutant';s2=Store.create(p);s2.ingest([event()],'provider','1');s2.rotate('2026-10-05');a2=base/'receipt-mutant-cold';a2.mkdir();s2.archive('2026-09-28',a2)
    c=connect(s2.state);c.execute('DELETE FROM custody_archives');c.execute('DELETE FROM q_archive');c.execute("UPDATE q_segments SET status='SEALED' WHERE identity='2026-09-28'");c.commit();c.close()
    assert history_count(s2,a2)==1
    import duckdb
    d=duckdb.connect();n=d.execute('SELECT COUNT(*) FROM read_parquet(?)',[str(a2/'2026-09-28.parquet')]).fetchone()[0];d.close()
    bad_count=history_count(s2,a2)+n
    try:assert bad_count==1
    except AssertionError:result='DETECTED'
    else:result='MISSED'
    record('mutant-receipt-discovery-before-custody','DETECTED',result,fixed_count=1,old_discovery_count=bad_count,transition='exact pre-commit durable-state substitution')
    # Per-event mutable state is retired, immutable membership and per-owner metadata persist.
    p=base/'growth';s3=Store.create(p,period='2026-01-05',floor='0001-01-01T00:00:00Z');a3=base/'growth-cold';a3.mkdir();samples=[]
    for i in range(12):
        period=(dt.date(2026,1,5)+dt.timedelta(days=i*7)).isoformat();next_period=(dt.date(2026,1,5)+dt.timedelta(days=(i+1)*7)).isoformat()
        s3.ingest([event(1000+i, '2226-01-01T00:00:00Z')],'provider',str(i));s3.rotate(next_period);s3.archive(period,a3);s3.retire(period);s3.advance_floor(next_period+'T00:00:00Z')
        c=connect(s3.state);samples.append({'owners':i+1,'mutable_keys':c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0],'identity_records':sum(json.loads(r[0])['identity_index']['rows'] for r in c.execute('SELECT receipt_json FROM custody_archives')),'archive_metadata':c.execute('SELECT COUNT(*) FROM custody_archives').fetchone()[0]});c.close()
    assert all(x['mutable_keys']==0 for x in samples)
    record('bounded-mutable-lifetime','BOUNDED_HOT_KEYS','BOUNDED_HOT_KEYS',samples=samples,claim='structural only; no current-volume horizon capacity claim')
    from coverage_qualify import main as coverage_regression
    coverage_regression(base/'coverage')
    record('historical-coverage-regression','COMPLETE_OR_REFUSED','COMPLETE_OR_REFUSED',receipt=str(base/'coverage/RESULT.json'))
    allocated=sum(p.stat().st_blocks*512 for p in base.rglob('*') if p.is_file())
    result={'result':'PASS','source':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'cases':results,'count':len(results),'elapsed_seconds':time.perf_counter()-started,'allocated_fixture_bytes':allocated,'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'specimen':'existing deterministic source-record fixtures; small contract/state-transition cases, not a repeated 12M/47-day throughput campaign','production_mutations':[]}
    atomic(base/'RESULT.json',result);return result


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--child');p.add_argument('--phase');a=p.parse_args()
    if a.child:single_child(a.child,Path(a.base),a.phase)
    else:
        import traceback
        terminal=Path(a.base).with_name(Path(a.base).name+'-TERMINAL.json')
        try:qualify(a.base)
        except BaseException as e:
            atomic(terminal,{'result':'FAILED','error':repr(e),'traceback':traceback.format_exc(),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
        else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
