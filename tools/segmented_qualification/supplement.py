"""Bounded qualification supplement; never connects to production."""
from __future__ import annotations
import hashlib,json,os,sqlite3,statistics,subprocess,sys,time
from collections import deque
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
from storage import Store,ROOT,atomic,connect,db,digest,FIELDS,COLS,sha,vessel
from qualify import event,seed_state


def model():
    # One accepted event; journal drain and retirement are separate commits.
    initial=(False,False,False,False,False,False) # accepted,journal,vessel,sealed,verified,retired
    visited={initial};queue=deque([initial]);edges=0
    while queue:
        a,j,v,s,p,r=state=queue.popleft()
        successors=[]
        if not a:successors.append((True,True,v,s,p,r))
        if j and not s:successors.append((a,j,True,s,p,r))
        if j and v:successors.append((a,False,v,s,p,r))
        if not j and v:successors.append((a,j,v,True,p,r))
        if s and v:successors.append((a,j,v,s,True,r))
        if p and s:successors.append((a,j,False,s,p,True))
        successors.append(state) # process death/retry changes no durable state
        for target in successors:
            edges+=1;accepted,journal,file,sealed,verified,retired=target
            assert not accepted or journal or file or verified
            assert not retired or verified
            if target not in visited:visited.add(target);queue.append(target)
    return {'states':len(visited),'edges':edges,'invariants':['accepted implies journal or vessel or verified archive','retired implies verified archive'],'scope':'One-event logical protocol, honest SQLite FULL/fsync storage; not NFS power loss or implementation proof'}


def qualify(base):
    base=Path(base);base.mkdir();results=[];s=Store.create(base/'state');seed_state(s)
    # Every observed production metadata key is represented without copying values.
    profile=json.loads((ROOT.parent/'evidence/PRIVATE-CURRENT-SAMPLE.json').read_text())
    c=connect(s.state)
    for i,key in enumerate(profile['metadata_keys']):
        if key.startswith(('q:','ingest_cursor:','ops:cursor:','retention:')) or key=='schema_version':continue
        db.set_meta(c,key,'fixture-'+str(i))
    c.commit();meta=digest(c.execute("SELECT key,value FROM meta WHERE key NOT LIKE 'q:%' ORDER BY key"));c.close()
    from labelwatch import state as sidecar
    sc=sidecar.init_sidecar(str(base/'pilot-state.sqlite'));sidecar.meta_set(sc,sidecar.META_CURSOR_ID,'123456789');sidecar.meta_set(sc,sidecar.META_BUILD_STATUS,'complete');sc.commit();sc.execute('PRAGMA wal_checkpoint(TRUNCATE)');sc.close();side_hash=sha(base/'pilot-state.sqlite')
    s.rotate('2026-10-05');c=connect(s.state);assert meta==digest(c.execute("SELECT key,value FROM meta WHERE key NOT LIKE 'q:%' ORDER BY key"));c.close();assert sha(base/'pilot-state.sqlite')==side_hash
    results.append({'case':'all_observed_metadata_keys_and_external_pilot_checkpoint','result':'PASS','production_keys':len(profile['metadata_keys']),'sidecar_active':'not activated; isolated schema/checkpoint fixture'})
    # Separate processes exercise protocol writer fencing and a state-transition race.
    programs=[]
    for worker in range(4):
        rows=base/f'writer-{worker}.json';atomic(rows,[event(200+worker)])
        args=[sys.executable,str(Path(__file__).with_name('storage.py')),'ingest',str(s.root),'--rows-json',str(rows),'--cursor','200']
        programs.append(subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True))
    for program in programs:
        out,err=program.communicate(timeout=20);assert program.returncode==0,err
    with s.queries() as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==4;last=q.execute('SELECT MAX(id) FROM label_events').fetchone()[0]
    s.rotate('2026-10-12');archive=base/'archive';archive.mkdir();s.archive('2026-10-05',archive);s.retire('2026-10-05');s.ingest([event(999)],'new-provider','999')
    with s.queries() as q:assert q.execute('SELECT MIN(id) FROM label_events').fetchone()[0]>last
    results.append({'case':'concurrent_writers_and_global_ingestion_sequence_after_retirement','result':'PASS','writers':4,'global_ids_monotonic':True})
    # Reader lease must prevent local source retirement until the snapshot closes.
    s.rotate('2026-10-19');s.archive('2026-10-12',archive)
    with s.queries() as q:
        program=subprocess.Popen([sys.executable,str(Path(__file__).with_name('storage.py')),'retire',str(s.root),'--period','2026-10-12'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        time.sleep(.25);assert program.poll() is None;assert (s.root/'2026-10-12.sqlite').exists()
        assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==1
    out,err=program.communicate(timeout=20);assert program.returncode==0,err
    results.append({'case':'reader_snapshot_fences_retirement','result':'PASS'})
    # Query coverage counterexample: archived recent events cannot be silently omitted.
    with s.queries() as q:remaining=q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]
    assert remaining==0
    results.append({'case':'local_only_reader_after_recent_vessel_retirement','result':'COUNTEREXAMPLE','accepted_events':5,'local_query_events':remaining,'required_rule':'Local-only query must refuse claimed coverage or retain vessels through live floor; archive is required for full coverage.'})
    # Correct the local-only counterexample with one archive query engine and
    # bounded streaming hot state. Archive absence must refuse, not mean clear.
    from tier import TierSession
    from labelwatch import frontdoor
    audit={'overall_verdict':'admissible'}
    with TierSession(s,archive,subject='did:plc:fixturesubject') as reader:
        assert reader.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==5
        answer=frontdoor.lookup_subject(reader,'did:plc:fixturesubject',audit_receipt=audit)
        assert answer.refusal is None,answer.refusal
    s.ingest([event(12345,'2026-10-19T00:00:00Z')],'next-source','12345')
    with TierSession(s,archive,include_below_floor=True) as reader:
        assert reader.execute('SELECT COUNT(*),MIN(id),MAX(id) FROM label_events').fetchone()[0]==6
    missing=base/'missing-catalog';missing.mkdir()
    try:
        with TierSession(s,missing):pass
    except RuntimeError:pass
    else:raise AssertionError('missing catalog represented as empty history')
    results.append({'case':'cross_tier_live_coverage_and_archive_plus_active','result':'PASS','events_before':5,'events_with_new_active':6,'actual_frontdoor':'PASS','archive_missing':'REFUSAL','implementation':'Streaming Arrow batches into DuckDB, hot point query uses source index; no SQLite extension or dozens of SQLite attaches'})
    # 30/90-day actual-history query distributions, not 12M/day extrapolation.
    old=ROOT.parent.parent/'labelwatch-storage-architecture-20261005/runtime/occurrence-fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19'
    manifest=json.loads((old/'evidence/DATASET.json').read_text());files=[]
    for item in manifest['provenance']:
        path=old/'runtime/corpus'/('sample-'+item['day']+'.parquet');assert sha(path)==item['sample_sha256'];files.append(path)
    baseline=connect(base/'history.sqlite');vessel(base/'history-events.sqlite','historical-query-source');source=connect(base/'history-events.sqlite')
    for path in files:
        records=pq.read_table(path).to_pylist();source.executemany('INSERT INTO label_events('+COLS+') VALUES('+','.join('?' for _ in FIELDS)+')',[tuple(r[k] for k in FIELDS) for r in records]);source.commit()
    d=duckdb.connect();d.execute("SET memory_limit='256MB'");d.execute('SET threads=1');d.from_parquet([str(p) for p in files]).create_view('label_events')
    queries=[]
    for name,start in [('30_day','2026-07-15'),('90_day','2026-05-16')]:
        sql='SELECT labeler_did,val,COUNT(*),MIN(ts),MAX(ts) FROM label_events WHERE ts>=? AND ts<? GROUP BY labeler_did,val'
        answers=[];timings={}
        for engine,conn in [('sqlite',source),('duckdb',d)]:
            times=[]
            for i in range(7):
                begin=time.perf_counter();rows=[tuple(r) for r in conn.execute(sql,(start,'2026-08-14')).fetchall()];times.append((time.perf_counter()-begin)*1000)
            answers.append(sorted(rows));timings[engine]={'p50_ms':statistics.median(times),'p95_ms':max(times),'rows':len(rows)}
        assert answers[0]==answers[1];queries.append({'range':name,'parity':'PASS','timings':timings})
    source.close();baseline.close();d.close()
    result={'result':'PASS_WITH_COUNTEREXAMPLE','cases':results,'model':model(),'historical_queries':queries,'history_provenance':{'manifest_sha256':sha(old/'evidence/DATASET.json'),'files':len(files),'events':sum(x['sample_rows'] for x in manifest['provenance']),'scope':'Real 90-day stratified sample, not current-scale 90-day volume'},'production_mutations':[]};atomic(base/'RESULT.json',result)
    return result


if __name__=='__main__':
    import datetime as dt
    terminal=Path(sys.argv[2])
    try:qualify(sys.argv[1])
    except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
    else:atomic(terminal,{'result':'PASS_WITH_COUNTEREXAMPLE','at':dt.datetime.now(dt.timezone.utc).isoformat()})
