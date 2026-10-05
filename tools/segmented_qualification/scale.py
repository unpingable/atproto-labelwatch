"""Durable producer: correctness gates, production-shaped 12M specimen, custody.

All identities are fixture pseudonyms. Timelines, expansion, late arrivals and
replays are synthetic and recorded; opaque signature sizes are retained, not
claimed to be valid signatures after mutation. No network is used by producer.
"""
from __future__ import annotations
import base64
import collections
import datetime as dt
import hashlib
import json
import math
import os
import resource
import statistics
import sys
import threading
import time
from functools import lru_cache
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from storage import Store, ROOT, ARROW_SCHEMA, COLS, FIELDS, atomic, connect, db, ingest, sha, digest
from qualify import qualify, seed_state, event
from schemas import qualify as qualify_schemas

CAMPAIGN=ROOT.parent
TOTAL=12000000


def allocated(root):
    return sum(f.stat().st_blocks*512 for f in Path(root).rglob('*') if f.is_file())


def progress(occurrence,phase,**extra):
    for fs in ('/','/data'):
        s=os.statvfs(fs)
        if s.f_bavail*s.f_frsize<64424509440:raise RuntimeError('shared reserve lost; producer refuses next phase')
    used=allocated(occurrence)
    if used>34359738368:raise RuntimeError('32GiB occurrence envelope exceeded')
    info={'at':dt.datetime.now(dt.timezone.utc).isoformat(),'phase':phase,'allocated_bytes':used,'cpu_seconds':time.process_time(),'rss_peak_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,**extra}
    atomic(occurrence/'PROGRESS.json',info);print(json.dumps(info),flush=True)


@lru_cache(maxsize=50000)
def did(value,suffix=''):
    if value is None:return None
    return 'did:plc:'+base64.b32encode(hashlib.sha256((str(value)+suffix).encode()).digest()[:15]).decode().lower()


def specimen(occurrence):
    a=json.loads((CAMPAIGN/'evidence/PRIVATE-CURRENT-SAMPLE.json').read_text())['rows']
    b=json.loads((CAMPAIGN/'evidence/PRIVATE-WEEK-SAMPLE.json').read_text())['rows']
    bases=list(reversed(a))+b
    counts=collections.Counter(x['target_did'] for x in bases if x['target_did'])
    dense={k for k,n in counts.most_common(16)}
    out=occurrence/'specimen.parquet';writer=pq.ParquetWriter(out,ARROW_SCHEMA,compression='zstd');batch=[];size_samples=[]
    start=dt.datetime(2026,9,28,tzinfo=dt.timezone.utc);queries={};t=time.perf_counter()
    try:
        for ordinal in range(TOTAL):
            raw=bases[ordinal%len(bases)];repeat=ordinal//len(bases)
            target=raw['target_did'];suffix='' if target in dense else '-'+str(repeat)
            target_new=did(target,suffix)
            uri=raw['uri']
            if uri.startswith('did:'):uri=did(uri,suffix)
            elif uri.startswith('at://'):
                parts=uri.split('/',4);parts[2]=did(parts[2],suffix);uri='/'.join(parts)
            author=start+dt.timedelta(microseconds=(ordinal*604800000000)//TOTAL)
            if ordinal%100==17:author-=dt.timedelta(hours=72)  # Explicit injected out-of-order tail.
            if ordinal%100000==19:author+=dt.timedelta(days=10)  # Bounded future/outlier stress.
            fields={k:raw.get(k) for k in ['src','cid','val','neg','exp','sig']}
            fields.update(labeler_did=did(raw['labeler_did']),src=did(raw['src']),uri=uri,ts=author.isoformat().replace('+00:00','Z'))
            normalized=ingest.normalize_label(fields)
            row={'id':ordinal+1,**normalized.__dict__,'target_did':target_new}
            batch.append(row)
            if len(size_samples)<100000:size_samples.append(len(json.dumps(row,separators=(',',':')).encode()))
            if target in dense and 'dense' not in queries:queries['dense']=target_new
            if suffix and 'sparse' not in queries:queries['sparse']=target_new
            if 'labeler' not in queries:queries['labeler']=row['labeler_did']
            if 'uri' not in queries:queries['uri']=row['uri']
            if len(batch)==10000:
                writer.write_table(pa.Table.from_pylist(batch,schema=ARROW_SCHEMA));batch.clear()
            if (ordinal+1)%500000==0:progress(occurrence,'SPECIMEN',events=ordinal+1,target=TOTAL)
    finally:writer.close()
    with out.open('rb') as f:os.fsync(f.fileno())
    sizes=sorted(size_samples)
    result={'events':TOTAL,'source_sample_events':len(bases),'source_sha256':[sha(CAMPAIGN/'evidence'/name) for name in ['PRIVATE-CURRENT-SAMPLE.json','PRIVATE-WEEK-SAMPLE.json']],
            'sample_scope':'latest-ID burst plus 21 bounded timestamp-index seeks across current week; not complete-week joint distribution',
            'generation':'Deterministic expanded fixture. Pseudonymized identity/subject keys; top16 density targets kept across repeats, long tail namespaces repeat. Synthetic evenly spread week, 1% 72h late, 0.001% future; opaque sig shapes copied without signature-validity claim. Replay offers generated separately at 0.5%. No empirical late/replay rate claimed.',
            'row_json_bytes':{'p50':sizes[len(sizes)//2],'p95':sizes[int(len(sizes)*.95)],'max':max(sizes)},'schema':23,'indexes':'actual source v23 event DDL and every event index','parquet_bytes':out.stat().st_size,'parquet_sha256':sha(out),'generation_seconds':time.perf_counter()-t,'query_parameters':queries,'source_file':str(out)}
    atomic(occurrence/'SPECIMEN.json',result);return out,result


def measure_query(sqlite,duck,sql,args,name):
    results=[];timings={}
    for engine,c in [('sqlite',sqlite),('parquet_duckdb',duck)]:
        times=[];answer=None
        for _ in range(7):
            t=time.perf_counter();answer=[tuple(r) for r in c.execute(sql,args).fetchall()];times.append((time.perf_counter()-t)*1000)
        results.append(sorted(answer,key=lambda r:json.dumps(r,default=str)))
        timings[engine]={'p50_ms':statistics.median(times),'p95_ms':max(times),'observations':7,'rows':len(answer)}
    if results[0]!=results[1]:raise RuntimeError('query parity differs: '+name)
    return {'name':name,'parity':'PASS','sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),'timings':timings}


def main(occurrence):
    occurrence=Path(occurrence);occurrence.mkdir();scratch=occurrence/'sqlite-scratch';scratch.mkdir();os.environ['SQLITE_TMPDIR']=str(scratch)
    for key in list(os.environ):
        if key.startswith('LABELWATCH_'):del os.environ[key]
    progress(occurrence,'FINITE_GATES')
    finite=qualify(occurrence/'finite');qualify_schemas(occurrence/'schemas')
    progress(occurrence,'FINITE_GATES_PASS',lifecycle_cases=len(finite))
    data,provenance=specimen(occurrence);progress(occurrence,'INGEST_CURRENT_SCALE')
    store=Store.create(occurrence/'store');seed_state(store);initial=store.snapshot();started=time.perf_counter();accepted=0;replay_offers=0;peak_wal=0;bytes_day={};batchno=0
    for batch in pq.ParquetFile(data).iter_batches(batch_size=10000):
        rows=[tuple(r[k] for k in FIELDS[1:]) for r in batch.to_pylist()]
        replay=rows[:50]  # Replay offers are a separate bounded page.
        r=store.ingest(rows,'fixture-current-provider',str(batchno));accepted+=r['inserted']
        if store.ingest(replay,'fixture-replay-provider',str(batchno))['inserted']!=0:raise RuntimeError('replay silently duplicated')
        replay_offers+=len(replay);batchno+=1
        peak_wal=max(peak_wal,max((f.stat().st_size for f in store.root.glob('*.sqlite-wal')),default=0))
        if batchno%50==0:
            progress(occurrence,'INGEST_CURRENT_SCALE',accepted=accepted,replay_offers=replay_offers,batches=batchno,peak_wal=peak_wal)
            day=str(min(7,(accepted*7)//TOTAL));bytes_day[day]={'events':accepted,'event_file_bytes':(store.root/'2026-09-28.sqlite').stat().st_size,'state_file_bytes':store.state.stat().st_size}
    if accepted<TOTAL:raise RuntimeError('specimen offered records unexpectedly refused')
    ingest_seconds=time.perf_counter()-started
    before=store.snapshot();t=time.perf_counter();store.rotate('2026-10-05');rollover_seconds=time.perf_counter()-t
    after=store.snapshot();assert before['cursor']==after['cursor'];assert before['state_tables']==after['state_tables']
    # Actual derived/reference checkpoints remain in stable global tables.
    c=connect(store.state);c.execute("INSERT OR REPLACE INTO meta VALUES ('fixture:derived_checkpoint','12000000')");c.commit();c.close()
    progress(occurrence,'ROLLOVER_COMPLETE',rollover_seconds=rollover_seconds)
    # New ingestion succeeds before old conversion begins; it does not wait for verification.
    accepted_new=store.ingest([event(99,'2026-10-05T00:00:00Z')],'fixture-next-provider','next-1');assert accepted_new['inserted']==1
    archive=Path(json.loads((CAMPAIGN/'evidence/ARCHIVE-DESTINATION.json').read_text())['campaign_archive'])/occurrence.name;archive.mkdir()
    stop=threading.Event();continuing=[];writer_errors=[]
    def continued_ingest():
        n=0
        while not stop.wait(1):
            try:
                t=time.perf_counter();r=store.ingest([event(1000+n,'2026-10-05T01:00:'+f'{n%60:02d}'+'Z')],'fixture-continuing-provider',str(n));continuing.append({'seconds':time.perf_counter()-t,'inserted':r['inserted']});n+=1
            except BaseException as e:writer_errors.append(repr(e));return
    thread=threading.Thread(target=continued_ingest);thread.start();t=time.perf_counter()
    try:receipt=store.archive('2026-09-28',archive)
    finally:stop.set();thread.join(timeout=10)
    if thread.is_alive() or writer_errors:raise RuntimeError('continuing ingest failed during archive: '+str(writer_errors))
    if not continuing:raise RuntimeError('archive finished without demonstrating continuing ingest')
    handoff_seconds=time.perf_counter()-t
    progress(occurrence,'ARCHIVE_VERIFIED',conversion_seconds=receipt['conversion_seconds'],verification_seconds=receipt['verification_seconds'])
    # Ordinary SQL is copied from the live module, not simplified aggregates alone.
    from labelwatch import frontdoor
    cold=duckdb.connect();cold.execute("SET memory_limit='512MB'");cold.execute("SET max_temp_directory_size='2GB'");cold.execute('SET threads=2');spill=occurrence/'duck-spill';spill.mkdir();cold.execute('SET temp_directory=?',[str(spill)]);cold.from_parquet(str(archive/'2026-09-28.parquet')).create_view('label_events')
    source=connect(store.root/'2026-09-28.sqlite',readonly=True);p=provenance['query_parameters'];queries=[]
    inventory=[('Q3_dense',frontdoor._Q3_LABEL_VALUES,(p['dense'],)),('Q8a_dense',frontdoor._Q8A_DISTINCT_STATES,(p['dense'],)),('Q8b_dense',frontdoor._Q8B_LOCUS,(p['dense'],)),('Q8c_dense',frontdoor._Q8C_LABELED_RECORDS,(p['dense'],50)),('Q3_sparse',frontdoor._Q3_LABEL_VALUES,(p['sparse'],)),('recent_report','SELECT labeler_did,COUNT(*),COUNT(DISTINCT uri) FROM label_events WHERE ts>=? AND ts<? GROUP BY labeler_did',('2026-09-28','2026-10-05')),('debug_horizon','SELECT COUNT(*),MIN(ts),MAX(ts),MAX(id) FROM label_events',()),('derive_value_distribution','SELECT labeler_did,val,COUNT(*) FROM label_events GROUP BY labeler_did,val',())]
    for name,sql,args in inventory:
        queries.append(measure_query(source,cold,sql,args,name));progress(occurrence,'QUERY_PARITY',query=name)
    # Unchanged frontdoor refusal is tested through the actual entry point against the hot adapter.
    audit={'overall_verdict':'admissible','generated_at':'2026-10-05T00:00:00Z'}
    with store.queries() as hot:
        t=time.perf_counter();response=frontdoor.lookup_subject(hot,p['dense'],audit_receipt=audit);dense_refusal_seconds=time.perf_counter()-t
        if response.refusal!='subject_too_dense':raise RuntimeError('actual dense frontdoor behavior changed: '+str(response.refusal))
        count=hot.execute('SELECT COUNT(*) FROM label_events').fetchone()[0];assert count==TOTAL+1+sum(x['inserted'] for x in continuing)
    source.close();cold.close();before_retire=store.snapshot();t=time.perf_counter();reclaimed=store.retire('2026-09-28');retire_seconds=time.perf_counter()-t
    assert reclaimed==receipt['source_allocated_bytes'];assert not (store.root/'2026-09-28.sqlite').exists()
    final=store.snapshot();assert before_retire['state_tables']==final['state_tables']
    first=next(pq.ParquetFile(data).iter_batches(batch_size=1)).to_pylist()
    assert store.ingest([tuple(r[k] for k in FIELDS[1:]) for r in first],'fixture-old-replay','replay-after-retirement')['inserted']==0
    progress(occurrence,'BOUNDED_HOT_KEY_EXPIRY');t=time.perf_counter();expired=store.advance_floor('2026-10-05T00:00:00Z');key_gc_seconds=time.perf_counter()-t
    c=connect(store.state);freelist=c.execute('PRAGMA freelist_count').fetchone()[0];c.close()
    result={'result':'PASS_PRODUCER','events':accepted,'extra_new_segment_events':1+sum(x['inserted'] for x in continuing),'continuing_ingest_during_archive':{'operations':len(continuing),'p50_seconds':statistics.median(x['seconds'] for x in continuing),'p95_seconds':sorted(x['seconds'] for x in continuing)[min(len(continuing)-1,math.ceil(len(continuing)*.95)-1)],'failures':writer_errors},'replay_offers':replay_offers,'ingest_seconds':ingest_seconds,'rows_per_second':accepted/ingest_seconds,'rollover_seconds':rollover_seconds,'handoff_seconds':handoff_seconds,'archive':receipt,'retirement_seconds_including_rehash':retire_seconds,'filesystem_reclaimed_bytes':reclaimed,'peak_observed_wal_bytes':peak_wal,'growth_samples':bytes_day,'queries':queries,'actual_frontdoor_dense_refusal_seconds':dense_refusal_seconds,'state':{'tables_preserved':len(final['state_tables']),'cursors':len(final['cursor']),'journal_pending':final['pending'],'dedupe_keys_before_expiry':final['keys']},'key_gc':{'deleted':expired,'seconds':key_gc_seconds,'reusable_pages':freelist,'policy':'Fixture clock advances beyond the existing horizon; production floor unchanged.'},'rss_peak_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cpu_seconds':time.process_time(),'hot_state_final_bytes':store.state.stat().st_size,'parquet_spill_allocated_bytes':allocated(spill),'specimen':provenance,'constraints':'Qualification only; production unchanged. Physical host/NFS loss not injected. Archive health failures are deterministic fixtures, not disruption of shared archive.'}
    atomic(occurrence/'RESULT.json',result);progress(occurrence,'TERMINAL_PASS',events=accepted)


if __name__=='__main__':
    occurrence=Path(sys.argv[1]);terminal=Path(sys.argv[2]);start=dt.datetime.now(dt.timezone.utc).isoformat()
    try:main(occurrence)
    except BaseException as e:
        atomic(terminal,{'result':'FAILED','started_at':start,'finished_at':dt.datetime.now(dt.timezone.utc).isoformat(),'error':repr(e)});raise
    else:atomic(terminal,{'result':'PASS','started_at':start,'finished_at':dt.datetime.now(dt.timezone.utc).isoformat()})
