"""Finite exact-library SQLite cycle and equal-index PostgreSQL comparison.

Run only after the first occurrence is terminal. Its evidence is not modified.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
import psycopg
from common import allocated,atomic,digest,guard_root,sha,sqlite_db,stats
from benchmark import COLS,INSERT,rows,pg_copy

BASE=Path('/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-storage-architecture-20261005')
ORIGINAL=BASE/'runtime/occurrence-fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19'
CONTAINER='labelwatch-storage-pg-fd3a3c43'
EXPECTED_CONTAINER_ID='6a6297659afdb56a7e17f2a6afaade1478d1a3b63107c97cf1141edfd238937d'
SOCKET='/data/git/.lane-sockets/'+CONTAINER

def main(root):
    root=guard_root(root);root.mkdir();(root/'work').mkdir();(root/'evidence').mkdir();work=root/'work';evidence=root/'evidence'
    for fs in ('/','/data'):
        st=os.statvfs(fs)
        if st.f_bavail*st.f_frsize-8*1024**3<64424509440:raise RuntimeError('supplement admission refused')
    prior=json.loads((ORIGINAL/'evidence/PRODUCER-TERMINAL.json').read_text())
    if prior['status']!='PASS':raise RuntimeError('primary occurrence must be terminal PASS first')
    files=sorted((ORIGINAL/'runtime/corpus').glob('week-*.parquet'))
    src=work/'current.sqlite';c=sqlite_db(src);c.execute('PRAGMA synchronous=NORMAL')
    atomic(evidence/'PROGRESS.json',{'phase':'CURRENT_SQLITE_LOAD','loaded_rows':0})
    started=time.perf_counter();n=0
    for file in files:
        for batch in rows(file):c.executemany(INSERT,batch);c.commit();n+=len(batch)
        atomic(evidence/'PROGRESS.json',{'phase':'CURRENT_SQLITE_LOAD','loaded_rows':n})
    ingest=time.perf_counter()-started;c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    indexes=[{'name':name,'bytes':size} for name,size in c.execute('SELECT name,SUM(pgsize) FROM dbstat GROUP BY name')]
    c.executemany('INSERT OR REPLACE INTO meta VALUES (?,?)',[
        ('retention:live_floor','2026-08-03T00:00:00Z'),
        ('retention:history_start','2026-08-03T00:00:00Z')]);c.commit()
    before=allocated(src);beforestats=stats(c);c.close()
    # Execute the actual current export, verification and trim implementations.
    sys.path.insert(0,str(Path(__file__).parents[2]/'src'))
    from labelwatch.trim import export_partition,verify_partition,run_trim
    stop=threading.Event();samples=[]
    def sample():
        while not stop.wait(.1):
            sizes={}
            for f in work.rglob('*'):
                if f.is_file():
                    try:sizes[f.name]={'logical':f.stat().st_size,'allocated':f.stat().st_blocks*512}
                    except FileNotFoundError:pass
            samples.append({'elapsed':time.perf_counter(),'files':sizes})
    thread=threading.Thread(target=sample);thread.start()
    try:
        atomic(evidence/'PROGRESS.json',{'phase':'CURRENT_EXPORT','rows':n})
        t=time.perf_counter();manifest=export_partition(str(src),'2026-08-10T00:00:00Z',str(work/'archive'));export_s=time.perf_counter()-t
        t=time.perf_counter();verified=verify_partition(manifest);verify_s=time.perf_counter()-t
        c=sqlite3.connect(src);c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA synchronous=NORMAL')
        t=time.perf_counter()
        receipt=run_trim(c,'2026-08-10T00:00:00Z',manifest,now=dt.datetime(2026,10,5,tzinfo=dt.timezone.utc),
                         on_batch=lambda b:atomic(evidence/'PROGRESS.json',{'phase':'CURRENT_TRIM','batches':b}))
        trim_s=time.perf_counter()-t;c.execute('PRAGMA wal_checkpoint(TRUNCATE)');afterstats=stats(c);c.close()
    finally:stop.set();thread.join()
    wal_peak=max([v['logical'] for s in samples for k,v in s['files'].items() if k.endswith('-wal')]+[0])
    temp_peak=max([sum(v['allocated'] for k,v in s['files'].items() if k!='current.sqlite') for s in samples]+[0])
    single={'source':'exact src/labelwatch/trim.py at source base 51f923e','trim_source_sha256':sha(Path(__file__).parents[2]/'src/labelwatch/trim.py'),
            'rows':n,'ingest_seconds':ingest,'rows_per_second':n/ingest,'synchronous':'NORMAL, matching current db.connect',
            'export_seconds':export_s,'verification_seconds':verify_s,'seven_day_trim_seconds':trim_s,
            'archive_sqlite_bytes':allocated(work/'archive'),'before_allocated':before,'after_delete_allocated':allocated(src),
            'filesystem_reclaimed_immediately_bytes':before-allocated(src),'before_stats':beforestats,'after_stats':afterstats,
            'observed_wal_peak_bytes':wal_peak,'observed_temporary_peak_allocated_bytes':temp_peak,
            'sampling_interval_seconds':.1,'indexes':indexes,'receipt':receipt,
            'limits':'No concurrent collector, no installed production cold catalog, no full 44GB history VACUUM; this qualifies the library seven-day row cycle, not production admission/deployment.'}
    atomic(evidence/'CURRENT-SQLITE.json',single)
    # Reuse only the exactly identified stopped isolated cluster; original producer is terminal.
    state=json.loads(subprocess.check_output(['docker','inspect',CONTAINER,'--format','{{json .}}'],text=True))
    if state['Id']!=EXPECTED_CONTAINER_ID or state['State']['Running']:raise RuntimeError('owned cluster identity/state mismatch')
    subprocess.run(['docker','start',CONTAINER],check=True)
    for _ in range(120):
        try:admin=psycopg.connect(host=SOCKET,dbname='postgres',user='postgres',autocommit=True);break
        except psycopg.OperationalError:time.sleep(1)
    else:raise RuntimeError('owned fixture cluster unavailable')
    admin.execute('CREATE DATABASE indexedweek');admin.close()
    pg=psycopg.connect(host=SOCKET,dbname='indexedweek',user='postgres',autocommit=True)
    pg.execute('CREATE TABLE label_events (id BIGINT,labeler_did TEXT,src TEXT,uri TEXT,cid TEXT,val TEXT,neg BIGINT,exp TEXT,sig TEXT,ts TEXT NOT NULL,event_hash TEXT,target_did TEXT,PRIMARY KEY(id,ts),UNIQUE(event_hash,ts)) PARTITION BY RANGE(ts)')
    names=[]
    for f in files:
        day=f.stem.split('-',1)[1];end=(dt.date.fromisoformat(day)+dt.timedelta(days=1)).isoformat();name='p'+day.replace('-','');names.append(name)
        pg.execute(f"CREATE TABLE {name} PARTITION OF label_events FOR VALUES FROM ('{day}') TO ('{end}')")
    for name,cols in [('labeler_ts','labeler_did,ts'),('uri_ts','uri,ts'),('target_ts','target_did,ts'),('ts','ts'),('state','labeler_did,uri,val,ts')]:
        pg.execute(f'CREATE INDEX idx_{name} ON label_events ({cols})')
    pg.execute("CREATE INDEX idx_hide ON label_events(src,ts,uri) WHERE val='!hide' AND neg=0")
    atomic(evidence/'PROGRESS.json',{'phase':'POSTGRES_ALL_INDEXES_LOAD','rows':n})
    pm=pg_copy(pg,'label_events',files);pm['all_live_event_index_shapes_present']=True
    pm['relation_bytes']=sum(pg.execute('SELECT pg_total_relation_size(%s)',(name,)).fetchone()[0] for name in names)
    expected=digest(r for f in files for batch in rows(f) for r in batch)
    actual=digest(pg.execute('SELECT '+COLS+' FROM label_events ORDER BY id'))
    if expected!=actual:raise RuntimeError('equal-index PostgreSQL ingest parity failed')
    # Query and index comparison on the existing sampled database, with the missing shape added.
    query=psycopg.connect(host=SOCKET,dbname='spike',user='postgres',autocommit=True)
    query.execute('CREATE INDEX idx_state ON label_events(labeler_did,uri,val,ts)');query.execute('ANALYZE label_events')
    labeler=query.execute('SELECT labeler_did FROM label_events GROUP BY labeler_did ORDER BY COUNT(*) DESC LIMIT 1').fetchone()[0]
    times=[]
    for _ in range(7):
        t=time.perf_counter();query.execute("SELECT COUNT(DISTINCT uri) FROM label_events WHERE labeler_did=%s AND ts>='2026-05-16' AND ts<'2026-08-14'",(labeler,)).fetchone();times.append((time.perf_counter()-t)*1000)
    pm['sample_90d_distinct_subjects_all_indexes']={'p50_ms':sorted(times)[3],'p95_ms':max(times),'observations':7};query.close()
    # Table archive custody already proven separately by full-week dump/restore.
    # Do not retire these new source rows without a new verified archive receipt.
    pm['retirement']='Not repeated: indexedweek retained as replay data; original full-week verified dump/detach/drop evidence remains separate.'
    atomic(evidence/'POSTGRES-ALL-INDEXES.json',pm);pg.close();subprocess.run(['docker','stop','--timeout','30',CONTAINER],check=True)
    atomic(evidence/'PROGRESS.json',{'phase':'SUPPLEMENT_COMPLETE','rows':n})

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args();result={}
    try:main(a.root);result['status']='PASS'
    except BaseException as e:result.update(status='FAILED',error=type(e).__name__+': '+str(e));raise
    finally:
        if (a.root/'evidence').exists():atomic(a.root/'evidence/TERMINAL.json',{**result,'at':dt.datetime.now(dt.timezone.utc).isoformat()})
