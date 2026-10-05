"""Comparable private corpus measurements, not production qualification."""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import io
import json
import os
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path
import duckdb
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from common import FIELDS,allocated,atomic,digest,guard_root,sha,sqlite_db,stats,timed

ARCHIVE=Path('/tank/nfs/archive/atproto/labelwatch-v3-qualification-20260920/full/partitions')
COLS=','.join(FIELDS)
INSERT='INSERT INTO label_events VALUES ('+','.join('?' for _ in FIELDS)+')'

def rows(path):
    for b in pq.ParquetFile(path).iter_batches(batch_size=10000):
        yield [tuple(r[k] for k in FIELDS) for r in b.to_pylist()]

def progress(root,phase,**details):
    for fs in ('/','/data'):
        s=os.statvfs(fs)
        if s.f_bavail*s.f_frsize<64424509440:raise RuntimeError('shared reserve lost; refuse next phase')
    if allocated(root)>17179869184:raise RuntimeError('campaign data allocation ceiling exceeded')
    print(json.dumps({'phase':phase,**details}),flush=True)
    atomic(root.parent/'evidence/PROGRESS.json',{'phase':phase,'time':dt.datetime.now(dt.timezone.utc).isoformat(),**details})

def corpus(root):
    out=root/'corpus';out.mkdir(); receipts=[]
    start=dt.date(2026,5,16); week=[]; sampled=[]
    for offset in range(90):
        day=(start+dt.timedelta(days=offset)).isoformat(); source=ARCHIVE/day/f'date={day}.parquet'
        manifest=json.loads((source.with_suffix('.manifest.json')).read_text())
        # Verify selected complete weekly inputs, do not scan/hash 90 full days.
        full='2026-08-03'<=day<'2026-08-10'
        if full:
            actual=sha(source)
            if actual!=manifest['partition']['sha256']:raise RuntimeError('source archive hash mismatch')
            dest=out/f'week-{day}.parquet';shutil.copyfile(source,dest);week.append(dest)
        # Bounded ordinal-stratified sampling preserves actual identities/NULLs/schema/time.
        pf=pq.ParquetFile(source);limit=2000;stride=max(1,pf.metadata.num_rows//limit)
        picked=[];ordinal=0
        for batch in pf.iter_batches(batch_size=50000):
            indices=[i for i in range(batch.num_rows) if (ordinal+i)%stride==0]
            picked.extend(pa.Table.from_batches([batch]).take(pa.array(indices, type=pa.int64())).to_pylist())
            ordinal+=batch.num_rows
        table=pa.Table.from_pylist(picked[:limit],schema=pf.schema_arrow)
        dest=out/f'sample-{day}.parquet';pq.write_table(table,dest,compression='zstd');sampled.append(dest)
        receipts.append({'day':day,'source':str(source),'manifest_sha256':sha(source.with_suffix('.manifest.json')),
                         'source_sha256':manifest['partition']['sha256'],'source_rows':pf.metadata.num_rows,
                         'sample_rows':table.num_rows,'sample_sha256':sha(dest),'full_week_verified':full,
                         'schema_version':manifest['source']['schema_version']})
        if offset%10==0:progress(root,'CORPUS',days=offset+1)
    atomic(root.parent/'evidence/DATASET.json',{'classification':'PRIVATE_IDENTITIES_NOT_FOR_PUBLICATION',
            'source_revision':'51f923e6f28bc9e5cb7550b5f4895c8a91c5cca9','provenance':receipts,
            'sample_method':'2000 maximum ordinal-stratified real rows per UTC day; 90 days. Seven full real days Aug3-9 separately. No synthetic rate assumption.'})
    return week,sampled

def load_sqlite(path,files):
    c=sqlite_db(path);n=0;t=time.perf_counter()
    for file in files:
        for batch in rows(file):c.executemany(INSERT,batch);c.commit();n+=len(batch)
    duration=time.perf_counter()-t;c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    return c,{'rows':n,'ingest_seconds':duration,'rows_per_second':n/duration,'allocated':allocated(path),'stats':stats(c)}

def pgconnect(root, db='spike'):
    return psycopg.connect(host=str(root/'pgsocket'),user='postgres',dbname=db,autocommit=True)

def pg_copy(c,table,files):
    count=0;t=time.perf_counter()
    with c.cursor() as cur:
        with cur.copy(f'COPY {table} ({COLS}) FROM STDIN') as copy:
            for file in files:
                for batch in rows(file):
                    for row in batch:copy.write_row(row);count+=1
    elapsed=time.perf_counter()-t
    return {'rows':count,'ingest_seconds':elapsed,'rows_per_second':count/elapsed}

def postgres(root,files):
    c=pgconnect(root,'postgres');c.execute('CREATE DATABASE spike');c.close();c=pgconnect(root)
    ddl='id BIGINT NOT NULL,labeler_did TEXT NOT NULL,src TEXT,uri TEXT NOT NULL,cid TEXT,val TEXT NOT NULL,neg BIGINT,exp TEXT,sig TEXT,ts TEXT NOT NULL,event_hash TEXT NOT NULL,target_did TEXT'
    c.execute(f'CREATE TABLE label_events ({ddl},PRIMARY KEY(id,ts),UNIQUE(event_hash,ts)) PARTITION BY RANGE(ts)')
    days=sorted({f.stem.split('-',1)[1] for f in files})
    for day in days:
        end=(dt.date.fromisoformat(day)+dt.timedelta(days=1)).isoformat()
        name='p'+day.replace('-','');c.execute(f"CREATE TABLE {name} PARTITION OF label_events FOR VALUES FROM ('{day}') TO ('{end}')")
    for name,cols in [('labeler_ts','labeler_did,ts'),('uri_ts','uri,ts'),('target_ts','target_did,ts'),('ts','ts')]:
        c.execute(f'CREATE INDEX idx_{name} ON label_events ({cols})')
    c.execute("CREATE INDEX idx_hide ON label_events(src,ts,uri) WHERE val='!hide' AND neg=0")
    c.execute('CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
    return c,pg_copy(c,'label_events',files)

def queries(root,files):
    c,m=load_sqlite(root/'queries.sqlite',files)
    d=duckdb.connect();d.execute("SET memory_limit='512MB'");d.execute('SET threads=2')
    spill=root/'duck-spill';spill.mkdir();d.execute('SET temp_directory=?',[str(spill)])
    d.from_parquet([str(f) for f in files]).create_view('label_events')
    pg,pm=postgres(root,files);pg.execute('ANALYZE label_events')
    labeler=c.execute('SELECT labeler_did FROM label_events GROUP BY labeler_did ORDER BY COUNT(*) DESC LIMIT 1').fetchone()[0]
    target=c.execute('SELECT target_did FROM label_events WHERE target_did IS NOT NULL GROUP BY target_did ORDER BY COUNT(*) DESC LIMIT 1').fetchone()[0]
    uri=c.execute('SELECT uri FROM label_events GROUP BY uri ORDER BY COUNT(*) DESC LIMIT 1').fetchone()[0]
    sqls={
      'report_30d_count':("SELECT COUNT(*) FROM label_events WHERE labeler_did=? AND ts>=? AND ts<?",(labeler,'2026-07-15','2026-08-14')),
      'report_90d_distinct_subjects':("SELECT COUNT(DISTINCT uri) FROM label_events WHERE labeler_did=? AND ts>=? AND ts<?",(labeler,'2026-05-16','2026-08-14')),
      'subject_Q3':("SELECT labeler_did,val,COUNT(*),MIN(ts),MAX(ts) FROM label_events WHERE target_did=? GROUP BY labeler_did,val ORDER BY labeler_did,val",(target,)),
      'reversal_ordered':("SELECT uri,val,neg,ts,event_hash FROM label_events WHERE labeler_did=? AND ts>=? AND ts<? ORDER BY uri,val,ts,event_hash",(labeler,'2026-08-07','2026-08-14')),
      'latest_subject_state':("SELECT labeler_did,val,neg,ts FROM label_events WHERE uri=? ORDER BY ts DESC,id DESC LIMIT 50",(uri,)),
      'retention_horizon':("SELECT MIN(ts),MAX(ts),COUNT(*) FROM label_events",()),
    }
    # Attached daily SQLite is evaluated in bounded groups, not silently beyond the default limit.
    segmentdir=root/'query-segments';segmentdir.mkdir();segpaths=[]
    for f in files:
        path=segmentdir/(f.stem+'.sqlite');sc,_=load_sqlite(path,[f]);sc.close();segpaths.append(path)
    # Executable fallback for the hot/cold union: stream bounded batches into
    # a DuckDB question-local table. Record the tax instead of hiding a cache.
    h=duckdb.connect();h.execute("SET memory_limit='512MB'");h.execute('SET threads=2')
    h.execute('SET temp_directory=?',[str(spill)])
    h.execute('CREATE TABLE label_events AS SELECT * FROM read_parquet(?) LIMIT 0',[str(files[0])])
    t=time.perf_counter()
    for path in segpaths:
        sc=sqlite3.connect(f'file:{path}?mode=ro&immutable=1',uri=True)
        cursor=sc.execute('SELECT '+COLS+' FROM label_events')
        while batch:=cursor.fetchmany(10000):
            arrow=pa.Table.from_pylist([dict(zip(FIELDS,r)) for r in batch],schema=pq.ParquetFile(files[0]).schema_arrow)
            h.register('batch',arrow);h.execute('INSERT INTO label_events SELECT * FROM batch');h.unregister('batch')
        sc.close()
    hot_union_setup=time.perf_counter()-t
    result={}
    for name,(sql,args) in sqls.items():
        answers={}; measures={}
        for engine,conn,statement in [('sqlite',c,sql),('parquet_duckdb',d,sql),('hot_union_duckdb',h,sql),('postgres',pg,sql.replace('?','%s'))]:
            times=[];ans=None
            for _ in range(7):
                elapsed,ans=timed(lambda:conn.execute(statement,args).fetchall());times.append(elapsed*1000)
            answers[engine]=ans;ordered=sorted(times)
            measures[engine]={'p50_ms':ordered[len(ordered)//2],'p95_ms':ordered[-1],'observations':7,'result_rows':len(ans)}
        if any(answer!=answers['sqlite'] for answer in answers.values()):
            raise RuntimeError('query parity differs: '+name)
        result[name]={'parity':'PASS','timings':measures}
    limit=c.getlimit(sqlite3.SQLITE_LIMIT_ATTACHED)
    atomic(root.parent/'evidence/QUERIES.json',{'dataset':'180000-row maximum 90-day stratified corpus, NOT full 90-day workload','queries':result,'sqlite_ingest':m,'postgres_ingest':pm,'sqlite_attach_limit':limit,'daily_segment_count':len(segpaths),'cold_duckdb_spill_allocated':allocated(spill),'hot_union_setup_seconds':hot_union_setup,'hot_fanout':'180k sampled hot rows streamed in bounded batches into an ephemeral DuckDB table. Timings exclude reported setup tax; full-hot-horizon cost UNQUALIFIED.'})
    c.close();d.close();h.close();pg.close()

def retention(root,week):
    progress(root,'SQLITE_FULL_WEEK')
    c,m=load_sqlite(root/'week.sqlite',week)
    before=allocated(root/'week.sqlite');beforestats=stats(c);peak_wal=0;t=time.perf_counter();deleted=0
    while True:
        ids=[r[0] for r in c.execute('SELECT id FROM label_events ORDER BY id LIMIT 50000')]
        if not ids:break
        c.executemany('DELETE FROM label_events WHERE id=?',[(i,) for i in ids]);c.commit();deleted+=len(ids)
        wal=root/'week.sqlite-wal'
        peak_wal=max(peak_wal,wal.stat().st_size if wal.exists() else 0)
        c.execute('PRAGMA wal_checkpoint(PASSIVE)')
    deletion=time.perf_counter()-t;c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    after=allocated(root/'week.sqlite');afterstats=stats(c)
    t=time.perf_counter();c.execute('VACUUM');vacuum=time.perf_counter()-t;c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    single={**m,'retired_rows':deleted,'delete_seconds':deletion,'before_allocated':before,'after_delete_allocated':after,'reclaimed_immediate_bytes':before-after,'before_stats':beforestats,'after_stats':afterstats,'observed_wal_high_water_bytes':peak_wal,'vacuum_seconds_empty_residual':vacuum,'after_vacuum_allocated':allocated(root/'week.sqlite'),'vacuum_full_history_cost':'NOT MEASURED: this empty-residual test is not a 44GB full-history VACUUM qualification.'}
    c.close()
    progress(root,'SEGMENTED_FULL_WEEK')
    segments=root/'retention-segments';segments.mkdir();paths=[];loads=[]
    for file in week:
        path=segments/(file.stem+'.sqlite');c,metrics=load_sqlite(path,[file]);c.close();paths.append(path);loads.append(metrics)
    before=allocated(segments)
    # Existing Parquet corpus is independently content-equivalent archive for each segment.
    archive_rows=0;verification_seconds=0
    for path,file in zip(paths,week):
        c=sqlite3.connect(f'file:{path}?mode=ro&immutable=1',uri=True)
        t=time.perf_counter();a=digest(c.execute('SELECT '+COLS+' FROM label_events ORDER BY id'));c.close()
        b=digest(r for batch in rows(file) for r in batch)
        if a!=b:raise RuntimeError('full weekly segment archive parity failed')
        verification_seconds+=time.perf_counter()-t;archive_rows+=a['rows']
    t=time.perf_counter()
    for path in paths:path.unlink()
    from common import fsync_dir
    fsync_dir(segments);retirement=time.perf_counter()-t
    segmented={'rows':archive_rows,'ingest_seconds':sum(m['ingest_seconds'] for m in loads),'rows_per_second':archive_rows/sum(m['ingest_seconds'] for m in loads),'file_count':len(paths),'retirement_seconds_excluding_archive_verification':retirement,'archive_verification_seconds':verification_seconds,'before_allocated':before,'after_allocated':allocated(segments),'filesystem_reclaimed_bytes':before-allocated(segments),'archive_bytes_existing':sum(p.stat().st_size for p in week),'archive_bytes_newly_written_in_this_measurement':0,'reuse_limit':'Archive already exists. Fresh seal-to-Parquet conversion measured in lifecycle fixtures; large conversion amplification not measured.'}
    progress(root,'POSTGRES_FULL_WEEK')
    # Separate owned DB retains the same indexes and physical partitioning.
    pg=pgconnect(root,'postgres');pg.execute('CREATE DATABASE weekspike');pg.close()
    # Use an independent database by temporarily selecting it via explicit wrapper.
    pg=pgconnect(root,'weekspike')
    pg.execute('CREATE TABLE label_events (id BIGINT,labeler_did TEXT,src TEXT,uri TEXT,cid TEXT,val TEXT,neg BIGINT,exp TEXT,sig TEXT,ts TEXT NOT NULL,event_hash TEXT,target_did TEXT,PRIMARY KEY(id,ts),UNIQUE(event_hash,ts)) PARTITION BY RANGE(ts)')
    names=[]
    for f in week:
        day=f.stem.split('-',1)[1];end=(dt.date.fromisoformat(day)+dt.timedelta(days=1)).isoformat();name='p'+day.replace('-','');names.append(name)
        pg.execute(f"CREATE TABLE {name} PARTITION OF label_events FOR VALUES FROM ('{day}') TO ('{end}')")
    for name,cols in [('labeler_ts','labeler_did,ts'),('uri_ts','uri,ts'),('target_ts','target_did,ts'),('ts','ts')]:pg.execute(f'CREATE INDEX idx_{name} ON label_events ({cols})')
    pg.execute("CREATE INDEX idx_hide ON label_events(src,ts,uri) WHERE val='!hide' AND neg=0")
    pm=pg_copy(pg,'label_events',week)
    relation_bytes=sum(pg.execute('SELECT pg_total_relation_size(%s)',(n,)).fetchone()[0] for n in names)
    dump=root/'weekspike.dump';container='labelwatch-storage-pg-abf37f3e'
    t=time.perf_counter()
    with dump.open('wb') as f:subprocess.run(['docker','exec',container,'pg_dump','-U','postgres','-Fc','weekspike'],stdout=f,check=True)
    dumpseconds=time.perf_counter()-t
    with dump.open('rb') as f:os.fsync(f.fileno())
    pgadmin=pgconnect(root,'postgres');pgadmin.execute('CREATE DATABASE weekrestore');pgadmin.close()
    t=time.perf_counter()
    with dump.open('rb') as f:subprocess.run(['docker','exec','-i',container,'pg_restore','-U','postgres','-d','weekrestore'],stdin=f,check=True)
    restores=time.perf_counter()-t;restored=pgconnect(root,'weekrestore')
    original=digest(pg.execute('SELECT '+COLS+' FROM label_events ORDER BY id'))
    rebound=digest(restored.execute('SELECT '+COLS+' FROM label_events ORDER BY id'));restored.close()
    if original!=rebound:raise RuntimeError('PostgreSQL dump/restore differs')
    t=time.perf_counter()
    with pg.transaction():
        for name in names:pg.execute(f'ALTER TABLE label_events DETACH PARTITION {name}')
    detach=time.perf_counter()-t
    # Detach alone reclaims zero. Drop relations after verified dump restore in isolated DB.
    t=time.perf_counter()
    for name in names:pg.execute(f'DROP TABLE {name}')
    drop=time.perf_counter()-t
    pm.update({'partition_relation_bytes':relation_bytes,'detach_seconds':detach,'drop_seconds':drop,'detach_reclaims_bytes':0,'dropped_relation_bytes':relation_bytes,'archive_dump_bytes':dump.stat().st_size,'dump_seconds':dumpseconds,'restore_seconds':restores,'restore_parity':'PASS','wal_directory_peak':'NOT MEASURED','filesystem_free_delta':'Concurrent shared /data allocation prevents attribution; relation bytes measured, WAL may remain.'});pg.close()
    atomic(root.parent/'evidence/RETENTION.json',{'single_sqlite':single,'segmented_sqlite':segmented,'postgres':pm})

def main():
    p=argparse.ArgumentParser();p.add_argument('root');a=p.parse_args();root=guard_root(a.root)
    week,sampled=corpus(root);progress(root,'QUERY_COMPARISON');queries(root,sampled);retention(root,week)
    progress(root,'BENCHMARK_COMPLETE')

if __name__=='__main__':main()
