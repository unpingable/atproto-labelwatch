"""Finite equal-index/equal-corpus ingest comparison; no production paths."""
import argparse
import json
import os
import subprocess
import time
from pathlib import Path
import psycopg
from common import atomic,guard_root,sqlite_db,allocated
from benchmark import rows,INSERT,pg_copy

BASE=Path('/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-storage-architecture-20261005')
CONTAINER='labelwatch-storage-pg-fd3a3c43'
SOCKET='/data/git/.lane-sockets/'+CONTAINER

def main(root):
    root=guard_root(root);root.mkdir();files=sorted((BASE/'runtime/occurrence-fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19/runtime/corpus').glob('sample-*.parquet'))
    result={'corpus':'same 180000 real rows over 90 dates','commit_batch_rows':10000,'profiles':{}}
    for name,sync,segmented in [('current_normal','NORMAL',False),('repaired_normal','NORMAL',False),('segmented_full','FULL',True)]:
        folder=root/name;folder.mkdir();n=0;seconds=0
        c=None if segmented else sqlite_db(folder/'events.sqlite')
        if c:c.execute('PRAGMA synchronous='+sync)
        for file in files:
            if segmented:c=sqlite_db(folder/(file.stem+'.sqlite'));c.execute('PRAGMA synchronous='+sync)
            t=time.perf_counter()
            for batch in rows(file):c.executemany(INSERT,batch);c.commit();n+=len(batch)
            seconds+=time.perf_counter()-t
            if segmented:c.close()
        if not segmented:c.execute('PRAGMA wal_checkpoint(TRUNCATE)');c.close()
        result['profiles'][name]={'rows':n,'insert_seconds_excluding_schema_creation':seconds,'rows_per_second':n/seconds,'allocated_bytes':allocated(folder),'indexes':'all seven live event index shapes including state/hash','sync':sync}
        atomic(root/'PROGRESS.json',{'phase':name,'rows':n})
    state=json.loads(subprocess.check_output(['docker','inspect',CONTAINER,'--format','{{json .}}'],text=True))
    if state['Id']!='6a6297659afdb56a7e17f2a6afaade1478d1a3b63107c97cf1141edfd238937d' or state['State']['Running']:
        raise RuntimeError('requires exact stopped qualified fixture cluster')
    subprocess.run(['docker','start',CONTAINER],check=True)
    for _ in range(120):
        try:c=psycopg.connect(host=SOCKET,dbname='postgres',user='postgres',autocommit=True);break
        except psycopg.OperationalError:time.sleep(1)
    else:raise RuntimeError('owned sample cluster unavailable')
    c.execute('CREATE DATABASE sampleingest');c.close();c=psycopg.connect(host=SOCKET,dbname='sampleingest',user='postgres',autocommit=True)
    c.execute('CREATE TABLE label_events (id BIGINT,labeler_did TEXT,src TEXT,uri TEXT,cid TEXT,val TEXT,neg BIGINT,exp TEXT,sig TEXT,ts TEXT NOT NULL,event_hash TEXT,target_did TEXT,PRIMARY KEY(id,ts),UNIQUE(event_hash,ts)) PARTITION BY RANGE(ts)')
    import datetime as dt
    for f in files:
        day=f.stem.split('-',1)[1];end=(dt.date.fromisoformat(day)+dt.timedelta(days=1)).isoformat();name='p'+day.replace('-','')
        c.execute(f"CREATE TABLE {name} PARTITION OF label_events FOR VALUES FROM ('{day}') TO ('{end}')")
    for name,cols in [('labeler_ts','labeler_did,ts'),('uri_ts','uri,ts'),('target_ts','target_did,ts'),('ts','ts'),('state','labeler_did,uri,val,ts')]:c.execute(f'CREATE INDEX idx_{name} ON label_events ({cols})')
    c.execute("CREATE INDEX idx_hide ON label_events(src,ts,uri) WHERE val='!hide' AND neg=0")
    result['profiles']['postgres_all_indexes']=pg_copy(c,'label_events',files);result['profiles']['postgres_all_indexes']['indexes']='all live event shapes; partition-key-inclusive uniqueness';c.close()
    subprocess.run(['docker','stop','--timeout','30',CONTAINER],check=True)
    atomic(root/'RESULT.json',result)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args();result={}
    try:main(a.root);result['status']='PASS'
    except BaseException as e:result.update(status='FAILED',error=type(e).__name__+': '+str(e));raise
    finally:
        if a.root.exists():atomic(a.root/'TERMINAL.json',result)
