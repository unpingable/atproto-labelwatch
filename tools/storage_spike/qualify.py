"""Finite process-interruption and boundary cases against owned substrates."""
from __future__ import annotations
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
import psycopg
from common import FIELDS,atomic,digest,guard_root,sqlite_db
from lifecycle import CUTS,run

def fixture(root,backend):
    root.mkdir();data=[(i,'did:fixture:labeler','did:fixture:labeler',f'at://did:fixture:subject/post/{i}',None,'test',i%2,None,None,'2026-01-01T00:00:00Z',f'fixture-hash-{i}','did:fixture:subject') for i in range(1,11)]
    if backend=='sqlite':
        c=sqlite_db(root/'sealed.sqlite');c.executemany('INSERT INTO label_events VALUES ('+','.join('?' for _ in FIELDS)+')',data);c.commit();c.execute('PRAGMA wal_checkpoint(TRUNCATE)');c.close()
    else:
        c=psycopg.connect(host=os.environ['SPIKE_PG_SOCKET'],dbname='fixtures',user='postgres',autocommit=True)
        ns=root.name;c.execute(f'CREATE SCHEMA {ns}')
        c.execute(f'CREATE TABLE {ns}.events (id BIGINT,labeler_did TEXT,src TEXT,uri TEXT,cid TEXT,val TEXT,neg BIGINT,exp TEXT,sig TEXT,ts TEXT,event_hash TEXT,target_did TEXT) PARTITION BY RANGE(ts)')
        c.execute(f"CREATE TABLE {ns}.label_events PARTITION OF {ns}.events FOR VALUES FROM ('2026-01-01') TO ('2026-01-02')")
        with c.cursor() as cur:cur.executemany(f'INSERT INTO {ns}.events VALUES ('+','.join('%s' for _ in FIELDS)+')',data)
        c.close()
    atomic(root/'accepted.json',digest(data));return root

def call(root,backend,cut=None):
    args=[sys.executable,str(Path(__file__).with_name('lifecycle.py')),str(root),'--backend',backend]
    if cut:args+=['--cut',cut]
    return subprocess.run(args,capture_output=True,text=True,timeout=30)

def qualifies(base):
    base=guard_root(base);suite=base/'qualification';suite.mkdir();results=[]
    for backend in ('sqlite','postgres'):
        for cut in CUTS:
            root=fixture(suite/f'{backend}_{cut}',backend)
            first=call(root,backend,cut);assert first.returncode==73,(cut,first.stderr)
            second=call(root,backend);assert second.returncode==0,(cut,second.stderr)
            retry=call(root,backend);assert retry.returncode==0,retry.stderr
            results.append({'backend':backend,'case':'process_loss_'+cut,'status':'PASS','recovery':'reopen source/receipt, verify archive, idempotently retire; one explicit supervisor retry','accepted_rows':10})
        for fail in ('archive_unavailable','archive_read_only','archive_full','root_low','retire_failed'):
            root=fixture(suite/f'{backend}_{fail}',backend)
            try:run(root,failure=fail,backend=backend)
            except (RuntimeError,PermissionError,OSError):pass
            else:raise AssertionError('failure not refused')
            if backend=='sqlite':assert (root/'sealed.sqlite').exists()
            else:
                c=psycopg.connect(host=os.environ['SPIKE_PG_SOCKET'],dbname='fixtures',user='postgres');assert c.execute(f'SELECT COUNT(*) FROM {root.name}.label_events').fetchone()[0]==10;c.close()
            retry=call(root,backend);assert retry.returncode==0,retry.stderr
            results.append({'backend':backend,'case':fail,'status':'PASS','recovery':'storage/operator prerequisite restores, then retry; source preserved'})
        root=fixture(suite/f'{backend}_corrupt',backend);first=call(root,backend,'after_receipt');assert first.returncode==73
        with (root/'archive.parquet').open('r+b') as f:f.write(b'BAD!')
        second=call(root,backend);assert second.returncode!=0
        results.append({'backend':backend,'case':'corrupted_published_archive','status':'PASS','recovery':'refused; source retained; operator custody reconciliation required'})
        root=fixture(suite/f'{backend}_stale',backend);assert call(root,backend,'after_receipt').returncode==73
        atomic(root/'checkpoint.json',{'phase':'after_retire'});atomic(root/'catalog.json',{'state':'ARCHIVED','sha256':'incorrect'})
        assert call(root,backend).returncode==0
        results.append({'backend':backend,'case':'stale_progress_and_catalog','status':'PASS','recovery':'authority reconstructed from accepted-set fixture and source/verified receipt, progress ignored'})
    # Demonstrate why a one-writable-file event-time design needs more than rollover.
    root=fixture(suite/'sqlite_late', 'sqlite');run(root)
    old=sqlite3.connect(f'file:{root}/sealed.sqlite?mode=ro',uri=True) if (root/'sealed.sqlite').exists() else None
    assert old is None
    results.append({'backend':'sqlite','case':'late_event_for_retired_event_time_period','status':'UNQUALIFIED','finding':'one-file event-time prototype has no destination after retirement; must quarantine below committed floor or retain mutable period / use late part protocol. Never acknowledged as accepted.'})
    c=psycopg.connect(host=os.environ['SPIKE_PG_SOCKET'],dbname='fixtures',user='postgres',autocommit=True)
    root=fixture(suite/'postgres_late','postgres');run(root,backend='postgres')
    try:c.execute("INSERT INTO postgres_late.events (ts) VALUES ('2026-01-01T01:00:00Z')")
    except psycopg.errors.CheckViolation: pass
    else:raise AssertionError('missing partition accepted late row')
    results.append({'backend':'postgres','case':'late_event_for_retired_period','status':'PASS_REFUSAL','finding':'native partition route refuses; production quarantine/cursor policy still requires integration'})
    c.close()
    root=fixture(suite/'sqlite_schema','sqlite');c=sqlite3.connect(root/'sealed.sqlite');c.execute("UPDATE meta SET value='24'");c.commit();c.close()
    assert call(root,'sqlite').returncode!=0
    results.append({'backend':'sqlite','case':'unsupported_schema_generation','status':'PASS_REFUSAL','finding':'version23 only; no silent coercion or immutable-history rewrite'})
    atomic(base.parent/'evidence/FAILURE-QUALIFICATION.json',{'cases':results,'process_loss_scope':'os._exit at deterministic boundaries, not host power loss; PostgreSQL server restart tested separately','unqualified':['clock-driven production rollover','cross-file atomic cursor/state transfer','schema-version adapters','concurrent writer fencing','real NFS server restart/stable storage','late-record supplemental custody']})
    return results

if __name__=='__main__':qualifies(Path(sys.argv[1]))
