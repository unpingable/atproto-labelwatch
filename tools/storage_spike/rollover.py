"""One writable arrival-period vessel; finite metadata/cursor handoff prototype.

Includes accepted events and ingest cursor in one SQLite transaction. This is
not the complete Labelwatch operational-state backend. Archive-only dedupe and
full mutable-state transfer remain explicit integration gates.
"""
from __future__ import annotations
import datetime as dt
import fcntl
import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from common import FIELDS,atomic,fsync_dir,guard_root,sha,sqlite_db

@contextmanager
def writer(root):
    with (root/'writer.lock').open('a') as f:
        fcntl.flock(f,fcntl.LOCK_EX)
        yield

def phase(cut,name):
    if cut==name:os._exit(73)

def metadata(c):return dict(c.execute('SELECT key,value FROM meta'))

def seal(c):
    c.execute("INSERT OR REPLACE INTO meta VALUES ('sealed','1')");c.commit()
    c.execute('PRAGMA wal_checkpoint(TRUNCATE)');c.close()

def create(root,period):
    root=guard_root(root);root.mkdir();dt.date.fromisoformat(period)
    path=root/('arrival-'+period+'.sqlite');c=sqlite_db(path)
    c.executemany('INSERT OR REPLACE INTO meta VALUES (?,?)',[('period',period),('sequence','0'),('cursor','0'),('live_floor','2026-01-01T00:00:00Z')]);c.commit();c.close()
    atomic(root/'ACTIVE.json',{'file':path.name,'period':period})

def _roll(root,nextperiod,cut=None):
    pointer=json.loads((root/'ACTIVE.json').read_text());path=root/pointer['file']
    c=sqlite3.connect(path);m=metadata(c)
    if nextperiod<=m['period']:c.close();raise RuntimeError('clock does not advance period')
    name='arrival-'+nextperiod+'.sqlite'
    if m.get('sealed')!='1':
        c.executemany('INSERT OR REPLACE INTO meta VALUES (?,?)',[('next_file',name),('next_period',nextperiod)])
        seal(c)
    else:
        c.close()
        if m.get('next_file')!=name:raise RuntimeError('sealed transition identity differs')
    phase(cut,'after_seal');oldsha=sha(path)
    c=sqlite3.connect(f'file:{path}?mode=ro&immutable=1',uri=True);m=metadata(c);c.close()
    nxt=root/name
    if not nxt.exists():
        c=sqlite_db(nxt)
        copied={k:v for k,v in m.items() if k not in ('sealed','next_file','next_period','predecessor_sha256')}
        copied.update(period=nextperiod,predecessor_sha256=oldsha)
        c.executemany('INSERT OR REPLACE INTO meta VALUES (?,?)',copied.items());c.commit();c.close()
        with nxt.open('rb') as f:os.fsync(f.fileno())
        fsync_dir(root)
    phase(cut,'after_next_create')
    c=sqlite3.connect(f'file:{nxt}?mode=ro',uri=True)
    if c.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('next vessel invalid')
    if metadata(c).get('predecessor_sha256')!=oldsha:raise RuntimeError('next predecessor identity differs')
    c.close();phase(cut,'after_next_verify')
    atomic(root/'ACTIVE.json',{'file':name,'period':nextperiod,'predecessor_sha256':oldsha})
    phase(cut,'after_pointer')

def recover(root):
    # Stale progress is irrelevant; immutable sealed predecessor binds its successor.
    for _ in range(16):
        pointer=json.loads((root/'ACTIVE.json').read_text());c=sqlite3.connect(f"file:{root/pointer['file']}?mode=ro",uri=True);m=metadata(c);c.close()
        if m.get('sealed')!='1':return pointer
        _roll(root,m['next_period'])
    raise RuntimeError('bounded successor chain exhausted; operator reconciliation')

def rotate(root,period,cut=None):
    root=guard_root(root);dt.date.fromisoformat(period)
    with writer(root):recover(root);_roll(root,period,cut)

def ingest(root,ts,token,cut=None):
    root=guard_root(root)
    with writer(root):
        p=recover(root);path=root/p['file'];c=sqlite3.connect(path)
        c.execute('PRAGMA synchronous=FULL');c.execute('BEGIN IMMEDIATE');m=metadata(c)
        if ts<m['live_floor']:c.rollback();c.close();return {'accepted':False,'reason':'below_committed_floor'}
        key=hashlib.sha256(json.dumps({'ts':ts,'uri':'did:fixture:subject','val':'test'},sort_keys=True).encode()).hexdigest()
        # Exact local-fixture dedupe. Once source files retire to Parquet this
        # is not sufficient; never promote it as a complete reader/ingest path.
        duplicate=False
        for file in root.glob('arrival-*.sqlite'):
            other=sqlite3.connect(f'file:{file}?mode=ro',uri=True)
            duplicate=bool(other.execute('SELECT 1 FROM label_events WHERE event_hash=?',(key,)).fetchone())
            other.close()
            if duplicate:break
        if not duplicate:
            i=int(m['sequence'])+1
            row=(i,'did:fixture:labeler',None,'did:fixture:subject',None,'test',0,None,None,ts,key,'did:fixture:subject')
            c.execute('INSERT INTO label_events VALUES ('+','.join('?' for _ in FIELDS)+')',row)
            c.execute("INSERT OR REPLACE INTO meta VALUES ('sequence',?)",(str(i),))
        c.execute("INSERT OR REPLACE INTO meta VALUES ('cursor',?)",(token,))
        phase(cut,'before_ingest_commit');c.commit();phase(cut,'after_ingest_commit');c.close()
        return {'accepted':True,'duplicate':duplicate}

def facts(root):
    data=[]
    for file in root.glob('arrival-*.sqlite'):
        c=sqlite3.connect(f'file:{file}?mode=ro',uri=True);data.extend(c.execute('SELECT id,event_hash,ts FROM label_events'));c.close()
    pointer=json.loads((root/'ACTIVE.json').read_text());c=sqlite3.connect(f"file:{root/pointer['file']}?mode=ro",uri=True);m=metadata(c);c.close()
    return {'rows':len(data),'distinct_hashes':len({r[1] for r in data}),'distinct_ids':len({r[0] for r in data}),'cursor':m['cursor'],'period':m['period']}

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('action',choices=['rotate','ingest']);p.add_argument('root',type=Path);p.add_argument('value');p.add_argument('--token',default='2');p.add_argument('--cut');a=p.parse_args()
    if a.action=='rotate':rotate(a.root,a.value,a.cut)
    else:print(json.dumps(ingest(a.root,a.value,a.token,a.cut)))
