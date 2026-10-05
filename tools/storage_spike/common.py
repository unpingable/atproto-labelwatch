"""Private, isolated architecture qualification. Never accepts production paths."""
from __future__ import annotations
import ast
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

FIELDS = ('id','labeler_did','src','uri','cid','val','neg','exp','sig','ts','event_hash','target_did')

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''): h.update(b)
    return h.hexdigest()

def fsync_dir(path):
    fd=os.open(path,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)

def atomic(path, value):
    path=Path(path); tmp=path.with_suffix(path.suffix+'.incomplete')
    with tmp.open('w') as f:
        json.dump(value,f,sort_keys=True,indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path); fsync_dir(path.parent)

def allocated(path):
    p=Path(path)
    return sum(f.stat().st_blocks*512 for f in p.rglob('*') if f.is_file()) if p.is_dir() else p.stat().st_blocks*512

def schema():
    tree=ast.parse((Path(__file__).parents[2]/'src/labelwatch/db.py').read_text())
    values={n.targets[0].id:ast.literal_eval(n.value) for n in tree.body
            if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)
            and n.targets[0].id in ('SCHEMA_TABLES','SCHEMA_INDEXES')}
    event_ddl=next(x for x in values['SCHEMA_TABLES'].split(';') if 'CREATE TABLE IF NOT EXISTS label_events (' in x)
    indexes=[x for x in values['SCHEMA_INDEXES'].split(';') if ' ON label_events(' in x]
    return event_ddl+';', ';'.join(indexes)+';'

def sqlite_db(path):
    c=sqlite3.connect(path); c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=FULL')
    tables,indexes=schema(); c.executescript(tables+indexes)
    c.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
    c.execute("INSERT OR REPLACE INTO meta VALUES ('schema_version','23')"); c.commit()
    return c

def digest(rows):
    h=hashlib.sha256(); n=0
    for r in rows:
        h.update((json.dumps(list(r),ensure_ascii=False,separators=(',',':'))+'\n').encode()); n+=1
    return {'rows':n,'ordered_row_sha256':h.hexdigest()}

def stats(c):
    return {k:c.execute('PRAGMA '+k).fetchone()[0] for k in ('page_count','page_size','freelist_count','auto_vacuum')}

def timed(fn):
    t=time.perf_counter(); result=fn(); return time.perf_counter()-t,result

def guard_root(root):
    root=Path(root).resolve()
    expected=Path('/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-storage-architecture-20261005/runtime').resolve()
    if root!=expected and expected not in root.parents: raise ValueError('only campaign-owned runtime permitted')
    return root
