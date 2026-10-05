"""Executable archive-first whole-file protocol; isolated fixtures only.

This deliberately DOES NOT qualify writer fencing, production cursor transfer,
power-loss behavior, or NFS stable storage. Those remain explicit design gates.
"""
from __future__ import annotations
import argparse
import errno
import json
import os
import sqlite3
import re
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from common import FIELDS,atomic,digest,fsync_dir,guard_root,sha

CUTS=('before_seal','after_seal','after_verify','after_archive_write','after_archive_publish',
      'after_receipt','before_retire','after_retire','after_catalog')

def run(root, cut=None, failure=None, backend='sqlite'):
    root=guard_root(root); source=root/'sealed.sqlite'; archive=root/'archive.parquet'
    receipt=root/'receipt.json'; catalog=root/'catalog.json'
    pg=None
    namespace=re.sub('[^a-z0-9_]','_',root.name.lower())
    if backend=='postgres':
        import psycopg
        pg=psycopg.connect(host=os.environ['SPIKE_PG_SOCKET'],dbname='fixtures',user='postgres',autocommit=True)
    def exists():
        return source.exists() if pg is None else pg.execute('SELECT to_regclass(%s)',(namespace+'.label_events',)).fetchone()[0] is not None
    def source_rows():
        if pg is not None:
            return pg.execute('SELECT '+','.join(FIELDS)+f' FROM {namespace}.label_events ORDER BY id'),None
        c=sqlite3.connect(f'file:{source}?mode=ro&immutable=1',uri=True)
        return c.execute('SELECT '+','.join(FIELDS)+' FROM label_events ORDER BY id'),c
    def phase(name):
        atomic(root/'checkpoint.json',{'phase':name,'source':'sealed.sqlite','archive':'archive.parquet'})
        if name==cut: os._exit(73)
    phase('before_seal')
    if failure=='root_low': raise RuntimeError('local admission refused before archive work')
    if exists() and pg is None:
        c=sqlite3.connect(source); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.close()
    elif exists() and pg is not None:
        attached=pg.execute('SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=to_regclass(%s))',(namespace+'.label_events',)).fetchone()[0]
        if attached:
            with pg.transaction():pg.execute(f'ALTER TABLE {namespace}.events DETACH PARTITION {namespace}.label_events')
    phase('after_seal')
    # Reconstruct authority from source + verified receipt, never progress checkpoint.
    expected=json.loads((root/'accepted.json').read_text())
    if exists():
        if pg is None:
            c=sqlite3.connect(f'file:{source}?mode=ro&immutable=1',uri=True)
            if tuple(r[1] for r in c.execute('PRAGMA table_info(label_events)'))!=FIELDS:
                raise RuntimeError('schema generation refused')
            if c.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]!='23':
                raise RuntimeError('schema generation refused')
            c.close()
        cursor,c=source_rows();actual=digest(cursor)
        if c:c.close()
        if actual!=expected: raise RuntimeError('source content differs from accepted set')
    elif not receipt.exists(): raise RuntimeError('source absent without archive receipt')
    phase('after_verify')
    if failure in ('archive_unavailable','archive_read_only'): raise PermissionError('archive admission refused')
    if not archive.exists():
        if not exists(): raise RuntimeError('missing source and archive')
        rows,c=source_rows()
        tmp=root/'archive.incomplete'; writer=None
        try:
            while batch:=rows.fetchmany(10000):
                table=pa.Table.from_pylist([dict(zip(FIELDS,r)) for r in batch])
                if writer is None: writer=pq.ParquetWriter(tmp,table.schema,compression='zstd')
                writer.write_table(table)
                if failure=='archive_full': raise OSError(errno.ENOSPC,'bounded archive-full fixture')
        finally:
            if writer: writer.close()
            if c:c.close()
        with tmp.open('rb') as f: os.fsync(f.fileno())
        phase('after_archive_write')
        os.replace(tmp,archive); fsync_dir(root)
    phase('after_archive_publish')
    # Whole-file digest alone is insufficient: independently read every value.
    actual=digest(tuple(row[k] for k in FIELDS)
                 for batch in pq.ParquetFile(archive).iter_batches(batch_size=10000)
                 for row in batch.to_pylist())
    if actual!=expected: raise RuntimeError('archive verification failed; source retained')
    archive_sha=sha(archive)
    if receipt.exists() and json.loads(receipt.read_text())['sha256']!=archive_sha:
        raise RuntimeError('published archive differs from durable receipt')
    atomic(receipt,{'schema':'spike.archive.v1','schema_version':23,**actual,'sha256':archive_sha})
    phase('after_receipt'); phase('before_retire')
    if failure=='retire_failed': raise PermissionError('local retirement refused; verified archive retained')
    if exists():
        if pg is None:source.unlink(); fsync_dir(root)
        else:pg.execute(f'DROP TABLE {namespace}.label_events')
    phase('after_retire')
    atomic(catalog,{'state':'ARCHIVED','sha256':archive_sha,'rows':actual['rows'],'schema_version':23})
    phase('after_catalog')
    if pg:pg.close()
    return json.loads(catalog.read_text())

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('--cut');p.add_argument('--failure');p.add_argument('--backend',default='sqlite',choices=['sqlite','postgres'])
    a=p.parse_args(); print(json.dumps(run(a.root,a.cut,a.failure,a.backend)))
