"""Minimal query-tier adapter: bounded hot SQLite + verified immutable Parquet.

A fresh streaming hot reader per query; no SQLite extension, no ingestion DuckDB.
Point queries use the existing hot target index; historical scans are batched.
One engine handles archival partitions, not application fanout across history.
"""
from __future__ import annotations
import contextlib,json,sqlite3
from pathlib import Path
import duckdb
import pyarrow as pa
from storage import Store,ARROW_SCHEMA,FIELDS,COLS,sha,retention,lock,connect
from schemas import relation


@contextlib.contextmanager
def tier_queries(store):
    # DuckDB Arrow callbacks use a worker thread even with threads=1. A
    # serialized SQLite build supports this connection handoff; execute waits
    # for callbacks, and no other thread is given this connection. The same
    # fenced snapshots/reader lease are established before releasing writer.
    if sqlite3.threadsafety!=3:raise RuntimeError('serialized SQLite required for Arrow reader handoff')
    lease=lock(store.root,'reader.lock',shared=True)
    with lock(store.root):
        writable=connect(store.state)
        try:store.recover(writable)
        finally:writable.close()
        lease.__enter__()
        c=sqlite3.connect(f'file:{store.state}?mode=ro',uri=True,check_same_thread=False,timeout=2);c.row_factory=sqlite3.Row
        c.execute('PRAGMA cache_size=-16384');c.execute('PRAGMA temp_store=FILE')
        files=c.execute("SELECT identity FROM q_segments WHERE status!='RETIRED' ORDER BY identity").fetchall();pieces=[]
        for i,(identity,) in enumerate(files):
            c.execute(f'ATTACH DATABASE ? AS s{i}',(f'file:{store.root/(identity+".sqlite")}?mode=ro',))
            pieces.append(f'SELECT {COLS} FROM s{i}.label_events')
        c.execute('CREATE TEMP VIEW label_events AS '+' UNION ALL '.join(pieces));c.execute('PRAGMA query_only=ON');c.execute('BEGIN');c.execute('SELECT 1 FROM main.meta LIMIT 1').fetchone()
        for i in range(len(files)):c.execute(f'SELECT id FROM s{i}.label_events LIMIT 1').fetchone()
    try:yield c
    finally:c.close();lease.__exit__(None,None,None)


class VerifiedCatalog:
    """Startup SHA admission; immutable fingerprints on request, like current cold catalog.

    Scope: one archive custodian, no conforming mutation after publication.
    Unexpected replacement/content mutation refuses until a fresh admission.
    Scheduled full integrity audits remain separate; stat is not a bit-rot proof.
    """
    @staticmethod
    def fingerprint(path):
        st=Path(path).stat();return (st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)
    def __init__(self,archive):
        self.archive=Path(archive);self.entries=[]
        for path in sorted(self.archive.glob('*.receipt.json')):
            receipt=json.loads(path.read_text());output=self.archive/(receipt['identity']+'.parquet')
            before=self.fingerprint(output)
            if sha(output)!=receipt['parquet_sha256'] or before!=self.fingerprint(output):raise RuntimeError('archive integrity/admission failed')
            self.entries.append((receipt['identity'],output,before,path,self.fingerprint(path)))
    def files(self):
        answer=[]
        for identity,output,stamp,receipt,receipt_stamp in self.entries:
            if self.fingerprint(output)!=stamp or self.fingerprint(receipt)!=receipt_stamp:raise RuntimeError('catalog immutable identity changed; explicit readmission required')
            answer.append((identity,output))
        return answer


class Row(dict):
    def __getitem__(self,key):
        if isinstance(key,int):return list(self.values())[key]
        return super().__getitem__(key)


class Result:
    def __init__(self,rows,names):self.rows=[Row(zip(names,r)) for r in rows];self.offset=0
    def fetchall(self):answer=self.rows[self.offset:];self.offset=len(self.rows);return answer
    def fetchone(self):
        if self.offset>=len(self.rows):return None
        answer=self.rows[self.offset];self.offset+=1;return answer


class TierSession:
    def __init__(self,store,archive,subject=None,include_below_floor=False,catalog=None,batch=False):
        self.store=store;self.archive=Path(archive);self.subject=subject;self.all_history=include_below_floor;self.catalog=catalog;self.batch=batch
    def __enter__(self):
        self.lease=tier_queries(self.store);self.sqlite=self.lease.__enter__();self.duck=duckdb.connect()
        self.duck.execute("SET memory_limit='"+('512MB' if self.batch else '256MB')+"'");self.duck.execute('SET threads=1');self.duck.execute('SET preserve_insertion_order=false')
        spill=self.store.root/'query-scratch';spill.mkdir(exist_ok=True);self.duck.execute('SET temp_directory=?',[str(spill)]);self.duck.execute("SET max_temp_directory_size='"+('2GB' if self.batch else '1GB')+"'")
        records=[tuple(r) for r in self.sqlite.execute("SELECT identity,status FROM q_segments ORDER BY identity")];local=[r for r in records if r[1]!='RETIRED'];self.hot_aliases=['s'+str(i) for i,r in enumerate(local) if r[1] in ('ACTIVE','SEALED')]
        self.floor=None if self.all_history else retention.live_floor(self.sqlite)
        paths=[];identities=set()
        try:
            catalog=self.catalog or VerifiedCatalog(self.archive)
            if catalog.archive.resolve()!=self.archive.resolve():raise RuntimeError('catalog namespace mismatch')
            for identity,output in catalog.files():
                if identity in identities:raise RuntimeError('duplicate catalog identity')
                identities.add(identity);paths.append(output)
            for identity,status in records:
                if status in ('ARCHIVED','RETIRED') and identity not in identities:raise RuntimeError('catalog coverage missing for retired/archived vessel')
            if paths:
                relation(self.duck,paths);self.duck.execute('ALTER VIEW label_events RENAME TO archived_events')
                self.has_archive=True
            else:self.has_archive=False
        except BaseException:self.__exit__(None,None,None);raise
        return self
    def hot_batches(self):
        for alias in self.hot_aliases:
            sql='SELECT '+COLS+' FROM '+alias+'.label_events';where=[];args=[]
            if self.floor:where.append('ts>=?');args.append(self.floor)
            if self.subject:where.append('target_did=?');args.append(self.subject)
            if where:sql+=' WHERE '+' AND '.join(where)
            cursor=self.sqlite.execute(sql,args)
            while rows:=cursor.fetchmany(10000):
                yield pa.RecordBatch.from_pylist([dict(zip(FIELDS,r)) for r in rows],schema=ARROW_SCHEMA)
    def execute(self,sql,args=()):
        if 'label_events' not in sql:return self.sqlite.execute(sql,args)
        if sql.strip()=="SELECT COUNT(*) AS c FROM label_events WHERE target_did = ?":
            n=0
            for alias in self.hot_aliases:
                query='SELECT COUNT(*) FROM '+alias+'.label_events WHERE target_did=?';values=list(args)
                if self.floor:query+=' AND ts>=?';values.append(self.floor)
                n+=self.sqlite.execute(query,values).fetchone()[0]
            if self.has_archive:
                query='SELECT COUNT(*) FROM archived_events WHERE target_did=?';values=list(args)
                if self.floor:query+=' AND ts>=?';values.append(self.floor)
                n+=self.duck.execute(query,values).fetchone()[0]
            return Result([(n,)],['c'])
        reader=pa.RecordBatchReader.from_batches(ARROW_SCHEMA,self.hot_batches());self.duck.register('hot_events',reader)
        parts=['SELECT '+COLS+' FROM hot_events']
        if self.has_archive:
            part='SELECT '+COLS+' FROM archived_events'
            # Validated retention ISO value; not arbitrary query text.
            if self.floor:part+=" WHERE ts>='"+retention._validate_floor(self.floor)+"'"
            parts.append(part)
        self.duck.execute('CREATE OR REPLACE VIEW label_events AS '+' UNION ALL '.join(parts))
        result=self.duck.execute(sql,args);names=[x[0] for x in result.description];return Result(result.fetchall(),names)
    def __exit__(self,*exc):
        self.duck.close();self.lease.__exit__(*exc)
