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
from storage import Store,ARROW_SCHEMA,FIELDS,COLS,sha,retention,lock,connect,db,custody
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
    """Derived indexed reader admission; custody remains the authority.

    Admit one committed candidate with one digest check. Existing payloads are
    never scanned by admission or startup. Full integrity audit is explicit.
    Immutable fingerprint checks on selected query files retain fail-closed
    replacement/mutation behavior; fingerprints are not a bit-rot proof.
    """
    @staticmethod
    def fingerprint(path):
        st=Path(path).stat();return (st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)

    def __init__(self,archive,store=None):
        self.archive=Path(archive)
        # Actual reader/writer stores keep mutable metadata on local storage,
        # independent of archive NFS. The default is for local fixtures only.
        root=store.root if store is not None else self.archive
        self.path=root/('reader-catalog-'+__import__('hashlib').sha256(str(self.archive.resolve()).encode()).hexdigest()[:16]+'.sqlite')
        with self.connection() as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS entries(
                identity TEXT PRIMARY KEY, receipt_path TEXT NOT NULL,
                receipt_json TEXT NOT NULL, parquet_sha256 TEXT NOT NULL,
                output_stamp TEXT NOT NULL, receipt_stamp TEXT NOT NULL,
                min_ts TEXT, max_ts TEXT);
            CREATE INDEX IF NOT EXISTS entries_time ON entries(min_ts,max_ts,identity);
            ''')

    @contextlib.contextmanager
    def connection(self):
        c=sqlite3.connect(self.path,timeout=30)
        c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA synchronous=FULL')
        c.execute('PRAGMA cache_size=-2048')
        try:
            yield c
            c.commit()
        except BaseException:
            c.rollback();raise
        finally:c.close()

    def admit(self,authority,identity,death=None,require_published=True):
        """PK lookup of committed receipt, then bounded candidate-only work."""
        if authority.in_transaction:
            raise RuntimeError('reader admission requires committed custody outside staging')
        row=authority.execute('SELECT receipt,receipt_json FROM custody_archives WHERE identity=?',(identity,)).fetchone()
        if row is None:raise RuntimeError('reader admission requires committed custody owner')
        path=Path(row[0]);receipt=json.loads(row[1]);output=self.archive/(identity+'.parquet')
        if receipt['identity']!=identity or Path(receipt['archive_root']).resolve()!=self.archive.resolve() or path.resolve()!=(self.archive/(identity+'.receipt.json')).resolve():
            raise RuntimeError('catalog candidate namespace/identity mismatch')
        receipt_stamp=self.fingerprint(path) if path.exists() else None
        if receipt_stamp is None and require_published:
            raise RuntimeError('committed receipt publication incomplete/conflicting')
        if receipt_stamp is not None and (json.loads(path.read_text())!=receipt or receipt_stamp!=self.fingerprint(path)):
            raise RuntimeError('committed receipt publication incomplete/conflicting')
        stamp=self.fingerprint(output)
        with self.connection() as c:
            old=c.execute('SELECT receipt_json,parquet_sha256,output_stamp,receipt_stamp FROM entries WHERE identity=?',(identity,)).fetchone()
            if old:
                if old[:2]!=(row[1],receipt['parquet_sha256']):
                    raise RuntimeError('catalog identity conflicts with committed custody')
                if tuple(json.loads(old[2]))==stamp:
                    if json.loads(old[3])!=(list(receipt_stamp) if receipt_stamp else None):
                        c.execute('UPDATE entries SET receipt_stamp=? WHERE identity=?',(json.dumps(receipt_stamp),identity))
                    return {'identity':identity,'payload_hash_calls':0,'payload_bytes_read':0,'duplicate':True}
            if sha(output)!=receipt['parquet_sha256'] or stamp!=self.fingerprint(output):
                raise RuntimeError('archive integrity/admission failed')
            import pyarrow.parquet as pq
            pf=pq.ParquetFile(output);schema=pf.schema_arrow
            if (schema.metadata or {}).get(b'labelwatch.segment_identity')!=identity.encode():
                raise RuntimeError('archive identity metadata conflicts with custody')
            if pf.metadata.num_rows!=receipt['content']['rows']:
                raise RuntimeError('archive row count conflicts with custody')
            column=schema.get_field_index('ts');lo=[];hi=[]
            if column<0:raise RuntimeError('archive time column unavailable')
            for i in range(pf.metadata.num_row_groups):
                stats=pf.metadata.row_group(i).column(column).statistics
                if stats and stats.has_min_max:lo.append(stats.min);hi.append(stats.max)
                elif pf.metadata.row_group(i).num_rows:raise RuntimeError('archive range metadata unavailable')
            if stamp!=self.fingerprint(output) or receipt_stamp!=(self.fingerprint(path) if path.exists() else None):
                raise RuntimeError('catalog candidate changed during admission')
            c.execute('BEGIN IMMEDIATE')
            c.execute('INSERT OR REPLACE INTO entries VALUES(?,?,?,?,?,?,?,?)',
                      (identity,str(path),row[1],receipt['parquet_sha256'],json.dumps(stamp),json.dumps(receipt_stamp),min(lo) if lo else None,max(hi) if hi else None))
            if death:death('before_catalog_commit')
        if death:death('after_catalog_commit')
        return {'identity':identity,'payload_hash_calls':1,'payload_bytes_read':stamp[2],'duplicate':False}

    @property
    def entries(self):
        with self.connection() as c:rows=c.execute('SELECT identity,receipt_path,output_stamp,receipt_stamp FROM entries ORDER BY identity').fetchall()
        return [(identity,self.archive/(identity+'.parquet'),tuple(json.loads(stamp)),Path(path),tuple(json.loads(receipt_stamp)) if json.loads(receipt_stamp) else None) for identity,path,stamp,receipt_stamp in rows]

    def lookup(self,identity):
        with self.connection() as c:return c.execute('SELECT identity,receipt_path,min_ts,max_ts FROM entries WHERE identity=?',(identity,)).fetchone()

    def validate_binding(self,identity,receipt):
        with self.connection() as c:row=c.execute('SELECT receipt_json,parquet_sha256 FROM entries WHERE identity=?',(identity,)).fetchone()
        if row is None or json.loads(row[0])!=receipt or row[1]!=receipt['parquet_sha256']:
            raise RuntimeError('catalog derived binding conflicts with committed custody')

    def files(self,start=None,end=None):
        with self.connection() as c:
            where=[];args=[]
            if start is not None:where.append('max_ts>=?');args.append(start)
            if end is not None:where.append('min_ts<?');args.append(end)
            rows=c.execute('SELECT identity,receipt_path,output_stamp,receipt_stamp,receipt_json FROM entries'+(' WHERE '+' AND '.join(where) if where else '')+' ORDER BY identity',args).fetchall()
        answer=[]
        for identity,path,stamp,receipt_stamp,expected in rows:
            output=self.archive/(identity+'.parquet')
            if self.fingerprint(output)!=tuple(json.loads(stamp)):
                raise RuntimeError('catalog immutable identity changed; explicit readmission required')
            observed=self.fingerprint(path);prior=json.loads(receipt_stamp)
            if (prior is None or observed!=tuple(prior)) and (json.loads(Path(path).read_text())!=json.loads(expected) or observed!=self.fingerprint(path)):
                raise RuntimeError('committed receipt publication incomplete/conflicting')
            answer.append((identity,output))
        return answer

    def full_audit(self):
        count=total=0
        with self.connection() as c:rows=c.execute('SELECT identity,parquet_sha256 FROM entries ORDER BY identity').fetchall()
        for identity,expected in rows:
            path=self.archive/(identity+'.parquet');stamp=self.fingerprint(path)
            if sha(path)!=expected or stamp!=self.fingerprint(path):raise RuntimeError('full historical integrity audit failed')
            total+=stamp[2];count+=1
        return {'objects':count,'payload_bytes_read':total,'scope':'Explicit full historical integrity audit; never implicit admission'}


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
            catalog=self.catalog or VerifiedCatalog(self.archive,store=self.store)
            if catalog.archive.resolve()!=self.archive.resolve():raise RuntimeError('catalog namespace mismatch')
            committed = {owner: (path, receipt) for owner,path,receipt in custody.committed(self.sqlite)}
            for identity,status in records:
                if identity in committed and status in ('ACTIVE','SEALED'):
                    raise RuntimeError('custody/layout ownership inconsistent; query refused')
            for identity,output in catalog.files():
                if identity not in committed: continue  # Orphan receipt is never authority.
                path, expected_receipt = committed[identity]
                catalog.validate_binding(identity,expected_receipt)
                if not path.exists() or json.loads(path.read_text()) != expected_receipt:
                    raise RuntimeError('committed receipt publication incomplete/conflicting')
                if identity in identities:raise RuntimeError('duplicate catalog identity')
                identities.add(identity);paths.append(output)
            if set(committed) != identities: raise RuntimeError('committed archive coverage unavailable')
            for identity,status in records:
                if status in ('ARCHIVED','RETIRED') and identity not in identities:raise RuntimeError('catalog coverage missing for retired/archived vessel')
            # Local retry-ring pruning never removes expected-history authority.
            # Its compact global anchor names an immutable archive-side ledger.
            coverage_path=db.get_meta(self.sqlite,'q:archive_coverage_path')
            coverage_hash=db.get_meta(self.sqlite,'q:archive_coverage_sha256')
            if bool(coverage_path)!=bool(coverage_hash):raise RuntimeError('expected archive coverage anchor incomplete')
            if coverage_path:
                coverage=Path(coverage_path)
                if coverage.parent.resolve()!=self.archive.resolve() or sha(coverage)!=coverage_hash:raise RuntimeError('expected archive coverage unavailable/corrupted')
                expected=json.loads(coverage.read_text())
                if expected.get('schema')!='labelwatch.archive-coverage.v1':raise RuntimeError('unknown expected coverage schema')
                receipts={identity:path for identity,output,stamp,path,receipt_stamp in catalog.entries}
                for identity,entry in expected['entries'].items():
                    if identity not in identities:raise RuntimeError('catalog coverage missing for pruned retired vessel')
                    path=receipts[identity]
                    if sha(path)!=entry['receipt_sha256'] or json.loads(path.read_text())['parquet_sha256']!=entry['parquet_sha256']:raise RuntimeError('expected archive receipt identity changed')
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
