"""Minimal query-tier adapter: bounded hot SQLite + verified immutable Parquet.

A fresh streaming hot reader per query; no SQLite extension, no ingestion DuckDB.
Point queries use the existing hot target index; historical scans are batched.
One engine handles archival partitions, not application fanout across history.
"""
from __future__ import annotations
import json
from pathlib import Path
import duckdb
import pyarrow as pa
from storage import Store,ARROW_SCHEMA,FIELDS,COLS,sha,retention
from schemas import relation


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
    def __init__(self,store,archive,subject=None,include_below_floor=False):
        self.store=store;self.archive=Path(archive);self.subject=subject;self.all_history=include_below_floor
    def __enter__(self):
        self.lease=self.store.queries();self.sqlite=self.lease.__enter__();self.duck=duckdb.connect()
        self.duck.execute("SET memory_limit='256MB'");self.duck.execute('SET threads=1')
        spill=self.store.root/'query-scratch';spill.mkdir(exist_ok=True);self.duck.execute('SET temp_directory=?',[str(spill)]);self.duck.execute("SET max_temp_directory_size='1GB'")
        records=[tuple(r) for r in self.sqlite.execute("SELECT identity,status FROM q_segments ORDER BY identity")];local=[r for r in records if r[1]!='RETIRED'];self.hot_aliases=['s'+str(i) for i,r in enumerate(local) if r[1] in ('ACTIVE','SEALED')]
        self.floor=None if self.all_history else retention.live_floor(self.sqlite)
        paths=[];identities=set()
        try:
            for receipt_path in sorted(self.archive.glob('*.receipt.json')):
                receipt=json.loads(receipt_path.read_text());identity=receipt['identity'];output=self.archive/(identity+'.parquet')
                if identity in identities:raise RuntimeError('duplicate catalog identity')
                if sha(output)!=receipt['parquet_sha256']:raise RuntimeError('archive integrity failed; historical query unavailable')
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
