"""Explicit finite archive-reader generations and shared value vectors."""
from __future__ import annotations
import json
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from storage import ARROW_SCHEMA, FIELDS, atomic, digest

SUPPORTED = (23, 24, 25, 26)


def write(path, rows, generation):
    records=[dict(zip(FIELDS,r)) for r in rows]
    schema=ARROW_SCHEMA
    if generation>=24:
        schema=schema.append(pa.field('note',pa.string(),nullable=True))
        for r in records:r['note']=None
    if generation>=25:
        schema=pa.schema([pa.field('label_value' if f.name=='val' else f.name,f.type,nullable=f.nullable) for f in schema])
        for r in records:r['label_value']=r.pop('val')
    if generation>=26:
        schema=pa.schema([pa.field(f.name,pa.string() if f.name=='neg' else f.type,nullable=f.nullable) for f in schema])
        for r in records:r['neg']=None if r['neg'] is None else ('true' if r['neg']==1 else 'false')
    schema=schema.with_metadata({b'labelwatch.schema_generation':str(generation).encode()})
    pq.write_table(pa.Table.from_pylist(records,schema=schema),path,compression='zstd')


def relation(conn, paths, reader_generation=26):
    parts=[]
    for path in paths:
        schema=pq.ParquetFile(path).schema_arrow
        generation=int((schema.metadata or {}).get(b'labelwatch.schema_generation',b'0'))
        if generation not in SUPPORTED or generation>reader_generation:raise RuntimeError('unsupported archive schema generation')
        select=[]
        for name in FIELDS:
            if name=='val' and generation>=25:select.append('label_value AS val')
            elif name=='neg' and generation>=26:
                select.append("CASE WHEN neg IS NULL THEN NULL WHEN neg='true' THEN 1 WHEN neg='false' THEN 0 ELSE error('invalid neg encoding') END AS neg")
            else:select.append(name)
        # Values are bound through the API, not interpolated path literals.
        view='part_'+str(len(parts));conn.from_parquet(str(path)).create_view(view)
        parts.append('SELECT '+','.join(select)+' FROM '+view)
    conn.execute('CREATE VIEW label_events AS '+' UNION ALL '.join(parts))


def qualify(root):
    root=Path(root);root.mkdir()
    rows=[(1,'did:plc:fixturelabeler',None,'at://did:plc:fixturesubject/post/α',None,'unicode-雪',0,None,None,'2026-09-28T00:00:00.000001Z','hash-1',None),
          (2,'did:plc:fixturelabeler','did:plc:fixturelabeler','did:plc:fixturesubject','cid','negative',1,'expiry','sig','2026-09-28T00:00:00.000002Z','hash-2','did:plc:fixturesubject'),
          (3,'did:plc:fixturelabeler',None,'did:plc:fixturesubject',None,'nullable-neg',None,None,'','2026-09-28T00:00:00.000003Z','hash-3',None)]
    expected=digest(rows);result=[];paths=[]
    for generation in SUPPORTED:
        path=root/f'generation-{generation}.parquet';write(path,rows,generation);paths.append(path)
        c=duckdb.connect();relation(c,[path]);actual=digest(c.execute('SELECT * FROM label_events ORDER BY id').fetchall());c.close();assert actual==expected
        result.append({'case':f'generation_{generation}_new_reader','result':'PASS','content':actual})
    c=duckdb.connect();relation(c,paths);assert c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==12;c.close();result.append({'case':'mixed_generations_union','result':'PASS'})
    for generation in [24,25,26]:
        c=duckdb.connect()
        try:relation(c,[root/f'generation-{generation}.parquet'],reader_generation=23)
        except RuntimeError:pass
        else:raise AssertionError('old reader silently accepted newer archive')
        c.close();result.append({'case':f'old_reader_refuses_{generation}','result':'PASS_REFUSAL'})
    # A new SQLite index changes access paths, not archive row semantics.
    import sqlite3
    from storage import vessel, connect, COLS
    for generation in SUPPORTED:
        path=root/f'generation-{generation}.sqlite';vessel(path,f'generation-{generation}');q=connect(path)
        q.executemany('INSERT INTO label_events('+COLS+') VALUES('+','.join('?' for _ in FIELDS)+')',rows)
        if generation>=24:q.execute('ALTER TABLE label_events ADD COLUMN note TEXT');q.execute("UPDATE label_events SET note='retained auxiliary value' WHERE id=2")
        if generation>=25:q.execute('ALTER TABLE label_events RENAME COLUMN val TO label_value')
        if generation>=26:q.execute("UPDATE label_events SET neg=CASE WHEN neg IS NULL THEN NULL WHEN neg=1 THEN 'true' ELSE 'false' END")
        q.execute('CREATE INDEX qualification_additive_index ON label_events(ts,id)');q.commit()
        physical=q.execute('SELECT * FROM label_events ORDER BY id').fetchall();names=[x[1] for x in q.execute('PRAGMA table_info(label_events)')]
        schema=pq.ParquetFile(root/f'generation-{generation}.parquet').schema_arrow
        dest=root/f'from-sqlite-{generation}.parquet';pq.write_table(pa.Table.from_pylist([dict(zip(names,r)) for r in physical],schema=schema),dest)
        assert digest(physical)==digest(tuple(r[n] for n in names) for r in pq.read_table(dest).to_pylist())
        q.close();d=duckdb.connect();relation(d,[dest]);assert digest(d.execute('SELECT * FROM label_events ORDER BY id').fetchall())==expected;d.close()
        result.append({'case':f'actual_sqlite_schema_{generation}_whole_column_custody','result':'PASS','auxiliary_columns_preserved':generation>=24})
    bad=root/'invalid-encoding.parquet';table=pq.read_table(paths[-1]);ix=table.schema.get_field_index('neg');table=table.set_column(ix,'neg',pa.array(['unknown','false',None]));pq.write_table(table,bad)
    c=duckdb.connect();relation(c,[bad])
    try:c.execute('SELECT * FROM label_events').fetchall()
    except duckdb.Error:pass
    else:raise AssertionError('invalid encoding silently coerced')
    c.close();result.append({'case':'invalid_encoding','result':'PASS_REFUSAL'})
    for unsupported in [22,27]:
        table=pq.read_table(paths[0]).replace_schema_metadata({b'labelwatch.schema_generation':str(unsupported).encode()});path=root/f'unsupported-{unsupported}.parquet';pq.write_table(table,path);c=duckdb.connect()
        try:relation(c,[path])
        except RuntimeError:pass
        else:raise AssertionError('unsupported generation accepted')
        c.close();result.append({'case':f'outside_compatibility_window_{unsupported}','result':'PASS_REFUSAL'})
    atomic(root/'RESULT.json',{'result':'PASS','cases':result,'compatibility_window':SUPPORTED,'policy':'No immutable rewrite. Retire a reader only after a successor verifies retained vectors/custody and no named consumer requires it. Unsupported generations refuse before answering.','vectors':rows,'scope':'Versioned complete canonical event columns; does not grant arbitrary Labelwatch application schema migrations.'})


if __name__=='__main__':
    import sys
    qualify(Path(sys.argv[1]))
