"""Offline constructed-state qualification, never a runtime acceptance adapter.

The CLI presently admits only a tiny differential. Bulk pages bypass acceptance
lookups, not real DDL/indexes, custody publication or physical retirement.
Construction across SQLite files is NOT atomic: a failed unpublished fixture is
refused, not resumed as an accepted store. No ingestion-throughput claim follows.
"""
import argparse
from contextlib import closing
import datetime as dt
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

import storage
from storage import FIELDS, connect, db, custody, ingest, atomic
from recent_storage import RecentStore, clock, iso, micros, day_number
from recent_provider import RecentProvider


def bulk_pages(store, pages, next_id):
    """At most ten distinct, already normalized 100-row source pages/day.

    Only freshly constructed unique identities are supported. Replay/conflict
    and interrupted acceptance belong to the actual writer qualification.
    """
    if not 0 < len(pages) <= 10:raise ValueError('construction batch ceiling')
    store.check_local()
    with closing(connect(store.state)) as state:
        store.require(state)
        owner=db.get_meta(state,'q:active')
        if state.execute('SELECT 1 FROM q_pending').fetchone():raise RuntimeError('construction requires post-flush state')
        with closing(connect(store.root/(owner+'.sqlite'))) as vessel:
            state.execute('BEGIN IMMEDIATE');vessel.execute('BEGIN IMMEDIATE')
            for page in pages:
                rows=page['rows'];stamp=clock(page['observed_at']);source=page['source']
                if not rows or len(rows)>100 or stamp.date().isoformat()!=owner:raise ValueError('source page/day bound')
                store.validate_rows(rows)
                if any(row[1]!=source for row in rows):raise ValueError('HTTP collector page source mismatch')
                if stamp<clock(db.get_meta(state,'q:recent_clock')):raise RuntimeError('construction clock regression')
                current=state.execute('SELECT provider_cursor,attempt_token FROM q_recent_sources WHERE did=?',(source,)).fetchone()
                if current is None or current[0]!=page['request_cursor'] or current[1] is not None:raise RuntimeError('construction continuation mismatch')
                expanded=[(next_id+i,*row) for i,row in enumerate(rows)]
                vessel.executemany('INSERT INTO label_events VALUES('+','.join('?' for _ in FIELDS)+')',expanded)
                state.executemany('INSERT INTO q_recent_seen VALUES(?,?,?,?)',[(r[0],day_number(owner),micros(stamp),r[3][5:].split('/')[0] if r[3].startswith('at://') else r[3]) for r in expanded])
                state.executemany('INSERT INTO q_hot_keys VALUES(?,?,?,?,?)',[(r[9],custody.format_ts(custody.now_utc()),next_id+i,owner,custody.payload_digest(r)) for i,r in enumerate(rows)])
                # Preserve actual page-level observed-src evidence multiplicity;
                # first-occurrence order preserves evidence AUTOINCREMENT IDs.
                for labeler in dict.fromkeys(r[0] for r in rows):db.upsert_labeler(state,labeler,iso(stamp))
                evidence=set()
                for src in dict.fromkeys(r[1] or r[0] for r in rows):ingest._track_observed_src(state,src,iso(stamp),evidence)
                next_id+=len(rows)
                for key,value in {'clock':iso(stamp),'end':iso(stamp+dt.timedelta(microseconds=1)),'events':str(int(db.get_meta(state,'q:recent_events'))+len(rows))}.items():db.set_meta(state,'q:recent_'+key,value)
                db.set_meta(state,'custody:generation',str(int(db.get_meta(state,'custody:generation'))+1))
                body={'request':page['request_cursor'],'next':page['next_cursor'],'hashes':[r[9] for r in rows],'payloads':[custody.payload_digest(r) for r in rows]}
                position=page['next_cursor'] if page['next_cursor'] is not None else page['request_cursor']
                state.execute('UPDATE q_recent_sources SET provider_cursor=?,last_page=?,last_observed=? WHERE did=?',(position,json.dumps(body,sort_keys=True),iso(stamp),source))
            changed=state.execute("UPDATE sqlite_sequence SET seq=? WHERE name='label_events'",(next_id-1,)).rowcount
            if not changed:state.execute("INSERT INTO sqlite_sequence(name,seq) VALUES('label_events',?)",(next_id-1,))
            store.caps(state,scan=False)
            vessel.commit();state.commit()  # Offline construction, not atomic acceptance.
    return next_id


def event(ordinal,source):
    raw={'src':source,'uri':('did:plc:dense' if ordinal%3 else 'at://did:plc:sparse/app.bsky.feed.post/'+str(ordinal)),
         'val':'synthetic-production-shape-café-'+str(ordinal),'cts':'1999-01-01T00:00:00Z'}
    return (source,source,raw['uri'],None,raw['val'],0,None,None,raw['cts'],custody.canonical_identity(raw),None)


def table_image(store):
    with closing(connect(store.state,readonly=True)) as c:
        result={}
        for (table,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            if table in ('custody_archives','q_archive'):continue  # Paths/timing differ; checked by independent content oracle.
            rows=[tuple(r) for r in c.execute('SELECT * FROM "'+table+'" ORDER BY rowid')]
            if table=='meta':rows=[(k,'<fixture>' if k=='custody:lineage' else v) for k,v in rows]
            result[table]=sorted(rows,key=repr)
        return result


def qualify(out):
    out.mkdir();storage.ROOT=out/'runtime';storage.ROOT.mkdir();(out/'evidence').mkdir()
    atomic(out/'evidence/ARCHIVE-DESTINATION.json',{'campaign_archive':str(storage.ROOT/'constructed/archive')})
    atomic(out/'CONSTRUCTION.json',{'state':'UNPUBLISHED','scope':'tiny differential only'})
    start=dt.datetime(2026,1,1,12,tzinfo=dt.timezone.utc)
    actual=RecentStore.create(storage.ROOT/'actual',iso(start));built=RecentStore.create(storage.ROOT/'constructed',iso(start))
    source='did:plc:source';roster=[{'did':source,'endpoint':None}]
    for s in (actual,built):s.remember_sources(roster)
    next_id=1;expected=[];cursors=None;checks=[]
    with patch.object(custody,'now_utc',return_value=start):
        for day in range(3):
            stamp=start+dt.timedelta(days=day)
            for s in (actual,built):s.advance_day(iso(stamp))
            pages=[]
            for page in range(2):
                observed=stamp+dt.timedelta(seconds=page);rows=[event(day*200+page*100+i,source) for i in range(100)]
                nxt='opaque/'+str(day*2+page);item=dict(rows=rows,source=source,request_cursor=cursors,next_cursor=nxt,observed_at=iso(observed));pages.append(item)
                actual.accept_page(**item);cursors=nxt
                expected.extend((day*200+page*100+i+1,row,iso(observed)) for i,row in enumerate(rows))
            next_id=bulk_pages(built,pages,next_id)
            assert table_image(actual)==table_image(built),'complete supporting-state differential'
            with closing(connect(actual.state,readonly=True)) as a,closing(connect(built.state,readonly=True)) as b:
                assert [tuple(r) for r in a.execute("SELECT name,sql FROM sqlite_master ORDER BY name")]==[tuple(r) for r in b.execute("SELECT name,sql FROM sqlite_master ORDER BY name")]
                for c in (a,b):
                    assert c.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
                    assert c.execute('SELECT COUNT(*) FROM labeler_evidence').fetchone()[0]==(day+1)*2
            checks.append({'day':day,'events':next_id-1,'tables':'equal','evidence_rows':(day+1)*2})
        # A real accepted page after construction must allocate the next global
        # ID and preserve the raw provider cursor, not collide with fixture IDs.
        observed+=dt.timedelta(seconds=1)
        follow=event(600,source)
        for s in (actual,built):
            answer=s.accept_page([follow],source=source,request_cursor=cursors,next_cursor='after-bulk',observed_at=iso(observed))
            assert answer['accepted_ids']==[601]
        expected.append((601,follow,iso(observed)))
        assert table_image(actual)==table_image(built)
        # Independent expected public projection from input tuples; no constructor query as oracle.
        for subject in ('did:plc:dense','did:plc:sparse'):
            oracle=[(i,row[2],row[4],row[8],obs) for i,row,obs in expected if row[2]==subject or row[2].startswith('at://'+subject+'/')]
            for s in (actual,built):
                with RecentProvider(s).snapshot(subject,iso(start),iso(observed+dt.timedelta(microseconds=1)),1001,max_bytes=1024**2) as answer:
                    got=[(r['id'],r['uri'],r['val'],r['ts'],r['observed_at']) for r in answer['rows']]
                    assert got==oracle,'public input oracle'
        # Independent per-owner ordered payload digest, including actual cold receipt.
        for s in (actual,built):
            with closing(connect(s.state,readonly=True)) as c:
                for owner,body in c.execute('SELECT identity,receipt_json FROM custody_archives'):
                    rows=[(i,*row) for i,row,obs in expected if obs[:10]==owner]
                    receipt=json.loads(body)
                    # Independent JSON streaming oracle, no storage.digest call.
                    digest=hashlib.sha256()
                    for row in rows:digest.update((json.dumps(list(row),ensure_ascii=False,separators=(',',':'))+'\n').encode())
                    assert receipt['content']=={'rows':len(rows),'ordered_row_sha256':digest.hexdigest()}
                    index=Path(receipt['archive_root'])/receipt['identity_index']['file']
                    with closing(sqlite3.connect(f'file:{index}?mode=ro&immutable=1',uri=True)) as identity:
                        got=list(identity.execute('SELECT event_hash,event_id,payload_sha256 FROM identities ORDER BY event_hash'))
                    oracle=sorted((row[10],row[0],hashlib.sha256(json.dumps(list(row[1:]),ensure_ascii=True,sort_keys=True,separators=(',',':')).encode()).hexdigest()) for row in rows)
                    assert got==oracle,'independent archive identity membership'
            s.advance_day(iso(start+dt.timedelta(days=35)))
            with closing(connect(s.state,readonly=True)) as c:
                assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]==0
                assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0]==0
                assert c.execute('SELECT COUNT(*) FROM custody_archives').fetchone()[0]==0
            assert not list((s.root/'archive').glob('*.parquet'))
        assert table_image(actual)==table_image(built)
    used=sum(p.stat().st_blocks*512 for p in out.rglob('*') if p.is_file());assert used<100*1024**2
    result={'result':'PASS','constructed_events':600,'real_followup_events':1,'source_page_rows':100,'bulk_transaction_rows':200,'checks':checks,'allocated_bytes':used,
            'public_oracle':'Independent input projection dense/sparse exact IDs,uri,value,source time,observation time',
            'custody_oracle':'Input ordered complete FIELDS digest equals real archive receipt','state_oracle':'All non-path global tables and exactDDL/indexes equal; real archive/retire/maintain expires both',
            'scope':'Constructed capacity correspondence only; not acceptance throughput, constructor crash recovery, full-volume capacity or empirical distribution',
            'next':'Independent review before admitting a specimen-driven larger constructed fixture'}
    atomic(out/'RESULT.json',result);atomic(out/'CONSTRUCTION.json',{'state':'QUALIFIED_TINY','events':600});return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--occurrence',type=Path,required=True);a=p.parse_args()
    storage.owned(a.occurrence)
    try:print(json.dumps(qualify(a.occurrence)),flush=True)
    except BaseException as ex:
        if a.occurrence.exists():atomic(a.occurrence/'TERMINAL.json',{'result':'FAIL','error':repr(ex)})
        raise
    else:atomic(a.occurrence/'TERMINAL.json',{'result':'PASS'})
