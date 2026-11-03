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
                progressing=bool(rows) and page['next_cursor'] is not None and page['next_cursor']!=page['request_cursor']
                due=iso(stamp if progressing else stamp+dt.timedelta(seconds=1))
                state.execute('UPDATE q_recent_sources SET provider_cursor=?,last_page=?,last_observed=?,hot=?,next_due=?,failures=0 WHERE did=?',(position,json.dumps(body,sort_keys=True),iso(stamp),1 if progressing else 2,due,source))
            changed=state.execute("UPDATE sqlite_sequence SET seq=? WHERE name='label_events'",(next_id-1,)).rowcount
            if not changed:state.execute("INSERT INTO sqlite_sequence(name,seq) VALUES('label_events',?)",(next_id-1,))
            store.caps(state,scan=False)
            vessel.commit();state.commit()  # Offline construction, not atomic acceptance.
    return next_id


def event(ordinal,source):
    raw={'src':source,'uri':('did:plc:dense' if ordinal%3 else 'at://did:plc:sparse/app.bsky.feed.post/'+str(ordinal)),
         'val':'synthetic-production-shape-café-'+str(ordinal),'cts':'1999-01-01T00:00:00Z'}
    normalized=ingest.normalize_label(raw,strict_identity=True)
    return tuple(getattr(normalized,k) for k in FIELDS[1:-1])+(db.parse_target_did(raw['uri']),)


def table_image(store):
    with closing(connect(store.state,readonly=True)) as c:
        result={}
        for (table,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            if table in ('custody_archives','q_archive'):continue  # Paths/timing differ; checked by independent content oracle.
            rows=[tuple(r) for r in c.execute('SELECT * FROM "'+table+'" ORDER BY rowid')]
            if table=='meta':
                acquisition=store.acquisition(c)  # Validate before normalizing only independent generation identity.
                rows=[(k,'<fixture>' if k=='custody:lineage' else
                       json.dumps({**acquisition,'generation_id':'<independent-generation>'},sort_keys=True)
                       if k=='q:recent_acquisition' else v) for k,v in rows]
            result[table]=sorted(rows,key=repr)
        return result


def qualify(out):
    out.mkdir();storage.ROOT=out/'runtime';storage.ROOT.mkdir();(out/'evidence').mkdir()
    atomic(out/'evidence/ARCHIVE-DESTINATION.json',{'campaign_archive':str(storage.ROOT/'constructed/archive')})
    atomic(out/'CONSTRUCTION.json',{'state':'UNPUBLISHED','scope':'tiny differential only'})
    start=dt.datetime(2026,1,1,12,tzinfo=dt.timezone.utc)
    actual=RecentStore.create(storage.ROOT/'actual',iso(start));built=RecentStore.create(storage.ROOT/'constructed',iso(start))
    identities=[s.frontier()['acquisition'] for s in (actual,built)]
    assert identities[0]['generation_id']!=identities[1]['generation_id'],'independent acquisition generations'
    assert [{k:v for k,v in item.items() if k!='generation_id'} for item in identities][0]=={k:v for k,v in identities[1].items() if k!='generation_id'},'equal acquisition schema/start/basis'
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
    result={'result':'PASS','constructed_events':600,'real_followup_events':1,'source_page_rows':100,'bulk_transaction_rows':200,'checks':checks,'allocated_bytes':used,'acquisition_identities':identities,
            'public_oracle':'Independent input projection dense/sparse exact IDs,uri,value,source time,observation time',
            'custody_oracle':'Input ordered complete FIELDS digest equals real archive receipt','state_oracle':'All non-path global tables and exactDDL/indexes equal after validated per-store acquisition UUID normalization only; real archive/retire/maintain expires both',
            'scope':'Constructed capacity correspondence only; not acceptance throughput, constructor crash recovery, full-volume capacity or empirical distribution',
            'next':'Independent review before admitting a specimen-driven larger constructed fixture'}
    atomic(out/'RESULT.json',result);atomic(out/'CONSTRUCTION.json',{'state':'QUALIFIED_TINY','events':600});return result


def collector_shape(raw,cycle,cycle_days):
    """Reconstruct an explicitly synthetic raw label accepted by strict ingest.

    Retained v23 drops ver, so ver is absent. Authored ranges of repeated cycles
    are disjoint; signatures are copied shapes without authenticity claims.
    """
    source=raw['src'] or raw['labeler_did']
    label={k:raw.get(k) for k in ('uri','cid','val','exp','sig')}
    label.update(src=source,labeler_did=source,neg=bool(raw.get('neg')),
                 cts=iso(clock(raw['ts'])+dt.timedelta(days=cycle*cycle_days)))
    normalized=ingest.normalize_label(label,strict_identity=True)
    row=tuple(getattr(normalized,k) for k in FIELDS[1:-1])+(db.parse_target_did(normalized.uri),)
    return source,row,label


def construct_specimen(args):
    """Explicitly admitted offline volume construction; never auto-launched.

    Root supplies a separate occurrence ceiling after the actual-ingest producer
    terminates. Per-source buffering packs valid source-homogeneous pages <=100.
    Repeated input cycles are declared synthetic identity namespaces.
    """
    import time
    import collections
    import threading
    import pyarrow.parquet as pq
    from labelwatch.recent_observations import RecentObservations, Refused
    if not 0<args.events<=100000000 or args.ceiling_bytes is None:raise ValueError('explicit count/occurrence budget required')
    out=args.occurrence;out.mkdir();scratch=out/'scratch';scratch.mkdir()
    destination_dev=out.stat().st_dev
    if scratch.is_symlink() or scratch.stat().st_dev!=destination_dev or not os.access(scratch,os.W_OK):raise RuntimeError('scratch destination unavailable; no fallback')
    os.environ['SQLITE_TMPDIR']=str(scratch);os.environ['TMPDIR']=str(scratch)
    import tempfile
    tempfile.tempdir=None
    if Path(tempfile.gettempdir()).resolve()!=scratch.resolve():raise RuntimeError('Python scratch fallback refused')
    if sum(p.stat().st_blocks*512 for p in out.rglob('*') if p.is_file())+4*1024**2>args.ceiling_bytes:raise RuntimeError('scratch probe occurrence envelope exceeded')
    for filesystem in ('/','/data'):
        fs=os.statvfs(filesystem);charge=4*1024**2 if Path(filesystem).stat().st_dev==destination_dev else 0
        if fs.f_bavail*fs.f_frsize-charge<64424509440:raise RuntimeError('scratch probe admission refused')
    with closing(sqlite3.connect(':memory:')) as probe:
        # SQLite may cache its process-wide temp directory during dependency imports.
        # Pin explicitly before any qualification connection/thread; unsupported builds refuse.
        probe.execute("PRAGMA temp_store_directory='"+str(scratch).replace("'","''")+"'")
        configured=probe.execute('PRAGMA temp_store_directory').fetchone()
        if not configured or Path(configured[0]).resolve()!=scratch.resolve():raise RuntimeError('SQLite scratch configuration unavailable')
        probe.execute('PRAGMA temp_store=FILE');probe.execute('PRAGMA temp.cache_size=-64')
        probe.execute('CREATE TEMP TABLE placement(value BLOB)')
        probe.executemany('INSERT INTO placement VALUES(zeroblob(4096))',[()]*512)
        actual=[]
        for fd in Path('/proc/self/fd').iterdir():
            try:link=os.readlink(fd);info=fd.stat()
            except (OSError,FileNotFoundError):continue
            if 'etilqs_' in link:
                if not link.startswith(str(scratch)+'/') or info.st_dev!=destination_dev:raise RuntimeError('SQLite scratch fallback refused')
                actual.append({'path':link,'device':info.st_dev,'allocated_bytes':info.st_blocks*512})
        if not actual:raise RuntimeError('SQLite scratch placement not established')
    atomic(out/'TEMP-PLACEMENT.json',{'destination':str(out),'device':destination_dev,'scratch':str(scratch),'actual_sqlite_temp':actual,'scope':'Forced2MiB FILE temp table; same-filesystem open-unlinked evidence, no fallback'})
    storage.ROOT=out/'runtime';storage.ROOT.mkdir();(out/'evidence').mkdir()
    atomic(out/'evidence/ARCHIVE-DESTINATION.json',{'campaign_archive':str(storage.ROOT/'constructed/archive')})
    receipt=json.loads(args.specimen.with_name('SPECIMEN.json').read_text())
    if storage.sha(args.specimen)!=receipt['parquet_sha256']:raise RuntimeError('specimen identity mismatch')
    atomic(out/'CONSTRUCTION.json',{'state':'UNPUBLISHED','input_sha256':receipt['parquet_sha256'],'target_events':args.events})
    start=dt.datetime(2026,1,1,12,tzinfo=dt.timezone.utc);end=start+dt.timedelta(days=30)
    store=RecentStore.create(storage.ROOT/'constructed',iso(start));began=time.monotonic();peak=0
    def admission(extra=0):
        nonlocal peak
        used=sum(p.stat().st_blocks*512 for p in out.rglob('*') if p.is_file());peak=max(peak,used)
        if used+extra>args.ceiling_bytes:raise RuntimeError('constructed occurrence envelope exceeded')
        free={p:os.statvfs(p).f_bavail*os.statvfs(p).f_frsize for p in ('/','/data')}
        charges={p:extra if Path(p).stat().st_dev==destination_dev else 0 for p in free}
        if any(n-charges[p]<64424509440 for p,n in free.items()):raise RuntimeError('shared reserve admission refused')
        destination_free=os.statvfs(out).f_bavail*os.statvfs(out).f_frsize
        if destination_free-extra<64424509440:raise RuntimeError('destination reserve admission refused')
        return {'allocated_bytes':used,'sampled_peak_bytes':peak,'free_bytes':free,'admitted_extra_bytes_by_filesystem':charges,'destination_device':destination_dev}
    def sample():
        families=collections.Counter();seen=set()
        for path in out.rglob('*'):
            try:info=path.stat()
            except FileNotFoundError:continue
            if not path.is_file():continue
            seen.add((info.st_dev,info.st_ino))
            family='archives' if path.parent==store.root/'archive' else 'global' if path.name.startswith('state.sqlite') else 'catalog' if path.name.startswith('reader-catalog') else 'vessels' if path.suffix=='.sqlite' else 'other'
            families[family]+=info.st_blocks*512
        for fd in Path('/proc/self/fd').iterdir():
            try:
                link=os.readlink(fd);info=fd.stat()
            except (FileNotFoundError,OSError):continue
            if info.st_nlink==0 and (info.st_dev,info.st_ino) not in seen and str(out) in link:
                families['open_unlinked']+=info.st_blocks*512;seen.add((info.st_dev,info.st_ino))
        return dict(families)
    # Method timings retain the actual implementation, only instrument calls.
    for name in ('archive','retire','maintain'):
        original=getattr(store,name)
        def measured(*values,_name=name,_original=original,**kwargs):
            t=time.monotonic();before=sample();maximum=sum(before.values());stop=threading.Event()
            def observe():
                nonlocal maximum
                while not stop.wait(.2):maximum=max(maximum,sum(sample().values()))
            thread=threading.Thread(target=observe,daemon=True);thread.start()
            try:return _original(*values,**kwargs)
            finally:
                stop.set();thread.join();after=sample();maximum=max(maximum,sum(after.values()))
                record={'method':_name,'seconds':time.monotonic()-t,'before':before,'after':after,'sampled_peak_bytes':maximum,'sample_period_seconds':.2,'scope':'includes visible allocated files and own open-unlinked scratch; sampled lower bound'}
                with (out/'TRANSITIONS.jsonl').open('a') as f:f.write(json.dumps(record)+'\n')
        setattr(store,name,measured)
    sources=set();source_min=None;source_max=None;normalized_rows=0;old_hash_differences=0
    parquet=pq.ParquetFile(args.specimen)
    if not 0<parquet.metadata.num_rows<=12000000:raise RuntimeError('qualified input row ceiling')
    # A qualification-only uniqueness oracle, not part of the successor state.
    # Base identities are unique; nonoverlapping authored ranges prove distinct
    # repeated cycles without manufacturing hashes independently of raw labels.
    with closing(sqlite3.connect(out/'input-identities.sqlite')) as oracle:
        oracle.execute('PRAGMA journal_mode=DELETE');oracle.execute('PRAGMA cache_size=-16384')
        oracle.execute('CREATE TABLE identities(hash BLOB PRIMARY KEY) WITHOUT ROWID')
        for batch in parquet.iter_batches(batch_size=1000):
            identities=[]
            for raw in batch.to_pylist():
                source,row,label=collector_shape(raw,0,0);sources.add(source)
                stamp=clock(row[8]);source_min=stamp if source_min is None else min(source_min,stamp);source_max=stamp if source_max is None else max(source_max,stamp)
                identities.append((bytes.fromhex(row[9]),));normalized_rows+=1
                old_hash_differences+=row[9]!=raw['event_hash']
            if len(sources)>2048:raise RuntimeError('source roster ceiling')
            admission(1024**2);oracle.executemany('INSERT INTO identities VALUES(?)',identities);oracle.commit()
            if normalized_rows%100000==0:atomic(out/'PROGRESS.json',{'phase':'STRICT_SOURCE_IDENTITY_ORACLE','rows':normalized_rows,'target':parquet.metadata.num_rows,'elapsed_seconds':time.monotonic()-began,**admission()})
        assert oracle.execute('SELECT COUNT(*) FROM identities').fetchone()[0]==parquet.metadata.num_rows
    cycle_days=(source_max-source_min).days+2
    atomic(out/'INPUT-NORMALIZATION.json',{'source_rows':normalized_rows,'old_hash_differences':old_hash_differences,'strict_unique_identities':normalized_rows,'authored_min':iso(source_min),'authored_max':iso(source_max),'cycle_shift_days':cycle_days,'contract':'Actual normalize_label(strict_identity=True); absent ver reconstructed explicitly; target_did from actual collector parser. Distinct authored ranges prevent cross-cycle identity equality. No signature authenticity claim.'})
    for n in range(max(0,584-len(sources))):sources.add('did:plc:unobserved-fixture-'+str(n))
    store.remember_sources([{'did':x,'endpoint':None} for x in sorted(sources)])
    subjects=list(dict.fromkeys(receipt.get('query_parameters',{}).get(k) for k in ('dense','sparse')))
    subjects=[x for x in subjects if x];public={x:{'count':0,'hash':hashlib.sha256()} for x in subjects}
    owners={};lookup=hashlib.sha256();buffers=collections.defaultdict(list);buffer_bytes=0
    pending=[];cursors={};offered=0;next_id=1;last_progress=0;active=start.date().isoformat();page_count=0
    def encode(value):return (json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n').encode()
    def flush():
        nonlocal pending,next_id,last_progress
        if not pending:return
        upcoming=sum(len(json.dumps(row).encode()) for page in pending for row in page['rows'])
        admission(max(8*1024**2,upcoming*8))
        next_id=bulk_pages(store,pending,next_id);pending=[]
        if (next_id-1)//100000>last_progress//100000:
            last_progress=next_id-1
            atomic(out/'PROGRESS.json',{'phase':'BULK_CONSTRUCTION','events':last_progress,'target':args.events,'owners':len(owners),'elapsed_seconds':time.monotonic()-began,**admission()})
    def page(rows,source):
        nonlocal offered,active,page_count
        observed=start+dt.timedelta(microseconds=offered*2592000000000//args.events)
        owner=observed.date().isoformat()
        if owner!=active:
            flush()
            local=list(store.root.glob('*.sqlite'));admission(2*sum(p.stat().st_size for p in local)+8*1024**2)
            store.advance_day(iso(observed));active=owner
            atomic(out/'PROGRESS.json',{'phase':'DAILY_TRANSITION','constructed_events':offered,'owner':owner,'elapsed_seconds':time.monotonic()-began,**admission()})
        request=cursors.get(source);following='synthetic-page/'+str(page_count+1)
        pending.append(dict(rows=rows,source=source,request_cursor=request,next_cursor=following,observed_at=iso(observed)))
        cursors[source]=following;page_count+=1
        oracle=owners.setdefault(owner,{'count':0,'payload_hash':hashlib.sha256(),'identity_sum':0})
        for row in rows:
            event_id=offered+1;expanded=(event_id,*row);oracle['count']+=1;oracle['payload_hash'].update(encode(list(expanded)))
            payload=hashlib.sha256(json.dumps(list(row),sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()).hexdigest()
            oracle['identity_sum']=(oracle['identity_sum']+int.from_bytes(hashlib.sha256(encode([row[9],event_id,payload])).digest(),'big'))%(1<<256)
            subject=row[2][5:].partition('/')[0] if row[2].startswith('at://') else row[2]
            lookup.update(encode([event_id,day_number(owner),micros(observed),subject]))
            if subject in public:
                public[subject]['count']+=1;public[subject]['hash'].update(encode([row[2],row[4],row[8],iso(observed)]))
            offered+=1
        with (out/'PAGES.jsonl').open('a') as f:f.write(json.dumps({'source':source,'clock':iso(observed),'rows':len(rows),'end_id':offered})+'\n')
        if len(pending)==10:flush()
    generated=0;cycle=0
    while generated<args.events:
        for batch in pq.ParquetFile(args.specimen).iter_batches(batch_size=1000):
            for raw in batch.to_pylist():
                if generated>=args.events:break
                source,row,label=collector_shape(raw,cycle,cycle_days);size=len(json.dumps(row).encode())
                buffers[source].append((row,size));buffer_bytes+=size;generated+=1
                if buffer_bytes>64*1024**2:raise RuntimeError('source-page buffering envelope exceeded')
                if len(buffers[source])==100:
                    part=buffers.pop(source);buffer_bytes-=sum(x[1] for x in part);page([x[0] for x in part],source)
            if generated>=args.events:break
        cycle+=1
        if not pq.ParquetFile(args.specimen).metadata.num_rows:raise RuntimeError('empty specimen')
    for source in sorted(buffers):page([x[0] for x in buffers[source]],source)
    flush();assert offered==args.events and next_id==args.events+1
    # Complete logical lookup hash; no second materialized reference database.
    with closing(connect(store.state,readonly=True)) as c:
        actual_lookup=hashlib.sha256()
        for index,row in enumerate(c.execute('SELECT event_id,owner_day,observed_us,target_did FROM q_recent_seen ORDER BY event_id'),1):
            actual_lookup.update(encode(list(row)))
            if index%100000==0:atomic(out/'PROGRESS.json',{'phase':'LOOKUP_ORACLE','rows':index,'target':offered,'elapsed_seconds':time.monotonic()-began})
        assert actual_lookup.hexdigest()==lookup.hexdigest()
        assert c.execute('SELECT COUNT(*) FROM labeler_evidence').fetchone()[0]==page_count
        store.caps(c)
        for table in __import__('recent_storage').DERIVED:assert c.execute('SELECT COUNT(*) FROM "'+table+'"').fetchone()[0]==0
        physical={'pages':c.execute('PRAGMA page_count').fetchone()[0],'freelist':c.execute('PRAGMA freelist_count').fetchone()[0],
                  'objects':dict(c.execute('SELECT name,sum(pgsize) FROM dbstat GROUP BY name'))}
        for owner,body in c.execute('SELECT identity,receipt_json FROM custody_archives'):
            value=json.loads(body);oracle=owners[owner]
            assert value['content']=={'rows':oracle['count'],'ordered_row_sha256':oracle['payload_hash'].hexdigest()}
            index=Path(value['archive_root'])/value['identity_index']['file'];count=0;total=0
            with closing(sqlite3.connect(f'file:{index}?mode=ro&immutable=1',uri=True)) as q:
                for row in q.execute('SELECT event_hash,event_id,payload_sha256 FROM identities'):
                    count+=1;total=(total+int.from_bytes(hashlib.sha256(encode(list(row))).digest(),'big'))%(1<<256)
                    if count%100000==0:atomic(out/'PROGRESS.json',{'phase':'ARCHIVE_IDENTITY_ORACLE','owner':owner,'rows':count,'owner_target':oracle['count'],'elapsed_seconds':time.monotonic()-began})
            assert count==oracle['count'] and total==oracle['identity_sum']
    with closing(connect(store.root/(active+'.sqlite'),readonly=True)) as c:
        digest=hashlib.sha256();count=0
        for row in c.execute('SELECT '+','.join(FIELDS)+' FROM label_events ORDER BY id'):
            digest.update(encode(list(row)));count+=1
            if count%100000==0:atomic(out/'PROGRESS.json',{'phase':'ACTIVE_PAYLOAD_ORACLE','rows':count,'owner':active,'elapsed_seconds':time.monotonic()-began})
        assert count==owners[active]['count'] and digest.hexdigest()==owners[active]['payload_hash'].hexdigest()
    with closing(connect(store.state,readonly=True)) as c:
        total=0;count=0
        for row in c.execute('SELECT event_hash,event_id,payload_sha256 FROM q_hot_keys'):
            count+=1;total=(total+int.from_bytes(hashlib.sha256(encode(list(row))).digest(),'big'))%(1<<256)
        assert count==owners[active]['count'] and total==owners[active]['identity_sum']
    with closing(connect(store.state,readonly=True)) as c:
        # The complete lookup was independently hashed against input above.
        atomic(out/'PROGRESS.json',{'phase':'TOP_ACCOUNT_DENSITY','lookup_rows':offered,'elapsed_seconds':time.monotonic()-began})
        vm_ticks=[0]
        def count_progress():
            vm_ticks[0]+=1
            atomic(out/'PROGRESS.json',{'phase':'TOP_ACCOUNT_DENSITY','sqlite_vm_instructions_at_least':vm_ticks[0]*1000000,'elapsed_seconds':time.monotonic()-began});return 0
        c.set_progress_handler(count_progress,1000000)
        top=list(c.execute('SELECT target_did,COUNT(*) n FROM q_recent_seen GROUP BY target_did ORDER BY n DESC,target_did LIMIT 3'))
    for subject,count in top:
        if subject not in public:subjects.append(subject);public[subject]={'count':count,'hash':None}
    query_end=store.frontier()['end'];queries=[]
    for subject in subjects:
        product=RecentObservations(RecentProvider(store),now=lambda:clock(query_end));expected=public[subject]
        preview_started=time.monotonic();preview_refusal=None
        try:product.create(subject,iso(start),query_end)
        except Refused as exc:preview_refusal=str(exc)
        preview={'seconds':time.monotonic()-preview_started,'refusal':preview_refusal,'maximum_rows':1000}
        observed_count=0;digest=hashlib.sha256();pages=0;t=time.monotonic();error=None;complete=False
        try:
            cursor=product.create_export(subject,iso(start),query_end)['cursor']
            while cursor:
                result=product.page(cursor);pages+=1
                for row in result['rows']:
                    observed_count+=1;digest.update(encode([row['uri'],row['value'],row['source_timestamp'],iso(clock(row['observed_at']))]))
                cursor=result['next_cursor'];complete=result['terminal']
                if pages%100==0:atomic(out/'PROGRESS.json',{'phase':'PRODUCT_EXPORT','subject':subject,'pages':pages,'delivered_rows':observed_count,'expected_rows':expected['count'],'elapsed_seconds':time.monotonic()-t})
            assert observed_count==expected['count'],('export count',subject,observed_count,expected['count'])
            if expected['hash'] is not None:assert digest.hexdigest()==expected['hash'].hexdigest(),('export input digest',subject)
        except Refused as exc:error=str(exc)
        queries.append({'subject':subject,'expected_rows':expected['count'],'delivered_rows':observed_count,'pages':pages,'seconds':time.monotonic()-t,'complete':complete,'refusal':error,'ttl_seconds':120,'page_rows':100,'account_preview':preview,'input_projection_hash_compared':expected['hash'] is not None,'scope':'Actual product fixed TTL, no network transfer overhead; terminal required for completion'})
    retained=admission();advances=[]
    for day in range(1,args.advance_days+1):
        when=end+dt.timedelta(days=day);local=list(store.root.glob('*.sqlite'));admission(2*sum(p.stat().st_size for p in local)+8*1024**2)
        store.advance_day(iso(when));expected=0
        with (out/'PAGES.jsonl').open() as f:
            for line in f:
                entry=json.loads(line)
                if clock(entry['clock'])>=when-dt.timedelta(days=30):expected+=entry['rows']
        with closing(connect(store.state,readonly=True)) as c:assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]==expected
        advances.append({'day':day,'expected_retained_rows':expected,**admission()})
    result={'result':'CONSTRUCTED_VOLUME_QUALIFIED' if all(x['complete'] for x in queries) else 'CONSTRUCTED_VOLUME_WITH_EXPORT_REFUSAL','events':offered,'roster':len(sources),'source_pages':page_count,'physical':physical,'retained':retained,'advances':advances,'queries':queries,'input_sha256':receipt['parquet_sha256'],
            'input_normalization':'INPUT-NORMALIZATION.json','transition_measurements':'TRANSITIONS.jsonl','top_accounts':[list(x) for x in top],'limits':{'ttl_seconds':120,'page_rows':100,'snapshot_rows':1000},'limitations':'Constructed post-flush state only; no actual acceptance throughput/recovery claim. Source-homogeneous <=100 pages, nonoverlapping synthetic authored-time cycles and actual strict canonical identities, src becomes selected labeler, copied unauthenticated signature shapes; disabled historical derived tables remain empty. Sampled peaks, not continuous maximum. Export deadline unchanged; incomplete export is not success.'}
    atomic(out/'RESULT.json',result);atomic(out/'CONSTRUCTION.json',{'state':'QUALIFIED_CONSTRUCTED_FIXTURE','events':offered});return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--occurrence',type=Path,required=True)
    p.add_argument('--specimen',type=Path);p.add_argument('--events',type=int);p.add_argument('--ceiling-bytes',type=int)
    p.add_argument('--advance-days',type=int,choices=range(3),default=2);a=p.parse_args()
    storage.owned(a.occurrence)
    if a.specimen and (a.events is None or a.ceiling_bytes is None):p.error('specimen construction requires explicit events and admitted ceiling')
    try:print(json.dumps(construct_specimen(a) if a.specimen else qualify(a.occurrence)),flush=True)
    except BaseException as ex:
        if a.occurrence.exists():atomic(a.occurrence/'TERMINAL.json',{'result':'FAIL','error':repr(ex)})
        raise
    else:atomic(a.occurrence/'TERMINAL.json',{'result':'PASS'})
