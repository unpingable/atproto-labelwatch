"""Bounded source adapter; no production enrollment or complete-stream claim."""
import contextlib
import hashlib
import json
import threading
import time
from pathlib import Path
import duckdb
from recent_storage import RecentStore, clock, iso
from storage import connect, lock, FIELDS, db, custody
from tier import VerifiedCatalog


class RecentProvider:
    def __init__(self, store, archive=None):
        self.store=store if isinstance(store,RecentStore) else RecentStore(store)
        self.archive=self.store.root/'archive'
        if archive is not None and Path(archive).resolve()!=self.archive.resolve():raise ValueError('archive mismatch')

    @contextlib.contextmanager
    def snapshot(self,did,start,end,max_rows,timeout_seconds=10,max_bytes=1024**2):
        if not 0<max_rows<=100001 or not 0<timeout_seconds<=30:raise ValueError('query bounds invalid')
        if not 0<max_bytes<=16*1024**2:raise ValueError('query byte bound invalid')
        deadline=time.monotonic()+timeout_seconds
        start,end=iso(clock(start)),iso(clock(end))
        # Serialize acquisition/materialization; all leases are released before
        # yielding immutable product rows, so pagination cannot pin storage.
        with lock(self.store.root,timeout=timeout_seconds),lock(self.store.root,'reader.lock',shared=True,timeout=max(.001,deadline-time.monotonic())):
            c=connect(self.store.state,readonly=True);duck=duckdb.connect()
            duck.execute("SET memory_limit='128MB'");duck.execute('SET threads=1');duck.execute("SET max_temp_directory_size='0B'")
            timer=threading.Timer(max(.001,deadline-time.monotonic()),duck.interrupt);timer.daemon=True;timer.start()
            try:
                self.store.require(c)
                if c.execute('SELECT 1 FROM q_recent_transition').fetchone():raise RuntimeError('maintenance recovery pending')
                frontier={k:db.get_meta(c,'q:recent_'+k) for k in ('start','end','generation')}
                if clock(start)<clock(frontier['start']) or clock(end)>clock(frontier['end']) or clock(start)>=clock(end):raise RuntimeError('requested observation range unavailable')
                c.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
                selected=list(c.execute('SELECT event_id,observed_at,owner FROM q_recent_seen WHERE target_did=? AND observed_at>=? AND observed_at<? ORDER BY observed_at,event_id LIMIT ?', (did,start,end,max_rows)))
                seen={r[0]:r[1] for r in selected}
                groups={}
                for event_id,observed,owner in selected:groups.setdefault(owner,[]).append(event_id)
                gaps=[]
                acquisition=db.get_meta(c,'q:recent_acquisition_start')
                if start<acquisition:gaps.append({'start':start,'end':min(end,acquisition),'reason':'not_observed'})
                for lo,hi,reason in c.execute('SELECT start,end,reason FROM q_recent_gaps WHERE end>? AND start<? ORDER BY start',(start,end)):
                    gaps.append({'start':max(start,lo),'end':min(end,hi),'reason':reason})
                if db.get_meta(c,'q:recent_gap_overflow')=='1' or len(gaps)>128:
                    gaps=[{'start':start,'end':end,'reason':'unknown'}]
                coverage={'status':'unknown','gaps':gaps}
                catalog=VerifiedCatalog(self.archive,store=self.store)
                owners={owner:(p,r) for owner,p,r in custody.committed(c)}
                archives=[]
                for owner,path in catalog.files():
                    if owner not in owners:continue
                    receipt_path,receipt=owners[owner];catalog.validate_binding(owner,receipt)
                    if json.loads(receipt_path.read_text())!=receipt:raise RuntimeError('receipt projection conflict')
                    archives.append((owner,path))
                if {owner for owner,path in archives}!=set(owners):raise RuntimeError('retained archive coverage unavailable')
                active=[owner for owner,status in c.execute('SELECT identity,status FROM q_segments') if status in ('ACTIVE','SEALED')]
                found={};ids=list(seen);used=0
                fields=tuple(x for x in FIELDS if x not in ('sig','event_hash','target_did'))
                def admit(value):
                    nonlocal used
                    if value['id'] in found:raise RuntimeError('duplicate retained event identity')
                    value['observed_at']=seen[value['id']]
                    used+=len(json.dumps(value,ensure_ascii=False).encode())
                    if used>max_bytes:raise RuntimeError('recent query byte ceiling')
                    found[value['id']]=value
                work=[(owner,values[offset:offset+500]) for owner,values in groups.items() for offset in range(0,len(values),500)]
                for owner,part in work:
                    query='SELECT '+','.join(fields)+' FROM label_events WHERE id IN ('+','.join('?' for _ in part)+')'
                    if owner in active:
                        hot=connect(self.store.root/(owner+'.sqlite'),readonly=True)
                        try:
                            hot.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
                            for row in hot.execute(query,part):admit(dict(row))
                        finally:hot.close()
                    for candidate,path in archives:
                        if candidate!=owner:continue
                        duck.from_parquet(str(path)).create_view('label_events',replace=True)
                        result=duck.execute(query,part)
                        while (row:=result.fetchone()) is not None:admit(dict(zip(fields,row)))
                    if time.monotonic()>deadline:raise TimeoutError('recent query deadline')
                if set(found)!=set(seen):raise RuntimeError('observation payload coverage unavailable')
                rows=[]
                for event_id,observed in seen.items():
                    value=found[event_id];value['observed_at']=observed;rows.append(value)
                if time.monotonic()>deadline:raise TimeoutError('recent query deadline')
            finally:timer.cancel();duck.close();c.close()
        yield {'rows':rows,'frontier':hashlib.sha256(json.dumps([frontier,coverage],sort_keys=True).encode()).hexdigest(),
               'coverage':coverage}
