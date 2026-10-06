"""Bounded source adapter; no production enrollment or complete-stream claim."""
import contextlib
import hashlib
import json
import threading
import time
from pathlib import Path
import duckdb
from recent_storage import RecentStore, clock, iso, micros, from_micros, day_name
from storage import connect, lock, FIELDS, db, custody
from tier import VerifiedCatalog


class RecentProvider:
    def __init__(self, store, archive=None):
        self.store=store if isinstance(store,RecentStore) else RecentStore(store)
        self.archive=self.store.root/'archive'
        if archive is not None and Path(archive).resolve()!=self.archive.resolve():raise ValueError('archive mismatch')

    @contextlib.contextmanager
    def snapshot(self,did,start,end,max_rows,timeout_seconds=10,max_bytes=1024**2,*,after_id=None,upper_id=None):
        if not 0<max_rows<=100001 or not 0<timeout_seconds<=30:raise ValueError('query bounds invalid')
        if not 0<max_bytes<=16*1024**2:raise ValueError('query byte bound invalid')
        prefix=after_id is not None
        if prefix and (type(after_id) is not int or after_id<0 or (upper_id is not None and (type(upper_id) is not int or upper_id<after_id))):raise ValueError('invalid export keyset')
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
                frontier={k:db.get_meta(c,'q:recent_'+k) for k in ('start','end','generation','events')}
                if clock(start)<clock(frontier['start']) or clock(end)>clock(frontier['end']) or clock(start)>=clock(end):raise RuntimeError('requested observation range unavailable')
                c.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
                if prefix:
                    if upper_id is None:upper_id=c.execute('SELECT COALESCE(MAX(event_id),0) FROM q_recent_seen').fetchone()[0]
                    selected=list(c.execute('SELECT event_id,observed_us,owner_day FROM q_recent_seen WHERE target_did=? AND observed_us>=? AND observed_us<? AND event_id>? AND event_id<=? ORDER BY event_id LIMIT ?', (did,micros(start),micros(end),after_id,upper_id,max_rows+1)))
                    more=len(selected)>max_rows;selected=selected[:max_rows]
                else:
                    selected=list(c.execute('SELECT event_id,observed_us,owner_day FROM q_recent_seen WHERE target_did=? AND observed_us>=? AND observed_us<? ORDER BY observed_us,event_id LIMIT ?', (did,micros(start),micros(end),max_rows)))
                    more=False
                seen={r[0]:from_micros(r[1]) for r in selected}
                groups={}
                for event_id,observed,owner in selected:groups.setdefault(day_name(owner),[]).append(event_id)
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
                found={};ids=list(seen);used=0;stopped=False
                fields=tuple(x for x in FIELDS if x not in ('event_hash','target_did'))
                def admit(value):
                    nonlocal used,stopped,more
                    if value['id'] in found:raise RuntimeError('duplicate retained event identity')
                    value['observed_at']=seen[value['id']]
                    size=len(json.dumps(value,ensure_ascii=False).encode())
                    if used+size>max_bytes:
                        if not prefix or not found:raise RuntimeError('recent query byte ceiling')
                        stopped=True;more=True;return
                    used+=size
                    found[value['id']]=value
                work=[(owner,values[offset:offset+500]) for owner,values in groups.items() for offset in range(0,len(values),500)]
                for owner,part in work:
                    query='SELECT '+','.join(fields)+' FROM label_events WHERE id IN ('+','.join('?' for _ in part)+') ORDER BY id'
                    if owner in active:
                        hot=connect(self.store.root/(owner+'.sqlite'),readonly=True)
                        try:
                            hot.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
                            for row in hot.execute(query,part):
                                admit(dict(row))
                                if stopped:break
                        finally:hot.close()
                    for candidate,path in archives:
                        if stopped:break
                        if candidate!=owner:continue
                        duck.from_parquet(str(path)).create_view('label_events',replace=True)
                        result=duck.execute(query,part)
                        while (row:=result.fetchone()) is not None:
                            admit(dict(zip(fields,row)))
                            if stopped:break
                    if time.monotonic()>deadline:raise TimeoutError('recent query deadline')
                    if stopped:break
                if prefix and stopped:
                    retained_ids=ids[:len(found)]
                    if set(found)!=set(retained_ids):raise RuntimeError('export prefix payload coverage unavailable')
                    seen={key:seen[key] for key in retained_ids}
                if set(found)!=set(seen):raise RuntimeError('observation payload coverage unavailable')
                rows=[]
                for event_id,observed in seen.items():
                    value=found[event_id];value['observed_at']=observed;rows.append(value)
                if time.monotonic()>deadline:raise TimeoutError('recent query deadline')
            finally:timer.cancel();duck.close();c.close()
        yield {'rows':rows,'frontier':hashlib.sha256(json.dumps([frontier,coverage],sort_keys=True).encode()).hexdigest(),
               'coverage':coverage, 'upper_id':upper_id if prefix else None,
               'next_after_id':max(seen) if seen else after_id, 'has_more':more}
