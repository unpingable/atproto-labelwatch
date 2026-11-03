"""Replay expiry at12M scale with continuing ingestion; restore qualified state."""
import datetime as dt,hashlib,json,os,statistics,sys,threading,time
from pathlib import Path
from storage import Store,ROOT,atomic,connect,sha,db
from qualify import event


def main(out,primary,custody):
 out=Path(out);out.mkdir();primary=Path(primary);receipt=json.loads(Path(custody).read_text());assert receipt['result']=='PASS';source=Path(receipt['destination']);root=out/'store';root.mkdir();dest=root/'state.sqlite';h=hashlib.sha256();copied=0
 with source.open('rb') as a,dest.open('xb') as b:
  while block:=a.read(8*1024*1024):
   h.update(block);b.write(block);b.flush();os.fsync(b.fileno());copied+=len(block);os.posix_fadvise(b.fileno(),0,copied,os.POSIX_FADV_DONTNEED)
 assert h.hexdigest()==receipt['sha256']==sha(dest)
 oldactive=primary/'store/2026-10-05.sqlite';active=root/oldactive.name;active.write_bytes(oldactive.read_bytes());assert sha(active)==sha(oldactive)
 for key in list(os.environ):
  if key.startswith('LABELWATCH_'):del os.environ[key]
 s=Store(root);c=connect(s.state);db.set_meta(c,'q:max_local_segments','2');c.commit();c.close();before=s.snapshot();assert before['pending']==0;oldcursors=before['cursor'];c=connect(s.state);initial_sequence=c.execute("SELECT seq FROM sqlite_sequence WHERE name='label_events'").fetchone()[0];c.close();offered=0;accepted=0;ops=[];errors=[];stop=threading.Event()
 def collector():
  nonlocal offered,accepted
  while not stop.wait(.1):
   rows=[event(800000000+offered+i,'2026-10-05T02:00:00Z') for i in range(5)];offered+=5;t=time.perf_counter()
   try:
    r=s.ingest(rows,'fixture-concurrent-gc',str(offered));accepted+=r['inserted'];ops.append({'at':time.perf_counter(),'seconds':time.perf_counter()-t,'inserted':r['inserted']})
   except BaseException as e:errors.append(repr(e));return
 t=time.perf_counter();worker=threading.Thread(target=collector);worker.start();observations=[]
 def progress(n):
  if n%500000==0:
   v={'at':dt.datetime.now(dt.timezone.utc).isoformat(),'phase':'EXPIRY_WITH_INGEST','expired_keys':n,'accepted_new_events':accepted,'successful_ingest_operations':len(ops)};observations.append(v);atomic(out/'PROGRESS.json',v);print(json.dumps(v),flush=True)
 try:expired=s.advance_floor('2026-10-05T00:00:00Z',observer=progress)
 finally:stop.set();worker.join(timeout=30)
 elapsed=time.perf_counter()-t;assert not worker.is_alive() and not errors;assert accepted>0;after=s.snapshot();assert after['pending']==0
 for k,v in oldcursors.items():assert after['cursor'][k]==v
 with s.queries() as q:
  hot=q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0];maximum=q.execute('SELECT MAX(id) FROM label_events').fetchone()[0]
 original_hot=connect(oldactive,readonly=True);old_count=original_hot.execute('SELECT COUNT(*) FROM label_events').fetchone()[0];original_hot.close();assert hot==old_count+accepted;assert maximum==initial_sequence+accepted
 assert after['cursor']['ingest_cursor:fixture-concurrent-gc']==str(offered);assert after['keys']==120+old_count+accepted;assert expired==11999880
 atomic(out/'RESULT.json',{'result':'PASS','source_global_checkpoint_sha256':receipt['sha256'],'restored_keys':before['keys'],'events_exercised':12000000,'expired_keys':expired,'expiry_seconds':elapsed,'offered_new_events':offered,'accepted_new_events':accepted,'ingest_operations':len(ops),'ingest_p50_seconds':statistics.median(x['seconds'] for x in ops),'ingest_p95_seconds':sorted(x['seconds'] for x in ops)[min(len(ops)-1,int(.95*len(ops)))],'prior_cursor_sources_preserved':len(oldcursors),'new_cursor':str(offered),'new_payload_count_and_global_sequence':'PASS','remaining_key_count':after['keys'],'global_state_bytes':s.state.stat().st_size,'observations':observations,'production_mutations':[],'scope':'Alternate qualification history restored from exact archived pre-expiry global state plus unchanged active vessel. Original failure and continuation remain separate. Key expiry releases writer between pages; no payload reingest.'})

if __name__=='__main__':
 terminal=Path(sys.argv[4])
 try:main(sys.argv[1],sys.argv[2],sys.argv[3])
 except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
 else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
