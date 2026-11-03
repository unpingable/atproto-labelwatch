"""Custody-qualified continuation of terminal12M fixture; never repeat ingest."""
import datetime as dt,hashlib,json,os,resource,statistics,sys,time
from pathlib import Path
from unittest.mock import patch
import duckdb,pyarrow.parquet as pq
from storage import Store,ROOT,atomic,connect,sha,digest,COLS,FIELDS,db,lock
from labelwatch import frontdoor


def main(out,primary,custody):
 out=Path(out);out.mkdir();primary=Path(primary);custody=json.loads(Path(custody).read_text());assert custody['result']=='PASS';assert sha(primary/'store/state.sqlite')==custody['sha256']
 s=Store(primary/'store');spec=json.loads((primary/'SPECIMEN.json').read_text());archive=Path(json.loads((ROOT.parent/'evidence/ARCHIVE-DESTINATION.json').read_text())['campaign_archive'])/primary.name;receipt=json.loads((archive/'2026-09-28.receipt.json').read_text());assert receipt['content']['rows']==12000000
 def progress(phase,**kw):
  atomic(out/'PROGRESS.json',{'at':dt.datetime.now(dt.timezone.utc).isoformat(),'phase':phase,**kw});print(json.dumps({'phase':phase,**kw}),flush=True)
 before=s.snapshot();assert before['pending']==0 and before['active']=='2026-10-05'
 with lock(s.root):
  c=connect(s.state);db.set_meta(c,'q:max_local_segments','2');c.commit();c.close()
 startcursor=dict(before['cursor']);c=connect(s.root/'2026-10-05.sqlite',readonly=True);new_events=c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0];c.close()
 first=next(pq.ParquetFile(primary/'specimen.parquet').iter_batches(batch_size=1)).to_pylist()[0];row=tuple(first[k] for k in FIELDS[1:]);assert s.ingest([row],'fixture-current-provider',startcursor['ingest_cursor:fixture-current-provider'])['inserted']==0;assert s.snapshot()['cursor']==startcursor
 progress('CUSTODY_HANDOFF',events=12000000,extra_new_segment_events=new_events)
 source=connect(s.root/'2026-09-28.sqlite',readonly=True);d=duckdb.connect();d.execute("SET memory_limit='512MB'");d.execute('SET threads=1');d.execute('SET preserve_insertion_order=false');d.execute("SET max_temp_directory_size='2GB'");spill=out/'duck-spill';spill.mkdir();d.execute('SET temp_directory=?',[str(spill)]);d.from_parquet(str(archive/'2026-09-28.parquet')).create_view('label_events')
 scratch=Path(os.environ['SQLITE_TMPDIR']).resolve();temp_peak={'root_bytes':0,'configured_data_bytes':0,'paths':[]};bad=set()
 def observe():
  totals={'root_bytes':0,'configured_data_bytes':0}
  for f in Path('/proc/self/fd').glob('*'):
   try:
    name=os.readlink(f)
    if 'etilqs_' not in name:continue
    n=f.stat().st_blocks*512;key='configured_data_bytes' if name.startswith(str(scratch)+'/') else 'root_bytes';totals[key]+=n
    if name not in temp_peak['paths']:temp_peak['paths'].append(name)
    if key=='root_bytes':bad.add(name)
   except OSError:pass
  for k,v in totals.items():temp_peak[k]=max(temp_peak[k],v)
  return int(bool(bad))
 source.set_progress_handler(observe,100000)
 p=spec['query_parameters'];inventory=[('Q3_dense',frontdoor._Q3_LABEL_VALUES,(p['dense'],)),('Q8a_dense',frontdoor._Q8A_DISTINCT_STATES,(p['dense'],)),('Q8b_dense',frontdoor._Q8B_LOCUS,(p['dense'],)),('Q8c_dense',frontdoor._Q8C_LABELED_RECORDS,(p['dense'],50)),('Q3_sparse',frontdoor._Q3_LABEL_VALUES,(p['sparse'],)),('recent_report','SELECT labeler_did,COUNT(*),COUNT(DISTINCT uri) FROM label_events WHERE ts>=? AND ts<? GROUP BY labeler_did',('2026-09-28','2026-10-05')),('debug_horizon','SELECT COUNT(*),MIN(ts),MAX(ts),MAX(id) FROM label_events',()),('derive_value_distribution','SELECT labeler_did,val,COUNT(*) FROM label_events GROUP BY labeler_did,val',())];queries=[]
 for name,sql,args in inventory:
  answers=[];timings={}
  for engine,c in [('sqlite',source),('parquet_duckdb',d)]:
   times=[];iterations=1 if name=='recent_report' and engine=='sqlite' else 7
   for i in range(iterations):
    t=time.perf_counter();rows=[tuple(r) for r in c.execute(sql,args).fetchall()];times.append(time.perf_counter()-t);atomic(out/(name+'-'+engine+'-'+str(i)+'.json'),{'seconds':times[-1],'result_rows':len(rows),'sorted_result':sorted(rows,key=lambda r:json.dumps(r,default=str)),'sql_sha256':hashlib.sha256(sql.encode()).hexdigest()});progress('QUERY_OBSERVATION',query=name,engine=engine,iteration=i+1,seconds=times[-1])
   answers.append(sorted(rows,key=lambda r:json.dumps(r,default=str)));timings[engine]={'p50_ms':statistics.median(times)*1000 if iterations>1 else None,'p95_ms':max(times)*1000 if iterations>1 else None,'single_pathological_observation_ms':times[0]*1000 if iterations==1 else None,'observations':iterations,'rows':len(rows)}
  assert answers[0]==answers[1],name;v={'name':name,'parity':'PASS','timings':timings};queries.append(v);atomic(out/(name+'-PARITY.json'),v)
 source.close();d.close();assert not bad;atomic(out/'TEMP-PLACEMENT.json',{'result':'PASS','sampled_peak':temp_peak,'scope':'SQLite progress-handler samples of actual open-unlinked files during reference sort. Paths must reside in precreated campaign data scratch; sampled lower bounds.'})
 with s.queries() as c:
  response=frontdoor.lookup_subject(c,p['dense'],audit_receipt={'overall_verdict':'admissible'});assert response.refusal=='subject_too_dense';assert c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==12000000+new_events
 before_retire=s.snapshot();unlink_times=[];real_unlink=Path.unlink
 def measured_unlink(path,*a,**kw):
  t=time.perf_counter();v=real_unlink(path,*a,**kw)
  if path==s.root/'2026-09-28.sqlite':unlink_times.append(time.perf_counter()-t)
  return v
 progress('RETIRE_VERIFIED_SOURCE');t=time.perf_counter()
 with patch.object(Path,'unlink',measured_unlink):reclaimed=s.retire('2026-09-28')
 retirement_seconds=time.perf_counter()-t;assert reclaimed==receipt['source_allocated_bytes'];assert not (s.root/'2026-09-28.sqlite').exists();after=s.snapshot();assert before_retire['state_tables']==after['state_tables'];assert before_retire['cursor']==after['cursor'];assert s.ingest([row],'fixture-old-replay','after-retirement')['inserted']==0
 progress('KEY_EXPIRY',deleted=0);t=time.perf_counter();expired=s.advance_floor('2026-10-05T00:00:00Z',observer=lambda n:progress('KEY_EXPIRY',deleted=n));gc_seconds=time.perf_counter()-t;c=connect(s.state);freelist=c.execute('PRAGMA freelist_count').fetchone()[0];c.close()
 result={'result':'PASS_CONTINUATION','events':12000000,'extra_new_segment_events':new_events,'source_primary':'82434791-694f-46d6-9036-c5a6b8f12017','original_terminal':'FAILED_QUERY_OOM','custody_checkpoint':custody,'archive':receipt,'specimen':spec,'queries':queries,'filesystem_reclaimed_bytes':reclaimed,'retirement_seconds_including_rehash':retirement_seconds,'source_unlink_seconds':unlink_times,'state':{'global_tables_preserved':len(after['state_tables']),'cursor_sources_preserved':len(startcursor),'pending':after['pending']},'key_gc':{'deleted':expired,'seconds':gc_seconds,'reusable_pages':freelist},'state_path':str(s.state),'rss_peak_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cpu_seconds':time.process_time(),'scope':'Owned fixture handoff after original terminal and verified unique-state replication; original failure not overwritten. Current-scale lifecycle continued without reingest. No production mutation.'};atomic(out/'RESULT.json',result);progress('TERMINAL_PASS',events=12000000)

if __name__=='__main__':
 terminal=Path(sys.argv[4])
 try:main(sys.argv[1],sys.argv[2],sys.argv[3])
 except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
 else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
