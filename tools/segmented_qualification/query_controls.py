"""Bounded report-query controls on already verified immutable archive."""
import datetime as dt,json,sys,time
from pathlib import Path
import duckdb
from storage import atomic,ROOT,sha
SQL='SELECT labeler_did,COUNT(*),COUNT(DISTINCT uri) FROM label_events WHERE ts>=? AND ts<? GROUP BY labeler_did'
REWRITE='''WITH counts AS (SELECT labeler_did,COUNT(*) AS c FROM label_events WHERE ts>=? AND ts<? GROUP BY labeler_did), uniques AS (SELECT labeler_did,COUNT(*) AS u FROM (SELECT DISTINCT labeler_did,uri FROM label_events WHERE ts>=? AND ts<? AND uri IS NOT NULL) GROUP BY labeler_did) SELECT counts.labeler_did,counts.c,COALESCE(uniques.u,0) FROM counts LEFT JOIN uniques USING(labeler_did)'''

def main(out,archive):
 out=Path(out);out.mkdir();results=[]
 for label,memory,threads,rewrite in [('native_512_one_thread',512,1,False),('native_1024_one_thread',1024,1,False),('external_distinct_512',512,1,True)]:
  scratch=out/label;scratch.mkdir();c=duckdb.connect();c.execute("SET memory_limit='"+str(memory)+"MB'");c.execute('SET threads='+str(threads));c.execute('SET preserve_insertion_order=false');c.execute("SET max_temp_directory_size='2GB'");c.execute('SET temp_directory=?',[str(scratch)]);c.from_parquet(str(archive)).create_view('label_events');t=time.perf_counter()
  try:
   rows=sorted(c.execute(REWRITE if rewrite else SQL,('2026-09-28','2026-10-05')*(2 if rewrite else 1)).fetchall());v={'case':label,'result':'PASS','seconds':time.perf_counter()-t,'rows':rows,'memory_limit_mb':memory,'threads':threads,'spill_ceiling_bytes':2000000000}
  except duckdb.Error as e:v={'case':label,'result':'REFUSAL','seconds':time.perf_counter()-t,'error':str(e),'memory_limit_mb':memory,'threads':threads}
  finally:c.close()
  atomic(out/(label+'.json'),v);results.append(v);print(json.dumps({k:v[k] for k in v if k!='rows'}),flush=True)
 passed=[v['rows'] for v in results if v['result']=='PASS']
 if len(passed)>1:assert all(x==passed[0] for x in passed)
 atomic(out/'RESULT.json',{'result':'CONTROLS_COMPLETE','archive_sha256':sha(archive),'cases':results,'scope':'No SQLite reference repeated; exact result equivalence among successful native/rewrite controls. One-run diagnostic, not p95 benchmark.'})

if __name__=='__main__':
 terminal=Path(sys.argv[3])
 try:main(sys.argv[1],sys.argv[2])
 except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
 else:atomic(terminal,{'result':'PASS_CONTROLS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
