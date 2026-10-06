"""Immutable catalog admission/replacement and content-mutation controls."""
import json,os,sys,datetime as dt
from pathlib import Path
from storage import Store,atomic,connect
from qualify import event
from tier import TierSession,VerifiedCatalog

def main(out):
 out=Path(out);out.mkdir();s=Store.create(out/'store');s.ingest([event()],'fixture-source','1');s.rotate('2026-10-05');a=out/'archive';a.mkdir();s.archive('2026-09-28',a);s.retire('2026-09-28');catalog=VerifiedCatalog(a,store=s)
 for i in range(3):
  with TierSession(s,a,catalog=catalog) as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==1
 file=a/'2026-09-28.parquet';old=file.read_bytes();stamp=file.stat()
 with file.open('r+b') as f:f.seek(5);f.write(b'X')
 os.utime(file,ns=(stamp.st_atime_ns,stamp.st_mtime_ns))
 try:
  with TierSession(s,a,catalog=catalog):pass
 except RuntimeError:pass
 else:raise AssertionError('content mutation with restored mtime silently accepted')
 file.write_bytes(old);fresh=VerifiedCatalog(a,store=s);c=connect(s.state);fresh.admit(c,'2026-09-28');c.close();replacement=a/'replacement.incomplete';replacement.write_bytes(old);os.replace(replacement,file)
 try:fresh.files()
 except RuntimeError:pass
 else:raise AssertionError('pathname replacement silently accepted')
 refreshed=VerifiedCatalog(a,store=s);c=connect(s.state);refreshed.admit(c,'2026-09-28');c.close()
 with TierSession(s,a,catalog=refreshed) as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==1
 s.ingest([event(2,'2026-10-06T00:00:00Z')],'fixture-source','2');s.rotate('2026-10-12');s.archive('2026-10-05',a)
 with TierSession(s,a,catalog=refreshed) as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==2
 # Missing derived entry must refuse; one explicit committed-owner admission repairs it.
 with refreshed.connection() as c:c.execute("DELETE FROM entries WHERE identity='2026-10-05'")
 try:
  with TierSession(s,a,catalog=refreshed):pass
 except RuntimeError:pass
 else:raise AssertionError('missing catalog owner silently omitted')
 c=connect(s.state);refreshed.admit(c,'2026-10-05');c.close()
 latest=VerifiedCatalog(a,store=s)
 with TierSession(s,a,catalog=latest) as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==2
 atomic(out/'RESULT.json',{'result':'PASS','cases':['three_cached_queries','content_mutation_mtime_restored_refuses','pathname_replacement_refuses','explicit_readmission_recovers','incremental_new_owner_complete_without_reconstruction','missing_catalog_owner_refuses','new_segment_readmission_recovers'],'scope':'Same-host local fixture, honest archive custody and no authorized mutation after admission. File fingerprints are not hardware bit-rot detection; full scheduled audits remain required. No production mutation.'})

if __name__=='__main__':
 terminal=Path(sys.argv[2])
 try:main(sys.argv[1])
 except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
 else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})


def bounded(out, count=8192):
 """Actual empty committed archives isolate metadata scaling from event volume."""
 import ast,hashlib,resource,statistics,subprocess,time
 import tier
 from storage import vessel,sha,cut
 out=Path(out);out.mkdir();archive=out/'archive';archive.mkdir()
 s=Store.create(out/'store',period='2100-01-01',floor='0001-01-01T00:00:00Z')
 catalog=VerifiedCatalog(archive,store=s);start=dt.date(1900,1,1);measures=[];latest=None
 def io():return {k:int(v) for k,v in (line.split(':') for line in Path('/proc/self/io').read_text().splitlines())}
 def prepare(identity):
  path=s.root/(identity+'.sqlite');vessel(path,identity)
  c=connect(path);c.execute("UPDATE segment_meta SET value='1' WHERE key='sealed'");c.commit();c.close()
  c=connect(s.state);c.execute("INSERT INTO q_segments VALUES(?,'SEALED',23)",(identity,));c.commit();c.close()
 for n in range(count+1):
  identity=(start+dt.timedelta(days=n)).isoformat();prepare(identity)
  calls=[];original=tier.sha
  def counted(path):calls.append(str(path));return original(path)
  tier.sha=counted;before=io();t=time.perf_counter()
  try:s._archive(identity,archive)
  finally:tier.sha=original
  elapsed=time.perf_counter()-t;after=io()
  assert calls==[str(archive/(identity+'.parquet'))],calls
  # Retiring this empty preparation vessel is normal isolated custody protocol,
  # not repeated volume reclamation qualification or retained-evidence cleanup.
  s.retire(identity);latest=identity
  if n in (128,1024,count):
   c=connect(s.state);old=tier.sha;hashes=[]
   def counted_lookup(path):hashes.append(str(path));return old(path)
   tier.sha=counted_lookup;times=[];b=io()
   try:
    for i in range(7):
     t=time.perf_counter();duplicate=catalog.admit(c,identity);assert duplicate['payload_hash_calls']==0
     assert catalog.lookup(identity)[0]==identity;times.append(time.perf_counter()-t)
    restarted=VerifiedCatalog(archive,store=s);assert restarted.lookup(identity)[0]==identity
   finally:tier.sha=old;c.close()
   e=io();assert not hashes
   with catalog.connection() as q:
    rows=q.execute('SELECT COUNT(*) FROM entries').fetchone()[0]
    plan=q.execute('EXPLAIN QUERY PLAN SELECT identity FROM entries WHERE identity=?',(identity,)).fetchall()
    assert 'SEARCH' in str(plan)
   m={'existing':n,'catalog_count':rows,'new_admission_payload_hashes':len(calls),'new_admission_payload_bytes':(archive/(identity+'.parquet')).stat().st_size,'new_admission_read_syscall_bytes':after['rchar']-before['rchar'],'new_admission_physical_read_bytes':after['read_bytes']-before['read_bytes'],'new_archive_protocol_seconds':elapsed,'duplicate_lookup_p50_seconds':statistics.median(times),'duplicate_lookup_p95_seconds':max(times),'duplicate_startup_payload_hashes':len(hashes),'duplicate_lookup_total_read_syscall_bytes':e['rchar']-b['rchar'],'catalog_bytes':catalog.path.stat().st_size,'point_query_plan':plan,'rss_peak_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}
   # Include SQLite index/metadata and candidate footer/hash reads, not just the
   # instrumented checksum: bound is far below the historic catalog file size.
   assert m['new_admission_read_syscall_bytes']<1048576,m
   measures.append(m);atomic(out/'PROGRESS.json',{'phase':'CATALOG_CARDINALITY','completed':n+1,'measurements':measures});print(json.dumps(m),flush=True)
  elif n%256==0:
   atomic(out/'PROGRESS.json',{'phase':'PREPARE_COMMITTED_EMPTY_ARCHIVES','completed':n+1,'target':count+1});print('catalog owners',n+1,flush=True)
 # Replay the actual former class, not a second implementation of its bug.
 source=subprocess.check_output(['git','show','cae2d4b490a8ad852accbed21f33997e4b82841c:tools/segmented_qualification/tier.py'],text=True)
 node=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=='VerifiedCatalog');old_calls=[]
 def legacy_sha(path):old_calls.append(str(path));return sha(path)
 scope={'Path':Path,'json':json,'sha':legacy_sha};exec(compile(ast.Module(body=[node],type_ignores=[]),'<preserved-cae2d4b-catalog>','exec'),scope)
 scope['VerifiedCatalog'](archive);assert len(old_calls)==count+1
 # The measured gate catches the exact old implementation; historical full
 # audit still deliberately traverses payloads, in a separately named phase.
 old=tier.sha;audit_calls=[]
 def audit_sha(path):audit_calls.append(str(path));return old(path)
 tier.sha=audit_sha
 try:audit=catalog.full_audit()
 finally:tier.sha=old
 assert len(audit_calls)==count+1 and audit['objects']==count+1
 atomic(out/'RESULT.json',{'result':'PASS','count':count+1,'events_in_cardinality_fixture':0,'specimen':'Actual schema23 empty sealed SQLite per identity, actual archive verification and canonical custody commit, actual retirement; no years of synthetic events claimed','measurements':measures,'former_implementation_negative_control':{'source':'cae2d4b490a8ad852accbed21f33997e4b82841c','hashes':len(old_calls),'bounded_admission_gate':'FAIL_AS_EXPECTED'},'separate_full_audit':audit,'latest_identity':latest,'catalog':str(catalog.path),'state':str(s.state),'archive':str(archive),'production_mutations':[]})
 return out/'RESULT.json'
