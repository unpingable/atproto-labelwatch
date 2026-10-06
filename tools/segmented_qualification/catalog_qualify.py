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
