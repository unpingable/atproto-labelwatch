"""Replicate terminal producer global state before explicit fixture handoff."""
import datetime as dt,json,os,shutil,sys
from pathlib import Path
from storage import ROOT,atomic,sha,syncdir,owned

def main(src,dst,result):
 src=Path(src);owned(src.parent);dst=Path(dst);archive=Path(json.loads((ROOT.parent/'evidence/ARCHIVE-DESTINATION.json').read_text())['campaign_archive']);assert archive in dst.resolve().parents
 assert not Path('/proc/3988364').exists(),'original producer still exists'
 assert not Path(str(src)+'-wal').exists(),'uncheckpointed source WAL'
 assert not dst.exists();s=os.statvfs(dst.parent);assert s.f_bavail*s.f_frsize>src.stat().st_size+1048576
 before=sha(src);tmp=dst.with_suffix('.incomplete');shutil.copyfile(src,tmp);tmp.chmod(0o600)
 with tmp.open('rb') as f:os.fsync(f.fileno())
 assert sha(tmp)==before==sha(src),'source changed during custody checkpoint';os.replace(tmp,dst);syncdir(dst.parent)
 atomic(result,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat(),'source':str(src),'destination':str(dst),'sha256':before,'bytes':src.stat().st_size,'source_and_destination_links':[src.stat().st_nlink,dst.stat().st_nlink],'purpose':'Unique terminal global state before continuation; original failure/log/source remain immutable. Handoff is qualification fixture only.'})

if __name__=='__main__':
 terminal=Path(sys.argv[4])
 try:main(sys.argv[1],sys.argv[2],sys.argv[3])
 except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
 else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
