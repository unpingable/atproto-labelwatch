"""Expected-history qualification after independent missing-period finding."""
from pathlib import Path
import sys,json,datetime as dt
from storage import Store,connect,db,atomic,sha
from qualify import event,child
from tier import TierSession

def main(out):
 out=Path(out);out.mkdir();s=Store.create(out/'store');archive=out/'archive';archive.mkdir();start=dt.date(2026,9,28);cases=[]
 for i in range(9):
  period=(start+dt.timedelta(days=7*i)).isoformat();nxt=(start+dt.timedelta(days=7*(i+1))).isoformat()
  assert s.ingest([event(i+1,period+'T12:00:00Z')],'coverage-fixture',str(i+1))['inserted']==1
  s.rotate(nxt);s.archive(period,archive);s.retire(period)
  if i==8:
   r=child('floor',s,'after_coverage_before_prune_commit',period=nxt+'T00:00:00Z');assert r.returncode==73,(r.returncode,r.stderr)
   c=connect(s.state,readonly=True);assert db.get_meta(c,'q:archive_coverage_path') is None;assert c.execute("SELECT 1 FROM q_segments WHERE identity='2026-09-28'").fetchone();c.close()
   with TierSession(s,archive,include_below_floor=True) as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==9
   cases.append({'case':'death_after_coverage_before_prune_commit','result':'PASS','old_expected_authority_retained':True})
  s.advance_floor(nxt+'T00:00:00Z')
 assert s.ingest([event(10,'2026-11-30T12:00:00Z')],'coverage-fixture','10')['inserted']==1
 c=connect(s.state,readonly=True);coverage=Path(db.get_meta(c,'q:archive_coverage_path'));assert sha(coverage)==db.get_meta(c,'q:archive_coverage_sha256');assert c.execute("SELECT 1 FROM q_segments WHERE identity='2026-09-28'").fetchone() is None;c.close()
 with TierSession(s,archive,include_below_floor=True) as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==10
 for suffix in ['.receipt.json','.parquet','coverage']:
  path=coverage if suffix=='coverage' else archive/('2026-09-28'+suffix);saved=path.with_suffix(path.suffix+'.fixture-absent');path.rename(saved)
  try:
   try:
    with TierSession(s,archive,include_below_floor=True) as q:q.execute('SELECT COUNT(*) FROM label_events').fetchone()
   except (RuntimeError,OSError) as e:cases.append({'case':'pruned_history_missing_'+suffix,'result':'PASS_REFUSAL','error':repr(e)})
   else:raise AssertionError('missing historical custody silently omitted')
  finally:saved.rename(path)
 previous=coverage.read_bytes();coverage.write_text('{}')
 try:
  try:
   with TierSession(s,archive,include_below_floor=True) as q:pass
  except RuntimeError as e:cases.append({'case':'corrupt_coverage_anchor','result':'PASS_REFUSAL','error':repr(e)})
  else:raise AssertionError('changed expected ledger accepted')
 finally:coverage.write_bytes(previous)
 s.advance_floor('2026-11-30T00:00:00Z')
 with TierSession(s,archive,include_below_floor=True) as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==10
 atomic(out/'RESULT.json',{'result':'PASS','cases':cases,'accepted':10,'complete_count_after_retry':10,'pruned_identity':'2026-09-28','coverage_bytes':coverage.stat().st_size,'scope':'Finite nine-closed-period fixture proves old expected coverage survives retry-ring pruning; full history admission scale still separate.','production_mutations':[]})

if __name__=='__main__':
 terminal=Path(sys.argv[2])
 try:main(sys.argv[1])
 except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
 else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
