"""Finite arrival-period transition and cursor boundary cases."""
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from common import atomic,guard_root
from rollover import create,rotate,ingest,facts,recover

def run(root):
    root=guard_root(root);root.mkdir();cases=[]
    for cut in ('after_seal','after_next_create','after_next_verify','after_pointer'):
        path=root/cut;create(path,'2026-01-01');ingest(path,'2026-01-01T12:00:00Z','1')
        p=subprocess.run([sys.executable,str(Path(__file__).with_name('rollover.py')),'rotate',str(path),'2026-01-02','--cut',cut],capture_output=True,text=True)
        assert p.returncode==73,p.stderr
        recover(path);assert facts(path)=={'rows':1,'distinct_hashes':1,'distinct_ids':1,'cursor':'1','period':'2026-01-02'}
        assert ingest(path,'2026-01-01T13:00:00Z','2')['accepted'] # authored time in sealed arrival day
        assert ingest(path,'2026-01-01T12:00:00Z','3')['duplicate']
        assert facts(path)['rows']==2
        cases.append({'case':cut,'status':'PASS','late_arrival':'accepted into current arrival vessel without mutating sealed vessel','recovery':'source-bound next vessel plus durable active pointer'})
    for cut,count in [('before_ingest_commit',0),('after_ingest_commit',1)]:
        path=root/cut;create(path,'2026-01-01')
        p=subprocess.run([sys.executable,str(Path(__file__).with_name('rollover.py')),'ingest',str(path),'2026-01-01T12:00:00Z','--token','1','--cut',cut],capture_output=True,text=True)
        assert p.returncode==73,p.stderr
        f=facts(path);assert f['rows']==count;assert f['cursor']==str(count)
        ingest(path,'2026-01-01T12:00:00Z','1');assert facts(path)['rows']==1
        cases.append({'case':cut,'status':'PASS','fact':'event and cursor commit together in the active vessel; retry deduplicates'})
    path=root/'boundary';create(path,'2026-01-01');ingest(path,'2026-01-01T23:59:59Z','1');rotate(path,'2026-01-02');ingest(path,'2026-01-02T00:00:00Z','2')
    assert facts(path)['rows']==2
    cases.append({'case':'clock_crosses_daily_boundary','status':'PASS'})
    try:rotate(path,'2025-12-31')
    except RuntimeError:pass
    else:raise AssertionError('backward clock not refused')
    assert not ingest(path,'2025-12-31T23:59:59Z','old')['accepted']
    cases.append({'case':'backward_clock_and_below_floor_arrival','status':'PASS_REFUSAL'})
    # A stale active pointer must find the already committed successor, not overwrite it.
    atomic(path/'ACTIVE.json',{'file':'arrival-2026-01-01.sqlite','period':'2026-01-01'});recover(path);assert facts(path)['rows']==2
    cases.append({'case':'stale_active_pointer','status':'PASS'})
    atomic(root/'RESULT.json',{'cases':cases,'scope':'Finite one-writer process interruption with event/cursor subset; not full Labelwatch backend or physical host crash','unqualified':['archive-only dedupe','full mutable state transfer','metadata archive/restore envelope','full-volume hot/cold query admission','real NFS stable custody']})
    return cases

if __name__=='__main__':run(Path(sys.argv[1]))
