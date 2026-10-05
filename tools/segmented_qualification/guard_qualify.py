"""Finite actual-cap and mid-write refusal qualification, not production."""
import errno,json,os,sqlite3,subprocess,sys
from pathlib import Path
from unittest.mock import patch
from storage import Store,ROOT,atomic,connect,digest,COLS,ingest,pq
from qualify import qualify,event
from supplement import qualify as supplement
from observations import qualify as observations


def large_event(i):
    raw={'src':'did:plc:abcdefghijklmnopqrstuvwx','uri':'at://did:plc:abcdefghijklmnopqrstuvwx/app.bsky.feed.post/'+str(i),'val':'large','ts':'2026-09-28T12:00:00Z','sig':'s'*8192}
    r=ingest.normalize_label(raw)
    return tuple(getattr(r,k) for k in ['labeler_did','src','uri','cid','val','neg','exp','sig','ts','event_hash'])+('did:plc:abcdefghijklmnopqrstuvwx',)


def main(base):
    base=Path(base);base.mkdir();finite=qualify(base/'finite');sup=supplement(base/'supplement');observations(base/'OBSERVATIONS.json');cases=[]
    s=Store.create(base/'full_vessel');c=connect(s.root/'2026-09-28.sqlite');limit=c.execute('PRAGMA page_count').fetchone()[0];c.execute("UPDATE segment_meta SET value=? WHERE key='page_cap'",(str(limit),));c.commit();c.close()
    for i in range(3):
        c=connect(s.root/'2026-09-28.sqlite');assert c.execute('PRAGMA max_page_count').fetchone()[0]==limit;c.close()
    rows=[large_event(i) for i in range(100)]
    try:s.ingest(rows,'capacity-provider','100')
    except sqlite3.OperationalError as e:assert 'full' in str(e)
    else:raise AssertionError('actual file ceiling not enforced')
    c=connect(s.state);assert c.execute('SELECT COUNT(*) FROM q_pending').fetchone()[0]==100;assert c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==100;assert c.execute("SELECT value FROM meta WHERE key='ingest_cursor:capacity-provider'").fetchone()[0]=='100';journal=digest(c.execute('SELECT '+COLS+' FROM label_events ORDER BY id'));c.close()
    # Owned bounded recovery allowance, not a new filesystem or production gate.
    c=connect(s.root/'2026-09-28.sqlite');c.execute("UPDATE segment_meta SET value='2048' WHERE key='page_cap'");c.commit();c.close();s.snapshot();assert s.ingest(rows,'capacity-provider','101')['inserted']==0
    with s.queries() as c:assert digest(c.execute('SELECT '+COLS+' FROM label_events ORDER BY id'))==journal
    c=connect(s.state);assert c.execute("SELECT observed_as_src FROM labelers WHERE labeler_did='did:plc:abcdefghijklmnopqrstuvwx'").fetchone()[0]==1;c.close()
    cases.append({'case':'actual_vessel_ceiling_across_reopen','result':'PASS_BOUNDED_RECOVERY','accepted_pending':100,'recovery_allowance_bytes':2048*4096,'cursor_and_exact_rows_preserved':True,'observed_source_metadata_preserved':True})
    s=Store.create(base/'full_global');c=connect(s.state);limit=c.execute('PRAGMA page_count').fetchone()[0];c.execute("INSERT INTO meta VALUES('q:state_page_cap',?)",(str(limit),));c.commit();c.close()
    try:s.ingest(rows,'capacity-provider','100')
    except sqlite3.OperationalError as e:assert 'full' in str(e)
    else:raise AssertionError('global cap not enforced')
    c=connect(s.state,readonly=True);assert c.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==0;assert c.execute("SELECT value FROM meta WHERE key='ingest_cursor:capacity-provider'").fetchone() is None;c.close();cases.append({'case':'actual_global_ceiling','result':'PASS_REFUSAL_BEFORE_ACCEPTANCE'})
    for kind in ['read_only_io','full_after_admission','permission_after_admission']:
        s=Store.create(base/kind)
        for offset in [0,10000]:s.ingest([event(i+offset) for i in range(10000)],'fixture-provider',str(offset))
        s.rotate('2026-10-05');dest=base/(kind+'-archive');dest.mkdir()
        if kind=='read_only_io':
            dest.chmod(0o500)
            try:s.archive('2026-09-28',dest)
            except PermissionError:pass
            else:raise AssertionError('actual write refusal missing')
            finally:dest.chmod(0o700)
        else:
            real=pq.ParquetWriter
            class RefuseWriter:
                def __init__(self,*a,**kw):self.inner=real(*a,**kw);self.calls=0
                def __enter__(self):self.inner.__enter__();return self
                def write_table(self,*a,**kw):
                    self.calls+=1
                    if self.calls>1:raise OSError(errno.ENOSPC if kind=='full_after_admission' else errno.EACCES,'deterministic archive write refusal')
                    return self.inner.write_table(*a,**kw)
                def __exit__(self,*a):return self.inner.__exit__(*a)
            with patch.object(pq,'ParquetWriter',RefuseWriter):
                try:s.archive('2026-09-28',dest)
                except OSError:pass
                else:raise AssertionError('mid-write refusal missing')
        assert (s.root/'2026-09-28.sqlite').exists();assert not (dest/'2026-09-28.receipt.json').exists();s.archive('2026-09-28',dest);s.retire('2026-09-28');cases.append({'case':kind,'result':'PASS_RETRY','events':20000,'source_preserved_until_verified_retry':True})
    s=Store.create(base/'concurrent_archive');s.ingest([event(i) for i in range(1000)],'fixture-provider','1000');s.rotate('2026-10-05');dest=base/'concurrent-archive';dest.mkdir();args=[sys.executable,str(Path(__file__).with_name('storage.py')),'archive',str(s.root),'--period','2026-09-28','--archive',str(dest)];children=[subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for i in range(2)];answers=[]
    for child in children:
        out,err=child.communicate(timeout=30);assert child.returncode==0,err;answers.append(json.loads(out))
    assert answers[0]==answers[1];original=(dest/'2026-09-28.receipt.json').read_bytes();s.archive('2026-09-28',dest);assert (dest/'2026-09-28.receipt.json').read_bytes()==original;s.retire('2026-09-28');cases.append({'case':'concurrent_conversion_and_immutable_retry_receipt','result':'PASS'})
    atomic(base/'RESULT.json',{'result':'PASS','finite_cases':len(finite),'supplement':sup,'guard_cases':cases,'production_mutations':[],'scope':'Page caps are applied to each writer; resource-full accepted journal has explicit <=8MiB fixture recovery, never loss. No production limit changed.'})


if __name__=='__main__':
    import datetime as dt
    terminal=Path(sys.argv[2])
    try:main(sys.argv[1])
    except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
    else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
