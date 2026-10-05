"""Current-scale query supplement, reusing sealed producer archive read-only."""
from __future__ import annotations
import datetime as dt,json,statistics,sys,time
from pathlib import Path
from storage import Store,ROOT,atomic,connect,db,ingest,sha,retention
from tier import TierSession
from labelwatch import frontdoor,report


def main(occurrence,primary):
    occurrence=Path(occurrence);occurrence.mkdir();primary=Path(primary);result=json.loads((primary/'RESULT.json').read_text());archive=Path(result['archive']['archive_root']);p=result['specimen']['query_parameters']
    s=Store.create(occurrence/'store',period='2026-10-05')
    # Restore global metadata/tables into an independent query fixture. The
    # original producer state stays sealed. No old replay keys are reconstructed:
    # this is a query fixture, not an admissible collector cutover.
    original=connect(primary/'store/state.sqlite',readonly=True);c=connect(s.state)
    for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'q_%' AND name NOT IN ('label_events','sqlite_sequence')").fetchall():
        rows=original.execute('SELECT * FROM "'+name+'"').fetchall();c.execute('DELETE FROM "'+name+'"')
        if rows:c.executemany('INSERT INTO "'+name+'" VALUES('+','.join('?' for _ in rows[0])+')',rows)
    db.set_meta(c,'q:active','2026-10-05');db.set_meta(c,'q:max_local_segments','2');db.set_meta(c,'q:max_pending','10000');db.set_meta(c,retention.RETENTION_FLOOR_KEY,'2026-08-15T00:00:00Z')
    c.execute("INSERT INTO q_segments VALUES ('2026-09-28','RETIRED',23)");c.execute("INSERT INTO q_archive VALUES ('2026-09-28',?)",(str(archive/'2026-09-28.receipt.json'),));c.execute("INSERT INTO sqlite_sequence(name,seq) VALUES('label_events',?)",(result['events']+result['extra_new_segment_events'],));c.commit();c.close();original.close()
    rows=[]
    for i in range(100):
        raw={'src':p['labeler'],'uri':'at://'+p['dense']+'/app.bsky.feed.post/cross-'+str(i),'val':'cross-tier','ts':'2026-10-05T12:00:00Z'};r=ingest.normalize_label(raw);rows.append(tuple(getattr(r,k) for k in ['labeler_did','src','uri','cid','val','neg','exp','sig','ts','event_hash'])+(p['dense'],))
    assert s.ingest(rows,'fixture-cross-tier','100')['inserted']==100
    measures={}
    for name,subject in [('dense',p['dense']),('sparse',p['sparse'])]:
        times=[];admissions=[]
        for i in range(7):
            start=time.perf_counter()
            with TierSession(s,archive,subject=subject) as reader:
                admissions.append(time.perf_counter()-start);answer=frontdoor.lookup_subject(reader,subject,audit_receipt={'overall_verdict':'admissible'})
                if name=='dense':assert answer.refusal=='subject_too_dense',answer.refusal
                else:assert answer.refusal is None,answer.refusal
            times.append(time.perf_counter()-start)
        measures[name]={'p50_seconds_including_integrity_admission':statistics.median(times),'p95_seconds_including_integrity_admission':max(times),'admission_p50_seconds':statistics.median(admissions),'observations':7,'actual_frontdoor_refusal':answer.refusal}
        if max(times)>10:raise RuntimeError('existing 10-second frontdoor generation budget failed: '+name)
    with TierSession(s,archive,include_below_floor=True) as reader:
        count=reader.execute('SELECT COUNT(*) AS c FROM label_events').fetchone()['c'];assert count==result['events']+100
        dense_count=reader.execute('SELECT COUNT(*) AS c FROM label_events WHERE target_did = ?',(p['dense'],)).fetchone()['c']
        report_groups=reader.execute('SELECT labeler_did,val,COUNT(*) AS c FROM label_events GROUP BY labeler_did,val').fetchall()
    # Execute the actual report helpers without activating a report job.
    # Hourly distribution, activity, top-target and evidence reads are batch
    # consumers; their timings have no invented interactive SLA.
    report_reads={}
    with TierSession(s,archive,include_below_floor=True) as reader:
        helpers=[('report_hourly_counts',lambda:report._hourly_counts(reader,p['labeler'],'2026-09-28T00:00:00Z','2026-10-05T00:00:00Z')),
                 ('report_labeler_activity',lambda:report._labeler_activity(reader,p['labeler'],'2026-09-28T00:00:00Z','2026-10-05T00:00:00Z')),
                 ('report_top_targets',lambda:report._top_targets(reader,p['labeler'],'2026-09-28T00:00:00Z','2026-10-05T00:00:00Z'))]
        for name,fn in helpers:
            times=[];previous=None
            for i in range(7):
                start=time.perf_counter();answer=fn();times.append(time.perf_counter()-start)
                if name!='report_top_targets' and previous is not None:assert answer==previous
                previous=answer
            report_reads[name]={'p50_seconds':statistics.median(times),'p95_seconds':max(times),'observations':7,'result_count':len(answer) if isinstance(answer,list) else answer,'actual_helper':True}
        evidence=report._alert_events(reader,[rows[0][9]])
        assert len(evidence)==1 and evidence[0]['event_hash']==rows[0][9]
        report_reads['report_alert_events_cross_tier']={'result':'PASS','hot_event_found':True}
    atomic(occurrence/'RESULT.json',{'result':'PASS','source_primary_result_sha256':sha(primary/'RESULT.json'),'source_archive_receipt_sha256':sha(archive/'2026-09-28.receipt.json'),'events_queried':count,'hot_added':100,'dense_events':dense_count,'frontdoor':measures,'report_groups':len(report_groups),'actual_report_reads':report_reads,'no_duckdb_synchronous_ingest':True,'scope':'Complete current-scale retired Parquet plus active SQLite. Restored global state is query-only, not a collector cutover; synthetic floor unchanged in production. Frontdoor keeps existing cold-history and density behavior.'})


if __name__=='__main__':
    terminal=Path(sys.argv[3])
    try:main(sys.argv[1],sys.argv[2])
    except BaseException as e:atomic(terminal,{'result':'FAILED','error':repr(e),'at':dt.datetime.now(dt.timezone.utc).isoformat()});raise
    else:atomic(terminal,{'result':'PASS','at':dt.datetime.now(dt.timezone.utc).isoformat()})
