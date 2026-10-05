"""Finite restart/state/custody qualification before current-scale allocation."""
from __future__ import annotations
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from storage import Store, ROOT, atomic, connect, db, digest, FIELDS, COLS, ingest


def event(i=1, timestamp='2026-09-28T12:00:00Z'):
    raw={'src':'did:plc:fixturelabeler','uri':'at://did:plc:fixturesubject/app.bsky.feed.post/'+str(i), 'val':'test', 'ts':timestamp}
    r=ingest.normalize_label(raw)
    return (r.labeler_did,r.src,r.uri,r.cid,r.val,r.neg,r.exp,r.sig,r.ts,r.event_hash,'did:plc:fixturesubject')


def seed_state(store):
    c=connect(store.state); tables=[]
    for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'q_%' AND name NOT IN ('meta','label_events','sqlite_sequence')").fetchall():
        cols=c.execute('PRAGMA table_info("'+name+'")').fetchall();values=[]
        for col in cols:
            column,kind=col[1],col[2]
            if 'INT' in kind or 'REAL' in kind:values.append(1)
            elif column.endswith('_json'):values.append('{}')
            elif column.endswith('_at') or column in ('ts','first_seen','last_seen'):values.append('2026-09-28T00:00:00Z')
            else:values.append('fixture-'+column)
        c.execute('INSERT INTO "'+name+'" VALUES ('+','.join('?' for _ in cols)+')',values);tables.append(name)
    c.commit();c.close();return tables


def child(action,store,death,**extra):
    args=[sys.executable,str(Path(__file__).with_name('storage.py')),action,str(store.root),'--death',death]
    for k,v in extra.items():args+=['--'+k.replace('_','-'),str(v)]
    return subprocess.run(args,capture_output=True,text=True,timeout=30)


def qualify(base):
    base=Path(base);base.mkdir();results=[]
    for phase in ['before_rollover','after_transition','after_seal','after_successor','after_active_before_projection']:
        s=Store.create(base/phase);seed_state(s);s.ingest([event()], 'fixture-provider','101');before=s.snapshot()
        r=child('rotate',s,phase);assert r.returncode==73,(phase,r.stderr)
        s.rotate('2026-10-05');s.rotate('2026-10-05');after=s.snapshot()
        assert before['cursor']==after['cursor'];assert before['state_tables']==after['state_tables'];assert after['pending']==0
        assert after['active']=='2026-10-05';assert sum(x[1]=='ACTIVE' for x in after['segments'])==1
        late=event(2,'2026-09-29T12:00:00Z');assert s.ingest([late],'fixture-provider','102')['inserted']==1
        with s.queries() as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==2
        try:s.rotate('2026-10-04')
        except RuntimeError:pass
        else:raise AssertionError('backward clock accepted')
        results.append({'case':phase,'result':'PASS','global_tables_preserved':len(before['state_tables']),'cursor_preserved':True,'late_authored_time_accepted_in_current_vessel':True})
    for phase in ['before_global_commit','after_global_commit','after_vessel_commit','after_journal_drain']:
        s=Store.create(base/phase);seed_state(s);rows=base/(phase+'.json');atomic(rows,[event()]);r=child('ingest',s,phase,rows_json=rows,cursor='103');assert r.returncode==73,(phase,r.stderr)
        # Query recovery itself must expose all committed accepted rows.
        with s.queries() as q:n=q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]
        assert n==(0 if phase=='before_global_commit' else 1),(phase,n)
        s.ingest([event()],'fixture-provider','103');s.ingest([event()],'fixture-provider','103')
        with s.queries() as q:assert q.execute('SELECT COUNT(*) FROM label_events').fetchone()[0]==1
        assert s.snapshot()['cursor']['ingest_cursor:fixture-provider']=='103'
        results.append({'case':phase,'result':'PASS','automatic_journal_recovery':True,'duplicate_retry_exactly_one':True})
    for phase in ['conversion_killed','verification_killed','after_parquet_before_checkpoint','after_checkpoint_before_retirement']:
        s=Store.create(base/phase);seed_state(s);s.ingest([event()], 'fixture-provider','104');s.rotate('2026-10-05');before=s.snapshot();dest=base/(phase+'-archive');dest.mkdir()
        r=child('archive',s,phase,period='2026-09-28',archive=dest);assert r.returncode==73,(phase,r.stderr)
        s.archive('2026-09-28',dest);s.archive('2026-09-28',dest);assert s.retire('2026-09-28')>0;assert s.retire('2026-09-28')==0
        assert before['cursor']==s.snapshot()['cursor'];assert before['state_tables']==s.snapshot()['state_tables']
        # Archived-only replay within the floor is rejected using bounded hot keys.
        assert s.ingest([event()], 'fixture-provider','105')['inserted']==0
        assert s.snapshot()['keys']==1
        results.append({'case':phase,'result':'PASS','archive_only_dedupe_without_DuckDB':True,'state_and_cursor_survive_file_retirement':True})
    s=Store.create(base/'after_unlink');s.ingest([event()], 'fixture-provider','106');s.rotate('2026-10-05');dest=base/'after-unlink-archive';dest.mkdir();s.archive('2026-09-28',dest)
    r=child('retire',s,'after_unlink_before_retired',period='2026-09-28');assert r.returncode==73
    assert s.retire('2026-09-28')==0;s.archive('2026-09-28',dest);results.append({'case':'after_unlink_before_retired','result':'PASS'})
    for failure in ['archive_unavailable','archive_read_only','archive_full','root_low','retirement_failed','corrupt_parquet','stale_checkpoint','unverified_retirement']:
        s=Store.create(base/failure);s.ingest([event()], 'fixture-provider','107');s.rotate('2026-10-05');dest=base/(failure+'-archive');dest.mkdir()
        if failure in ['archive_unavailable','archive_read_only','archive_full','root_low']:
            try:s.archive('2026-09-28',dest,injected=failure)
            except (OSError,RuntimeError):pass
            else:raise AssertionError(failure)
            assert not list(dest.glob('*.parquet'))
        elif failure=='unverified_retirement':
            try:s.retire('2026-09-28')
            except RuntimeError:pass
            else:raise AssertionError(failure)
        else:
            s.archive('2026-09-28',dest)
            if failure=='corrupt_parquet':
                with (dest/'2026-09-28.parquet').open('r+b') as f:f.write(b'BAD!')
            if failure=='stale_checkpoint':atomic(s.root/'ACTIVE.json',{'identity':'wrong'})
            try:s.retire('2026-09-28',injected='retirement_failed' if failure=='retirement_failed' else None)
            except (OSError,RuntimeError):assert failure!='stale_checkpoint'
            else:assert failure=='stale_checkpoint'
        if failure!='stale_checkpoint':assert (s.root/'2026-09-28.sqlite').exists()
        results.append({'case':failure,'result':'PASS','behavior':'source preserved/refusal, or stale projection reconstructed from global authority'})
    # Below floor replays take the existing quarantine path and advance source cursor.
    s=Store.create(base/'below-floor');s.ingest([event(1,'2026-08-01T00:00:00Z')], 'fixture-provider','108');s.ingest([event(1,'2026-08-01T00:00:00Z')], 'fixture-provider','109')
    c=connect(s.state);assert c.execute('SELECT seen_count FROM quarantined_events').fetchone()[0]==2;assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0]==0;c.close();results.append({'case':'archive_below_floor_replay','result':'PASS','existing_quarantine_semantics':True})
    for phase in ['after_floor_commit','during_key_gc']:
        s=Store.create(base/phase);s.ingest([event()], 'fixture-provider','110');s.rotate('2026-10-05');dest=base/(phase+'-archive');dest.mkdir();s.archive('2026-09-28',dest);s.retire('2026-09-28')
        r=child('floor',s,phase,period='2026-10-05T00:00:00Z');assert r.returncode==73
        s.advance_floor('2026-10-05T00:00:00Z');assert s.snapshot()['keys']==0
        assert s.ingest([event()],'fixture-provider','111')['inserted']==0
        results.append({'case':phase,'result':'PASS','below_floor_replay_quarantined_after_cache_expiry':True})
    s=Store.create(base/'floor-before-custody');s.ingest([event()], 'fixture-provider','112');s.rotate('2026-10-05')
    try:s.advance_floor('2026-10-05T00:00:00Z')
    except RuntimeError:pass
    else:raise AssertionError('floor advanced before custody')
    results.append({'case':'floor_before_archive_custody','result':'PASS_REFUSAL'})
    s=Store.create(base/'unknown_schema');s.ingest([event()], 'fixture-provider','113');s.rotate('2026-10-05');c=connect(s.root/'2026-09-28.sqlite');c.execute('ALTER TABLE label_events ADD COLUMN new_field TEXT');c.commit();c.close();dest=base/'unknown-schema-archive';dest.mkdir()
    try:s.archive('2026-09-28',dest)
    except RuntimeError:pass
    else:raise AssertionError('unknown writer schema silently lost a column')
    assert not list(dest.glob('*.parquet'));results.append({'case':'unknown_writer_column','result':'PASS_REFUSAL'})
    atomic(base/'RESULT.json',{'cases':results,'rows_are_synthetic':'Finite typed state/control cases; current-scale corpus is separate','scope':'Process death and protocol-conforming concurrent writers; not physical host power loss','result':'PASS'})
    return results


if __name__=='__main__':qualify(Path(sys.argv[1]))
