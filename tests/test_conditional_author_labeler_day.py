import sqlite3

import pytest

from labelwatch import db, scan

DAY=1788566400


def seed(c):
    db.init_db(c)
    c.executemany('INSERT INTO label_events(labeler_did,uri,val,neg,ts,event_hash,target_did) VALUES(?,?,?,?,?,?,?)',[
        ('did:l','at://did:a/app.bsky.feed.post/1','x',0,'2026-09-05T12:00:00.000000Z','one','did:a'),
        ('did:l','at://did:a/app.bsky.feed.post/2','x',None,'2026-09-05T13:00:00.000000Z','two','did:a'),
        ('did:l','at://did:b/app.bsky.feed.post/1','x',1,'2026-09-05T13:00:00.000000Z','three','did:b'),
    ])
    c.commit()


def rows(c):
    return [tuple(r) for r in c.execute('SELECT * FROM derived_author_labeler_day ORDER BY author_did,day_epoch,labeler_did')]


def test_exact_changes_new_keys_deletions_nulls_and_other_day(tmp_path):
    c=db.connect(str(tmp_path/'state.db'));seed(c)
    c.execute('INSERT INTO derived_author_labeler_day VALUES(?,?,?,?,?,?,?)',('did:old',DAY-86400,'did:l',1,1,0,1));c.commit()
    scan._rebuild_author_labeler_day(c,DAY);c.commit()
    assert rows(c)==[('did:a',DAY,'did:l',2,1,0,2),('did:b',DAY,'did:l',1,0,1,1),('did:old',DAY-86400,'did:l',1,1,0,1)]
    c.execute("DELETE FROM label_events WHERE target_did='did:b'")
    c.execute("UPDATE label_events SET neg=1 WHERE target_did='did:a'")
    c.execute("UPDATE label_events SET target_did='did:new' WHERE event_hash='two'");c.commit()
    scan._rebuild_author_labeler_day(c,DAY);c.commit()
    assert rows(c)==[('did:a',DAY,'did:l',1,0,1,1),('did:new',DAY,'did:l',1,0,1,1),('did:old',DAY-86400,'did:l',1,1,0,1)]
    c.execute('DELETE FROM label_events');c.commit()
    scan._rebuild_author_labeler_day(c,DAY);c.commit()
    assert rows(c)==[('did:old',DAY-86400,'did:l',1,1,0,1)]


def test_unchanged_day_has_zero_main_wal_frames(tmp_path):
    path=tmp_path/'state.db';c=db.connect(str(path));seed(c)
    scan._rebuild_author_labeler_day(c,DAY);c.commit()
    c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    before=rows(c)
    scan._rebuild_author_labeler_day(c,DAY);c.commit()
    assert rows(c)==before
    assert (tmp_path/'state.db-wal').stat().st_size==0
    assert c.execute('PRAGMA temp_store').fetchone()[0]==1


def test_mid_merge_failure_rolls_back_deleted_keys(tmp_path):
    class Failing(sqlite3.Connection):
        fail=False
        def execute(self,sql,*args,**kwargs):
            if self.fail and 'ON CONFLICT(author_did' in sql:
                raise sqlite3.OperationalError('fixture merge failure')
            return super().execute(sql,*args,**kwargs)
    c=sqlite3.connect(tmp_path/'state.db',factory=Failing);c.row_factory=sqlite3.Row;seed(c)
    scan._rebuild_author_labeler_day(c,DAY);c.commit();before=rows(c)
    c.execute("DELETE FROM label_events WHERE target_did='did:b'");c.commit()
    c.fail=True
    with pytest.raises(sqlite3.OperationalError):scan._rebuild_author_labeler_day(c,DAY)
    c.rollback();assert rows(c)==before
    c.fail=False;scan._rebuild_author_labeler_day(c,DAY);c.commit()
    assert len(rows(c))==1


def test_main_writer_owned_before_staging_source_read(tmp_path):
    path=tmp_path/'state.db';other=sqlite3.connect(path,timeout=0)
    class Observed(sqlite3.Connection):
        checked=False
        def execute(self,sql,*args,**kwargs):
            if 'INSERT INTO _author_labeler_day_stage' in sql:
                with pytest.raises(sqlite3.OperationalError,match='locked'):
                    other.execute("INSERT INTO meta VALUES('concurrent-fixture','1')")
                self.checked=True
            return super().execute(sql,*args,**kwargs)
    c=sqlite3.connect(path,factory=Observed);c.row_factory=sqlite3.Row;seed(c)
    scan._rebuild_author_labeler_day(c,DAY);c.commit();assert c.checked
    other.execute("INSERT INTO meta VALUES('concurrent-fixture','1')");other.commit()
