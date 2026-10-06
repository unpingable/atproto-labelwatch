"""Bounded recent-state lifecycle, distinct from production migration acceptance."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[1]/'tools/segmented_qualification'))
import pytest
import storage
from storage import custody,connect
from recent_storage import RecentStore
from recent_provider import RecentProvider

@pytest.fixture
def store(tmp_path,monkeypatch):
    root=tmp_path/'runtime';root.mkdir()
    evidence=tmp_path/'evidence';evidence.mkdir()
    import json
    (evidence/'ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive':str(root/'store/archive')}))
    monkeypatch.setattr(storage,'ROOT',root)
    return RecentStore.create(root/'store','2026-09-01T00:00:00Z')

def event(value='test',authored='1999-01-01T00:00:00Z'):
    raw={'src':'did:plc:labeler','uri':'did:plc:subject','val':value,'cts':authored}
    return ('did:plc:labeler','did:plc:labeler','did:plc:subject',None,value,0,None,None,authored,custody.canonical_identity(raw),'did:plc:subject')

def seed(s):
    return s.ingest([event()],'source','opaque1','2026-09-01T00:01:00Z')

def archive(s):
    seed(s);s.rotate('2026-10-02');s.archive('2026-09-01');s.retire('2026-09-01')

def test_observation_clock_boundary_and_idempotence(store):
    seed(store)
    assert store.ingest([event()],'source','opaque1','2026-09-01T00:01:00Z')['inserted']==0
    with pytest.raises(RuntimeError,match='different batch'):
        store.ingest([event('changed')],'source','opaque1','2026-09-01T00:01:00Z')
    with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:01:00Z','2026-09-01T00:01:00.000001Z',10) as result:
        assert len(result['rows'])==1
        assert result['rows'][0]['ts']=='1999-01-01T00:00:00Z'
    with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:00:00Z','2026-09-01T00:01:00Z',10) as result:
        assert result['rows']==[]

def test_warmup_and_explicit_gap(store):
    seed(store);store.record_gap('2026-08-31T00:00:00Z','2026-09-01T00:00:00Z','source_unavailable')
    with RecentProvider(store).snapshot('did:plc:subject','2026-08-31T00:00:00Z','2026-09-01T00:01:00.000001Z',10) as result:
        assert result['coverage']['status']=='unknown'
        assert {g['reason'] for g in result['coverage']['gaps']}=={'not_observed','source_unavailable'}

@pytest.mark.parametrize('phase',['after_recent_plan','after_recent_authority','during_recent_unlink'])
def test_interrupted_retirement_refuses_then_recovers(store,phase):
    archive(store)
    def fail(here):
        if here==phase:raise RuntimeError('interrupted')
    with pytest.raises(RuntimeError,match='interrupted'):store.maintain('2026-10-02T00:00:00Z',death=fail)
    with pytest.raises(RuntimeError,match='recovery required'):store.frontier()
    assert store.maintain('2026-10-02T00:00:00Z')['expired_owners']==['2026-09-01']
    assert list((store.root/'archive').iterdir())==[]
    with connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM custody_archives').fetchone()[0]==0
        assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]==0
        store.caps(c)

def test_evidence_dependency_preserves_owner(store):
    archive(store)
    with connect(store.state) as c:c.execute('INSERT INTO q_recent_protected VALUES (?,?)',('2026-09-01','independent review'))
    with pytest.raises(RuntimeError,match='evidence dependency'):store.maintain('2026-10-02T00:00:00Z')
    assert (store.root/'archive/2026-09-01.parquet').exists()

def test_row_counts_replace_and_transaction_refusal(store):
    with connect(store.state) as c:
        for _ in range(2):c.execute('INSERT OR REPLACE INTO posted_findings VALUES (?,?,?,?)',('x','fixture',None,'2026-09-01T00:00:00Z'))
        assert c.execute("SELECT n FROM q_recent_counts WHERE name='posted_findings'").fetchone()[0]==1
        c.execute("UPDATE q_recent_counts SET ceiling=0 WHERE name='q_recent_seen'")
    with pytest.raises(Exception,match='recent row ceiling'):seed(store)
    with connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0]==0
        assert c.execute('SELECT COUNT(*) FROM q_pending').fetchone()[0]==0

def test_payload_byte_guard(store):
    with pytest.raises(RuntimeError,match='field byte'):store.ingest([event('x'*16385)],'source','opaque1','2026-09-01T00:01:00Z')
    seed(store)
    with pytest.raises(RuntimeError,match='byte ceiling'):
        with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:00:00Z','2026-09-01T00:01:00.000001Z',10,max_bytes=1):pass

def test_partial_owner_survives_full_thirty_day_cut(store):
    seed(store);store.rotate('2026-10-01');store.archive('2026-09-01');store.retire('2026-09-01')
    store.maintain('2026-10-01T00:00:00Z')
    assert store.frontier()['start']=='2026-09-01T00:00:00.000000Z'
    assert (store.root/'archive/2026-09-01.parquet').exists()
    with RecentProvider(store).snapshot('did:plc:subject','2026-09-01T00:00:00Z','2026-09-01T00:02:00Z',10) as result:assert len(result['rows'])==1
