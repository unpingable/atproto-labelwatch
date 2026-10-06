"""Recorded public signatures remain bounded export evidence, not secrets."""
from datetime import datetime,timezone
from pathlib import Path
from labelwatch.ingest import normalize_label
from labelwatch.recent_observations import RecentObservations,Limits


def test_real_provider_exports_recorded_signature_with_adaptive_bytes(tmp_path,monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'tools'/'segmented_qualification'))
    import storage
    from recent_storage import RecentStore
    from recent_provider import RecentProvider
    monkeypatch.setattr(storage,'ROOT',tmp_path)
    start='2026-10-05T00:00:00Z';end='2026-10-05T00:00:00.000001Z';did='did:plc:subject'
    store=RecentStore.create(tmp_path/'store',start)
    signature='s'*16000
    rows=[]
    for n in range(20):
        r=normalize_label({'src':'did:plc:source','uri':did,'val':str(n)+'x'*8000,'cts':'2020-01-01T00:00:00Z','sig':{'$bytes':signature}},strict_identity=True)
        rows.append((r.labeler_did,r.src,r.uri,r.cid,r.val,r.neg,r.exp,r.sig,r.ts,r.event_hash,None))
    store.ingest(rows,'fixture','one',start)
    product=RecentObservations(RecentProvider(store),Limits(snapshot_bytes=100000),now=lambda:datetime(2026,10,6,tzinfo=timezone.utc))
    token=product.create_export(did,start,end)['cursor'];seen=[]
    while token:
        reply=product.page(token)
        assert 0<len(reply['rows'])<=2
        assert all(row['sig']==signature for row in reply['rows'])
        assert all('event_hash' not in row and 'id' not in row for row in reply['rows'])
        seen.extend(reply['rows']);token=reply['next_cursor']
    assert len(seen)==20 and reply['count']==20 and reply['terminal']
