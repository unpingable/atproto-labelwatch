"""Dense equal-time exports keep a fixed ID frontier without source leases."""
from contextlib import contextmanager
from datetime import datetime,timezone
import pytest
from labelwatch.recent_observations import RecentObservations,Limits,Refused

DID='did:plc:subject'
NOW=datetime(2026,10,6,tzinfo=timezone.utc)
START='2026-10-05T00:00:00Z'
END='2026-10-05T00:00:00.000001Z'


def row(n):
    return {'id':n,'labeler_did':'did:plc:source','uri':DID,'val':str(n),
            'ts':'2020-01-01T00:00:00Z','observed_at':START,'neg':0}


class Provider:
    def __init__(self,count):self.rows=[row(n) for n in range(1,count+1)];self.retired=False;self.leased=False
    @contextmanager
    def snapshot(self,did,start,end,max_rows,*,after_id=None,upper_id=None,**kwargs):
        if self.retired:raise RuntimeError('retired required range')
        self.leased=True
        try:
            upper=max(r['id'] for r in self.rows) if upper_id is None else upper_id
            rows=[r for r in self.rows if after_id<r['id']<=upper]
            selected=rows[:max_rows]
            yield {'rows':selected,'upper_id':upper,'next_after_id':selected[-1]['id'] if selected else after_id,
                   'has_more':len(rows)>max_rows,'coverage':{'status':'unknown','gaps':[], 'acquisition':{'schema':'labelwatch.acquisition.v1','generation_id':'12345678-1234-4234-9234-123456789012','started_at':'2026-09-01T00:00:00Z','basis':'local_acceptance_clock'}},'frontier':'fixed-test-source'}
        finally:self.leased=False


def test_dense_equal_timestamp_export_excludes_simultaneous_append():
    provider=Provider(1101);product=RecentObservations(provider,now=lambda:NOW)
    created=product.create_export(DID,START,END)
    provider.rows.append(row(1102))
    token=created['cursor'];rows=[];manifests=[]
    while token:
        response=product.page(token);rows.extend(response['rows']);manifests.append(response['manifest'])
        assert not provider.leased
        token=response['next_cursor']
    assert len(rows)==1101 and len({r['value'] for r in rows})==1101
    assert response['count']==1101 and response['content_sha256']
    assert all(m==created['manifest'] for m in manifests)


def test_retirement_expires_incomplete_export_without_terminal_success():
    provider=Provider(1101);product=RecentObservations(provider,now=lambda:NOW)
    created=product.create_export(DID,START,END);first=product.page(created['cursor'])
    provider.retired=True
    with pytest.raises(Refused,match='expired'):product.page(first['next_cursor'])
    with pytest.raises(Refused,match='expired'):product.page(created['cursor'])


def test_progress_is_not_retry_budget_but_retries_and_ttl_are_finite():
    provider=Provider(1101);ticks=[0]
    product=RecentObservations(provider,Limits(page_requests=1),now=lambda:NOW,monotonic=lambda:ticks[0])
    created=product.create_export(DID,START,END);first=product.page(created['cursor'])
    assert product.page(created['cursor'])==first
    second=product.page(first['next_cursor'])
    with pytest.raises(Refused,match='retry'):product.page(first['next_cursor'])
    created=product.create_export(DID,START,END);ticks[0]=121
    with pytest.raises(Refused,match='expired'):product.page(created['cursor'])


def test_oversize_page_refused_and_no_partial_terminal():
    provider=Provider(2);provider.rows[0]['val']='x'*10000
    product=RecentObservations(provider,Limits(snapshot_bytes=3000),now=lambda:NOW)
    with pytest.raises(Refused,match='byte_limit'):product.create_export(DID,START,END)
    assert not product._exports


def test_real_dense_store_fixed_frontier_and_byte_prefix(tmp_path,monkeypatch):
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'tools'/'segmented_qualification'))
    import storage
    from recent_storage import RecentStore
    from recent_provider import RecentProvider
    from labelwatch.ingest import normalize_label
    import json
    runtime=tmp_path/'runtime';runtime.mkdir()
    evidence=tmp_path/'evidence';evidence.mkdir()
    (evidence/'ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive':str(runtime/'store/archive')}))
    monkeypatch.setattr(storage,'ROOT',runtime)
    store=RecentStore.create(runtime/'store',START)
    def stored(n,large=False):
        r=normalize_label({'src':'did:plc:source','uri':DID,'val':str(n)+('x'*8000 if large else ''),'cts':'2020-01-01T00:00:00Z'},strict_identity=True)
        return (r.labeler_did,r.src,r.uri,r.cid,r.val,r.neg,r.exp,r.sig,r.ts,r.event_hash,None)
    import time,json
    store.ingest([stored(n) for n in range(10000)],'fixture','one',START)
    provider=RecentProvider(store);product=RecentObservations(provider,now=lambda:NOW)
    with pytest.raises(Refused,match='row_limit'):product.create(DID,START,END)
    started=time.monotonic()
    created=product.create_export(DID,START,END)
    store.ingest([stored(10001)],'fixture','two',START)
    token=created['cursor'];values=[]
    while token:
        response=product.page(token);values.extend(r['value'] for r in response['rows']);token=response['next_cursor']
    elapsed=time.monotonic()-started
    assert len(values)==10000 and '10001' not in values
    (tmp_path/'MEASUREMENTS.json').write_text(json.dumps({'records':10000,'identical_observation_time':START,'elapsed_seconds':elapsed,'ttl_seconds':product.limits.ttl_seconds,'scope':'local active SQLite fixture; not production network throughput'})+'\n')
    store.ingest([stored(n,True) for n in range(11000,11100)],'fixture','three',START)
    with provider.snapshot(DID,START,END,100,after_id=10001,upper_id=None,max_bytes=20000) as source:
        assert 0<len(source['rows'])<100 and source['has_more']
        assert source['next_after_id']==source['rows'][-1]['id']

    # Entire mixed-size dataset remains exportable: valid large rows produce
    # smaller positive pages, never a successful truncated export.
    large=RecentObservations(provider,Limits(page_rows=100,snapshot_bytes=100000),now=lambda:NOW)
    made=large.create_export(DID,START,END);token=made['cursor'];count=0;small_positive=False
    while token:
        reply=large.page(token);count+=len(reply['rows'])
        if any(len(r['value'])>8000 for r in reply['rows']):
            assert 0<len(reply['rows'])<100
            small_positive=True
        assert len(json.dumps(reply,separators=(',',':')).encode())<=100000
        token=reply['next_cursor']
    assert count==10101 and small_positive
    created=product.create_export(DID,START,END);first=product.page(created['cursor'])
    store.advance_day('2026-11-06T00:00:00Z')
    with pytest.raises(Refused,match='expired'):product.page(first['next_cursor'])
