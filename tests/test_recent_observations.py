"""Finite public product checks; providers are released before pagination."""
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from labelwatch.recent_observations import Limits, RecentObservations, Refused, account_html

NOW = datetime(2026, 10, 6, tzinfo=timezone.utc)
START, END = "2026-10-05T00:00:00Z", "2026-10-06T00:00:00Z"
DID = "did:plc:subject"


def event(n=0, **kwargs):
    return dict(labeler_did="did:plc:labeler", uri=DID, val=f"value-{n}",
                ts="2020-01-01T00:00:00Z", observed_at=f"2026-10-05T00:00:{n:02d}Z",
                neg=0, internal_probe="PRIVATE", quarantine={"secret": True}, **kwargs)


class Provider:
    def __init__(self, rows):
        self.rows = rows
        self.leased = False
        self.coverage = {"status": "unknown", "gaps": [], "acquisition": {"schema":"labelwatch.acquisition.v1","generation_id":"12345678-1234-4234-9234-123456789012","started_at":"2026-09-01T00:00:00Z","basis":"local_acceptance_clock"}}

    @contextmanager
    def snapshot(self, did, start, end, max_rows, **budgets):
        self.leased = True
        try:
            if 'after_id' in budgets:
                after=budgets['after_id'];upper=budgets.get('upper_id')
                if upper is None:upper=len(self.rows)
                rows=[dict(row,id=n) for n,row in enumerate(self.rows,1) if after<n<=upper]
                selected=rows[:max_rows]
                yield {"rows":selected,"frontier":"fixture-frontier","coverage":self.coverage,
                       "upper_id":upper,"next_after_id":selected[-1]['id'] if selected else after,
                       "has_more":len(rows)>max_rows}
            else:
                yield {"rows": iter(self.rows), "frontier": "fixture-frontier", "coverage": self.coverage}
        finally:
            self.leased = False


def make(events, **limits):
    provider = Provider(events)
    ticks = [0]
    product = RecentObservations(provider, Limits(**limits), now=lambda: NOW,
                                 monotonic=lambda: ticks[0])
    return product, provider, ticks


def test_account_and_export_share_snapshot_after_source_mutation_and_retirement():
    product, source, _ = make([event(0), event(1)], page_rows=1)
    created = product.create(DID, START, END)
    assert not source.leased
    view = product.account(created["cursor"])
    assert view["summary"]["current_state"] == "unknown"
    assert view["summary"]["observed_events"] == 2
    source.rows.clear()  # Retirement cannot change a copied public snapshot.
    first = product.page(created["cursor"])
    second = product.page(first["next_cursor"])
    assert first["manifest"] == second["manifest"] == view["manifest"]
    assert first["rows"] + second["rows"] == view["timeline"]
    assert second["terminal"] and second["next_cursor"] is None
    assert all("internal_probe" not in r and "quarantine" not in r for r in view["timeline"])
    assert first["rows"][0]["source_timestamp"].startswith("2020")
    assert first["rows"][0]["observed_at"].startswith("2026")


def test_response_mutation_cannot_change_custodied_snapshot():
    product, _, _ = make([event()])
    created = product.create(DID, START, END)
    created["manifest"]["count"] = 90
    page = product.page(created["cursor"])
    page["rows"][0]["value"] = "changed"
    assert product.page(created["cursor"])["rows"][0]["value"] == "value-0"
    assert product.account(created["cursor"])["manifest"]["count"] == 1


def test_expiry_not_extended_by_retries_and_expired_slot_reclaimed():
    product, _, ticks = make([event()], ttl_seconds=10, snapshots=1)
    cursor = product.create(DID, START, END)["cursor"]
    ticks[0] = 9
    product.page(cursor)
    ticks[0] = 10
    with pytest.raises(Refused, match="expired"):
        product.page(cursor)
    product.create(DID, START, END)


def test_cursor_tamper_restart_and_retry_budget_refuse():
    product, _, _ = make([event()], page_requests=1)
    cursor = product.create(DID, START, END)["cursor"]
    with pytest.raises(Refused, match="invalid_cursor"):
        product.page(cursor[:-4] + "AAAA")
    other, _, _ = make([])
    with pytest.raises(Refused, match="invalid_cursor"):
        other.page(cursor)
    product.page(cursor)
    with pytest.raises(Refused, match="retry_limit"):
        product.page(cursor)


@pytest.mark.parametrize("limits,rows,reason", [
    ({"rows": 1}, [event(), event(1)], "row_limit"),
    ({"snapshot_bytes": 100}, [event()], "byte_limit"),
])
def test_materialization_caps_release_source_and_publish_no_partial(limits, rows, reason):
    product, source, _ = make(rows, **limits)
    with pytest.raises(Refused, match=reason):
        product.create(DID, START, END)
    assert not source.leased and not product._snapshots


def test_concurrency_limit_is_refusal_not_eviction():
    product, _, _ = make([event()], snapshots=1)
    cursor = product.create(DID, START, END)["cursor"]
    with pytest.raises(Refused, match="concurrency"):
        product.create(DID, START, END)
    assert product.page(cursor)["terminal"]


@pytest.mark.parametrize("start,end", [
    ("2026-09-05T00:00:00Z", END), (START, "2026-10-07T00:00:00Z"),
    (END, END), ("2026-10-05T00:00:00", END),
])
def test_window_edges_refuse_explicitly(start, end):
    product, _, _ = make([])
    with pytest.raises(Refused):
        product.create(DID, start, end)


def test_window_boundary_late_arrival_and_unknown_current_state():
    row = event()
    row["observed_at"] = "2026-09-06T00:00:00Z"
    row["ts"] = "2000-01-01T00:00:00Z"
    row["neg"] = 1
    product, _, _ = make([row])
    cursor = product.create(DID, row["observed_at"], END)["cursor"]
    view = product.account(cursor)
    assert view["summary"]["removals_observed"] == 1
    assert view["summary"]["current_state"] == "unknown"


def test_missing_observation_clock_or_outside_subject_never_inferred():
    for key, value in [("observed_at", None), ("uri", "did:plc:other"), ("observed_at", END)]:
        row = event()
        row[key] = value
        product, source, _ = make([row])
        with pytest.raises(Refused):
            product.create(DID, START, END)
        assert not source.leased


def test_filters_summary_and_html_escape_preserve_event_attribution():
    row = event()
    row.update(val="<script>bad</script>", uri="at://did:plc:subject/app.bsky.feed.post/one")
    product, _, _ = make([row, event(1)])
    cursor = product.create(DID, START, END, value=row["val"])["cursor"]
    view = product.account(cursor)
    assert view["summary"]["observed_events"] == 1
    page = account_html(view)
    assert "<script>bad" not in page and "&lt;script&gt;" in page
    assert "did:plc:labeler" in page and "Current label state is unknown" in page
    assert "Export this period" in page and "Expert observation rows" in page


def test_gap_reason_allowlist_and_coverage_are_not_silently_complete():
    product, source, _ = make([])
    source.coverage = {**source.coverage, "status": "gapped", "gaps": [{"start": START, "end": END,
                                                   "reason": "private operational detail"}]}
    cursor = product.create(DID, START, END)["cursor"]
    coverage = product.page(cursor)["manifest"]["coverage"]
    assert coverage["status"] == "gapped" and coverage["gaps"][0]["reason"] == "unknown"


def test_provider_failure_never_publishes_partial_snapshot():
    product, source, _ = make([])
    def fail():
        yield event()
        raise RuntimeError("missing archive")
    source.rows = fail()
    with pytest.raises(RuntimeError, match="missing archive"):
        product.create(DID, START, END)
    assert not product._snapshots and not source.leased


def test_action_and_target_filters_share_export_counts():
    removal = event(1)
    removal.update(neg=1, uri="at://did:plc:subject/app.bsky.feed.post/one")
    other_record = event(2)
    other_record["uri"] = "at://did:plc:subject/app.bsky.actor.profile/self"
    product, _, _ = make([event(), removal, other_record])
    cursor = product.create(DID, START, END, action="removal_observed", target_kind="post")["cursor"]
    assert product.account(cursor)["summary"]["removals_observed"] == 1
    assert product.page(cursor)["manifest"]["count"] == 1
    assert product.page(cursor)["rows"][0]["uri"] == removal["uri"]


def test_dense_period_refuses_even_if_late_filter_would_reduce_rows():
    product, source, _ = make([event(i) for i in range(12)], rows=10)
    with pytest.raises(Refused, match="row_limit"):
        product.create(DID, START, END, value="value-0")
    assert not source.leased and not product._snapshots


def test_creation_deadline_releases_snapshot_without_publication():
    product, source, ticks = make([], ttl_seconds=2)
    def rows():
        yield event()
        ticks[0] = 2
        yield event(1)
    source.rows = rows()
    with pytest.raises(Refused, match="expired"):
        product.create(DID, START, END)
    assert not source.leased and not product._snapshots


def test_actual_provider_includes_exact_observation_lower_boundary(tmp_path, monkeypatch):
    """Use real successor SQLite ownership; source time is not observation time."""
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "tools" / "segmented_qualification"))
    import storage
    from recent_storage import RecentStore
    from recent_provider import RecentProvider
    from qualify import event as stored_event
    monkeypatch.setattr(storage, "ROOT", tmp_path)
    store = RecentStore.create(tmp_path / "product-boundary", START)
    store.ingest([stored_event(timestamp="2020-01-01T00:00:00Z")], "fixture", "1", START)
    product = RecentObservations(RecentProvider(store), now=lambda: NOW)
    cursor = product.create("did:plc:fixturesubject", START, "2026-10-05T00:00:00.000001Z")["cursor"]
    answer = product.page(cursor)
    assert answer["manifest"]["count"] == 1
    assert answer["rows"][0]["observed_at"] == START
    assert answer["rows"][0]["source_timestamp"].startswith("2020")
    assert answer["manifest"]["coverage"]["status"] == "unknown"


def test_http_handler_shares_account_export_and_explains_dense_refusal(monkeypatch):
    import importlib.util
    from pathlib import Path
    from urllib.parse import urlencode
    path = Path(__file__).parents[1] / "tools" / "segmented_qualification" / "recent_product.py"
    spec = importlib.util.spec_from_file_location("recent_product_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    product, source, _ = make([event()])
    handler_class = module.handler(product)
    request = handler_class.__new__(handler_class)
    replies = []
    request.respond = lambda *args: replies.append(args)
    request.path = "/exports?" + urlencode({"did": DID, "start": START, "end": END})
    request.do_GET()
    status, created, _ = replies.pop()
    assert status == 200
    source.rows.clear()
    request.path = "/exports?" + urlencode({"cursor": created["cursor"]})
    request.do_GET()
    assert replies.pop()[1]["count"] == 1
    dense, _, _ = make([event(), event(1)], rows=1)
    handler_class = module.handler(dense)
    request = handler_class.__new__(handler_class)
    request.respond = lambda *args: replies.append(args)
    request.path = "/account?" + urlencode({"did": DID, "start": START, "end": END})
    request.do_GET()
    status, body, content_type = replies.pop()
    assert status == 409 and content_type == "text/html"
    assert "bounded paginated export" in body and "/exports?" in body
    assert "No partial account view" in body


def test_landing_defaults_use_actual_frontier_and_offer_shorter_period(monkeypatch):
    import importlib.util
    from pathlib import Path
    from types import SimpleNamespace
    path = Path(__file__).parents[1] / "tools" / "segmented_qualification" / "recent_product.py"
    spec = importlib.util.spec_from_file_location("recent_product_landing_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    product, source, _ = make([])
    source.store = SimpleNamespace(frontier=lambda: {"start": "2026-09-01T00:00:00Z",
                                                    "end": START})
    assert module.interval(product) == ("2026-09-06T00:00:00Z", START)
    assert module.interval(product, 7) == ("2026-09-29T00:00:00Z", START)
    request = module.handler(product).__new__(module.handler(product))
    rendered, is_html = request.dispatch("/", {})
    assert is_html and "Last 7 days" in rendered and "warm-up gap" in rendered
    with pytest.raises(Refused, match="invalid_period"):
        module.interval(product, 45)


def test_public_signature_projection_and_readable_newest_first_timeline():
    first=event(0,sig='public-signature')
    second=event(1);second['neg']=1;second['uri']='at://'+DID+'/app.bsky.feed.post/record'
    product,provider,_=make([first,second])
    provider.coverage={**provider.coverage,'status':'unknown','gaps':[{'start':START,'end':'2026-10-05T00:00:00.000001Z','reason':'not_observed'}]}
    created=product.create(DID,START,END);account=product.account(created['cursor'])
    assert account['timeline'][0]['sig']=='public-signature'
    html=account_html(account)
    assert html.index('Removal observed')<html.index('Application observed')
    assert 'Applications observed' in html and 'Affected subjects' in html and 'Labelers' in html
    assert 'Acquisition had not begun' in html and 'Current label state is unknown' in html
    assert '<ol class="timeline">' in html and '>Post</span>' in html and '>Account</span>' in html
    assert html.index('<ol class="timeline">')<html.index('<table>')
    assert 'normalized projection' in html and 'public-signature' in html
    assert 'Labeler DID<input' in html and 'Subject type<select' in html


def test_account_attachment_is_complete_copied_bounded_and_expires():
    product,source,ticks=make([event(),event(1)],page_rows=1)
    created=product.create(DID,START,END)
    source.rows.clear()
    body=product.download(created['cursor'])
    assert body['terminal'] and body['count']==2 and len(body['rows'])==2
    assert body['manifest']==created['manifest']
    assert '/download?' in account_html(product.account(created['cursor']))
    assert not source.leased
    ticks[0]=121
    with pytest.raises(Refused,match='expired'):product.download(created['cursor'])


def test_download_http_attachment_has_fixed_filename_and_all_rows():
    import importlib.util,io,json
    from pathlib import Path
    path=Path(__file__).parents[1]/'tools'/'segmented_qualification'/'recent_product.py'
    spec=importlib.util.spec_from_file_location('recent_download_test',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    first=event();first['val']='雪'*100
    product,_,_=make([first,event(1)],page_rows=1)
    created=product.create(DID,START,END)
    request=module.handler(product).__new__(module.handler(product))
    headers={};status=[];request.send_response=status.append
    request.send_header=lambda key,value:headers.update({key:value})
    request.end_headers=lambda:None;request.wfile=io.BytesIO()
    request.path='/download?cursor='+created['cursor'];request.do_GET()
    body=json.loads(request.wfile.getvalue())
    assert status==[200] and body['terminal'] and len(body['rows'])==2
    assert headers['Content-Disposition']=='attachment; filename="labelwatch-observations.json"'
    assert int(headers['Content-Length'])==len(request.wfile.getvalue())
    assert headers['Cache-Control']=='no-store'
    assert '雪'.encode() in request.wfile.getvalue()
