"""Rolling-observatory pages over a real RecentStore; account/export routes unchanged."""
import importlib.util
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
from recent_provider import RecentProvider  # noqa: E402
from labelwatch.recent_observations import RecentObservations, coverage_statement  # noqa: E402
from test_recent_overview import store, NOW, A, SUBJECT  # noqa: E402,F401

PATH = Path(__file__).parents[1] / 'tools' / 'segmented_qualification' / 'recent_product.py'


def module():
    spec = importlib.util.spec_from_file_location('recent_product_site_test', PATH)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def get(handler_class, path):
    request = handler_class.__new__(handler_class)
    sent = {'status': None, 'headers': {}}
    request.send_response = lambda code: sent.update(status=code)
    request.send_header = lambda key, value: sent['headers'].update({key: value})
    request.end_headers = lambda: None
    request.wfile = io.BytesIO()
    request.path = path
    request.do_GET()
    return sent['status'], sent['headers'], request.wfile.getvalue()


def site(store, now=NOW, **kwargs):
    mod = module()
    product = RecentObservations(RecentProvider(store), now=lambda: now)
    return mod, mod.handler(product, **kwargs)


def test_landing_discloses_activity_epoch_coverage_gaps_roster_and_state(store):
    _, h = site(store)
    status, headers, body = get(h, '/')
    text = body.decode()
    assert status == 200 and headers['Content-Type'].startswith('text/html')
    assert "font-src 'self'" in headers['Content-Security-Policy']
    for expected in ('Recently observed activity', 'Events observed, last 24 hours', 'Events observed, last 7 days',
                     'Epoch began', 'Sep 01, 2026 at 00:00 UTC', '30 days, rolling',
                     '7 of 30 days observed since Sep 01, 2026 at 00:00 UTC',
                     'was not observed by this acquisition generation',
                     'A source was unavailable during this interval.',
                     'Labelers enrolled', 'Pre-epoch rows skipped',
                     'Observations older than 30 days expire',
                     'Labelwatch records observed application/removal events; current label state is unknown.',
                     'action="/lookup"', '/exports?did=', 'href="/about"', 'href="/previous-epoch"',
                     '/instruments/fonts/IBMPlexSans-Regular.woff2'):
        assert expected in text, expected
    assert '<strong>4</strong><span>Events observed, last 7 days' in text
    assert '<td>unknown</td>' in text  # probing/refused until the collector exposes them
    assert SUBJECT not in text


def test_about_states_verbatim_coverage_and_glossary(store):
    _, h = site(store)
    status, _, body = get(h, '/about')
    text = body.decode()
    assert status == 200
    assert coverage_statement('2026-09-01T00:00:00.000000Z', 30) in text
    assert 'Labelwatch keeps a rolling window of the most recent 30 days of public label observations.' in text
    assert 'which ended 2026-10-06, are not part of this service' in text
    for reason in ('not_observed', 'source_unavailable', 'ingest_paused', 'cursor_reset', 'maintenance', 'warmup', 'unknown', 'pre_epoch'):
        assert '<code>' + reason + '</code>' in text
    words = coverage_statement('2026-09-01T00:00:00Z').replace('&quot;', '"').split()
    assert len(words) <= 150


def test_previous_epoch_is_withdrawn_and_legacy_pages_are_not_served(store):
    _, h = site(store)
    status, _, body = get(h, '/previous-epoch')
    assert status == 200 and b'ended on 2026-10-06' in body and b'withdrawn' in body
    for legacy in ('/index.html', '/census.html', '/labeler/did-plc-x.html', '/alerts.json', '/sitemap.xml'):
        status, _, body = get(h, legacy)
        assert status == 409 and json.loads(body)['error'] == 'unknown_route'


def test_health_reports_acquisition_coverage_maintenance_and_freshness(store):
    _, h = site(store)
    status, headers, body = get(h, '/health')
    health = json.loads(body)
    assert status == 200 and headers['Cache-Control'] == 'no-store'
    assert health['acquisition']['started_at'] == '2026-09-01T00:00:00.000000Z'
    assert health['coverage']['gaps_count'] == 1 and health['coverage']['observed_days'] == 7
    assert health['last_maintenance'] == {'generation': 1, 'at': '2026-09-02T00:00:00.000000Z'}
    # Last acceptance 2026-09-02T12:00Z is six days before NOW: collection is stale.
    assert health['freshness']['state'] == 'stale' and health['status'] == 'degraded'
    _, fresh = site(store, now=NOW.replace(day=2, hour=12, minute=5))
    health = json.loads(get(fresh, '/health')[2])
    assert health['freshness']['state'] == 'fresh' and health['status'] == 'ok'


def test_unavailable_summary_is_disclosed_not_zero():
    mod = module()
    from test_recent_observations import make
    product, source, _ = make([])
    source.store = SimpleNamespace(frontier=lambda: {'start': '2026-09-01T00:00:00Z', 'end': '2026-10-06T00:00:00Z'})
    h = mod.handler(product)
    status, _, body = get(h, '/')
    assert status == 200 and b'Observation summary unavailable' in body and b'absence of evidence' in body
    status, _, body = get(h, '/health')
    assert status == 503 and json.loads(body)['status'] == 'unavailable'


def test_lookup_routes_dids_and_handles_to_unchanged_account_view(store):
    _, h = site(store)
    status, headers, _ = get(h, '/lookup?account=did%3Aplc%3Aprivatesubject&days=7')
    assert status == 303 and headers['Location'] == '/account?did=did%3Aplc%3Aprivatesubject&days=7'
    status, _, body = get(h, '/lookup?account=someone.bsky.social')
    assert status == 409 and b'Handle lookup is not enabled' in body
    status, _, body = get(h, '/lookup?account=not+a+handle')
    assert status == 409 and b'not an account DID or handle' in body
    status, _, _ = get(h, '/lookup?account=did%3Aplc%3Ax&days=45')
    assert status == 409
    _, resolving = site(store, resolver=lambda handle: SUBJECT if handle == 'someone.bsky.social' else None)
    status, headers, _ = get(resolving, '/lookup?account=%40someone.bsky.social&days=30')
    assert status == 303 and headers['Location'] == '/account?did=did%3Aplc%3Aprivatesubject&days=30'
    status, _, body = get(resolving, '/lookup?account=nobody.example')
    assert status == 409 and b'could not be resolved' in body
    # /account keeps refusing parameters it never accepted.
    status, _, body = get(h, '/account?account=x&did=' + SUBJECT)
    assert status == 409 and json.loads(body)['error'] == 'unknown_parameter'


def test_account_view_over_real_store_still_serves_observed_events(store):
    _, h = site(store, now=NOW.replace(day=2, hour=13))
    status, _, body = get(h, '/account?did=' + SUBJECT + '&days=7')
    assert status == 200 and b'Current label state is unknown' in body and A.encode() in body


def test_vendored_fonts_only(store):
    _, h = site(store)
    status, headers, body = get(h, '/instruments/fonts/IBMPlexSans-Regular.woff2')
    assert status == 200 and headers['Content-Type'] == 'font/woff2' and body[:4] == b'wOF2'
    status, _, _ = get(h, '/instruments/fonts/OFL-IBM-Plex.txt')
    assert status == 409
