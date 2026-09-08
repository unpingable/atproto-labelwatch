from unittest.mock import patch

from labelwatch import db, ingest
from labelwatch.config import Config


def setup(path):
    conn = db.connect(str(path))
    db.init_db(conn)
    for did in ('did:plc:a', 'did:plc:b', 'did:plc:c'):
        conn.execute("INSERT OR IGNORE INTO labelers(labeler_did,service_endpoint,endpoint_status) VALUES(?,?,'accessible')", (did, 'https://example.test'))
    conn.commit()
    return conn


def test_budget_rotation_survives_restart(tmp_path):
    path = tmp_path / 'state.db'
    attempted = []
    for _ in range(3):
        conn = setup(path)
        clock = [0]
        def fetch(endpoint, sources, **kwargs):
            attempted.extend(sources)
            clock[0] += 2
            return {'labels': []}
        with patch.object(ingest.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(ingest, 'fetch_labels', side_effect=fetch):
            ingest.ingest_multi(conn, Config(), budget=1)
        conn.close()
    assert attempted == ['did:plc:a', 'did:plc:b', 'did:plc:c']


def test_terminal_page_reobserves_existing_cursor(tmp_path):
    conn = setup(tmp_path / 'state.db')
    did = 'did:plc:a'
    db.set_cursor(conn, did, 'prior')
    db.set_meta(conn, 'ops:cursor:observed_at:' + did, '2000-01-01T00:00:00Z')
    conn.commit()
    response = {'labels': [{'src': did, 'uri': 'at://did:plc:a/app.bsky.feed.post/a', 'val': 'test', 'ts': '2026-01-01T00:00:00Z'}]}
    with patch.object(ingest, 'fetch_labels', return_value=response):
        ingest.ingest_multi(conn, Config())
    assert db.get_cursor(conn, did) == 'prior'
    assert db.get_meta(conn, 'ops:cursor:observed_at:' + did) != '2000-01-01T00:00:00Z'


def test_rotation_handles_removed_source_and_failed_attempt(tmp_path):
    conn = setup(tmp_path / 'state.db')
    db.set_meta(conn, 'multi_ingest:last_attempted_did', 'did:plc:b')
    conn.execute("DELETE FROM labelers WHERE labeler_did='did:plc:b'")
    conn.commit()
    attempted = []
    def fetch(endpoint, sources, **kwargs):
        attempted.extend(sources)
        raise ConnectionError('fixture unavailable')
    with patch.object(ingest, 'fetch_labels', side_effect=fetch):
        ingest.ingest_multi(conn, Config())
    assert attempted == ['did:plc:c', 'did:plc:a']
    assert db.get_meta(conn, 'multi_ingest:last_attempted_did') == 'did:plc:a'


def test_count_only_weather_matches_full_summary(tmp_path):
    from labelwatch.boundary import boundary_summary_for_report, moderation_edge_count
    from labelwatch.label_family import FAMILY_VERSION
    conn = setup(tmp_path / 'state.db')
    start, end = '2026-09-01T00:00:00Z', '2026-09-08T00:00:00Z'
    cases = [
        ('spam', 'hate', start, FAMILY_VERSION, 'contradiction'),
        ('spam', 'hate', end, FAMILY_VERSION, 'contradiction'),
        ('spam', 'hate', '2026-08-31T23:59:59Z', FAMILY_VERSION, 'contradiction'),
        ('spam', 'hate', '2026-09-08T00:00:01Z', FAMILY_VERSION, 'contradiction'),
        ('spam', 'hate', start, 'old-version', 'contradiction'),
        ('spam', 'hate', start, FAMILY_VERSION, 'lead_lag'),
        (None, 'hate', start, FAMILY_VERSION, 'contradiction'),
        ('custom', 'hate', start, FAMILY_VERSION, 'contradiction'),
        ('spam', None, start, FAMILY_VERSION, 'contradiction'),
    ]
    for i, (a,b,ts,version,edge_type) in enumerate(cases):
        conn.execute('''INSERT INTO boundary_edges(edge_type,target_uri,window_start,window_end,labeler_a,labeler_b,top_family_a,top_family_b,family_version,config_hash,computed_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)''', (edge_type, f'at://fixture/{i}', start,end,'a','b',a,b,version,'fixture',ts))
    assert moderation_edge_count(conn, start,end) == 2
    assert moderation_edge_count(conn, start,end) == boundary_summary_for_report(conn,start,end)['moderation_edges']


def test_homepage_weather_deadline_is_visible_and_connection_is_closed(tmp_path):
    import time
    import sqlite3
    from labelwatch import server, frontdoor
    path = tmp_path / 'state.db'
    setup(path).close()
    connections = []
    def expensive(conn):
        connections.append(conn)
        conn.execute('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n').fetchone()
        return {'signals': ['calm']}
    started = time.monotonic()
    with patch.object(frontdoor, 'network_weather', side_effect=expensive):
        result = server._homepage_weather(str(path), budget_seconds=0.02)
    assert time.monotonic() - started < 1
    assert result == {'unavailable': True}
    html = frontdoor._render_weather_strip_html(result)
    assert 'temporarily unavailable' in html and 'calm' not in html
    try:
        connections[0].execute('SELECT 1')
    except sqlite3.ProgrammingError:
        pass
    else:
        raise AssertionError('request connection remained open')


def test_homepage_weather_zero_and_failure_are_distinct(tmp_path):
    from labelwatch import server, frontdoor
    path = tmp_path / 'state.db'
    setup(path).close()
    result = server._homepage_weather(str(path))
    assert result['events_7d_total'] == 0
    assert not result.get('unavailable')
    with patch.object(frontdoor, 'network_weather', side_effect=ValueError('fixture failure')):
        assert server._homepage_weather(str(path)) == {'unavailable': True}
