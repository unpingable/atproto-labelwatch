import pytest

from labelwatch import db, retention
from labelwatch.frontdoor import lookup_subject
from labelwatch.frontdoor_archive import ColdMergeIncomplete, load_catalog, query_subject
from _cold_catalog_fixture import build_test_catalog


def test_pruned_uri_with_live_contributions_refuses_instead_of_undercounting(tmp_path):
    subject, labeler = "did:plc:subject", "did:plc:labeler"
    rows = [(labeler, f"at://{subject}/app.bsky.feed.post/{i}", "spam", 0,
             "2026-08-13T12:00:00Z", subject) for i in range(51)]
    manifest = build_test_catalog(tmp_path / "catalog", rows,
                                  start_day="2026-08-13", end_day_exclusive="2026-08-14",
                                  watermark=51, uri_top_n=50)
    cat = load_catalog(manifest, filesystem_type="ext4")
    conn = db.connect(str(tmp_path / "live.db"))
    db.init_db(conn)
    retention.set_retention_floor(conn, "2026-08-14T00:00:00Z", None)
    db.set_meta(conn, retention.RETENTION_HISTORY_START_KEY, "2026-08-13T00:00:00Z")
    # Cold-only top 50 is still exact. The omitted URI then gains live rows.
    assert len(query_subject(cat, subject, live_conn=conn)["uri_values"]) == 50
    conn.execute("INSERT OR REPLACE INTO sqlite_sequence(name,seq) VALUES('label_events',51)")
    db.insert_label_events(conn, [(labeler, subject,
        f"at://{subject}/app.bsky.feed.post/9", None, "spam", 0, None, None,
        "2026-08-14T12:00:00Z", "live", subject)])
    conn.commit()
    with pytest.raises(ColdMergeIncomplete, match="cold_uri_pruning_cross_boundary"):
        query_subject(cat, subject, live_conn=conn)
    result = lookup_subject(conn, subject,
        audit_receipt={"overall_verdict": "admissible", "generated_at": "2026-10-03T00:00:00Z"},
        cold_catalog=cat)
    assert result.refusal == "cold_history_unavailable"
    assert result.labelers == []
    assert result.cold_history["coverage"]["reason"] == "cold_uri_pruning_cross_boundary"
    conn.close()
