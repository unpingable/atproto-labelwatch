import sqlite3

import pytest

from labelwatch import db
from labelwatch.frontdoor import (
    _Q3_LABEL_VALUES,
    _Q8A_DISTINCT_STATES,
    _Q8B_LOCUS,
    _Q8C_LABELED_RECORDS,
)
from tools.label_events_cold_archive import export_day, reconstruct
from tools.label_events_frontdoor_archive import (
    SummaryRef,
    build_summary,
    query_subject,
    verify_summary,
)


SOURCE_SHA = "b" * 64


def test_cold_summary_reproduces_frontdoor_aggregate_shapes(tmp_path):
    source = tmp_path / "source.db"
    conn = db.connect(str(source))
    db.init_db(conn)
    subject = "did:plc:subject"
    rows = [
        ("did:plc:one", subject, subject, None, "spam", 0, None, "s1", "2026-09-01T00:00:01Z", "h1", subject),
        ("did:plc:one", subject, subject, None, "spam", 1, None, "s2", "2026-09-01T01:00:01Z", "h2", subject),
        ("did:plc:one", subject, f"at://{subject}/app.bsky.feed.post/a", None, "spam", 0, None, "s3", "2026-09-01T02:00:01Z", "h3", subject),
        ("did:plc:one", subject, f"at://{subject}/app.bsky.feed.post/a", None, "warn", 0, None, "s4", "2026-09-01T03:00:01Z", "h4", subject),
        ("did:plc:two", subject, f"at://{subject}/app.bsky.actor.profile/self", None, "warn", 0, None, "s5", "2026-09-01T04:00:01Z", "h5", subject),
        ("did:plc:one", None, "https://example.invalid", None, "other", 0, None, None, "2026-09-01T05:00:01Z", "h6", None),
    ]
    db.insert_label_events(conn, rows)
    conn.commit()
    conn.close()

    manifest = export_day(source, tmp_path / "archive", "2026-09-01", SOURCE_SHA)
    summary = tmp_path / "summary.db"
    receipt = build_summary(manifest, summary)
    assert receipt["included_events"] == 5
    assert receipt["events_without_target_did"] == 1
    restored = tmp_path / "restored.db"
    reconstruct(manifest, restored)

    actual_conn = sqlite3.connect(restored)
    actual_conn.row_factory = sqlite3.Row
    try:
        actual = {
            "label_values": [dict(row) for row in actual_conn.execute(_Q3_LABEL_VALUES, (subject,))],
            "distinct_states": [dict(row) for row in actual_conn.execute(_Q8A_DISTINCT_STATES, (subject,))],
            "loci": [dict(row) for row in actual_conn.execute(_Q8B_LOCUS, (subject,))],
            "uri_values": [dict(row) for row in actual_conn.execute(_Q8C_LABELED_RECORDS, (subject, 50))],
        }
    finally:
        actual_conn.close()
    for rows in actual.values():
        rows.sort(key=lambda row: tuple(str(value) for value in row.values()))
    observed = query_subject([SummaryRef(summary, receipt["summary_sha256"])], subject)
    for rows in observed.values():
        rows.sort(key=lambda row: tuple(str(value) for value in row.values()))
    assert observed == actual


def test_multiple_partition_summaries_merge_before_top_n(tmp_path):
    summaries = []
    subject = "did:plc:subject"
    for offset, day in enumerate(("2026-09-01", "2026-09-02")):
        source = tmp_path / f"source-{offset}.db"
        conn = db.connect(str(source))
        db.init_db(conn)
        rows = []
        for index in range(3):
            uri = f"at://{subject}/app.bsky.feed.post/{index}"
            rows.append(("did:plc:one", subject, uri, None, "spam", 0, None, None,
                         f"{day}T00:00:0{index}Z", f"h-{offset}-{index}", subject))
        if offset:
            rows.extend([("did:plc:one", subject, f"at://{subject}/app.bsky.feed.post/2", None,
                          "spam", 0, None, None, f"{day}T01:00:0{index}Z",
                          f"extra-{index}", subject) for index in range(3)])
        db.insert_label_events(conn, rows)
        conn.commit()
        conn.close()
        manifest = export_day(source, tmp_path / f"archive-{offset}", day, SOURCE_SHA)
        summary = tmp_path / f"summary-{offset}.db"
        receipt = build_summary(manifest, summary)
        summaries.append(SummaryRef(summary, receipt["summary_sha256"]))
    result = query_subject(summaries, subject, top_n=1)
    assert {row["uri"] for row in result["uri_values"]} == {
        f"at://{subject}/app.bsky.feed.post/2"
    }
    assert result["uri_values"][0]["uri_total"] == 5


def test_live_and_cold_rows_merge_to_unreduced_frontdoor_shapes(tmp_path):
    subject = "did:plc:subject"
    archived_rows = [
        ("did:plc:one", subject, f"at://{subject}/app.bsky.feed.post/a", None,
         "spam", 0, None, None, "2026-09-01T00:00:01Z", "cold-1", subject),
        ("did:plc:one", subject, f"at://{subject}/app.bsky.feed.post/b", None,
         "warn", 0, None, None, "2026-09-01T01:00:01Z", "cold-2", subject),
    ]
    live_rows = [
        ("did:plc:one", subject, f"at://{subject}/app.bsky.feed.post/a", None,
         "spam", 1, None, None, "2026-09-02T00:00:01Z", "live-1", subject),
        ("did:plc:two", subject, subject, None,
         "hide", 0, None, None, "2026-09-02T01:00:01Z", "live-2", subject),
    ]

    archive_source = tmp_path / "archive-source.db"
    conn = db.connect(str(archive_source))
    db.init_db(conn)
    db.insert_label_events(conn, archived_rows)
    conn.commit()
    conn.close()
    manifest = export_day(archive_source, tmp_path / "archive", "2026-09-01", SOURCE_SHA)
    summary = tmp_path / "summary.db"
    receipt = build_summary(manifest, summary)

    live = tmp_path / "live.db"
    live_conn = db.connect(str(live))
    db.init_db(live_conn)
    db.insert_label_events(live_conn, archived_rows + live_rows)
    live_conn.commit()
    live_conn.execute(
        "CREATE TEMP VIEW label_events AS "
        "SELECT * FROM main.label_events WHERE ts >= '2026-09-02'"
    )

    full = tmp_path / "full.db"
    full_conn = db.connect(str(full))
    db.init_db(full_conn)
    db.insert_label_events(full_conn, archived_rows + live_rows)
    full_conn.commit()
    full_conn.row_factory = sqlite3.Row
    try:
        actual = {
            "label_values": [dict(row) for row in full_conn.execute(_Q3_LABEL_VALUES, (subject,))],
            "distinct_states": [dict(row) for row in full_conn.execute(_Q8A_DISTINCT_STATES, (subject,))],
            "loci": [dict(row) for row in full_conn.execute(_Q8B_LOCUS, (subject,))],
            "uri_values": [dict(row) for row in full_conn.execute(_Q8C_LABELED_RECORDS, (subject, 50))],
        }
        observed = query_subject(
            [SummaryRef(summary, receipt["summary_sha256"])],
            subject,
            live_conn=live_conn,
        )
    finally:
        live_conn.close()
        full_conn.close()
    for rows in actual.values():
        rows.sort(key=lambda row: tuple(str(value) for value in row.values()))
    for rows in observed.values():
        rows.sort(key=lambda row: tuple(str(value) for value in row.values()))
    assert observed == actual


def test_missing_symlinked_and_checksum_mismatched_summaries_refuse(tmp_path):
    missing = tmp_path / "missing.db"
    with pytest.raises(RuntimeError, match="absent or symlinked"):
        verify_summary(SummaryRef(missing, "0" * 64))

    source_path = tmp_path / "source.db"
    conn = db.connect(str(source_path))
    db.init_db(conn)
    db.insert_label_events(conn, [
        ("did:plc:one", "did:plc:subject", "did:plc:subject", None,
         "spam", 0, None, None, "2026-09-01T00:00:01Z", "hash-one",
         "did:plc:subject"),
    ])
    conn.commit()
    conn.close()
    manifest = export_day(source_path, tmp_path / "archive", "2026-09-01", SOURCE_SHA)
    summary = tmp_path / "summary.db"
    receipt = build_summary(manifest, summary)
    link = tmp_path / "summary-link.db"
    link.symlink_to(summary)
    with pytest.raises(RuntimeError, match="absent or symlinked"):
        verify_summary(SummaryRef(link, receipt["summary_sha256"]))
    with summary.open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        verify_summary(SummaryRef(summary, receipt["summary_sha256"]))
