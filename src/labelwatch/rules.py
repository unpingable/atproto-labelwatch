from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from .config import Config
from .utils import format_ts


RULE_RATE_SPIKE = "label_rate_spike"
RULE_FLIP_FLOP = "flip_flop"
RULE_TARGET_CONCENTRATION = "target_concentration"
RULE_CHURN = "churn_index"


def _window_bounds(now: datetime, minutes: int) -> tuple[str, str]:
    end = now
    start = now - timedelta(minutes=minutes)
    return format_ts(start), format_ts(end)


def label_rate_spike(conn, config: Config, now: datetime) -> List[Dict]:
    alerts = []
    now = now.astimezone(timezone.utc)
    cur_start, cur_end = _window_bounds(now, config.window_minutes)
    base_start = format_ts(now - timedelta(hours=config.baseline_hours))
    base_end = cur_start

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]
        cur_count = conn.execute(
            "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=? AND ts<?",
            (labeler_did, cur_start, cur_end),
        ).fetchone()["c"]
        base_count = conn.execute(
            "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=? AND ts<?",
            (labeler_did, base_start, base_end),
        ).fetchone()["c"]

        cur_rate = cur_count / max(config.window_minutes, 1)
        base_minutes = max(int(config.baseline_hours * 60) - config.window_minutes, 1)
        base_rate = base_count / base_minutes

        triggered = False
        ratio = None
        if base_rate > 0:
            ratio = cur_rate / base_rate
            triggered = ratio >= config.spike_k
        else:
            ratio = float("inf") if cur_count else 0.0
            triggered = cur_count >= config.min_current_count

        if not triggered:
            continue

        evidence_rows = conn.execute(
            "SELECT event_hash FROM label_events WHERE labeler_did=? AND ts>=? AND ts<? LIMIT ?",
            (labeler_did, cur_start, cur_end, config.max_evidence),
        ).fetchall()
        evidence_hashes = [r["event_hash"] for r in evidence_rows]

        inputs = {
            "current_count": cur_count,
            "baseline_count": base_count,
            "current_rate_per_min": cur_rate,
            "baseline_rate_per_min": base_rate,
            "ratio": ratio,
            "window_minutes": config.window_minutes,
            "baseline_hours": config.baseline_hours,
        }
        alerts.append(
            {
                "rule_id": RULE_RATE_SPIKE,
                "labeler_did": labeler_did,
                "ts": format_ts(now),
                "inputs": inputs,
                "evidence_hashes": evidence_hashes,
            }
        )
    return alerts


def flip_flop(conn, config: Config, now: datetime) -> List[Dict]:
    alerts = []
    now = now.astimezone(timezone.utc)
    start = format_ts(now - timedelta(hours=config.flip_flop_window_hours))
    end = format_ts(now)

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]
        rows = conn.execute(
            """
            SELECT uri, val, neg, ts, event_hash
            FROM label_events
            WHERE labeler_did=? AND ts>=? AND ts<?
            ORDER BY uri, val, ts
            """,
            (labeler_did, start, end),
        ).fetchall()
        if not rows:
            continue

        grouped: Dict[tuple, List[dict]] = defaultdict(list)
        for r in rows:
            grouped[(r["uri"], r["val"])].append(
                {"neg": int(r["neg"]), "ts": r["ts"], "event_hash": r["event_hash"]}
            )

        match_hashes: List[str] = []
        flip_flop_count = 0
        for events in grouped.values():
            # find apply -> neg -> apply
            state = 0
            chain = []
            for ev in events:
                if state == 0 and ev["neg"] == 0:
                    state = 1
                    chain = [ev]
                elif state == 1 and ev["neg"] == 1:
                    state = 2
                    chain.append(ev)
                elif state == 2 and ev["neg"] == 0:
                    chain.append(ev)
                    flip_flop_count += 1
                    match_hashes.extend([c["event_hash"] for c in chain])
                    state = 0
                    chain = []
            if flip_flop_count >= config.max_events_per_scan:
                break

        if flip_flop_count == 0:
            continue
        evidence_hashes = match_hashes[: config.max_evidence]
        inputs = {
            "flip_flop_count": flip_flop_count,
            "window_hours": config.flip_flop_window_hours,
        }
        alerts.append(
            {
                "rule_id": RULE_FLIP_FLOP,
                "labeler_did": labeler_did,
                "ts": format_ts(now),
                "inputs": inputs,
                "evidence_hashes": evidence_hashes,
            }
        )
    return alerts


def target_concentration(conn, config: Config, now: datetime) -> List[Dict]:
    """HHI on target URI distribution. High HHI = fixated on few targets."""
    alerts = []
    now = now.astimezone(timezone.utc)
    start = format_ts(now - timedelta(hours=config.concentration_window_hours))
    end = format_ts(now)

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]
        rows = conn.execute(
            "SELECT uri, COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=? AND ts<? GROUP BY uri",
            (labeler_did, start, end),
        ).fetchall()
        if not rows:
            continue
        total = sum(r["c"] for r in rows)
        if total < config.concentration_min_labels:
            continue
        hhi = sum((r["c"] / total) ** 2 for r in rows)
        if hhi < config.concentration_threshold:
            continue

        top_targets = sorted(rows, key=lambda r: r["c"], reverse=True)
        evidence_rows = conn.execute(
            "SELECT event_hash FROM label_events WHERE labeler_did=? AND ts>=? AND ts<? LIMIT ?",
            (labeler_did, start, end, config.max_evidence),
        ).fetchall()
        inputs = {
            "hhi": round(hhi, 6),
            "total_labels": total,
            "unique_targets": len(rows),
            "top_target_count": top_targets[0]["c"] if top_targets else 0,
            "window_hours": config.concentration_window_hours,
        }
        alerts.append({
            "rule_id": RULE_TARGET_CONCENTRATION,
            "labeler_did": labeler_did,
            "ts": format_ts(now),
            "inputs": inputs,
            "evidence_hashes": [r["event_hash"] for r in evidence_rows],
        })
    return alerts


def churn_index(conn, config: Config, now: datetime) -> List[Dict]:
    """Jaccard distance of target sets across two adjacent half-windows."""
    alerts = []
    now = now.astimezone(timezone.utc)
    window = timedelta(hours=config.churn_window_hours)
    mid = now - window / 2
    start = now - window

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]
        first_half = conn.execute(
            "SELECT DISTINCT uri FROM label_events WHERE labeler_did=? AND ts>=? AND ts<?",
            (labeler_did, format_ts(start), format_ts(mid)),
        ).fetchall()
        second_half = conn.execute(
            "SELECT DISTINCT uri FROM label_events WHERE labeler_did=? AND ts>=? AND ts<?",
            (labeler_did, format_ts(mid), format_ts(now)),
        ).fetchall()
        set_a = {r["uri"] for r in first_half}
        set_b = {r["uri"] for r in second_half}
        union = set_a | set_b
        if len(union) < config.churn_min_targets:
            continue
        intersection = set_a & set_b
        jaccard_distance = 1.0 - (len(intersection) / len(union))
        if jaccard_distance < config.churn_threshold:
            continue

        evidence_rows = conn.execute(
            "SELECT event_hash FROM label_events WHERE labeler_did=? AND ts>=? AND ts<? LIMIT ?",
            (labeler_did, format_ts(start), format_ts(now), config.max_evidence),
        ).fetchall()
        inputs = {
            "jaccard_distance": round(jaccard_distance, 6),
            "first_half_targets": len(set_a),
            "second_half_targets": len(set_b),
            "intersection": len(intersection),
            "union": len(union),
            "window_hours": config.churn_window_hours,
        }
        alerts.append({
            "rule_id": RULE_CHURN,
            "labeler_did": labeler_did,
            "ts": format_ts(now),
            "inputs": inputs,
            "evidence_hashes": [r["event_hash"] for r in evidence_rows],
        })
    return alerts


def run_rules(conn, config: Config, now: datetime) -> List[Dict]:
    alerts = []
    alerts.extend(label_rate_spike(conn, config, now))
    alerts.extend(flip_flop(conn, config, now))
    alerts.extend(target_concentration(conn, config, now))
    alerts.extend(churn_index(conn, config, now))
    return alerts
