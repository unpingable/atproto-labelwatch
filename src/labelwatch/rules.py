from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from .config import Config
from .utils import format_ts


RULE_RATE_SPIKE = "label_rate_spike"
RULE_FLIP_FLOP = "flip_flop"


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


def run_rules(conn, config: Config, now: datetime) -> List[Dict]:
    alerts = []
    alerts.extend(label_rate_spike(conn, config, now))
    alerts.extend(flip_flop(conn, config, now))
    return alerts
