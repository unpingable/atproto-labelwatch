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
RULE_DATA_GAP = "data_gap"

# Rate-based rules: suppressed when warmup state is "sparse"
_RATE_BASED_RULES = {RULE_RATE_SPIKE, RULE_CHURN}


def _window_bounds(now: datetime, minutes: int) -> tuple[str, str]:
    end = now
    start = now - timedelta(minutes=minutes)
    return format_ts(start), format_ts(end)


def _is_reference_labeler(conn, labeler_did: str) -> bool:
    """Check if a labeler is marked as reference in the DB."""
    row = conn.execute(
        "SELECT is_reference FROM labelers WHERE labeler_did=?", (labeler_did,)
    ).fetchone()
    return bool(row and row["is_reference"])


def _labeler_age_hours(conn, labeler_did: str) -> float:
    """Hours since labeler was first seen."""
    row = conn.execute(
        "SELECT first_seen FROM labelers WHERE labeler_did=?", (labeler_did,)
    ).fetchone()
    if not row or not row["first_seen"]:
        return 0.0
    from .utils import parse_ts
    first = parse_ts(row["first_seen"])
    if first.tzinfo is None:
        first = first.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - first).total_seconds() / 3600
    return max(0.0, age)


def _total_events(conn, labeler_did: str, _cache: dict | None = None) -> int:
    """Total label events ever recorded for a labeler."""
    if _cache is not None and labeler_did in _cache:
        return _cache[labeler_did]
    return conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=?", (labeler_did,)
    ).fetchone()["c"]


def _build_event_count_cache(conn) -> dict[str, int]:
    """One query: per-labeler total event counts."""
    rows = conn.execute(
        "SELECT labeler_did, COUNT(*) AS c FROM label_events GROUP BY labeler_did"
    ).fetchall()
    return {r["labeler_did"]: r["c"] for r in rows}


def _build_coverage_cache(conn, now: datetime, config: Config) -> dict[str, dict]:
    """One query: per-labeler coverage stats over the coverage window.

    Returns {did: {"ratio": float, "attempts": int, "successes": int, "sufficient": bool}}.
    If no ingest_outcomes table exists (pre-migration), returns empty dict.
    """
    window_start = format_ts(now - timedelta(minutes=config.coverage_window_minutes))
    try:
        rows = conn.execute(
            """SELECT labeler_did,
                      COUNT(*) AS attempts,
                      SUM(CASE WHEN outcome IN ('success','empty') THEN 1 ELSE 0 END) AS successes
               FROM ingest_outcomes WHERE ts >= ? GROUP BY labeler_did""",
            (window_start,),
        ).fetchall()
    except Exception:
        return {}
    cache: dict[str, dict] = {}
    for r in rows:
        attempts = r["attempts"]
        successes = r["successes"]
        ratio = successes / attempts if attempts > 0 else 0.0
        cache[r["labeler_did"]] = {
            "ratio": ratio,
            "attempts": attempts,
            "successes": successes,
            "sufficient": ratio >= config.coverage_threshold,
        }
    return cache


def _confidence_tag(conn, config: Config, labeler_did: str,
                    _cache: dict | None = None) -> str:
    """Return 'high' or 'low' confidence based on event count and age."""
    total = _total_events(conn, labeler_did, _cache)
    age = _labeler_age_hours(conn, labeler_did)
    if total >= config.confidence_min_events and age >= config.confidence_min_age_hours:
        return "high"
    return "low"


def _warmup_state(conn, config: Config, labeler_did: str,
                  _cache: dict | None = None) -> str:
    """Determine warmup state for a labeler.

    Returns:
        "ready" — mature enough for full alerting
        "warming_up" — recent labeler, not enough history yet
        "sparse" — age threshold met but event volume too low for rate-based rules
    """
    if not config.warmup_enabled:
        return "ready"

    row = conn.execute(
        "SELECT first_seen, scan_count FROM labelers WHERE labeler_did=?",
        (labeler_did,),
    ).fetchone()
    if not row or not row["first_seen"]:
        return "warming_up"

    from .utils import parse_ts
    first = parse_ts(row["first_seen"])
    if first.tzinfo is None:
        first = first.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (datetime.now(timezone.utc) - first).total_seconds() / 3600)
    scan_count = row["scan_count"] or 0

    total = _total_events(conn, labeler_did, _cache)

    # Check minimum age
    if age_hours < config.warmup_min_age_hours:
        return "warming_up"

    # Check minimum scans
    if scan_count < config.warmup_min_scans:
        return "warming_up"

    # Age met — check if volume is too low for rate-based rules
    if total < config.warmup_min_events:
        return "sparse"

    return "ready"


def _should_suppress(warmup: str, rule_id: str, config: Config) -> bool:
    """Decide whether to suppress an alert based on warmup state."""
    if warmup == "ready":
        return False
    if warmup == "warming_up" and config.warmup_suppress_alerts:
        return True
    if warmup == "sparse" and rule_id in _RATE_BASED_RULES:
        return True
    return False


def label_rate_spike(conn, config: Config, now: datetime,
                     _cache: dict | None = None,
                     _cov_cache: dict | None = None) -> List[Dict]:
    alerts = []
    now = now.astimezone(timezone.utc)
    cur_start, cur_end = _window_bounds(now, config.window_minutes)
    base_start = format_ts(now - timedelta(hours=config.baseline_hours))
    base_end = cur_start

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]

        cov = (_cov_cache or {}).get(labeler_did, {"sufficient": True})
        if not cov["sufficient"]:
            continue

        warmup = _warmup_state(conn, config, labeler_did, _cache)
        if _should_suppress(warmup, RULE_RATE_SPIKE, config):
            continue

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

        # Two-tier threshold: reference labelers use spike_min_count_reference,
        # others use spike_min_count_default
        is_ref = _is_reference_labeler(conn, labeler_did)
        min_count = config.spike_min_count_reference if is_ref else config.spike_min_count_default

        triggered = False
        ratio = None
        if base_rate > 0:
            ratio = cur_rate / base_rate
            triggered = ratio >= config.spike_k
        else:
            ratio = float("inf") if cur_count else 0.0
            triggered = cur_count >= min_count

        if not triggered:
            continue

        evidence_rows = conn.execute(
            "SELECT event_hash FROM label_events WHERE labeler_did=? AND ts>=? AND ts<? LIMIT ?",
            (labeler_did, cur_start, cur_end, config.max_evidence),
        ).fetchall()
        evidence_hashes = [r["event_hash"] for r in evidence_rows]

        confidence = _confidence_tag(conn, config, labeler_did, _cache)

        inputs = {
            "current_count": cur_count,
            "baseline_count": base_count,
            "current_rate_per_min": cur_rate,
            "baseline_rate_per_min": base_rate,
            "ratio": ratio,
            "window_minutes": config.window_minutes,
            "baseline_hours": config.baseline_hours,
            "is_reference": is_ref,
            "min_current_count_used": min_count,
            "confidence": confidence,
        }
        if warmup != "ready":
            inputs["warmup"] = warmup
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


def flip_flop(conn, config: Config, now: datetime,
              _cache: dict | None = None,
              _cov_cache: dict | None = None) -> List[Dict]:
    alerts = []
    now = now.astimezone(timezone.utc)
    start = format_ts(now - timedelta(hours=config.flip_flop_window_hours))
    end = format_ts(now)

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]

        cov = (_cov_cache or {}).get(labeler_did, {"sufficient": True})
        if not cov["sufficient"]:
            continue

        warmup = _warmup_state(conn, config, labeler_did, _cache)
        if _should_suppress(warmup, RULE_FLIP_FLOP, config):
            continue

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
        confidence = _confidence_tag(conn, config, labeler_did, _cache)
        inputs = {
            "flip_flop_count": flip_flop_count,
            "window_hours": config.flip_flop_window_hours,
            "confidence": confidence,
        }
        if warmup != "ready":
            inputs["warmup"] = warmup
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


def target_concentration(conn, config: Config, now: datetime,
                         _cache: dict | None = None,
                         _cov_cache: dict | None = None) -> List[Dict]:
    """HHI on target URI distribution. High HHI = fixated on few targets."""
    alerts = []
    now = now.astimezone(timezone.utc)
    start = format_ts(now - timedelta(hours=config.concentration_window_hours))
    end = format_ts(now)

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]

        cov = (_cov_cache or {}).get(labeler_did, {"sufficient": True})
        if not cov["sufficient"]:
            continue

        warmup = _warmup_state(conn, config, labeler_did, _cache)
        if _should_suppress(warmup, RULE_TARGET_CONCENTRATION, config):
            continue

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
        confidence = _confidence_tag(conn, config, labeler_did, _cache)
        inputs = {
            "hhi": round(hhi, 6),
            "total_labels": total,
            "unique_targets": len(rows),
            "top_target_count": top_targets[0]["c"] if top_targets else 0,
            "window_hours": config.concentration_window_hours,
            "confidence": confidence,
        }
        if warmup != "ready":
            inputs["warmup"] = warmup
        alerts.append({
            "rule_id": RULE_TARGET_CONCENTRATION,
            "labeler_did": labeler_did,
            "ts": format_ts(now),
            "inputs": inputs,
            "evidence_hashes": [r["event_hash"] for r in evidence_rows],
        })
    return alerts


def churn_index(conn, config: Config, now: datetime,
                _cache: dict | None = None,
                _cov_cache: dict | None = None) -> List[Dict]:
    """Jaccard distance of target sets across two adjacent half-windows."""
    alerts = []
    now = now.astimezone(timezone.utc)
    window = timedelta(hours=config.churn_window_hours)
    mid = now - window / 2
    start = now - window

    labelers = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]

        cov = (_cov_cache or {}).get(labeler_did, {"sufficient": True})
        if not cov["sufficient"]:
            continue

        warmup = _warmup_state(conn, config, labeler_did, _cache)
        if _should_suppress(warmup, RULE_CHURN, config):
            continue

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
        confidence = _confidence_tag(conn, config, labeler_did, _cache)
        inputs = {
            "jaccard_distance": round(jaccard_distance, 6),
            "first_half_targets": len(set_a),
            "second_half_targets": len(set_b),
            "intersection": len(intersection),
            "union": len(union),
            "window_hours": config.churn_window_hours,
            "confidence": confidence,
        }
        if warmup != "ready":
            inputs["warmup"] = warmup
        alerts.append({
            "rule_id": RULE_CHURN,
            "labeler_did": labeler_did,
            "ts": format_ts(now),
            "inputs": inputs,
            "evidence_hashes": [r["event_hash"] for r in evidence_rows],
        })
    return alerts


def data_gap(conn, config: Config, now: datetime,
             _cov_cache: dict | None = None) -> List[Dict]:
    """Emit alerts for labelers with insufficient ingest coverage."""
    alerts = []
    if not _cov_cache:
        return alerts

    now = now.astimezone(timezone.utc)

    labelers = conn.execute("SELECT labeler_did, first_seen, scan_count FROM labelers").fetchall()
    for row in labelers:
        labeler_did = row["labeler_did"]
        cov = _cov_cache.get(labeler_did)
        if cov is None:
            continue
        if cov["sufficient"]:
            continue

        # Skip labelers still in warmup
        warmup = _warmup_state(conn, config, labeler_did)
        if warmup == "warming_up":
            continue

        # Get last success/attempt timestamps
        last_success_row = conn.execute(
            "SELECT MAX(ts) AS ts FROM ingest_outcomes WHERE labeler_did=? AND outcome IN ('success','empty')",
            (labeler_did,),
        ).fetchone()
        last_attempt_row = conn.execute(
            "SELECT MAX(ts) AS ts FROM ingest_outcomes WHERE labeler_did=?",
            (labeler_did,),
        ).fetchone()

        alerts.append({
            "rule_id": RULE_DATA_GAP,
            "labeler_did": labeler_did,
            "ts": format_ts(now),
            "inputs": {
                "coverage_ratio": round(cov["ratio"], 4),
                "coverage_attempts": cov["attempts"],
                "coverage_successes": cov["successes"],
                "coverage_threshold": config.coverage_threshold,
                "last_success_ts": last_success_row["ts"] if last_success_row else None,
                "last_attempt_ts": last_attempt_row["ts"] if last_attempt_row else None,
            },
            "evidence_hashes": [],
        })
    return alerts


def run_rules(conn, config: Config, now: datetime) -> List[Dict]:
    # Pre-compute per-labeler event counts once (1 query instead of ~1600)
    cache = _build_event_count_cache(conn)
    cov_cache = _build_coverage_cache(conn, now, config)
    alerts = []
    alerts.extend(label_rate_spike(conn, config, now, cache, cov_cache))
    alerts.extend(flip_flop(conn, config, now, cache, cov_cache))
    alerts.extend(target_concentration(conn, config, now, cache, cov_cache))
    alerts.extend(churn_index(conn, config, now, cache, cov_cache))
    alerts.extend(data_gap(conn, config, now, cov_cache))
    return alerts
