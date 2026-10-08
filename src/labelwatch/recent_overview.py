"""Aggregate-only estate overview for the rolling observation window.

Schema ``labelwatch.recent-overview.v1``. The envelope is composed from store
statistics gathered by the caller (tools/segmented_qualification/recent_publish.py)
and published atomically into a world-traversable directory.

Published: acquisition identity, window coverage (warm-up, recorded gaps with
reasons, source roster counts), event and labeler counts, and the top labelers
over seven days. Labeler DIDs identify public moderation services. Nothing
else carries a DID: no subject/target accounts, URIs, label values, probe,
quarantine or operational evidence. Current label state is always unknown.
"""
from __future__ import annotations

import json
import os
import re
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA = "labelwatch.recent-overview.v1"
WINDOW_DAYS = 30
MAX_GAPS = 128
MAX_TOP_LABELERS = 20
OVERVIEW_NAME = "overview.json"
GAP_REASONS = ("not_observed", "source_unavailable", "ingest_paused", "cursor_reset",
               "maintenance", "warmup", "unknown")
SOURCE_KEYS = ("seeded", "unstarted", "probing", "established", "refused", "pre_epoch_skipped",
               "undated_skipped")
COUNT_KEYS = ("events_24h", "events_7d", "events_window", "labelers_active_7d",
              "labelers_active_window")
TOP_KEYS = {"labeler_did", "count"}
ENVELOPE_KEYS = {"schema_version", "generated_at", "build_signature", "window_days",
                 "acquisition", "coverage", "counts", "top_labelers_7d", "state", "hold"}
COVERAGE_KEYS = {"status", "observed_days", "observed_through", "warmup", "gaps",
                 "gap_overflow", "sources"}
_DID = re.compile(r"did:[a-z0-9]+:[A-Za-z0-9._:%-]+")


class OverviewRefused(RuntimeError):
    """The overview cannot be published truthfully; nothing was replaced."""


def utc(value):
    if isinstance(value, datetime):
        stamp = value
    else:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise OverviewRefused("explicit UTC clock required")
    return stamp.astimezone(timezone.utc)


def iso(value):
    return utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def build_signature():
    try:
        from .utils import get_git_commit
        return get_git_commit() or "unknown"
    except Exception:
        return "unknown"


def observed_days(started_at, now, window_days=WINDOW_DAYS):
    """Whole UTC-independent days elapsed since acquisition, capped at the window."""
    elapsed = utc(now) - utc(started_at)
    if elapsed < timedelta(0):
        return 0
    return min(window_days, elapsed.days)


def coverage(stats, now, window_days=WINDOW_DAYS):
    """Window coverage from acquisition and recorded gaps; never a completeness claim."""
    now = utc(now)
    window_start = now - timedelta(days=window_days)
    started = utc(stats["acquisition"]["started_at"])
    warmup = None
    if started > window_start:
        warmup = {"start": iso(window_start), "end": iso(min(started, now)), "reason": "not_observed"}
    gaps = []
    overflow = bool(stats.get("gap_overflow"))
    for gap in stats.get("gaps") or []:
        lo, hi = max(utc(gap["start"]), window_start), min(utc(gap["end"]), now)
        if lo >= hi:
            continue
        reason = gap.get("reason") if gap.get("reason") in GAP_REASONS else "unknown"
        gaps.append({"start": iso(lo), "end": iso(hi), "reason": reason})
    gaps.sort(key=lambda g: (g["start"], g["end"], g["reason"]))
    if overflow or len(gaps) > MAX_GAPS:
        # Same disclosure the account provider uses: an overflowing gap list
        # becomes one window-wide unknown interval, never a truncated list.
        overflow = True
        gaps = [{"start": iso(window_start), "end": iso(now), "reason": "unknown"}]
    days = observed_days(started, now, window_days)
    status = "warming_up" if warmup or days < window_days else "gapped" if gaps else "no_recorded_gaps"
    sources = {key: stats.get("sources", {}).get(key) for key in SOURCE_KEYS}
    return {"status": status, "observed_days": days, "observed_through": stats.get("observed_through"),
            "warmup": warmup, "gaps": gaps, "gap_overflow": overflow, "sources": sources}


def overview(stats, now, *, signature=None, window_days=WINDOW_DAYS):
    """Compose the public envelope from caller-gathered aggregate statistics."""
    acquisition = stats["acquisition"]
    counts = {key: int(stats["counts"][key]) for key in COUNT_KEYS}
    top = sorted(((row["labeler_did"], int(row["count"])) for row in stats.get("top_labelers_7d") or []),
                 key=lambda item: (-item[1], item[0]))[:MAX_TOP_LABELERS]
    envelope = {
        "schema_version": SCHEMA,
        "generated_at": iso(now),
        "build_signature": signature or build_signature(),
        "window_days": window_days,
        "acquisition": {"generation_id": acquisition["generation_id"],
                        "started_at": iso(acquisition["started_at"]),
                        "basis": acquisition["basis"]},
        "coverage": coverage(stats, now, window_days),
        "counts": counts,
        "top_labelers_7d": [{"labeler_did": did, "count": n} for did, n in top],
        "state": {"current_label_state": "unknown"},
        "hold": False,
    }
    validate(envelope)
    return envelope


def held(now, *, signature=None, window_days=WINDOW_DAYS):
    """Explicit held artifact: a held service publishes this, never a stale reading."""
    envelope = {
        "schema_version": SCHEMA,
        "generated_at": iso(now),
        "build_signature": signature or build_signature(),
        "window_days": window_days,
        "acquisition": None,
        "coverage": {"status": "held", "observed_days": None, "observed_through": None,
                     "warmup": None, "gaps": [], "gap_overflow": False,
                     "sources": {key: None for key in SOURCE_KEYS}},
        "counts": {},
        "top_labelers_7d": [],
        "state": {"current_label_state": "unknown"},
        "hold": True,
    }
    validate(envelope)
    return envelope


def validate(envelope):
    """Refuse anything outside the declared aggregate projection."""
    if not isinstance(envelope, dict) or set(envelope) != ENVELOPE_KEYS:
        raise OverviewRefused("overview envelope fields")
    if envelope["schema_version"] != SCHEMA or envelope["window_days"] != WINDOW_DAYS:
        raise OverviewRefused("overview schema or window")
    if envelope["state"] != {"current_label_state": "unknown"}:
        raise OverviewRefused("current label state must remain unknown")
    utc(envelope["generated_at"])
    cov = envelope["coverage"]
    if not isinstance(cov, dict) or set(cov) != COVERAGE_KEYS or set(cov["sources"]) != set(SOURCE_KEYS):
        raise OverviewRefused("overview coverage fields")
    if len(cov["gaps"]) > MAX_GAPS or any(set(g) != {"start", "end", "reason"} or g["reason"] not in GAP_REASONS
                                          for g in cov["gaps"]):
        raise OverviewRefused("overview gap disclosure")
    top = envelope["top_labelers_7d"]
    if len(top) > MAX_TOP_LABELERS or any(set(row) != TOP_KEYS for row in top):
        raise OverviewRefused("overview top labelers")
    if envelope["hold"] is True:
        if envelope["counts"] != {} or top or envelope["acquisition"] is not None or cov["status"] != "held":
            raise OverviewRefused("held overview carries readings")
    elif envelope["hold"] is False:
        acquisition = envelope["acquisition"]
        if set(acquisition or {}) != {"generation_id", "started_at", "basis"} or acquisition["basis"] != "local_acceptance_clock":
            raise OverviewRefused("overview acquisition identity")
        if set(envelope["counts"]) != set(COUNT_KEYS) or any(type(v) is not int or v < 0 for v in envelope["counts"].values()):
            raise OverviewRefused("overview counts")
        if any(type(cov["sources"][k]) is not int or cov["sources"][k] < 0 for k in SOURCE_KEYS):
            raise OverviewRefused("overview source roster counts")
        if cov["warmup"] is not None and cov["status"] != "warming_up":
            raise OverviewRefused("warm-up must be disclosed as warming_up")
    else:
        raise OverviewRefused("overview hold flag")
    labelers = {row["labeler_did"] for row in top}
    found = set(_DID.findall(json.dumps(envelope)))
    if not found <= labelers:
        raise OverviewRefused("overview carries a DID outside top_labelers_7d")
    return envelope


def _traversable(path):
    """o+rx and nobody but the owner may write (a g/o-writable directory lets
    another account swap overview.json between publications)."""
    mode = os.stat(path).st_mode
    return stat.S_ISDIR(mode) and mode & 0o005 == 0o005 and mode & 0o022 == 0


def ensure_pubdir(pubdir):
    """Create the directory 0755 if absent; an existing one is never loosened silently."""
    pubdir = Path(pubdir)
    if not pubdir.exists():
        old = os.umask(0o022)
        try:
            pubdir.mkdir(mode=0o755, parents=False)
        finally:
            os.umask(old)
        os.chmod(pubdir, 0o755)
    if pubdir.is_symlink() or not pubdir.is_dir():
        raise OverviewRefused("publication directory is not a plain directory")
    return pubdir


def sweep_stale_temporaries(pubdir):
    """Remove .overview.tmp-* left by an interrupted publisher (regular files only)."""
    removed = []
    for path in Path(pubdir).glob(".overview.tmp-*"):
        if path.is_symlink() or not path.is_file():
            continue
        path.unlink(missing_ok=True)
        removed.append(path.name)
    return removed


def verify_public(pubdir, *, ancestors=True):
    """Every reader needs o+rx on the directory (and o+x on its ancestors) and o+r on the file."""
    pubdir = Path(pubdir).resolve()
    if not _traversable(pubdir):
        raise OverviewRefused(f"publication directory must be o+rx and not group/world-writable (0755): {oct(os.stat(pubdir).st_mode & 0o777)}")
    if ancestors:
        for parent in pubdir.parents:
            if os.stat(parent).st_mode & 0o001 != 0o001:
                raise OverviewRefused(f"publication ancestor is not traversable: {parent}")
    target = pubdir / OVERVIEW_NAME
    if target.exists():
        mode = os.stat(target).st_mode
        if not stat.S_ISREG(mode) or mode & 0o777 != 0o644:
            raise OverviewRefused(f"published overview mode is not 0644: {oct(mode & 0o777)}")
    return pubdir


def publish(pubdir, envelope, *, ancestors=True):
    """Atomic replace of <pubdir>/overview.json; refuse before writing if unreadable to others."""
    validate(envelope)
    pubdir = ensure_pubdir(pubdir)
    verify_public(pubdir, ancestors=ancestors)
    sweep_stale_temporaries(pubdir)
    data = json.dumps(envelope, sort_keys=True, ensure_ascii=False, indent=2).encode() + b"\n"
    tmp = pubdir / f".overview.tmp-{os.getpid()}"
    target = pubdir / OVERVIEW_NAME
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.fchmod(fd, 0o644)  # umask-independent
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        tmp.unlink(missing_ok=True)
        raise
    os.close(fd)
    try:
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    dirfd = os.open(pubdir, os.O_RDONLY)
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)
    verify_public(pubdir, ancestors=ancestors)
    return target
