"""Create one empty recent store under the admitted root and seed its roster.

Seeded DIDs carry no endpoint and no cursor: their coverage is unknown until
the collector bootstraps each one. Nothing here enables a unit, touches a hold
or activation marker, or imports legacy history.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import sqlite3
import subprocess
import sys
from contextlib import closing
from importlib import metadata

import storage
from recent_storage import RecentStore, clock, iso
from labelwatch.ingest import _is_valid_did
from labelwatch.utils import get_git_commit

ROSTER_SCHEMA = 'labelwatch.roster-seed.v1'


def load_roster(path):
    raw = open(path, 'rb').read()
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get('schema') != ROSTER_SCHEMA or not isinstance(value.get('dids'), list):
        raise ValueError('roster seed schema')
    dids = value['dids']
    if value.get('count') != len(dids) or len(set(dids)) != len(dids):
        raise ValueError('roster seed count or duplicate DID')
    if any(not isinstance(d, str) or not _is_valid_did(d) for d in dids):
        raise ValueError('roster seed invalid DID')
    return dids, hashlib.sha256(raw).hexdigest()


def source_sha():
    try:
        sha = subprocess.run(['git', '-C', str(storage.REPO), 'rev-parse', 'HEAD'], capture_output=True, text=True,
                             timeout=10, check=True).stdout.strip()
        dirty = subprocess.run(['git', '-C', str(storage.REPO), 'status', '--porcelain'],
                               capture_output=True, text=True, timeout=10, check=True).stdout.strip() != ''
        return {'sha': sha, 'dirty': dirty, 'basis': 'git'}
    except (OSError, subprocess.SubprocessError):
        return {'sha': get_git_commit(), 'dirty': None, 'basis': 'deploy_marker'}


def versions():
    found = {'python': platform.python_version(), 'sqlite': sqlite3.sqlite_version}
    for name in ('pyarrow', 'duckdb'):
        try: found[name] = metadata.version(name)
        except metadata.PackageNotFoundError: found[name] = None
    return found


MAX_SKEW_SECONDS = 300


def acceptance_clock(now, allow_skew_seconds=None, wall=None):
    """The epoch start must be the real wall clock.

    A backdated start would erase warm-up and claim a full observed window.
    Wider skew needs an explicit operator bound, recorded in the receipt.
    """
    stamp = clock(now)
    wall = wall or dt.datetime.now(dt.timezone.utc)
    skew = (stamp - wall).total_seconds()
    if allow_skew_seconds is not None and (type(allow_skew_seconds) is not int or allow_skew_seconds < 0):
        raise ValueError('clock skew allowance must be a non-negative integer')
    bound = MAX_SKEW_SECONDS if allow_skew_seconds is None else allow_skew_seconds
    if abs(skew) > bound:
        raise ValueError('acceptance clock %.3f s from wall clock exceeds %d s' % (skew, bound))
    return {'requested': iso(stamp), 'wall': iso(wall), 'skew_seconds': skew,
            'allowed_skew_seconds': bound, 'explicit_allowance': allow_skew_seconds is not None}


def init(root, now, days=30, roster=None, allow_skew_seconds=None, wall=None):
    root = storage.owned(root)
    if days != 30: raise ValueError('only 30-day candidate admitted')
    acceptance = acceptance_clock(now, allow_skew_seconds, wall)
    # The policy must hold now, not merely parse, before any effect.
    storage.admit_reserves(storage.reserve_policy(root), 0, (root.parent,), 'shared_reserve_unavailable')
    dids, roster_sha = load_roster(roster) if roster else ([], None)
    if root.exists():
        if not root.is_dir() or any(root.iterdir()): raise RuntimeError('store root exists and is not empty')
        root.rmdir()  # Store.create owns the mkdir; an empty placeholder carries nothing.
    store = RecentStore.create(root, now, days)
    if dids: store.remember_sources([{'did': d} for d in dids], limit=2048)
    with closing(storage.connect(store.state, readonly=True)) as c:
        acquisition = RecentStore.acquisition(c)
        policy = json.loads(storage.db.get_meta(c, storage.RESERVES_KEY))
        seeded = c.execute('SELECT COUNT(*) FROM q_recent_sources WHERE endpoint IS NULL AND provider_cursor IS NULL').fetchone()[0]
    if seeded != len(dids): raise RuntimeError('roster seed count mismatch after enrollment')
    return {'schema': 'labelwatch.recent-init-receipt.v1', 'store': str(root),
            'generation_id': acquisition['generation_id'], 'started_at': acquisition['started_at'],
            'basis': acquisition['basis'], 'days': days,
            'roster': {'path': os.path.abspath(roster) if roster else None, 'sha256': roster_sha,
                       'count': len(dids), 'cursors': 'none', 'coverage': 'unknown_until_bootstrap'},
            'acceptance_clock': acceptance, 'reserves': policy, 'source': source_sha(), 'versions': versions(), 'created_at': iso(clock(now))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, help='new store directory, a strict child of the admitted root')
    parser.add_argument('--now', required=True, help='explicit UTC acceptance clock, e.g. 2026-10-08T00:00:00Z')
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--roster', help='labelwatch.roster-seed.v1 JSON')
    parser.add_argument('--allow-clock-skew-seconds', type=int, default=None,
                        help='explicitly admit |now - wall clock| up to N s (default bound 300 s); recorded')
    parser.add_argument('--receipt', help='write the receipt JSON here as well (must not exist)')
    args = parser.parse_args(argv)
    if args.receipt and os.path.exists(args.receipt): parser.error('receipt path exists')
    receipt = init(args.root, args.now, args.days, args.roster, args.allow_clock_skew_seconds)
    text = json.dumps(receipt, sort_keys=True, indent=2) + '\n'
    if args.receipt:
        with open(args.receipt, 'x') as f:
            f.write(text); f.flush(); os.fsync(f.fileno())
    sys.stdout.write(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
