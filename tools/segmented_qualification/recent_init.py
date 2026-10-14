"""Create one empty recent store under the admitted root and seed its roster.

Seeded DIDs carry no endpoint and no cursor: their coverage is unknown until
the collector bootstraps each one.

Every new store records its predecessor. By default init reads the last
published overview.json (in $LABELWATCH_RECENT_PUBDIR, or --published-overview):
if it names a generation, that epoch is recorded as observed and then lost up to
this epoch's start. If it is absent, unreadable, held or carries no generation,
continuity before this epoch is recorded as unknown. Explicit declarations skip
discovery: --predecessor-receipt (the lost store's init receipt, overview.json or
STATUS.json), --continuity-break (lost, extent unknown) or --first-install (no
previous epoch). Nothing here enables a unit, touches a hold or activation
marker, or imports legacy history.
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
from recent_storage import (RecentStore, clock, iso, FIRST_INSTALL, UNKNOWN_CONTINUITY, PREDECESSOR_SCHEMA,
                            predecessor_record)
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


RECEIPT_SCHEMA = 'labelwatch.recent-init-receipt.v1'
PRIOR_SCHEMAS = (RECEIPT_SCHEMA, 'labelwatch.recent-overview.v1', 'labelwatch.recent-status.v1')


def lost_predecessor(generation_id=None, started_at=None, observed_through=None, basis='continuity_break'):
    """A previous epoch existed and its store was lost; unknown bounds stay None."""
    return predecessor_record({'schema': PREDECESSOR_SCHEMA, 'recorded': True, 'generation_id': generation_id,
                               'started_at': started_at, 'observed_through': observed_through, 'ended_on': None,
                               'end_reason': 'store_lost', 'basis': basis})


def predecessor_from_receipt(path):
    """Bounds of a lost epoch from its init receipt, published overview or STATUS.json."""
    value = json.loads(open(path, 'rb').read())
    schema = value.get('schema') or value.get('schema_version') if isinstance(value, dict) else None
    if schema not in PRIOR_SCHEMAS: raise ValueError('predecessor receipt schema')
    if schema == RECEIPT_SCHEMA:
        acquisition, through = value, None
    else:
        acquisition, through = value.get('acquisition'), (value.get('coverage') or {}).get('observed_through')
    if not isinstance(acquisition, dict) or not acquisition.get('generation_id') or not acquisition.get('started_at'):
        raise ValueError('predecessor receipt carries no acquisition identity (a held overview is not a receipt)')
    return lost_predecessor(acquisition['generation_id'], acquisition['started_at'], through, 'predecessor_receipt')


PUBDIR_ENV = 'LABELWATCH_RECENT_PUBDIR'  # the directory recent_publish.py --pubdir is given
OVERVIEW_SCHEMA = 'labelwatch.recent-overview.v1'


def discover_predecessor(path, now):
    """(predecessor, outcome) from the last published overview; unknown continuity unless it names an earlier epoch."""
    if path is None: return UNKNOWN_CONTINUITY, 'no_publish_location'
    try:
        value = json.loads(open(path, 'rb').read())
    except FileNotFoundError:
        return UNKNOWN_CONTINUITY, 'absent'
    except (OSError, ValueError):
        return UNKNOWN_CONTINUITY, 'unreadable'
    if not isinstance(value, dict) or value.get('schema_version') != OVERVIEW_SCHEMA:
        return UNKNOWN_CONTINUITY, 'unreadable'
    if value.get('hold') is not False:
        return UNKNOWN_CONTINUITY, 'held'
    acquisition = value.get('acquisition')
    if not isinstance(acquisition, dict) or not acquisition.get('generation_id') or not acquisition.get('started_at'):
        return UNKNOWN_CONTINUITY, 'no_generation'
    try:
        coverage = value.get('coverage')
        through = coverage.get('observed_through') if isinstance(coverage, dict) else None
        found = lost_predecessor(acquisition['generation_id'], acquisition['started_at'], through, 'published_overview')
        if clock(found['started_at']) >= clock(now): return UNKNOWN_CONTINUITY, 'not_earlier'
    except (TypeError, ValueError):
        return UNKNOWN_CONTINUITY, 'unreadable'
    return found, 'discovered'


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


def init(root, now, days=30, roster=None, allow_skew_seconds=None, wall=None, predecessor=None, discovery=None):
    root = storage.owned(root)
    if days != 30: raise ValueError('only 30-day candidate admitted')
    acceptance = acceptance_clock(now, allow_skew_seconds, wall)
    # The policy must hold now, not merely parse, before any effect.
    storage.admit_reserves(storage.reserve_policy(root), 0, (root.parent,), 'shared_reserve_unavailable')
    dids, roster_sha = load_roster(roster) if roster else ([], None)
    if root.exists():
        if not root.is_dir() or any(root.iterdir()): raise RuntimeError('store root exists and is not empty')
        root.rmdir()  # Store.create owns the mkdir; an empty placeholder carries nothing.
    store = RecentStore.create(root, now, days, predecessor=predecessor or UNKNOWN_CONTINUITY)
    if dids: store.remember_sources([{'did': d} for d in dids], limit=2048)
    with closing(storage.connect(store.state, readonly=True)) as c:
        acquisition = RecentStore.acquisition(c)
        recorded_predecessor = store.predecessor(c)
        policy = json.loads(storage.db.get_meta(c, storage.RESERVES_KEY))
        seeded = c.execute('SELECT COUNT(*) FROM q_recent_sources WHERE endpoint IS NULL AND provider_cursor IS NULL').fetchone()[0]
    if seeded != len(dids): raise RuntimeError('roster seed count mismatch after enrollment')
    return {'schema': RECEIPT_SCHEMA, 'store': str(root), 'predecessor': recorded_predecessor,
            'predecessor_discovery': discovery,
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
    parser.add_argument('--predecessor-receipt',
                        help="the lost store's init receipt, overview.json or STATUS.json; records its known bounds")
    parser.add_argument('--continuity-break', action='store_true',
                        help='a prior epoch existed and was lost; its extent is unknown')
    parser.add_argument('--first-install', action='store_true',
                        help='declare a genuine first installation: no previous epoch existed')
    parser.add_argument('--published-overview',
                        help='discover the predecessor from this overview.json instead of $%s/overview.json' % PUBDIR_ENV)
    args = parser.parse_args(argv)
    if args.receipt and os.path.exists(args.receipt): parser.error('receipt path exists')
    chosen = [flag for flag, given in (('--predecessor-receipt', args.predecessor_receipt), ('--continuity-break', args.continuity_break),
                                       ('--first-install', args.first_install), ('--published-overview', args.published_overview)) if given]
    if len(chosen) > 1: parser.error('conflicting predecessor flags: ' + ' '.join(chosen))
    discovery = None
    if args.predecessor_receipt:
        predecessor = predecessor_from_receipt(args.predecessor_receipt)
    elif args.continuity_break:
        predecessor = lost_predecessor()
    elif args.first_install:
        predecessor = FIRST_INSTALL
    else:
        pubdir = os.environ.get(PUBDIR_ENV)
        path = args.published_overview or (os.path.join(pubdir, 'overview.json') if pubdir else None)
        predecessor, outcome = discover_predecessor(path, args.now)
        discovery = {'path': os.path.abspath(path) if path else None, 'outcome': outcome}
    receipt = init(args.root, args.now, args.days, args.roster, args.allow_clock_skew_seconds, predecessor=predecessor,
                   discovery=discovery)
    text = json.dumps(receipt, sort_keys=True, indent=2) + '\n'
    if args.receipt:
        with open(args.receipt, 'x') as f:
            f.write(text); f.flush(); os.fsync(f.fileno())
    sys.stdout.write(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
