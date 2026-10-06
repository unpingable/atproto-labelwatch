"""Bounded off-production recent-store daily maintenance command."""
import argparse
import datetime as dt
import json
from recent_storage import RecentStore


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store',required=True)
    parser.add_argument('--now',default=None,help='UTC acceptance clock; defaults to current UTC')
    args=parser.parse_args(argv)
    now=args.now or dt.datetime.now(dt.timezone.utc).isoformat()
    print(json.dumps(RecentStore(args.store).advance_day(now),sort_keys=True))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
