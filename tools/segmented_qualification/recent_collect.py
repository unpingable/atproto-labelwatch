"""Finite successor collector command; explicitly opt in to network use.

Discovery continuation and collection scheduling claims are persisted atomically
by the store. This command
does not create a store, rotate owners or enable a service. Existing RecentStore enrollment/maintenance admission remains required.
"""
import argparse
import json

from labelwatch.recent_collector import Collector, HTTPTransport
from recent_storage import RecentStore


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store', required=True)
    parser.add_argument('--network', action='store_true', help='explicitly permit this finite HTTP collection')
    parser.add_argument('--discover', action='store_true', help='one bounded discovery page before collection')
    args = parser.parse_args(argv)
    if not args.network:
        parser.error('network collection requires explicit --network')
    collector = Collector(RecentStore(args.store), HTTPTransport())
    discovery = collector.discover() if args.discover else None
    result = collector.tick()
    print(json.dumps({'discovery': discovery, 'collection': result}, sort_keys=True))
    return 1 if any(row['status'] != 'accepted' for row in result['results']) else 0


if __name__ == '__main__':
    raise SystemExit(main())
