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
    # Exit 0 when the round ran, even if single sources were refused (they are
    # per-source state, listed in the JSON). Exit 1 only for whole-round failure:
    # the store refused (open, reserve, maintenance hold) or nothing was attempted.
    try:
        with HTTPTransport() as transport:
            collector=Collector(RecentStore(args.store),transport)
            result=collector.run_round(discover=args.discover)
    except Exception as exc:
        print(json.dumps({'round':{'status':'refused','reason':(str(exc) or type(exc).__name__)[:160]}},sort_keys=True),flush=True)
        return 1
    print(json.dumps(result,sort_keys=True),flush=True)
    return 1 if result['collection'].get('round',{}).get('status')=='refused' else 0


if __name__ == '__main__':
    raise SystemExit(main())
