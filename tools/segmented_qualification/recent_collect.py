"""Finite successor collector command; explicitly opt in to network use.

Discovery continuation and collection scheduling claims are persisted atomically
by the store. This command
does not create a store, rotate owners or enable a service. Existing RecentStore enrollment/maintenance admission remains required.
"""
import argparse
import json

import os
import time

from labelwatch.recent_collector import Collector, HTTPTransport, Limits, STORE_HOLD

HOLD_EXIT = 75


def _env_seconds(name, default, maximum):
    raw = os.environ.get(name)
    if raw is None or raw == '':
        return default
    value = float(raw)
    if not 0 < value <= maximum:
        raise SystemExit(f'{name} must be in (0, {maximum}]')
    return value


def limits():
    """Round deadline from LABELWATCH_RECENT_ROUND_SECONDS (default 10, max 30).

    Longer rounds give bootstrapping sources more spaced probes per round (1 s
    per-source spacing) at the cost of a longer unit run; the unit's stop
    deadline must stay above it.
    """
    return Limits(seconds=_env_seconds('LABELWATCH_RECENT_ROUND_SECONDS', 10, 30))
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
    # per-source state, listed in the JSON). Exit 75 for the daily owner hold.
    # Exit 1 for whole-round failure: store refused (open, reserve, recovery) or
    # nothing was attempted before the deadline.
    try:
        store = RecentStore(args.store)
        # Refuse the current filesystem reserve before discovery, requests or
        # bootstrap metadata writes; policy parsing alone is not admission.
        store.check_capacity()
        with HTTPTransport() as transport:
            collector=Collector(store,transport,limits=limits(),sleep=time.sleep,
                                stale_seconds=_env_seconds('LABELWATCH_RECENT_STALE_SECONDS',60,3600))
            result=collector.run_round(discover=args.discover)
    except Exception as exc:
        result = {'round': {'status': 'refused', 'reason': (str(exc) or type(exc).__name__)[:160]}}
        print(json.dumps(result, sort_keys=True), flush=True)
        __import__('recent_publish').write_status_quietly(args.store, result)
        return 1
    print(json.dumps(result,sort_keys=True),flush=True)
    __import__('recent_publish').write_status_quietly(args.store,result)  # /health reads <store>/STATUS.json
    round_=result['collection'].get('round',{})
    if round_.get('status')!='refused':return 0
    # The expected 00:00 UTC owner hold is not a failure: 75 (EX_TEMPFAIL) is
    # listed in SuccessExitStatus of the unit; every other refusal stays 1.
    return HOLD_EXIT if round_.get('store_hold')==STORE_HOLD else 1


if __name__ == '__main__':
    raise SystemExit(main())
