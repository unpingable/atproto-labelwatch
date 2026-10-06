"""Offline virtual-time workloads using the actual persisted scheduler and writer.

No deployment cadence is inferred: zero idle is an optimistic service envelope.
Network latency is synthetic; measured wall time is separate local processing cost.
"""
import json
import time
from datetime import datetime, timezone, timedelta
from labelwatch.recent_collector import Collector

BASE = datetime(2026, 10, 6, tzinfo=timezone.utc)

class Clock:
    value = 0.0
    def monotonic(self): return self.value
    def now(self): return BASE + timedelta(seconds=self.value)

class WorkloadTransport:
    def __init__(self, clock, rates, fail, latency, initial):
        self.clock, self.rates, self.fail, self.latency = clock, rates, fail, latency
        self.served = {did: 0 for did in rates}
        self.initial = initial
        self.visits = {}
    def request(self, operation, params, max_bytes, timeout_seconds):
        assert operation == 'labels'
        did = params['did']
        self.visits.setdefault(did, []).append(self.clock.value)
        busy = did in self.rates
        delay = self.latency if busy or not self.fail else timeout_seconds
        self.clock.value += min(delay, timeout_seconds)
        if delay >= timeout_seconds: raise TimeoutError('finite fixture deadline')
        if not busy: return b'{"labels":[]}'
        offset = self.served[did]
        count = min(100, max(0, self.initial + int(self.rates[did]*self.clock.value)-offset))
        rows = [{'src':did,'uri':'did:plc:subject','val':str(offset+n),
                 'cts':'2026-10-06T00:00:00Z'} for n in range(count)]
        # Advance only when commit succeeds, in simulate's checked result loop.
        return json.dumps({'labels':rows, **({'cursor':f'opaque-{offset+count}'} if count else {})}).encode()

def simulate(root, *, count=584, busy_count=1, total_rate=20, fail=False,
             latency=.05, duration=120, idle=0, busy_last=False, initial=1000):
    import sqlite3
    from recent_storage import RecentStore
    clock=Clock()
    store=RecentStore.create(root, clock.now().isoformat())
    roster=[{'did':f'did:plc:s{n:04d}','endpoint':'https://fixture.invalid'} for n in range(count)]
    store.remember_sources(roster)
    selected=roster[-busy_count:] if busy_last else roster[:busy_count]
    rates={row['did']:total_rate/busy_count for row in selected}
    transport=WorkloadTransport(clock,rates,fail,latency,initial)
    # Every accepted page increments fixture delivery immediately, including
    # repeated pages within a tick; actual store remains sole scheduling authority.
    original=store.accept_page
    def accept(rows, **kwargs):
        result=original(rows, **kwargs)
        if kwargs['source'] in rates: transport.served[kwargs['source']]+=result['inserted']
        return result
    store.accept_page=accept
    started=time.monotonic();ticks=0;refusals=0;peak=initial*busy_count
    while clock.value<duration:
        # New adapter for every invocation; all scheduling state lives in SQLite.
        result=Collector(store,transport,monotonic=clock.monotonic,now=clock.now).tick()
        ticks+=1
        refusals+=sum(r['status']=='refused' for r in result['results'])
        peak=max(peak,sum(initial+int(rate*clock.value)-transport.served[did] for did,rate in rates.items()))
        if not result['attempted']:
            with sqlite3.connect(store.state) as conn:
                due=conn.execute('SELECT MIN(next_due) FROM q_recent_sources').fetchone()[0]
            clock.value=max(clock.value+.001,(datetime.fromisoformat(due)-BASE).total_seconds())
        clock.value+=idle
    wall=time.monotonic()-started
    backlog=sum(initial+int(rate*clock.value)-transport.served[did] for did,rate in rates.items())
    busy_visits=[transport.visits.get(did,[]) for did in rates]
    return {'virtual_seconds':clock.value,'wall_seconds':wall,'ticks':ticks,
            'committed':sum(transport.served.values()),'backlog':backlog,'peak_backlog':peak,
            'busy_first_visit_max':max((v[0] for v in busy_visits if v),default=None),
            'busy_sources_unvisited':sum(not v for v in busy_visits),
            'busy_max_visit_gap':max((b-a for v in busy_visits for a,b in zip(v,v[1:])),default=None),
            'sources_attempted':len(transport.visits),'requests':sum(map(len,transport.visits.values())),
            'refusals':refusals,'scenario':dict(count=count,busy_count=busy_count,total_rate=total_rate,
                fail=fail,latency=latency,duration=duration,idle=idle,busy_last=busy_last,initial=initial),
            'scope':'actual SQLite scheduler and commits; fresh Collector each tick; virtual network and clock; no installed cadence'}
