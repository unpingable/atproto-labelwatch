"""Review r4: D2 with a real SIGKILL of a collector process mid-request."""
import json, os, signal, sqlite3, subprocess, sys, time
from pathlib import Path

from test_recent_collector_bootstrap import A, B, store  # noqa: F401
from labelwatch.recent_collector import Collector

CHILD = r'''
import sys, time, json
from pathlib import Path
sys.path.insert(0, sys.argv[2])
import storage; storage.ROOT = Path(sys.argv[3])
from recent_storage import RecentStore
from labelwatch.recent_collector import Collector
class Hang:
    def request(self, *a, **k):
        time.sleep(300)
Collector(RecentStore(sys.argv[1]), Hang(), sleep=time.sleep, stale_seconds=2).tick()
'''


def test_sigkilled_round_is_swept_once_and_young_attempts_are_kept(store, tmp_path):
    from datetime import datetime, timezone
    store.remember_sources([{'did': A, 'endpoint': 'https://a.invalid'}, {'did': B, 'endpoint': 'https://b.invalid'}])
    tools = str(Path(__file__).parents[1]/'tools'/'segmented_qualification')
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join(sys.path)}
    import storage
    child = subprocess.Popen([sys.executable, '-c', CHILD, str(store.root), tools, str(storage.ROOT)], env=env)
    try:
        for _ in range(200):
            if store.collector_bootstrap_summary()['pending_attempts'] == 1:
                break
            time.sleep(.05)
        assert store.collector_bootstrap_summary()['pending_attempts'] == 1
    finally:
        child.send_signal(signal.SIGKILL); child.wait()
    # A young in-flight attempt (other did) is not swept.
    class Empty:
        def request(self, *a, **k):
            return json.dumps({'labels': []}).encode()
    now = lambda: datetime.now(timezone.utc)
    first = Collector(store, Empty(), stale_seconds=60, now=now).tick()
    assert first['round']['lost_attempts'] == 0
    time.sleep(2.5)
    second = Collector(store, Empty(), stale_seconds=2, now=now).tick()
    assert second['round']['lost_attempts'] == 1
    with sqlite3.connect(store.state) as c:
        unknown = list(c.execute("SELECT start,end FROM q_recent_gaps WHERE reason='unknown'"))
    assert len(unknown) == 1, unknown
    assert store.collector_bootstrap_summary()['pending_attempts'] == 0
    third = Collector(store, Empty(), stale_seconds=2, now=now).tick()
    assert third['round']['lost_attempts'] == 0
