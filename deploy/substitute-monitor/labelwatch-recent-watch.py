#!/usr/bin/env python3
"""labelwatch-recent-watch: TEMPORARY obligation watcher for the Labelwatch rolling observatory.

Exists only while Constellation NQ #27 / Monitor #18 block native enrollment (README.md: owner, routes, retirement).
Evaluates L1-L10 of PHASE-C-NQ-MONITOR-RECONCILIATION-20261009 section 3.2 plus the NC1 control occurrence (C1),
appends observations.jsonl, rewrites state.json/report.json and retains one intent file per transition.
Exits non-zero only on an internal error, never on a verdict. Python 3.10, stdlib only.
"""
import argparse, hashlib, json, os, re, socket, subprocess, sys, urllib.error, urllib.request
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path

UTC, GIB, P = timezone.utc, 1 << 30, 'labelwatch-recent'
STATUS_SCHEMA, OVERVIEW_SCHEMA = 'labelwatch.recent-status.v1', 'labelwatch.recent-overview.v1'
UNITS = ['collect.service', 'collect.timer', 'maintain.service', 'maintain.timer', 'overview.service', 'product.service']
SERVICE_PROPS = ('LoadState,ActiveState,Result,ExecMainStatus,ExecMainStartTimestamp,ExecMainExitTimestamp,InvocationID,'
                 'ConditionResult,ConditionTimestamp,NRestarts')
TIMER_PROPS = 'LoadState,ActiveState,UnitFileState,LastTriggerUSec,NextElapseUSecRealtime'
DEFAULTS = {'WATCH_HOLD_MARKER': '/etc/labelwatch-operator-safe-hold', 'WATCH_RECENT_ENV': '/etc/labelwatch-recent.env',
            'WATCH_ACTIVATION_MARKER': '/etc/labelwatch-recent-qualified-activation', 'WATCH_CONTROL_UNIT': 'labelwatch-recent-control',
            'WATCH_CONTROL_HOLD': '/etc/labelwatch-recent-control-hold', 'WATCH_HEALTH_URL': 'http://127.0.0.1:8431/health',
            'WATCH_CONTROL_ACTIVATION': '/etc/labelwatch-recent-control-activation',
            'WATCH_FACTS_BASE': 'http://127.0.0.1:8425', 'WATCH_PAGERDUTY_URL': 'https://events.pagerduty.com/v2/enqueue',
            'WATCH_SYSTEMCTL': '/usr/bin/systemctl', 'WATCH_JOURNALCTL': '/usr/bin/journalctl'}
# The enrolled obligation definitions (thresholds and response classes); the digest rides on every observation and intent.
OBLIGATIONS = {'version': 1,
    'L1': {'name': 'collection-progress', 'rule': 'stalled >900 s notice, page at 3600 s; STATUS >600 s unknown; 00:00-00:35 held'},
    'L2': {'name': 'collection-unit-result', 'rule': 'failing on 2 observed invocations or timer >600 s: notice'},
    'L3': {'name': 'maintenance-by-deadline', 'rule': 'due 00:05, judged 00:35: failed/overdue page, running-late notice'},
    'L4': {'name': 'window-advanced', 'rule': 'settle 120 s; not-advanced notice day 1 page day 2; expiry-stalled page'},
    'L5': {'name': 'reserve-admission', 'rule': 'reserve-refused page; free < reserve + 2 GiB or inodes < floor notice'},
    'L6': {'name': 'overview-published', 'rule': 'stale >2700 s, refused (exit 2) x2, unavailable, hold contradiction: notice'},
    'L7': {'name': 'public-facts-freshness', 'rule': 'stale/unavailable notice, page at 7200 s; hold-contradiction notice'},
    'L8': {'name': 'hold-and-activation', 'rule': 'hold gates everything and never pages; hold-incomplete notice'},
    'L9': {'name': 'product-reachability', 'rule': 'refused page at 300 s; 503 health-stale notice; timeout unknown'},
    'L10': {'name': 'evidence-presence', 'rule': 'no evaluation for 900 s notice; daily digest at 00:40Z'},
    'C1': {'name': 'control-occurrence', 'rule': 'NC1: failed within 900 s or no occurrence for 900 s: page'},
    'evidence': {'name': 'evidence-unavailable', 'rule': 'input unreadable/stale for 900 s: one notice per input'}}
DIGEST = hashlib.sha256(json.dumps(OBLIGATIONS, sort_keys=True).encode()).hexdigest()
RESERVE_RE = re.compile(r'shared_reserve_unavailable|local_capacity_below_required_bound')
MAINT_RESERVE_RE = re.compile(r'(daily archive|global compaction) allocation not admitted')
RANK, ROTATE_BYTES, HOLD_MARGIN_S, RETRIES = {None: 0, 'notice': 1, 'page': 2}, 10 * 1024 * 1024, 300, 12
def iso(d): return d.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
def ts(value):
    """ISO-8601 or `systemctl show` (TZ=UTC) timestamp -> aware UTC datetime; anything else -> None."""
    if not isinstance(value, str) or value in ('', 'n/a', '0'): return None
    try:
        m = re.match(r'^\w{3} (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) UTC$', value)
        if m: return datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)
        d = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return d.astimezone(UTC) if d.tzinfo else None
    except (ValueError, OverflowError): return None
def secs(delta): return int(delta.total_seconds())
def res(verdict, **facts): return {'verdict': verdict, 'facts': facts}
def cond(name, text, target, page_after=None, notice_after=0, ids=None, **facts):  # page/notice after N s of persistence; None = never
    return dict(verdict=name, text=text, target=target, page_after=page_after, notice_after=notice_after, ids=ids or {}, facts=facts)
def unit(f, short): return f['units'].get(P + '-' + short)
def status_doc(f, now, max_age=600):
    s = f.get('status')
    if not isinstance(s, dict) or s.get('schema') != STATUS_SCHEMA or ts(s.get('generated_at')) is None: return None
    return s if max_age is None or secs(now - ts(s['generated_at'])) <= max_age else None
def occurrences(st, key, u):
    """Last 5 observed (sampled every 5 min) occurrences, one per InvocationID once it has exited."""
    hist, u = st.setdefault('occurrences', {}).setdefault(key, []), u or {}
    if u.get('ActiveState') != 'activating' and ts(u.get('ExecMainExitTimestamp')) and u.get('InvocationID') not in [None] + [
            h['invocation'] for h in hist[-1:]]:
        hist.append({'invocation': u['InvocationID'], 'result': u.get('Result'), 'exit_status': u.get('ExecMainStatus'),
                     'exit_at': u.get('ExecMainExitTimestamp')})
    del hist[:-5]
    return list(hist)
def hold_after(st, scope, generated):  # a hold claim generated after the hold was last seen (+ one cadence) contradicts the markers
    seen = ts(st.get('hold_seen', {}).get(scope))
    return generated is None or seen is None or generated > seen + timedelta(seconds=HOLD_MARGIN_S)
def l1(f, st, now):
    s = status_doc(f, now)
    if s is None: return res('unknown', reason='STATUS.json absent, unreadable, not v1 or generated_at older than 600 s')
    cov, rnd = s.get('coverage') or {}, s.get('round') or {}
    through, health = ts(cov.get('observed_through')), (f.get('health') or {}).get('body') or {}
    facts = dict(observed_through=cov.get('observed_through'), status_generated_at=s['generated_at'], round=rnd,
                 health_freshness=(health.get('freshness') or {}).get('state'))
    if through is None: return res('unknown', reason='no accepted labeler page recorded', **facts)
    age = secs(now - through)
    if age <= 900: return res('ok', **facts)
    if now.time() < dtime(0, 35) and (rnd.get('store_hold') or (unit(f, 'maintain.service') or {}).get('ActiveState') == 'activating'):
        return res('held', mode='held-for-maintenance', **facts)
    ids = {'unit': P + '-collect.service', 'status_generated_at': s['generated_at']}
    return cond('collection-stalled', 'Labelwatch has accepted no labeler page for %d min; last accepted %s' % (age // 60, iso(through)),
                P + '-collect', 0 if age >= 3600 else None, ids=ids, **facts)
def l2(f, st, now):
    u, t = unit(f, 'collect.service'), unit(f, 'collect.timer')
    if u is None or t is None: return res('unknown', reason='systemctl show unreadable for collect service/timer')
    occ, s = occurrences(st, 'L2', u), status_doc(f, now, None) or {}
    ids = {'unit': P + '-collect.service', 'invocation_id': u.get('InvocationID'), 'status_generated_at': s.get('generated_at')}
    if u.get('ConditionResult') == 'no': return res('held', occurrences=occ)
    if len(occ) >= 2 and all(o['result'] != 'success' for o in occ[-2:]):
        return cond('collection-failing', 'collection rounds failing (exit %s on 2 consecutive observed invocations): %s'
                    % (occ[-1]['exit_status'], (s.get('round') or {}).get('reason')), P + '-collect', ids=ids, occurrences=occ)
    if secs(now - (ts(t.get('LastTriggerUSec')) or datetime.min.replace(tzinfo=UTC))) > 600:
        return cond('collection-not-scheduled', 'collection timer has not fired since %s' % t.get('LastTriggerUSec'), P + '-collect', ids=ids)
    return res('ok', occurrences=occ, last_trigger=t.get('LastTriggerUSec'))
def journal_reason(f):
    lines = [x.strip() for x in (f.get('journal') or '').splitlines() if x.strip()]
    hits = [x for x in lines if re.search(r'not admitted|Error|refus|Traceback|failed', x)]
    return (hits or lines or ['no journal reason'])[-1][:200]
def l3(f, st, now):
    u, t = unit(f, 'maintain.service'), unit(f, 'maintain.timer')
    if u is None or t is None: return res('unknown', reason='systemctl show unreadable for maintain service/timer')
    day = now.date() if now.time() >= dtime(0, 35) else now.date() - timedelta(days=1)
    due, exit_at, tgt = datetime.combine(day, dtime(0, 5), UTC), ts(u.get('ExecMainExitTimestamp')), P + '-maintain'
    facts = dict(day=day.isoformat(), result=u.get('Result'), exit_status=u.get('ExecMainStatus'), exit_at=u.get('ExecMainExitTimestamp'))
    ids = {'unit': P + '-maintain.service', 'invocation_id': u.get('InvocationID')}
    if u.get('ActiveState') == 'activating':  # running-late becomes failed or succeeded on exit (2 h unit timeout)
        late = now.time() >= dtime(0, 35)
        return cond('maintenance-running-late', 'daily maintenance still running after 00:35Z', tgt, ids=ids) if late else res('skip')
    if exit_at and exit_at >= due:
        if u.get('Result') == 'success' and u.get('ExecMainStatus') == '0': return res('ok', **facts)
        return cond('maintenance-failed', 'daily window maintenance failed: %s; expiry did not run, store grows' % journal_reason(f), tgt, 0,
                    ids=ids, **facts)
    held_at = ts(u.get('ConditionTimestamp'))
    if u.get('ConditionResult') == 'no' and held_at and held_at >= due: return res('held', mode='held-at-occurrence', **facts)
    return cond('maintenance-overdue', 'daily window maintenance has no occurrence for %s (timer %s, next %s); expiry did not run'
                % (day, t.get('UnitFileState'), t.get('NextElapseUSecRealtime') or 'missing'), tgt, 0, ids=ids, **facts)
def l4(f, st, now, l3r):
    if l3r['verdict'] != 'ok': return res(l3r['verdict'] if l3r['verdict'] in ('unknown', 'held') else 'skip')  # L3 owns its failures
    exit_at, day = ts(l3r['facts']['exit_at']), l3r['facts']['day']
    if secs(now - exit_at) < 120 or exit_at.date().isoformat() != day: return res('skip')  # settle 2 min; a later run is not day D's
    s = status_doc(f, now)
    if s is None: return res('unknown', reason='STATUS.json unreadable or stale')
    lm, days = s.get('last_maintenance') or {}, st.setdefault('l4_days', {})
    prev, at = days.get(max((k for k in days if k < day), default='')), ts(lm.get('at'))
    generation = lm.get('generation')
    valid_generation = lambda value: type(value) is int and value >= 0
    baseline_valid = prev is not None and valid_generation(prev.get('generation'))
    generations_valid = valid_generation(generation) and baseline_valid
    advanced = bool(at and at.date().isoformat() == day and generations_valid and generation > prev['generation'])
    days[day] = rec = {'generation': generation if valid_generation(generation) else None, 'at': lm.get('at'), 'recovery_pending': bool(lm.get('recovery_pending')),
                       'advanced': advanced if generations_valid else None}
    for k in sorted(days)[:-7]: del days[k]
    ids = {'unit': P + '-maintain.service', 'status_generated_at': s['generated_at']}
    if rec['recovery_pending'] and prev and prev['recovery_pending']:
        return cond('expiry-stalled', 'window expiry recovery-pending across two daily runs', P + '-maintain', 0, ids=ids, **rec)
    if not generations_valid:
        return res('unknown', reason='current or prior observed generation unavailable or invalid', **rec)
    qualified_days = [k for k in sorted(days, reverse=True) if days[k]['advanced'] is not None]
    n = next((i for i, k in enumerate(qualified_days) if days[k]['advanced']), len(qualified_days))
    if n:
        return cond('window-not-advanced', 'maintenance succeeded but the window did not advance (generation %s, at %s; day %d)'
                    % (rec['generation'], rec['at'], n), P + '-maintain', 0 if n >= 2 else None, ids=ids, **rec)
    return res('ok', **rec)
def l5(f, st, now):
    s, u, mounts = status_doc(f, now), unit(f, 'maintain.service'), f.get('reserves')
    rnd, refusals = (s or {}).get('round') or {}, []
    if rnd.get('status') == 'refused' and RESERVE_RE.search(str(rnd.get('reason'))):
        refusals.append({'refusal_source': 'collect', 'reason': rnd['reason']})
    maint_failed = (u or {}).get('Result') not in (None, 'success')
    hit = MAINT_RESERVE_RE.search(f.get('journal') or '') if maint_failed else None
    if hit: refusals.append({'refusal_source': 'maintain', 'reason': hit.group(0)})
    where = '; '.join('%.0f GiB on %s, free %.1f GiB' % (m['reserve_bytes'] / GIB, m['mount'], m['free_bytes'] / GIB) for m in mounts or [])
    if refusals:
        return cond('reserve-refused', 'collection/maintenance refused by the configured reserve (%s): %s' % (where or 'free unknown',
                    ', '.join(r['reason'] for r in refusals)), P + '-reserve', 0, refusals=refusals, mounts=mounts)
    if mounts is None or s is None or u is None or (maint_failed and f.get('journal') is None):
        return res('unknown', reason='reserve policy/statvfs, STATUS.json, maintain unit or journal unreadable')
    if [m for m in mounts if m['free_bytes'] < m['reserve_bytes'] + 2 * GIB or m['free_inodes'] < m['inode_floor']]:
        return cond('reserve-headroom-low', 'free space within 2 GiB of the reserve, or inodes under the floor (%s)' % where, P + '-reserve',
                    mounts=mounts)
    return res('ok', mounts=mounts)
def l6(f, st, now):
    o, occ, tgt = f.get('overview'), occurrences(st, 'L6', unit(f, 'overview.service')), P + '-overview'
    gen = ts(o.get('generated_at')) if isinstance(o, dict) else None
    if gen is None or o.get('schema_version') != OVERVIEW_SCHEMA:
        return cond('overview-unavailable', 'overview artifact absent, unreadable or not ' + OVERVIEW_SCHEMA, tgt, reason=f.get('overview_error'))
    facts = dict(generated_at=o['generated_at'], hold=o.get('hold'), coverage_status=(o.get('coverage') or {}).get('status'), occurrences=occ)
    if len(occ) >= 2 and all(x['exit_status'] == '2' for x in occ[-2:]):
        return cond('overview-refused', 'overview publisher refused (exit 2) on 2 consecutive observed runs', tgt, **facts)
    if secs(now - gen) > 2700:
        return cond('overview-stale', 'the public overview artifact is %d min old (bound 45 min)' % (secs(now - gen) // 60), tgt, **facts)
    if o.get('hold') is True and hold_after(st, 'prod', gen):
        return cond('overview-hold-contradiction', 'the overview artifact declares hold:true but no hold marker is present', tgt, **facts)
    return res('ok', **facts)
def l7(f, st, now):
    x = f.get('facts')
    if x is None: return res('unknown', reason='facts API timeout or connection refused')
    entry = next((e for e in (x.get('body') or {}).get('instruments') or [] if e.get('instrument') == 'labelwatch'), None)
    if entry is None: return res('unknown', reason='no labelwatch instrument entry', http=x.get('http'))
    fs, tgt = entry.get('freshness_state'), 'factsapi-labelwatch'
    facts = dict(state=entry.get('state'), freshness_state=fs, http_status=x.get('overview_http'), code=x.get('overview_code'))
    if entry.get('state') == 'UNAVAILABLE' or fs == 'unavailable':
        return cond('public-unavailable', 'public Labelwatch facts unavailable (source_unavailable; refusing truthfully)', tgt, 7200, **facts)
    if fs in ('fresh', 'warming_up'): return res('ok', **facts)
    if fs == 'held': return (cond('hold-contradiction', 'the facts API serves source_held but no hold marker is present', tgt, **facts)
                             if hold_after(st, 'prod', ts(entry.get('generated_at'))) else res('held', **facts))
    if fs == 'stale': return cond('public-stale', 'public Labelwatch facts stale (source_stale; degraded, not lying)', tgt, 7200, **facts)
    return res('unknown', reason='unrecognised freshness_state', **facts)
def l8(f, st, now):
    m, active = f['markers'], (unit(f, 'product.service') or {}).get('ActiveState')
    facts = dict(hold_present=m.get('hold'), hold_mtime=m.get('hold_mtime'), activation_present=m.get('activation'), product_active=active)
    if m.get('hold') is None or m.get('activation') is None: return res('unknown', reason='marker stat failed', **facts)
    if m['hold'] and active in ('active', 'activating', 'reloading'):
        return cond('hold-incomplete', 'safe hold present but the product server still runs (runbook: operator stops it)', P + '-product', **facts)
    return res('ok', mode='held' if m['hold'] else 'active' if m['activation'] else 'not_activated', **facts)
def l9(f, st, now):
    h, prod = f.get('health'), unit(f, 'product.service') or {}
    facts = dict(active=prod.get('ActiveState'), restarts=prod.get('NRestarts'))
    if h is None: return res('unknown', reason='/health timeout or unreadable', **facts)
    if h.get('refused'): return cond('product-down', 'the product server refuses connections on :8431', P + '-product', 300, None, **facts)
    facts.update(http=h.get('http'), status=(h.get('body') or {}).get('status'), status_age=(h.get('body') or {}).get('status_age_seconds'))
    if h.get('http') == 503: return cond('health-stale', '/health answers 503: the STATUS.json writer stopped', P + '-product', **facts)
    return res('ok' if h.get('http') == 200 else 'unknown', **facts)
def c1(f, st, now, name):
    u, t = f['units'].get(name + '.service'), f['units'].get(name + '.timer') or {}
    if u is None: return res('unknown', reason='control unit status unreadable')
    exit_at, held_at = ts(u.get('ExecMainExitTimestamp')), ts(u.get('ConditionTimestamp'))
    ids = {'unit': name + '.service', 'invocation_id': u.get('InvocationID')}
    facts = dict(result=u.get('Result'), exit_status=u.get('ExecMainStatus'), exit_at=u.get('ExecMainExitTimestamp'))
    if u.get('ActiveState') == 'activating': return res('skip', **facts)
    if exit_at and secs(now - exit_at) <= 900:
        if u.get('Result') == 'success': return res('ok', **facts)
        return cond('scheduled-occurrence-failed', '%s failed its scheduled occurrence (Result=%s, exit %s)'
                    % (name, u.get('Result'), u.get('ExecMainStatus')), name, 0, ids=ids, **facts)
    if u.get('ConditionResult') == 'no' and held_at and secs(now - held_at) <= 900: return res('held', **facts)
    return cond('scheduled-occurrence-overdue', '%s has no occurrence in 15 min (timer %s)' % (name, t.get('ActiveState')), name, 0,
                ids=ids, **facts)
def gate(m, hold, act):
    if m.get(hold) is None or m.get(act) is None: return 'unknown'
    return 'held' if m[hold] else None if m[act] else 'not_activated'
def make_intent(action, oid, condition, target, text, level, now, ids=None, row=None):
    return {'schema': 'labelwatch-recent-watch.intent.v1', 'created_at': iso(now), 'action': action, 'response_class': level, 'text': text,
            'obligation': oid, 'condition': condition, 'target': target, 'dedup_key': '%s:%s:%s:%s' % (P, oid, condition, target),
            'ids': dict(ids or {}, obligation=oid), 'obligations_digest': DIGEST, 'first_seen': (row or {}).get('first_seen'),
            'persisted_seconds': (row or {}).get('persisted_seconds')}
def transition(st, oid, r, now):
    conds, v, out = st.setdefault('conditions', {}), r['verdict'], []
    mine = {k: c for k, c in conds.items() if c['obligation'] == oid}
    for c in mine.values():
        c['observation'] = 'present' if c['condition'] == v else v if v in ('held', 'not_activated', 'unknown') else 'absent'
    if v == 'ok':  # a fresh verified success resolves everything this obligation has open
        for k, c in mine.items():
            out += [make_intent('resolve', oid, c['condition'], c['target'], 'recovered after a fresh success: ' + c['text'], c['notified'],
                                now, c.get('ids'), c)] if c.get('notified') else []
            del conds[k]
        return out
    if v in ('held', 'not_activated', 'unknown', 'skip'): return out  # hold wins, never pages; unknown never resolves; incidents stay open
    key = '%s:%s:%s' % (oid, v, r['target'])
    c = conds.setdefault(key, {'obligation': oid, 'condition': v, 'target': r['target'], 'first_seen': iso(now), 'notified': None,
                               'dedup_key': '%s:%s' % (P, key)})
    persisted = secs(now - ts(c['first_seen']))
    level = ('page' if r['page_after'] is not None and persisted >= r['page_after'] else
             'notice' if r['notice_after'] is not None and persisted >= r['notice_after'] else None)
    c.update(last_seen=iso(now), persisted_seconds=persisted, observation='present', ids=r['ids'], text=r['text'],
             response_class=max(level, c['notified'], key=RANK.get) or 'pending')
    if RANK[level] > RANK[c['notified']]:
        out.append(make_intent('trigger', oid, v, r['target'], r['text'], level, now, r['ids'], c))
        c['notified'] = level
    return out
def evidence(st, f, now, quiet):
    """One evidence-unavailable notice per input after 15 min unreadable/stale; it never touches obligation incidents."""
    inputs, out = st.setdefault('inputs', {}), []
    for name, s in f['sources'].items():
        row = inputs.get(name)
        if s['ok']:
            out += [make_intent('resolve', 'evidence', 'evidence-unavailable', name, 'readable again: ' + name, 'notice', now, row=row)
                    ] if row and row.get('notified') else []
            inputs.pop(name, None)
            continue
        row = inputs.setdefault(name, {'first_seen': iso(now), 'notified': None})
        row.update(error=s['error'], persisted_seconds=secs(now - ts(row['first_seen'])))
        if not row['notified'] and row['persisted_seconds'] >= 900 and not quiet('control' if 'control' in name else 'prod'):
            out.append(make_intent('trigger', 'evidence', 'evidence-unavailable', name, 'evidence unavailable for %d min: %s (%s); dependent '
                                   'obligations are unknown' % (row['persisted_seconds'] // 60, name, s['error']), 'notice', now, row=row))
            row['notified'] = 'notice'
    return out
def evaluate(f, st, now, cfg):
    m = f['markers']
    st.setdefault('hold_seen', {}).update({sc: iso(now) for sc, key in (('prod', 'hold'), ('control', 'control_hold')) if m.get(key)})
    gates, out = {'prod': gate(m, 'hold', 'activation'), 'control': None}, {'L8': l8(f, st, now)}
    for oid, fn in (('L1', l1), ('L2', l2), ('L3', l3), ('L4', lambda *a: l4(*a, out['L3'])), ('L5', l5), ('L6', l6), ('L7', l7), ('L9', l9)):
        out[oid] = res(gates['prod']) if gates['prod'] else fn(f, st, now)
    if cfg.get('WATCH_CONTROL_UNIT'):
        gates['control'] = gate(m, 'control_hold', 'control_activation')
        out['C1'] = res(gates['control']) if gates['control'] else c1(f, st, now, cfg['WATCH_CONTROL_UNIT'])
    intents = [i for oid, r in out.items() for i in transition(st, oid, r, now)]
    return out, intents + evidence(st, f, now, lambda scope: gates[scope] in ('held', 'not_activated'))
def load_env(path):  # KEY=VALUE lines as systemd EnvironmentFile= reads them (no expansion)
    try: pairs = [ln.split('=', 1) for ln in Path(path).read_text().splitlines() if '=' in ln and not ln.lstrip().startswith('#')]
    except (OSError, TypeError): return None
    return {k.strip(): v.strip().strip('"\'') for k, v in pairs}
def http_get(url, timeout=5):
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(url, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e: return e.code, e.read()
def run_cmd(argv, **kw): return subprocess.run(argv, capture_output=True, timeout=15, check=True, **kw).stdout
def gather(cfg, now):
    """Read every source once; each read is noted with its sha256, read time and error (a failed read makes that input unknown)."""
    f, recent = {'sources': {}, 'units': {}, 'markers': {}}, load_env(cfg['WATCH_RECENT_ENV']) or {}
    def note(name, where, data=None, error=None, counts=True):
        f['sources'][name] = {'source': where, 'read_at': iso(datetime.now(UTC)), 'ok': error is None or not counts, 'error': error,
                              'sha256': hashlib.sha256(data).hexdigest() if data is not None else None}
    for key in ('hold', 'activation', 'control_hold', 'control_activation'):
        path, err = cfg['WATCH_' + key.upper() + ('' if key.startswith('control') else '_MARKER')], None
        try: f['markers'][key], f['markers'][key + '_mtime'] = True, iso(datetime.fromtimestamp(os.stat(path).st_mtime, UTC))
        except FileNotFoundError: f['markers'][key] = False
        except OSError as e: f['markers'][key], err = None, type(e).__name__
        note('marker:' + key.replace('_', '-'), path, error=err)
    ctl = cfg.get('WATCH_CONTROL_UNIT')
    for n in [P + '-' + u for u in UNITS] + ([ctl + '.service', ctl + '.timer'] if ctl else []):
        out, d = None, None
        try:
            out = run_cmd([cfg['WATCH_SYSTEMCTL'], 'show', n, '-p', TIMER_PROPS if n.endswith('.timer') else SERVICE_PROPS],
                          env={'TZ': 'UTC', 'LC_ALL': 'C', 'PATH': '/usr/bin:/bin'})
            d = dict(ln.split('=', 1) for ln in out.decode().splitlines() if '=' in ln)
            err = None if d.get('LoadState') == 'loaded' else 'LoadState=%s' % d.get('LoadState')
        except (OSError, subprocess.SubprocessError, UnicodeDecodeError) as e: err = type(e).__name__
        f['units'][n] = d if err is None else None
        note('systemd:' + n, 'systemctl show ' + n, out, err)
    if (f['units'].get(P + '-maintain.service') or {}).get('Result') not in (None, 'success'):  # L3/L5 reason
        out, err, f['journal'] = None, None, None
        invocation = f['units'][P + '-maintain.service'].get('InvocationID')
        try:
            if not invocation or not re.fullmatch(r'[0-9a-fA-F]{32}', invocation):
                raise ValueError('maintenance invocation identity unavailable')
            out = run_cmd([cfg['WATCH_JOURNALCTL'], '-u', P + '-maintain.service',
                           '_SYSTEMD_INVOCATION_ID=' + invocation, '-n', '50', '-o', 'cat', '--no-pager'])
        except (OSError, subprocess.SubprocessError, ValueError) as e: err = type(e).__name__
        f['journal'] = out.decode(errors='replace') if out is not None else None
        note('journal:' + P + '-maintain', 'journalctl -u %s-maintain.service _SYSTEMD_INVOCATION_ID=%s -n 50' % (P, invocation or 'unavailable'), out, err)
    for key, var, name in (('status', 'STORE', 'STATUS.json'), ('overview', 'PUBDIR', 'overview.json')):
        base = cfg.get('WATCH_' + var) or recent.get('LABELWATCH_RECENT_' + var)
        path, data, doc, err = base and os.path.join(base, name), None, None, None
        try:
            doc = json.loads(data := Path(path).read_bytes())
            if key == 'status' and status_doc({'status': doc}, now) is None: err = 'not %s or older than 600 s' % STATUS_SCHEMA
        except (OSError, TypeError, ValueError) as e: err = type(e).__name__
        f[key], f[key + '_error'] = doc, err
        note(key, path or 'unconfigured', data, err, counts=key == 'status')  # an absent overview is L6's own condition
    body, err = None, None
    try:
        code, body = http_get(cfg['WATCH_HEALTH_URL'])
        f['health'] = {'http': code, 'body': json.loads(body or b'{}')}
    except Exception as e:  # refused is a product-down fact; timeouts and garbage are unknown
        refused = isinstance(getattr(e, 'reason', e), ConnectionRefusedError)
        f['health'], err = ({'refused': True}, None) if refused else (None, type(e).__name__)
    note('health', cfg['WATCH_HEALTH_URL'], body, err)
    base, body, err, f['facts'] = cfg['WATCH_FACTS_BASE'].rstrip('/'), None, None, None
    try:  # timeout and connection refused are both unknown for L7, not "unavailable"
        code, body = http_get(base + '/atproto/v1/instruments')
        f['facts'] = {'http': code, 'body': json.loads(body)}
        code, obody = http_get(base + '/atproto/v1/labelwatch/overview')  # corroboration: route status and error.code
        f['facts'].update(overview_http=code, overview_code=(json.loads(obody or b'{}').get('error') or {}).get('code'))
    except Exception as e: err = None if f['facts'] else type(e).__name__
    note('facts:instruments', base + '/atproto/v1/instruments', body, err)
    spec = cfg.get('WATCH_RESERVES') or recent.get('LABELWATCH_RECENT_RESERVES')
    try:
        spec = spec or Path(recent['LABELWATCH_RECENT_RESERVES_FILE']).read_text()
        floor, f['reserves'], err = int(recent.get('LABELWATCH_RECENT_MIN_FREE_INODES') or 1000000), [], None
        for item in json.loads(spec):
            sv = os.statvfs(item['mount'])
            f['reserves'].append({'mount': item['mount'], 'reserve_bytes': int(item['bytes']), 'free_bytes': sv.f_bavail * sv.f_frsize,
                                  'free_inodes': sv.f_favail, 'inode_floor': floor})
    except (OSError, TypeError, ValueError, KeyError) as e: f['reserves'], err = None, type(e).__name__
    note('reserves', 'LABELWATCH_RECENT_RESERVES + statvfs', (spec or '').encode(), err)
    return f
def post(url, body, timeout=10):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, json.dumps(body).encode(), {'Content-Type': 'application/json'}),
                                    timeout=timeout) as r: return r.status
    except urllib.error.HTTPError as e: return e.code
def deliver(cfg, intent, only=None):
    """Slack for every intent; PagerDuty Events v2 only for page-class trigger/resolve. Each route is OFF unless configured."""
    routes, slack, a = [], cfg.get('WATCH_SLACK_WEBHOOK_URL'), intent['action']
    key = (intent['obligation'] == 'C1' and cfg.get('WATCH_CONTROL_PAGERDUTY_ROUTING_KEY')) or cfg.get('WATCH_PAGERDUTY_ROUTING_KEY')
    if slack:
        routes.append(('slack', slack, {'text': '[%s %s] %s (%s)' % (intent['response_class'], a, intent['text'], intent['dedup_key'])}, 200))
    if key and intent['response_class'] == 'page' and a in ('trigger', 'resolve'):
        body = {'routing_key': key, 'event_action': a, 'dedup_key': intent['dedup_key']}
        body.update({'payload': {'summary': intent['text'][:1000], 'source': socket.gethostname(), 'severity': 'critical', 'group': P,
                                 'component': intent['target'], 'class': intent['condition'], 'custom_details': intent['ids']}}
                    if a == 'trigger' else {})
        routes.append(('pagerduty', cfg['WATCH_PAGERDUTY_URL'], body, 202))
    results = []
    for name, url, body, want in [r for r in routes if only is None or r[0] in only]:
        try: results.append({'route': name, 'http': post(url, body), 'at': iso(datetime.now(UTC))})
        except Exception as e: results.append({'route': name, 'error': type(e).__name__, 'at': iso(datetime.now(UTC))})  # never echo URL/key
        results[-1]['accepted'] = results[-1].get('http') == want
    return results
def write_json(path, doc):
    tmp = path.with_name('.%s.tmp' % path.name)
    tmp.write_text(json.dumps(doc, sort_keys=True, indent=1, default=str) + '\n')
    os.replace(tmp, path)
def read_json(path):
    try: return json.loads(path.read_text())
    except (OSError, ValueError): return None
def emit(cfg, sd, st, intents, now):
    """Retain one file per intent before delivery; routes that failed are retried on later runs (at most RETRIES times)."""
    pending = st.setdefault('pending', {})
    for name in list(pending):
        doc = read_json(sd / 'intents' / name) or {}
        failed = {d['route'] for d in doc.get('delivery', [])} - {d['route'] for d in doc.get('delivery', []) if d['accepted']}
        pending[name] += 1
        if failed and pending[name] <= RETRIES:
            doc['delivery'] += deliver(cfg, doc, only=failed)
            write_json(sd / 'intents' / name, doc)
        else: del pending[name]
    for n, intent in enumerate(intents):
        path = sd / 'intents' / ('%s-%02d-%s-%s-%s.json' % (now.strftime('%Y%m%dT%H%M%SZ'), n, intent['obligation'], intent['condition'],
                                                            intent['action']))
        write_json(path, intent)
        intent['delivery'] = deliver(cfg, intent)
        write_json(path, intent)
        if not all(d['accepted'] for d in intent['delivery']): pending[path.name] = 0
def state_dir(cfg): return Path(cfg.get('WATCH_STATE_DIR') or os.environ.get('STATE_DIRECTORY') or '/var/lib/labelwatch-recent-watch')
def run(cfg, now=None, gather_fn=gather):
    now, sd = now or datetime.now(UTC), state_dir(cfg)
    (sd / 'intents').mkdir(parents=True, exist_ok=True)
    st, pre, today = read_json(sd / 'state.json') or {}, [], now.date().isoformat()
    prev = ts(st.get('evaluated_at'))
    if prev and secs(now - prev) > 900:  # L10: three missed cadences, seen when the watcher comes back
        pre.append(make_intent('notice', 'L10', 'obligation-evaluator-stale', P + '-watch', 'no evaluation for %d min (last %s); '
                               'obligations were not evaluated in between' % (secs(now - prev) // 60, iso(prev)), 'notice', now))
    f = gather_fn(cfg, now)
    results, intents = evaluate(f, st, now, cfg)
    if now.time() >= dtime(0, 40) and st.setdefault('digest_day', today) != today:  # a missing digest exposes a silent watcher
        st['digest_day'] = today
        pre.append(make_intent('notice', 'L10', 'daily-digest', P + '-watch', 'daily digest %s: maintenance %s, window %s; open: %s' % (
            today, results['L3']['verdict'], results['L4']['verdict'], ', '.join(sorted(st.get('conditions', {}))) or 'none'), 'notice', now))
    intents, obs = pre + intents, sd / 'observations.jsonl'
    if obs.exists() and obs.stat().st_size > ROTATE_BYTES: os.replace(obs, sd / 'observations.jsonl.1')
    with obs.open('a') as fh:
        fh.write(json.dumps({'schema': 'labelwatch-recent-watch.observation.v1', 'evaluated_at': iso(now), 'obligations_digest': DIGEST,
                             'sources': f['sources'], 'markers': f['markers'], 'obligations': results,
                             'intents': ['%s %s' % (i['dedup_key'], i['action']) for i in intents]}, sort_keys=True, default=str) + '\n')
    emit(cfg, sd, st, intents, now)
    st['evaluated_at'] = iso(now)
    write_json(sd / 'state.json', st)
    write_json(sd / 'report.json', {'evaluated_at': iso(now), 'obligations_digest': DIGEST, 'open_conditions': st.get('conditions', {}),
                                    'verdicts': {k: v['verdict'] for k, v in results.items()}})
    return results
def report_failure(cfg, env=os.environ):
    """ExecStopPost hook: one Slack line (and a retained intent) when the watcher run itself failed (L10 self-visibility)."""
    if env.get('SERVICE_RESULT', 'success') == 'success': return 0
    intent = make_intent('notice', 'L10', 'watcher-run-failed', P + '-watch', 'watcher run failed (%s, %s=%s); obligations are not being '
                         'evaluated' % (env.get('SERVICE_RESULT'), env.get('EXIT_CODE'), env.get('EXIT_STATUS')), 'notice', datetime.now(UTC))
    intent['delivery'] = deliver(cfg, intent)
    try: write_json(state_dir(cfg) / 'intents' / ('%s-run-failed.json' % intent['created_at'].replace(':', '')), intent)
    except OSError: pass
    return 0
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--config', help='KEY=VALUE file overriding the environment (e.g. the NC6 fixture profile)')
    ap.add_argument('--state-dir'), ap.add_argument('--now', help='evaluation clock, ISO-8601 UTC (fixtures only)')
    ap.add_argument('--report-failure', action='store_true', help='ExecStopPost hook')
    a = ap.parse_args(argv)
    cfg = {**DEFAULTS, **{k: v for k, v in os.environ.items() if k.startswith('WATCH_')}}
    cfg.update((load_env(a.config) or {}) if a.config else {})
    cfg.update({'WATCH_STATE_DIR': a.state_dir} if a.state_dir else {})
    if a.report_failure: return report_failure(cfg)
    print(json.dumps({k: v['verdict'] for k, v in run(cfg, ts(a.now) if a.now else None).items()}, sort_keys=True))
    return 0


if __name__ == '__main__': sys.exit(main())
