import hashlib
import json
import os
from pathlib import Path
import runpy
import tempfile

import pytest

from labelwatch.maintenance_step import execute, read_record


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_refused_initial_diagnosis_is_retained(tmp_path):
    source = Path(__file__).resolve().parents[1]
    module = runpy.run_path(str(source / 'scripts/m3_fixture_enrollment.py'))
    initializer = module['initialize']
    actual = initializer.__globals__['diagnose']
    def unavailable(*args, **kwargs):
        record = actual(*args, **kwargs)
        record['entry_disposition'] = 'NOT_OBSERVABLE'
        record['unknowns'].append({'slot': 'fixture_control', 'reason': 'unavailable'})
        return record
    initializer.__globals__['diagnose'] = unavailable
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-entry-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        target = tmp_path / 'fixture'
        with pytest.raises(module['VerificationRefused']):
            initializer(target, Path(temporary), 'a' * 40)
        record = json.loads((target / 'entry-diagnosis.json').read_bytes())
        assert record['entry_disposition'] == 'NOT_OBSERVABLE'
        assert record['facts']['sqlite']['freelist_count'] >= 64
        assert list((target / 'journal').iterdir()) == []
        assert list((target / 'enrollment-candidates').iterdir()) == []


def test_fifteen_case_inventory_includes_every_closed_interruption_cut():
    source = Path(__file__).resolve().parents[1]
    cases = json.loads((source / 'qualification/m3-admission/cases.json').read_text())
    wrapper = runpy.run_path(str(source / 'qualification/m3-admission/interrupted_step.py'))
    assert [case['id'] for case in cases['cases']] == list(range(1, 16))
    assert {cut for row in cases['controller_cuts'] for cut in row['cuts']} == set(wrapper['CUTS'])
    assert {'rollback-pre-ingest', 'reconcile-cleanup'} <= {action for row in cases['controller_cuts'] for action in row['actions']}


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_fixed_interruption_candidate_binds_cut_and_exact_input(tmp_path):
    source = Path(__file__).resolve().parents[1]
    module = runpy.run_path(str(source / 'scripts/m3_fixture_enrollment.py'))
    wrapper = runpy.run_path(str(source / 'qualification/m3-admission/interrupted_step.py'))
    assert tuple(module['INTERRUPTION_CUTS']) == tuple(wrapper['CUTS'])
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-cut-enroll-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        target = tmp_path / 'fixture'
        module['initialize'](target, Path(temporary), 'a' * 40)
        entry_path = target / 'entry-diagnosis.json'
        entry_raw = entry_path.read_bytes()
        entry = json.loads(entry_raw)
        assert entry['facts']['sqlite']['freelist_count'] >= 64
        for disposition in ('NOT_NEEDED', 'NOT_OBSERVABLE'):
            entry['entry_disposition'] = disposition
            entry_path.write_text(json.dumps(entry))
            with pytest.raises(module['VerificationRefused']):
                module['seal'](target, 'stage', None, source, Path('/usr/bin/python3'), 'after_started')
            assert list((target / 'enrollment-candidates').iterdir()) == []
            assert list((target / 'journal').iterdir()) == []
        entry_path.write_bytes(entry_raw)
        candidate = module['seal'](target, 'stage', None, source, Path('/usr/bin/python3'), 'after_started')
        unit = Path(candidate['step']).parent / candidate['unit']
        assert candidate['qualification_interruption'] == 'after_started'
        assert candidate['unit'].endswith('-q-after_started.service')
        assert 'interrupted_step.py --step ' in unit.read_text()
        assert '--cut after_started\n' in unit.read_text()
        assert hashlib.sha256(unit.read_bytes()).hexdigest() == candidate['unit_sha256']


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_sealed_fixture_candidate_is_not_authority_and_preserves_repeated_inputs(tmp_path):
    source = Path(__file__).resolve().parents[1]
    module = runpy.run_path(str(source / 'scripts/m3_fixture_enrollment.py'))
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-enroll-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        target = tmp_path / 'fixture'
        prepared = module['initialize'](target, Path(temporary), 'a' * 40)
        assert prepared['status'] == 'PREPARED_NOT_AUTHORIZED'
        candidate = module['seal'](target, 'stage', None, source, Path('/usr/bin/python3'))
        step = Path(candidate['step'])
        assert hashlib.sha256(step.read_bytes()).hexdigest() == candidate['step_sha256']
        unit = step.parent / candidate['unit']
        assert hashlib.sha256(unit.read_bytes()).hexdigest() == candidate['unit_sha256']
        assert 'Restart=no\n' in unit.read_text()
        assert candidate['status'] == 'NOT_ENROLLED_NOT_AUTHORIZED'
        assert execute(step, candidate['step_sha256'])['disposition'] == 'STAGED_NOT_INSTALLED'
        previous = Path(candidate['expected_result'])
        next_candidate = module['seal'](target, 'replace', previous, source, Path('/usr/bin/python3'))
        bound, _ = read_record(Path(next_candidate['step']))
        assert bound['predecessor_sha256'] == hashlib.sha256(previous.read_bytes()).hexdigest()
        assert execute(Path(next_candidate['step']), next_candidate['step_sha256'])['disposition'] == 'REPLACEMENT_ACCEPTED'
        before = step.read_bytes()
        with pytest.raises(FileExistsError):
            module['seal'](target, 'stage', None, source, Path('/usr/bin/python3'))
        assert step.read_bytes() == before
        with pytest.raises(FileExistsError):
            module['initialize'](target, Path(temporary), 'a' * 40)
