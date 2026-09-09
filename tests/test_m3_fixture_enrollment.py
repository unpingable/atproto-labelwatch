import hashlib
import os
from pathlib import Path
import runpy
import tempfile

import pytest

from labelwatch.maintenance_step import execute, read_record


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
