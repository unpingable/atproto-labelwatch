#!/usr/bin/env python3
"""Qualification-only restored-row substitution before real application verify.

Only the exact campaign-owned restored copy is changed. No production entrypoint
uses this script. The expected result is real helper refusal after actual backup
and restore; script exit alone is not proof that the intended cut was reached.
"""
import argparse
import hashlib
from pathlib import Path
import sqlite3

from labelwatch import maintenance_artifacts as artifacts
from labelwatch import maintenance_step as helper


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--step', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    args = parser.parse_args()
    step, raw = helper.read_record(args.step)
    if hashlib.sha256(raw).hexdigest() != args.expected_sha256 or step['action'] != 'stage':
        raise ValueError('exact enrolled stage fixture required')
    verify = artifacts.verify_closed
    reached = False
    def verify_substitution(path, **kwargs):
        nonlocal reached
        if path == Path(step['restore']):
            with sqlite3.connect(path) as connection:
                changed = connection.execute("UPDATE maintenance_types SET value='fixture-restored-substitution' WHERE key='text'").rowcount
            connection.close()
            if changed != 1:
                raise ValueError('declared fixture row absent')
            reached = True
            print(helper.canonical({'schema': 'labelwatch.m3-restored-substitution/v1',
                'step_sha256': args.expected_sha256, 'restored_path': str(path),
                'cut': 'actual_restore_before_application_verification'}).decode(), flush=True)
        return verify(path, **kwargs)
    artifacts.verify_closed = verify_substitution
    try:
        helper.execute(args.step, args.expected_sha256)
    except artifacts.VerificationRefused:
        if reached:
            return 78
        raise
    raise RuntimeError('restored substitution was not refused')


if __name__ == '__main__':
    raise SystemExit(main())
