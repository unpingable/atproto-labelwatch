#!/usr/bin/env python3
"""Qualification-only abrupt exit at a sealed cut in the real helper.

Not a production entrypoint or retry controller. The positive fixture must use
the ordinary helper. Enroll this exact script/import chain and cut separately;
its nonzero exit is intentional evidence, not an application acceptance claim.
"""
import argparse
import hashlib
import os
from pathlib import Path

from labelwatch import maintenance_artifacts as artifacts
from labelwatch import maintenance_step as helper

CUTS = ('before_started', 'after_started', 'before_terminal', 'after_terminal',
        'after_original_rename', 'after_replacement_rename', 'after_backup_sync',
        'after_restore_sync', 'after_staging_sync', 'before_cleanup_unlink',
        'after_cleanup_unlink', 'after_cleanup_authorized', 'after_release_record')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--step', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--cut', choices=CUTS, required=True)
    args = parser.parse_args()
    step, raw = helper.read_record(args.step)
    if hashlib.sha256(raw).hexdigest() != args.expected_sha256:
        raise ValueError('exact enrolled qualification input differs')

    def cut(name):
        if args.cut == name:
            print(helper.canonical({'schema': 'labelwatch.m3-interruption-fixture/v1',
                'cut': name, 'step_sha256': args.expected_sha256,
                'result': 'INTENTIONAL_PROCESS_EXIT_NO_CLEANUP'}).decode(), flush=True)
            os._exit(77)  # deliberately skip Python finally handlers

    retained = helper.retain
    def retain(path, value):
        if path.name.endswith('.started.json'):
            cut('before_started')
        if path.name.endswith('.completed.json'):
            cut('before_terminal')
        retained(path, value)
        if path.name.endswith('.started.json'):
            cut('after_started')
        if path.name.endswith('.completed.json'):
            cut('after_terminal')
        if path.name.endswith('.cleanup-authorized.json'):
            cut('after_cleanup_authorized')
        if path == helper.hold_paths(step['source'])[1]:
            cut('after_release_record')
    helper.retain = retain

    renamed = helper.rename_no_replace
    def rename(source, destination):
        renamed(source, destination)
        if destination == Path(step['original']):
            cut('after_original_rename')
        if destination == Path(step['source']):
            cut('after_replacement_rename')
    helper.rename_no_replace = rename

    synced = artifacts._sync
    sync_counts = {}
    def sync(path):
        synced(path)
        sync_counts[path] = sync_counts.get(path, 0) + 1
        # _new_file syncs the exclusive empty candidate first; the second sync
        # follows the actual copy/restore/staging write and file+directory fsync.
        if sync_counts[path] == 2:
            for field, name in [('backup', 'after_backup_sync'), ('restore', 'after_restore_sync'),
                                ('staging', 'after_staging_sync')]:
                if path == Path(step[field]):
                    cut(name)
    artifacts._sync = sync
    unlinked = Path.unlink
    def unlink(path, *arguments, **keywords):
        if path == Path(step['original']):
            cut('before_cleanup_unlink')
        result = unlinked(path, *arguments, **keywords)
        if path == Path(step['original']):
            cut('after_cleanup_unlink')
        return result
    Path.unlink = unlink
    helper.execute(args.step, args.expected_sha256)
    # A mismatched cut/action must not be credited as a tested interruption.
    raise RuntimeError('requested qualification cut was not reached')


if __name__ == '__main__':
    main()
