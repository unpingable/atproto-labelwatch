"""Bounded backup/restore/compaction mechanics; never authorizes cleanup.

Precondition: exclusive application/OS writer custody, stable trusted parent
directories, and a held quiescence cut. These functions do not acquire service
authority. They never replace or remove the enrolled source database.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sqlite3
import stat

from .maintenance_manifest import VerificationRefused, offline_application_verify


def identity(path: Path) -> dict:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise VerificationRefused('artifact is not a single-link regular file')
        digest = hashlib.sha256()
        while block := os.read(fd, 1024 * 1024):
            digest.update(block)
        after = os.fstat(fd)
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise VerificationRefused('artifact changed while measuring')
        current = path.lstat()
        if (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino):
            raise VerificationRefused('artifact pathname changed while measuring')
        return {'device': after.st_dev, 'inode': after.st_ino, 'bytes': after.st_size,
                'uid': after.st_uid, 'gid': after.st_gid,
                'mode': stat.S_IMODE(after.st_mode), 'sha256': digest.hexdigest()}
    finally:
        os.close(fd)


def _sync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def _new_file(path: Path) -> None:
    if path.parent.resolve(strict=True) != path.parent:
        raise VerificationRefused('artifact parent must be exact physical directory')
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    _sync(path)


def readonly(path: Path) -> sqlite3.Connection:
    if path.resolve(strict=True) != path:
        raise VerificationRefused('database path must be exact physical file')
    identity(path)
    conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)
    conn.execute('PRAGMA query_only=ON')
    conn.row_factory = sqlite3.Row
    return conn


def verify_closed(path: Path, *, revision: str, expected: dict) -> dict:
    before = identity(path)
    with readonly(path) as conn:
        conn.execute('BEGIN')
        observed = offline_application_verify(conn, application_revision=revision)
    conn.close()
    if observed != expected:
        raise VerificationRefused('logical/application verification differs from quiescence cut')
    after = identity(path)
    if before != after:
        raise VerificationRefused('artifact identity changed during verification')
    return {'identity': after, 'verification': observed}


def copy_restore_verify(source: Path, backup: Path, restored: Path, *, revision: str,
                        expected: dict) -> dict:
    """Retain partial artifacts on every failure; no existing output is overwritten.

    A verified result reports recoverability of this exact copy. The caller must
    additionally establish a separate backup filesystem and custody before it
    can be used in any cleanup prerequisite.
    """
    before = identity(source)
    _new_file(backup)
    with readonly(source) as source_conn, sqlite3.connect(backup) as destination:
        source_conn.backup(destination)
    source_conn.close()
    destination.close()
    _sync(backup)
    backup_result = verify_closed(backup, revision=revision, expected=expected)
    _new_file(restored)
    with readonly(backup) as backup_conn, sqlite3.connect(restored) as restored_conn:
        backup_conn.backup(restored_conn)
    backup_conn.close()
    restored_conn.close()
    _sync(restored)
    restore_result = verify_closed(restored, revision=revision, expected=expected)
    if identity(source) != before:
        raise VerificationRefused('source changed across backup/restore cut')
    return {'source': before, 'backup': backup_result, 'restored': restore_result,
            'separate_filesystem': before['device'] != backup_result['identity']['device'],
            'cleanup_authorized': False}


def compact_verified(source: Path, staging: Path, *, revision: str, expected: dict) -> dict:
    before = identity(source)
    # SQLite VACUUM INTO requires a new/empty destination; exclusive creation
    # prevents this bounded procedure overwriting an existing candidate.
    _new_file(staging)
    with readonly(source) as conn:
        # query_only forbids VACUUM INTO even though source is unchanged. The
        # source connection remains mode=ro; only the explicit staging is written.
        conn.execute('PRAGMA query_only=OFF')
        conn.execute('VACUUM INTO ?', (str(staging),))
    conn.close()
    os.chown(staging, before['uid'], before['gid'])
    os.chmod(staging, before['mode'])
    _sync(staging)
    verified = verify_closed(staging, revision=revision, expected=expected)
    if identity(source) != before:
        raise VerificationRefused('source changed across compact staging cut')
    return {'source': before, 'staging': verified, 'replacement_accepted': False}


def space_prerequisites(source: Path, backup_directory: Path, *, operating_margin: int) -> dict:
    """Conservative full-size backup + replacement + restore, no freelist guess.

    Original remains allocated and is not counted as already reclaimed. Backup
    and disposable restore require two full source sizes on the backup volume.
    """
    if type(operating_margin) is not int or operating_margin < 0:
        raise VerificationRefused('nonnegative explicit operating margin required')
    source_id = identity(source)
    if not backup_directory.is_dir() or backup_directory.resolve() != backup_directory:
        raise VerificationRefused('exact backup directory required')
    if source_id['device'] == backup_directory.stat().st_dev:
        raise VerificationRefused('backup must be on separately identified filesystem')
    target = os.statvfs(source.parent)
    backup = os.statvfs(backup_directory)
    target_free = target.f_bavail * target.f_frsize
    backup_free = backup.f_bavail * backup.f_frsize
    target_required = source_id['bytes'] + operating_margin
    backup_required = 2 * source_id['bytes'] + operating_margin
    if target_free < target_required or backup_free < backup_required:
        raise VerificationRefused('insufficient temporary space before mutation')
    return {'target_device': source_id['device'], 'backup_device': backup_directory.stat().st_dev,
            'target_free': target_free, 'backup_free': backup_free,
            'target_required': target_required, 'backup_required': backup_required,
            'operating_margin': operating_margin}
