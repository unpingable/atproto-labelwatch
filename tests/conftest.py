"""Deterministic local filesystem observations for maintenance qualification."""
import os
from pathlib import Path

import pytest


def _fixture_root(path):
    value = Path(path).resolve()
    chain = (value, *value.parents)
    for candidate in chain:
        if candidate.name.startswith('test_') and candidate.parent.name.startswith('pytest-'):
            return candidate
        if candidate.parent == Path('/dev/shm') and candidate.name.startswith('labelwatch-m3-'):
            return candidate
    return None


def _allocated_bytes(root):
    total = 0
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                total += (Path(directory) / name).stat().st_blocks * 512
            except FileNotFoundError:
                pass
    return total


@pytest.fixture(autouse=True)
def deterministic_fixture_filesystem_availability(monkeypatch):
    """Exclude unrelated shared-volume motion while retaining fixture effects."""
    actual_statvfs = os.statvfs
    capacity_bytes = 256 * 1024 * 1024

    def observed(path):
        actual = actual_statvfs(path)
        root = _fixture_root(path)
        if root is None:
            return actual
        blocks = capacity_bytes // actual.f_frsize
        used = (_allocated_bytes(root) + actual.f_frsize - 1) // actual.f_frsize
        available = max(0, blocks - used)
        return os.statvfs_result(
            tuple(actual[:2]) + (blocks, available, available) + tuple(actual[5:]))

    monkeypatch.setattr(os, 'statvfs', observed)
