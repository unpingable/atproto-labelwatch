"""Deterministic publication cuts; no maintenance action is retried."""
import json
import os
from pathlib import Path
import stat
import threading

import pytest

from labelwatch.maintenance_hold import _publish_ready
from labelwatch.maintenance_step import read_record


def test_partial_private_write_is_not_visible_as_readiness(tmp_path, monkeypatch):
    final = tmp_path / 'main-123.ready.json'
    raw = b'{"schema":"qualification-fixture","value":"complete"}\n'
    arrived, release = threading.Event(), threading.Event()
    original = os.fdopen
    errors = []
    class Writer:
        def __init__(self, fd, mode):
            self.stream = original(fd, mode)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.stream.close()
        def write(self, value):
            split = len(value) // 2
            self.stream.write(value[:split])
            self.stream.flush()
            arrived.set()
            assert release.wait(3)
            self.stream.write(value[split:])
        def flush(self):
            self.stream.flush()
        def fileno(self):
            return self.stream.fileno()
    monkeypatch.setattr(os, 'fdopen', Writer)
    def publish():
        try:
            _publish_ready(final, raw)
        except BaseException as error:
            errors.append(error)
    thread = threading.Thread(target=publish)
    thread.start()
    try:
        assert arrived.wait(3)
        assert list(tmp_path.glob('*.ready.json')) == []
        pending = list(tmp_path.glob('.ready-pending-*'))
        assert len(pending) == 1 and pending[0].read_bytes() == raw[:len(raw) // 2]
    finally:
        release.set()
        thread.join(3)
    assert not thread.is_alive() and errors == []
    assert final.read_bytes() == raw and read_record(final)[0]['value'] == 'complete'
    assert list(tmp_path.glob('.ready-pending-*')) == []


def test_existing_final_record_is_not_replaced(tmp_path):
    final = tmp_path / 'main-123.ready.json'
    original = b'{"role":"original"}\n'
    final.write_bytes(original)
    with pytest.raises(FileExistsError):
        _publish_ready(final, b'{"role":"substitution"}\n')
    assert final.read_bytes() == original
    assert list(tmp_path.glob('.ready-pending-*')) == []


def test_concurrent_publishers_have_one_original_no_overwrite(tmp_path, monkeypatch):
    final = tmp_path / 'main-123.ready.json'
    barrier = threading.Barrier(2)
    original = os.link
    results = []
    def synchronized(*args, **kwargs):
        barrier.wait(timeout=3)
        return original(*args, **kwargs)
    monkeypatch.setattr(os, 'link', synchronized)
    def publish(raw):
        try:
            _publish_ready(final, raw)
            results.append(('published', raw))
        except FileExistsError:
            results.append(('refused', raw))
    threads = [threading.Thread(target=publish, args=(json.dumps({'writer': n}).encode(),)) for n in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(4)
        assert not thread.is_alive()
    assert sorted(result[0] for result in results) == ['published', 'refused']
    assert final.read_bytes() == next(raw for status, raw in results if status == 'published')


def test_failed_file_sync_never_publishes_final(tmp_path, monkeypatch):
    def failed(fd):
        raise OSError('qualification interrupted before publication')
    monkeypatch.setattr(os, 'fsync', failed)
    final = tmp_path / 'main-123.ready.json'
    with pytest.raises(OSError):
        _publish_ready(final, b'{"complete":true}')
    assert not final.exists() and list(tmp_path.iterdir()) == []


def test_symlink_final_is_not_followed_or_replaced(tmp_path):
    original = tmp_path / 'retained.json'
    original.write_bytes(b'{"original":true}')
    final = tmp_path / 'main-123.ready.json'
    final.symlink_to(original)
    with pytest.raises(FileExistsError):
        _publish_ready(final, b'{"substituted":true}')
    assert final.is_symlink() and original.read_bytes() == b'{"original":true}'


def test_file_sync_precedes_publication_and_directory_sync_follows(tmp_path, monkeypatch):
    events = []
    sync, link = os.fsync, os.link
    def observe_sync(fd):
        events.append('directory' if stat.S_ISDIR(os.fstat(fd).st_mode) else 'file')
        return sync(fd)
    def observe_link(*args, **kwargs):
        assert events == ['file']
        events.append('publish')
        return link(*args, **kwargs)
    monkeypatch.setattr(os, 'fsync', observe_sync)
    monkeypatch.setattr(os, 'link', observe_link)
    _publish_ready(tmp_path / 'main-123.ready.json', b'{"complete":true}')
    assert events == ['file', 'publish', 'directory']


def test_interrupted_private_specimen_ignored_malformed_final_still_refused(tmp_path):
    (tmp_path / '.ready-pending-interrupted').write_bytes(b'{')
    assert list(tmp_path.glob('*.ready.json')) == []
    final = tmp_path / 'main-123.ready.json'
    final.write_bytes(b'')
    with pytest.raises(json.JSONDecodeError):
        read_record(final)
    with pytest.raises(FileExistsError):
        _publish_ready(final, b'{"complete":true}')
    assert final.read_bytes() == b''
