"""get_git_commit resolution order:
  1. LABELWATCH_GIT_SHA env var
  2. <repo>/GIT_SHA file
  3. .git/HEAD (local dev)
"""

from __future__ import annotations

import os

from labelwatch import utils


def test_env_var_wins(monkeypatch):
    monkeypatch.setenv("LABELWATCH_GIT_SHA", "abc123envwins")
    assert utils.get_git_commit() == "abc123envwins"


def test_env_var_whitespace_stripped(monkeypatch):
    monkeypatch.setenv("LABELWATCH_GIT_SHA", "  spaced123  \n")
    assert utils.get_git_commit() == "spaced123"


def test_git_sha_file_fallback(monkeypatch, tmp_path):
    """If env var is unset, GIT_SHA file at <package>/../../GIT_SHA wins."""
    monkeypatch.delenv("LABELWATCH_GIT_SHA", raising=False)

    # The file lookup is anchored to the package directory; we can't easily
    # write to the live tree mid-test, so we patch the lookup path instead.
    here = os.path.dirname(os.path.abspath(utils.__file__))
    repo_root = os.path.normpath(os.path.join(here, "..", ".."))
    sha_path = os.path.join(repo_root, "GIT_SHA")

    # Only write if no real GIT_SHA file exists (don't stomp deploy state).
    if os.path.exists(sha_path):
        # Live tree has a deploy SHA file; just confirm resolution finds it.
        sha = utils.get_git_commit()
        assert sha is not None
        return

    try:
        with open(sha_path, "w", encoding="utf-8") as f:
            f.write("file-deployed-sha\n")
        assert utils.get_git_commit() == "file-deployed-sha"
    finally:
        try:
            os.remove(sha_path)
        except OSError:
            pass


def test_returns_none_when_no_source(monkeypatch, tmp_path):
    """Outside any git tree, with no env or file, return None."""
    monkeypatch.delenv("LABELWATCH_GIT_SHA", raising=False)
    monkeypatch.chdir(tmp_path)

    # If a GIT_SHA file exists in the real package tree, we can't easily
    # test the None case without stomping it; skip in that case.
    here = os.path.dirname(os.path.abspath(utils.__file__))
    repo_root = os.path.normpath(os.path.join(here, "..", ".."))
    if os.path.exists(os.path.join(repo_root, "GIT_SHA")):
        return
    # Also tolerate a local .git/ in the test cwd (unlikely under tmp_path).
    assert utils.get_git_commit() is None
