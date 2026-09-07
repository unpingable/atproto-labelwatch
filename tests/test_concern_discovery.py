import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "discover_concerns.py"
SPEC = importlib.util.spec_from_file_location("discover_concerns", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_discovery_uses_manifest_contents_not_project_lists(tmp_path):
    repo = tmp_path / "arbitrary-project"
    (repo / ".ops").mkdir(parents=True)
    (repo / ".ops" / "concerns.toml").write_text(
        'schema = "project.concerns/v1"\nproject = "arbitrary"\n'
        '[[concerns]]\nid = "arbitrary.only.concern"\nquestion = "arbitrary.question/v1"\n'
        'profile = "arbitrary.profile/v1"\nrequired = true\ndescription = "Declared by the fixture."\n',
        encoding="utf-8",
    )
    (repo / ".ops" / "observation.toml").write_text(
        'schema = "project.observation-binding/v1"\n'
        'producer = "arbitrary.status"\noutput_schema = "project.ops.status/v1"\n'
        'kind = "exec/v1"\n'
        'argv = ["python3", "-c", "import json; print(json.dumps({\'schema\': \'project.ops.status/v1\'}))"]\n',
        encoding="utf-8",
    )
    result = MODULE.discover([tmp_path], execute_status=True)
    assert result["repositories"][0]["required_concerns"] == ["arbitrary.only.concern"]
    assert result["repositories"][0]["status"]["available"] is True


def test_workspace_discovers_both_real_repositories_without_known_concern_ids():
    labelwatch = Path(__file__).resolve().parents[1]
    driftwatch = labelwatch.parent / "driftwatch"
    if not (driftwatch / ".ops" / "concerns.toml").exists():
        pytest.skip("sibling Driftwatch checkout is not present")
    result = MODULE.discover([labelwatch, driftwatch], execute_status=True, timeout=30)
    found = {item["project"]: item for item in result["repositories"]}
    assert set(found) == {"labelwatch", "driftwatch"}
    assert all(item["required_concerns"] for item in found.values())
    assert all(item["status"]["available"] for item in found.values())
    assert all(item["status"]["payload"]["schema"] == "project.ops.status/v1" for item in found.values())
    assert (labelwatch / ".ops" / "status.schema.json").read_bytes() == (driftwatch / ".ops" / "status.schema.json").read_bytes()
