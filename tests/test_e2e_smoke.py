"""End-to-end smoke test: bootstrap → session → query lifecycle using FakeODPS."""
import json
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

from maxc_cli.cli import run


def clear_odps_env(monkeypatch) -> None:
    import maxc_cli.backend as backend_module
    for aliases in backend_module.ODPS_ENV_ALIASES.values():
        for alias in aliases:
            monkeypatch.delenv(alias, raising=False)


def isolate_home(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))


def run_cmd(tmp_path: Path, config_path: Path, argv: list) -> tuple:
    stdout, stderr = StringIO(), StringIO()
    code = run(["--config", str(config_path), *argv], cwd=tmp_path, stdout=stdout, stderr=stderr)
    return code, json.loads(stdout.getvalue()), stderr.getvalue()


class FakeODPS:
    def __init__(self, access_id=None, **kwargs):
        if hasattr(access_id, "access_id"):
            access_id = getattr(access_id, "access_id", None)
        self.account = type("Account", (), {"access_id": access_id})()
        self.project = kwargs.get("project")
        self.endpoint = kwargs.get("endpoint")

    def get_project(self, project):
        return type("Project", (), {"owner": f"ALIYUN$mock_user_{project}"})()

    def execute_security_query(self, query, project=None):
        if query == "whoami":
            return {"DisplayName": f"ALIYUN$mock_user_{project or self.project}", "ID": "1", "SourceIP": "127.0.0.1"}
        raise NotImplementedError


def test_e2e_bootstrap_session_query(tmp_path: Path, monkeypatch) -> None:
    """Full lifecycle: login → whoami → session set → session show → agent context."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    config_path = tmp_path / "config.yaml"

    # 1. auth login --no-validate
    code, payload, _ = run_cmd(tmp_path, config_path, [
        "auth", "login",
        "--access-id", "AK_SMOKE",
        "--secret-access-key", "SK_SMOKE",
        "--project", "smoke_project",
        "--endpoint", "http://service.smoke.maxcompute.aliyun.com/api",
        "--no-validate", "--json",
    ])
    assert code == 0, f"login failed: {payload}"
    assert payload["status"] == "success"
    assert payload["data"]["persistence"]["saved"] is True

    # 2. auth whoami — should now be authenticated via saved config
    code, payload, _ = run_cmd(tmp_path, config_path, ["auth", "whoami", "--json"])
    assert code == 0, f"whoami failed: {payload}"
    identity = payload["data"]["identity"]
    assert identity["authenticated"] is True
    assert identity["validation_status"] == "verified"
    assert identity["project"] == "smoke_project"

    # 3. session set — switch to a different project
    code, payload, _ = run_cmd(tmp_path, config_path, [
        "session", "set", "--project", "other_project", "--schema", "my_schema", "--json",
    ])
    assert code == 0, f"session set failed: {payload}"

    # 4. session show — verify override
    code, payload, _ = run_cmd(tmp_path, config_path, ["session", "show", "--json"])
    assert code == 0, f"session show failed: {payload}"
    project_info = payload["data"]["project"]
    schema_info = payload["data"]["schema"]
    # session show always returns {"value": ..., "source": ...} dicts; assert
    # the shape directly so a future flattening regression fails this test.
    assert isinstance(project_info, dict) and isinstance(schema_info, dict), payload
    assert project_info["value"] == "other_project"
    assert schema_info["value"] == "my_schema"

    # 5. session unset — revert
    code, payload, _ = run_cmd(tmp_path, config_path, ["session", "unset", "--json"])
    assert code == 0, f"session unset failed: {payload}"

    # 6. session show — should be back to config defaults
    code, payload, _ = run_cmd(tmp_path, config_path, ["session", "show", "--json"])
    assert code == 0
    project_info = payload["data"]["project"]
    assert isinstance(project_info, dict), payload
    assert project_info["value"] == "smoke_project"

    # 7. agent context — quick config summary
    code, payload, _ = run_cmd(tmp_path, config_path, ["agent", "context", "--json"])
    assert code == 0, f"agent context failed: {payload}"
    assert payload["data"]["context"]["project"] == "smoke_project"

    # 8. cache status — should work even without built cache
    code, payload, _ = run_cmd(tmp_path, config_path, ["cache", "status", "--json"])
    assert code == 0, f"cache status failed: {payload}"
