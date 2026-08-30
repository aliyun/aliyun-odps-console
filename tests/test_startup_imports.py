"""Structural regressions for fast, offline CLI startup."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "argv",
    [
        ["--version"],
        ["--help"],
        ["session", "show", "--json"],
        ["agent", "context", "--json"],
    ],
)
def test_offline_command_does_not_import_pyodps(tmp_path: Path, argv: list[str]) -> None:
    """Local commands must remain usable without loading PyODPS's dependency tree."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: test_project\n"
        "default_region: cn-hangzhou\n"
        f"state_dir: {tmp_path / 'state'}\n"
        f"cache_dir: {tmp_path / 'cache'}\n"
        "auth:\n"
        "  provider: access_key\n"
        "  access_id: test_access_id\n"
        "  secret_access_key: test_secret\n"
        "  project: test_project\n"
        "  endpoint: https://service.cn-hangzhou.maxcompute.aliyun.com/api\n",
        encoding="utf-8",
    )
    command_argv = ["--config", str(config_path), *argv]
    script = r'''
import builtins
import json
import sys

real_import = builtins.__import__
attempted_odps_imports = []

def guarded_import(name, *args, **kwargs):
    if name == "odps" or name.startswith("odps."):
        attempted_odps_imports.append(name)
        raise AssertionError(f"offline command imported {name}")
    return real_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
from maxc_cli.cli import main

try:
    code = main(json.loads(sys.argv[1]))
except SystemExit as exc:
    code = exc.code

assert attempted_odps_imports == [], attempted_odps_imports
assert not any(name == "odps" or name.startswith("odps.") for name in sys.modules)
raise SystemExit(code)
'''
    env = dict(os.environ)
    env["PYTHONPATH"] = str(_REPO_ROOT / "src")
    completed = subprocess.run(
        [sys.executable, "-c", script, json.dumps(command_argv)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_cli_maxc_app_export_remains_patchable(monkeypatch, tmp_path: Path) -> None:
    """Lazy loading must not remove the long-standing cli.MaxCApp patch seam."""
    import maxc_cli.cli as cli
    from maxc_cli.models import Envelope

    constructed = []

    class _StubApp:
        def __init__(self, **kwargs):
            constructed.append(kwargs)

        def session_show(self, *, target_config_path):
            return Envelope(
                command="session.show",
                status="success",
                data={"project": "stub", "schema": None},
            )

    monkeypatch.setattr(cli, "MaxCApp", _StubApp)
    stdout = StringIO()
    code = cli.run(
        ["session", "show", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )

    assert code == 0
    assert constructed and constructed[0]["load_backend"] is False
    assert json.loads(stdout.getvalue())["status"] == "success"
