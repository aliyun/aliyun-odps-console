"""Packaging metadata must share the runtime version source."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from runpy import run_path

import pytest

pytestmark = pytest.mark.unit


def test_setup_metadata_matches_runtime_without_importing_setuptools() -> None:
    root = Path(__file__).resolve().parents[1]
    runtime_version = run_path(str(root / "src" / "maxc_cli" / "__init__.py"))["__version__"]
    probe = """
import json
import runpy
import sys
import types

captured = {}
fake_setuptools = types.ModuleType("setuptools")
fake_setuptools.find_packages = lambda **kwargs: []
fake_setuptools.setup = lambda **kwargs: captured.update(kwargs)
sys.modules["setuptools"] = fake_setuptools
runpy.run_path("setup.py", run_name="__main__")
print(json.dumps({
    "version": captured["version"],
    "python_requires": captured["python_requires"],
    "include_package_data": captured["include_package_data"],
    "package_data": captured["package_data"],
}))
"""
    result = subprocess.run(
        [sys.executable, "-S", "-c", probe],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    metadata = json.loads(result.stdout)

    assert metadata == {
        "version": runtime_version,
        "python_requires": ">=3.9",
        "include_package_data": False,
        "package_data": {
            "maxc_cli": [
                "skills/SKILL.md",
                "skills/references/*.md",
                "skills/agents/*.yaml",
            ]
        },
    }
    assert 'version="' not in (root / "setup.py").read_text(encoding="utf-8")


def test_manifest_has_no_retired_skill_tree() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = (root / "MANIFEST.in").read_text(encoding="utf-8")

    assert "recursive-include src/maxc_cli/skills *" in manifest
    assert "skills/use-maxc-cli" not in manifest
    assert not (root / "use-maxc-cli.zip").exists()
