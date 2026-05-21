"""Smoke tests against the PyInstaller onedir bundle in dist/maxc/.

Requires that `make build-bin` or `scripts/build_release.sh` has been run.
Skips cleanly if the bundle isn't present.
"""
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BUNDLE = REPO / "dist" / "maxc"
BIN = BUNDLE / ("maxc.exe" if os.name == "nt" else "maxc")

pytestmark = pytest.mark.skipif(
    not BIN.exists(),
    reason=f"PyInstaller bundle not built at {BIN}; run `make build-bin` first",
)


def _run(*args, **kwargs):
    return subprocess.run(
        [str(BIN), *args],
        capture_output=True,
        text=True,
        timeout=60,
        **kwargs,
    )


def test_version_is_semver():
    r = _run("--version")
    assert r.returncode == 0, r.stderr
    assert re.search(r"\d+\.\d+\.\d+", r.stdout), f"no semver in: {r.stdout!r}"


def test_help_works():
    r = _run("--help")
    assert r.returncode == 0
    assert "usage" in r.stdout.lower() or "Usage" in r.stdout


def test_skill_resource_bundled():
    r = _run("--format", "json", "agent", "skill")
    assert r.returncode == 0, r.stderr
    payload = json.loads(r.stdout)
    assert payload["data"]["skill_exists"] is True


def test_no_plain_source_in_bundle():
    """Spec § 5: bundle should not leak maxc_cli .py source files."""
    src_files = []
    for root, _, files in os.walk(BUNDLE):
        for f in files:
            if f.endswith(".py"):
                src_files.append(os.path.join(root, f))
    leaks = [p for p in src_files if "maxc_cli" in p and "skills" not in p]
    assert not leaks, f"maxc_cli source files leaked: {leaks[:5]}"
