"""End-to-end test for scripts/build_release.sh.

Builds a real PyInstaller onedir from this repo and asserts the tarball + sha256
match the OSS contract defined in
docs/superpowers/specs/2026-05-19-maxc-cliext-integration-design.md § 3.

Gated behind the ``e2e`` marker — pytest.ini excludes ``e2e`` by default
since this takes ~30s and writes to disk.

Run explicitly:  ``pytest tests/test_build_release_script.py -v -m e2e``
"""
from __future__ import annotations

import os
import re
import subprocess
import tarfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "build_release.sh"

pytestmark = pytest.mark.e2e


def test_build_release_produces_tarball_and_sha256(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    env = os.environ.copy()
    env["OUTPUT_DIR"] = str(out_dir)
    subprocess.run(
        ["bash", str(SCRIPT)],
        check=True,
        env=env,
        cwd=REPO,
    )

    tarball = out_dir / "maxc.tar.gz"
    sha = out_dir / "maxc.tar.gz.sha256"
    assert tarball.exists(), f"missing {tarball}"
    assert sha.exists(), f"missing {sha}"

    digest = sha.read_text().strip()
    assert re.fullmatch(r"[0-9a-f]{64}", digest), (
        f"sha256 file should contain a single hex digest, got: {digest!r}"
    )

    with tarfile.open(tarball) as tf:
        archive_members = tf.getmembers()
        members = [member.name for member in archive_members]
        roots = {m.split("/", 1)[0] for m in members if m}
        assert roots == {"maxc"}, f"tarball top-level dirs should be {{'maxc'}}, got {roots}"
        assert not any(Path(member).name.startswith("._") for member in members)
        assert not any(
            key.startswith(("LIBARCHIVE.xattr.", "SCHILY.xattr."))
            for member in archive_members
            for key in member.pax_headers
        ), "release archive must not carry builder-local extended attributes"

        bins = [n for n in members if n in ("maxc/maxc", "maxc/maxc.exe")]
        assert bins, (
            "tarball missing maxc binary at maxc/maxc[.exe]; "
            f"members sample: {members[:10]}"
        )
