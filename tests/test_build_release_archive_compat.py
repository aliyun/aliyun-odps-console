"""Fast platform-contract tests for the release archive command."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(os.name == "nt", reason="release archiving runs under Unix shell jobs"),
]

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_release.sh"


@pytest.mark.parametrize(
    ("kernel", "expect_no_xattrs", "reject_no_xattrs"),
    [
        ("Darwin", True, False),
        ("Linux", False, True),
    ],
)
def test_archive_flags_are_compatible_with_platform_tar(
    tmp_path: Path,
    kernel: str,
    expect_no_xattrs: bool,
    reject_no_xattrs: bool,
) -> None:
    repo = tmp_path / "repo"
    scripts = repo / "scripts"
    fake_bin = tmp_path / "fake-bin"
    scripts.mkdir(parents=True)
    fake_bin.mkdir()
    (scripts / "build_release.sh").write_bytes(SCRIPT.read_bytes())
    (repo / "maxc.spec").write_text("# fake spec\n", encoding="utf-8")

    _write_executable(fake_bin / "uname", f"#!/bin/sh\nprintf '%s\\n' '{kernel}'\n")
    _write_executable(
        fake_bin / "pyinstaller",
        """#!/bin/sh
set -eu
mkdir -p dist/maxc
printf '#!/bin/sh\\nexit 0\\n' > dist/maxc/maxc
chmod +x dist/maxc/maxc
""",
    )
    _write_executable(
        fake_bin / "tar",
        """#!/bin/sh
set -eu
: "${TAR_LOG:?}"
printf 'copyfile_disable=%s\\n' "${COPYFILE_DISABLE:-}" > "${TAR_LOG}"
for arg in "$@"; do
  printf 'arg=%s\\n' "$arg" >> "${TAR_LOG}"
  if [ "${REJECT_NO_XATTRS:-0}" = 1 ] && [ "$arg" = "--no-xattrs" ]; then
    echo 'simulated GNU tar 1.26: unsupported --no-xattrs' >&2
    exit 64
  fi
done
exec /usr/bin/tar "$@"
""",
    )

    tar_log = tmp_path / "tar.log"
    output_dir = tmp_path / "out"
    env = os.environ.copy()
    env.update(
        {
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{env['PATH']}",
            "REJECT_NO_XATTRS": "1" if reject_no_xattrs else "0",
            "TAR_LOG": str(tar_log),
        }
    )
    subprocess.run(
        ["bash", str(scripts / "build_release.sh")],
        cwd=repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    log = tar_log.read_text(encoding="utf-8")
    assert ("arg=--no-xattrs\n" in log) is expect_no_xattrs
    assert ("copyfile_disable=1\n" in log) is (kernel == "Darwin")
    assert (output_dir / "maxc.tar.gz").is_file()
    assert (output_dir / "maxc.tar.gz.sha256").is_file()


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
