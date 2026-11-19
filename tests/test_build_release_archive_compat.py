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


def _fresh_exec_works() -> bool:
    """True when the OS lets us exec a just-written executable directly.

    Corporate endpoint agents can hold freshly-created binaries open, which
    stalls direct exec indefinitely; explicit interpreter launches are not
    affected.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        probe = Path(td) / "probe"
        probe.write_text("#!/bin/sh\nprintf ok\n")
        probe.chmod(0o755)
        try:
            r = subprocess.run([str(probe)], capture_output=True, timeout=5)
            return r.returncode == 0 and r.stdout == b"ok"
        except (subprocess.TimeoutExpired, OSError):
            return False


_FRESH_EXEC: bool | None = None


def _fresh_exec_supported() -> bool:
    global _FRESH_EXEC
    if _FRESH_EXEC is None:
        _FRESH_EXEC = _fresh_exec_works()
    return _FRESH_EXEC


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

    if not _fresh_exec_supported():
        pytest.skip(
            "direct exec of freshly-written files is blocked in this "
            "environment (corporate endpoint agent); the archive-flag "
            "contract cannot run without executing the built bundle entry"
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
        timeout=60,
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


@pytest.mark.parametrize(
    ("developer_id", "notary_status", "expect_archive"),
    [(True, "Accepted", True), (True, "Invalid", False), (False, "Accepted", False)],
)
def test_macos_signed_archive_requires_developer_id_and_accepted_notarization(
    tmp_path: Path, developer_id: bool, notary_status: str, expect_archive: bool,
) -> None:
    if not _fresh_exec_supported():
        pytest.skip("the OS blocks execution of freshly-written shell fixtures")

    repo = tmp_path / "repo"
    scripts = repo / "scripts"
    scripts.mkdir(parents=True)
    (scripts / SCRIPT.name).write_bytes(SCRIPT.read_bytes())
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    commands = {
        "uname": "printf 'Darwin\\n'",
        "pyinstaller": "mkdir -p dist/maxc; touch dist/maxc/maxc dist/maxc/module.so",
        "file": "printf 'Mach-O fixture\\n'",
        "codesign": """if [ "$1" = "--display" ]; then
    printf '%s\\n' "$FIXTURE_AUTHORITY" >&2
else
    printf '%s\\n' "$@" >> "$FIXTURE_LOG"
fi""",
        "ditto": 'touch "$5"',
        "xcrun": """test "$1" = notarytool
test "$2" = submit
test -f "$3"
printf 'notary\\n' >> "$FIXTURE_LOG"
printf '{"id":"fixture","status":"%s"}\\n' "$FIXTURE_NOTARY_STATUS" """,
        "tar": """test "$1" = --no-xattrs
shift
exec /usr/bin/tar "$@" """,
    }
    for name, command in commands.items():
        _write_executable(fake_bin / name, f"#!/bin/sh\nset -eu\n{command}\n")

    log = tmp_path / "signing.log"
    output_dir = tmp_path / "out"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "OUTPUT_DIR": str(output_dir),
        "MAXC_MACOS_CODESIGN_IDENTITY": "fixture-identity",
        "MAXC_MACOS_NOTARY_PROFILE": "fixture-profile",
        "FIXTURE_LOG": str(log),
        "FIXTURE_AUTHORITY": (
            "Authority=Developer ID Application: Test" if developer_id else "Signature=adhoc"
        ),
        "FIXTURE_NOTARY_STATUS": notary_status,
    }
    result = subprocess.run(
        ["bash", str(scripts / SCRIPT.name)], cwd=repo, env=env, timeout=30,
        capture_output=True, text=True, check=False,
    )

    assert (result.returncode == 0) is expect_archive, result.stderr
    assert (output_dir / "maxc.tar.gz").exists() is expect_archive
    assert (output_dir / "maxc.tar.gz.sha256").exists() is expect_archive
    if developer_id:
        operations = log.read_text(encoding="utf-8")
        assert "dist/maxc/maxc\n" in operations
        assert "dist/maxc/module.so\n" in operations
        assert operations.endswith("notary\n")
    else:
        assert not log.exists()


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
