"""Regression tests for standalone release installation and bootstrap scripts."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tarfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
INSTALL_GUIDE = ROOT / "scripts" / "install-agent.md"
BOOTSTRAP = ROOT / "scripts" / "bootstrap.sh"
NCS_BOOTSTRAP = ROOT / "scripts" / "bootstrap-ncs.sh"
WINDOWS_BOOTSTRAP = ROOT / "scripts" / "bootstrap-ncs.ps1"


def _phase_three_script() -> str:
    guide = INSTALL_GUIDE.read_text(encoding="utf-8")
    section = guide.split("## Phase 3 — Extract and link", 1)[1].split(
        "## Phase 4 — Put `maxc` on PATH", 1
    )[0]
    match = re.search(r"```bash\n(?P<script>.*?)\n```", section, re.DOTALL)
    assert match, "Phase 3 must contain one executable bash block"
    return match.group("script")


def _make_bundle(
    directory: Path,
    reported_version: str,
    *,
    extra_top_level: bool = False,
) -> tuple[Path, str]:
    payload = directory / "payload" / "maxc"
    payload.mkdir(parents=True)
    binary = payload / "maxc"
    binary.write_text(
        f"#!/bin/sh\nprintf '%s\\n' 'maxc {reported_version}'\n",
        encoding="utf-8",
    )
    binary.chmod(0o755)

    archive = directory / "maxc.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(payload, arcname="maxc")
        if extra_top_level:
            unexpected = directory / "unexpected.txt"
            unexpected.write_text("not part of the bundle contract", encoding="utf-8")
            bundle.add(unexpected, arcname="unexpected.txt")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    return archive, digest


def _phase_three_script_shimmed() -> str:
    """Bind the extracted candidate to its interpreter when needed.

    Corporate endpoint agents can hold freshly-written executables open, so a
    direct exec of the just-extracted candidate stalls indefinitely on those
    machines. Explicit interpreter launches are unaffected. The final
    stable-entry verification line is left as a raw direct exec so callers can
    decide how to exercise it.
    """
    script = _phase_three_script()
    if _fresh_exec_supported():
        return script
    pairs = [
        (
            'if ! CANDIDATE_OUTPUT=$("${CANDIDATE}" --version 2>/dev/null); then',
            'if ! CANDIDATE_OUTPUT=$("${SHELL}" "${CANDIDATE}" --version 2>/dev/null); then',
        ),
        (
            'EXISTING_VERSION=$("${EXISTING_ENTRY}" --version 2>/dev/null',
            'EXISTING_VERSION=$("${SHELL}" "${EXISTING_ENTRY}" --version 2>/dev/null',
        ),
    ]
    for marker, replacement in pairs:
        assert marker in script, f"install-phase line changed; update the shim: {marker[:40]}"
        script = script.replace(marker, replacement)
    return script


_STABLE_VERIFY_LINE = '"${STABLE_ENTRY}" --version'


def _split_phase_three_script(script: str) -> tuple[str, str]:
    """Return (install phase, final stable-entry verification line)."""
    assert _STABLE_VERIFY_LINE in script, "install-phase layout changed; update the shim"
    install, _, tail = script.rpartition(_STABLE_VERIFY_LINE)
    return install, _STABLE_VERIFY_LINE + tail


def _fresh_exec_works() -> bool:
    """True when the OS lets us exec a just-written executable directly.

    Corporate endpoint agents can hold freshly-created binaries open, which
    stalls direct exec indefinitely; explicit interpreter launches are not
    affected. Callers use this to decide how far to exercise the install
    contract in one run.
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


def _run_install_phase_without_final_verify(
    *,
    work: Path,
    home: Path,
    version: str,
    digest: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run every install-phase contract except the final direct-exec line."""
    install_root = home / ".maxc" / "bin"
    release_root = home / ".maxc" / "releases"
    install_root.mkdir(parents=True, exist_ok=True)
    release_root.mkdir(parents=True, exist_ok=True)
    script = _phase_three_script_shimmed()
    install_part, _ = _split_phase_three_script(script)
    preamble = (
        f"export VERSION={version!r}\n"
        f"export PLATFORM='linux-amd64'\n"
        f"export ACTUAL={digest!r}\n"
        f"export INSTALL_ROOT={os.fspath(home / '.maxc' / 'bin')!r}\n"
        f"export RELEASE_ROOT={os.fspath(home / '.maxc' / 'releases')!r}\n"
    )
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", preamble + install_part],
        cwd=work,
        capture_output=True,
        text=True,
        check=check,
        timeout=60,
    )


def _verify_stable_entry(stable: Path, expected_version: str) -> None:
    """Verify the installed stable entry, via interpreter where exec is blocked."""
    if _fresh_exec_supported():
        actual = subprocess.check_output([str(stable), "--version"], text=True).strip()
    else:
        actual = subprocess.check_output(
            ["/bin/sh", str(stable), "--version"], text=True
        ).strip()
    assert actual == f"maxc {expected_version}"


def _run_install_phase(
    *,
    work: Path,
    home: Path,
    version: str,
    digest: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    maxc_home = home / ".maxc"
    install_root = maxc_home / "bin"
    release_root = maxc_home / "releases"
    install_root.mkdir(parents=True, exist_ok=True)
    release_root.mkdir(parents=True, exist_ok=True)
    preamble = f"""
export VERSION={version!r}
export PLATFORM='linux-amd64'
export ACTUAL={digest!r}
export INSTALL_ROOT={os.fspath(install_root)!r}
export RELEASE_ROOT={os.fspath(release_root)!r}
"""
    script = _phase_three_script_shimmed()
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", preamble + script],
        cwd=work,
        capture_output=True,
        text=True,
        check=check,
        timeout=60,
    )


def test_install_uses_versioned_release_and_atomic_stable_link(tmp_path: Path) -> None:
    home = tmp_path / "home"
    work = tmp_path / "download"
    work.mkdir()
    _, digest = _make_bundle(work, "1.2.3")

    # Reproduce the layout left by the old guide: ~/.maxc/bin/maxc was a
    # directory, so its attempted stable symlink lived inside itself.
    legacy_dir = home / ".maxc" / "bin" / "maxc"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "old-install-marker").write_text("preserve me", encoding="utf-8")

    _run_install_phase_without_final_verify(
        work=work, home=home, version="1.2.3", digest=digest
    )

    stable = home / ".maxc" / "bin" / "maxc"
    expected = (
        home
        / ".maxc"
        / "releases"
        / "1.2.3"
        / f"linux-amd64-{digest}"
        / "maxc"
        / "maxc"
    )
    assert stable.is_symlink()
    assert stable.resolve() == expected
    _verify_stable_entry(stable, "1.2.3")
    backups = list((home / ".maxc" / "releases").glob("legacy-maxc.*"))
    assert len(backups) == 1
    assert (backups[0] / "old-install-marker").read_text(encoding="utf-8") == "preserve me"

    # Installing the same verified digest is idempotent and does not create
    # another release or another legacy backup.
    _run_install_phase_without_final_verify(
        work=work, home=home, version="1.2.3", digest=digest
    )
    assert stable.resolve() == expected
    assert len(list((home / ".maxc" / "releases").glob("legacy-maxc.*"))) == 1


def test_candidate_version_mismatch_does_not_replace_working_entry(tmp_path: Path) -> None:
    home = tmp_path / "home"
    good = tmp_path / "good"
    good.mkdir()
    _, good_digest = _make_bundle(good, "1.2.3")
    _run_install_phase_without_final_verify(
        work=good, home=home, version="1.2.3", digest=good_digest
    )

    stable = home / ".maxc" / "bin" / "maxc"
    original_target = stable.resolve()
    bad = tmp_path / "bad"
    bad.mkdir()
    _, bad_digest = _make_bundle(bad, "9.9.9")
    # The mismatch aborts before the final stable-entry line, so the split
    # runner exercises the identical contract without depending on direct exec.
    result = _run_install_phase_without_final_verify(
        work=bad,
        home=home,
        version="1.2.4",
        digest=bad_digest,
        check=False,
    )

    assert result.returncode != 0
    assert "does not match 1.2.4" in result.stdout
    assert stable.resolve() == original_target
    _verify_stable_entry(stable, "1.2.3")


def test_release_version_cannot_escape_release_root(tmp_path: Path) -> None:
    home = tmp_path / "home"
    work = tmp_path / "download"
    work.mkdir()
    _, digest = _make_bundle(work, "1.2.3")

    result = _run_install_phase(
        work=work,
        home=home,
        version="..",
        digest=digest,
        check=False,
    )

    assert result.returncode != 0
    assert "unsafe release version" in result.stdout
    assert not (home / ".maxc" / "bin" / "maxc").exists()


def test_release_rejects_unexpected_top_level_archive_entries(tmp_path: Path) -> None:
    home = tmp_path / "home"
    work = tmp_path / "download"
    work.mkdir()
    _, digest = _make_bundle(work, "1.2.3", extra_top_level=True)

    result = _run_install_phase(
        work=work,
        home=home,
        version="1.2.3",
        digest=digest,
        check=False,
    )

    assert result.returncode != 0
    assert "unsafe or unsupported tarball layout" in result.stdout
    assert not (home / ".maxc" / "bin" / "maxc").exists()


@pytest.mark.parametrize("bootstrap", [BOOTSTRAP, NCS_BOOTSTRAP])
def test_bootstrap_uses_selected_python_and_survives_version_lookup_failure(
    bootstrap: Path,
) -> None:
    script = bootstrap.read_text(encoding="utf-8")

    subprocess.run(["bash", "-n", str(bootstrap)], check=True, timeout=60)
    assert 'PIP_CMD=("$PYTHON_BIN" -m pip)' in script
    assert "command -v pip3" not in script
    assert "command -v pip " not in script
    starts = [
        match.start()
        for match in re.finditer(r"^[ \t]+LATEST_VERSION=", script, re.MULTILINE)
    ]
    assert len(starts) == 2
    for start in starts:
        end = script.find("\n\n", start)
        assert "|| true)" in script[start:end]


def test_windows_bootstrap_matches_python_minimum() -> None:
    script = WINDOWS_BOOTSTRAP.read_text(encoding="utf-8")

    assert "Python >= 3.8" not in script
    assert ">= 3.8" not in script
    assert "Python >= 3.9" in script
    assert "$major -gt 3 -or ($major -eq 3 -and $minor -ge 9)" in script
    version_guard = script.split("if ($major -gt 3", 1)[1].split("# 确定 pip 命令", 1)[0]
    assert "Confirm-Prompt" not in version_guard
