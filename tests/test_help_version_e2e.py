"""Black-box guard: argparse-eager flags reach the parser the user typed at.

Spawned as actual subprocesses (`python -m maxc_cli ...`) so these tests
also catch regressions that in-process tests miss: broken console_scripts
entry, import errors at startup, module-level side effects.

The motivating regression: a `_hoist_global_flags` change pushed `-h` to the
front of argv, which made `maxc query -h` print the TOP-LEVEL help instead
of the `query` subparser's help. The bug shipped because the only test was
a unit test that asserted the wrong post-hoist argv shape.
"""
from __future__ import annotations

import subprocess
import sys

import pytest

from maxc_cli import __version__

pytestmark = pytest.mark.e2e


def _maxc_argv() -> list[str]:
    # `python -m maxc_cli` avoids depending on PATH resolution of the
    # `maxc` console script, so the test works in any environment where
    # maxc_cli is importable (which is already a prerequisite for the
    # rest of the test suite).
    return [sys.executable, "-m", "maxc_cli"]


def _run(argv: list[str], timeout: float = 15.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*_maxc_argv(), *argv],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# Each row: (argv, must_appear_in_stdout, must_NOT_appear_in_stdout).
# `must_not` is the critical half — it catches the case where every help
# request silently degrades to top-level help.
_TOPLEVEL_COMMAND_LIST = "{query,job,meta,session,data,auth,agent,cache}"


@pytest.mark.parametrize(
    "argv, must_contain, must_not_contain",
    [
        # Top-level help (sanity — must show the command list).
        (["-h"],     [_TOPLEVEL_COMMAND_LIST], []),
        (["--help"], [_TOPLEVEL_COMMAND_LIST], []),
        # 1-deep subparser help: must show the subcommand's own usage line,
        # must NOT degrade to the top-level command list.
        (["query", "-h"],     ["maxc query"], [_TOPLEVEL_COMMAND_LIST]),
        (["query", "--help"], ["maxc query"], [_TOPLEVEL_COMMAND_LIST]),
        # 2-deep nested (agent subcommands).
        (["agent", "skill", "-h"],
         ["maxc agent skill", "{install,update,uninstall,list,diff,path}"],
         [_TOPLEVEL_COMMAND_LIST]),
        # 3-deep nested (agent skill install — the worst-case hoist regression).
        (["agent", "skill", "install", "-h"],
         ["maxc agent skill install", "{claude-code,cursor"],
         [_TOPLEVEL_COMMAND_LIST]),
        # Leaf with distinctive flags — proves it's really the leaf's help.
        (["auth", "login", "--help"],
         ["maxc auth login", "--access-id"],
         [_TOPLEVEL_COMMAND_LIST]),
        (["meta", "describe", "-h"],
         ["maxc meta describe", "--schema"],
         [_TOPLEVEL_COMMAND_LIST]),
        # Other hoistable globals MUST NOT redirect --help. If --debug is
        # hoisted to the front, --help must still reach the query subparser.
        (["--debug", "query", "--help"],
         ["maxc query"],
         [_TOPLEVEL_COMMAND_LIST]),
        # --help after a positional value (post-SQL) must still reach the
        # subparser — regression check for any future "hoist --help after
        # positional" attempt.
        (["query", "SELECT 1", "-h"],
         ["maxc query"],
         [_TOPLEVEL_COMMAND_LIST]),
    ],
    ids=[
        "top -h",
        "top --help",
        "query -h",
        "query --help",
        "agent skill -h (nested)",
        "agent skill install -h (deep nested)",
        "auth login --help (leaf with --access-id)",
        "meta describe -h (leaf with --schema)",
        "--debug query --help (hoist coexists)",
        "query SQL -h (help after positional)",
    ],
)
def test_help_reaches_correct_parser(
    argv: list[str],
    must_contain: list[str],
    must_not_contain: list[str],
) -> None:
    result = _run(argv)
    assert result.returncode == 0, (
        f"argv={argv!r} exited {result.returncode}\nstderr:\n{result.stderr}"
    )
    out = result.stdout
    for needle in must_contain:
        assert needle in out, (
            f"argv={argv!r}: expected {needle!r} in help output, got:\n{out}"
        )
    for needle in must_not_contain:
        assert needle not in out, (
            f"argv={argv!r}: help silently degraded — found top-level marker "
            f"{needle!r} in:\n{out}"
        )


@pytest.mark.parametrize(
    "argv",
    [
        ["-v"],
        ["--version"],
        # --version is hoisted on purpose (only the top parser defines it).
        # These prove the hoist for version still works after the -h/--help
        # hoist was removed.
        ["query", "-v"],
        ["query", "--version"],
        ["agent", "skill", "install", "--version"],
    ],
    ids=["top -v", "top --version", "query -v (hoisted)",
         "query --version (hoisted)", "deep --version (hoisted)"],
)
def test_version_reaches_top_parser(argv: list[str]) -> None:
    result = _run(argv)
    assert result.returncode == 0, (
        f"argv={argv!r} exited {result.returncode}\nstderr:\n{result.stderr}"
    )
    # argparse writes --version output to stdout in Python 3.4+.
    out = (result.stdout or "") + (result.stderr or "")
    expected = f"maxc {__version__}"
    assert expected in out, (
        f"argv={argv!r}: expected {expected!r}, got stdout={result.stdout!r} "
        f"stderr={result.stderr!r}"
    )
