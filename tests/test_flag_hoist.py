"""Verify global flags are hoisted to the front before argparse sees them."""
import pytest

from maxc_cli.cli import _hoist_global_flags


@pytest.mark.parametrize(
    "raw, expected",
    [
        # 0-arity flags appended after the subcommand
        (["query", "select 1", "--json"], ["--json", "query", "select 1"]),
        (["query", "select 1", "--quiet"], ["--quiet", "query", "select 1"]),
        (["query", "select 1", "-q"], ["-q", "query", "select 1"]),
        (["meta", "describe", "t", "--debug"], ["--debug", "meta", "describe", "t"]),
        # Already at the front — no change
        (["--json", "query", "select 1"], ["--json", "query", "select 1"]),
        # 1-arity --format with value as next arg
        (["query", "x", "--format", "csv"], ["--format", "csv", "query", "x"]),
        # 1-arity --format with =VALUE form
        (["query", "x", "--format=csv"], ["--format=csv", "query", "x"]),
        # 1-arity -f shortcut
        (["query", "x", "-f", "csv"], ["-f", "csv", "query", "x"]),
        # 1-arity --config
        (["query", "x", "--config", "/tmp/a.yaml"], ["--config", "/tmp/a.yaml", "query", "x"]),
        (["query", "x", "--config=/tmp/a.yaml"], ["--config=/tmp/a.yaml", "query", "x"]),
        # POSIX terminator stops hoist
        (["query", "select 1", "--", "--json"], ["query", "select 1", "--", "--json"]),
        # Flag literal inside a positional-argument string never reaches argv
        # (the shell already separated quoted tokens); we only need to verify
        # _hoist treats argv[1] = "select '--json'" as a value, not a flag.
        (["query", "select '--json'"], ["query", "select '--json'"]),
        # Multiple global flags + subgroup
        (["meta", "describe", "t", "--json", "--quiet"], ["--json", "--quiet", "meta", "describe", "t"]),
        # No arguments
        ([], []),
        # --version is hoisted (only the top-level parser defines it)
        (["--version"], ["--version"]),
        (["query", "x", "-v"], ["-v", "query", "x"]),
        # -h/--help is NOT hoisted — every subparser auto-registers its own,
        # so `maxc query -h` must reach the query subparser, not the root.
        (["query", "x", "--help"], ["query", "x", "--help"]),
        (["query", "x", "-h"], ["query", "x", "-h"]),
        (["agent", "skill", "install", "-h"], ["agent", "skill", "install", "-h"]),
    ],
)
def test_hoist_matrix(raw, expected):
    assert _hoist_global_flags(raw) == expected


def test_hoist_handles_unknown_global_flag_as_passthrough():
    """Unknown flags must be left alone — they're either subcommand flags
    (e.g., --project on `query`) or genuine typos (argparse will reject)."""
    assert _hoist_global_flags(["query", "x", "--project", "p"]) == ["query", "x", "--project", "p"]
