"""Tests for CLI numeric flag validation and exit-code propagation.

Until PR-2, a flag like `--limit -5` silently accepted the negative
value and the backend either misbehaved or returned garbage with
exit code 0. Failure envelopes built directly inside a CLI handler
(e.g., JSON parse errors in `meta semantic set`) were emitted but
the process still exited 0, so wrappers couldn't tell success from
failure without parsing the envelope.

These tests pin:
1. Negative values on positive-only flags exit 2 (argparse convention).
2. Negative values on nonneg-only flags exit 2.
3. Direct-failure envelope handlers exit non-zero.
"""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.cli import run


def _make_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  access_id: FAKE\n"
        "  secret_access_key: FAKE\n"
        "  project: test_project\n"
        "  endpoint: http://localhost/api\n"
        "backend:\n"
        "  type: auto\n",
        encoding="utf-8",
    )
    return config_path


def _run(tmp_path: Path, argv: list[str]) -> tuple[int, str, str]:
    """Invoke the CLI, catching SystemExit (argparse's exit path)."""
    stdout = StringIO()
    stderr = StringIO()
    config_path = _make_config(tmp_path)
    full_argv = ["--config", str(config_path), *argv]
    try:
        code = run(full_argv, cwd=tmp_path, stdout=stdout, stderr=stderr)
    except SystemExit as exc:
        code = int(exc.code) if isinstance(exc.code, int) else 1
    return code, stdout.getvalue(), stderr.getvalue()


# ── Positive-only numeric flags ────────────────────────────────────────────


@pytest.mark.parametrize(
    "argv",
    [
        ["meta", "list-tables", "--limit", "-5"],
        ["meta", "list-tables", "--limit", "0"],
        ["job", "list", "--limit", "-1"],
        ["data", "sample", "t", "--rows", "0"],
        ["data", "sample", "t", "--rows", "-3"],
        ["query", "select 1", "--max-rows", "0"],
        ["query", "select 1", "--max-rows", "-1"],
        ["query", "select 1", "--page-size", "0"],
        ["job", "wait", "j", "--timeout", "0"],
    ],
)
def test_negative_or_zero_on_positive_flag_exits_with_argparse_error(
    tmp_path: Path, argv: list[str], capsys
) -> None:
    """Positive-only int flags must reject <=0 with argparse's exit code 2."""
    code, _, _ = _run(tmp_path, argv)
    assert code == 2, f"expected argparse exit code 2, got {code}"
    # Argparse writes to module-level sys.stderr; pytest's capsys catches it.
    captured = capsys.readouterr()
    assert "must be a positive integer" in captured.err


# ── Nonneg numeric flags ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "argv",
    [
        ["query", "select 1", "--max-retries", "-1"],
    ],
)
def test_negative_on_nonneg_flag_exits_with_argparse_error(
    tmp_path: Path, argv: list[str], capsys
) -> None:
    """Nonneg int flags (allow 0) must still reject negative values."""
    code, _, _ = _run(tmp_path, argv)
    assert code == 2
    captured = capsys.readouterr()
    assert "must be a non-negative integer" in captured.err


def test_zero_on_nonneg_flag_is_accepted(tmp_path: Path) -> None:
    """--max-retries 0 is the default and must remain valid."""
    # We don't care if the query itself succeeds — only that argparse let it through.
    # A backend-related failure (e.g., no real ODPS) returns a different exit code (1)
    # but NOT 2, since 2 means argparse rejected the flag.
    code, _, _ = _run(tmp_path, ["query", "select 1", "--max-retries", "0", "--json"])
    assert code != 2


# ── Failure envelope must propagate non-zero exit code ─────────────────────


def test_direct_failure_envelope_exits_non_zero(tmp_path: Path) -> None:
    """`meta semantic set --column-semantics` with bad JSON builds a failure
    envelope inline. The exit code must be non-zero so wrappers (CI, shell
    pipelines) can detect the failure without parsing the envelope.
    """
    code, stdout, _ = _run(
        tmp_path,
        [
            "meta", "semantic", "set", "some_table",
            "--column-semantics", "{not valid json",
            "--json",
        ],
    )
    assert code != 0, "failure envelope must produce non-zero exit code"
    payload = json.loads(stdout)
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "INVALID_JSON"
