"""Tests for the agent platform registry — single source of truth for SKILL targets.

The registry replaces the hardcoded _SKILL_PLATFORMS dict in app.py. These
tests lock in the legacy install paths byte-for-byte so the PR #2 deletion
doesn't silently move where users' already-installed skills live.
"""
from pathlib import Path

import pytest

from maxc_cli import agent_platforms as ap


def test_registry_has_seven_canonical_platforms():
    names = {p.name for p in ap.REGISTRY}
    assert names == {
        "claude-code", "cursor", "windsurf", "codex",
        "qwen", "qoder", "qoderwork",
    }


def test_resolve_returns_matching_platform():
    assert ap.resolve("cursor").name == "cursor"


def test_resolve_unknown_raises():
    with pytest.raises(KeyError):
        ap.resolve("nonexistent")


def test_claude_code_has_no_extra_files():
    claude = ap.resolve("claude-code")
    assert claude.extra_files == ()


def test_install_root_matches_legacy_paths():
    # Snapshot the seven legacy install_dirs from app.py:3375-3411 (HEAD@2026-05-25)
    # to guarantee byte-equivalent migration. If a path changes, this test fails
    # and the new value must be justified in the PR description (it would break
    # already-installed users).
    expected = {
        "claude-code": Path.home() / ".claude" / "skills" / "maxc-cli",
        "cursor":      Path.home() / ".cursor"    / "skills" / "maxc-cli",
        "windsurf":    Path.home() / ".windsurf"  / "skills" / "maxc-cli",
        "qwen":        Path.home() / ".qwen"      / "skills" / "maxc-cli",
        "qoder":       Path.home() / ".qoder"     / "skills" / "maxc-cli",
        "qoderwork":   Path.home() / ".qoderwork" / "skills" / "maxc-cli",
    }
    for name, expected_path in expected.items():
        assert ap.resolve(name).install_root == expected_path, name


def test_codex_install_root_respects_CODEX_HOME(monkeypatch, tmp_path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "my-codex"))
    # Force re-import so module-level Path() is re-evaluated. The registry
    # holds frozen paths captured at import time, so resolve+lazy is required.
    # Implementation note: codex install_root must be evaluated lazily (function-call)
    # not at REGISTRY construction time. See implementation step.
    import importlib
    importlib.reload(ap)
    try:
        assert ap.resolve("codex").install_root == (
            tmp_path / "my-codex" / "skills" / "maxc-cli"
        )
    finally:
        # Reload again after monkeypatch rollback so REGISTRY doesn't keep the
        # fake CODEX_HOME path and pollute subsequent tests in the same run.
        monkeypatch.undo()
        importlib.reload(ap)


def test_render_claude_plugin_writes_declared_path(tmp_path):
    # The render_fn must write to install_dir / extra_file.relative_path.
    # If it writes elsewhere, agent skill diff/uninstall can't find the file.
    ap.render_claude_plugin(tmp_path, cli="maxc", cli_module="python3 -m maxc_cli")
    assert (tmp_path / ".claude-plugin" / "plugin.json").is_file()


def test_render_claude_plugin_byte_equivalent_to_legacy():
    # Locked-in byte output prevents `agent skill diff` from reporting a phantom
    # delta on day-1 for users who already have plugin.json installed via the
    # legacy code path (app.py:3493-3499).
    expected = (
        '{\n  "name": "maxc-cli",\n'
        '  "description": "MaxCompute/ODPS CLI — query tables, view schema, '
        'search metadata, execute SQL, check partitions, sample data, '
        'track jobs. Install via: pip install maxc-cli",\n'
        '  "author": { "name": "maxc-cli contributors" }\n}\n'
    )
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        ap.render_claude_plugin(Path(td), cli="maxc", cli_module="x")
        actual = (Path(td) / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8")
    assert actual == expected
