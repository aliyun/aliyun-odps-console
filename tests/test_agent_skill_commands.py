"""Tests for the new six-verb `agent skill` MaxCApp methods.

These coexist with the legacy `agent_install_skill` (PR #1 keeps both).
Once PR #2 lands, the legacy method is deleted but these tests still hold.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from maxc_cli import agent_platforms as ap
from maxc_cli.app import MaxCApp


@pytest.fixture
def app(tmp_path, monkeypatch):
    """MaxCApp instance with HOME redirected under tmp_path.

    Reloads agent_platforms so REGISTRY entries pick up the new HOME.
    load_backend=False to avoid spurious ODPS client construction.
    On teardown, restore REGISTRY by reloading once more *after* monkeypatch
    rollback — otherwise REGISTRY keeps the fake paths and pollutes other tests.
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    importlib.reload(ap)
    yield MaxCApp(cwd=tmp_path, load_backend=False)
    monkeypatch.undo()
    importlib.reload(ap)


def test_skill_install_creates_install_dir_and_marker(app, tmp_path):
    env = app.skill_install(platform="cursor", invocation="maxc")
    assert env.status == "success"
    install_path = Path(env.data["install_path"])
    assert install_path.is_dir()
    assert (install_path / ".maxc-skill-version").is_file()
    assert (install_path / "SKILL.md").is_file()


def test_skill_install_with_dir_override(app, tmp_path):
    custom = tmp_path / "custom-target"
    env = app.skill_install(platform="cursor", invocation="maxc", dir_override=custom)
    assert env.status == "success"
    # cursor.skill_subpath is None → effective_target == custom
    assert Path(env.data["install_path"]) == custom
    assert (custom / "SKILL.md").is_file()


def test_skill_install_no_extra_files_for_claude_code(app, tmp_path):
    env = app.skill_install(platform="claude-code", invocation="maxc")
    install = Path(env.data["install_path"])
    assert not (install / ".claude-plugin").exists()


def test_skill_install_force_overwrites(app, tmp_path):
    app.skill_install(platform="cursor", invocation="maxc")
    install = Path(ap.resolve("cursor").install_root)
    skill = install / "SKILL.md"
    skill.write_text("CORRUPTED\n", encoding="utf-8")
    app.skill_install(platform="cursor", invocation="maxc", force=True)
    assert skill.read_text(encoding="utf-8") != "CORRUPTED\n"


def test_skill_install_without_force_is_idempotent_same_version(app, tmp_path):
    e1 = app.skill_install(platform="cursor", invocation="maxc")
    e2 = app.skill_install(platform="cursor", invocation="maxc")
    assert e1.data["upgraded"] is True
    assert e2.data["upgraded"] is False  # marker matched → no copy


def test_skill_update_requires_explicit_target(app):
    from maxc_cli.exceptions import ValidationError
    with pytest.raises(ValidationError):
        app.skill_update(platform=None, all_platforms=False, invocation="maxc")


def test_skill_update_all_iterates_installed(app, tmp_path):
    app.skill_install(platform="cursor", invocation="maxc")
    app.skill_install(platform="qwen", invocation="maxc")
    env = app.skill_update(platform=None, all_platforms=True, invocation="maxc")
    assert env.status == "success"
    assert set(env.data["platforms_updated"]) == {"cursor", "qwen"}


def test_skill_list_shows_only_installed(app, tmp_path):
    app.skill_install(platform="cursor", invocation="maxc")
    env = app.skill_list()
    names = [p["platform"] for p in env.data["installed"]]
    assert names == ["cursor"]
    # Warning about non-default --dir installs being invisible
    assert any("--dir" in w for w in (env.agent_hints.warnings or []))


def test_skill_diff_reports_no_changes_when_fresh_install(app):
    app.skill_install(platform="cursor", invocation="maxc")
    env = app.skill_diff(platform="cursor")
    assert env.status == "success"
    assert env.data["differences"] == []


def test_skill_diff_detects_local_modification(app):
    app.skill_install(platform="cursor", invocation="maxc")
    install = Path(ap.resolve("cursor").install_root)
    (install / "SKILL.md").write_text("LOCAL EDIT\n", encoding="utf-8")
    env = app.skill_diff(platform="cursor")
    assert any(d.get("path") == "SKILL.md" and d["kind"] == "modified"
               for d in env.data["differences"])


def test_skill_diff_unified_includes_diff_text(app):
    app.skill_install(platform="cursor", invocation="maxc")
    install = Path(ap.resolve("cursor").install_root)
    (install / "SKILL.md").write_text("LOCAL EDIT\n", encoding="utf-8")
    env = app.skill_diff(platform="cursor", unified=True)
    modified = [d for d in env.data["differences"] if d["kind"] == "modified"]
    assert modified and "LOCAL EDIT" in modified[0]["diff"]


def test_skill_uninstall_removes_install_dir(app):
    app.skill_install(platform="cursor", invocation="maxc")
    install = Path(ap.resolve("cursor").install_root)
    assert install.is_dir()
    app.skill_uninstall(platform="cursor")
    assert not install.exists()


def test_skill_path_prints_target_by_default(app):
    env = app.skill_path(platform="cursor")
    assert env.data["path"] == str(ap.resolve("cursor").install_root)


def test_skill_path_with_source_returns_wheel_skills_dir(app):
    env = app.skill_path(platform=None, source=True)
    assert env.data["path"].endswith("skills")
    assert "maxc_cli" in env.data["path"]


# ── CLI integration: `maxc agent skill {install,update,uninstall,list,diff,path}` ──
#
# The MaxCApp methods above are covered; this block proves the argparse wiring +
# _LOCAL_ONLY_COMMANDS membership + handler dispatch are correct. Reuses the
# `app` fixture's monkeypatched HOME so installs land under tmp_path.

import json as _json
from io import StringIO

from maxc_cli.cli import run as _run


def _make_config(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "default_project: test_proj\n"
        "default_region: cn-hangzhou\n"
        "default_format: json\n"
        "project_context: testing\n"
        "allowed_operations:\n  - SELECT\n"
        "cost_threshold_cu: 100\n"
        "sensitive_columns: []\n"
    )
    return cfg


def _cli(cfg, argv):
    out, err = StringIO(), StringIO()
    code = _run(["--config", str(cfg), *argv], cwd=cfg.parent, stdout=out, stderr=err)
    text = out.getvalue()
    payload = _json.loads(text) if text.strip() else {}
    return code, payload, err.getvalue()


def test_cli_agent_skill_install_creates_install_dir(app, tmp_path):
    cfg = _make_config(tmp_path)
    code, payload, _ = _cli(cfg, ["agent", "skill", "install", "cursor", "--json"])
    assert code == 0, payload
    assert payload["status"] == "success"
    assert payload["command"] == "agent skill install"  # to_dict normalizes dots → spaces
    install_path = Path(payload["data"]["install_path"])
    assert (install_path / "SKILL.md").is_file()


def test_cli_agent_skill_list_shows_installed(app, tmp_path):
    cfg = _make_config(tmp_path)
    _cli(cfg, ["agent", "skill", "install", "cursor", "--json"])
    code, payload, _ = _cli(cfg, ["agent", "skill", "list", "--json"])
    assert code == 0, payload
    platforms = [p["platform"] for p in payload["data"]["installed"]]
    assert "cursor" in platforms


def test_cli_agent_skill_update_requires_target(app, tmp_path):
    cfg = _make_config(tmp_path)
    code, payload, _ = _cli(cfg, ["agent", "skill", "update", "--json"])
    # ValidationError → non-zero exit with failure envelope
    assert code != 0
    assert payload["status"] == "failure"


def test_cli_agent_skill_update_all_iterates(app, tmp_path):
    cfg = _make_config(tmp_path)
    _cli(cfg, ["agent", "skill", "install", "cursor", "--json"])
    _cli(cfg, ["agent", "skill", "install", "qwen", "--json"])
    code, payload, _ = _cli(cfg, ["agent", "skill", "update", "--all", "--json"])
    assert code == 0, payload
    assert set(payload["data"]["platforms_updated"]) == {"cursor", "qwen"}


def test_cli_agent_skill_uninstall_removes(app, tmp_path):
    cfg = _make_config(tmp_path)
    _cli(cfg, ["agent", "skill", "install", "cursor", "--json"])
    install = Path(ap.resolve("cursor").install_root)
    assert install.is_dir()
    code, _, _ = _cli(cfg, ["agent", "skill", "uninstall", "cursor", "--json"])
    assert code == 0
    assert not install.exists()


def test_cli_agent_skill_diff_no_changes(app, tmp_path):
    cfg = _make_config(tmp_path)
    _cli(cfg, ["agent", "skill", "install", "cursor", "--json"])
    code, payload, _ = _cli(cfg, ["agent", "skill", "diff", "cursor", "--json"])
    assert code == 0, payload
    assert payload["data"]["differences"] == []


def test_cli_agent_skill_path_default(app, tmp_path):
    cfg = _make_config(tmp_path)
    code, payload, _ = _cli(cfg, ["agent", "skill", "path", "cursor", "--json"])
    assert code == 0, payload
    assert payload["data"]["path"] == str(ap.resolve("cursor").install_root)


def test_cli_agent_skill_path_source(app, tmp_path):
    cfg = _make_config(tmp_path)
    code, payload, _ = _cli(cfg, ["agent", "skill", "path", "--source", "--json"])
    assert code == 0, payload
    assert payload["data"]["path"].endswith("skills")


def test_cli_bare_agent_skill_still_works(app, tmp_path):
    # PR #1 is additive — `maxc agent skill --json` (no verb) must still emit
    # the legacy single-skill envelope so existing scripts keep working until
    # PR #2 removes the leaf form.
    cfg = _make_config(tmp_path)
    code, payload, _ = _cli(cfg, ["agent", "skill", "--json"])
    assert code == 0, payload
    assert "skill_path" in payload.get("data", {})
