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


def test_skill_install_writes_extra_files_for_claude_code(app, tmp_path):
    env = app.skill_install(platform="claude-code", invocation="maxc")
    install = Path(env.data["install_path"])
    assert (install / ".claude-plugin" / "plugin.json").is_file()


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
