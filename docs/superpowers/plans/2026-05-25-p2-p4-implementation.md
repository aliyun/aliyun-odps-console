# P2 + P4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mature the SKILL distribution system (six-verb `agent skill` + platform registry + drift detection) and ship P4 polish (flag hoist, exit code unification, error context, CI slim-down).

**Architecture:** Two parallel tracks — P2 (PRs #1–#2) extracts a `Platform` registry then hard-removes the legacy `agent install-skill` entrypoint, and P4 (PRs #3–#7) does focused single-file improvements. Each PR is independently mergeable and passes tests on its own; P2-B can only land after P2-A; P4-D-gate can only land after P4-D-fix.

**Tech Stack:** Python 3.8+, argparse, dataclasses, pytest, importlib.resources, ruff (new), pytest-cov (new).

**Spec source:** [`docs/superpowers/specs/2026-05-25-p2-p4-design.md`](../specs/2026-05-25-p2-p4-design.md)

---

## Conventions used by every task below

- **TDD:** write failing test → run it to confirm RED → implement minimal code → run test to confirm GREEN → commit.
- **Commit messages:** Conventional Commits — `feat:`, `fix:`, `refactor:`, `chore:`, `test:`, `docs:`. Sign with `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>`.
- **Run all tests** at the end of each PR's last task: `pytest -q` (must be green before the PR's final commit).
- **Line-number drift:** the spec used `<file>:<N>` line numbers taken at HEAD of 2026-05-25. If your branch has rebased, locate by **text marker** (e.g., `_ACTION_TITLES["agent.skill"]`, `if action == "agent.install-skill"`) rather than trusting the line number.
- **Don't touch** what the task doesn't list. Bigger blast radius = more PRs; resist the urge to "fix this while I'm here".
- **`grep` before each removal step**: `grep -rn "<marker>"` from repo root to make sure nothing outside the listed files references it.

---

# PR #1 — P2-A: Platform registry + new `agent skill` commands (pure additive)

**Goal:** Introduce `src/maxc_cli/agent_platforms.py` and the six new `agent skill {install,update,uninstall,list,diff,path}` subcommands. The existing `agent install-skill` remains untouched and keeps working — both code paths coexist.

**End-state check:** `maxc agent skill list --json` returns an envelope; `maxc agent install-skill claude-code --json` still works; all existing tests pass; new tests pass.

---

### Task 1: Create `agent_platforms.py` (dataclasses + REGISTRY + resolve)

**Files:**
- Create: `src/maxc_cli/agent_platforms.py`
- Test: `tests/test_agent_platforms.py`

- [ ] **Step 1: Write failing tests for the registry shape**

```python
# tests/test_agent_platforms.py
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


def test_claude_code_has_plugin_json_extra_file():
    claude = ap.resolve("claude-code")
    assert any(ef.relative_path == ".claude-plugin/plugin.json"
               for ef in claude.extra_files)


def test_install_root_matches_legacy_paths():
    # Snapshot the seven legacy install_dirs from app.py:3375-3411 (HEAD@2026-05-25)
    # to guarantee byte-equivalent migration. If a path changes, this test fails
    # and the new value must be justified in the PR description (it would break
    # already-installed users).
    expected = {
        "claude-code": Path.home() / ".claude" / "plugins" / "maxc-cli",
        "cursor":      Path.home() / ".cursor"    / "skills" / "maxcompute-cli-guidance",
        "windsurf":    Path.home() / ".windsurf"  / "skills" / "maxcompute-cli-guidance",
        "qwen":        Path.home() / ".qwen"      / "skills" / "maxcompute-cli-guidance",
        "qoder":       Path.home() / ".qoder"     / "skills" / "maxcompute-cli-guidance",
        "qoderwork":   Path.home() / ".qoderwork" / "skills" / "maxcompute-cli-guidance",
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
    assert ap.resolve("codex").install_root == (
        tmp_path / "my-codex" / "skills" / "maxcompute-cli-guidance"
    )


def test_render_claude_plugin_writes_declared_path(tmp_path):
    # The render_fn must write to install_dir / extra_file.relative_path.
    # If it writes elsewhere, agent skill diff/uninstall can't find the file.
    ap.render_claude_plugin(tmp_path, cli="maxc", cli_module="python3 -m maxc_cli")
    assert (tmp_path / ".claude-plugin" / "plugin.json").is_file()


def test_render_claude_plugin_byte_equivalent_to_legacy():
    # Locked-in byte output prevents `agent skill diff` from reporting a phantom
    # delta on day-1 for users who already have plugin.json installed via the
    # legacy code path (app.py:3493-3499).
    import io
    buf = io.StringIO()
    expected = (
        '{\n  "name": "maxc-cli",\n'
        '  "description": "MaxCompute/ODPS CLI — query tables, view schema, '
        'search metadata, execute SQL, check partitions, sample data, '
        'track jobs. Install via: pip install maxc-cli",\n'
        '  "author": { "name": "maxc-cli contributors" }\n}\n'
    )
    import tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        ap.render_claude_plugin(pathlib.Path(td), cli="maxc", cli_module="x")
        actual = (pathlib.Path(td) / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8")
    assert actual == expected
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_agent_platforms.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'maxc_cli.agent_platforms'`.

- [ ] **Step 3: Implement `agent_platforms.py`**

Create `src/maxc_cli/agent_platforms.py`:

```python
"""Agent platform registry — single source of truth for SKILL install targets.

Each Platform declares where SKILL.md (and optional extra files) live for a
specific agent product. The MaxCApp skill_install/update/uninstall/list/diff/path
methods all consult REGISTRY here, so adding a new platform = one entry below
(no code changes elsewhere).

CRITICAL: install_root values must stay byte-equivalent to the legacy
`_SKILL_PLATFORMS` table in `app.py:3375-3411` — see tests/test_agent_platforms.py
::test_install_root_matches_legacy_paths.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

RenderFn = Callable[[Path, str, str], None]
# (effective_install_root, cli, cli_module) -> None
# Callee mkdir + write_text directly; returns nothing.
# The first arg is the *effective* install root (after --dir override is applied
# by the caller), not platform.install_root.
# Args (cli, cli_module) are currently only consumed by SKILL.md template render;
# extra_files with fixed-literal output (e.g., plugin.json) may ignore them.
# Keep them in the signature so future extra_files using {{cli}} placeholders
# don't need a protocol bump.


@dataclass(frozen=True)
class ExtraFile:
    """Non-SKILL file co-installed with the skill (e.g., claude-plugin manifest).

    `relative_path` is the discovery anchor for list/diff/uninstall — the
    render_fn MUST write its content at `effective_install_root / relative_path`,
    otherwise diff will report "file missing". Enforced by
    test_render_fn_writes_declared_path.
    """
    relative_path: str
    render_fn_name: str   # name of a callable in this module matching RenderFn


@dataclass(frozen=True)
class Platform:
    name: str
    install_root: Path
    skill_subpath: str | None = None  # SKILL contents location relative to install_root;
                                      # None = write into effective_install_root directly
                                      # (claude-code uses this — SKILL.md and .claude-plugin/
                                      # are siblings).
    requires_dir: bool = True
    extra_files: tuple[ExtraFile, ...] = ()
    aliases: tuple[str, ...] = ()
    deprecated_aliases: tuple[str, ...] = ()
    next_step_hint: str = ""


def _claude_root() -> Path:
    return Path.home() / ".claude" / "plugins" / "maxc-cli"


def _codex_root() -> Path:
    # Lazy — must re-read CODEX_HOME at call time, not module-import time,
    # so the env var can be overridden in tests/CI.
    return (
        Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
        / "skills"
        / "maxcompute-cli-guidance"
    )


def _simple_root(dotdir: str) -> Path:
    return Path.home() / dotdir / "skills" / "maxcompute-cli-guidance"


def render_claude_plugin(install_dir: Path, cli: str, cli_module: str) -> None:
    """Write `.claude-plugin/plugin.json` under `install_dir`.

    Byte-equivalent to legacy literal at `app.py:3493-3499`. Do NOT reformat
    via json.dumps(indent=…) — that re-flows whitespace and causes
    `agent skill diff` to report a phantom delta against pre-existing installs.
    """
    meta_dir = install_dir / ".claude-plugin"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "plugin.json").write_text(
        '{\n  "name": "maxc-cli",\n'
        '  "description": "MaxCompute/ODPS CLI — query tables, view schema, '
        'search metadata, execute SQL, check partitions, sample data, '
        'track jobs. Install via: pip install maxc-cli",\n'
        '  "author": { "name": "maxc-cli contributors" }\n}\n',
        encoding="utf-8",
    )


# REGISTRY entries use property-style install_root via field(default_factory=…)
# so test_codex_install_root_respects_CODEX_HOME's monkeypatch+reload works.
# Other platforms could be eager, but we keep them all lazy for uniformity.
def _build_registry() -> tuple[Platform, ...]:
    return (
        Platform(
            name="claude-code",
            install_root=_claude_root(),
            skill_subpath=None,
            extra_files=(ExtraFile(".claude-plugin/plugin.json", "render_claude_plugin"),),
            next_step_hint="Run /reload-plugins in Claude Code to activate",
        ),
        Platform(name="cursor",    install_root=_simple_root(".cursor"),
                 next_step_hint="Restart Cursor to activate"),
        Platform(name="windsurf",  install_root=_simple_root(".windsurf"),
                 next_step_hint="Restart Windsurf to activate"),
        Platform(name="codex",     install_root=_codex_root(),
                 next_step_hint="Restart Codex to activate"),
        Platform(name="qwen",      install_root=_simple_root(".qwen"),
                 next_step_hint="Restart Qwen to activate"),
        Platform(name="qoder",     install_root=_simple_root(".qoder"),
                 next_step_hint="Restart Qoder to activate"),
        Platform(name="qoderwork", install_root=_simple_root(".qoderwork"),
                 next_step_hint="Restart QoderWork to activate"),
    )


REGISTRY: tuple[Platform, ...] = _build_registry()


def resolve(name_or_alias: str) -> Platform:
    """Return the Platform matching name / alias / deprecated_alias.

    Raises KeyError when no match. Caller is responsible for emitting a
    stderr warning when the input matches `deprecated_aliases`.
    """
    for p in REGISTRY:
        if name_or_alias == p.name:
            return p
        if name_or_alias in p.aliases:
            return p
        if name_or_alias in p.deprecated_aliases:
            return p
    raise KeyError(f"Unknown agent platform: {name_or_alias!r}")


def all_platforms() -> tuple[Platform, ...]:
    return REGISTRY


def is_deprecated_alias(name: str) -> bool:
    for p in REGISTRY:
        if name in p.deprecated_aliases:
            return True
    return False


def effective_target(platform: Platform, dir_override: Path | None) -> Path:
    """Resolve where SKILL content for `platform` should be written.

    effective_target = (--dir override OR platform.install_root) / (skill_subpath or "")
    """
    root = dir_override if dir_override is not None else platform.install_root
    if platform.skill_subpath:
        return root / platform.skill_subpath
    return root


# Resolve a render_fn name (declared in ExtraFile) into the actual callable.
# Kept here so caller can `getattr(agent_platforms, name)` without exposing the
# module-internal naming convention to MaxCApp.
def get_render_fn(name: str) -> RenderFn:
    fn = globals().get(name)
    if not callable(fn):
        raise KeyError(f"render_fn not found in agent_platforms: {name!r}")
    return fn  # type: ignore[return-value]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_agent_platforms.py -v`
Expected: All 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maxc_cli/agent_platforms.py tests/test_agent_platforms.py
git commit -m "$(cat <<'EOF'
feat(agent): add platform registry (agent_platforms.py)

Single source of truth for SKILL install targets. Frozen dataclass Platform
captures install_root + skill_subpath + extra_files; REGISTRY holds the seven
existing platforms with byte-equivalent paths to the legacy _SKILL_PLATFORMS
table. This is purely additive — the legacy MaxCApp.agent_install_skill code
keeps running until PR #2 removes it.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add `MaxCApp.skill_install / skill_update / skill_uninstall / skill_list / skill_diff / skill_path`

**Files:**
- Modify: `src/maxc_cli/app.py` (add new methods near `agent_install_skill`, keep both)
- Test: `tests/test_agent_skill_commands.py` (new)

- [ ] **Step 1: Write failing test for `skill_install`**

```python
# tests/test_agent_skill_commands.py
"""Tests for the new six-verb `agent skill` MaxCApp methods.

These coexist with the legacy `agent_install_skill` (PR #1 keeps both).
Once PR #2 lands, the legacy method is deleted but these tests still hold.
"""
from pathlib import Path
import pytest
from maxc_cli.app import MaxCApp
from maxc_cli.config import MaxCConfig
from maxc_cli import agent_platforms as ap


@pytest.fixture
def app(tmp_path, monkeypatch):
    # Redirect HOME so install_root for every platform lives under tmp_path
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    # Re-build REGISTRY because module-level _build_registry captured the old HOME
    import importlib
    importlib.reload(ap)
    cfg = MaxCConfig()
    return MaxCApp(config=cfg, load_backend=False)


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
    # `update` with neither platform nor --all → ValidationError
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_agent_skill_commands.py -v`
Expected: FAIL with `AttributeError: 'MaxCApp' object has no attribute 'skill_install'` (or similar).

- [ ] **Step 3: Implement the six methods in `MaxCApp`**

In `src/maxc_cli/app.py`, after the existing `agent_install_skill` method (around line 3572), add (do not modify `agent_install_skill` itself — it stays for PR #1):

```python
    # ── new agent skill {install,update,uninstall,list,diff,path} ────────────
    # These coexist with the legacy `agent_install_skill` for one release.
    # PR #2 deletes the legacy method + the _SKILL_PLATFORMS / _SKILL_INVOCATIONS
    # dicts above, after which only these methods remain.

    def _resolve_skill_target(
        self,
        platform_name: 'str',
        dir_override: 'Path | None',
    ) -> 'tuple[agent_platforms.Platform, Path]':
        from . import agent_platforms
        try:
            platform = agent_platforms.resolve(platform_name)
        except KeyError as exc:
            raise ValidationError(str(exc))
        if agent_platforms.is_deprecated_alias(platform_name):
            # caller (CLI handler) is responsible for stderr emission;
            # the app layer only annotates the envelope.
            pass
        target = agent_platforms.effective_target(platform, dir_override)
        return platform, target

    def _locate_skills_source(self) -> 'Path':
        import importlib.resources
        try:
            skills_dir = importlib.resources.files("maxc_cli") / "skills"
            if not skills_dir.is_dir():
                raise MaxCError("Skills directory not found in installed package")
            return Path(str(skills_dir))
        except MaxCError:
            raise
        except Exception as exc:
            raise MaxCError(f"Cannot locate skills directory: {exc}")

    def _render_skill_into(
        self,
        skills_src: 'Path',
        target_dir: 'Path',
        platform: 'agent_platforms.Platform',
        invocation_map: 'dict[str, str]',
        force: 'bool',
    ) -> 'list[str]':
        """Render SKILL.md + references/ + extra_files into target_dir.

        Returns the list of file/dir names written. Re-uses the legacy
        excluded-names set + render_skill_template logic so behavior matches
        agent_install_skill byte-for-byte (PR #2 deletion safety).
        """
        import shutil

        EXCLUDED_NAMES = {
            ".git", "__pycache__", ".DS_Store", "nohup.out",
            ".gitignore", ".pytest_cache", ".mypy_cache", ".ruff_cache",
        }
        EXCLUDED_SUFFIXES = (".pyc", ".pyo", ".log")
        TEMPLATED_SUFFIXES = (".md", ".yaml", ".yml")

        cli_str = invocation_map["cli"]
        cli_module_str = invocation_map["cli_module"]

        def _is_excluded(name: 'str') -> 'bool':
            if name in EXCLUDED_NAMES:
                return True
            return any(name.endswith(suf) for suf in EXCLUDED_SUFFIXES)

        def _render_or_copy(src: 'Path', dst: 'Path') -> 'None':
            if not force and dst.exists():
                # force is enforced at the file level; if not force, skip overwrite
                return
            if src.suffix.lower() in TEMPLATED_SUFFIXES:
                content = render_skill_template(
                    src.read_text(encoding="utf-8"),
                    cli=cli_str,
                    cli_module=cli_module_str,
                )
                dst.write_text(content, encoding="utf-8")
                try:
                    shutil.copystat(str(src), str(dst))
                except OSError:
                    pass
            else:
                shutil.copy2(str(src), str(dst))

        def _render_tree(src_dir: 'Path', dst_dir: 'Path') -> 'None':
            dst_dir.mkdir(parents=True, exist_ok=True)
            for child in src_dir.iterdir():
                if _is_excluded(child.name):
                    continue
                target = dst_dir / child.name
                if child.is_file():
                    _render_or_copy(child, target)
                elif child.is_dir():
                    _render_tree(child, target)

        target_dir.mkdir(parents=True, exist_ok=True)

        files_copied: 'list[str]' = []
        for item in skills_src.iterdir():
            if _is_excluded(item.name):
                continue
            dst = target_dir / item.name
            if item.is_file():
                _render_or_copy(item, dst)
                files_copied.append(item.name)
            elif item.is_dir():
                if force and dst.exists():
                    import shutil as _sh
                    _sh.rmtree(str(dst))
                _render_tree(item, dst)
                files_copied.append(item.name + "/")

        # Extra files (e.g., claude-plugin/plugin.json)
        from . import agent_platforms
        for ef in platform.extra_files:
            render_fn = agent_platforms.get_render_fn(ef.render_fn_name)
            render_fn(target_dir, cli_str, cli_module_str)
            files_copied.append(ef.relative_path)

        return sorted(files_copied)

    def skill_install(
        self,
        *,
        platform: 'str',
        invocation: 'str' = "maxc",
        dir_override: 'Path | None' = None,
        force: 'bool' = False,
    ) -> 'Envelope':
        if invocation not in self._SKILL_INVOCATIONS:
            raise ValidationError(
                f"Unsupported invocation: {invocation}. "
                f"Supported: {', '.join(self._SKILL_INVOCATIONS)}"
            )
        platform_spec, target = self._resolve_skill_target(platform, dir_override)
        invocation_map = self._SKILL_INVOCATIONS[invocation]
        skills_src = self._locate_skills_source()
        version_marker = f"{__version__}+{invocation}"
        marker_path = target / ".maxc-skill-version"
        if not force and marker_path.is_file() and marker_path.read_text().strip() == version_marker:
            return Envelope(
                command="agent.skill.install",
                status="success",
                data={
                    "platform": platform_spec.name,
                    "invocation": invocation,
                    "install_path": str(target),
                    "installed_version": __version__,
                    "upgraded": False,
                    "files_copied": [],
                    "next_step": "Skill is already up to date",
                },
            )
        files = self._render_skill_into(skills_src, target, platform_spec, invocation_map, force=True)
        marker_path.write_text(version_marker)
        return Envelope(
            command="agent.skill.install",
            status="success",
            data={
                "platform": platform_spec.name,
                "invocation": invocation,
                "install_path": str(target),
                "installed_version": __version__,
                "upgraded": True,
                "files_copied": files,
                "next_step": platform_spec.next_step_hint,
            },
        )

    def skill_update(
        self,
        *,
        platform: 'str | None',
        all_platforms: 'bool',
        invocation: 'str' = "maxc",
    ) -> 'Envelope':
        from . import agent_platforms
        if platform is None and not all_platforms:
            raise ValidationError(
                "agent skill update requires either a <platform> argument or --all"
            )
        if platform is not None and all_platforms:
            raise ValidationError(
                "agent skill update accepts either <platform> or --all, not both"
            )
        if platform is not None:
            env = self.skill_install(platform=platform, invocation=invocation, force=True)
            env.command = "agent.skill.update"
            return env
        # --all: iterate every REGISTRY platform whose default install_root
        # has a .maxc-skill-version (i.e., previously installed at default path).
        updated: 'list[str]' = []
        for p in agent_platforms.all_platforms():
            target = agent_platforms.effective_target(p, None)
            if (target / ".maxc-skill-version").is_file():
                self.skill_install(platform=p.name, invocation=invocation, force=True)
                updated.append(p.name)
        return Envelope(
            command="agent.skill.update",
            status="success",
            data={"platforms_updated": updated, "invocation": invocation},
        )

    def skill_uninstall(
        self,
        *,
        platform: 'str',
        dir_override: 'Path | None' = None,
    ) -> 'Envelope':
        import shutil
        _, target = self._resolve_skill_target(platform, dir_override)
        removed = False
        if target.exists():
            shutil.rmtree(str(target))
            removed = True
        return Envelope(
            command="agent.skill.uninstall",
            status="success",
            data={"platform": platform, "install_path": str(target), "removed": removed},
        )

    def skill_list(self) -> 'Envelope':
        from . import agent_platforms
        installed: 'list[dict[str, Any]]' = []
        for p in agent_platforms.all_platforms():
            target = agent_platforms.effective_target(p, None)
            marker = target / ".maxc-skill-version"
            if marker.is_file():
                installed.append({
                    "platform": p.name,
                    "install_path": str(target),
                    "installed_version_marker": marker.read_text().strip(),
                })
        hints = AgentHints(warnings=[
            "agent skill list only inspects default install paths. "
            "If you installed with --dir <CUSTOM>, that copy is not shown — "
            "pass --platform <name> --dir <CUSTOM> to skill_path to verify."
        ])
        return Envelope(
            command="agent.skill.list",
            status="success",
            data={"installed": installed},
            agent_hints=hints,
        )

    def skill_diff(
        self,
        *,
        platform: 'str',
        unified: 'bool' = False,
        dir_override: 'Path | None' = None,
    ) -> 'Envelope':
        import difflib
        platform_spec, target = self._resolve_skill_target(platform, dir_override)
        skills_src = self._locate_skills_source()
        differences: 'list[dict[str, Any]]' = []
        for src in skills_src.rglob("*"):
            if not src.is_file():
                continue
            rel = src.relative_to(skills_src)
            dst = target / rel
            if not dst.exists():
                differences.append({"path": str(rel), "kind": "missing"})
                continue
            src_text = src.read_text(encoding="utf-8", errors="replace")
            dst_text = dst.read_text(encoding="utf-8", errors="replace")
            if src.suffix.lower() in (".md", ".yaml", ".yml"):
                # Render the source through the same template the install path
                # uses, otherwise placeholders look like fake deltas.
                invocation_map = self._SKILL_INVOCATIONS["maxc"]
                src_text = render_skill_template(
                    src_text, cli=invocation_map["cli"], cli_module=invocation_map["cli_module"]
                )
            if src_text != dst_text:
                entry: 'dict[str, Any]' = {"path": str(rel), "kind": "modified"}
                if unified:
                    entry["diff"] = "".join(difflib.unified_diff(
                        dst_text.splitlines(keepends=True),
                        src_text.splitlines(keepends=True),
                        fromfile=f"local/{rel}",
                        tofile=f"wheel/{rel}",
                    ))
                differences.append(entry)
        return Envelope(
            command="agent.skill.diff",
            status="success",
            data={
                "platform": platform_spec.name,
                "install_path": str(target),
                "differences": differences,
            },
        )

    def skill_path(
        self,
        *,
        platform: 'str | None' = None,
        source: 'bool' = False,
        dir_override: 'Path | None' = None,
    ) -> 'Envelope':
        if source:
            return Envelope(
                command="agent.skill.path",
                status="success",
                data={"path": str(self._locate_skills_source()), "kind": "source"},
            )
        if platform is None:
            raise ValidationError(
                "agent skill path requires --platform <name> unless --source is given"
            )
        _, target = self._resolve_skill_target(platform, dir_override)
        return Envelope(
            command="agent.skill.path",
            status="success",
            data={"path": str(target), "kind": "target", "platform": platform},
        )
```

Also add these imports at the top of `app.py` if missing (check first):
```python
from .exceptions import ValidationError  # likely already present
```

- [ ] **Step 4: Run new tests to verify they pass**

Run: `pytest tests/test_agent_skill_commands.py -v`
Expected: All ~14 tests PASS.

- [ ] **Step 5: Run full suite to make sure legacy path still works**

Run: `pytest -q tests/test_agent_skill_commands_context.py tests/test_agent_skill_commands.py tests/test_agent_platforms.py`
Expected: All PASS (legacy install-skill tests unchanged).

- [ ] **Step 6: Commit**

```bash
git add src/maxc_cli/app.py tests/test_agent_skill_commands.py
git commit -m "$(cat <<'EOF'
feat(agent): add skill_{install,update,uninstall,list,diff,path} on MaxCApp

Six new methods backed by agent_platforms.REGISTRY. Coexist with the legacy
agent_install_skill — PR #2 will delete the legacy code. Render logic moved
into a private _render_skill_into helper that mirrors the legacy excludes
and templating rules so existing installs see no byte-level drift.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Wire CLI subparsers + handlers + `_LOCAL_ONLY_COMMANDS` for the six new verbs

**Files:**
- Modify: `src/maxc_cli/cli.py` (insert new subparsers after `agent_skill` parser at ~line 485; add handlers next to `_handle_agent_install_skill` at ~line 1339; extend `_LOCAL_ONLY_COMMANDS` at ~line 1758)
- Test: `tests/test_agent_skill_commands.py` (add CLI invocation tests)

- [ ] **Step 1: Write failing CLI tests**

Append to `tests/test_agent_skill_commands.py`:

```python
# ── CLI integration tests ────────────────────────────────────────────────────
import json
import io
from maxc_cli.cli import main


def _run(argv, monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    import importlib
    import maxc_cli.agent_platforms as ap_
    importlib.reload(ap_)
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(argv)  # main reads sys.argv if argv=None; pass explicitly
    return code, stdout.getvalue(), stderr.getvalue()


def test_cli_agent_skill_install_returns_envelope(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    import importlib
    import maxc_cli.agent_platforms as ap_
    importlib.reload(ap_)
    code = main(["agent", "skill", "install", "cursor", "--json"])
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "agent.skill.install"


def test_cli_agent_skill_list_empty_then_populated(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    import importlib
    import maxc_cli.agent_platforms as ap_
    importlib.reload(ap_)
    main(["agent", "skill", "list", "--json"])
    out1 = json.loads(capsys.readouterr().out)
    assert out1["data"]["installed"] == []
    main(["agent", "skill", "install", "cursor", "--json"])
    capsys.readouterr()
    main(["agent", "skill", "list", "--json"])
    out2 = json.loads(capsys.readouterr().out)
    assert [p["platform"] for p in out2["data"]["installed"]] == ["cursor"]


def test_cli_agent_skill_update_requires_all_or_platform(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    import importlib
    import maxc_cli.agent_platforms as ap_
    importlib.reload(ap_)
    code = main(["agent", "skill", "update", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "failure"
    assert out["error"]["code"] == "VALIDATION_ERROR"
    assert code != 0  # P4-A will tighten this to a specific exit code


def test_cli_agent_skill_does_not_require_backend_auth(monkeypatch, tmp_path, capsys):
    """`agent skill *` must be in _LOCAL_ONLY_COMMANDS to avoid spurious
    ODPS client construction on fresh machines before `auth login`."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("ALIBABA_CLOUD_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("ODPS_ACCESS_ID", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    import importlib
    import maxc_cli.agent_platforms as ap_
    importlib.reload(ap_)
    code = main(["agent", "skill", "install", "cursor", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "success"
    assert code == 0
```

- [ ] **Step 2: Run failing**

Run: `pytest tests/test_agent_skill_commands.py -v -k "cli_"`
Expected: FAIL (commands not registered).

- [ ] **Step 3: Add CLI subparsers + handlers**

In `src/maxc_cli/cli.py`, **after** the existing `agent_skill` parser (around line 485, before `agent_install_skill`), insert the six-verb tree:

```python
    # ── agent skill {install,update,uninstall,list,diff,path} ────────────────
    agent_skill_subparsers = _add_required_subparsers(agent_skill, dest="agent_skill_command")

    _ask_install = _make_parser(
        agent_skill_subparsers, "install", "agent.skill.install",
        help="Install SKILL into an agent platform",
    )
    _ask_install.add_argument(
        "platform",
        choices=[p.name for p in __import__("maxc_cli.agent_platforms", fromlist=["REGISTRY"]).REGISTRY],
    )
    _ask_install.add_argument("--dir", dest="dir_override", default=None,
                              help="Override the platform's default install root")
    _ask_install.add_argument("--invocation", default=None, choices=["maxc", "aliyun-maxc"])
    _ask_install.add_argument("--force", action="store_true",
                              help="Overwrite existing files unconditionally")
    _ask_install.add_argument("--json", action="store_true")
    _ask_install.set_defaults(handler=_handle_agent_skill_install)

    _ask_update = _make_parser(
        agent_skill_subparsers, "update", "agent.skill.update",
        help="Re-render SKILL for one or all installed platforms",
    )
    _ask_update.add_argument("platform", nargs="?", default=None,
                             choices=[p.name for p in __import__("maxc_cli.agent_platforms", fromlist=["REGISTRY"]).REGISTRY])
    _ask_update.add_argument("--all", dest="all_platforms", action="store_true",
                             help="Update every previously installed platform")
    _ask_update.add_argument("--invocation", default=None, choices=["maxc", "aliyun-maxc"])
    _ask_update.add_argument("--json", action="store_true")
    _ask_update.set_defaults(handler=_handle_agent_skill_update)

    _ask_uninstall = _make_parser(
        agent_skill_subparsers, "uninstall", "agent.skill.uninstall",
        help="Remove SKILL from an agent platform's directory",
    )
    _ask_uninstall.add_argument(
        "platform",
        choices=[p.name for p in __import__("maxc_cli.agent_platforms", fromlist=["REGISTRY"]).REGISTRY],
    )
    _ask_uninstall.add_argument("--dir", dest="dir_override", default=None)
    _ask_uninstall.add_argument("--json", action="store_true")
    _ask_uninstall.set_defaults(handler=_handle_agent_skill_uninstall)

    _ask_list = _make_parser(
        agent_skill_subparsers, "list", "agent.skill.list",
        help="List platforms with SKILL installed at the default path",
    )
    _ask_list.add_argument("--json", action="store_true")
    _ask_list.set_defaults(handler=_handle_agent_skill_list)

    _ask_diff = _make_parser(
        agent_skill_subparsers, "diff", "agent.skill.diff",
        help="Show what differs between the installed SKILL and the wheel's copy",
    )
    _ask_diff.add_argument(
        "platform",
        choices=[p.name for p in __import__("maxc_cli.agent_platforms", fromlist=["REGISTRY"]).REGISTRY],
    )
    _ask_diff.add_argument("--unified", action="store_true",
                           help="Include per-file unified diff text in the output")
    _ask_diff.add_argument("--dir", dest="dir_override", default=None)
    _ask_diff.add_argument("--json", action="store_true")
    _ask_diff.set_defaults(handler=_handle_agent_skill_diff)

    _ask_path = _make_parser(
        agent_skill_subparsers, "path", "agent.skill.path",
        help="Print SKILL install path (or --source for the wheel's source dir)",
    )
    _ask_path.add_argument("--platform", default=None,
                           choices=[p.name for p in __import__("maxc_cli.agent_platforms", fromlist=["REGISTRY"]).REGISTRY])
    _ask_path.add_argument("--source", action="store_true",
                           help="Print the wheel-internal skills/ source path instead of the install target")
    _ask_path.add_argument("--dir", dest="dir_override", default=None)
    _ask_path.add_argument("--json", action="store_true")
    _ask_path.set_defaults(handler=_handle_agent_skill_path)
```

> **Note:** the inline `__import__("maxc_cli.agent_platforms", fromlist=["REGISTRY"]).REGISTRY` keeps `build_parser()` free of a top-level import that would create a circular-import risk during early CLI bootstrap. If your linter complains, hoist a single module-level `from .agent_platforms import REGISTRY as _AGENT_REGISTRY` near the imports — but be sure agent_platforms doesn't import from cli.py.

Now add the six handlers near `_handle_agent_install_skill` (~line 1339):

```python
def _resolve_dir_override(args: 'argparse.Namespace') -> 'Path | None':
    val = getattr(args, "dir_override", None)
    return Path(val).expanduser() if val else None


def _handle_agent_skill_install(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    invocation = args.invocation or _detect_invocation()
    envelope = app.skill_install(
        platform=args.platform,
        invocation=invocation,
        dir_override=_resolve_dir_override(args),
        force=getattr(args, "force", False),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_update(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    invocation = args.invocation or _detect_invocation()
    envelope = app.skill_update(
        platform=args.platform,
        all_platforms=getattr(args, "all_platforms", False),
        invocation=invocation,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_uninstall(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.skill_uninstall(
        platform=args.platform,
        dir_override=_resolve_dir_override(args),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_list(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.skill_list()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_diff(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.skill_diff(
        platform=args.platform,
        unified=getattr(args, "unified", False),
        dir_override=_resolve_dir_override(args),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_path(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.skill_path(
        platform=args.platform,
        source=getattr(args, "source", False),
        dir_override=_resolve_dir_override(args),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
```

- [ ] **Step 4: Extend `_LOCAL_ONLY_COMMANDS`** at line ~1758

```python
_LOCAL_ONLY_COMMANDS = frozenset({
    "auth.login",
    "auth.login-external",
    "auth.whoami",
    "session.set",
    "session.show",
    "session.unset",
    "agent.context",
    "agent.skill",
    "agent.install-skill",
    # New skill subverbs — local-only (no backend needed)
    "agent.skill.install",
    "agent.skill.update",
    "agent.skill.uninstall",
    "agent.skill.list",
    "agent.skill.diff",
    "agent.skill.path",
    "cache.status",
    "cache.clear",
})
```

- [ ] **Step 5: Run CLI tests**

Run: `pytest tests/test_agent_skill_commands.py -v -k "cli_"`
Expected: All 4 CLI tests PASS.

- [ ] **Step 6: Run full suite — must remain green**

Run: `pytest -q`
Expected: All PASS, no regressions in legacy `install-skill` tests.

- [ ] **Step 7: Commit**

```bash
git add src/maxc_cli/cli.py tests/test_agent_skill_commands.py
git commit -m "$(cat <<'EOF'
feat(cli): wire agent skill {install,update,uninstall,list,diff,path}

Six new subparsers under `agent skill`, each backed by the matching MaxCApp
method added in the previous commit. All six are added to _LOCAL_ONLY_COMMANDS
so they don't trigger ODPS client construction on fresh machines.

The legacy `agent install-skill` parser/handler is unchanged — PR #2 removes
it. This commit ships strictly additive: existing scripts keep working.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

> 🟢 **PR #1 done.** Open the PR. After it merges, proceed to PR #2.

---

# PR #2 — P2-B: Hard-remove legacy `agent install-skill` + SKILL doc replacement + enable drift test

**Goal:** Delete the old code paths and rewrite every reference to `agent install-skill` → `agent skill install`. Add the SKILL ↔ CLI drift detection test (enabled — it should pass on day 1).

**End-state check:** `maxc agent install-skill` produces an argparse error; `maxc agent skill install` still works; SKILL.md, README, all docs, and Aone scripts use the new form; `pytest tests/test_skill_cli_consistency.py` is green.

---

### Task 4: Pre-flight grep (audit footprint before deletion)

**Files:** none (pure inspection)

- [ ] **Step 1: Grep the repo for every install-skill reference**

```bash
grep -rn "install-skill\|install_skill\|agent.install-skill" \
  --exclude-dir=.git --exclude-dir=.venv --exclude-dir=__pycache__
```

Expected hits (from spec §2.1 + repo audit):
- `src/maxc_cli/cli.py:487-511, 1339-1341, 1767`
- `src/maxc_cli/app.py:3413-3561` (method) and `3413,3476,3561` (string literals inside it)
- `src/maxc_cli/models.py:373` (`_ACTION_TITLES`) + `560-561` (handler branch)
- `src/maxc_cli/skills/references/command-patterns.md:266-272,277`
- `README.md:40,50,56,59`
- `docs/install-guide.md`, `docs/promo.md`, `docs/maxc-cli-launch-article.md`,
  `docs/design.md`, `docs/ata-maxc-cli.md`, `docs/ARCHITECTURE.md`,
  `docs/implementation.md`, `docs/roadmap.md` (review-only — historical roadmap
  entries can stay as past-tense facts; user-facing doc lines need updating)
- `scripts/bootstrap.sh:513`, `scripts/bootstrap-ncs.sh:701`, `scripts/bootstrap-ncs.ps1:643`
- `scripts/bootstrap-ncs-prompt.md:224`
- `tests/test_agent_skill_commands_context.py` — full file, ~30 occurrences

> If grep finds any path **not** in the list above, stop and add it to the deletion plan before proceeding. Surprise references → surprise breakage for someone.

- [ ] **Step 2: Confirm no external `.gitlab` mirror or third-party script consumes `install-skill`**

This is a judgment call. The spec mitigation (§6) says: "if you can't sync external scripts, fall back to keeping a deprecation alias". For this PR we assume no external consumer — if your team has one, surface the question to the user before deleting.

---

### Task 5: Delete legacy code (cli.py + app.py + models.py)

**Files:**
- Modify: `src/maxc_cli/cli.py:487-511` (subparser), `:1339-1341` (handler), `:1767` (frozenset entry)
- Modify: `src/maxc_cli/app.py:3363-3411` (`_SKILL_INVOCATIONS` + `_SKILL_PLATFORMS`), `:3413-3572` (`agent_install_skill`)
- Modify: `src/maxc_cli/models.py:372-373` (rename + delete `_ACTION_TITLES` entries), `:558-561` (handler branches)

- [ ] **Step 1: Delete in `cli.py`**

Locate the parser block by text marker (`"install-skill"`, `"agent.install-skill"`) — line numbers may have drifted. Remove:

1. The entire `agent_install_skill = _make_parser(...)` block at ~lines 487-511 (12 lines).
2. The `_handle_agent_install_skill` handler at ~lines 1339-1342 (4 lines).
3. From `_LOCAL_ONLY_COMMANDS` frozenset at ~line 1767: delete the `"agent.install-skill",` line.

> Keep `agent.skill.{install,update,uninstall,list,diff,path}` entries from PR #1.

- [ ] **Step 2: Delete in `app.py`**

Remove:

1. `_SKILL_INVOCATIONS` dict (~lines 3363-3372) — **wait**: the new `skill_install` references `self._SKILL_INVOCATIONS`. Before deleting from `app.py`, **migrate the dict into `agent_platforms.py`** as a module-level constant `INVOCATIONS = {...}`, then update `skill_install`/`skill_update`/`skill_diff` in `app.py` to `from . import agent_platforms; agent_platforms.INVOCATIONS[invocation]`. After migration, delete `_SKILL_INVOCATIONS` from `app.py`.
2. `_SKILL_PLATFORMS` dict (~lines 3374-3411) — pure deletion (now superseded by `REGISTRY`).
3. The entire `agent_install_skill` method (~lines 3413-3572) — pure deletion.

After this, run `grep -n "_SKILL_PLATFORMS\|_SKILL_INVOCATIONS\|agent_install_skill" src/maxc_cli/` — expect zero hits.

- [ ] **Step 3: Update `models.py`**

In `_ACTION_TITLES` (~lines 365-379), change:

```python
"agent.skill": "Agent skill info",      # OLD
"agent.install-skill": "Install skill", # OLD
```

to:

```python
"agent.skill.list": "List installed skills",
"agent.skill.install": "Install skill",
"agent.skill.update": "Update skill",
"agent.skill.uninstall": "Uninstall skill",
"agent.skill.diff": "Show skill differences",
"agent.skill.path": "Show skill path",
```

(The old `"agent.skill"` entry is removed; breadcrumb consumers reference `agent.skill.list` instead.)

In `action()` dispatch (~lines 556-561), change:

```python
if action == "agent.context":
    return _cli_command("agent", "context", "--json")
if action == "agent.skill":
    return _cli_command("agent", "skill", "--json")
if action == "agent.install-skill":
    return _cli_command("agent", "install-skill", "--json")
```

to:

```python
if action == "agent.context":
    return _cli_command("agent", "context", "--json")
if action == "agent.skill.list":
    return _cli_command("agent", "skill", "list", "--json")
if action == "agent.skill.install":
    return _cli_command("agent", "skill", "install", "--json")
```

(Other `agent.skill.*` forms fall through to the generic `group.subcommand` handler at the bottom of `action()`.)

- [ ] **Step 4: Run tests — expect old install-skill tests to FAIL**

Run: `pytest tests/test_agent_skill_commands_context.py -v`
Expected: ~7 FAIL (every test that calls `["agent", "install-skill", ...]`).

That's intentional — Task 6 rewrites those tests.

- [ ] **Step 5: Commit (RED state for legacy tests)**

```bash
git add src/maxc_cli/cli.py src/maxc_cli/app.py src/maxc_cli/models.py
git commit -m "$(cat <<'EOF'
refactor(agent): remove legacy agent install-skill code paths

Drops the v0.3-era `agent install-skill` subparser, MaxCApp.agent_install_skill
method, and the hard-coded _SKILL_PLATFORMS / _SKILL_INVOCATIONS dicts. The
new agent_platforms.REGISTRY (PR #1) is now the sole source of truth.

_ACTION_TITLES + action() in models.py re-pointed at agent.skill.list /
agent.skill.install — breadcrumbs no longer reference the deleted entry.

tests/test_agent_skill_commands_context.py will fail until rewritten in the
next commit.

BREAKING: scripts and SKILL docs that invoke `maxc agent install-skill` must
switch to `maxc agent skill install`. See release notes.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Migrate `test_agent_skill_commands_context.py` to the new command form

**Files:**
- Modify: `tests/test_agent_skill_commands_context.py` (~486 lines, ~30 occurrences)

- [ ] **Step 1: sed-replace the command form**

```bash
# Use Edit tool, not sed-in-shell — preview each change.
# The pattern is: ["agent", "install-skill"  →  ["agent", "skill", "install"
# But sed is safer here because of the count.
```

Use Edit with `replace_all=True`:

```
old_string: "agent", "install-skill"
new_string: "agent", "skill", "install"
```

Then handle the prose / docstring lines individually:
- Module docstring line 1: `agent install-skill` → `agent skill install`
- Section header at ~line 213: same
- Class docstring at ~line 216: same

- [ ] **Step 2: Update the envelope command-name assertion**

The legacy command emitted `command="agent.install-skill"` (`app.py:3476,3561`). The new `skill_install` emits `command="agent.skill.install"`. Find every assertion:

```bash
grep -n "agent.install-skill\|agent\\.install-skill" tests/test_agent_skill_commands_context.py
```

Replace string literals `"agent.install-skill"` → `"agent.skill.install"`.

- [ ] **Step 3: Run the migrated tests**

Run: `pytest tests/test_agent_skill_commands_context.py -v`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_agent_skill_commands_context.py
git commit -m "$(cat <<'EOF'
test(agent): migrate install-skill tests to agent skill install

Mechanical rewrite — same coverage, new CLI form.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Rewrite user-facing docs + scripts

**Files:**
- Modify: `README.md`, `docs/install-guide.md`, `docs/promo.md`, `docs/maxc-cli-launch-article.md`, `docs/design.md`, `docs/ata-maxc-cli.md`, `docs/ARCHITECTURE.md`, `docs/implementation.md`
- Modify: `scripts/bootstrap.sh:513`, `scripts/bootstrap-ncs.sh:701`, `scripts/bootstrap-ncs.ps1:643`, `scripts/bootstrap-ncs-prompt.md:224`
- Modify: `src/maxc_cli/skills/references/command-patterns.md:266-277`
- **Do not modify:** `docs/roadmap.md` (historical "[x] `maxc agent install-skill` command…" line is past-tense history); `docs/superpowers/plans/2026-04-20-fix-all-issues.md` (historical plan).

- [ ] **Step 1: Run grep, then per-file Edit**

```bash
grep -rn "agent install-skill" README.md docs/ scripts/ src/maxc_cli/skills/
```

For each hit:
- User-facing command-form: replace `agent install-skill` → `agent skill install`.
- README table at line 40 (`| **agent** | \`context\`, \`skill\`, \`install-skill\` |`) → `\`context\`, \`skill install\`, \`skill list\``.
- README headline at line 50 (`### 方式 2：install-skill`) → `### 方式 2：agent skill install`.

For SKILL `command-patterns.md` (which uses `{{cli}}` template), the new form should be `{{cli}} agent skill install <platform> --json`.

- [ ] **Step 2: Verify zero remaining matches in user-facing surface**

```bash
grep -rn "agent install-skill" README.md docs/install-guide.md docs/promo.md \
  docs/maxc-cli-launch-article.md docs/design.md docs/ata-maxc-cli.md \
  docs/ARCHITECTURE.md docs/implementation.md scripts/ src/maxc_cli/skills/
```

Expected: no output.

- [ ] **Step 3: Verify scripts still parse**

```bash
bash -n scripts/bootstrap.sh
bash -n scripts/bootstrap-ncs.sh
```

Expected: no syntax errors.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/install-guide.md docs/promo.md docs/maxc-cli-launch-article.md \
        docs/design.md docs/ata-maxc-cli.md docs/ARCHITECTURE.md docs/implementation.md \
        scripts/bootstrap.sh scripts/bootstrap-ncs.sh scripts/bootstrap-ncs.ps1 \
        scripts/bootstrap-ncs-prompt.md \
        src/maxc_cli/skills/references/command-patterns.md
git commit -m "$(cat <<'EOF'
docs: replace `agent install-skill` with `agent skill install`

Mechanical replacement across README, install-guide, promo articles, design
docs, bootstrap scripts, and the SKILL reference itself. Roadmap historical
entries left untouched (past-tense facts).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Add SKILL minor content edits (decision matrix + on-demand banner)

**Files:**
- Modify: `src/maxc_cli/skills/SKILL.md` (add decision matrix section near top, before Red-lines)
- Modify: every `src/maxc_cli/skills/references/*.md` (add one-line "> Loaded on demand" banner at top)

- [ ] **Step 1: Add decision matrix block to SKILL.md**

Open `src/maxc_cli/skills/SKILL.md`. Find the existing Red-lines section. **Immediately before** it, insert:

```markdown
## Intent → Command Quick Map

Load the matching reference only when the intent below applies. Otherwise stay
in SKILL.md alone.

| Agent intent | First command to try | Load this reference if needed |
|---|---|---|
| Run a SELECT | `{{cli}} query "<sql>" --json` | `references/command-patterns.md` |
| Schema/columns of a table | `{{cli}} meta describe <table> --json` | `references/command-patterns.md` |
| Find tables by keyword | `{{cli}} meta search <keyword> --json` | `references/command-patterns.md` |
| Latest partition / freshness | `{{cli}} meta latest-partition <table> --json` | `references/command-patterns.md` |
| Sample rows | `{{cli}} data sample <table> --limit 10 --json` | `references/command-patterns.md` |
| SQL kept erroring | (see error envelope's `error.context`) | `references/error-handling.md` |
| Install SKILL for a new platform | `{{cli}} agent skill install <platform> --json` | `references/installation.md` (if exists) |
| Inspect what's installed | `{{cli}} agent skill list --json` | n/a |

```

> Adjust the right-most column to match actual filenames under `references/` — `ls src/maxc_cli/skills/references/` first.

- [ ] **Step 2: Add `> Loaded on demand` banner to each reference**

For each `src/maxc_cli/skills/references/*.md`, insert at line 1 (above any existing heading):

```markdown
> Loaded on demand — covers <one-line topic>. Skip unless the agent is doing <one-line scenario>.

```

You'll write a different `<topic>`/`<scenario>` per file. Don't modify the existing content below.

- [ ] **Step 3: Re-run the existing SKILL-content tests to make sure nothing else broke**

```bash
pytest tests/test_skill_renderer.py -v
```

Expected: all PASS (the renderer only touches template syntax, not narrative content).

- [ ] **Step 4: Commit**

```bash
git add src/maxc_cli/skills/SKILL.md src/maxc_cli/skills/references/
git commit -m "$(cat <<'EOF'
docs(skill): add intent→command quick map + on-demand reference banners

SKILL.md gains a single decision matrix mapping agent intents to the first
command + reference to consult. Each references/*.md gets a one-line banner
explaining when it's worth loading. No existing prose was modified.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Add SKILL ↔ CLI drift detection test (and ensure it passes)

**Files:**
- Create: `tests/test_skill_cli_consistency.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_skill_cli_consistency.py
"""Lint: every CLI verb mentioned in skills/**.md must exist in argparse.

If a SKILL doc references `maxc qeury` (typo) or a renamed verb, this fails
loudly — so we catch SKILL/CLI drift at PR time, not in production.
"""
from __future__ import annotations

import re
from pathlib import Path

from maxc_cli.cli import build_parser

SKILL_DIR = Path(__file__).parent.parent / "src" / "maxc_cli" / "skills"
SHELL_FENCE = re.compile(r"```(?:bash|shell|sh|console)\n(.*?)\n```", re.DOTALL)
CLI_CALL = re.compile(
    r"\b(?:\{\{cli\}\}|maxc|aliyun\s+maxc)\s+"
    r"([a-z][a-z0-9-]*(?:\s+[a-z][a-z0-9-]*)?)"
)

# Each whitelist entry must include a comment explaining why it's NOT a real
# subcommand. Empty by default — populate only with proven false positives.
WHITELIST: 'set[str]' = {
    # Example shape:
    # "query install",  # appears in references/installation.md as prose describing
    #                   # the install flow, not a literal command (re-check before adding).
}


def _collect_subcommand_pairs(parser) -> 'set[str]':
    """Walk argparse subparsers and emit {"group", "group sub"} strings."""
    out: 'set[str]' = set()
    for action in parser._actions:
        if not getattr(action, "choices", None):
            continue
        for name, sub in action.choices.items():
            if name in {"-h", "--help"}:
                continue
            out.add(name)
            # Recurse one level (e.g., "agent skill install" needs "agent skill")
            sub_pairs = _collect_subcommand_pairs(sub)
            for sp in sub_pairs:
                out.add(f"{name} {sp}")
    return out


def test_skill_references_only_real_subcommands():
    parser = build_parser()
    known = _collect_subcommand_pairs(parser)
    referenced: 'set[str]' = set()
    for md in SKILL_DIR.rglob("*.md"):
        text = md.read_text(encoding="utf-8")
        for block in SHELL_FENCE.findall(text):
            for m in CLI_CALL.finditer(block):
                referenced.add(m.group(1).strip())
    leaked = referenced - known - WHITELIST
    assert not leaked, (
        f"SKILL docs reference unknown CLI verbs: {sorted(leaked)}.\n"
        f"Either fix the doc, or add to WHITELIST with a comment if it's a "
        f"genuine prose example (not a runnable command)."
    )


def test_no_residual_install_skill_in_skill_docs():
    """PR #2 deletion safety — SKILL docs must use `agent skill install`,
    not the deprecated `agent install-skill`."""
    for md in SKILL_DIR.rglob("*.md"):
        text = md.read_text(encoding="utf-8")
        assert "agent install-skill" not in text, (
            f"{md} still references the deprecated `agent install-skill` form. "
            f"Replace with `agent skill install`."
        )
```

- [ ] **Step 2: Run the test**

Run: `pytest tests/test_skill_cli_consistency.py -v`
Expected: Both tests PASS (because Task 7 + Task 8 already cleaned the SKILL docs).

If the first test fails, the leak set tells you what needs fixing — either typos in SKILL docs (fix the doc) or genuine prose example (add to `WHITELIST` with a comment).

- [ ] **Step 3: Run the full suite**

Run: `pytest -q`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_skill_cli_consistency.py
git commit -m "$(cat <<'EOF'
test(skill): enforce SKILL docs reference only real CLI verbs

Lints every fenced shell block in skills/**.md against the live argparse
parser. Catches typos, stale verb names, and rename drift at PR time.
A small WHITELIST is provided for prose examples that look like commands
but aren't (empty on landing — populate only with proven false positives).

A companion check forbids the deprecated `agent install-skill` form,
defending the PR #2 deletion against accidental re-introduction.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

> 🟢 **PR #2 done.** After merge: release v0.4.0 with breaking-change note.

---

# PR #3 — P4-A: envelope-failure exit code + `MaxCError.context` plumbing

**Goal:** Fix `cli.py:1610` always-1 bug by reading `exit_code` from the exception's payload. Let `MaxCError.__init__` accept `context=...` and store it on `ErrorPayload`. Migrate `cli.py:_build_error_schema_context` injection to raise with `context=…` instead.

**End-state check:** `pytest tests/test_exit_codes.py` proves each exception class produces its declared exit code; `tests/test_envelope_shape.py` confirms `error.context` is populated for `TableNotFoundError`-style failures.

---

### Task 10: Add `ErrorPayload.exit_code` (internal-only, not serialized) and `MaxCError(context=…)`

**Files:**
- Modify: `src/maxc_cli/exceptions.py`
- Test: `tests/test_exit_codes.py` (new), extend `tests/test_envelope_shape.py`

- [ ] **Step 1: Write failing tests for the new contract**

```python
# tests/test_exit_codes.py
"""Verify exit-code propagation from exception → ErrorPayload → CLI exit."""
import json
import pytest
from maxc_cli.exceptions import (
    MaxCError, PermissionDeniedError, QuotaExceededError,
    SqlError, CostLimitExceededError, NotFoundError,
    ValidationError, TableNotFoundError,
)


@pytest.mark.parametrize(
    "exc_cls, expected_exit",
    [
        (MaxCError, 1),
        (PermissionDeniedError, 2),
        (QuotaExceededError, 3),
        (SqlError, 4),
        (CostLimitExceededError, 5),
        (NotFoundError, 1),
        (ValidationError, 1),
        (TableNotFoundError, 1),
    ],
)
def test_to_payload_carries_exit_code(exc_cls, expected_exit):
    payload = exc_cls("boom").to_payload()
    assert payload.exit_code == expected_exit


def test_to_payload_exit_code_not_serialized():
    """exit_code is internal-only; envelope JSON must not leak it."""
    payload = SqlError("oops").to_payload()
    serialized = payload.to_dict()
    assert "exit_code" not in serialized


def test_max_cerror_accepts_context():
    err = TableNotFoundError("nope", context={"table": "x", "project": "p"})
    payload = err.to_payload()
    assert payload.context == {"table": "x", "project": "p"}
    assert payload.to_dict()["context"] == {"table": "x", "project": "p"}


def test_csv_parse_error_context_still_works():
    """Regression: CsvParseError uses a custom to_payload() — must keep working."""
    from maxc_cli.exceptions import CsvParseError
    err = CsvParseError("bad line", line=5, column="email")
    payload = err.to_payload()
    assert payload.context == {"line": 5, "column": "email"}
    # exit_code defaults to 1 (CsvParseError inherits from ValidationError)
    assert payload.exit_code == 1
```

- [ ] **Step 2: Run failing**

Run: `pytest tests/test_exit_codes.py -v`
Expected: FAIL — `AttributeError: 'ErrorPayload' object has no attribute 'exit_code'` (and TableNotFoundError doesn't accept `context=`).

- [ ] **Step 3: Modify `src/maxc_cli/exceptions.py`**

Update `ErrorPayload` (lines 6-30):

```python
@dataclass
class ErrorPayload:
    code: 'str'
    message: 'str'
    suggestion: 'str | None'
    recoverable: 'bool'
    instance_id: 'str | None' = None
    logview: 'str | None' = None
    context: 'dict[str, Any] | None' = None
    # exit_code is CLI-internal: it tells cli.py:_emit_envelope which exit code to
    # surface for failure envelopes. NOT serialized to the envelope JSON (keeping
    # the schema 2.0 contract intact — agents reading the envelope never see it).
    exit_code: 'int' = 1

    def to_dict(self) -> 'dict[str, Any]':
        payload: 'dict[str, Any]' = {
            "code": self.code,
            "message": self.message,
            "recoverable": self.recoverable,
        }
        if self.suggestion:
            payload["suggestion"] = self.suggestion
        if self.instance_id:
            payload["instance_id"] = self.instance_id
        if self.logview:
            payload["logview"] = self.logview
        if self.context:
            payload["context"] = self.context
        return payload
```

Update `MaxCError.__init__` (lines 38-56) to accept `context`:

```python
class MaxCError(Exception):
    exit_code = 1
    error_code = "EXECUTION_FAILED"
    recoverable = True

    def __init__(
        self,
        message: 'str',
        *,
        suggestion: 'str | None' = None,
        recoverable: 'bool | None' = None,
        instance_id: 'str | None' = None,
        logview: 'str | None' = None,
        context: 'dict[str, Any] | None' = None,
    ) -> 'None':
        super().__init__(message)
        self.message = message
        self.suggestion = suggestion
        self.instance_id = instance_id
        self.logview = logview
        self.context = context
        if recoverable is None:
            self.recoverable = self.__class__.recoverable
        else:
            self.recoverable = recoverable

    def to_payload(self) -> 'ErrorPayload':
        return ErrorPayload(
            code=self.error_code,
            message=self.message,
            suggestion=self.suggestion,
            recoverable=self.recoverable,
            instance_id=self.instance_id,
            logview=self.logview,
            context=self.context,
            exit_code=self.__class__.exit_code,
        )
```

Also update `CsvParseError.to_payload` (lines 158-167) — the parent already sets `exit_code`, so the subclass just needs to keep its line/column merging:

```python
    def to_payload(self) -> 'ErrorPayload':
        payload = super().to_payload()
        context: 'dict[str, Any]' = dict(payload.context or {})
        if self.line is not None:
            context["line"] = self.line
        if self.column is not None:
            context["column"] = self.column
        if context:
            payload.context = context
        return payload
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_exit_codes.py -v`
Expected: All 11 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maxc_cli/exceptions.py tests/test_exit_codes.py
git commit -m "$(cat <<'EOF'
feat(exceptions): plumb exit_code + context through ErrorPayload

- ErrorPayload.exit_code (int, default 1): internal-only field carrying the
  exception class's declared exit_code. Not serialized to the envelope JSON
  (schema 2.0 compatibility preserved).
- MaxCError.__init__ now accepts `context: dict | None`; previously only
  CsvParseError could populate ErrorPayload.context via a custom to_payload().
- CsvParseError.to_payload() updated to merge with any base-level context
  rather than overwrite.

cli.py wiring follows in the next commit.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Wire `cli.py:1610` to read `exit_code` from payload; migrate `_build_error_schema_context` to context-on-raise

**Files:**
- Modify: `src/maxc_cli/cli.py:1605-1640` (`_emit_envelope`)
- Modify: `src/maxc_cli/cli.py:552-635` (`_build_error_schema_context` — convert callers to raise-with-context)

- [ ] **Step 1: Write failing integration test (full envelope path)**

Append to `tests/test_exit_codes.py`:

```python
def test_cli_envelope_failure_uses_class_exit_code(capsys, monkeypatch):
    """The envelope-failure path used to hardcode exit 1. Now it must read
    the originating exception's exit_code."""
    from maxc_cli.cli import _emit_envelope
    from maxc_cli.exceptions import SqlError
    from maxc_cli.models import Envelope
    import argparse
    import io

    args = argparse.Namespace(json=True, format=None)
    payload = SqlError("syntax error near FOO").to_payload()
    env = Envelope(command="query", status="failure", error=payload)
    _emit_envelope(env, args=args, stdout=io.StringIO(), default_format="json")
    assert getattr(args, "_envelope_exit_code", None) == 4


def test_cli_envelope_failure_default_when_no_payload_exit_code(capsys):
    from maxc_cli.cli import _emit_envelope
    from maxc_cli.exceptions import ErrorPayload
    from maxc_cli.models import Envelope
    import argparse, io

    args = argparse.Namespace(json=True, format=None)
    # Hand-built payload with no exit_code override (default 1)
    payload = ErrorPayload(code="X", message="x", suggestion=None, recoverable=False)
    env = Envelope(command="query", status="failure", error=payload)
    _emit_envelope(env, args=args, stdout=io.StringIO(), default_format="json")
    assert getattr(args, "_envelope_exit_code", None) == 1
```

- [ ] **Step 2: Run failing**

Run: `pytest tests/test_exit_codes.py::test_cli_envelope_failure_uses_class_exit_code -v`
Expected: FAIL — value is 1, not 4.

- [ ] **Step 3: Patch `_emit_envelope`**

In `src/maxc_cli/cli.py`, change line 1609-1610:

```python
    if envelope.status == "failure":
        args._envelope_exit_code = 1
```

to:

```python
    if envelope.status == "failure":
        # Pull exit code from the originating exception via ErrorPayload.exit_code.
        # Default 1 when payload is None or hand-built without an override.
        args._envelope_exit_code = getattr(envelope.error, "exit_code", 1) or 1
```

- [ ] **Step 4: Run failing test → PASS**

Run: `pytest tests/test_exit_codes.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Migrate `_build_error_schema_context` callers to raise-with-context**

In `cli.py:552-635`, `_build_error_schema_context` currently returns a dict the caller then attaches to envelope metadata. Find every caller:

```bash
grep -n "_build_error_schema_context\b" src/maxc_cli/cli.py
```

For each call site (the exception-handling block around lines 840-860 — look for `_build_error_schema_context`):

**Before:**
```python
schema_context = _build_error_schema_context(app, exc, sql=...)
if _is_json_mode(args):
    data = {}
    if schema_context:
        data["schema_context"] = schema_context
    payload = Envelope(
        ...
        data=data,
        error=exc.to_payload(),
        ...
    )
```

**After:**
```python
schema_context = _build_error_schema_context(app, exc, sql=...)
if schema_context and exc.context is None:
    # Promote schema_context into the exception so it lands on error.context
    # in the envelope (where agents look for self-correction data).
    exc.context = schema_context
if _is_json_mode(args):
    payload = Envelope(
        ...
        data={},
        error=exc.to_payload(),
        ...
    )
```

> Don't change the *shape* of `schema_context` — `_build_error_schema_context` still returns the same dict. Only the destination moves from `envelope.data.schema_context` to `envelope.error.context`.

- [ ] **Step 6: Update existing tests that read `envelope.data.schema_context`**

```bash
grep -rn "schema_context" tests/
```

Each test that asserts `envelope["data"]["schema_context"]` should be rewritten to `envelope["error"]["context"]`. Likely candidate: `tests/test_error_self_correction.py`.

- [ ] **Step 7: Run impacted tests**

Run: `pytest tests/test_error_self_correction.py tests/test_envelope_shape.py tests/test_exit_codes.py -v`
Expected: All PASS.

- [ ] **Step 8: Run full suite**

Run: `pytest -q`
Expected: All PASS.

- [ ] **Step 9: Commit**

```bash
git add src/maxc_cli/cli.py tests/
git commit -m "$(cat <<'EOF'
fix(cli): envelope-failure exit code now reflects originating exception

- _emit_envelope reads ErrorPayload.exit_code instead of hardcoding 1.
  Exception subclasses that declare exit_code = 2/3/4/5 (PermissionDenied,
  QuotaExceeded, SqlError, CostLimitExceeded) now surface those codes when
  the failure flows through the envelope path, not just the raise-and-catch
  path at cli.py:872.
- _build_error_schema_context output migrated from envelope.data.schema_context
  to envelope.error.context — single canonical place for structured failure
  context, consumed by agents for self-correction.
- Affected tests (test_error_self_correction, test_envelope_shape) updated
  to read the new location.

Envelope schema stays at version 2.0 — error.context already existed in the
schema since CsvParseError; this commit just makes more callers use it.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

> 🟢 **PR #3 done.**

---

# PR #4 — P4-B: Global flag hoist

**Goal:** Make `maxc query "..." --json` work identically to `maxc --json query "..."`.

---

### Task 12: Implement `_hoist_global_flags` and wire it into `run()`

**Files:**
- Modify: `src/maxc_cli/cli.py` (add helper near top; call it in `run()` at ~line 689)
- Test: `tests/test_flag_hoist.py` (new)

- [ ] **Step 1: Write failing test matrix**

```python
# tests/test_flag_hoist.py
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
        # Help / version flags
        (["query", "x", "--help"], ["--help", "query", "x"]),
        (["--version"], ["--version"]),
    ],
)
def test_hoist_matrix(raw, expected):
    assert _hoist_global_flags(raw) == expected


def test_hoist_handles_unknown_global_flag_as_passthrough():
    """Unknown flags must be left alone — they're either subcommand flags
    (e.g., --project on `query`) or genuine typos (argparse will reject)."""
    assert _hoist_global_flags(["query", "x", "--project", "p"]) == ["query", "x", "--project", "p"]
```

- [ ] **Step 2: Run failing**

Run: `pytest tests/test_flag_hoist.py -v`
Expected: FAIL — `ImportError: cannot import name '_hoist_global_flags'`.

- [ ] **Step 3: Implement `_hoist_global_flags`**

In `src/maxc_cli/cli.py`, near the other module-level helpers (after `_epilog_for` at line 78 is a good spot), add:

```python
# Global flags that must work in any argv position. Subcommand-local flags
# (e.g., --project, --limit) are NOT hoisted — they belong to specific subparsers.
# arity = number of subsequent argv tokens consumed as values when given as
# `--flag value` (separate); `--flag=value` is always consumed as a single token.
_GLOBAL_FLAG_ARITY: 'dict[str, int]' = {
    "--format":  1,
    "-f":        1,
    "--config":  1,
    "--json":    0,
    "--quiet":   0,
    "-q":        0,
    "--debug":   0,
    "-v":        0,
    "--version": 0,
    "--help":    0,
    "-h":        0,
}


def _hoist_global_flags(argv: 'list[str]') -> 'list[str]':
    """Move any global flag found after the subcommand to the front of argv.

    Lets agents write `maxc query "..." --json` interchangeably with
    `maxc --json query "..."`. Stops at the POSIX `--` terminator. Unknown
    flags are passed through untouched (they belong to subparsers).
    """
    hoisted: 'list[str]' = []
    rest: 'list[str]' = []
    i = 0
    n = len(argv)
    while i < n:
        token = argv[i]
        if token == "--":
            rest.extend(argv[i:])
            break
        # `--flag=value` form
        if "=" in token and token.startswith("--"):
            flag = token.split("=", 1)[0]
            if flag in _GLOBAL_FLAG_ARITY:
                hoisted.append(token)
                i += 1
                continue
        # bare flag (with potential value as next token)
        if token in _GLOBAL_FLAG_ARITY:
            arity = _GLOBAL_FLAG_ARITY[token]
            hoisted.append(token)
            if arity == 1 and i + 1 < n:
                hoisted.append(argv[i + 1])
                i += 2
                continue
            i += 1
            continue
        rest.append(token)
        i += 1
    return hoisted + rest
```

- [ ] **Step 4: Wire it into `run()`**

In `cli.py:run()` (~line 689), change:

```python
    argv_list = list(argv) if argv is not None else list(sys.argv[1:])
    args = parser.parse_args(argv_list)
```

to:

```python
    argv_list = list(argv) if argv is not None else list(sys.argv[1:])
    argv_list = _hoist_global_flags(argv_list)
    args = parser.parse_args(argv_list)
```

- [ ] **Step 5: Run unit tests + a smoke integration**

```bash
pytest tests/test_flag_hoist.py -v
```

Expected: All PASS.

```bash
# Smoke: same envelope output for both forms
python -m maxc_cli query "select 1" --json 2>/dev/null | python -m json.tool > /tmp/a.json
python -m maxc_cli --json query "select 1"             2>/dev/null | python -m json.tool > /tmp/b.json
diff /tmp/a.json /tmp/b.json  # may differ in elapsed_ms etc.; status/command should match
```

(Don't fail the task on whitespace/elapsed_ms differences — just eyeball it.)

- [ ] **Step 6: Run full suite**

Run: `pytest -q`
Expected: All PASS.

- [ ] **Step 7: Commit**

```bash
git add src/maxc_cli/cli.py tests/test_flag_hoist.py
git commit -m "$(cat <<'EOF'
feat(cli): hoist global flags from any argv position to the front

Agents commonly write `maxc query "..." --json` (flag after the subcommand);
argparse rejects that unless the flag is registered on the subparser. The new
_hoist_global_flags() rewrites argv before parse_args so --format/-f, --config
(arity=1) and --json/--quiet/-q/--debug/-v/--version/--help (arity=0) all work
in any position. Stops at the POSIX `--` terminator. Unknown flags are
passed through untouched — subparser-local flags (--project, --limit, etc.)
keep their existing behavior.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

> 🟢 **PR #4 done.**

---

# PR #5 — P4-C: CI slim-down + repo housekeeping

**Goal:** Remove `.gitlab-ci.yml`, update `.gitignore`, delete the one-off scratch file.

---

### Task 13: Confirm GitLab CI has no live consumer, then delete it + housekeeping

**Files:**
- Delete: `.gitlab-ci.yml`
- Delete: `2026-04-29-111854-cli-golang-aliyun-cliu.txt`
- Modify: `.gitignore`

- [ ] **Step 1: Sanity-check there's no GitLab mirror reading this file**

```bash
grep -rni "gitlab" --exclude-dir=.git --exclude-dir=.venv --exclude-dir=__pycache__
```

Review the hits. If `README.md` / `docs/` refer to GitLab as a mirror that runs `.gitlab-ci.yml`, **stop and surface to the user**. (Per spec §6, fall back to keeping the file if there's a live consumer.)

- [ ] **Step 2: Delete `.gitlab-ci.yml`**

```bash
git rm .gitlab-ci.yml
```

- [ ] **Step 3: Delete the scratch text file**

```bash
git rm 2026-04-29-111854-cli-golang-aliyun-cliu.txt
```

- [ ] **Step 4: Update `.gitignore`**

Append (don't reorder existing rules — keep diff small):

```
nohup.out
*.tar.gz
use-maxc-cli.zip
```

> Don't add `dist/`, `build/`, `.qoder/`, `.idea/` — they're already in the file.

- [ ] **Step 5: Verify**

```bash
git status -s
# expect:
#  D .gitlab-ci.yml
#  D 2026-04-29-111854-cli-golang-aliyun-cliu.txt
#  M .gitignore
```

- [ ] **Step 6: Run tests (sanity)**

Run: `pytest -q`
Expected: All PASS (none of these files were referenced by code).

- [ ] **Step 7: Commit**

```bash
git add .gitignore .gitlab-ci.yml 2026-04-29-111854-cli-golang-aliyun-cliu.txt
git commit -m "$(cat <<'EOF'
chore: drop GitLab CI, scratch file, extend .gitignore

- Remove .gitlab-ci.yml: no live GitLab mirror consumes this anymore;
  Aone CI is the daily quality gate, GitHub Actions handles releases.
- Remove 2026-04-29-111854-cli-golang-aliyun-cliu.txt: one-off scratch
  draft from April that should never have been committed.
- Append nohup.out / *.tar.gz / use-maxc-cli.zip to .gitignore so they
  don't get re-introduced.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

> 🟢 **PR #5 done.**

---

# PR #6 — P4-D-fix: `ruff --fix` sweep + coverage padding (no CI gate yet)

**Goal:** Get the codebase compatible with `ruff check src/ tests/` and `pytest --cov-fail-under=80`. **No CI changes** in this PR — that's PR #7.

---

### Task 14: Add ruff config + autofix sweep

**Files:**
- Modify: `pyproject.toml`
- Modify: many files across `src/` and `tests/` (whatever ruff autofix touches)

- [ ] **Step 1: Add ruff to dev deps + config**

In `pyproject.toml`, add:

```toml
[tool.ruff]
target-version = "py38"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "UP"]   # pycodestyle errors, pyflakes, isort, pyupgrade
ignore = []
```

If `pyproject.toml` already has a `[tool]` table tree, slot these in alongside other tool configs.

- [ ] **Step 2: Install ruff and run autofix**

```bash
pip install ruff
ruff check --fix src/ tests/
```

Review the diff:

```bash
git diff --stat
```

- [ ] **Step 3: Manually fix the residual (non-autofixable) issues**

```bash
ruff check src/ tests/
```

For each remaining hit, fix by hand. **Resist the urge to refactor anything** — only do the minimum to make the rule pass.

- [ ] **Step 4: Run full test suite**

Run: `pytest -q`
Expected: All PASS — ruff autofix should not change behavior, only style.

- [ ] **Step 5: Commit ruff config + autofix**

```bash
git add pyproject.toml src/ tests/
git commit -m "$(cat <<'EOF'
chore: apply ruff check --fix across src/ and tests/

Enables [tool.ruff.lint] with select = [E, F, I, UP] — conservative rule set
covering pycodestyle errors, pyflakes, import order, and pyupgrade.
The autofix sweep landed pure stylistic changes (imports reorder, % → f-string,
trailing whitespace). No behavioral change; pytest -q remains green.

CI gate-on follows in a separate PR after coverage is padded above 80%.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 15: Measure coverage; pad low-coverage modules to ≥ 80%

**Files:**
- Possibly create: `tests/test_cache_coverage.py`, `tests/test_store_coverage.py`, `tests/test_audit_coverage.py` (per measurement)

- [ ] **Step 1: Install pytest-cov and measure**

```bash
pip install pytest-cov
pytest --cov=src/maxc_cli --cov-report=term-missing -q | tee /tmp/cov.txt
```

Read the bottom of the report. If TOTAL is ≥ 80%, skip to Step 4.

If TOTAL < 80%, the report lists per-file coverage. Spec §3.5 predicts low coverage in `cache.py`, `store.py`, `audit.py` — confirm or adjust.

- [ ] **Step 2: Add targeted unit tests for the lowest-coverage modules**

For each module below the cutoff (typically anything under 70%):

- Read the module to find the uncovered branches (the term-missing report shows the line numbers).
- Add unit tests that exercise the uncovered code paths.
- Keep tests minimal — don't try to hit every line, just the ones that drag TOTAL below 80%.

Avoid testing private helpers when you can test through the public API instead.

- [ ] **Step 3: Re-measure**

```bash
pytest --cov=src/maxc_cli --cov-report=term -q
```

Repeat Step 2 until TOTAL ≥ 80%.

- [ ] **Step 4: Commit the test additions**

```bash
git add tests/
git commit -m "$(cat <<'EOF'
test: pad unit-test coverage to ≥ 80%

Adds targeted unit tests for modules previously below the cutoff (cache,
store, audit — see PR description for per-module numbers before/after).
Preparing for the CI gate-on in the next PR; no behavioral change.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

> 🟢 **PR #6 done.**

---

# PR #7 — P4-D-gate: enable ruff + coverage in Aone CI

**Goal:** Add the lint/coverage steps to `.aoneci/cli-publish.yaml` so future PRs can't regress what PR #6 established. **Tiny PR** — one config file.

---

### Task 16: Enable the lint + coverage gate in Aone

**Files:**
- Modify: `.aoneci/cli-publish.yaml`

- [ ] **Step 1: Inspect current Aone config**

```bash
cat .aoneci/cli-publish.yaml
```

- [ ] **Step 2: Add ruff + cov steps**

Find the steps block (likely under `jobs.build-and-upload.steps`). Add **before** the existing pytest step (so failures gate early):

```yaml
      - id: 'install-quality-tools'
        run: |
          pip install ruff pytest-cov

      - id: 'ruff'
        run: |
          ruff check src/ tests/

      - id: 'pytest-coverage'
        run: |
          pytest --cov=src/maxc_cli --cov-fail-under=80 -q
```

If a `pytest` step already exists, **replace** it with the `pytest-coverage` step above (don't run pytest twice).

- [ ] **Step 3: Locally verify the commands work**

```bash
ruff check src/ tests/   # should be clean (PR #6 made it so)
pytest --cov=src/maxc_cli --cov-fail-under=80 -q   # should pass at ≥ 80%
```

Expected: both exit 0.

- [ ] **Step 4: Commit + push and watch CI**

```bash
git add .aoneci/cli-publish.yaml
git commit -m "$(cat <<'EOF'
ci(aone): gate on ruff + pytest --cov-fail-under=80

Future PRs that introduce lint violations or drop coverage below 80% will
fail the daily Aone pipeline. Both PR #6's autofix sweep and coverage
padding land on main first, so this PR should be green on day 1.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"

git push
# Watch the next Aone run before merging.
```

> 🟢 **PR #7 done.** Release **v0.4.1** after PR #3–#7 land.

---

## Final acceptance summary

After all seven PRs merge:

| Spec section | Check |
|---|---|
| §2.1 | `maxc agent skill {install,update,uninstall,list,diff,path}` work; `maxc agent install-skill` is gone (argparse error) |
| §2.2 | `tests/test_agent_platforms.py` green; new platform = 1-line REGISTRY entry |
| §2.3 | `tests/test_skill_cli_consistency.py` green; SKILL typo intentionally breaks it (verify once) |
| §2.4 | SKILL.md has the intent matrix; every reference has the `> Loaded on demand` banner |
| §3.1 | `tests/test_flag_hoist.py` green; `maxc query x --json` ≡ `maxc --json query x` |
| §3.2 | `tests/test_exit_codes.py` green; envelope-failure path returns class-declared exit code |
| §3.3 | `envelope.error.context` populated for `_build_error_schema_context` callers |
| §3.4 | `.gitlab-ci.yml` deleted |
| §3.5 | Aone CI runs `ruff check` + `pytest --cov-fail-under=80` |
| §3.6 | `git status -s` is clean on fresh checkout |

Run the final full-suite sanity check:

```bash
pytest -q
ruff check src/ tests/
pytest --cov=src/maxc_cli --cov-fail-under=80 -q
```

All three must be green.
