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
from dataclasses import dataclass
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
    test_render_claude_plugin_writes_declared_path.
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


# Invocation map: cli front-end name → (cli, cli_module) substituted into
# SKILL templates. Migrated here from app.py:3363-3372 so the registry is the
# single source of truth.
INVOCATIONS: dict[str, dict[str, str]] = {
    "maxc": {
        "cli": "maxc",
        # `python3 -m` (not `python`) matches the legacy _SKILL_INVOCATIONS at
        # app.py:3363-3372 byte-for-byte — changing this re-renders SKILL.md for
        # every existing install and triggers a phantom `agent skill diff`.
        "cli_module": "python3 -m maxc_cli",
    },
    "aliyun-maxc": {
        "cli": "aliyun maxc",
        "cli_module": "aliyun maxc",
    },
}


def _claude_root() -> Path:
    return Path.home() / ".claude" / "skills" / "maxc-cli"


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


def _build_registry() -> tuple[Platform, ...]:
    """Build REGISTRY lazily so tests can monkeypatch HOME/CODEX_HOME + reload."""
    return (
        Platform(
            name="claude-code",
            install_root=_claude_root(),
            skill_subpath=None,
            extra_files=(),
            next_step_hint="Restart Claude Code or run /reload-plugins to activate",
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


def get_render_fn(name: str) -> RenderFn:
    """Resolve a render_fn name (declared in ExtraFile) into the actual callable.

    Kept here so caller can `getattr(agent_platforms, name)` without exposing the
    module-internal naming convention to MaxCApp.
    """
    fn = globals().get(name)
    if not callable(fn):
        raise KeyError(f"render_fn not found in agent_platforms: {name!r}")
    return fn  # type: ignore[return-value]
