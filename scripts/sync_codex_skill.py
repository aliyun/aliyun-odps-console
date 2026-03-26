#!/usr/bin/env python3
"""Sync the repo-tracked Codex skill into a Codex home."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def _default_source() -> Path:
    return Path(__file__).resolve().parent.parent / "skills" / "use-maxc-cli"


def _default_target() -> Path:
    return Path.home() / ".codex" / "skills" / "use-maxc-cli"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync the repo-tracked use-maxc-cli skill into Codex.")
    parser.add_argument(
        "--source",
        default=str(_default_source()),
        help="Path to the source skill directory. Defaults to the repo-tracked skill.",
    )
    parser.add_argument(
        "--target",
        default=str(_default_target()),
        help="Path to the target Codex skill directory. Defaults to ~/.codex/skills/use-maxc-cli.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved paths without copying files.",
    )
    return parser.parse_args()


def _validate_paths(source: Path, target: Path) -> None:
    if not (source / "SKILL.md").exists():
        raise SystemExit(f"Source skill is missing SKILL.md: {source}")
    if source.name != "use-maxc-cli":
        raise SystemExit(f"Source directory must be named `use-maxc-cli`: {source}")
    if target.name != "use-maxc-cli":
        raise SystemExit(f"Target directory must end with `use-maxc-cli`: {target}")


def main() -> int:
    args = _parse_args()
    source = Path(args.source).expanduser().resolve()
    target = Path(args.target).expanduser().resolve()
    _validate_paths(source, target)

    if args.dry_run:
        print(f"Source: {source}")
        print(f"Target: {target}")
        return 0

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target)
    print(f"Synced {source} -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
