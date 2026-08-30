"""Durability and confidentiality checks for persisted CLI configuration."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from maxc_cli.config import (
    load_config_mapping,
    migrate_legacy_session_override,
    save_config_mapping,
)
from maxc_cli.exceptions import ValidationError


def test_save_config_round_trips_and_is_private(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "config.yaml"

    save_config_mapping(target, {"auth": {"provider": "oauth"}, "name": "测试"})

    assert load_config_mapping(target) == {
        "auth": {"provider": "oauth"},
        "name": "测试",
    }
    if os.name == "posix":
        assert target.stat().st_mode & 0o777 == 0o600


def test_failed_replace_preserves_previous_config(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "config.yaml"
    save_config_mapping(target, {"version": "old"})

    def fail_replace(source, destination, **_kwargs):
        raise OSError("simulated replace failure")

    monkeypatch.setattr("maxc_cli.config.os.replace", fail_replace)

    with pytest.raises(OSError, match="simulated replace failure"):
        save_config_mapping(target, {"version": "new"})

    assert load_config_mapping(target) == {"version": "old"}
    assert list(tmp_path.glob(".config.yaml.*.tmp")) == []


def test_save_config_fsyncs_before_replace(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "config.yaml"
    calls: list[int] = []
    real_fsync = os.fsync

    def record_fsync(fd: int) -> None:
        calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr("maxc_cli.config.os.fsync", record_fsync)

    save_config_mapping(target, {"ready": True})

    assert calls
    assert load_config_mapping(target) == {"ready": True}


@pytest.mark.parametrize(
    "legacy_payload",
    [
        "project: [unterminated\n",
        "- project\n- schema\n",
    ],
)
def test_invalid_legacy_session_migration_preserves_only_source_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    legacy_payload: str,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    state_dir = tmp_path / ".maxc"
    state_dir.mkdir(mode=0o700)
    source = state_dir / "session_override.yaml"
    source.write_text(legacy_payload, encoding="utf-8")

    with pytest.raises(ValidationError, match="Legacy session override"):
        migrate_legacy_session_override()

    assert source.read_text(encoding="utf-8") == legacy_payload
    assert not (state_dir / ".session_override_migrated").exists()
    assert not (state_dir / "config.yaml").exists()


def test_successful_legacy_session_migration_is_durable_before_source_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    state_dir = tmp_path / ".maxc"
    state_dir.mkdir(mode=0o700)
    source = state_dir / "session_override.yaml"
    source.write_text("project: legacy_project\nschema: legacy_schema\n", encoding="utf-8")

    migrate_legacy_session_override()

    assert not source.exists()
    assert load_config_mapping(state_dir / "config.yaml") == {
        "default_project": "legacy_project",
        "default_schema": "legacy_schema",
    }
    marker = state_dir / ".session_override_migrated"
    assert marker.read_text(encoding="utf-8") == "migrated\n"
    if os.name == "posix":
        assert marker.stat().st_mode & 0o777 == 0o600
