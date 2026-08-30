"""Tests for explicit local credential removal."""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from pathlib import Path

import pytest
import yaml

from maxc_cli.app import MaxCApp
from maxc_cli.auth_continuation import (
    load_auth_continuation,
    save_auth_continuation,
)
from maxc_cli.cli import run
from maxc_cli.exceptions import ValidationError


def _write_config(path: Path) -> None:
    path.write_text(
        "default_project: keep_project\n"
        "default_region: cn-hangzhou\n"
        "auth:\n"
        "  provider: oauth\n"
        "  project: keep_project\n"
        "  endpoint: https://service.example.test/api\n"
        "  access_id: temporary-id\n"
        "  secret_access_key: temporary-secret\n"
        "  oauth:\n"
        "    site_type: CN\n"
        "    refresh_token: refresh-token\n"
        "cache_dir: ./cache\n"
        "state_dir: ./state\n",
        encoding="utf-8",
    )


def test_logout_removes_auth_and_external_credential_cache_only(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    app.cache.set_kv("ext_creds:one", "secret payload")
    app.cache.set_kv("tenant_id:keep_project", "tenant")

    envelope = app.auth_logout(target_config_path=config_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert envelope.status == "success"
    assert envelope.data["config_auth_removed"] is True
    assert envelope.data["cached_credentials_removed"] == 1
    assert "auth" not in payload
    assert payload["default_project"] == "keep_project"
    assert payload["default_region"] == "cn-hangzhou"
    assert app.cache.get_kv("ext_creds:one") is None
    assert app.cache.get_kv("tenant_id:keep_project") == "tenant"
    if os.name == "posix":
        assert config_path.stat().st_mode & 0o777 == 0o600


def test_logout_reports_parent_environment_without_exposing_values(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "must-not-leak")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "also-secret")
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)

    serialized = json.dumps(
        app.auth_logout(target_config_path=config_path).to_dict()
    )

    assert "ALIBABA_CLOUD_ACCESS_KEY_ID" in serialized
    assert "ALIBABA_CLOUD_ACCESS_KEY_SECRET" in serialized
    assert "must-not-leak" not in serialized
    assert "also-secret" not in serialized


def test_logout_cli_is_local_and_returns_a_single_envelope(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    stdout = StringIO()
    stderr = StringIO()

    code = run(
        ["--config", str(config_path), "auth", "logout", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )
    payload = json.loads(stdout.getvalue())

    assert code == 0
    assert stderr.getvalue() == ""
    assert payload["command"] == "auth logout"
    assert payload["status"] == "success"
    assert "auth" not in yaml.safe_load(config_path.read_text(encoding="utf-8"))


def test_logout_clears_only_auth_continuations_bound_to_target_config(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    other_config_path = tmp_path / "other.yaml"
    _write_config(config_path)
    _write_config(other_config_path)
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    target_access_key, _ = save_auth_continuation(
        app.config.state_dir,
        kind="access_key",
        target_config_path=config_path,
        secret_payload={"access_id": "private"},
    )
    target_oauth, _ = save_auth_continuation(
        app.config.state_dir,
        kind="oauth",
        target_config_path=config_path,
        secret_payload={"access_id": "private"},
    )
    other_access_key, _ = save_auth_continuation(
        app.config.state_dir,
        kind="access_key",
        target_config_path=other_config_path,
        secret_payload={"access_id": "keep"},
    )

    envelope = app.auth_logout(target_config_path=config_path)

    assert envelope.data["auth_continuations_removed"] == 2
    assert envelope.data["auth_continuation_cleanup_failures"] == 0
    for continuation_id, kind in (
        (target_access_key, "access_key"),
        (target_oauth, "oauth"),
    ):
        with pytest.raises(ValidationError, match="not found"):
            load_auth_continuation(
                app.config.state_dir,
                continuation_id,
                kind=kind,
                target_config_path=config_path,
            )
    assert load_auth_continuation(
        app.config.state_dir,
        other_access_key,
        kind="access_key",
        target_config_path=other_config_path,
    ) == {"access_id": "keep"}


def test_auth_continuation_can_only_be_claimed_once_concurrently(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    config_path = tmp_path / "config.yaml"
    continuation_id, _ = save_auth_continuation(
        state_dir,
        kind="access_key",
        target_config_path=config_path,
        secret_payload={"access_id": "private"},
    )

    def consume():
        try:
            return load_auth_continuation(
                state_dir,
                continuation_id,
                kind="access_key",
                target_config_path=config_path,
            )
        except ValidationError as exc:
            return exc.error_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _index: consume(), range(2)))

    assert outcomes.count({"access_id": "private"}) == 1
    assert outcomes.count("VALIDATION_ERROR") == 1
