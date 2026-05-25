"""Tests for ExternalCredentialProvider, build_external_account, and
infer_auth_provider external branch."""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.auth_providers import (
    ExternalCredentialProvider,
    SimpleTempCredential,
    build_external_account,
    infer_auth_provider,
    resolve_auth_connection,
)
from maxc_cli.cache import LocalCache
from maxc_cli.config import AuthConfig, ExternalAuthConfig, MaxCConfig
from maxc_cli.exceptions import ValidationError

# ============================================================
# Helpers
# ============================================================

def _make_cache(tmp_path: Path) -> LocalCache:
    """Create a LocalCache backed by a temp directory."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return LocalCache(cache_dir)


def _minimal_config(
    *,
    provider: Optional[str] = None,
    ext_command: Optional[str] = None,
    project: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> MaxCConfig:
    """Create a MaxCConfig with minimal required fields for auth testing."""
    from pathlib import Path as _P

    from maxc_cli.config import AgentConfig

    return MaxCConfig(
        default_project=project or "test_proj",
        default_schema=None,
        default_format="json",
        default_region="cn-shanghai",
        project_context="testing",
        allowed_operations=["SELECT"],
        cost_threshold_cu=100,
        sensitive_columns=[],
        masking_enabled=True,
        agent=AgentConfig(),
        auth=AuthConfig(
            provider=provider,
            project=project,
            endpoint=endpoint,
            external=ExternalAuthConfig(process_command=ext_command),
        ),
        state_dir=_P("/tmp/maxc_test_state"),
        cache_dir=_P("/tmp/maxc_test_cache"),
        catalog={},
        sources=[],
    )


def _cred_json(
    *,
    access_key_id: str = "LTAI_TEST",
    access_key_secret: str = "secret_test",
    security_token: Optional[str] = None,
    expiration: Optional[str] = None,
) -> str:
    """Build credential JSON string for echo commands."""
    payload: dict = {
        "AccessKeyId": access_key_id,
        "AccessKeySecret": access_key_secret,
    }
    if security_token is not None:
        payload["SecurityToken"] = security_token
    if expiration is not None:
        payload["Expiration"] = expiration
    return json.dumps(payload)


def _echo_cmd(json_str: str) -> str:
    """Shell command that echoes the given JSON to stdout."""
    # Use single quotes; escape any single quotes inside (unlikely for test data)
    return f"echo '{json_str}'"


def _far_future() -> str:
    return "2099-01-01T00:00:00Z"


def _past() -> str:
    return "2000-01-01T00:00:00Z"


def _soon(seconds: int = 30) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


# ============================================================
# ExternalCredentialProvider — get_credential
# ============================================================

class TestExternalCredentialProviderGetCredential:

    def test_basic_json_output(self, tmp_path):
        """Provider parses JSON output with AccessKeyId/AccessKeySecret."""
        cmd = _echo_cmd(_cred_json())
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        cred = p.get_credential()
        assert cred.access_key_id == "LTAI_TEST"
        assert cred.access_key_secret == "secret_test"
        assert cred.security_token is None
        assert cred.expires_at is None

    def test_json_with_security_token_and_expiration(self, tmp_path):
        """Provider parses optional SecurityToken and Expiration."""
        cmd = _echo_cmd(_cred_json(
            security_token="CAIS_TEST",
            expiration=_far_future(),
        ))
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        cred = p.get_credential()
        assert cred.security_token == "CAIS_TEST"
        assert cred.expires_at is not None
        assert cred.expires_at.year == 2099

    def test_key_value_output_format(self, tmp_path):
        """Provider parses key=value output (e.g. AccessKeyId=LTAI_KV)."""
        cmd = "echo 'AccessKeyId=LTAI_KV\nAccessKeySecret=secret_kv'"
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        cred = p.get_credential()
        assert cred.access_key_id == "LTAI_KV"
        assert cred.access_key_secret == "secret_kv"

    def test_command_timeout(self, tmp_path):
        """Provider raises ValidationError when command times out."""
        # sleep 10s with a 1s timeout
        cmd = "sleep 10"
        p = ExternalCredentialProvider(command=cmd, timeout=1)
        with pytest.raises(ValidationError, match="timed out"):
            p.get_credential()

    def test_command_nonzero_exit(self, tmp_path):
        """Provider raises ValidationError when command exits non-zero."""
        cmd = "exit 42"
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        with pytest.raises(ValidationError, match="exited with code 42"):
            p.get_credential()

    def test_command_missing_access_key_id(self, tmp_path):
        """Provider raises ValidationError when output lacks AccessKeyId."""
        cmd = _echo_cmd('{"AccessKeySecret": "secret_only"}')
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        with pytest.raises(ValidationError):
            p.get_credential()


# ============================================================
# ExternalCredentialProvider — L1 in-process cache
# ============================================================

class TestExternalCredentialProviderL1Cache:

    def test_l1_cache_hit_within_same_instance(self, tmp_path):
        """Second call within same provider returns cached credential."""
        cmd = _echo_cmd(_cred_json(expiration=_far_future()))

        p = ExternalCredentialProvider(command=cmd, timeout=10)
        c1 = p.get_credential()
        c2 = p.get_credential()
        assert c1.access_key_id == c2.access_key_id

    def test_l1_cache_expired_triggers_rerun(self, tmp_path):
        """When L1 cached credential is near expiry, provider re-runs command."""
        # Manually set cached credential that is already past the buffer
        cmd = _echo_cmd(_cred_json(
            access_key_id="FRESH_KEY",
            expiration=_far_future(),
        ))
        p = ExternalCredentialProvider(command=cmd, timeout=10)

        # Inject an expired L1 credential
        expired_time = datetime.now(timezone.utc) - timedelta(seconds=120)
        p._cached = SimpleTempCredential(
            access_key_id="STALE_KEY",
            access_key_secret="stale_secret",
            security_token="stale_token",
            expires_at=expired_time,
        )
        cred = p.get_credential()
        assert cred.access_key_id == "FRESH_KEY"

    def test_no_expiration_always_considered_expired(self, tmp_path):
        """Without Expiration, L1 cache is always stale → command runs every time."""
        cmd = _echo_cmd(_cred_json(access_key_id="NO_EXP"))
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        p.get_credential()
        # Overwrite L1 manually to prove it's not reused
        p._cached = SimpleTempCredential(
            access_key_id="SHOULD_NOT_SEE",
            access_key_secret="x",
            security_token=None,
            expires_at=None,
        )
        c2 = p.get_credential()
        assert c2.access_key_id == "NO_EXP"


# ============================================================
# ExternalCredentialProvider — L2 kv_store cache
# ============================================================

class TestExternalCredentialProviderL2KVStore:

    def test_kv_store_hit_skips_command(self, tmp_path):
        """New provider instance reads from kv_store without executing command."""
        cache = _make_cache(tmp_path)
        cmd = _echo_cmd(_cred_json(
            access_key_id="KV_KEY",
            expiration=_far_future(),
        ))

        # First instance: executes command, writes kv_store
        p1 = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)
        c1 = p1.get_credential()
        assert c1.access_key_id == "KV_KEY"

        # Second instance: should hit kv_store, not run command
        p2 = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)
        c2 = p2.get_credential()
        assert c2.access_key_id == "KV_KEY"

    def test_no_expiration_not_cached_to_kv_store(self, tmp_path):
        """Credentials without Expiration are NOT persisted to kv_store."""
        cache = _make_cache(tmp_path)
        cmd = _echo_cmd(_cred_json(access_key_id="NO_EXP_KV"))
        p = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)
        p.get_credential()

        raw = cache.get_kv(p._kv_key)
        assert raw is None

    def test_expired_kv_store_entry_ignored(self, tmp_path):
        """Expired kv_store entry is ignored; command is re-run."""
        cache = _make_cache(tmp_path)
        cmd = _echo_cmd(_cred_json(
            access_key_id="REFRESHED_KEY",
            expiration=_far_future(),
        ))
        p = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)

        # Inject an expired entry directly into kv_store
        expired_payload = json.dumps({
            "access_key_id": "STALE_KV_KEY",
            "access_key_secret": "stale_secret",
            "security_token": "stale_token",
            "expires_at": _past(),
        })
        cache.set_kv(p._kv_key, expired_payload)

        cred = p.get_credential()
        assert cred.access_key_id == "REFRESHED_KEY"

    def test_nearly_expired_kv_store_entry_ignored(self, tmp_path):
        """kv_store entry within 60s buffer is treated as expired."""
        cache = _make_cache(tmp_path)
        cmd = _echo_cmd(_cred_json(
            access_key_id="ALMOST_EXPIRED_REFRESH",
            expiration=_far_future(),
        ))
        p = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)

        # Inject an entry that expires in 30s (within 60s buffer)
        soon_payload = json.dumps({
            "access_key_id": "ALMOST_EXPIRED_KEY",
            "access_key_secret": "almost_secret",
            "security_token": "almost_token",
            "expires_at": _soon(seconds=30),
        })
        cache.set_kv(p._kv_key, soon_payload)

        cred = p.get_credential()
        assert cred.access_key_id == "ALMOST_EXPIRED_REFRESH"

    def test_fresh_kv_store_entry_returned(self, tmp_path):
        """kv_store entry far from expiry is returned without running command."""
        cache = _make_cache(tmp_path)
        cmd = _echo_cmd(_cred_json(access_key_id="SHOULD_NOT_RUN"))
        p = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)

        # Inject a fresh entry (expires in 2099)
        fresh_payload = json.dumps({
            "access_key_id": "KV_FRESH",
            "access_key_secret": "kv_fresh_secret",
            "security_token": "kv_fresh_token",
            "expires_at": _far_future(),
        })
        cache.set_kv(p._kv_key, fresh_payload)

        cred = p.get_credential()
        assert cred.access_key_id == "KV_FRESH"

    def test_corrupt_kv_store_entry_ignored(self, tmp_path):
        """Malformed kv_store data is silently ignored; command runs instead."""
        cache = _make_cache(tmp_path)
        cmd = _echo_cmd(_cred_json(
            access_key_id="FALLBACK_KEY",
            expiration=_far_future(),
        ))
        p = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)

        # Inject garbage
        cache.set_kv(p._kv_key, "not valid json {{{")

        cred = p.get_credential()
        assert cred.access_key_id == "FALLBACK_KEY"

    def test_kv_store_write_failure_nonfatal(self, tmp_path):
        """If kv_store write fails, get_credential still succeeds."""
        cache = _make_cache(tmp_path)
        cmd = _echo_cmd(_cred_json(
            access_key_id="KV_WRITE_FAIL",
            expiration=_far_future(),
        ))
        p = ExternalCredentialProvider(command=cmd, timeout=10, cache=cache)

        # Make set_kv raise
        cache.set_kv = MagicMock(side_effect=Exception("db locked"))

        # Should NOT raise
        cred = p.get_credential()
        assert cred.access_key_id == "KV_WRITE_FAIL"

    def test_no_cache_means_no_kv_store(self, tmp_path):
        """Without cache parameter, kv_store is not used."""
        cmd = _echo_cmd(_cred_json(
            access_key_id="NO_CACHE",
            expiration=_far_future(),
        ))
        p = ExternalCredentialProvider(command=cmd, timeout=10, cache=None)
        cred = p.get_credential()
        assert cred.access_key_id == "NO_CACHE"
        # _try_kv_store_hit returns None when cache is None
        assert p._try_kv_store_hit() is None


# ============================================================
# ExternalCredentialProvider — pyodps interface methods
# ============================================================

class TestExternalCredentialProviderPyodpsInterface:

    def test_get_access_id(self, tmp_path):
        cmd = _echo_cmd(_cred_json(access_key_id="PYODPS_AK"))
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        assert p.get_access_id() == "PYODPS_AK"

    def test_get_access_key(self, tmp_path):
        cmd = _echo_cmd(_cred_json(access_key_secret="PYODPS_SK"))
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        assert p.get_access_key() == "PYODPS_SK"


# ============================================================
# ExternalCredentialProvider — timeout cap
# ============================================================

class TestExternalCredentialProviderTimeout:

    def test_timeout_capped_at_600(self):
        p = ExternalCredentialProvider(command="echo hi", timeout=9999)
        assert p.timeout == 600

    def test_timeout_within_limit(self):
        p = ExternalCredentialProvider(command="echo hi", timeout=30)
        assert p.timeout == 30


# ============================================================
# build_external_account
# ============================================================

class TestBuildExternalAccount:

    def test_builds_credential_provider_account(self):
        """build_external_account returns a CredentialProviderAccount."""
        try:
            from odps.accounts import CredentialProviderAccount
        except ImportError:
            pytest.skip("pyodps not installed")

        settings = {
            "external_process_command": _echo_cmd(_cred_json()),
            "external_process_timeout": "10",
        }
        account = build_external_account(settings)
        assert isinstance(account, CredentialProviderAccount)

    def test_missing_command_raises(self):
        """build_external_account raises ValidationError without command."""
        settings = {"external_process_command": "", "external_process_timeout": "10"}
        with pytest.raises(ValidationError, match="process_command"):
            build_external_account(settings)

    def test_cache_passed_through(self, tmp_path):
        """Cache parameter is forwarded to ExternalCredentialProvider."""
        cache = _make_cache(tmp_path)
        settings = {
            "external_process_command": _echo_cmd(_cred_json(
                expiration=_far_future(),
            )),
            "external_process_timeout": "10",
        }
        account = build_external_account(settings, cache=cache)
        # Verify by checking the provider's _cache attribute
        assert account.provider._cache is cache


# ============================================================
# infer_auth_provider — external branch
# ============================================================

class TestInferAuthProviderExternal:

    def _make_config(self, *, provider: Optional[str] = None,
                     ext_command: Optional[str] = None) -> MaxCConfig:
        return _minimal_config(provider=provider, ext_command=ext_command)

    def test_explicit_external_provider(self):
        config = self._make_config(provider="external", ext_command="/usr/bin/foo")
        assert infer_auth_provider(config, {}) == "external"

    def test_inferred_from_external_config(self):
        """No explicit provider, but external.process_command set → external."""
        config = self._make_config(ext_command="/usr/bin/foo")
        assert infer_auth_provider(config, {}) == "external"

    def test_inferred_from_env_var(self):
        """external_process_command in settings → external."""
        config = self._make_config()  # no provider, no ext config
        settings = {"external_process_command": "/usr/bin/foo"}
        assert infer_auth_provider(config, settings) == "external"

    def test_explicit_provider_takes_precedence(self):
        """Explicit access_key wins over external config."""
        config = self._make_config(provider="access_key", ext_command="/usr/bin/foo")
        assert infer_auth_provider(config, {}) == "access_key"

    def test_no_external_falls_back_to_access_key(self):
        config = self._make_config()
        assert infer_auth_provider(config, {}) == "access_key"


# ============================================================
# resolve_auth_connection — external branch
# ============================================================

class TestResolveAuthConnectionExternal:

    def _make_config(self, command: str) -> MaxCConfig:
        return _minimal_config(
            provider="external",
            ext_command=command,
            project="test_proj",
            endpoint="http://service.cn-shanghai.maxcompute.aliyun.com/api",
        )

    def test_resolves_external_auth(self, tmp_path):
        """resolve_auth_connection with provider=external succeeds."""
        config = self._make_config(_echo_cmd(_cred_json(expiration=_far_future())))
        cache = _make_cache(tmp_path)
        resolved = resolve_auth_connection(config, cache=cache)
        assert resolved.auth_type == "external"
        assert resolved.provider == "external"
        assert resolved.project == "test_proj"

    def test_missing_command_raises(self):
        """resolve_auth_connection raises when external has no command."""
        config = _minimal_config(
            provider="external",
            project="test_proj",
            endpoint="http://service.cn-shanghai.maxcompute.aliyun.com/api",
        )
        with pytest.raises(ValidationError, match="process_command"):
            resolve_auth_connection(config)


# ============================================================
# ExternalAuthConfig — serialization
# ============================================================

class TestExternalAuthConfigSerialization:

    def test_from_mapping_full(self):
        cfg = ExternalAuthConfig.from_mapping({
            "process_command": "/usr/bin/foo",
            "process_timeout": 30,
        })
        assert cfg.process_command == "/usr/bin/foo"
        assert cfg.process_timeout == 30

    def test_from_mapping_defaults(self):
        cfg = ExternalAuthConfig.from_mapping({})
        assert cfg.process_command is None
        assert cfg.process_timeout == 60

    def test_from_mapping_command_alias(self):
        """'command' is an alias for 'process_command'."""
        cfg = ExternalAuthConfig.from_mapping({"command": "/usr/bin/bar"})
        assert cfg.process_command == "/usr/bin/bar"

    def test_to_mapping(self):
        cfg = ExternalAuthConfig(process_command="/usr/bin/foo", process_timeout=30)
        m = cfg.to_mapping()
        assert m["process_command"] == "/usr/bin/foo"
        assert m["process_timeout"] == 30

    def test_is_configured(self):
        assert ExternalAuthConfig(process_command="/usr/bin/foo").is_configured()
        assert not ExternalAuthConfig(process_command=None).is_configured()
        assert not ExternalAuthConfig(process_command="").is_configured()


# ============================================================
# SimpleTempCredential — security_token is Optional
# ============================================================

class TestSimpleTempCredential:

    def test_without_security_token(self):
        cred = SimpleTempCredential(
            access_key_id="LTAI",
            access_key_secret="secret",
            security_token=None,
            expires_at=None,
        )
        assert cred.get_security_token() is None

    def test_with_security_token(self):
        cred = SimpleTempCredential(
            access_key_id="LTAI",
            access_key_secret="secret",
            security_token="CAIS...",
            expires_at=None,
        )
        assert cred.get_security_token() == "CAIS..."

    def test_get_access_key_id_and_secret(self):
        cred = SimpleTempCredential(
            access_key_id="LTAI",
            access_key_secret="secret",
            security_token=None,
            expires_at=None,
        )
        assert cred.get_access_key_id() == "LTAI"
        assert cred.get_access_key_secret() == "secret"


# ============================================================
# NCS → External transparent migration tests
# ============================================================

class TestNcsToExternalMigration:
    """Old configs with ``provider: ncs`` are normalized to ``external``
    at runtime so that a single code path handles both."""

    def _make_ncs_config(self, **ncs_overrides):
        from pathlib import Path as _P

        from maxc_cli.config import AgentConfig, AuthConfig, MaxCConfig, NcsAuthConfig
        defaults = dict(
            account_type="user",
            employee_id="123456",
            process_command="ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd",
        )
        defaults.update(ncs_overrides)
        ncs = NcsAuthConfig(**defaults)
        return MaxCConfig(
            default_project="demo",
            default_schema=None,
            default_format="json",
            default_region="cn-shanghai",
            project_context="testing",
            allowed_operations=["SELECT"],
            cost_threshold_cu=100,
            sensitive_columns=[],
            masking_enabled=True,
            agent=AgentConfig(),
            auth=AuthConfig(
                provider="ncs",
                project="demo",
                endpoint="http://service.cn.maxcompute.aliyun.com/api",
                ncs=ncs,
            ),
            state_dir=_P("/tmp/maxc_test_state"),
            cache_dir=_P("/tmp/maxc_test_cache"),
            catalog={},
            sources=[],
        )

    def test_infer_auth_provider_ncs_returns_external(self):
        """``ncs`` is treated as ``external`` in provider inference."""
        config = self._make_ncs_config()
        settings = {"provider": "ncs", "ncs_process_command": config.auth.ncs.process_command}
        result = infer_auth_provider(config, settings)
        assert result == "external"

    def test_resolve_odps_settings_converts_ncs_to_external(self):
        """resolve_odps_settings transparently converts ``provider: ncs``
        to ``provider: external`` and moves the process_command."""
        from maxc_cli.helpers import resolve_odps_settings

        config = self._make_ncs_config()
        settings, _, _ = resolve_odps_settings(config)
        assert settings["provider"] == "external"
        assert settings["external_process_command"] == "ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd"

    def test_resolve_odps_settings_ncs_derives_command_from_account_type(self):
        """When ncs config has account_type + identifier but no explicit
        process_command, the command is derived automatically."""
        from maxc_cli.helpers import resolve_odps_settings

        config = self._make_ncs_config(process_command=None)
        settings, _, _ = resolve_odps_settings(config)
        assert settings["provider"] == "external"
        assert "123456" in (settings["external_process_command"] or "")
        assert "ncs create credential odpsuser" in (settings["external_process_command"] or "")

    def test_resolve_odps_settings_ncs_app_type_derives_command(self):
        """NCS app account type is also auto-derived."""
        from maxc_cli.helpers import resolve_odps_settings

        config = self._make_ncs_config(
            account_type="app",
            employee_id=None,
            app_name="my-app",
            process_command=None,
        )
        settings, _, _ = resolve_odps_settings(config)
        assert settings["provider"] == "external"
        assert "odpsapp" in (settings["external_process_command"] or "")
        assert "my-app" in (settings["external_process_command"] or "")

    def test_resolve_auth_connection_ncs_uses_external_provider(self):
        """resolve_auth_connection with ``provider: ncs`` config ends up
        using ExternalCredentialProvider (not a separate NCS path)."""
        config = self._make_ncs_config()
        conn = resolve_auth_connection(config)
        assert conn.auth_type == "external"
        assert conn.provider == "external"


# ============================================================
# get_credentials alias + exception logging
# ============================================================

class TestGetCredentialsAliasAndExceptionLogging:
    """Verify that get_credentials() works (pyodps fallback) and that
    original exceptions are logged instead of being silently swallowed."""

    def test_get_credentials_is_alias_of_get_credential(self):
        """get_credentials() returns the same result as get_credential()."""
        cmd = _echo_cmd(_cred_json())
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        c1 = p.get_credential()
        c2 = p.get_credentials()
        assert c1.access_key_id == c2.access_key_id
        assert c1.access_key_secret == c2.access_key_secret

    def test_get_credentials_propagates_original_error(self):
        """When the command fails, get_credentials() raises the original
        ValidationError — not AttributeError."""
        cmd = "exit 42"
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        with pytest.raises(ValidationError, match="exited with code 42"):
            p.get_credentials()

    def test_get_credential_logs_original_error(self, caplog):
        """When get_credential() fails, the original error is logged
        at WARNING level so it is not silently swallowed by pyodps'
        bare ``except`` in CredentialProviderAccount._refresh_credential."""
        cmd = "exit 42"
        p = ExternalCredentialProvider(command=cmd, timeout=10)
        with caplog.at_level(logging.WARNING, logger="maxc_cli.auth_providers"):
            with pytest.raises(ValidationError, match="exited with code 42"):
                p.get_credential()
        assert any("get_credential() failed" in rec.message for rec in caplog.records)

    def test_credential_provider_account_fallback_uses_get_credentials(self):
        """Integration test: CredentialProviderAccount._refresh_credential
        falls back from get_credential → get_credentials when the first
        call raises.  With our alias, both calls raise the same
        ValidationError (not AttributeError)."""
        try:
            from odps.accounts import CredentialProviderAccount
        except ImportError:
            pytest.skip("pyodps not installed")

        cmd = "exit 42"
        provider = ExternalCredentialProvider(command=cmd, timeout=10)
        account = CredentialProviderAccount(provider)

        with pytest.raises(ValidationError, match="exited with code 42"):
            account._refresh_credential()


# ============================================================
# _auth_seems_configured — regression for external / ncs auth
# ============================================================

class TestAuthSeemsConfiguredExternal:
    """Regression: pre-0.3.1, ``_auth_seems_configured`` only recognized
    AK/SK (config or env), so users who ran ``maxc auth login-external``
    were treated as unauthenticated and either redirected to ``maxc auth
    login`` (TTY) or hit VALIDATION_ERROR (non-TTY) on ``query`` / ``data``
    / ``meta`` commands.
    """

    @staticmethod
    def _clear_odps_env(monkeypatch):
        import maxc_cli.backend as backend_module
        for aliases in backend_module.ODPS_ENV_ALIASES.values():
            for alias in aliases:
                monkeypatch.delenv(alias, raising=False)

    @staticmethod
    def _stub_app(config: MaxCConfig):
        return type("StubApp", (), {"config": config})()

    def test_external_process_command_is_recognized(self, monkeypatch):
        from maxc_cli.cli import _auth_seems_configured
        self._clear_odps_env(monkeypatch)
        config = _minimal_config(provider="external", ext_command="/usr/bin/foo")
        assert _auth_seems_configured(self._stub_app(config)) is True

    def test_ncs_process_command_is_recognized(self, monkeypatch):
        from pathlib import Path as _P

        from maxc_cli.cli import _auth_seems_configured
        from maxc_cli.config import AgentConfig, AuthConfig, MaxCConfig, NcsAuthConfig
        self._clear_odps_env(monkeypatch)
        config = MaxCConfig(
            default_project="demo",
            default_schema=None,
            default_format="json",
            default_region="cn-shanghai",
            project_context="testing",
            allowed_operations=["SELECT"],
            cost_threshold_cu=100,
            sensitive_columns=[],
            masking_enabled=True,
            agent=AgentConfig(),
            auth=AuthConfig(
                provider="ncs",
                project="demo",
                endpoint="http://service.cn.maxcompute.aliyun.com/api",
                ncs=NcsAuthConfig(
                    employee_id="123456",
                    process_command="ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd",
                ),
            ),
            state_dir=_P("/tmp/maxc_test_state"),
            cache_dir=_P("/tmp/maxc_test_cache"),
            catalog={},
            sources=[],
        )
        assert _auth_seems_configured(self._stub_app(config)) is True

    def test_access_key_in_config_is_recognized(self, monkeypatch):
        from maxc_cli.cli import _auth_seems_configured
        self._clear_odps_env(monkeypatch)
        config = _minimal_config()
        config.auth.access_id = "AK"
        config.auth.secret_access_key = "SK"
        assert _auth_seems_configured(self._stub_app(config)) is True

    def test_nothing_configured_returns_false(self, monkeypatch):
        from maxc_cli.cli import _auth_seems_configured
        self._clear_odps_env(monkeypatch)
        config = _minimal_config()
        assert _auth_seems_configured(self._stub_app(config)) is False
