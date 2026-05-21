"""Tests for maxc-cli using FakeODPS mock.

These tests use FakeODPS to mock the ODPS client, allowing testing of
authentication and configuration flows without a real MaxCompute connection.
"""


import json
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
import yaml

import maxc_cli.backend as backend_module
from maxc_cli.cli import run


def clear_odps_env(monkeypatch) -> 'None':
    """Clear all ODPS-related environment variables."""
    for aliases in backend_module.ODPS_ENV_ALIASES.values():
        for alias in aliases:
            monkeypatch.delenv(alias, raising=False)


def isolate_home(monkeypatch, tmp_path: 'Path') -> 'None':
    monkeypatch.setenv("HOME", str(tmp_path))


def run_json_command(
    tmp_path: 'Path',
    config_path: 'Path | None',
    argv: 'list[str]',
) -> 'tuple[int, dict[str, object], str]':
    """Run a command and return (exit_code, json_payload, stderr).

    Pass ``config_path=None`` to skip ``--config`` and let normal config discovery run.
    """
    stdout = StringIO()
    stderr = StringIO()

    full_argv = list(argv) if config_path is None else ["--config", str(config_path), *argv]
    code = run(
        full_argv,
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    return code, json.loads(stdout.getvalue()), stderr.getvalue()


class FakeODPS:
    """Mock ODPS client for testing."""

    def __init__(
        self,
        access_id=None,
        secret_access_key: 'str | None' = None,
        project: 'str | None' = None,
        endpoint: 'str | None' = None,
        region_name: 'str | None' = None,
        tunnel_endpoint: 'str | None' = None,
        **_: 'object',
    ) -> 'None':
        if hasattr(access_id, "access_id"):
            account = access_id
            access_id = getattr(account, "access_id", None)
        self.account = type("Account", (), {"access_id": access_id})()
        self.project = project
        self.endpoint = endpoint
        self.region_name = region_name
        self.tunnel_endpoint = tunnel_endpoint
        self.tunnel = FakeTunnel()
        # Catalog API stubs — no real catalog in tests
        self.schema = None
        self.app_account = None
        self.namespace = None
        self._rest_client_cls = None
        self._rest_client_kwargs = {}

    @property
    def catalog_endpoint(self):
        return None

    @property
    def catalog_rest(self):
        return None

    def get_project(self, project: 'str'):
        """Return mock project with owner and tenant_id."""
        return type("Project", (), {
            "owner": f"ALIYUN$mock_user_{project}",
            "tenant_id": "000000000000000",
        })()

    def execute_security_query(self, query: 'str', project: 'str | None' = None):
        """Mock security query - returns dict with DisplayName."""
        if query == "whoami":
            target_project = project or self.project
            return {
                "DisplayName": f"ALIYUN$mock_user_{target_project}",
                "ID": "123456789",
                "SourceIP": "127.0.0.1"
            }
        raise NotImplementedError(f"Unknown security query: {query}")


class _FakeRecord(dict):
    """Behaves like an odps Record: indexable by column name."""


class FakeUploadSession:
    def __init__(self, table, partition, overwrite, store):
        self.table = table
        self.partition = partition
        self.overwrite = overwrite
        self._store = store
        self.committed_blocks: 'list[int]' = []
        self.aborted = False

    def new_record(self):
        return _FakeRecord()

    def open_record_writer(self, block_id: int):
        records: 'list[dict]' = []
        self._store.setdefault((self.table, self.partition), []).append(
            (block_id, records, self.overwrite)
        )

        class _Writer:
            def write(self_inner, record):
                records.append(dict(record))

            def close(self_inner):
                pass

        return _Writer()

    def commit(self, blocks):
        self.committed_blocks = list(blocks)

    def abort(self):
        self.aborted = True


class FakeDownloadSession:
    def __init__(self, table, partition, rows):
        self.table = table
        self.partition = partition
        self._rows = list(rows)
        self.count = len(self._rows)

    def open_record_reader(self, start: int, count: int):
        return iter(self._rows[start:start + count])


class FakeTunnel:
    """Stub for odps.tunnel.TableTunnel.

    Class-level `last_upload_session` and `download_rows` allow tests to
    inspect/seed state across the FakeODPS instance the CLI constructs.
    """

    last_upload_session: 'FakeUploadSession | None' = None
    download_rows: 'dict[tuple, list[_FakeRecord]]' = {}

    def __init__(self):
        self.upload_store: 'dict[tuple, list]' = {}

    def create_upload_session(self, table, partition_spec=None, overwrite=False):
        sess = FakeUploadSession(table, partition_spec, overwrite, self.upload_store)
        FakeTunnel.last_upload_session = sess
        return sess

    def create_download_session(self, table, partition_spec=None):
        rows = FakeTunnel.download_rows.get((table, partition_spec), [])
        return FakeDownloadSession(table, partition_spec, rows)


class BrokenWhoamiODPS(FakeODPS):
    """Mock ODPS client that resolves config but fails remote whoami validation."""

    def execute_security_query(self, query: 'str', project: 'str | None' = None):
        if query == "whoami":
            raise OSError("failed to resolve remote whoami endpoint")
        return super().execute_security_query(query, project=project)


def test_csv_parse_error_carries_line_and_column():
    from maxc_cli.exceptions import CsvParseError, ValidationError

    err = CsvParseError(
        "could not parse 'abc' as bigint",
        line=42,
        column="user_id",
        suggestion="check the row format",
    )
    assert isinstance(err, ValidationError)
    assert err.line == 42
    assert err.column == "user_id"
    assert err.error_code == "CSV_PARSE_ERROR"
    payload = err.to_payload().to_dict()
    assert payload["code"] == "CSV_PARSE_ERROR"
    assert payload["suggestion"] == "check the row format"


# ============================================================
# Auth Login Tests (don't require backend connection)
# ============================================================

def test_auth_login_can_create_new_explicit_config_without_validation(
    tmp_path: 'Path',
    monkeypatch,
) -> 'None':
    """Test auth login creates config file with --no-validate."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config_path = tmp_path / "login.yaml"

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth",
            "login",
            "--access-id",
            "TESTACCESS1234",
            "--secret-access-key",
            "TESTSECRET1234",
            "--project",
            "login_project",
            "--endpoint",
            "http://service.cn-test.maxcompute.aliyun.com/api",
            "--region",
            "cn-test",
            "--no-validate",
            "--json",
        ],
    )

    assert code == 0
    assert payload["command"] == "auth login"
    assert payload["data"]["persistence"]["saved"] is True
    assert payload["data"]["persistence"]["validated"] is False
    assert payload["data"]["identity"]["identity_source"] == "config_file"
    assert payload["metadata"]["config_path"] == str(config_path.resolve())

    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert saved["auth"]["access_id"] == "TESTACCESS1234"
    assert saved["auth"]["secret_access_key"] == "TESTSECRET1234"
    assert saved["auth"]["project"] == "login_project"
    assert saved["auth"]["endpoint"] == "http://service.cn-test.maxcompute.aliyun.com/api"
    assert saved["auth"]["region_name"] == "cn-test"
    assert saved["default_project"] == "login_project"
    assert saved["default_region"] == "cn-test"


def test_auth_whoami_uses_saved_config_credentials_when_env_missing(
    tmp_path: 'Path',
    monkeypatch,
) -> 'None':
    """Test auth whoami reads from saved config when env vars are missing."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    # Mock ODPS in the odps package where it's imported from
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_format: json
state_dir: .maxc/state
backend:
  type: auto
allowed_operations:
  - SELECT
auth:
  access_id: TESTACCESS1234
  secret_access_key: TESTSECRET1234
  project: config_project
  endpoint: http://service.cn-test.maxcompute.aliyun.com/api
  region_name: cn-test
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "whoami", "--json"],
    )

    assert code == 0
    assert payload["command"] == "auth whoami"
    identity = payload["data"]["identity"]
    assert identity["authenticated"] is True
    assert identity["configured"] is True
    assert identity["validation_status"] == "verified"
    assert identity["backend"] == "odps"
    assert identity["identity_source"] == "config_file"
    assert identity["project"] == "config_project"
    assert identity["region"] == "cn-test"
    assert identity["endpoint"] == "http://service.cn-test.maxcompute.aliyun.com/api"
    assert identity["project_owner"] == "ALIYUN$mock_user_config_project"


# ============================================================
# Backend Creation Tests
# ============================================================

def test_auth_whoami_returns_guidance_without_odps_config(tmp_path: 'Path', monkeypatch) -> 'None':
    """Verify auth whoami returns guidance when auth config is missing."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
backend:
  type: auto
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "whoami", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["identity"]["authenticated"] is False
    assert payload["data"]["identity"]["configured"] is False
    assert payload["data"]["identity"]["validation_status"] == "missing_configuration"
    assert payload["data"]["auth_options"][0]["type"] == "access_key"


def test_auth_whoami_marks_configured_but_unverified_when_remote_check_fails(
    tmp_path: 'Path',
    monkeypatch,
) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps

    monkeypatch.setattr(odps, "ODPS", BrokenWhoamiODPS)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo_project
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
auth:
  access_id: TESTACCESS1234
  secret_access_key: TESTSECRET1234
  project: demo_project
  endpoint: http://service.cn-test.maxcompute.aliyun.com/api
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "whoami", "--json"],
    )

    assert code == 0
    identity = payload["data"]["identity"]
    assert identity["authenticated"] is False
    assert identity["configured"] is True
    assert identity["validation_status"] == "failed"
    assert identity["identity_source"] == "config_file"
    assert "maxc auth login" in str(payload["agent_hints"]["next_actions"])
    assert any(
        "failed to resolve remote whoami endpoint" in warning
        for warning in payload["agent_hints"]["warnings"]
    )


def test_auth_login_supports_sts_token_payload(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config_path = tmp_path / "login-sts.yaml"

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth",
            "login",
            "--access-id",
            "TESTACCESS1234",
            "--secret-access-key",
            "TESTSECRET1234",
            "--security-token",
            "TESTSTS1234",
            "--project",
            "login_project",
            "--endpoint",
            "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-validate",
            "--json",
        ],
    )

    assert code == 0
    assert payload["data"]["identity"]["auth_type"] == "sts_token"
    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert saved["auth"]["provider"] == "sts_token"
    assert saved["auth"]["security_token"] == "TESTSTS1234"


# ============================================================
# auth login: interactive Catalog API picker (Task 6)
# ============================================================


def test_auth_login_picker_selects_project_and_derives_endpoint(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """auth_login without --project but with TTY pops the picker."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    monkeypatch.setattr(cb, "build_bootstrap_odps", lambda **kw: object())
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: [
            cb.ProjectInfo("test_proj_a", "cn-hangzhou", "ALIYUN$x", True, ""),
            cb.ProjectInfo("test_proj_b", "cn-shanghai", "ALIYUN$y", False, ""),
        ],
    )
    # Force "TTY available" + user picks #2
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "2")

    config_path = tmp_path / "login.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    assert payload["status"] == "success"
    identity = payload["data"]["identity"]
    assert identity["project"] == "test_proj_b"
    assert "cn-shanghai" in identity["endpoint"]
    assert identity["region"] == "cn-shanghai"


def test_auth_login_picker_skipped_when_project_provided(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """Explicit --project must skip the picker even with TTY."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    called = []
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: (called.append(1) or []),
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    config_path = tmp_path / "login.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--project", "explicit_proj",
            "--endpoint", "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    assert called == []  # picker was not invoked
    assert payload["data"]["identity"]["project"] == "explicit_proj"


def test_auth_login_picker_falls_back_to_prompt_when_catalog_fails(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """If catalog raises, fall back to today's behavior: prompt for project."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    def _boom(**kw):
        raise RuntimeError("catalog unreachable")
    monkeypatch.setattr(cb, "build_bootstrap_odps", _boom)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "manual_proj")

    config_path = tmp_path / "login.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--endpoint", "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["identity"]["project"] == "manual_proj"


def test_auth_login_picker_runs_when_project_in_env_without_from_env(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """MAXCOMPUTE_PROJECT in the env must NOT skip the picker unless
    --from-env is set — gating mirrors ``_resolve_login_value``'s
    ``use_env`` semantics and prevents the silent re-routing pattern
    called out in CLAUDE.md.
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    # Set MAXCOMPUTE_PROJECT *after* clear_odps_env so it survives into the test.
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "env_proj")

    from maxc_cli import catalog_bootstrap as cb

    called = []
    monkeypatch.setattr(
        cb, "build_bootstrap_odps",
        lambda **kw: (called.append(1) or object()),
    )
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: [cb.ProjectInfo("picked_proj", "cn-hangzhou", "ALIYUN$x", True, "")],
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "1")

    config_path = tmp_path / "login.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    # Picker WAS invoked — env value did not silently win.
    assert called == [1]
    assert payload["data"]["identity"]["project"] == "picked_proj"


def test_auth_login_accepts_catalog_endpoint_and_no_picker_flags(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """argparse must accept --catalog-endpoint and --no-picker without rejecting.

    Task 7: CLI argparse changes. With --no-picker, the picker is bypassed
    so the existing --project flow runs and persists normally.
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "login.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--project", "explicit_proj",
            "--endpoint", "http://service.cn-test.maxcompute.aliyun.com/api",
            "--region", "cn-test",
            "--catalog-endpoint", "http://catalog.cn-test.maxcompute.aliyun.com",
            "--no-picker",
            "--no-validate",
            "--json",
        ],
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["identity"]["project"] == "explicit_proj"


def test_auth_login_reselect_forces_picker_even_with_existing_config(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """--reselect must ignore a previously saved auth.project and re-open the picker."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    # Pre-seed an existing config with auth.project=old_proj.
    config_path = tmp_path / "login.yaml"
    config_path.write_text(
        "auth:\n"
        "  access_id: AK_OLD\n"
        "  secret_access_key: SK_OLD\n"
        "  project: old_proj\n"
        "  endpoint: http://service.cn-old.maxcompute.aliyun.com/api\n",
        encoding="utf-8",
    )

    called = []
    monkeypatch.setattr(
        cb, "build_bootstrap_odps",
        lambda **kw: (called.append(1) or object()),
    )
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: [cb.ProjectInfo("new_proj", "cn-shanghai", "ALIYUN$x", True, "")],
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "1")

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--reselect",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    # Picker WAS invoked even though auth.project=old_proj was saved.
    assert called == [1]
    assert payload["data"]["identity"]["project"] == "new_proj"


def test_auth_login_reselect_with_no_picker_skips_picker(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """--no-picker wins over --reselect: the picker is NOT invoked."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    # Pre-seed an existing config with auth.project=old_proj.
    config_path = tmp_path / "login.yaml"
    config_path.write_text(
        "auth:\n"
        "  access_id: AK_OLD\n"
        "  secret_access_key: SK_OLD\n"
        "  project: old_proj\n"
        "  endpoint: http://service.cn-old.maxcompute.aliyun.com/api\n",
        encoding="utf-8",
    )

    called = []
    monkeypatch.setattr(
        cb, "build_bootstrap_odps",
        lambda **kw: (called.append(1) or object()),
    )
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: (called.append("list") or []),
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--reselect",
            "--no-picker",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    # Picker was NOT invoked — --no-picker takes precedence.
    assert called == []
    # With --reselect the saved project is ignored; --no-picker on non-explicit
    # project falls back to _resolve_login_value which uses existing_value
    # from existing_auth.project as a fallback (today's prompt behavior).
    # The saved old_proj is used as the existing-value fallback for the prompt.
    assert payload["data"]["identity"]["project"] == "old_proj"


def test_session_show_and_agent_context_work_without_auth(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    session_code, session_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["session", "show", "--json"],
    )
    assert session_code == 0
    assert session_payload["command"] == "session show"
    assert session_payload["data"]["project"]["value"] == "demo"

    context_code, context_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["agent", "context", "--json"],
    )
    assert context_code == 0
    assert context_payload["command"] == "agent context"
    assert context_payload["data"]["context"]["project"] == "demo"
    assert context_payload["metadata"]["job_mode"] == "unknown"


def test_cache_status_requires_auth_with_structured_failure(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["cache", "status", "--json"],
    )

    assert code == 1
    assert payload["command"] == "cache status"
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"


def test_session_set_without_values_returns_standard_failure_envelope(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["session", "set", "--json"],
    )

    assert code == 1
    assert payload["command"] == "session set"
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"


# ============================================================
# (NCS-specific tests removed — NCS is now a runtime alias for external)
# ============================================================

def test_auth_whoami_metadata_includes_config_sources(tmp_path: 'Path', monkeypatch) -> None:
    """auth whoami metadata should list the active config file paths."""
    import odps as odps_pkg
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: access_key\n"
        "  access_id: TESTID1234\n"
        "  secret_access_key: TESTSECRET1234\n"
        "  project: test_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(odps_pkg, "ODPS", FakeODPS)

    code, payload, _ = run_json_command(tmp_path, config_path, ["auth", "whoami", "--json"])
    assert code == 0
    assert "config_sources" in payload["metadata"]
    assert isinstance(payload["metadata"]["config_sources"], list)
    assert any(str(config_path) in s for s in payload["metadata"]["config_sources"])


def test_session_show_data_includes_config_sources(tmp_path: 'Path', monkeypatch) -> None:
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: demo\n"
        "default_format: json\n"
        "state_dir: .maxc/state\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(tmp_path, config_path, ["session", "show", "--json"])
    assert code == 0
    assert "config_sources" in payload["data"]
    assert isinstance(payload["data"]["config_sources"], list)


# ============================================================
# Task 3: session set warns on auth project mismatch
# ============================================================

def test_session_set_warns_when_project_differs_from_auth_project(tmp_path: 'Path', monkeypatch) -> None:
    """session set should warn when override project differs from auth.project."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: external\n"
        "  project: ext_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n"
        "  external:\n"
        "    process_command: 'ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd'\n"
        "default_project: ext_project\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["session", "set", "--project", "other_project", "--json"]
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("ext_project" in w and "other_project" in w for w in warnings), (
        f"Expected a warning about project mismatch, got: {warnings}"
    )


# ============================================================
# (login-ncs interactive tests removed — command consolidated into login-external)
# ============================================================

def test_env_vars_suppressed_when_explicit_provider_in_config(
    tmp_path: 'Path', monkeypatch
) -> None:
    """When config has an explicit auth provider, env vars must not override any auth settings."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    # Set env vars that would normally override project/endpoint
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "env_project")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "http://env-endpoint.example.com/api")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env_key")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env_secret")

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
auth:
  provider: access_key
  access_id: config_key
  secret_access_key: config_secret
  project: config_project
  endpoint: http://config-endpoint.example.com/api
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(tmp_path, config_path, ["auth", "whoami", "--json"])

    assert code == 0
    identity = payload["data"]["identity"]
    # Config values must win — env vars must not override
    assert identity["project"] == "config_project", (
        f"Expected config_project but got {identity['project']!r}; env var leaked through"
    )
    assert identity["endpoint"] == "http://config-endpoint.example.com/api", (
        f"Expected config endpoint but got {identity['endpoint']!r}; env var leaked through"
    )
    assert identity["identity_source"] == "config_file"
    # Suppressed env vars must be surfaced in warnings
    warnings = payload["agent_hints"]["warnings"]
    assert any("ignored" in w.lower() or "suppressed" in w.lower() or "ignored" in w.lower() for w in warnings), (
        f"Expected a warning about suppressed env vars: {warnings}"
    )


def test_env_vars_active_when_no_provider_in_config(
    tmp_path: 'Path', monkeypatch
) -> None:
    """When config has no explicit provider, env vars should still provide auth settings."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env_key")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env_secret")
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "env_project")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "http://env-endpoint.example.com/api")

    # Config has no provider field — env vars should take effect
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
backend:
  type: auto
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(tmp_path, config_path, ["auth", "whoami", "--json"])

    assert code == 0
    identity = payload["data"]["identity"]
    assert identity["project"] == "env_project", (
        f"Expected env_project but got {identity['project']!r}"
    )
    assert identity["identity_source"] in ("environment", "mixed")


# ============================================================
# (NCS credential provider tests removed — NcsCredentialProvider replaced by ExternalCredentialProvider)
# ============================================================

def test_legacy_session_override_is_migrated_into_global_config(
    tmp_path: 'Path', monkeypatch
) -> None:
    """A pre-existing ~/.maxc/session_override.yaml must be folded into config.yaml on load."""
    from maxc_cli.config import load_config

    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    maxc_dir = tmp_path / ".maxc"
    maxc_dir.mkdir(parents=True)
    override = maxc_dir / "session_override.yaml"
    override.write_text("project: legacy_proj\nschema: legacy_schema\n", encoding="utf-8")

    global_config = maxc_dir / "config.yaml"
    global_config.write_text("default_project: ignored_old_value\n", encoding="utf-8")

    cfg = load_config(cwd=tmp_path)

    assert cfg.default_project == "legacy_proj"
    assert cfg.default_schema == "legacy_schema"
    assert not override.exists(), "legacy session_override.yaml should be removed after migration"
    new_global = yaml.safe_load(global_config.read_text(encoding="utf-8"))
    assert new_global["default_project"] == "legacy_proj"
    assert new_global["default_schema"] == "legacy_schema"


def test_session_set_writes_to_global_config(tmp_path: 'Path', monkeypatch) -> None:
    """session set should persist project/schema to ~/.maxc/config.yaml, no override file created."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: demo\n"
        "default_format: json\n"
        "state_dir: .maxc/state\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["session", "set", "--project", "new_proj", "--schema", "new_schema", "--json"],
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["project"] == "new_proj"
    assert payload["data"]["schema"] == "new_schema"

    global_config_path = tmp_path / ".maxc" / "config.yaml"
    assert global_config_path.exists()
    persisted = yaml.safe_load(global_config_path.read_text(encoding="utf-8"))
    assert persisted["default_project"] == "new_proj"
    assert persisted["default_schema"] == "new_schema"

    assert not (tmp_path / ".maxc" / "session_override.yaml").exists()


def test_session_set_warns_when_project_config_shadows(tmp_path: 'Path', monkeypatch) -> None:
    """If a higher-precedence config file sets default_project, session set should warn."""
    clear_odps_env(monkeypatch)

    # Separate HOME from cwd so ~/.maxc/config.yaml and cwd/.maxc/config.yaml are distinct.
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    cwd_maxc = work_dir / ".maxc"
    cwd_maxc.mkdir(parents=True)
    (cwd_maxc / "config.yaml").write_text(
        "default_project: project_level_proj\n"
        "default_format: json\n"
        "state_dir: .maxc/state\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        work_dir, None,
        ["session", "set", "--project", "user_pref", "--json"],
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("shadow" in w.lower() for w in warnings), (
        f"Expected a warning about project-level config shadowing the user-level write: {warnings}"
    )


def test_session_override_file_is_no_longer_consulted(tmp_path: 'Path', monkeypatch) -> None:
    """After migration runs, even a freshly-created session_override.yaml must NOT influence load_config."""
    from maxc_cli.config import load_config

    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    maxc_dir = tmp_path / ".maxc"
    maxc_dir.mkdir(parents=True)
    (maxc_dir / "config.yaml").write_text("default_project: from_config\n", encoding="utf-8")

    # First load: any pre-existing override (none here) gets migrated.
    load_config(cwd=tmp_path)

    # Now write an override file AFTER migration has run.
    # In the new world this file is meaningless — load_config should ignore it.
    (maxc_dir / "session_override.yaml").write_text("project: should_be_ignored\n", encoding="utf-8")

    cfg = load_config(cwd=tmp_path)
    assert cfg.default_project == "from_config", (
        "session_override.yaml must no longer influence load_config; "
        f"got {cfg.default_project!r}"
    )
    assert not (maxc_dir / "session_override.yaml").exists(), (
        "Stale session_override.yaml should be cleaned up after migration marker is in place."
    )


def test_session_show_does_not_expose_override_path(tmp_path: 'Path', monkeypatch) -> None:
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: demo\n"
        "default_format: json\n"
        "state_dir: .maxc/state\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["session", "show", "--json"]
    )
    assert code == 0
    assert "override_path" not in payload["data"]
    assert payload["data"]["project"]["source"] in ("config_file", "environment")
    assert payload["data"]["schema"]["source"] == "config_file"


def test_session_unset_removes_keys_from_global_config(tmp_path: 'Path', monkeypatch) -> None:
    """session unset should strip default_project/default_schema from ~/.maxc/config.yaml."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    global_config_path = tmp_path / ".maxc" / "config.yaml"
    global_config_path.parent.mkdir(parents=True, exist_ok=True)
    global_config_path.write_text(
        "default_project: to_remove\n"
        "default_schema: also_to_remove\n"
        "default_format: json\n"
        "state_dir: .maxc/state\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path, None, ["session", "unset", "--json"],
    )
    assert code == 0
    assert payload["status"] == "success"
    assert set(payload["data"]["cleared"]) == {"default_project", "default_schema"}

    persisted = yaml.safe_load(global_config_path.read_text(encoding="utf-8"))
    assert "default_project" not in persisted
    assert "default_schema" not in persisted
    assert persisted["default_format"] == "json"


def test_auth_login_from_env_fails_when_required_env_var_missing(
    tmp_path: 'Path', monkeypatch
) -> None:
    """--from-env must raise a clear error when a required env var is missing."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    # Only set access_id, leave secret/project/endpoint unset
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "TEST_ID")

    config_path = tmp_path / "config.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "login", "--from-env", "--no-validate", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert "--from-env" in payload["error"]["message"]


def test_auth_login_from_env_shows_imported_warning(
    tmp_path: 'Path', monkeypatch
) -> None:
    """--from-env should show 'imported from env' warning, not 'may override' warning."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "TEST_ID")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "TEST_SECRET")
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "test_proj")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "http://service.cn-test.maxcompute.aliyun.com/api")

    config_path = tmp_path / "config.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "login", "--from-env", "--no-validate", "--json"],
    )

    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("imported" in w.lower() for w in warnings), (
        f"Expected an 'imported from environment' warning, got: {warnings}"
    )
    assert not any("may override" in w.lower() for w in warnings), (
        f"Should not show 'may override' warning when --from-env is used: {warnings}"
    )


def test_auth_login_reads_env_without_from_env_flag(
    tmp_path: 'Path', monkeypatch
) -> None:
    """maxc auth login should pick up AK/SK/endpoint from env without --from-env.

    Reproduces the UX bug: launcher injects ALIBABA_CLOUD_ACCESS_KEY_ID etc. so
    `maxc query` works, but `maxc auth login` used to ignore the env and prompt
    on stdin — leaving the user in a half-authenticated state.
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "ENVFROMSHELL_ID")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ENV_SECRET")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "http://service.cn-test.maxcompute.aliyun.com/api")
    # Non-TTY so a prompt fallback would deterministically return None and the
    # required-value check would fail. If the env is honored, we succeed.
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    config_path = tmp_path / "config.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            # Project still needs an explicit value because the picker is
            # gated separately ("avoid silent re-routing" — see CLAUDE.md).
            "--project", "explicit_proj",
            "--no-picker",
            "--no-validate",
            "--json",
        ],
    )

    assert code == 0, payload
    identity = payload["data"]["identity"]
    # mask_access_id keeps the first 4 chars when len > 8.
    assert identity["principal_display"].startswith("ENVF"), identity["principal_display"]
    assert identity["endpoint"] == "http://service.cn-test.maxcompute.aliyun.com/api"
    warnings = payload["agent_hints"]["warnings"]
    assert any("env" in w.lower() for w in warnings), warnings


def test_malformed_config_yaml_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """A broken YAML config file should produce a structured error, not a raw traceback."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "broken.yaml"
    config_path.write_text("auth:\n  project: [unterminated\n", encoding="utf-8")

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["auth", "whoami", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert "invalid yaml" in payload["error"]["message"].lower()
    assert payload["error"]["suggestion"] is not None


# ============================================================
# Structured error output for ODPS / unexpected exceptions
# ============================================================


class _NoSuchObjectODPS(FakeODPS):
    """Mock ODPS client whose get_table raises NoSuchObject on schema access."""

    def get_table(self, name, *, project=None, schema=None):
        """Return a table object whose table_schema raises NoSuchObject."""
        try:
            from odps.errors import NoSuchObject
        except ImportError:
            pytest.skip("odps package not installed")

        class _ExplodingSchema:
            @property
            def columns(self):
                raise NoSuchObject(f"Table not found - '{name}'")

            @property
            def partitions(self):
                raise NoSuchObject(f"Table not found - '{name}'")

        return type("FakeTable", (), {
            "name": name,
            "table_schema": _ExplodingSchema(),
            "comment": "",
            "owner": None,
            "creation_time": None,
            "last_data_modified_time": None,
            "is_virtual_view": False,
            "size": 0,
            "lifecycle": None,
        })()

    def list_tables(self, project=None):
        return []

    def read_table(self, *a, **kw):
        return []


def _make_config_with_odps(tmp_path):
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


def test_data_profile_not_found_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """ODPS NoSuchObject from data profile must produce a structured envelope, not a traceback."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _NoSuchObjectODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path, ["data", "profile", "nonexistent_table", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] in ("NOT_FOUND", "TABLE_NOT_FOUND")
    assert "not found" in payload["error"]["message"].lower()


def test_meta_describe_not_found_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """ODPS NoSuchObject from meta describe must produce a structured envelope."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _NoSuchObjectODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path, ["meta", "describe", "nonexistent_table", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] in ("NOT_FOUND", "TABLE_NOT_FOUND")


def test_unexpected_exception_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """An unexpected non-MaxCError exception with --json must produce a structured INTERNAL_ERROR envelope."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    config_path = _make_config_with_odps(tmp_path)

    # Monkeypatch the handler to raise an unexpected error
    import maxc_cli.cli as cli_module

    def _exploding_handler(*a, **kw):
        raise RuntimeError("something completely unexpected")

    monkeypatch.setattr(cli_module, "_handle_data_profile", _exploding_handler)

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["data", "profile", "some_table", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "INTERNAL_ERROR"
    assert "unexpected" in payload["error"]["message"].lower()


def test_unexpected_exception_renders_markdown_without_json_flag(
    tmp_path: 'Path', monkeypatch
) -> None:
    """Without --json, unexpected exception writes markdown error to stderr."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    config_path = _make_config_with_odps(tmp_path)

    import maxc_cli.cli as cli_module

    def _exploding_handler(*a, **kw):
        raise RuntimeError("something completely unexpected")

    monkeypatch.setattr(cli_module, "_handle_data_profile", _exploding_handler)

    stdout = StringIO()
    stderr = StringIO()
    code = run(
        ["--config", str(config_path), "data", "profile", "some_table"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code != 0
    err_text = stderr.getvalue()
    assert "**Error**" in err_text
    assert "`INTERNAL_ERROR`" in err_text
    assert "unexpected" in err_text.lower()
    assert "**Suggestion**" in err_text
    assert stdout.getvalue().strip() == ""


def test_not_found_error_renders_markdown_without_json_flag(
    tmp_path: 'Path', monkeypatch
) -> None:
    """ODPS NoSuchObject without --json writes markdown error to stderr."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _NoSuchObjectODPS)

    config_path = _make_config_with_odps(tmp_path)

    stdout = StringIO()
    stderr = StringIO()
    code = run(
        ["--config", str(config_path), "data", "profile", "nonexistent_table"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code != 0
    err_text = stderr.getvalue()
    assert "**Error**" in err_text
    assert "`NOT_FOUND`" in err_text or "`TABLE_NOT_FOUND`" in err_text
    assert "**Suggestion**" in err_text


# ============================================================
# Schema Passthrough Tests
# ============================================================


class _SchemaAwareODPS(FakeODPS):
    """Mock ODPS client that returns different tables per schema."""

    _SCHEMA_TABLES = {
        None: ["default_table_a", "default_table_b"],
        "california_schools": ["frpm", "satscores", "schools"],
    }

    def list_tables(self, *, project=None, schema=None):
        names = self._SCHEMA_TABLES.get(schema, [])
        return [
            type("FakeTable", (), {"name": n})()
            for n in names
        ]

    def get_table(self, name, *, project=None, schema=None):
        # minimal stub for describe
        return type("FakeTable", (), {
            "name": name,
            "comment": "",
            "table_schema": type("Schema", (), {"columns": [], "partitions": []})(),
            "owner": "test_owner",
            "creation_time": None,
            "last_data_modified_time": None,
            "is_virtual_view": False,
            "size": 0,
            "lifecycle": None,
        })()


def test_meta_list_tables_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta list-tables --schema should list tables from the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "list-tables", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    table_names = [t["table_name"] for t in payload["data"]["tables"]]
    assert sorted(table_names) == ["frpm", "satscores", "schools"]


def test_meta_list_tables_without_schema_uses_default(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta list-tables without --schema should list tables from default schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "list-tables", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    table_names = [t["table_name"] for t in payload["data"]["tables"]]
    assert sorted(table_names) == ["default_table_a", "default_table_b"]


def test_cache_build_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """cache build --schema should list tables from the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["cache", "build", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["data"]["tables_scanned"] == 3
    assert payload["data"]["cached_tables"] == 3


def test_meta_search_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta search --schema should search tables in the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "search", "frpm", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    matches = payload["data"]["search"]["matches"]
    assert len(matches) >= 1
    assert any(m["table_name"] == "frpm" for m in matches)


def test_meta_search_columns_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta search-columns --schema should search in the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    # search for a keyword that won't match (stub tables have no columns),
    # but verify it runs without error and uses the right schema
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "search-columns", "nonexistent", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["search"]["matches"] == []


def _install_data_doubles(
    monkeypatch,
    *,
    columns: 'list[tuple[str, str]]',
    partition_columns: 'list[tuple[str, str]]' = (),
    download_rows: 'list[dict] | None' = None,
    download_table: 'str | None' = None,
    download_partition: 'str | None' = None,
):
    """Install FakeODPS + a fixed describe_table + optional download seed.

    Resets FakeTunnel class state so tests do not leak into each other.
    """
    import odps
    from maxc_cli.backend.meta import MetaMixin
    from maxc_cli.config import TableColumn, TableDefinition

    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    table_def = TableDefinition(
        name="proj.sch.tbl",
        description="",
        columns=[TableColumn(name=n, type=t) for n, t in columns],
        partition_columns=[TableColumn(name=n, type=t) for n, t in partition_columns],
    )
    monkeypatch.setattr(
        MetaMixin, "describe_table",
        lambda self, name, project=None: table_def,
    )

    # Reset class-level FakeTunnel state.
    FakeTunnel.last_upload_session = None
    FakeTunnel.download_rows = {}
    if download_rows is not None:
        key = (download_table or table_def.name, download_partition)
        FakeTunnel.download_rows[key] = [_FakeRecord(r) for r in download_rows]



def test_cli_data_upload_appends_csv_to_partitioned_table(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
        partition_columns=[("ds", "string")],
    )

    csv_path = tmp_path / "in.csv"
    csv_path.write_text("user_id,name\n1,alice\n2,bob\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path),
         "--partition", "ds=20260508",
         "--json"],
    )

    assert code == 0, payload
    assert payload["command"] == "data upload"
    assert payload["status"] == "success"
    assert payload["data"]["rows_written"] == 2
    assert payload["data"]["table"] == "proj.sch.tbl"
    assert payload["data"]["applied_partition"] == "ds=20260508"
    assert payload["data"]["overwrite"] is False
    assert payload["data"]["blocks"] == 1

    sess = FakeTunnel.last_upload_session
    assert sess.partition == "ds=20260508"
    assert sess.overwrite is False
    assert sess.committed_blocks == [0]
    [(_block_id, recs, _ow)] = sess._store[("proj.sch.tbl", "ds=20260508")]
    assert recs == [
        {"user_id": 1, "name": "alice"},
        {"user_id": 2, "name": "bob"},
    ]


def test_cli_data_upload_overwrite_partition(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("v", "bigint")],
        partition_columns=[("ds", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n42\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path),
         "--partition", "ds=20260508",
         "--overwrite", "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["overwrite"] is True
    assert FakeTunnel.last_upload_session.overwrite is True


def test_cli_data_upload_rejects_missing_partition_for_partitioned_table(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("v", "bigint")],
        partition_columns=[("ds", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n1\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "partition" in payload["error"]["message"].lower()


def test_cli_data_upload_rejects_unsupported_complex_type(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("a", "array<bigint>")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("a\n1\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code != 0
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "complex types" in payload["error"]["message"]


def test_cli_data_upload_fail_fast_on_bad_row_aborts_session(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(monkeypatch, columns=[("v", "bigint")])
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n1\nabc\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code != 0
    assert payload["error"]["code"] == "CSV_PARSE_ERROR"
    assert payload["error"]["context"]["line"] == 3
    assert payload["error"]["context"]["column"] == "v"
    sess = FakeTunnel.last_upload_session
    assert sess.aborted is True
    assert sess.committed_blocks == []


def test_cli_data_upload_no_header_uses_ordinal_mapping(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("1,alice\n2,bob\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--no-header", "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["rows_written"] == 2


def test_cli_data_upload_empty_file_commits_zero_rows(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(monkeypatch, columns=[("v", "bigint")])
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["rows_written"] == 0
    assert payload["data"]["blocks"] == 1
    assert FakeTunnel.last_upload_session.committed_blocks == [0]


def test_cli_data_upload_extra_header_columns_warning(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(monkeypatch, columns=[("v", "bigint")])
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v,extra\n1,ignored\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["rows_written"] == 1
    warnings = payload["data"]["warnings"]
    assert any("extra columns" in w for w in warnings), warnings


def test_cli_data_upload_rejects_unknown_partition_key(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("v", "bigint")],
        partition_columns=[("ds", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n1\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path),
         "--partition", "wrong=1", "--json"],
    )
    assert code != 0
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "wrong" in payload["error"]["message"]


def test_cli_data_download_writes_full_partition(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
        partition_columns=[("ds", "string")],
        download_rows=[{"user_id": 1, "name": "alice"}, {"user_id": 2, "name": "bob"}],
        download_partition="ds=20260508",
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--partition", "ds=20260508",
         "--json"],
    )

    assert code == 0, payload
    assert payload["command"] == "data download"
    assert payload["data"]["rows_written"] == 2
    assert payload["data"]["truncated"] is False
    assert payload["data"]["columns"] == ["user_id", "name"]
    assert payload["data"]["applied_partition"] == "ds=20260508"
    assert out.read_text(encoding="utf-8") == "user_id,name\n1,alice\n2,bob\n"


def test_cli_data_download_respects_limit_and_marks_truncated(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("v", "bigint")],
        partition_columns=[("ds", "string")],
        download_rows=[{"v": i} for i in range(10)],
        download_partition="ds=1",
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--partition", "ds=1",
         "--limit", "3", "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["rows_written"] == 3
    assert payload["data"]["truncated"] is True
    assert "limit reached" in payload["data"]["warnings"][0]


def test_cli_data_download_columns_subset_in_requested_order(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("a", "bigint"), ("b", "string"), ("c", "double")],
        download_rows=[{"a": 1, "b": "x", "c": 1.5}],
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, _payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--columns", "c,a", "--json"],
    )
    assert code == 0
    assert out.read_text(encoding="utf-8") == "c,a\n1.5,1\n"


def test_cli_data_download_rejects_unknown_column(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(monkeypatch, columns=[("a", "bigint")])
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--columns", "nope", "--json"],
    )
    assert code != 0
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "Unknown columns" in payload["error"]["message"]


def test_cli_data_download_null_marker_renders_none(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("a", "bigint"), ("b", "string")],
        download_rows=[{"a": None, "b": None}],
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, _payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--null-marker", r"\N", "--json"],
    )
    assert code == 0
    assert out.read_text(encoding="utf-8") == "a,b\n\\N,\\N\n"


# ============================================================
# Auto-redirect to `auth login` when no auth is configured
# ============================================================


def test_bare_maxc_with_auth_prints_help(tmp_path: 'Path', monkeypatch) -> None:
    """`maxc` with auth configured → prints help, no redirect."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump({
        "auth": {
            "access_id": "AK", "secret_access_key": "SK",
            "project": "p", "endpoint": "http://x/api",
        },
    }), encoding="utf-8")

    stdout = StringIO()
    stderr = StringIO()
    code = run(["--config", str(config_path)], cwd=tmp_path, stdout=stdout, stderr=stderr)
    assert code == 0
    assert "Usage:" in stdout.getvalue() and "maxc" in stdout.getvalue()


def test_bare_maxc_no_auth_non_tty_prints_help(tmp_path: 'Path', monkeypatch) -> None:
    """`maxc` with no auth and non-TTY stdin → prints help (no redirect)."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    stdout = StringIO()
    stderr = StringIO()
    code = run([], cwd=tmp_path, stdout=stdout, stderr=stderr)
    assert code == 0
    assert "Usage:" in stdout.getvalue() and "maxc" in stdout.getvalue()


def test_bare_maxc_no_auth_tty_redirects_to_login(tmp_path: 'Path', monkeypatch) -> None:
    """`maxc` with no auth and TTY → triggers auth login."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    from maxc_cli import catalog_bootstrap as cb
    import maxc_cli.app as app_module
    monkeypatch.setattr(cb, "build_bootstrap_odps", lambda **kw: object())
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: [cb.ProjectInfo("auto_proj", "cn-shanghai", "ALIYUN$x", True, "")],
    )
    # Skip remote validation
    monkeypatch.setattr(app_module, "resolve_auth_connection", lambda *a, **kw: None)
    monkeypatch.setattr(
        app_module.MaxCApp, "_validate_auth_config",
        lambda self, auth: (
            {"authenticated": True, "configured": True, "validation_status": "ok",
             "backend": "odps", "auth_type": "access_key", "identity_source": "config_file",
             "principal_display": "AK", "principal_masked": "AK",
             "project": auth.project, "region": auth.region_name, "endpoint": auth.endpoint,
             "project_owner": None, "allowed_operations": [],
             "saved": True, "validated": True},
            [],
        ),
    )
    inputs = iter(["AK_AUTO", "1"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs, ""))
    monkeypatch.setattr("getpass.getpass", lambda _prompt="": "SK_AUTO")

    stdout = StringIO()
    stderr = StringIO()
    config_path = tmp_path / "redir.yaml"
    code = run(
        ["--config", str(config_path)],
        cwd=tmp_path, stdout=stdout, stderr=stderr,
    )
    assert code == 0, f"stderr={stderr.getvalue()}\nstdout={stdout.getvalue()}"
    assert config_path.exists()
    written = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert written["auth"]["access_id"] == "AK_AUTO"
    assert "auth login" in stderr.getvalue() or "未配置认证" in stderr.getvalue()


def test_query_no_auth_non_tty_no_redirect(tmp_path: 'Path', monkeypatch) -> None:
    """`maxc query` with no auth and non-TTY → original VALIDATION_ERROR, no infinite redirect."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    config_path = tmp_path / "empty.yaml"
    config_path.write_text("auth: {}\n", encoding="utf-8")

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["query", "SELECT 1", "--json"],
    )
    assert code != 0
    assert payload["status"] == "failure"


def test_session_show_no_auth_no_redirect(tmp_path: 'Path', monkeypatch) -> None:
    """`maxc session show` is exempt from auto-redirect even without auth."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    from maxc_cli import catalog_bootstrap as cb
    catalog_called = []
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: catalog_called.append(1) or [],
    )

    config_path = tmp_path / "empty.yaml"
    config_path.write_text("auth: {}\n", encoding="utf-8")

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["session", "show", "--json"],
    )
    assert code == 0
    assert catalog_called == []  # no redirect happened


def test_auth_login_no_recursion(tmp_path: 'Path', monkeypatch) -> None:
    """`maxc auth login` does not auto-redirect to itself (would infinite loop)."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    config_path = tmp_path / "login.yaml"
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--project", "p", "--endpoint", "http://x/api",
            "--no-picker", "--no-validate", "--json",
        ],
    )
    assert code == 0
    assert payload["status"] == "success"


def test_query_no_auth_tty_redirects_then_runs(tmp_path: 'Path', monkeypatch) -> None:
    """`maxc query` without auth + TTY → login → query runs."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    from maxc_cli import catalog_bootstrap as cb
    import maxc_cli.app as app_module
    monkeypatch.setattr(cb, "build_bootstrap_odps", lambda **kw: object())
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: [cb.ProjectInfo("after_login_proj", "cn-shanghai", "ALIYUN$x", True, "")],
    )
    # Skip remote validation during auth login
    monkeypatch.setattr(app_module, "resolve_auth_connection", lambda *a, **kw: None)
    monkeypatch.setattr(
        app_module.MaxCApp, "_validate_auth_config",
        lambda self, auth: (
            {"authenticated": True, "configured": True, "validation_status": "ok",
             "backend": "odps", "auth_type": "access_key", "identity_source": "config_file",
             "principal_display": "AK", "principal_masked": "AK",
             "project": auth.project, "region": auth.region_name, "endpoint": auth.endpoint,
             "project_owner": None, "allowed_operations": [],
             "saved": True, "validated": True},
            [],
        ),
    )
    inputs = iter(["AK_X", "1"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs, ""))
    monkeypatch.setattr("getpass.getpass", lambda _prompt="": "SK_X")

    # Stub the actual query backend so the re-executed query call returns success
    import maxc_cli.backend as backend_module
    monkeypatch.setattr(backend_module, "OdpsBackend", lambda *a, **kw: _StubBackend())
    import maxc_cli.app as _app_for_backend
    monkeypatch.setattr(_app_for_backend, "OdpsBackend", lambda *a, **kw: _StubBackend())

    config_path = tmp_path / "qredir.yaml"
    stdout = StringIO()
    stderr = StringIO()
    code = run(
        ["--config", str(config_path), "query", "SELECT 1", "--json"],
        cwd=tmp_path, stdout=stdout, stderr=stderr,
    )
    # query runs after login
    assert config_path.exists()
    payload = json.loads(stdout.getvalue())
    assert payload["status"] == "success", (
        f"code={code}\nstdout={stdout.getvalue()}\nstderr={stderr.getvalue()}\npayload={payload}"
    )
    assert payload["command"] == "query"


class _StubBackend:
    """Minimal backend stub for the redirect re-run test."""
    supports_remote_jobs = False

    def __init__(self, *a, **kw): pass
    def execute_query(self, *a, **kw):
        from maxc_cli.models import QueryResult
        return QueryResult(
            rows=[{"_c0": 1}],
            schema=[{"name": "_c0", "type": "bigint"}],
            total_rows=1, returned_rows=1, has_more=False, next_cursor=None,
            elapsed_ms=1, bytes_scanned=None,
            project="p", sql_executed="SELECT 1", tables_used=[], job_id="j",
        )
    def estimate_query_cost(self, *a, **kw):
        return {"input_size_bytes": 0, "estimated_cu": 0.0}

