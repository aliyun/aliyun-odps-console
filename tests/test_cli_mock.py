"""Tests for maxc-cli using FakeODPS mock.

These tests use FakeODPS to mock the ODPS client, allowing testing of
authentication and configuration flows without a real MaxCompute connection.
"""

import json
import shlex
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
        self.committed_blocks: list[int] = []
        self.aborted = False

    def new_record(self):
        return _FakeRecord()

    def open_record_writer(self, block_id: int):
        records: list[dict] = []
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
    # Records the project the backend asked the tunnel to operate on, set by
    # OdpsBackend._table_tunnel(project=...). None until a session is created.
    last_session_project: 'str | None' = None

    def __init__(self):
        self.upload_store: dict[tuple, list] = {}
        self.requested_project = None

    def create_upload_session(
        self, table, partition_spec=None, overwrite=False, create_partition=False,
        schema=None,
    ):
        sess = FakeUploadSession(table, partition_spec, overwrite, self.upload_store)
        sess.create_partition = create_partition
        sess.schema = schema
        FakeTunnel.last_upload_session = sess
        FakeTunnel.last_session_project = self.requested_project
        return sess

    def create_download_session(self, table, partition_spec=None, schema=None):
        rows = FakeTunnel.download_rows.get((table, partition_spec), [])
        sess = FakeDownloadSession(table, partition_spec, rows)
        sess.schema = schema
        FakeTunnel.last_session_project = self.requested_project
        return sess


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


def test_auth_login_failed_validation_preserves_existing_config(
    tmp_path: 'Path',
    monkeypatch,
) -> 'None':
    """A rejected replacement login must not destroy a working identity."""
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import BackendConnectionError

    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config_path = tmp_path / "login.yaml"
    original_text = (
        "default_project: working_project\n"
        "auth:\n"
        "  provider: access_key\n"
        "  access_id: WORKING_ID\n"
        "  secret_access_key: WORKING_SECRET\n"
        "  project: working_project\n"
        "  endpoint: http://working.example/api\n"
    )
    config_path.write_text(original_text, encoding="utf-8")
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    monkeypatch.setattr(
        app,
        "_validate_auth_config",
        lambda _auth: (_ for _ in ()).throw(
            BackendConnectionError("replacement credentials were rejected")
        ),
    )

    with pytest.raises(BackendConnectionError, match="replacement credentials"):
        app.auth_login(
            access_id="BAD_ID",
            secret_access_key="BAD_SECRET",
            project="bad_project",
            endpoint="http://bad.example/api",
            no_picker=True,
            target_config_path=config_path,
        )

    assert config_path.read_text(encoding="utf-8") == original_text


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
    assert payload["data"]["auth_options"][0]["type"] == "oauth"
    assert any(
        option["type"] == "access_key" for option in payload["data"]["auth_options"]
    )


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


def test_auth_login_non_tty_returns_pending_project_list(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """Non-TTY + no --project returns pending envelope with project list."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    monkeypatch.setattr(cb, "build_bootstrap_odps", lambda **kw: object())
    monkeypatch.setattr(
        cb, "list_all_projects",
        lambda odps: [
            cb.ProjectInfo("proj_a_dev", "cn-hangzhou", "ALIYUN$x", True, "desc a"),
            cb.ProjectInfo("proj_b_dev", "cn-shanghai", "ALIYUN$y", False, "desc b"),
        ],
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

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
    assert payload["status"] == "pending"
    identity = payload["data"]["identity"]
    assert identity["reason"] == "project_selection_required"
    projects = identity["projects"]
    assert len(projects) == 2
    assert projects[0]["project_id"] == "proj_a_dev"
    assert projects[0]["region"] == "cn-hangzhou"
    assert "cn-hangzhou" in projects[0]["endpoint"]
    assert projects[1]["project_id"] == "proj_b_dev"
    assert identity["count"] == 2
    actions = payload["agent_hints"]["actions"]
    assert len(actions) == 2
    selected = actions[0]
    assert selected["executable"] is False
    assert selected["confirmation_required"] is True
    assert selected["agent_allowed"] is False
    assert "AK" not in selected["command"]
    assert "SK" not in selected["command"]
    action_argv = shlex.split(selected["command"])
    assert action_argv[0] == "maxc"
    assert "--login-continuation" in action_argv
    assert action_argv[action_argv.index("--project") + 1] == "proj_a_dev"

    resumed_stdout = StringIO()
    resumed_stderr = StringIO()
    resumed_code = run(
        action_argv[1:],
        cwd=tmp_path,
        stdout=resumed_stdout,
        stderr=resumed_stderr,
    )
    resumed = json.loads(resumed_stdout.getvalue())
    assert resumed_code == 0, resumed_stderr.getvalue()
    assert resumed["status"] == "success"
    assert resumed["data"]["identity"]["project"] == "proj_a_dev"


def test_auth_login_non_tty_catalog_failure_falls_through(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """Non-TTY + catalog failure falls through to existing behavior (validation error)."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    def _boom(**kw):
        raise RuntimeError("catalog unreachable")
    monkeypatch.setattr(cb, "build_bootstrap_odps", _boom)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

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
    # Falls through — project is None, validation fails
    assert code == 1
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"


def test_auth_login_non_tty_no_picker_skips_catalog(
    tmp_path: 'Path', monkeypatch,
) -> None:
    """Non-TTY + --no-picker must NOT try the catalog listing."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    from maxc_cli import catalog_bootstrap as cb

    called = []
    monkeypatch.setattr(
        cb, "build_bootstrap_odps",
        lambda **kw: (called.append(1) or object()),
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    config_path = tmp_path / "login.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "AK", "--access-key-secret", "SK",
            "--endpoint", "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-picker",
            "--no-validate", "--json",
        ],
    )
    # --no-picker: catalog never tried, project=None → validation fails
    assert called == []
    assert payload["status"] == "failure"


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


def test_cache_status_works_without_auth(tmp_path: 'Path', monkeypatch) -> 'None':
    """cache.status only reads the local SQLite cache; it must not require auth."""
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

    assert code == 0
    assert payload["command"] == "cache status"
    assert payload["status"] == "success"
    assert "table_count" in payload["data"]


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

def test_legacy_session_override_is_migrated_only_on_declared_config_write(
    tmp_path: 'Path', monkeypatch
) -> None:
    """Read paths preserve legacy values; an explicit write performs migration."""
    from maxc_cli.config import load_config, migrate_legacy_session_override

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
    assert override.exists()
    assert not (maxc_dir / ".session_override_migrated").exists()

    migrate_legacy_session_override()

    assert not override.exists(), "legacy session_override.yaml should be removed after migration"
    new_global = yaml.safe_load(global_config.read_text(encoding="utf-8"))
    assert new_global["default_project"] == "legacy_proj"
    assert new_global["default_schema"] == "legacy_schema"


def test_agent_context_reads_legacy_session_without_migrating_it(
    tmp_path: 'Path', monkeypatch
) -> None:
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    maxc_dir = tmp_path / ".maxc"
    maxc_dir.mkdir(mode=0o700)
    config_path = maxc_dir / "config.yaml"
    config_path.write_text("default_project: old_default\n", encoding="utf-8")
    config_path.chmod(0o600)
    override = maxc_dir / "session_override.yaml"
    override.write_text("project: legacy_proj\n", encoding="utf-8")
    override.chmod(0o600)
    before = {
        path.name: (path.read_bytes(), path.stat().st_mode & 0o777)
        for path in maxc_dir.iterdir()
    }

    code, payload, _ = run_json_command(
        tmp_path,
        None,
        ["agent", "context", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    assert {
        path.name: (path.read_bytes(), path.stat().st_mode & 0o777)
        for path in maxc_dir.iterdir()
    } == before
    assert not (maxc_dir / ".session_override_migrated").exists()


def test_session_set_writes_to_global_config(tmp_path: 'Path', monkeypatch) -> None:
    """When --config is NOT passed, session set persists to ~/.maxc/config.yaml."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    # No --config: rely on normal discovery so session set hits the global path.
    code, payload, _ = run_json_command(
        tmp_path, None,
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


def test_session_set_writes_to_explicit_config_when_passed(tmp_path: 'Path', monkeypatch) -> None:
    """When --config is passed, session set writes to THAT file (not global), so
    a subsequent `session show --config <same>` round-trips."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "explicit.yaml"
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

    persisted = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert persisted["default_project"] == "new_proj"
    assert persisted["default_schema"] == "new_schema"
    # Global file must NOT be touched when --config is explicit.
    assert not (tmp_path / ".maxc" / "config.yaml").exists()

    # Round-trip: session show via same --config sees the new value.
    code, show_payload, _ = run_json_command(
        tmp_path, config_path, ["session", "show", "--json"]
    )
    assert code == 0
    assert show_payload["data"]["project"]["value"] == "new_proj"
    assert show_payload["data"]["schema"]["value"] == "new_schema"


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


def test_auto_discovered_workspace_config_cannot_activate_external_auth(
    tmp_path: 'Path',
    monkeypatch,
) -> None:
    clear_odps_env(monkeypatch)
    home_dir = tmp_path / "home"
    work_dir = tmp_path / "untrusted-repository"
    home_dir.mkdir()
    (work_dir / ".maxc").mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home_dir))
    marker = tmp_path / "credential-helper-executed"
    (work_dir / ".maxc" / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "auth": {
                    "provider": "external",
                    "project": "attacker_project",
                    "endpoint": "http://127.0.0.1:9/api",
                    "external": {"process_command": f"/usr/bin/touch {marker}"},
                }
            }
        ),
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        work_dir,
        None,
        ["auth", "whoami", "--json"],
    )

    assert code == 1
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "workspace config cannot define `auth`" in payload["error"]["message"]
    assert not marker.exists()


def test_session_override_file_is_no_longer_consulted(tmp_path: 'Path', monkeypatch) -> None:
    """After migration is recorded, a stale override must not influence config."""
    from maxc_cli.config import load_config

    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    maxc_dir = tmp_path / ".maxc"
    maxc_dir.mkdir(parents=True)
    (maxc_dir / "config.yaml").write_text("default_project: from_config\n", encoding="utf-8")

    # Simulate a release that already migrated a real legacy override. Fresh
    # installs no longer create this marker just by reading configuration.
    (maxc_dir / ".session_override_migrated").touch()
    (maxc_dir / "session_override.yaml").write_text("project: should_be_ignored\n", encoding="utf-8")

    cfg = load_config(cwd=tmp_path)
    assert cfg.default_project == "from_config", (
        "session_override.yaml must no longer influence load_config; "
        f"got {cfg.default_project!r}"
    )
    assert (maxc_dir / "session_override.yaml").exists(), (
        "A read-only config load must not delete stale legacy state."
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

        def _raises(*args, **kwargs):
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
            # Real ODPS raises NoSuchObject from these on a missing table —
            # the fake models that so meta.py's narrowed `except ODPSError`
            # path swallows them as best-effort empty results, matching prod.
            "head": _raises,
            "iterate_partitions": _raises,
            "get_max_partition": _raises,
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
        tmp_path, config_path,
        ["data", "profile", "nonexistent_table", "--schema", "default", "--json"],
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


class _JobReloadNoSuchObjectODPS(FakeODPS):
    """Mock ODPS client whose instance.reload() raises NoSuchObject.

    Mirrors the real-world failure mode where the ODPS server has purged
    a job ID (or it never existed): get_instance() returns lazily but the
    first reload() call surfaces the not-found error.
    """

    def get_instance(self, job_id, *, project=None):
        try:
            from odps.errors import NoSuchObject
        except ImportError:
            pytest.skip("odps package not installed")

        class _ExplodingInstance:
            id = job_id

            def reload(self_inner, blocking=False):
                raise NoSuchObject(f"Job not found: {job_id}")

            def get_sql_query(self_inner):
                raise NoSuchObject(f"Job not found: {job_id}")

            def get_logview_address(self_inner, *a, **kw):
                raise NoSuchObject(f"Job not found: {job_id}")

            def get_task_statuses(self_inner):
                raise NoSuchObject(f"Job not found: {job_id}")

            @property
            def status(self_inner):
                return ""

            @property
            def start_time(self_inner):
                return None

            @property
            def end_time(self_inner):
                return None

        return _ExplodingInstance()


def test_job_get_surfaces_nosuchobject_not_unknown(
    tmp_path: 'Path', monkeypatch
) -> None:
    """instance.reload() raising NoSuchObject must surface as a NOT_FOUND
    envelope, not be silently swallowed into a phony job status.

    Regression guard for: _instance_to_job_info used to wrap reload() in a
    bare ``except Exception: pass``, masking 'job not found' into a JobInfo
    with status='pending' (and a 'success' envelope).
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _JobReloadNoSuchObjectODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path, ["job", "status", "20260521abc", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] in (
        "NOT_FOUND", "TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND",
    )
    assert "not found" in payload["error"]["message"].lower()


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
        # minimal stub for describe: tables exist with empty schema, no rows,
        # no partitions. The head/iterate/max methods now need to be present
        # because meta.py's best-effort helpers no longer swallow AttributeError.
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
            "head": lambda *a, **k: iter([]),
            "iterate_partitions": lambda *a, **k: iter([]),
            "get_max_partition": lambda *a, **k: None,
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


def test_cache_build_routes_project_and_schema_end_to_end(tmp_path, monkeypatch):
    """cache build --project/--schema must scope BOTH the backend fetch and
    the cache write key.

    Regression: list_tables/describe_table ignored --project (read the default
    project) and the cache write always used schema_name="default", so
    `cache build --project X --schema S` populated the wrong namespace with the
    default project's tables, and `cache status --project X --schema S` found
    nothing.
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps

    from maxc_cli.backend.meta import MetaMixin
    from maxc_cli.config import TableColumn, TableDefinition

    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    calls: dict = {"list": [], "describe": []}

    def fake_list_tables(self, *, schema=None, project=None):
        calls["list"].append((project, schema))
        tables = [type("T", (), {"name": n})() for n in ("tbl_a", "tbl_b")]
        return tables, False

    def fake_describe(self, name, *, project=None, schema=None):
        calls["describe"].append((name, project, schema))
        return TableDefinition(
            name=name, description="",
            columns=[TableColumn(name="c", type="bigint")],
            partition_columns=[],
        )

    monkeypatch.setattr(MetaMixin, "list_tables", fake_list_tables)
    monkeypatch.setattr(MetaMixin, "describe_table", fake_describe)
    monkeypatch.setattr(MetaMixin, "describe_table_metadata", fake_describe)

    config_path = _make_config_with_odps(tmp_path)  # default project = test_project

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["cache", "build", "--project", "other_proj", "--schema", "myschema", "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["cached_tables"] == 2

    # Backend fetch was scoped to the explicit project AND schema.
    assert calls["list"] == [("other_proj", "myschema")]
    assert calls["describe"], "describe_table was never called"
    assert all(p == "other_proj" and s == "myschema" for (_n, p, s) in calls["describe"])

    # Cache rows landed under (other_proj, myschema) — not the default schema.
    code2, status_payload, _ = run_json_command(
        tmp_path, config_path,
        ["cache", "status", "--project", "other_proj", "--schema", "myschema", "--json"],
    )
    assert code2 == 0, status_payload
    assert status_payload["data"]["table_count"] == 2

    # And nothing leaked into the default schema for this project.
    code3, default_payload, _ = run_json_command(
        tmp_path, config_path,
        ["cache", "status", "--project", "other_proj", "--schema", "default", "--json"],
    )
    assert code3 == 0, default_payload
    assert default_payload["data"]["table_count"] == 0


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


def test_catalog_search_tables_scopes_to_explicit_project():
    """catalog_search_tables(project=X) must scope the REST query to X.

    Regression: the Catalog path hard-coded config.default_project, so
    `meta search --project other_proj` silently searched the default project
    (the Catalog path takes priority over cache/live fallbacks).
    """
    from maxc_cli.backend.odps import OdpsBackend

    captured: dict = {}

    class _FakeResp:
        text = '{"entries": []}'

    class _FakeCatalogRest:
        endpoint = "http://catalog.example/api"

        def request(self, url, method, params=None, curr_project=None):
            captured["query"] = params["query"]
            captured["curr_project"] = curr_project
            return _FakeResp()

    backend = OdpsBackend.__new__(OdpsBackend)
    backend.config = type("C", (), {"default_project": "default_proj"})()
    backend._catalog_rest_cached = _FakeCatalogRest()
    backend._tenant_id_cached = "tenant123"

    result = backend.catalog_search_tables("frpm", project="other_proj")

    assert result == []
    assert "project=other_proj" in captured["query"]
    assert "project=default_proj" not in captured["query"]
    assert captured["curr_project"] == "other_proj"


def test_catalog_search_tables_defaults_to_config_project():
    """Without an explicit project, the Catalog query falls back to default."""
    from maxc_cli.backend.odps import OdpsBackend

    captured: dict = {}

    class _FakeResp:
        text = '{"entries": []}'

    class _FakeCatalogRest:
        endpoint = "http://catalog.example/api"

        def request(self, url, method, params=None, curr_project=None):
            captured["query"] = params["query"]
            return _FakeResp()

    backend = OdpsBackend.__new__(OdpsBackend)
    backend.config = type("C", (), {"default_project": "default_proj"})()
    backend._catalog_rest_cached = _FakeCatalogRest()
    backend._tenant_id_cached = "tenant123"

    backend.catalog_search_tables("frpm")

    assert "project=default_proj" in captured["query"]


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
        lambda self, name, project=None, schema=None: table_def,
    )

    # Reset class-level FakeTunnel state.
    FakeTunnel.last_upload_session = None
    FakeTunnel.last_session_project = None
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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


def test_cli_data_upload_dry_run_reports_exact_validation_scope(
    tmp_path, monkeypatch
):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("user_id,name\n1,alice\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "data",
            "upload",
            "proj.sch.tbl",
            "--file",
            str(csv_path),
            "--dry-run",
            "--json",
        ],
    )

    assert code == 0, payload
    assert payload["data"]["rows_found"] == 1
    assert payload["data"]["validation"] == {
        "table_schema": True,
        "csv_structure": True,
        "row_widths": True,
        "mapped_value_types": True,
        "upload_session_created": False,
    }
    assert FakeTunnel.last_upload_session is None
    [insight] = payload["agent_hints"]["insights"]
    assert "every CSV row width" in insight
    assert "without creating an upload session" in insight


def test_cli_data_upload_dry_run_replay_action_preserves_verified_options(
    tmp_path, monkeypatch
):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
        partition_columns=[("ds", "string")],
    )
    csv_path = tmp_path / "in.tsv"
    csv_path.write_text("1|NULL\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "data", "upload", "proj.sch.tbl",
            "--file", str(csv_path),
            "--partition", "ds=20260508",
            "--create-partition",
            "--overwrite",
            "--delimiter", "|",
            "--no-header",
            "--null-marker", "NULL",
            "--block-size", "7",
            "--dry-run",
            "--json",
        ],
    )

    assert code == 0, payload
    [replay] = payload["agent_hints"]["actions"]
    assert replay["id"] == "data.upload"
    assert replay["executable"] is False
    assert replay["effect"] == "remote_write"
    assert replay["confirmation_required"] is True
    assert replay["agent_allowed"] is False
    assert str(csv_path) in replay["command"]
    for expected in (
        "--partition ds=20260508",
        "--create-partition",
        "--overwrite",
        "--delimiter '|'",
        "--no-header",
        "--null-marker NULL",
        "--block-size 7",
        "--project proj",
        "--schema sch",
    ):
        assert expected in replay["command"]
    assert "--dry-run" not in replay["command"]
    assert "next_actions" not in payload["agent_hints"]


def test_cli_data_upload_rejects_missing_partition_for_partitioned_table(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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


def test_cli_data_upload_bad_row_is_rejected_before_session_creation(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    assert FakeTunnel.last_upload_session is None


def test_cli_data_upload_no_header_uses_ordinal_mapping(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
        ["data", "download", "sch.tbl",
         "--output", str(out),
         "--partition", "ds=20260508",
         "--schema", "sch",
         "--json"],
    )

    assert code == 0, payload
    assert payload["command"] == "data download"
    assert payload["data"]["rows_written"] == 2
    assert payload["data"]["truncated"] is False
    assert payload["data"]["columns"] == ["user_id", "name"]
    assert payload["data"]["applied_partition"] == "ds=20260508"
    assert out.read_text(encoding="utf-8") == "user_id,name\n1,alice\n2,bob\n"


def test_cli_data_download_routes_tunnel_to_explicit_project(tmp_path, monkeypatch):
    """--project must reach the tunnel session, not just the metadata lookup.

    Regression: tunnel was built against the client's default project, so
    `data download --project X` silently read from the default project.
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
        partition_columns=[("ds", "string")],
        download_rows=[{"user_id": 1, "name": "alice"}],
        download_partition="ds=20260508",
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)  # default project = test_project

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "sch.tbl",
         "--output", str(out),
         "--partition", "ds=20260508",
         "--project", "other_proj",
         "--json"],
    )

    assert code == 0, payload
    assert FakeTunnel.last_session_project == "other_proj"


def test_cli_data_upload_routes_tunnel_to_explicit_project(tmp_path, monkeypatch):
    """--project must reach the upload tunnel session, not just metadata."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
        partition_columns=[("ds", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("user_id,name\n1,alice\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)  # default project = test_project

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "sch.tbl",
         "--file", str(csv_path),
         "--partition", "ds=20260508",
         "--project", "other_proj",
         "--json"],
    )

    assert code == 0, payload
    assert FakeTunnel.last_session_project == "other_proj"


def test_cli_data_download_respects_limit_and_marks_truncated(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
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
    """`maxc` with no auth and TTY → triggers OAuth-first login."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    import maxc_cli.app as app_module
    import maxc_cli.oauth as oauth_module
    from maxc_cli import catalog_bootstrap as cb
    monkeypatch.setattr(
        oauth_module,
        "start_oauth_flow",
        lambda *a, **kw: oauth_module.OAuthTokens("AT", "RT", 9_999_999_999),
    )
    monkeypatch.setattr(
        oauth_module,
        "exchange_sts",
        lambda *a, **kw: oauth_module.StsCredential(
            "STS.AUTO", "STS.SECRET", "STS.TOKEN", "2099-01-01T00:00:00Z"
        ),
    )
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
    inputs = iter(["1"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs, ""))

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
    assert written["auth"]["provider"] == "oauth"
    assert written["auth"]["oauth"]["refresh_token"] == "RT"
    assert "auth login --oauth" in stderr.getvalue()


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
    """`maxc query` without auth + TTY → OAuth login → query runs."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)

    import maxc_cli.app as app_module
    import maxc_cli.oauth as oauth_module
    from maxc_cli import catalog_bootstrap as cb
    monkeypatch.setattr(
        oauth_module,
        "start_oauth_flow",
        lambda *a, **kw: oauth_module.OAuthTokens("AT", "RT", 9_999_999_999),
    )
    monkeypatch.setattr(
        oauth_module,
        "exchange_sts",
        lambda *a, **kw: oauth_module.StsCredential(
            "STS.QUERY", "STS.SECRET", "STS.TOKEN", "2099-01-01T00:00:00Z"
        ),
    )
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
    inputs = iter(["1"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs, ""))

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


class _RecordingMcqaBackend:
    supports_remote_jobs = False

    def __init__(self, *a, **kw):
        self.execute_query_calls: list[dict[str, object]] = []
        self.submit_query_calls: list[dict[str, object]] = []
        self.cancel_job_calls: list[dict[str, object]] = []

    def execute_query(self, sql, *, project, max_rows, dry_run, offset=0, timeout=None, force=False, execution_settings=None):
        from maxc_cli.models import QueryResult
        self.execute_query_calls.append({
            "sql": sql,
            "project": project,
            "max_rows": max_rows,
            "dry_run": dry_run,
            "offset": offset,
            "timeout": timeout,
            "force": force,
            "execution_settings": execution_settings,
        })
        return QueryResult(
            rows=[{"_c0": 1}],
            schema=[{"name": "_c0", "type": "bigint"}],
            total_rows=1,
            returned_rows=1,
            has_more=False,
            next_cursor=None,
            elapsed_ms=1,
            bytes_scanned=None,
            project=project,
            sql_executed=sql,
            tables_used=[],
            job_id="job_mcqa",
            extra_metadata={
                "execution_requested": getattr(execution_settings, "requested_mode", "offline") if execution_settings else "offline",
                "execution_mode": getattr(execution_settings, "requested_mode", "offline") if execution_settings else "offline",
                "mcqa_fallback_enabled": getattr(execution_settings, "fallback", False) if execution_settings else False,
                "mcqa_fallback_used": False,
                "mcqa_quota_name": getattr(execution_settings, "quota_name", None) if execution_settings else None,
            },
        )

    def submit_query(self, sql, *, project, idempotency_key=None, force=False, execution_settings=None):
        from maxc_cli.models import JobInfo
        self.submit_query_calls.append({
            "sql": sql,
            "project": project,
            "idempotency_key": idempotency_key,
            "force": force,
            "execution_settings": execution_settings,
        })
        return JobInfo(
            job_id="job_mcqa_submit",
            status="pending",
            project=project,
            progress=0,
            sql=sql,
            submitted_at="2026-06-23T00:00:00Z",
            updated_at="2026-06-23T00:00:00Z",
            logview=None,
            warnings=[],
        )

    def cancel_job(self, job_id, *, project=None):
        from maxc_cli.models import JobInfo

        self.cancel_job_calls.append({
            "job_id": job_id,
            "project": project,
        })
        return JobInfo(
            job_id=job_id,
            status="success",
            project=project or "proj",
            progress=0,
            sql="SELECT 1",
            submitted_at="2026-06-23T00:00:00Z",
            updated_at="2026-06-23T00:00:01Z",
            logview=None,
            warnings=[],
        )


def test_parse_job_id_accepts_plain_instance_id():
    from maxc_cli.job_ids import parse_job_id

    parsed = parse_job_id("20260625123051222giksgsz8mo1")

    assert parsed.instance_id == "20260625123051222giksgsz8mo1"
    assert parsed.subquery_id is None



def test_parse_job_id_accepts_composite_mcqa_id_and_trims_outer_whitespace():
    from maxc_cli.job_ids import parse_job_id

    parsed = parse_job_id("  20260626083225488ghuj8l7k6ym@7\t")

    assert parsed.instance_id == "20260626083225488ghuj8l7k6ym"
    assert parsed.subquery_id == 7



def test_parse_job_id_rejects_invalid_composite_forms():
    from maxc_cli.exceptions import ValidationError
    from maxc_cli.job_ids import parse_job_id

    for raw in ["abc @1", "abc@ 1", "@0", "abc@", "abc@x", "abc@1@2"]:
        with pytest.raises(ValidationError, match=r"<instance-id>@<subquery-id>"):
            parse_job_id(raw)



def test_format_job_id_preserves_plain_and_composite_forms():
    from maxc_cli.job_ids import format_job_id

    assert format_job_id("20260625123051222giksgsz8mo1", None) == "20260625123051222giksgsz8mo1"
    assert format_job_id("20260626083225488ghuj8l7k6ym", 7) == "20260626083225488ghuj8l7k6ym@7"



def test_query_parser_accepts_mcqa_v1_shorthand_flag():
    from maxc_cli.cli import build_parser

    parser = build_parser()
    args = parser.parse_args([
        "query",
        "--mcqa",
        "--no-mcqa-fallback",
        "SELECT 1",
    ])

    assert args.mcqa is True
    assert args.maxqa is False
    assert args.no_mcqa is False
    assert args.mcqa_version is None
    assert args.quota is None
    assert args.mcqa_fallback is False



def test_query_parser_accepts_maxqa_v2_shorthand_flag():
    from maxc_cli.cli import build_parser

    parser = build_parser()
    args = parser.parse_args([
        "query",
        "--maxqa",
        "--quota", "fast_quota",
        "SELECT 1",
    ])

    assert args.mcqa is None
    assert args.maxqa is True
    assert args.no_mcqa is False
    assert args.mcqa_version is None
    assert args.quota == "fast_quota"



def test_job_submit_parser_accepts_mcqa_and_maxqa_flags():
    from maxc_cli.cli import build_parser

    parser = build_parser()
    mcqa_args = parser.parse_args([
        "job", "submit",
        "--mcqa",
        "SELECT 1",
    ])
    maxqa_args = parser.parse_args([
        "job", "submit",
        "--maxqa",
        "--quota", "fast_quota",
        "SELECT 1",
    ])

    assert mcqa_args.mcqa is True
    assert mcqa_args.maxqa is False
    assert mcqa_args.no_mcqa is False
    assert mcqa_args.mcqa_version is None
    assert mcqa_args.quota is None
    assert mcqa_args.mcqa_fallback is None

    assert maxqa_args.mcqa is None
    assert maxqa_args.maxqa is True
    assert maxqa_args.no_mcqa is False
    assert maxqa_args.mcqa_version is None
    assert maxqa_args.quota == "fast_quota"


@pytest.mark.parametrize(
    ("flag", "expected"),
    [("--mcqa-fallback", True), ("--no-mcqa-fallback", False)],
)
def test_job_submit_parser_retains_hidden_fallback_compatibility(flag, expected):
    from maxc_cli.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["job", "submit", "--mcqa", flag, "SELECT 1"])

    assert args.mcqa_fallback is expected


def test_negative_mcqa_fallback_does_not_enable_mcqa_or_require_quota(
    tmp_path: 'Path', monkeypatch
):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {"enabled": False, "version": "v2"},
        }, sort_keys=False),
        encoding="utf-8",
    )
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)

    settings = app._resolve_mcqa_settings(
        command="query",
        mcqa_fallback=False,
    )

    assert settings.enabled is False
    assert settings.requested_mode == "offline"
    assert settings.quota_name is None



def test_load_config_reads_mcqa_defaults(tmp_path: 'Path', monkeypatch):
    from maxc_cli.config import load_config

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {
                "enabled": True,
                "version": "v2",
                "quota_name": "fast_quota",
                "fallback": False,
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    cfg = load_config(tmp_path, config_path)

    assert cfg.mcqa.enabled is True
    assert cfg.mcqa.version == "v2"
    assert cfg.mcqa.quota_name == "fast_quota"
    assert cfg.mcqa.fallback is False



def test_query_mcqa_flag_defaults_to_v1_even_if_config_prefers_v2(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {
                "enabled": False,
                "version": "v2",
                "quota_name": "fast_quota",
                "fallback": True,
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(
        command="query",
        sql="SELECT 1",
        mcqa=True,
    )

    call = backend.execute_query_calls[-1]
    execution_settings = call["execution_settings"]
    assert execution_settings.enabled is True
    assert execution_settings.version == "v1"
    assert execution_settings.quota_name is None
    assert execution_settings.requested_mode == "mcqa_v1"
    assert envelope.metadata["execution_requested"] == "mcqa_v1"
    assert envelope.metadata["mcqa_quota_name"] is None



def test_query_maxqa_flag_defaults_to_v2_even_if_config_prefers_v1(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {
                "enabled": False,
                "version": "v1",
                "quota_name": "fast_quota",
                "fallback": True,
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(
        command="query",
        sql="SELECT 1",
        maxqa=True,
    )

    call = backend.execute_query_calls[-1]
    execution_settings = call["execution_settings"]
    assert execution_settings.enabled is True
    assert execution_settings.version == "v2"
    assert execution_settings.quota_name == "fast_quota"
    assert execution_settings.requested_mode == "mcqa_v2"
    assert envelope.metadata["execution_requested"] == "mcqa_v2"
    assert envelope.metadata["mcqa_quota_name"] == "fast_quota"



def test_query_mcqa_flags_override_config_defaults(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {
                "enabled": False,
                "version": "v1",
                "fallback": True,
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(
        command="query",
        sql="SELECT 1",
        mcqa=True,
        mcqa_version="v2",
        quota="fast_quota",
        mcqa_fallback=False,
    )

    call = backend.execute_query_calls[-1]
    execution_settings = call["execution_settings"]
    assert execution_settings.enabled is True
    assert execution_settings.version == "v2"
    assert execution_settings.quota_name == "fast_quota"
    assert execution_settings.fallback is False
    assert execution_settings.requested_mode == "mcqa_v2"
    assert envelope.metadata["execution_requested"] == "mcqa_v2"
    assert envelope.metadata["execution_mode"] == "mcqa_v2"
    assert envelope.metadata["mcqa_quota_name"] == "fast_quota"



def test_submit_job_rejects_mcqa_fallback(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import ValidationError

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: _RecordingMcqaBackend())

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    with pytest.raises(ValidationError, match="job submit"):
        app.submit_job(
            sql="SELECT 1",
            mcqa=True,
            mcqa_version="v1",
            mcqa_fallback=True,
        )


class _InteractiveInstance:
    def __init__(self, instance_id="i-1", *, fallback_to_offline=False):
        self.id = instance_id
        self.subquery_id = 1
        self._session_task_name = "AnonymousSQLRTTask"
        self.project = type("Project", (), {"name": "proj"})()
        self._is_select = True
        self.fallback_to_offline = fallback_to_offline

    def wait_for_success(self, timeout=None, interval=None, max_interval=None):
        return None


class _RecordingInteractiveClient:
    def __init__(self, *, fallback_to_offline=False):
        self.execute_sql_interactive_calls: list[dict[str, object]] = []
        self.run_sql_interactive_calls: list[dict[str, object]] = []
        self.run_sql_calls: list[dict[str, object]] = []
        self.execute_sql_cost_calls: list[dict[str, object]] = []
        self.instance = _InteractiveInstance(fallback_to_offline=fallback_to_offline)

    def execute_sql_interactive(self, sql, **kwargs):
        self.execute_sql_interactive_calls.append({"sql": sql, **kwargs})
        return self.instance

    def run_sql_interactive(self, sql, **kwargs):
        self.run_sql_interactive_calls.append({"sql": sql, **kwargs})
        return self.instance

    def run_sql(self, sql, **kwargs):
        self.run_sql_calls.append({"sql": sql, **kwargs})
        return self.instance

    def execute_sql_cost(self, sql, **kwargs):
        self.execute_sql_cost_calls.append({"sql": sql, **kwargs})
        return type("SqlCost", (), {"input_size": 0, "complexity": None, "udf_num": 0})()


class _QueryHarness:
    from maxc_cli.backend.query import QueryMixin as _QueryMixinBase

    class Backend(_QueryMixinBase):
        def __init__(self, client):
            self.client = client

        def _safe_logview(self, instance):
            return None

        def _instance_to_query_result(self, instance, *, project, max_rows, sql, elapsed_ms, offset=0):
            from maxc_cli.models import QueryResult
            return QueryResult(
                rows=[{"_c0": 1}],
                schema=[{"name": "_c0", "type": "bigint"}],
                total_rows=1,
                returned_rows=1,
                has_more=False,
                next_cursor=None,
                elapsed_ms=elapsed_ms,
                bytes_scanned=0,
                project=project,
                sql_executed=sql,
                tables_used=[],
                job_id=instance.id,
            )


class _ExecutionSettings:
    def __init__(self, *, enabled=True, version="v2", quota_name=None, fallback=True, requested_mode="mcqa_v2"):
        self.enabled = enabled
        self.version = version
        self.quota_name = quota_name
        self.fallback = fallback
        self.requested_mode = requested_mode


class _TaskStatusStub:
    def __init__(self, task_type: str, status: str = "TaskStatus.SUCCESS"):
        self.type = task_type
        self.status = status


class _PlainJobResultInstance:
    def __init__(self, *, task_type: str, task_name: str = "AnonymousSQLRTTask"):
        self.id = "job-1"
        self.status = "TERMINATED"
        self.start_time = None
        self.end_time = None
        self.parent = object()
        self._client = object()
        self.project = type("Project", (), {"name": "proj"})()
        self._task_type = task_type
        self._task_name = task_name

    def reload(self, blocking=False):
        return None

    def is_successful(self):
        return True

    def get_task_statuses(self):
        return {self._task_name: _TaskStatusStub(self._task_type)}

    def get_task_detail2(self, task_name=None, **kwargs):
        return {
            "summary": {
                "stages": [
                    {
                        "jobs": [
                            {
                                "name": "proj_job-1_session_query_7_SQLRT_0_0",
                            }
                        ]
                    }
                ]
            }
        }

    def get_sql_query(self):
        raise RuntimeError("plain SQLRT instance has no SQL task")

    def get_logview_address(self):
        return None


class _ReadableJobResultInstance:
    def __init__(self, job_id: str = "job-1"):
        self.id = job_id
        self.start_time = None
        self.end_time = None


class _OuterSqlrtTerminalInstance:
    def __init__(self, *, job_id: str = "job-1", task_name: str = "AnonymousSQLRTTask"):
        from datetime import datetime, timezone

        self.id = job_id
        self._task_name = task_name
        self._status = "TERMINATED"
        self.start_time = datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)
        self.end_time = datetime(2026, 6, 25, 12, 0, 1, tzinfo=timezone.utc)

    def reload(self, blocking=False):
        return None

    @property
    def status(self):
        return self._status

    def is_successful(self):
        return True

    def get_task_statuses(self):
        return {self._task_name: _TaskStatusStub("SQLRT", status="TaskStatus.SUCCESS")}

    def get_task_results(self):
        return {self._task_name: ""}

    def get_task_detail2(self, task_name=None, **kwargs):
        return {
            "mapReduce": {
                "jobs": [
                    {
                        "name": "proj_job-1_session_query_7_SQLRT_0_0",
                        "tasks": [{"status": "Terminated", "startTime": 1, "endTime": 2}],
                    }
                ],
                "plans": [
                    {
                        "jobName": "proj_job-1_session_query_7_SQLRT_0_0",
                        "query": "SELECT 1;",
                    }
                ],
            }
        }

    def get_logview_address(self):
        return "http://logview/session?subQuery=7"


class _BrokenSqlrtStatusInstance:
    def __init__(self, outer_instance, *, job_id: str = "job-1", task_name: str = "AnonymousSQLRTTask", subquery_id: int = 7):
        self.id = job_id
        self._maxc_outer_instance = outer_instance
        self._session_task_name = task_name
        self._subquery_id = subquery_id

    def reload(self, blocking=False):
        raise RuntimeError("Invalid Response Format: 'status'\n Response JSON:{}\n")

    @property
    def status(self):
        raise RuntimeError("Invalid Response Format: 'status'\n Response JSON:{}\n")

    def is_successful(self):
        raise RuntimeError("Invalid Response Format: 'status'\n Response JSON:{}\n")

    def get_task_statuses(self):
        return {}

    def get_task_results(self):
        return {}

    def get_sql_query(self):
        raise RuntimeError("no direct sql query")

    def get_logview_address(self):
        raise RuntimeError("no direct logview")


class _RecordingJobClient:
    def __init__(self, instance):
        self.instance = instance

    def get_instance(self, job_id, *, project=None):
        return self.instance


class _JobHarness:
    from maxc_cli.backend.job import JobMixin as _JobMixinBase

    class Backend(_JobMixinBase):
        def __init__(self, client):
            self.client = client
            self.project = "proj"
            self.result_reader_instances: list[object] = []

        def _instance_to_query_result(self, instance, *, project, max_rows, sql, elapsed_ms, offset=0):
            from maxc_cli.models import QueryResult

            self.result_reader_instances.append(instance)
            return QueryResult(
                rows=[{"_c0": 1}],
                schema=[{"name": "_c0", "type": "bigint"}],
                total_rows=1,
                returned_rows=1,
                has_more=False,
                next_cursor=None,
                elapsed_ms=elapsed_ms,
                bytes_scanned=0,
                project=project,
                sql_executed=sql,
                tables_used=[],
                job_id=instance.id,
            )



def test_extract_sqlrt_subquery_id_from_nested_task_detail():
    from maxc_cli.backend.job import _extract_sqlrt_subquery_id

    detail = {
        "summary": {
            "stages": [
                {
                    "jobs": [
                        {"name": "proj_job-1_session_query_7_SQLRT_0_0"},
                    ]
                }
            ]
        }
    }

    assert _extract_sqlrt_subquery_id(detail) == 7



def test_extract_sqlrt_subquery_id_prefers_explicit_field_and_last_fallback_match():
    from maxc_cli.backend.job import _extract_sqlrt_subquery_id

    assert _extract_sqlrt_subquery_id(
        {
            "summary": {
                "jobs": [
                    {"name": "proj_job-1_session_query_1_SQLRT_0_0"},
                    {"name": "proj_job-1_session_query_7_SQLRT_0_0"},
                ]
            }
        }
    ) == 7
    assert _extract_sqlrt_subquery_id(
        {
            "summary": {
                "jobs": [
                    {"name": "proj_job-1_session_query_1_SQLRT_0_0"},
                    {"subQueryId": "session_query_9"},
                ]
            }
        }
    ) == 9



def test_backend_fetch_job_result_rehydrates_sqlrt_instance_for_reader(monkeypatch):
    plain_instance = _PlainJobResultInstance(task_type="SQLRT")
    readable_instance = _ReadableJobResultInstance()
    backend = _JobHarness.Backend(_RecordingJobClient(plain_instance))

    def fake_rehydrate(self, instance):
        assert instance is plain_instance
        return readable_instance

    monkeypatch.setattr(
        _JobHarness.Backend,
        "_rehydrate_sqlrt_result_instance",
        fake_rehydrate,
        raising=False,
    )

    backend.fetch_job_result("job-1", project="proj", max_rows=10)

    assert backend.result_reader_instances[-1] is readable_instance



def test_backend_get_instance_infers_sqlrt_session_from_subquery_only(monkeypatch):
    plain_instance = _PlainJobResultInstance(task_type="SQLRT")
    backend = _JobHarness.Backend(_RecordingJobClient(plain_instance))
    sentinel = object()

    def fake_build(self, instance, *, session_task_name, session_subquery_id, session_project_name=None, session_is_select=True):
        assert instance is plain_instance
        assert session_task_name == "AnonymousSQLRTTask"
        assert session_subquery_id == 7
        assert session_project_name == "proj"
        assert session_is_select is True
        return sentinel

    monkeypatch.setattr(
        _JobHarness.Backend,
        "_build_session_result_instance",
        fake_build,
        raising=False,
    )

    instance = backend._get_instance(
        "job-1",
        project="proj",
        session_context={"session_subquery_id": 7},
    )

    assert instance is sentinel



def test_backend_get_instance_rejects_ambiguous_sqlrt_inference():
    from maxc_cli.exceptions import ValidationError

    class _AmbiguousSqlrtInstance(_PlainJobResultInstance):
        def __init__(self):
            super().__init__(task_type="SQLRT")

        def get_task_statuses(self):
            return {
                "TaskA": _TaskStatusStub("SQLRT"),
                "TaskB": _TaskStatusStub("SQLRT"),
            }

    backend = _JobHarness.Backend(_RecordingJobClient(_AmbiguousSqlrtInstance()))

    with pytest.raises(ValidationError, match="SQLRT"):
        backend._get_instance(
            "job-1",
            project="proj",
            session_context={"session_subquery_id": 7},
        )



def test_backend_get_instance_rejects_non_sqlrt_composite_target():
    from maxc_cli.exceptions import ValidationError

    backend = _JobHarness.Backend(_RecordingJobClient(_PlainJobResultInstance(task_type="SQL")))

    with pytest.raises(ValidationError, match="SQLRT"):
        backend._get_instance(
            "job-1",
            project="proj",
            session_context={"session_subquery_id": 7},
        )



def test_backend_diagnose_job_accepts_session_context_for_sqlrt_inference(monkeypatch):
    from maxc_cli.models import JobInfo

    plain_instance = _PlainJobResultInstance(task_type="SQLRT")
    backend = _JobHarness.Backend(_RecordingJobClient(plain_instance))
    sentinel = type("ResolvedInstance", (), {"id": "job-1"})()

    def fake_build(self, instance, *, session_task_name, session_subquery_id, session_project_name=None, session_is_select=True):
        assert instance is plain_instance
        assert session_task_name == "AnonymousSQLRTTask"
        assert session_subquery_id == 7
        return sentinel

    def fake_info(self, instance, *, project):
        return JobInfo(
            job_id=instance.id,
            status="failure",
            project=project,
            progress=100,
            stage="failed",
            retryable=False,
            failure_reason="sql failed",
            task_summary=[],
            sql="SELECT 1",
            submitted_at="2026-06-25T00:00:00Z",
            updated_at="2026-06-25T00:00:01Z",
            completed_at="2026-06-25T00:00:01Z",
            logview=None,
        )

    monkeypatch.setattr(_JobHarness.Backend, "_build_session_result_instance", fake_build, raising=False)
    monkeypatch.setattr(_JobHarness.Backend, "_instance_to_job_info", fake_info, raising=False)

    payload = backend.diagnose_job(
        "job-1",
        project="proj",
        session_context={"session_subquery_id": 7},
    )

    assert payload["job_id"] == "job-1"
    assert payload["status"] == "failure"



def test_backend_get_instance_translates_missing_outer_instance_to_not_found():
    from maxc_cli.exceptions import NotFoundError
    from maxc_cli.helpers import OdpsNoSuchObject

    class _MissingInstanceClient:
        def get_instance(self, job_id, *, project=None):
            raise OdpsNoSuchObject("missing")

    backend = _JobHarness.Backend(_MissingInstanceClient())

    with pytest.raises(NotFoundError):
        backend._get_instance(
            "job-1",
            project="proj",
            session_context={"session_subquery_id": 7},
        )



def test_backend_get_job_falls_back_to_outer_instance_when_sqlrt_status_payload_is_empty():
    outer = _OuterSqlrtTerminalInstance()
    broken = _BrokenSqlrtStatusInstance(outer)
    backend = _JobHarness.Backend(_RecordingJobClient(broken))

    info = backend.get_job("job-1", project="proj")

    assert info.status == "success"
    assert info.stage == "completed"
    assert info.sql == "SELECT 1"
    assert info.logview == "http://logview/session?subQuery=7"
    assert info.submitted_at == "2026-06-25T12:00:00+00:00"
    assert info.completed_at == "2026-06-25T12:00:01+00:00"



def test_backend_wait_job_falls_back_to_outer_terminated_status_when_sqlrt_status_payload_is_empty():
    outer = _OuterSqlrtTerminalInstance()
    broken = _BrokenSqlrtStatusInstance(outer)
    backend = _JobHarness.Backend(_RecordingJobClient(broken))

    info = backend.wait_job("job-1", project="proj", timeout=1, poll_interval=0)

    assert info.status == "success"
    assert info.stage == "completed"



def test_backend_execute_query_uses_mcqa_v2_interactive_path():
    client = _RecordingInteractiveClient()
    backend = _QueryHarness.Backend(client)

    result = backend.execute_query(
        "SELECT 1",
        project="proj",
        max_rows=10,
        dry_run=False,
        execution_settings=_ExecutionSettings(version="v2", quota_name="fast_quota", fallback=True, requested_mode="mcqa_v2"),
    )

    assert client.execute_sql_interactive_calls
    call = client.execute_sql_interactive_calls[-1]
    assert call["project"] == "proj"
    assert call["use_mcqa_v2"] is True
    assert call["quota_name"] == "fast_quota"
    assert call["fallback"] is True
    assert result.extra_metadata["execution_requested"] == "mcqa_v2"
    assert result.extra_metadata["execution_mode"] == "mcqa_v2"



def test_backend_submit_query_uses_mcqa_v1_interactive_path():
    client = _RecordingInteractiveClient()
    backend = _QueryHarness.Backend(client)

    job = backend.submit_query(
        "SELECT 1",
        project="proj",
        execution_settings=_ExecutionSettings(version="v1", quota_name=None, fallback=False, requested_mode="mcqa_v1"),
    )

    assert client.run_sql_interactive_calls
    assert not client.run_sql_calls
    call = client.run_sql_interactive_calls[-1]
    assert "project" not in call
    assert "use_mcqa_v2" not in call
    assert job.job_id == "i-1@1"



def test_backend_submit_query_uses_mcqa_v2_interactive_project_and_quota_kwargs():
    client = _RecordingInteractiveClient()
    backend = _QueryHarness.Backend(client)

    job = backend.submit_query(
        "SELECT 1",
        project="proj",
        execution_settings=_ExecutionSettings(version="v2", quota_name="fast_quota", fallback=False, requested_mode="mcqa_v2"),
    )

    assert client.run_sql_interactive_calls
    assert not client.run_sql_calls
    call = client.run_sql_interactive_calls[-1]
    assert call["project"] == "proj"
    assert call["use_mcqa_v2"] is True
    assert call["quota_name"] == "fast_quota"
    assert job.job_id == "i-1"



def test_backend_execute_query_uses_mcqa_v1_interactive_path_without_project_kwarg():
    client = _RecordingInteractiveClient()
    backend = _QueryHarness.Backend(client)

    result = backend.execute_query(
        "SELECT 1",
        project="proj",
        max_rows=10,
        dry_run=False,
        execution_settings=_ExecutionSettings(version="v1", quota_name=None, fallback=True, requested_mode="mcqa_v1"),
    )

    assert client.execute_sql_interactive_calls
    call = client.execute_sql_interactive_calls[-1]
    assert "project" not in call
    assert "use_mcqa_v2" not in call
    assert result.extra_metadata["execution_requested"] == "mcqa_v1"



def test_backend_execute_query_marks_offline_fallback_in_metadata():
    client = _RecordingInteractiveClient(fallback_to_offline=True)
    backend = _QueryHarness.Backend(client)

    result = backend.execute_query(
        "SELECT 1",
        project="proj",
        max_rows=10,
        dry_run=False,
        execution_settings=_ExecutionSettings(version="v2", quota_name="fast_quota", fallback=True, requested_mode="mcqa_v2"),
    )

    assert result.extra_metadata["execution_requested"] == "mcqa_v2"
    assert result.extra_metadata["execution_mode"] == "offline"
    assert result.extra_metadata["mcqa_fallback_used"] is True


class _RemoteRecordingMcqaBackend(_RecordingMcqaBackend):
    supports_remote_jobs = True

    def wait_job(
        self,
        job_id,
        *,
        project=None,
        timeout=None,
        poll_interval=3,
        session_context=None,
    ):
        from maxc_cli.models import JobInfo

        return JobInfo(
            job_id=job_id,
            status="success",
            project=project or "proj",
            progress=100,
            stage="completed",
            sql="SELECT 1",
            submitted_at="2026-06-23T00:00:00Z",
            completed_at="2026-06-23T00:00:01Z",
            logview=None,
        )

    def fetch_job_result(
        self,
        job_id,
        *,
        project=None,
        max_rows=100,
        offset=0,
        session_context=None,
    ):
        from maxc_cli.models import QueryResult

        return QueryResult(
            rows=[{"_c0": 1}],
            schema=[{"name": "_c0", "type": "bigint"}],
            total_rows=1,
            returned_rows=1,
            has_more=False,
            next_cursor=None,
            elapsed_ms=1,
            bytes_scanned=None,
            project=project or "proj",
            sql_executed="SELECT 1",
            tables_used=[],
            job_id=job_id,
        )


class _RemoteSessionAwareBackend(_RecordingMcqaBackend):
    supports_remote_jobs = True

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.get_job_calls: list[dict[str, object]] = []
        self.wait_job_calls: list[dict[str, object]] = []
        self.fetch_job_result_calls: list[dict[str, object]] = []
        self.diagnose_job_calls: list[dict[str, object]] = []

    def submit_query(self, sql, *, project, idempotency_key=None, force=False, execution_settings=None):
        from maxc_cli.models import JobInfo

        return JobInfo(
            job_id="mcqa-session-instance",
            status="pending",
            project=project,
            progress=0,
            sql=sql,
            submitted_at="2026-06-25T00:00:00Z",
            updated_at="2026-06-25T00:00:00Z",
            logview=None,
            warnings=[],
            session_task_name="AnonymousSQLRTTask",
            session_subquery_id=7,
            session_project_name=project,
            session_is_select=True,
        )

    def get_job(self, job_id, *, project=None, session_context=None):
        from maxc_cli.models import JobInfo

        self.get_job_calls.append({
            "job_id": job_id,
            "project": project,
            "session_context": session_context,
        })
        return JobInfo(
            job_id=job_id,
            status="success",
            project=project or "proj",
            progress=100,
            stage="completed",
            sql="SELECT 1",
            submitted_at="2026-06-25T00:00:00Z",
            completed_at="2026-06-25T00:00:01Z",
            logview=None,
        )

    def wait_job(self, job_id, *, project=None, timeout=None, poll_interval=3, session_context=None):
        from maxc_cli.models import JobInfo

        self.wait_job_calls.append({
            "job_id": job_id,
            "project": project,
            "timeout": timeout,
            "poll_interval": poll_interval,
            "session_context": session_context,
        })
        return JobInfo(
            job_id=job_id,
            status="success",
            project=project or "proj",
            progress=100,
            stage="completed",
            sql="SELECT 1",
            submitted_at="2026-06-25T00:00:00Z",
            completed_at="2026-06-25T00:00:01Z",
            logview=None,
        )

    def fetch_job_result(self, job_id, *, project=None, max_rows=100, offset=0, session_context=None):
        from maxc_cli.models import QueryResult

        self.fetch_job_result_calls.append({
            "job_id": job_id,
            "project": project,
            "max_rows": max_rows,
            "offset": offset,
            "session_context": session_context,
        })
        return QueryResult(
            rows=[{"_c0": 1}],
            schema=[{"name": "_c0", "type": "bigint"}],
            total_rows=1,
            returned_rows=1,
            has_more=False,
            next_cursor=None,
            elapsed_ms=1,
            bytes_scanned=None,
            project=project or "proj",
            sql_executed="SELECT 1",
            tables_used=[],
            job_id=job_id,
        )

    def diagnose_job(self, job_id, *, project=None, session_context=None):
        self.diagnose_job_calls.append({
            "job_id": job_id,
            "project": project,
            "session_context": session_context,
        })
        return {
            "job_id": job_id,
            "status": "failure",
            "stage": "failed",
            "retryable": False,
            "failure_reason": "sql failed",
            "diagnosis_category": "sql",
            "diagnosis_summary": "sql failed",
            "logview": None,
            "task_summary": [],
            "task_statuses": [],
            "task_results": {},
        }


class _RemoteSessionMetadataMissingBackend(_RecordingMcqaBackend):
    supports_remote_jobs = True

    def submit_query(self, sql, *, project, idempotency_key=None, force=False, execution_settings=None):
        from maxc_cli.models import JobInfo

        return JobInfo(
            job_id="mcqa-session-instance",
            status="pending",
            project=project,
            progress=0,
            sql=sql,
            submitted_at="2026-06-25T00:00:00Z",
            updated_at="2026-06-25T00:00:00Z",
            logview=None,
            warnings=[],
            session_task_name="AnonymousSQLRTTask",
            session_subquery_id=None,
            session_project_name=project,
            session_is_select=True,
        )


class _RemoteSessionAwareFixedSubqueryBackend(_RemoteSessionAwareBackend):
    def __init__(self, subquery_id: int, *a, **kw):
        super().__init__(*a, **kw)
        self._subquery_id = subquery_id

    def submit_query(self, sql, *, project, idempotency_key=None, force=False, execution_settings=None):
        from maxc_cli.models import JobInfo

        return JobInfo(
            job_id="mcqa-session-instance",
            status="pending",
            project=project,
            progress=0,
            sql=sql,
            submitted_at="2026-06-25T00:00:00Z",
            updated_at="2026-06-25T00:00:00Z",
            logview=None,
            warnings=[],
            session_task_name="AnonymousSQLRTTask",
            session_subquery_id=self._subquery_id,
            session_project_name=project,
            session_is_select=True,
        )


class _RemoteInteractiveQueryResultBackend(_RecordingMcqaBackend):
    supports_remote_jobs = True

    def submit_query(
        self,
        sql,
        *,
        project,
        idempotency_key=None,
        force=False,
        execution_settings=None,
    ):
        from maxc_cli.models import JobInfo

        self._submitted_execution_settings = execution_settings
        self.submit_query_calls.append(
            {
                "sql": sql,
                "project": project,
                "idempotency_key": idempotency_key,
                "force": force,
                "execution_settings": execution_settings,
            }
        )
        return JobInfo(
            job_id="mcqa-session-instance",
            status="pending",
            project=project,
            progress=0,
            sql=sql,
            submitted_at="2026-06-23T00:00:00Z",
            updated_at="2026-06-23T00:00:00Z",
            logview=None,
            warnings=[],
            session_task_name="AnonymousSQLRTTask",
            session_subquery_id=7,
            session_project_name=project,
            session_is_select=True,
        )

    def wait_job(
        self,
        job_id,
        *,
        project=None,
        timeout=None,
        poll_interval=3,
        session_context=None,
    ):
        from maxc_cli.models import JobInfo

        return JobInfo(
            job_id=job_id,
            status="success",
            project=project or "proj",
            progress=100,
            stage="completed",
            sql="SELECT 1",
            submitted_at="2026-06-23T00:00:00Z",
            completed_at="2026-06-23T00:00:01Z",
            logview=None,
        )

    def fetch_job_result(
        self,
        job_id,
        *,
        project=None,
        max_rows=100,
        offset=0,
        session_context=None,
    ):
        result = self.execute_query(
            "SELECT 1",
            project=project or "proj",
            max_rows=max_rows,
            dry_run=False,
            offset=offset,
            execution_settings=self._submitted_execution_settings,
        )
        result.job_id = job_id
        return result

    def execute_query(self, sql, *, project, max_rows, dry_run, offset=0, timeout=None, force=False, execution_settings=None):
        from maxc_cli.models import QueryResult

        self.execute_query_calls.append({
            "sql": sql,
            "project": project,
            "max_rows": max_rows,
            "dry_run": dry_run,
            "offset": offset,
            "timeout": timeout,
            "force": force,
            "execution_settings": execution_settings,
        })
        return QueryResult(
            rows=[{"_c0": 1}],
            schema=[{"name": "_c0", "type": "bigint"}],
            total_rows=1,
            returned_rows=1,
            has_more=False,
            next_cursor=None,
            elapsed_ms=1,
            bytes_scanned=None,
            project=project,
            sql_executed=sql,
            tables_used=[],
            job_id="mcqa-session-instance",
            session_task_name="AnonymousSQLRTTask",
            session_subquery_id=7,
            session_project_name=project,
            session_is_select=True,
            extra_metadata={
                "execution_requested": getattr(execution_settings, "requested_mode", "offline") if execution_settings else "offline",
                "execution_mode": getattr(execution_settings, "requested_mode", "offline") if execution_settings else "offline",
                "mcqa_fallback_enabled": getattr(execution_settings, "fallback", False) if execution_settings else False,
                "mcqa_fallback_used": False,
                "mcqa_quota_name": getattr(execution_settings, "quota_name", None) if execution_settings else None,
            },
        )



def test_remote_query_mcqa_uses_resumable_submit_and_poll_path(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(
        command="query",
        sql="SELECT 1",
        mcqa=True,
        mcqa_version="v2",
        quota="fast_quota",
    )

    assert backend.submit_query_calls
    assert not backend.execute_query_calls
    assert envelope.status == "success"
    assert envelope.metadata["execution_requested"] == "mcqa_v2"


def test_waiting_mcqa_applies_idempotency_key_to_the_resumable_submission(
    tmp_path: 'Path', monkeypatch
):
    from maxc_cli.app import MaxCApp
    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    envelope = app.query(
        command="query",
        sql="SELECT 1",
        mcqa=True,
        idempotency_key="request-1",
    )

    assert envelope.status == "success"
    assert backend.execute_query_calls == []
    assert backend.submit_query_calls[-1]["idempotency_key"] == "request-1"
    assert envelope.metadata["idempotency_key"] == "request-1"


def test_mutating_query_rejects_automatic_retry_before_backend_call(
    tmp_path: 'Path', monkeypatch
):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import ValidationError

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    with pytest.raises(ValidationError, match="mutating SQL"):
        app.query(
            command="query",
            sql="CREATE TABLE test_retry_guard (id BIGINT)",
            force=True,
            mcqa=True,
            retry_on=["BACKEND_CONNECTION_ERROR"],
            max_retries=1,
        )

    assert backend.execute_query_calls == []
    assert backend.submit_query_calls == []



def test_job_submit_ignores_config_fallback_default_and_stays_strict(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {
                "enabled": True,
                "version": "v2",
                "quota_name": "fast_quota",
                "fallback": True,
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.submit_job(sql="SELECT 1")

    call = backend.submit_query_calls[-1]
    execution_settings = call["execution_settings"]
    assert execution_settings.enabled is True
    assert execution_settings.fallback is False
    assert envelope.metadata["mcqa_fallback_enabled"] is False



def test_remote_mcqa_submit_emits_composite_job_id_and_persists_richer_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.submit_job(sql="SELECT 1", mcqa=True)

    assert envelope.data["job_id"] == "mcqa-session-instance@7"
    assert app._ensure_job_store().get_remote_job_context("mcqa-session-instance@7") == {
        "instance_id": "mcqa-session-instance",
        "subquery_id": 7,
        "project": "proj",
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }



def test_remote_mcqa_query_wait_zero_emits_composite_job_id_and_persists_richer_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(command="query", sql="SELECT 1", mcqa=True, wait=0)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "mcqa-session-instance@7"
    assert app._ensure_job_store().get_remote_job_context("mcqa-session-instance@7") == {
        "instance_id": "mcqa-session-instance",
        "subquery_id": 7,
        "project": "proj",
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }


def test_remote_mcqa_wait_timeout_returns_the_same_resumable_composite_job(
    tmp_path: 'Path', monkeypatch
):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import JobTimeoutError

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "auth": {
                    "access_id": "AK",
                    "secret_access_key": "SK",
                    "project": "proj",
                    "endpoint": "http://service",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()

    def timeout_wait(
        job_id,
        *,
        project=None,
        timeout=None,
        poll_interval=3,
        session_context=None,
    ):
        backend.wait_job_calls.append(
            {
                "job_id": job_id,
                "project": project,
                "timeout": timeout,
                "poll_interval": poll_interval,
                "session_context": session_context,
            }
        )
        raise JobTimeoutError("still running")

    backend.wait_job = timeout_wait
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    envelope = app.query(command="query", sql="SELECT 1", mcqa=True, wait=2)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "mcqa-session-instance@7"
    assert envelope.metadata["job_id"] == "mcqa-session-instance@7"
    assert envelope.metadata["execution_requested"] == "mcqa_v1"
    assert envelope.metadata["mcqa_fallback_enabled"] is False
    assert backend.wait_job_calls == [
        {
            "job_id": "mcqa-session-instance",
            "project": "proj",
            "timeout": 2,
            "poll_interval": 1,
            "session_context": {
                "session_task_name": "AnonymousSQLRTTask",
                "session_subquery_id": 7,
                "session_project_name": "proj",
                "session_is_select": True,
            },
        }
    ]
    assert any(
        "fallback is disabled" in warning
        for warning in envelope.agent_hints.warnings
    )



def test_remote_offline_submit_keeps_plain_job_id(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.submit_job(sql="SELECT 1")

    assert envelope.data["job_id"] == "job_mcqa_submit"



def test_remote_maxqa_submit_keeps_plain_job_id_with_project_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.submit_job(sql="SELECT 1", maxqa=True, quota="fast_quota")

    assert envelope.data["job_id"] == "mcqa-session-instance"
    assert envelope.metadata["execution_requested"] == "mcqa_v2"
    assert app._ensure_job_store().get_remote_job_context("mcqa-session-instance") == {
        "instance_id": "mcqa-session-instance",
        "project": "proj",
    }
    assert app._ensure_job_store().get_remote_job_context("mcqa-session-instance@7") is None



def test_remote_maxqa_query_keeps_plain_job_id_with_project_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteInteractiveQueryResultBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(command="query", sql="SELECT 1", maxqa=True, quota="fast_quota")

    assert envelope.metadata["job_id"] == "mcqa-session-instance"
    assert envelope.metadata["execution_requested"] == "mcqa_v2"
    assert app._ensure_job_store().get_remote_job_context("mcqa-session-instance") == {
        "instance_id": "mcqa-session-instance",
        "project": "proj",
    }
    assert app._ensure_job_store().get_remote_job_context("mcqa-session-instance@7") is None


def test_completed_remote_query_survives_unreadable_local_job_store(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteInteractiveQueryResultBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    class BrokenStore:
        def get_remote_job_context(self, job_id):
            raise OSError("state directory unavailable")

    monkeypatch.setattr(app, "_ensure_job_store", lambda: BrokenStore())
    envelope = app.query(command="query", sql="SELECT 1", mcqa=True)
    payload = envelope.to_dict()

    assert envelope.status == "success"
    assert envelope.metadata["job_id"] == "mcqa-session-instance@7"
    assert payload["data"]["result"]["rows"] == [{"_c0": 1}]
    assert any("Do not re" in warning for warning in envelope.agent_hints.warnings)


def test_completed_remote_query_survives_pagination_cache_failure(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    class PaginatedBackend(_RemoteInteractiveQueryResultBackend):
        def execute_query(self, *args, **kwargs):
            result = super().execute_query(*args, **kwargs)
            result.total_rows = 2
            result.has_more = True
            return result

    backend = PaginatedBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    monkeypatch.setattr(
        app.cache,
        "create_session",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("cache locked")),
    )

    envelope = app.query(command="query", sql="SELECT 1", mcqa=True)
    payload = envelope.to_dict()

    assert envelope.status == "success"
    assert payload["data"]["pagination"]["has_more"] is True
    assert payload["data"]["pagination"]["next_cursor"] is None
    assert envelope.agent_hints.actions[-1].id == "job.result"
    assert "Do not rerun" in envelope.agent_hints.warnings[-1]



def test_remote_mcqa_submit_preserves_remote_id_when_subquery_metadata_is_missing(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionMetadataMissingBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.submit_job(sql="SELECT 1", mcqa=True)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "mcqa-session-instance"
    assert "Do not resubmit" in envelope.agent_hints.warnings[0]



def test_remote_mcqa_query_wait_zero_preserves_remote_id_when_metadata_is_missing(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionMetadataMissingBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(command="query", sql="SELECT 1", mcqa=True, wait=0)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "mcqa-session-instance"
    assert "Do not resubmit" in envelope.agent_hints.warnings[0]


def test_remote_submit_survives_local_context_persistence_failure(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    class BrokenStore:
        def save_remote_job_context(self, job_id, context):
            raise OSError("disk full")

    monkeypatch.setattr(app, "_ensure_job_store", lambda: BrokenStore())
    envelope = app.submit_job(sql="SELECT 1", mcqa=True)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "mcqa-session-instance@7"
    assert envelope.metadata["job_id"] == "mcqa-session-instance@7"
    assert "Do not resubmit" in envelope.agent_hints.warnings[0]
    assert "--project proj" in envelope.agent_hints.warnings[0]


def test_explicit_project_bypasses_unreadable_local_job_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    class BrokenStore:
        def get_remote_job_context(self, job_id):
            raise OSError("state directory unavailable")

    monkeypatch.setattr(app, "_ensure_job_store", lambda: BrokenStore())

    envelope = app.job_status("mcqa-session-instance@7", project="proj")

    assert envelope.status == "success"
    assert backend.get_job_calls[-1] == {
        "job_id": "mcqa-session-instance",
        "project": "proj",
        "session_context": {"session_subquery_id": 7},
    }


def test_missing_project_does_not_guess_when_local_job_context_is_unreadable(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    class BrokenStore:
        def get_remote_job_context(self, job_id):
            raise OSError("state directory unavailable")

    monkeypatch.setattr(app, "_ensure_job_store", lambda: BrokenStore())

    with pytest.raises(OSError, match="state directory unavailable"):
        app.job_status("mcqa-session-instance@7")
    assert backend.get_job_calls == []



def test_remote_mcqa_job_wait_uses_persisted_session_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    wait_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, wait_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    wait_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    wait_app.job_wait(job_id)

    expected = {
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }
    assert wait_backend.wait_job_calls[-1]["session_context"] == expected
    assert wait_backend.fetch_job_result_calls[-1]["session_context"] == expected



def test_remote_mcqa_job_result_uses_persisted_session_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    result_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, result_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    result_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    result_app.job_result(job_id)

    expected = {
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }
    assert result_backend.get_job_calls[-1]["session_context"] == expected
    assert result_backend.fetch_job_result_calls[-1]["session_context"] == expected



def test_remote_mcqa_job_status_uses_outer_instance_id_and_preserves_composite_id(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    status_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, status_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    status_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = status_app.job_status(job_id)

    assert status_backend.get_job_calls[-1]["job_id"] == "mcqa-session-instance"
    assert status_backend.get_job_calls[-1]["session_context"] == {
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }
    assert envelope.data["job_id"] == job_id
    assert job_id in envelope.agent_hints.actions[0].command



def test_remote_mcqa_job_wait_uses_outer_instance_id_and_preserves_composite_id(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    wait_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, wait_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    wait_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope, events = wait_app.job_wait(job_id)

    assert wait_backend.get_job_calls[-1]["job_id"] == "mcqa-session-instance"
    assert wait_backend.wait_job_calls[-1]["job_id"] == "mcqa-session-instance"
    assert wait_backend.fetch_job_result_calls[-1]["job_id"] == "mcqa-session-instance"
    assert envelope.metadata["job_id"] == job_id
    assert [event["job_id"] for event in events] == [job_id, job_id]



def test_remote_mcqa_job_result_uses_outer_instance_id_and_preserves_composite_id(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    result_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, result_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    result_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = result_app.job_result(job_id)

    assert result_backend.get_job_calls[-1]["job_id"] == "mcqa-session-instance"
    assert result_backend.fetch_job_result_calls[-1]["job_id"] == "mcqa-session-instance"
    assert envelope.metadata["job_id"] == job_id



def test_remote_mcqa_job_diagnose_uses_outer_instance_id_and_preserves_composite_id(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    diagnose_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, diagnose_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    diagnose_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = diagnose_app.job_diagnose(job_id)

    assert diagnose_backend.diagnose_job_calls[-1]["job_id"] == "mcqa-session-instance"
    assert diagnose_backend.diagnose_job_calls[-1]["session_context"] == {
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }
    assert envelope.data["job_id"] == job_id
    assert envelope.metadata["job_id"] == job_id
    assert job_id in envelope.agent_hints.actions[0].command
    assert job_id in envelope.agent_hints.actions[1].command



def test_remote_mcqa_composite_cancel_is_rejected_before_backend_call(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import ValidationError

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    cancel_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, cancel_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    cancel_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    with pytest.raises(ValidationError, match="not yet supported"):
        cancel_app.cancel_job(job_id)

    assert cancel_backend.cancel_job_calls == []



def test_remote_offline_cancel_keeps_plain_job_id(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = app.submit_job(sql="SELECT 1")
    app.cancel_job(submit_envelope.data["job_id"])

    assert backend.cancel_job_calls[-1]["job_id"] == "job_mcqa_submit"



def test_remote_mcqa_plain_legacy_key_cancel_uses_plain_outer_instance_id(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    app._ensure_job_store().save_remote_job_context(
        "mcqa-session-instance",
        {
            "project": "proj",
            "session_task_name": "AnonymousSQLRTTask",
            "session_subquery_id": 7,
            "session_project_name": "proj",
            "session_is_select": True,
        },
    )

    app.cancel_job("mcqa-session-instance")

    assert backend.cancel_job_calls[-1]["job_id"] == "mcqa-session-instance"



def test_remote_mcqa_job_wait_uses_persisted_submission_project(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    wait_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, wait_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.submit_job(sql="SELECT 1", mcqa=True)
    job_id = submit_envelope.data["job_id"]

    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "other_proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    wait_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    wait_app.job_wait(job_id)

    assert wait_backend.get_job_calls[-1]["project"] == "proj"
    assert wait_backend.wait_job_calls[-1]["project"] == "proj"
    assert wait_backend.fetch_job_result_calls[-1]["project"] == "proj"


def test_remote_offline_job_round_trip_preserves_cross_project_scope(
    tmp_path: 'Path', monkeypatch
):
    """Plain remote jobs need the same durable project context as MCQA jobs."""
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "default_proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )
    submit_backend = _RemoteSessionAwareBackend()
    status_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, status_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submitted = submit_app.submit_job(sql="SELECT 1", project="other_proj")
    job_id = submitted.data["job_id"]

    assert "--project other_proj" in submitted.agent_hints.actions[0].command
    assert submit_app._ensure_job_store().get_remote_job_context(job_id) == {
        "instance_id": job_id,
        "project": "other_proj",
    }

    status_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    status_app.job_status(job_id)
    assert status_backend.get_job_calls[-1]["project"] == "other_proj"


def test_external_remote_job_id_accepts_explicit_project_fallback(
    tmp_path: 'Path', monkeypatch
):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "default_proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )
    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    app.job_status("external-instance", project="other_proj")

    assert backend.get_job_calls[-1]["project"] == "other_proj"



def test_remote_mcqa_query_wait_zero_persists_session_context_for_later_job_wait(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    wait_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, wait_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    query_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = query_app.query(command="query", sql="SELECT 1", mcqa=True, wait=0)
    job_id = envelope.data["job_id"]

    assert envelope.status == "pending"

    wait_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    wait_app.job_wait(job_id)

    expected = {
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }
    assert wait_backend.wait_job_calls[-1]["session_context"] == expected
    assert wait_backend.fetch_job_result_calls[-1]["session_context"] == expected



def test_remote_mcqa_plain_legacy_key_still_resolves_status_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RemoteSessionAwareBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    app._ensure_job_store().save_remote_job_context(
        "mcqa-session-instance",
        {
            "project": "proj",
            "session_task_name": "AnonymousSQLRTTask",
            "session_subquery_id": 7,
            "session_project_name": "proj",
            "session_is_select": True,
        },
    )

    envelope = app.job_status("mcqa-session-instance")

    assert backend.get_job_calls[-1]["job_id"] == "mcqa-session-instance"
    assert backend.get_job_calls[-1]["session_context"] == {
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }
    assert envelope.data["job_id"] == "mcqa-session-instance"



def test_remote_mcqa_job_status_without_local_record_uses_backend_inference(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp
    from maxc_cli.models import JobInfo

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    plain_instance = _PlainJobResultInstance(task_type="SQLRT")
    backend = _JobHarness.Backend(_RecordingJobClient(plain_instance))
    backend.supports_remote_jobs = True
    sentinel = type("ResolvedInstance", (), {"id": "job-1"})()

    def fake_build(self, instance, *, session_task_name, session_subquery_id, session_project_name=None, session_is_select=True):
        assert instance is plain_instance
        assert session_task_name == "AnonymousSQLRTTask"
        assert session_subquery_id == 7
        return sentinel

    def fake_info(self, instance, *, project):
        return JobInfo(
            job_id=instance.id,
            status="success",
            project=project,
            progress=100,
            stage="completed",
            retryable=False,
            failure_reason=None,
            task_summary=[],
            sql="SELECT 1",
            submitted_at="2026-06-25T00:00:00Z",
            updated_at="2026-06-25T00:00:01Z",
            completed_at="2026-06-25T00:00:01Z",
            logview=None,
        )

    monkeypatch.setattr(_JobHarness.Backend, "_build_session_result_instance", fake_build, raising=False)
    monkeypatch.setattr(_JobHarness.Backend, "_instance_to_job_info", fake_info, raising=False)
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.job_status("job-1@7")

    assert envelope.data["job_id"] == "job-1@7"



def test_remote_mcqa_same_outer_instance_keeps_distinct_composite_keys(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend_0 = _RemoteSessionAwareFixedSubqueryBackend(0)
    submit_backend_1 = _RemoteSessionAwareFixedSubqueryBackend(1)
    wait_backend_0 = _RemoteSessionAwareBackend()
    wait_backend_1 = _RemoteSessionAwareBackend()
    backends = iter([submit_backend_0, submit_backend_1, wait_backend_0, wait_backend_1])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app_0 = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope_0 = submit_app_0.submit_job(sql="SELECT 1", mcqa=True)
    submit_app_1 = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope_1 = submit_app_1.submit_job(sql="SELECT 2", mcqa=True)

    assert envelope_0.data["job_id"] == "mcqa-session-instance@0"
    assert envelope_1.data["job_id"] == "mcqa-session-instance@1"
    assert submit_app_1._ensure_job_store().get_remote_job_context("mcqa-session-instance@0") is not None
    assert submit_app_1._ensure_job_store().get_remote_job_context("mcqa-session-instance@1") is not None

    wait_app_0 = MaxCApp(cwd=tmp_path, config_path=config_path)
    wait_app_0.job_wait(envelope_0.data["job_id"])
    wait_app_1 = MaxCApp(cwd=tmp_path, config_path=config_path)
    wait_app_1.job_wait(envelope_1.data["job_id"])

    assert wait_backend_0.wait_job_calls[-1]["session_context"]["session_subquery_id"] == 0
    assert wait_backend_1.wait_job_calls[-1]["session_context"]["session_subquery_id"] == 1



def test_remote_query_cursor_uses_persisted_session_context(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import ValidationError
    from maxc_cli.utils import encode_cursor

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    submit_backend = _RemoteSessionAwareBackend()
    cursor_backend = _RemoteSessionAwareBackend()
    backends = iter([submit_backend, cursor_backend])
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: next(backends))

    submit_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    submit_envelope = submit_app.query(command="query", sql="SELECT 1", mcqa=True, wait=0)

    # Submission defaults may drift after a job is created. Incomplete MCQA
    # v2 config must not make the persisted result cursor unreadable.
    config_payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config_payload["mcqa"] = {
        "enabled": True,
        "version": "v2",
        "quota_name": None,
    }
    config_path.write_text(
        yaml.safe_dump(config_payload, sort_keys=False),
        encoding="utf-8",
    )

    cursor_app = MaxCApp(cwd=tmp_path, config_path=config_path)
    session_id = cursor_app.cache.create_session(
        job_id=submit_envelope.data["job_id"],
        project="proj",
        sql="SELECT 1",
    )
    cursor = encode_cursor(0, session_id=session_id)

    cursor_app.query(command="query", sql="SELECT 1", cursor=cursor)

    expected = {
        "session_task_name": "AnonymousSQLRTTask",
        "session_subquery_id": 7,
        "session_project_name": "proj",
        "session_is_select": True,
    }
    assert cursor_backend.fetch_job_result_calls[-1]["job_id"] == "mcqa-session-instance"
    assert cursor_backend.fetch_job_result_calls[-1]["session_context"] == expected

    with pytest.raises(ValidationError, match="new query submission"):
        cursor_app.query(
            command="query",
            sql="SELECT 1",
            cursor=cursor,
            maxqa=True,
            quota="new-quota",
        )


@pytest.mark.parametrize("include_session", [False, True])
def test_remote_query_cursor_without_live_context_never_resubmits(
    tmp_path: 'Path', monkeypatch, include_session: bool
):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import ValidationError
    from maxc_cli.utils import encode_cursor

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "auth": {
                    "access_id": "AK",
                    "secret_access_key": "SK",
                    "project": "proj",
                    "endpoint": "http://service",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    cursor = encode_cursor(100, session_id=999 if include_session else None)

    with pytest.raises(ValidationError, match="remote.*context|pagination context"):
        app.query(command="query", sql="SELECT 1", cursor=cursor)

    assert backend.submit_query_calls == []
    assert backend.execute_query_calls == []


@pytest.mark.parametrize(
    ("requested_sql", "requested_project", "stored_sql", "message"),
    [
        ("SELECT 999", "project-a", "SELECT 1", "different SQL"),
        (
            "SELECT '--other' AS value",
            "project-a",
            "SELECT '--secret' AS value",
            "different SQL",
        ),
        ("SELECT 1", "project-b", "SELECT 1", "different project"),
    ],
)
def test_remote_query_cursor_rejects_cross_scope_reuse_before_fetch(
    tmp_path: 'Path',
    monkeypatch,
    requested_sql,
    requested_project,
    stored_sql,
    message,
):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import ValidationError
    from maxc_cli.utils import encode_cursor

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "project-a",
                "endpoint": "http://service",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )
    backend = _RemoteRecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    session_id = app.cache.create_session(
        job_id="job-a",
        project="project-a",
        sql=stored_sql,
    )

    with pytest.raises(ValidationError, match=message):
        app.query(
            command="query",
            sql=requested_sql,
            project=requested_project,
            cursor=encode_cursor(10, session_id=session_id),
        )

    assert backend.submit_query_calls == []



def test_query_mcqa_version_flag_implicitly_enables_mcqa(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {
                "enabled": False,
                "version": "v1",
                "fallback": True,
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    backend = _RecordingMcqaBackend()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: backend)

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    envelope = app.query(
        command="query",
        sql="SELECT 1",
        mcqa_version="v2",
        quota="fast_quota",
    )

    call = backend.execute_query_calls[-1]
    execution_settings = call["execution_settings"]
    assert execution_settings.enabled is True
    assert execution_settings.requested_mode == "mcqa_v2"
    assert envelope.metadata["execution_requested"] == "mcqa_v2"



def test_query_rejects_combined_mcqa_and_maxqa_flags(tmp_path: 'Path', monkeypatch):
    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import ValidationError

    isolate_home(monkeypatch, tmp_path)
    clear_odps_env(monkeypatch)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({
            "auth": {
                "access_id": "AK",
                "secret_access_key": "SK",
                "project": "proj",
                "endpoint": "http://service",
            },
            "mcqa": {
                "enabled": False,
                "version": "v1",
                "fallback": True,
                "quota_name": "fast_quota",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **kw: _RecordingMcqaBackend())

    app = MaxCApp(cwd=tmp_path, config_path=config_path)
    with pytest.raises(ValidationError, match="cannot be combined"):
        app.query(
            command="query",
            sql="SELECT 1",
            mcqa=True,
            maxqa=True,
        )
