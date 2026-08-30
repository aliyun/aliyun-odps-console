
import json
import time
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

import maxc_cli.backend as backend_module
from maxc_cli.app import MaxCApp
from maxc_cli.cli import build_parser
from maxc_cli.config import TableColumn, TableDefinition
from maxc_cli.exceptions import ValidationError
from maxc_cli.models import AgentHints, Envelope, action


def test_agent_hints_render_executable_commands() -> 'None':
    envelope = Envelope(
        command="query.cost",
        status="success",
        data={"estimated_input_size_bytes": 0},
        metadata={
            "project": "demo_project",
            "sql_executed": "SELECT 1 AS one",
        },
        agent_hints=AgentHints(actions=[
            action("query.explain", metadata={"sql_executed": "SELECT 1 AS one"}),
            action("query", metadata={"sql_executed": "SELECT 1 AS one"}),
        ]),
    )

    payload = envelope.to_dict()

    assert payload["command"] == "query cost"
    # command_id removed in v0.1.6+
    assert payload["data"] == {"analysis": {"estimated_input_size_bytes": 0}}
    assert payload["agent_hints"]["next_actions"] == [
        "maxc query explain 'SELECT 1 AS one' --json",
        "maxc query 'SELECT 1 AS one' --json",
    ]


def test_agent_hints_infer_table_query_and_pagination_commands() -> 'None':
    envelope = Envelope(
        command="query",
        status="success",
        data={
            "table_name": "sales.orders",
            "next_cursor": "eyJvIjoyMH0=",
        },
        metadata={},
        agent_hints=AgentHints(actions=[
            action("query", data={"table_name": "sales.orders"}),
            action("query.paginate", data={"table_name": "sales.orders", "next_cursor": "eyJvIjoyMH0="}),
            action("meta.describe", data={"table_name": "sales.orders"}),
        ]),
    )

    payload = envelope.to_dict()

    assert payload["data"] == {
        "result": {
            "rows": [],
            "schema": [],
            "row_count": None,
            "returned_rows": None,
        },
        "pagination": {
            "has_more": False,
            "next_cursor": "eyJvIjoyMH0=",
        },
    }
    assert payload["agent_hints"]["next_actions"] == [
        "maxc query 'SELECT * FROM sales.orders LIMIT 20' --json",
        "maxc query 'SELECT * FROM sales.orders LIMIT 20' --cursor eyJvIjoyMH0= --json",
        "maxc meta describe sales.orders --json",
    ]


def test_agent_hints_prefer_qualified_table_name() -> None:
    envelope = Envelope(
        command="meta.list-tables",
        status="success",
        data={
            "table_name": "orders",
            "qualified_name": "sales.orders",
        },
        metadata={},
        agent_hints=AgentHints(actions=[
            action("meta.describe", data={"table_name": "orders", "qualified_name": "sales.orders"}),
            action("data.sample", data={"table_name": "orders", "qualified_name": "sales.orders"}),
        ]),
    )

    payload = envelope.to_dict()

    assert payload["agent_hints"]["next_actions"] == [
        "maxc meta describe sales.orders --json",
        "maxc data sample sales.orders --json",
    ]


class _StubQueryApp:
    def __init__(self) -> 'None':
        self.calls: list[tuple[str, str, str | None]] = []

    def query_cost(self, *, sql: 'str', project: 'str | None' = None, force: 'bool' = False) -> 'Envelope':
        self.calls.append(("cost", sql, project))
        return Envelope(command="query.cost", status="success", data={"mode": "cost"})

    def query_explain(self, *, sql: 'str', project: 'str | None' = None, force: 'bool' = False) -> 'Envelope':
        self.calls.append(("explain", sql, project))
        return Envelope(command="query.explain", status="success", data={"mode": "explain"})

    def query(
        self,
        *,
        command: 'str',
        sql: 'str',
        project: 'str | None' = None,
        max_rows: 'int' = 100,
        cursor: 'str | None' = None,
        dry_run: 'bool' = False,
        async_mode: 'bool' = False,
        cost_check: 'float | None' = None,
        idempotency_key: 'str | None' = None,
        retry_on: 'list[str] | None' = None,
        max_retries: 'int' = 0,
        force: 'bool' = False,
    ) -> 'Envelope':
        _ = (
            command,
            max_rows,
            cursor,
            dry_run,
            async_mode,
            cost_check,
            idempotency_key,
            retry_on,
            max_retries,
            force,
        )
        self.calls.append(("run", sql, project))
        return Envelope(command="query", status="success", data={"mode": "run"})


def test_query_alias_routes_to_query_cost() -> 'None':
    parser = build_parser()
    args = parser.parse_args(["query", "cost", "SELECT 1 AS one", "--json"])
    app = _StubQueryApp()
    stdout = StringIO()

    args.handler(app, args, stdout)

    assert app.calls == [("cost", "SELECT 1 AS one", None)]
    payload = json.loads(stdout.getvalue())
    assert payload["command"] == "query cost"
    # command_id removed in v0.1.6+


def test_query_alias_and_mode_flag_cannot_be_combined() -> 'None':
    parser = build_parser()
    args = parser.parse_args(["query", "cost", "SELECT 1 AS one", "--mode", "explain", "--json"])

    with pytest.raises(ValidationError, match="Do not combine query subcommands"):
        args.handler(_StubQueryApp(), args, StringIO())


def _write_config(tmp_path: 'Path') -> 'Path':
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo_project
state_dir: .maxc/state
cache_dir: .maxc/cache
backend:
  type: auto
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path


def _table(name: 'str' = "sales.orders") -> 'TableDefinition':
    return TableDefinition(
        name=name,
        description="Orders table",
        columns=[TableColumn(name="id", type="bigint", comment="order id")],
        partition_columns=[],
        sample_rows=[],
        partitions=[],
        upstream_tables=[],
        downstream_tables=[],
        owner="owner_a",
        table_type="TABLE",
        size_bytes=1024,
        extra_metadata={"row_count": 128, "row_count_source": "odps_record_num"},
    )


class _StubMetaBackend:
    def list_schemas(self, *, project: 'str | None' = None):
        from maxc_cli.exceptions import TwoTierNamespaceError

        raise TwoTierNamespaceError(
            f"Project {project} does not use the 3-tier namespace model."
        )

    def list_tables(
        self,
        *,
        schema: 'str | None' = None,
        project: 'str | None' = None,
        limit: 'int | None' = None,
        offset: 'int' = 0,
    ) -> 'tuple[list[TableDefinition], bool]':
        tables = [_table()]
        if offset:
            tables = tables[offset:]
        if limit is not None:
            return tables[:limit], len(tables) > limit
        return tables, False

    def describe_table(
        self,
        table_name: 'str',
        *,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'TableDefinition':
        time.sleep(0.01)
        return _table(table_name)

    def list_projects(self) -> 'list[dict[str, str]]':
        return [{"name": "project_a"}, {"name": "project_b"}]


def _make_app(tmp_path: 'Path') -> 'MaxCApp':
    app = MaxCApp(
        cwd=tmp_path,
        config_path=_write_config(tmp_path),
        load_backend=False,
    )
    app.backend = _StubMetaBackend()
    return app


def test_meta_list_tables_returns_live_results_when_cache_is_empty(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)

    envelope = app.meta_list_tables()

    assert envelope.status == "success"
    tables = envelope.to_dict()["data"]["tables"]
    assert len(tables) == 1
    assert tables[0]["table_name"] == "sales.orders"


def test_cache_build_returns_clear_metadata_and_async_alias_is_truthful(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)

    sync_envelope = app.cache_build(max_workers=1)
    assert sync_envelope.data["action"] == "build"
    assert sync_envelope.data["mode"] == "sync"
    assert sync_envelope.data["scope"] == "project"
    assert sync_envelope.data["tables_scanned"] == 1
    assert sync_envelope.data["cached_tables"] == 1
    assert sync_envelope.data["cache_location"].endswith("cache.db")

    async_envelope = app.cache_build(async_mode=True, max_workers=1)
    build_id = async_envelope.data["build_id"]
    assert async_envelope.status == "success"
    assert async_envelope.data["mode"] == "sync"
    assert async_envelope.data["async_requested"] is True
    assert "completed synchronously" in async_envelope.agent_hints.warnings[0]
    assert app.cache_build_status(build_id=build_id).data["status"] == "completed"


def test_cache_build_requires_schema_for_three_tier_project(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)

    class ThreeTierBackend(_StubMetaBackend):
        def list_schemas(self, *, project=None):
            return [{"name": "default"}, {"name": "sales"}]

        def list_tables(self, **_kwargs):
            raise AssertionError("table listing must not start before schema selection")

    app.backend = ThreeTierBackend()

    with pytest.raises(ValidationError, match="requires an explicit schema"):
        app.cache_build(max_workers=1)


def test_cache_build_does_not_guess_namespace_when_probe_fails(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)

    class UnresolvedBackend(_StubMetaBackend):
        def list_schemas(self, *, project=None):
            raise RuntimeError("permission denied")

        def list_tables(self, **_kwargs):
            raise AssertionError("table listing must not start with unknown namespace")

    app.backend = UnresolvedBackend()

    with pytest.raises(ValidationError, match="Could not verify"):
        app.cache_build(max_workers=1)


def test_cache_build_partial_result_uses_success_envelope_and_complete_progress(
    tmp_path: 'Path',
) -> 'None':
    app = _make_app(tmp_path)
    app.backend.list_tables = lambda **_kwargs: ([_table("ok"), _table("bad")], False)

    def describe(table_name: str, **_kwargs) -> TableDefinition:
        if table_name == "bad":
            raise RuntimeError("cannot describe")
        return _table(table_name)

    app.backend.describe_table = describe

    envelope = app.cache_build(max_workers=1, schema_name="sales")
    status = app.cache_build_status(build_id=envelope.data["build_id"])

    assert envelope.status == "success"
    assert envelope.data["build_status"] == "completed_with_errors"
    assert envelope.data["processed_tables"] == 2
    assert envelope.data["cached_tables"] == 1
    assert envelope.data["tables_failed"] == 1
    assert status.data["status"] == "completed_with_errors"
    assert status.data["processed_tables"] == 2
    assert status.data["progress_percent"] == 100
    assert all("--schema sales" in item.command for item in envelope.agent_hints.actions)


def test_cache_build_all_failures_returns_failure_envelope(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)

    def describe(_table_name: str, **_kwargs) -> TableDefinition:
        raise RuntimeError("permission denied")

    app.backend.describe_table = describe

    envelope = app.cache_build(max_workers=1, schema_name="sales")
    status = app.cache_build_status(build_id=envelope.data["build_id"])

    assert envelope.status == "failure"
    assert envelope.error.code == "CACHE_BUILD_FAILED"
    assert envelope.data["build_status"] == "failed"
    assert envelope.data["processed_tables"] == 1
    assert status.data["status"] == "failed"
    assert status.data["progress_percent"] == 100
    assert envelope.agent_hints.actions[0].command == (
        f"maxc cache build --project {app.config.default_project} --schema sales --json"
    )


def test_cache_build_uses_metadata_only_describe_and_caches_partition_columns(
    tmp_path: 'Path',
) -> 'None':
    app = _make_app(tmp_path)

    class MetadataOnlyBackend(_StubMetaBackend):
        def describe_table(self, *_args, **_kwargs):
            raise AssertionError("cache build must not sample rows or list partition values")

        def describe_table_metadata(self, table_name, *, project=None, schema=None):
            _ = (project, schema)
            return TableDefinition(
                name=table_name,
                description="Partitioned orders",
                columns=[TableColumn(name="id", type="bigint")],
                partition_columns=[TableColumn(name="ds", type="string")],
                partitions=["ds=20260830"],
            )

    app.backend = MetadataOnlyBackend()

    envelope = app.cache_build(max_workers=1, schema_name="sales")
    cached = app.cache.get_cached_table(
        app.config.default_project,
        "sales.orders",
        schema_name="sales",
    )

    assert envelope.data["build_status"] == "completed"
    assert cached is not None
    assert cached["partitions"] == ["ds"]


def test_cache_status_and_clear_report_fts_and_preserved_semantics(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)
    project = app.config.default_project
    app.cache.cache_table(
        project, "orders", "Orders", [{"name": "id", "type": "bigint"}],
        schema_name="sales",
    )
    app.cache.save_semantic(
        project, "orders", "Commerce", ["revenue"], [], [],
        schema_name="sales",
    )

    status = app.cache_status(schema_name="sales")
    dry_run = app.cache_clear(schema_name="sales")
    cleared = app.cache_clear(schema_name="sales", force=True)

    assert status.data["table_count"] == 1
    assert status.data["semantic_count"] == 1
    assert status.data["fts_entries"] in {1, None}
    assert dry_run.data["would_delete_tables"] == 1
    assert dry_run.data["preserved_semantics"] == 1
    assert "--schema sales" in dry_run.agent_hints.actions[0].command
    assert dry_run.agent_hints.actions[0].executable is False
    assert dry_run.agent_hints.actions[0].confirmation_required is True
    assert dry_run.agent_hints.actions[0].agent_allowed is False
    assert cleared.data["deleted_tables"] == 1
    assert cleared.data["preserved_semantics"] == 1
    assert app.cache.get_semantic_count(project, "sales") == 1
    assert app.cache.fts_search("revenue", project=project) == []


def test_cache_build_status_missing_is_successful_absence(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)
    project = app.config.default_project

    envelope = app.cache_build_status(build_id="missing")

    assert envelope.status == "success"
    assert envelope.data == {
        "found": False,
        "project": project,
        "build_id": "missing",
        "status": "not_found",
        "message": "No cache build record was found.",
    }
    assert envelope.agent_hints.actions[0].command == (
        f"maxc cache build --project {project} --json"
    )


def _clear_odps_env(monkeypatch) -> 'None':
    for aliases in backend_module.ODPS_ENV_ALIASES.values():
        for alias in aliases:
            monkeypatch.delenv(alias, raising=False)


def test_auth_whoami_without_credentials_returns_guidance(tmp_path: 'Path', monkeypatch) -> 'None':
    _clear_odps_env(monkeypatch)
    app = MaxCApp(
        cwd=tmp_path,
        config_path=_write_config(tmp_path),
        load_backend=False,
    )

    envelope = app.auth_whoami()
    payload = envelope.to_dict()

    assert payload["command"] == "auth whoami"
    # command_id removed in v0.1.6+
    assert payload["data"]["identity"]["authenticated"] is False
    assert payload["data"]["identity"]["configured"] is False
    # OAuth is the recommended login mode; AK remains available.
    auth_commands = [opt["command"] for opt in payload["data"]["auth_options"]]
    assert auth_commands[0] == "auth login --oauth"
    assert "auth login --from-env" in auth_commands


def test_meta_list_projects_hints_use_existing_commands(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)

    envelope = app.meta_list_projects()
    payload = envelope.to_dict()

    assert "next_actions" not in payload["agent_hints"]
    assert payload["agent_hints"]["action_ids"] == [
        "session.set",
        "meta.list-schemas",
    ]
    assert all(
        action["executable"] is False
        for action in payload["agent_hints"]["actions"]
    )


def test_agent_hints_use_distribution_entry_point(monkeypatch) -> None:
    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun   maxc")

    suggested = action("meta.describe", data={"table_name": "sales.orders"})

    assert suggested.command == "aliyun maxc meta describe sales.orders --json"


def test_agent_hints_reject_unsafe_entry_point(monkeypatch) -> None:
    monkeypatch.setenv("MAXC_CLI_NAME", "maxc; echo unsafe")

    suggested = action("auth.whoami")

    assert suggested.command == "maxc auth whoami --json"


def test_agent_hints_detect_aliyun_managed_binary_without_launcher_env(monkeypatch) -> None:
    import maxc_cli.utils as utils

    monkeypatch.delenv("MAXC_CLI_NAME", raising=False)
    monkeypatch.setattr(utils.sys, "executable", "/Users/example/.aliyun/maxc/maxc")
    monkeypatch.setattr(utils.sys, "argv", ["/Users/example/.aliyun/maxc/maxc"])

    suggested = action("auth.whoami")

    assert suggested.command == "aliyun maxc auth whoami --json"


def test_serialized_hints_rewrite_legacy_command_references(monkeypatch) -> None:
    from maxc_cli.models import AgentHints, SuggestedAction

    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    hints = AgentHints(
        actions=[SuggestedAction(id="legacy", title="Legacy", command="maxc auth whoami --json")],
        warnings=["Run `maxc agent context --json` before retrying."],
    )

    payload = hints.to_dict()

    assert payload["next_actions"] == ["aliyun maxc auth whoami --json"]
    assert payload["actions"][0]["command"] == "aliyun maxc auth whoami --json"
    assert payload["warnings"] == [
        "Run `aliyun maxc agent context --json` before retrying."
    ]


@pytest.mark.parametrize(
    ("executable", "agent_allowed", "confirmation_required", "included"),
    [
        (True, True, False, True),
        (False, True, False, False),
        (True, False, False, False),
        (True, True, True, False),
        (False, False, True, False),
    ],
)
def test_legacy_next_actions_only_contains_immediately_safe_commands(
    executable: bool,
    agent_allowed: bool,
    confirmation_required: bool,
    included: bool,
) -> None:
    from maxc_cli.models import SuggestedAction

    hints = AgentHints(
        actions=[
            SuggestedAction(
                id="test.action",
                title="Test action",
                command="maxc auth whoami --json",
                executable=executable,
                agent_allowed=agent_allowed,
                confirmation_required=confirmation_required,
            )
        ]
    ).to_dict()

    assert ("next_actions" in hints) is included


def test_required_argument_actions_are_complete_or_non_executable() -> None:
    external = action(
        "auth.login-external",
        data={"endpoint": "https://service.example.test/api"},
        metadata={"project": "analytics"},
    )
    session = action("session.set")
    upload = action(
        "data.upload",
        data={"table": "analytics.orders", "applied_partition": "ds=20260509"},
        metadata={
            "project": "analytics",
            "file_path": "/tmp/orders.csv",
            "overwrite": False,
            "has_header": True,
        },
    )
    clear = action(
        "cache.clear",
        metadata={"project": "analytics", "schema": "sales"},
    )

    assert external.command == (
        "maxc auth login-external --process-command <credential_helper> "
        "--project analytics --endpoint https://service.example.test/api --json"
    )
    assert external.executable is False
    assert session.command == "maxc session set --project <project> --json"
    assert session.executable is False
    assert upload.command == (
        "maxc data upload analytics.orders --file /tmp/orders.csv "
        "--partition ds=20260509 --project analytics --json"
    )
    assert upload.executable is True
    assert clear.command == (
        "maxc cache clear --project analytics --schema sales --force --json"
    )
    assert clear.executable is True


def test_partition_context_never_suggests_unfiltered_query() -> None:
    query_action = action(
        "query",
        data={
            "table_name": "sales.orders",
            "partition_columns": [{"name": "ds", "type": "string"}],
        },
    )
    sample_action = action(
        "data.sample",
        data={
            "table_name": "sales.orders",
            "has_partitions": True,
            "latest_partition": "ds=20260509",
        },
    )

    assert query_action.command == "maxc query <sql> --json"
    assert query_action.executable is False
    assert sample_action.command == (
        "maxc data sample sales.orders --partition ds=20260509 --json"
    )


def test_table_and_query_actions_preserve_project_and_schema_context() -> None:
    describe = action(
        "meta.describe",
        data={"qualified_name": "sales.orders"},
        metadata={"project": "other_project", "schema": "sales"},
    )
    search = action(
        "meta.search",
        data={"keyword": "orders"},
        metadata={"project": "other_project", "schema": "sales"},
    )
    query_action = action(
        "query",
        data={"qualified_name": "sales.orders"},
        metadata={"project": "other_project"},
    )

    assert describe.command == (
        "maxc meta describe sales.orders --project other_project --schema sales --json"
    )
    assert search.command == (
        "maxc meta search orders --project other_project --schema sales --json"
    )
    assert query_action.command == (
        "maxc query 'SELECT * FROM sales.orders LIMIT 20' --project other_project --json"
    )


class _StubCacheBuildApp:
    def __init__(self) -> 'None':
        self.config = type("Config", (), {"default_project": "demo_project"})()
        self.calls: list[tuple[str | None, str | None, bool, bool]] = []

    def cache_build(
        self,
        *,
        project: 'str | None' = None,
        schema_name: 'str | None' = None,
        async_mode: 'bool' = False,
        progress_callback=None,
    ) -> 'Envelope':
        self.calls.append((project, schema_name, async_mode, progress_callback is not None))
        if progress_callback is not None:
            progress_callback({"type": "listing_start"})
            progress_callback({"type": "listing_complete", "total_tables": 2})
            progress_callback(
                {
                    "type": "progress",
                    "processed_tables": 1,
                    "cached_tables": 1,
                    "total_tables": 2,
                    "failed_tables": 0,
                }
            )
            progress_callback(
                {
                    "type": "completed",
                    "processed_tables": 2,
                    "cached_tables": 2,
                    "total_tables": 2,
                    "failed_tables": 0,
                }
            )
        return Envelope(
            command="cache.build",
            status="success",
            data={"action": "build", "mode": "sync", "scope": "project"},
            metadata={"project": project or "demo_project"},
        )


def test_cache_build_json_handler_emits_single_envelope() -> 'None':
    parser = build_parser()
    args = parser.parse_args(["cache", "build", "--json"])
    args.stderr = StringIO()
    app = _StubCacheBuildApp()
    stdout = StringIO()

    args.handler(app, args, stdout)

    assert app.calls == [(None, None, False, True)]
    payload = json.loads(stdout.getvalue())
    assert payload["command"] == "cache build"
    # command_id removed in v0.1.6+
    assert payload["status"] == "success"
    stderr_text = args.stderr.getvalue()
    assert "Fetching table list..." in stderr_text
    assert "Discovered 2 table(s), starting cache build..." in stderr_text
    assert "Progress: 2/2 tables processed (cached: 2, failed: 0)" in stderr_text


def test_cache_build_human_handler_uses_app_implementation() -> 'None':
    parser = build_parser()
    args = parser.parse_args(["cache", "build"])
    app = _StubCacheBuildApp()
    stdout = StringIO()

    args.handler(app, args, stdout)

    assert app.calls == [(None, None, False, True)]
    text = stdout.getvalue()
    assert "Fetching table list..." in text
    assert "Progress: 2/2 tables processed" in text
    assert "| action | build" in text


def test_cache_build_async_cli_alias_returns_final_json_envelope() -> 'None':
    parser = build_parser()
    args = parser.parse_args(["cache", "build", "--async"])
    args.stderr = StringIO()
    app = _StubCacheBuildApp()
    stdout = StringIO()

    args.handler(app, args, stdout)

    assert app.calls == [(None, None, True, True)]
    assert json.loads(stdout.getvalue())["status"] == "success"
