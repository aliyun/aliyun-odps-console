
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
from maxc_cli.models import AgentHints, Envelope


def test_agent_hints_render_executable_commands_with_action_ids() -> 'None':
    envelope = Envelope(
        command="query.cost",
        status="success",
        data={"estimated_input_size_bytes": 0},
        metadata={
            "project": "demo_project",
            "sql_executed": "SELECT 1 AS one",
        },
        agent_hints=AgentHints(next_actions=["maxc query explain", "maxc query"]),
    )

    payload = envelope.to_dict()

    assert payload["command"] == "query cost"
    assert payload["command_id"] == "query.cost"
    assert payload["data"] == {"analysis": {"estimated_input_size_bytes": 0}}
    assert payload["agent_hints"]["action_ids"] == ["query.explain", "query"]
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
        agent_hints=AgentHints(next_actions=["maxc query", "maxc query.paginate", "maxc meta describe"]),
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
    assert payload["agent_hints"]["action_ids"] == ["query", "query.paginate", "meta.describe"]
    assert payload["agent_hints"]["next_actions"] == [
        "maxc query 'SELECT * FROM sales.orders LIMIT 20' --json",
        "maxc query 'SELECT * FROM sales.orders LIMIT 20' --cursor eyJvIjoyMH0= --json",
        "maxc meta describe sales.orders --json",
    ]


class _StubQueryApp:
    def __init__(self) -> 'None':
        self.calls: 'list[tuple[str, str, str | None]]' = []

    def query_cost(self, *, sql: 'str', project: 'str | None' = None) -> 'Envelope':
        self.calls.append(("cost", sql, project))
        return Envelope(command="query.cost", status="success", data={"mode": "cost"})

    def query_explain(self, *, sql: 'str', project: 'str | None' = None) -> 'Envelope':
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
    assert payload["command_id"] == "query.cost"


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
    def list_tables(self, *, schema: 'str | None' = None) -> 'list[TableDefinition]':
        return [_table()]

    def describe_table(self, table_name: 'str') -> 'TableDefinition':
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


def test_cache_build_returns_clear_metadata_and_async_build_completes(tmp_path: 'Path') -> 'None':
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

    deadline = time.time() + 2
    status = None
    while time.time() < deadline:
        status_envelope = app.cache_build_status(build_id=build_id)
        status = status_envelope.data["status"]
        if status != "running":
            break
        time.sleep(0.02)

    assert status == "completed"


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
    assert payload["command_id"] == "auth.whoami"
    assert payload["data"]["identity"]["authenticated"] is False
    assert payload["data"]["identity"]["configured"] is False
    assert payload["data"]["auth_options"][0]["command"] == "auth login --from-env"


def test_meta_list_projects_hints_use_existing_commands(tmp_path: 'Path') -> 'None':
    app = _make_app(tmp_path)

    envelope = app.meta_list_projects()
    payload = envelope.to_dict()

    assert payload["agent_hints"]["action_ids"] == ["session.set", "meta.list-schemas"]
    assert payload["agent_hints"]["next_actions"] == [
        "maxc session set --json",
        "maxc meta list-schemas --json",
    ]


class _StubCacheBuildApp:
    def __init__(self) -> 'None':
        self.config = type("Config", (), {"default_project": "demo_project"})()
        self.calls: 'list[tuple[str | None, str | None, bool, bool]]' = []

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
                {"type": "progress", "cached_tables": 1, "total_tables": 2, "failed_tables": 0}
            )
            progress_callback(
                {"type": "completed", "cached_tables": 2, "total_tables": 2, "failed_tables": 0}
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
    assert payload["command_id"] == "cache.build"
    assert payload["status"] == "success"
    stderr_text = args.stderr.getvalue()
    assert "Fetching table list..." in stderr_text
    assert "Discovered 2 table(s), starting cache build..." in stderr_text
    assert "Progress: 2/2 tables cached (failed: 0)" in stderr_text
