"""PR-6 regressions: --schema on meta subcommands + partition_columns from cache.

Pins:
  1. ``meta describe``, ``meta latest-partition``, ``meta freshness``, and
     ``meta partitions`` all accept ``--schema`` and forward it to the
     backend so 3-tier projects resolve table names in the right namespace.
  2. ``meta describe`` cache-hit path uses the live API's
     ``partition_columns`` (with real types) when it can reach the backend,
     instead of inferring column NAMES from the cached partitions field
     and silently calling everything ``string``.
"""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.unit


# ── --schema is forwarded to backend ──────────────────────────────────────


class _SchemaRecordingBackend:
    """Stand-in backend that records every schema kwarg it receives."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def list_tables(self, *, schema=None, project=None, limit=None, offset=0):
        self.calls.append({"method": "list_tables", "project": project, "schema": schema})
        from maxc_cli.config import TableColumn, TableDefinition
        return (
            [
                TableDefinition(
                    name="t1",
                    description="",
                    columns=[TableColumn(name="id", type="bigint")],
                    partition_columns=[],
                )
            ],
            False,
        )

    def list_schemas(self, *, project=None):
        self.calls.append({"method": "list_schemas", "project": project})
        from maxc_cli.exceptions import TwoTierNamespaceError
        raise TwoTierNamespaceError(f"Project {project} does not use the 3-tier namespace model.")

    def describe_table(self, table_name, *, project=None, schema=None):
        self.calls.append({"method": "describe_table", "table": table_name, "project": project, "schema": schema})
        from maxc_cli.config import TableColumn, TableDefinition
        return TableDefinition(
            name=table_name,
            description="",
            columns=[TableColumn(name="id", type="bigint")],
            partition_columns=[TableColumn(name="ds", type="string")],
        )

    def latest_partition_info(self, table_name, *, project=None, schema=None):
        self.calls.append({"method": "latest_partition_info", "table": table_name, "project": project, "schema": schema})
        return ({"table_name": table_name, "has_partitions": False}, [])

    def freshness_info(self, table_name, *, project=None, schema=None):
        self.calls.append({"method": "freshness_info", "table": table_name, "project": project, "schema": schema})
        return ({"table_name": table_name, "freshness_status": "fresh"}, [])

    def list_partitions(self, table_name, *, limit=100, project=None, schema=None):
        self.calls.append({"method": "list_partitions", "table": table_name, "project": project, "schema": schema, "limit": limit})
        return ({
            "table_name": table_name,
            "partitions": [],
            "visible_count": 0,
            "has_more": False,
            "limit": limit,
            "latest_partition": None,
            "is_partitioned": False,
        }, [])


def _make_app(backend: _SchemaRecordingBackend) -> Any:
    """Build a MaxCApp with the recording backend bolted on.

    We can't easily reuse the production __init__ (it requires real config files
    and an OdpsBackend), so we instantiate via __new__ and manually attach the
    fields the meta methods touch."""
    import tempfile
    from types import SimpleNamespace

    from maxc_cli.app import MaxCApp
    from maxc_cli.cache import LocalCache

    app = MaxCApp.__new__(MaxCApp)
    app.backend = backend
    # The meta methods only read default_project / default_schema off config.
    app.config = SimpleNamespace(default_project="p1", default_schema="default")
    # Cache is touched only by meta_describe; point it at a temp DB so the
    # cache lookup is benign (returns None for an unknown table).
    tmp = tempfile.mkdtemp(prefix="maxc_test_")
    from pathlib import Path
    app._cache = LocalCache(Path(tmp))
    app._logs = []
    app.log = lambda *a, **kw: None  # silence audit log
    return app


def test_meta_describe_forwards_schema() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)
    app.meta_describe("t1", schema="silver")
    describe_calls = [c for c in backend.calls if c["method"] == "describe_table"]
    assert describe_calls, "expected describe_table to be called"
    assert describe_calls[0]["schema"] == "silver"


def test_meta_describe_parses_qualified_name_and_preserves_agent_identity() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)

    envelope = app.meta_describe("sales.orders", project="p1")

    describe_call = next(c for c in backend.calls if c["method"] == "describe_table")
    assert describe_call == {
        "method": "describe_table",
        "table": "orders",
        "project": "p1",
        "schema": "sales",
    }
    assert envelope.data["table_name"] == "orders"
    assert envelope.data["schema_name"] == "sales"
    assert envelope.data["qualified_name"] == "sales.orders"
    assert envelope.metadata["schema"] == "sales"
    assert envelope.agent_hints.actions[0].command == (
        "maxc data sample sales.orders --project p1 --schema sales --json"
    )


def test_meta_describe_rejects_conflicting_qualified_scope() -> None:
    from maxc_cli.exceptions import ValidationError

    app = _make_app(_SchemaRecordingBackend())

    with pytest.raises(ValidationError, match="schema conflicts"):
        app.meta_describe("sales.orders", schema="marketing")
    with pytest.raises(ValidationError, match="project conflicts"):
        app.meta_describe("other.sales.orders", project="p1")


def test_two_part_table_name_fails_closed_for_different_project_in_two_tier() -> None:
    from maxc_cli.exceptions import ValidationError

    backend = _SchemaRecordingBackend()
    app = _make_app(backend)

    with pytest.raises(ValidationError, match="ambiguous"):
        app.meta_describe("other.orders")

    assert not any(call["method"] == "describe_table" for call in backend.calls)


def test_two_part_active_project_name_is_valid_for_verified_two_tier() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)

    envelope = app.meta_describe("p1.orders")

    describe_call = next(c for c in backend.calls if c["method"] == "describe_table")
    assert describe_call["project"] == "p1"
    assert describe_call["schema"] is None
    assert envelope.data["qualified_name"] == "p1.orders"


def test_two_part_schema_name_requires_verified_three_tier_namespace() -> None:
    class ThreeTierBackend(_SchemaRecordingBackend):
        def list_schemas(self, *, project=None):
            self.calls.append({"method": "list_schemas", "project": project})
            return [{"name": "default"}, {"name": "sales"}]

    backend = ThreeTierBackend()
    app = _make_app(backend)

    envelope = app.meta_describe("sales.orders")

    describe_call = next(c for c in backend.calls if c["method"] == "describe_table")
    assert describe_call["project"] == "p1"
    assert describe_call["schema"] == "sales"
    assert envelope.data["qualified_name"] == "sales.orders"


def test_two_part_table_name_fails_closed_when_namespace_probe_fails() -> None:
    from maxc_cli.exceptions import ValidationError

    class UnresolvedBackend(_SchemaRecordingBackend):
        def list_schemas(self, *, project=None):
            raise RuntimeError("schema listing denied")

    app = _make_app(UnresolvedBackend())

    with pytest.raises(ValidationError, match="Could not verify"):
        app.meta_describe("sales.orders")


@pytest.mark.parametrize(
    ("method_name", "backend_method"),
    [
        ("meta_latest_partition", "latest_partition_info"),
        ("meta_freshness", "freshness_info"),
        ("meta_partitions", "list_partitions"),
    ],
)
def test_table_metadata_commands_parse_schema_qualified_names(
    method_name: str,
    backend_method: str,
) -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)

    envelope = getattr(app, method_name)("sales.orders", project="p1")

    call = next(c for c in backend.calls if c["method"] == backend_method)
    assert call["table"] == "orders"
    assert call["project"] == "p1"
    assert call["schema"] == "sales"
    assert envelope.data["table_name"] == "orders"
    assert envelope.data["schema_name"] == "sales"
    assert envelope.data["qualified_name"] == "sales.orders"
    assert envelope.metadata["schema"] == "sales"


def test_meta_list_tables_omits_default_schema_for_2tier() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)
    app.config.default_schema = None

    payload = app.meta_list_tables().to_dict()

    assert payload["data"]["namespace_model"] == "2-tier"
    assert payload["data"]["tables"][0]["schema_name"] is None
    assert payload["data"]["tables"][0]["qualified_name"] == "t1"
    assert payload["agent_hints"]["next_actions"][0] == (
        "maxc meta describe t1 --project p1 --json"
    )


def test_meta_list_tables_detects_default_schema_for_3tier() -> None:
    class _ThreeTierBackend(_SchemaRecordingBackend):
        def list_schemas(self, *, project=None):
            self.calls.append({"method": "list_schemas", "project": project})
            return [{"name": "analytics"}, {"name": "default"}]

    backend = _ThreeTierBackend()
    app = _make_app(backend)
    app.config.default_schema = None

    payload = app.meta_list_tables().to_dict()

    assert payload["data"]["namespace_model"] == "3-tier"
    assert payload["data"]["schema"] == "default"
    assert payload["data"]["tables"][0]["schema_name"] == "default"
    assert payload["data"]["tables"][0]["qualified_name"] == "default.t1"
    assert backend.calls[:2] == [
        {"method": "list_schemas", "project": "p1"},
        {"method": "list_tables", "project": "p1", "schema": "default"},
    ]
    assert payload["agent_hints"]["next_actions"][0] == (
        "maxc meta describe default.t1 --project p1 --schema default --json"
    )


def test_meta_list_tables_does_not_guess_when_namespace_probe_fails() -> None:
    class _UnresolvedBackend(_SchemaRecordingBackend):
        def list_schemas(self, *, project=None):
            self.calls.append({"method": "list_schemas", "project": project})
            from maxc_cli.exceptions import ValidationError
            raise ValidationError("schema name must be provided")

    backend = _UnresolvedBackend()
    app = _make_app(backend)
    app.config.default_schema = None

    payload = app.meta_list_tables().to_dict()

    assert payload["data"]["namespace_model"] == "unknown"
    assert payload["data"]["schema"] is None
    assert payload["data"]["tables"][0]["qualified_name"] is None
    assert payload["agent_hints"]["next_actions"] == [
        "maxc meta list-schemas --project p1 --json"
    ]
    assert "Could not verify" in payload["agent_hints"]["warnings"][0]


def test_meta_list_tables_does_not_probe_an_explicit_schema() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)
    app.config.default_schema = None

    payload = app.meta_list_tables(schema="analytics").to_dict()

    assert payload["data"]["namespace_model"] == "3-tier"
    assert payload["data"]["schema"] == "analytics"
    assert backend.calls == [
        {"method": "list_tables", "project": "p1", "schema": "analytics"}
    ]


def test_meta_list_tables_keeps_empty_schema_probe_as_3tier_unresolved() -> None:
    class _EmptyThreeTierBackend(_SchemaRecordingBackend):
        def list_schemas(self, *, project=None):
            self.calls.append({"method": "list_schemas", "project": project})
            return []

    backend = _EmptyThreeTierBackend()
    app = _make_app(backend)
    app.config.default_schema = None

    payload = app.meta_list_tables().to_dict()

    assert payload["data"]["namespace_model"] == "3-tier"
    assert payload["data"]["schema"] is None
    assert payload["data"]["tables"][0]["qualified_name"] is None
    assert payload["agent_hints"]["next_actions"] == [
        "maxc meta list-schemas --project p1 --json"
    ]
    assert "no active schema" in payload["agent_hints"]["warnings"][0]


def test_meta_list_tables_scopes_2tier_cache_to_legacy_default_key() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)
    app.config.default_schema = None
    app.cache.cache_table(
        project="p1", table_name="cached_default", description="", columns=[], schema_name="default"
    )
    app.cache.cache_table(
        project="p1", table_name="cached_other", description="", columns=[], schema_name="analytics"
    )

    payload = app.meta_list_tables().to_dict()

    assert payload["metadata"]["source"] == "cache"
    assert [table["table_name"] for table in payload["data"]["tables"]] == [
        "cached_default"
    ]
    assert payload["data"]["tables"][0]["schema_name"] is None
    assert payload["data"]["tables"][0]["qualified_name"] == "cached_default"


def test_meta_search_hides_internal_default_schema_for_verified_2tier() -> None:
    class _TwoTierSearchBackend(_SchemaRecordingBackend):
        @staticmethod
        def catalog_search_tables(*_args, **_kwargs):
            return None

    backend = _TwoTierSearchBackend()
    app = _make_app(backend)
    app.config.default_schema = None
    app.cache.cache_table(
        project="p1",
        table_name="orders",
        description="order facts",
        columns=[{"name": "order_id", "type": "bigint"}],
        schema_name="default",
    )

    tables = app.meta_search("orders").to_dict()
    columns = app.meta_search_columns("order_id").to_dict()

    match = tables["data"]["search"]["matches"][0]
    assert match["schema_name"] is None
    assert match["qualified_name"] == "orders"
    assert tables["agent_hints"]["next_actions"][0] == (
        "maxc meta describe orders --project p1 --json"
    )
    column_match = columns["data"]["search"]["matches"][0]
    assert column_match["schema_name"] is None
    assert column_match["qualified_name"] == "orders"


def test_meta_list_tables_ignores_cross_schema_cache_when_probe_is_unresolved() -> None:
    class _UnresolvedBackend(_SchemaRecordingBackend):
        def list_schemas(self, *, project=None):
            self.calls.append({"method": "list_schemas", "project": project})
            raise RuntimeError("temporary schema API failure")

    backend = _UnresolvedBackend()
    app = _make_app(backend)
    app.config.default_schema = None
    app.cache.cache_table(
        project="p1", table_name="cached_default", description="", columns=[], schema_name="default"
    )
    app.cache.cache_table(
        project="p1", table_name="cached_other", description="", columns=[], schema_name="analytics"
    )

    payload = app.meta_list_tables().to_dict()

    assert payload["metadata"]["source"] == "backend"
    assert [table["table_name"] for table in payload["data"]["tables"]] == ["t1"]
    assert payload["data"]["tables"][0]["qualified_name"] is None


def test_backend_list_schemas_emits_explicit_two_tier_signal() -> None:
    from maxc_cli.backend.meta import MetaMixin
    from maxc_cli.exceptions import TwoTierNamespaceError

    class _Client:
        def list_schemas(self, *, project=None):
            raise RuntimeError(
                f"Project '{project}' Does Not Use The 3-Tier Namespace Model."
            )

    backend = MetaMixin.__new__(MetaMixin)
    backend.client = _Client()
    backend.project = "p1"

    with pytest.raises(TwoTierNamespaceError):
        backend.list_schemas(project="p1")


def test_meta_list_schemas_empty_success_does_not_claim_2tier() -> None:
    class _EmptyThreeTierBackend(_SchemaRecordingBackend):
        def list_schemas(self, *, project=None):
            return []

    app = _make_app(_EmptyThreeTierBackend())

    payload = app.meta_list_schemas(project="p1").to_dict()

    assert payload["status"] == "success"
    assert payload["data"]["schemas"] == []
    assert "supports 3-tier" in payload["agent_hints"]["warnings"][0]


def test_meta_describe_exposes_columns_alias() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)

    payload = app.meta_describe("t1").to_dict()

    table = payload["data"]["table"]
    assert table["schema"] == table["columns"]
    assert table["columns"][0]["name"] == "id"


def test_meta_latest_partition_forwards_schema() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)
    app.meta_latest_partition("t1", schema="silver")
    assert backend.calls == [
        {"method": "latest_partition_info", "table": "t1", "project": "p1", "schema": "silver"}
    ]


def test_meta_freshness_forwards_schema() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)
    app.meta_freshness("t1", schema="silver")
    assert backend.calls == [
        {"method": "freshness_info", "table": "t1", "project": "p1", "schema": "silver"}
    ]


def test_meta_partitions_forwards_schema() -> None:
    backend = _SchemaRecordingBackend()
    app = _make_app(backend)
    app.meta_partitions("t1", schema="silver")
    assert backend.calls == [
        {"method": "list_partitions", "table": "t1", "project": "p1", "schema": "silver", "limit": 100}
    ]


# ── meta describe cache hit picks up live partition_columns ───────────────


# ── cache build --async compatibility is truthful ────────────────────────


def test_cache_async_alias_does_not_claim_background_execution(monkeypatch) -> None:
    """A non-daemon thread still blocks interpreter exit, so the old
    ``--async`` path was synchronous in practice while reporting ``running``.
    The compatibility flag now completes inline and reports that explicitly."""
    import tempfile
    from pathlib import Path
    from types import SimpleNamespace

    from maxc_cli.app import MaxCApp
    from maxc_cli.cache import LocalCache

    class _StubBackend:
        def list_tables(self, *, schema=None, project=None):
            return [], False

    app = MaxCApp.__new__(MaxCApp)
    app.backend = _StubBackend()
    app.config = SimpleNamespace(default_project="p1", default_schema="default")
    tmp = tempfile.mkdtemp(prefix="maxc_test_")
    app._cache = LocalCache(Path(tmp))
    app._logs = []
    app.log = lambda *a, **kw: None

    def _thread_must_not_start(*_args, **_kwargs):
        raise AssertionError("deprecated async alias must not spawn a background thread")

    monkeypatch.setattr("threading.Thread", _thread_must_not_start)
    envelope = app.cache_build(async_mode=True, max_workers=1)

    assert envelope.status == "success"
    assert envelope.data["mode"] == "sync"
    assert envelope.data["async_requested"] is True
    assert envelope.data["build_status"] == "empty"
    assert "No tables were found" in envelope.agent_hints.warnings[1]
    assert "completed synchronously" in envelope.agent_hints.warnings[0]


def test_meta_describe_overwrites_partition_columns_from_live_api(tmp_path) -> None:
    """When the cache populated `partitions` with column NAMES (the cache-build
    path does this), the old code rebuilt partition_columns as
    ``[TableColumn(name=p, type="string", ...)]`` — losing the real type.
    After this PR, a successful live describe_table call overwrites
    partition_columns with the authoritative typed list."""
    from types import SimpleNamespace

    from maxc_cli.app import MaxCApp
    from maxc_cli.cache import LocalCache
    from maxc_cli.config import TableColumn, TableDefinition

    app = MaxCApp.__new__(MaxCApp)
    app.config = SimpleNamespace(default_project="p1", default_schema="default")
    app._cache = LocalCache(tmp_path)
    app._logs = []
    app.log = lambda *a, **kw: None

    # Seed the cache as the cache-build path would: partitions is a list of
    # partition column NAMES.
    app.cache.cache_table(
        project="p1",
        table_name="events",
        description="",
        columns=[{"name": "id", "type": "bigint", "comment": ""}],
        partitions=["ds", "hh"],
        schema_name="default",
    )

    class _LiveBackend:
        def describe_table(self, table_name, *, project=None, schema=None):
            return TableDefinition(
                name=table_name,
                description="live desc",
                columns=[TableColumn(name="id", type="bigint", comment="")],
                partition_columns=[
                    TableColumn(name="ds", type="string", comment="date partition"),
                    TableColumn(name="hh", type="int", comment="hour partition"),
                ],
            )

    app.backend = _LiveBackend()
    envelope = app.meta_describe("events")
    partition_cols = envelope.data.get("partition_columns") or []
    by_name = {p["name"]: p for p in partition_cols}
    assert by_name["hh"]["type"] == "int", (
        f"expected hh partition type 'int' from live API, got {by_name['hh']['type']!r}; "
        "cache-only inference probably won out over the live result"
    )
    assert by_name["ds"]["comment"] == "date partition"
