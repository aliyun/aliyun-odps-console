import json
from io import StringIO
from pathlib import Path

import pytest

from maxc_cli.app import MaxCApp
from maxc_cli.cli import run
from maxc_cli.exceptions import ValidationError

pytestmark = pytest.mark.unit


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "default_project: demo_project",
                "default_schema: default",
                "state_dir: .maxc/state",
                "cache_dir: .maxc/cache",
                "backend:",
                "  type: auto",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _app(tmp_path: Path, config_path: 'Path | None' = None) -> MaxCApp:
    return MaxCApp(
        cwd=tmp_path,
        config_path=config_path or _write_config(tmp_path),
        load_backend=False,
    )


def _cache_table(app: MaxCApp, project: str, schema: str, table: str) -> None:
    app.cache.cache_table(
        project,
        table,
        f"{schema} {table}",
        [{"name": "id", "type": "bigint"}],
        schema_name=schema,
    )


def _run_json(tmp_path: Path, config_path: Path, argv: list[str]) -> tuple[int, dict]:
    stdout = StringIO()
    stderr = StringIO()
    code = run(
        ["--config", str(config_path), *argv],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )
    assert stderr.getvalue() == ""
    return code, json.loads(stdout.getvalue())


def test_semantic_scope_distinguishes_same_table_across_schemas(tmp_path: Path) -> None:
    app = _app(tmp_path)
    _cache_table(app, "demo_project", "sales", "orders")
    _cache_table(app, "demo_project", "marketing", "orders")

    saved = app.semantic_set(
        "orders",
        semantic_desc="Sales facts",
        project="demo_project",
        schema="sales",
    )
    sales = app.semantic_get("orders", project="demo_project", schema="sales")
    marketing = app.semantic_get(
        "orders", project="demo_project", schema="marketing"
    )
    missing = app.semantic_list_missing(project="demo_project")

    assert saved.status == "success"
    assert saved.data["qualified_name"] == "sales.orders"
    assert sales.data["semantic"]["semantic_desc"] == "Sales facts"
    assert marketing.data["semantic"] is None
    assert missing.data["missing_semantic"] == 1
    assert missing.data["tables"] == [
        {
            "table_name": "orders",
            "qualified_name": "marketing.orders",
            "schema_name": "marketing",
            "description": "marketing orders",
            "column_count": 1,
        }
    ]
    next_action = missing.agent_hints.actions[0].command
    assert "--project demo_project" in next_action
    assert "--schema marketing" in next_action
    assert "--desc '<semantic_description>'" in next_action
    assert missing.agent_hints.actions[0].executable is False
    assert missing.agent_hints.actions[0].confirmation_required is True
    assert missing.agent_hints.actions[0].agent_allowed is False


def test_semantic_get_never_suggests_an_empty_overwrite(tmp_path: Path) -> None:
    app = _app(tmp_path)
    _cache_table(app, "demo_project", "sales", "orders")
    app.semantic_set(
        "orders",
        semantic_desc="Existing meaning",
        project="demo_project",
        schema="sales",
    )

    existing = app.semantic_get(
        "orders",
        project="demo_project",
        schema="sales",
    )
    missing = app.semantic_get(
        "other",
        project="demo_project",
        schema="sales",
    )

    assert "meta.semantic.set" not in [item.id for item in existing.agent_hints.actions]
    missing_action = missing.agent_hints.actions[0]
    assert missing_action.id == "meta.semantic.set"
    assert "<semantic_description>" in missing_action.command
    assert missing_action.executable is False
    assert "next_actions" not in missing.agent_hints.to_dict()


def test_semantic_set_rejects_empty_metadata_instead_of_erasing_context(
    tmp_path: Path,
) -> None:
    app = _app(tmp_path)

    with pytest.raises(ValidationError, match="Semantic metadata is required"):
        app.semantic_set(
            "orders",
            project="demo_project",
            schema="sales",
        )


def test_semantic_qualified_name_conflicts_are_rejected(tmp_path: Path) -> None:
    app = _app(tmp_path)

    with pytest.raises(ValidationError, match="schema conflicts"):
        app.semantic_get("sales.orders", schema="marketing")
    with pytest.raises(ValidationError, match="project conflicts"):
        app.semantic_get(
            "project_a.sales.orders",
            project="project_b",
        )


def test_semantic_clear_is_scoped_and_bulk_requires_force(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    app = _app(tmp_path, config_path)
    for project, schema in [
        ("demo_project", "sales"),
        ("demo_project", "marketing"),
        ("other_project", "sales"),
    ]:
        app.cache.save_semantic(
            project,
            "orders",
            f"{project}.{schema}",
            [],
            [],
            [],
            schema_name=schema,
        )

    code, failure = _run_json(
        tmp_path,
        config_path,
        ["meta", "semantic", "clear", "--all", "--project", "demo_project", "--json"],
    )
    assert code == 1
    assert failure["status"] == "failure"
    assert failure["error"]["code"] == "VALIDATION_ERROR"
    assert not failure.get("agent_hints")
    assert app.cache.get_semantic_count("demo_project") == 2

    code, cleared = _run_json(
        tmp_path,
        config_path,
        [
            "meta",
            "semantic",
            "clear",
            "--all",
            "--force",
            "--project",
            "demo_project",
            "--schema",
            "sales",
            "--json",
        ],
    )
    assert code == 0
    assert cleared["data"]["cleared"] == 1
    assert cleared["data"]["scope"] == "schema"
    assert app.cache.get_semantic("demo_project", "orders", "sales") is None
    assert app.cache.get_semantic("demo_project", "orders", "marketing") is not None
    assert app.cache.get_semantic("other_project", "orders", "sales") is not None


def test_semantic_cli_set_get_respects_explicit_scope(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    code, saved = _run_json(
        tmp_path,
        config_path,
        [
            "meta",
            "semantic",
            "set",
            "orders",
            "--project",
            "demo_project",
            "--schema",
            "sales",
            "--desc",
            "Sales facts",
            "--json",
        ],
    )
    assert code == 0
    assert saved["metadata"] == {"project": "demo_project", "schema": "sales"}

    code, loaded = _run_json(
        tmp_path,
        config_path,
        [
            "meta",
            "semantic",
            "get",
            "sales.orders",
            "--project",
            "demo_project",
            "--json",
        ],
    )
    assert code == 0
    assert loaded["data"]["qualified_name"] == "sales.orders"
    assert loaded["data"]["semantic"]["semantic_desc"] == "Sales facts"


def test_meta_describe_reads_semantic_from_explicit_schema(tmp_path: Path) -> None:
    app = _app(tmp_path)
    _cache_table(app, "demo_project", "sales", "orders")
    app.cache.save_semantic(
        "demo_project",
        "orders",
        "Sales facts",
        [],
        [],
        [],
        schema_name="sales",
    )
    app.cache.save_semantic(
        "demo_project",
        "orders",
        "Wrong default facts",
        [],
        [],
        [],
        schema_name="default",
    )

    class OfflineBackend:
        @staticmethod
        def describe_table(*_args, **_kwargs):
            raise RuntimeError("offline")

    app.backend = OfflineBackend()
    result = app.meta_describe(
        "orders",
        project="demo_project",
        schema="sales",
    )

    assert result.status == "success"
    assert result.data["semantic"]["semantic_desc"] == "Sales facts"


def test_cached_search_results_are_unambiguous_across_schemas(tmp_path: Path) -> None:
    app = _app(tmp_path)
    app.config.default_schema = None

    class CachedThreeTierBackend:
        @staticmethod
        def list_schemas(*, project=None):
            return [{"name": "sales"}, {"name": "marketing"}]

        @staticmethod
        def catalog_search_tables(*_args, **_kwargs):
            return None

    app.backend = CachedThreeTierBackend()
    _cache_table(app, "demo_project", "sales", "orders")
    _cache_table(app, "demo_project", "marketing", "orders")

    tables = app.meta_search("orders", project="demo_project")
    columns = app.meta_search_columns("id", project="demo_project")

    assert {item["qualified_name"] for item in tables.data["matches"]} == {
        "sales.orders",
        "marketing.orders",
    }
    assert {item["schema_name"] for item in tables.data["matches"]} == {
        "sales",
        "marketing",
    }
    assert {item["qualified_name"] for item in columns.data["matches"]} == {
        "sales.orders",
        "marketing.orders",
    }
    assert "<table_name>" in tables.agent_hints.actions[0].command
    assert tables.agent_hints.actions[0].executable is False
