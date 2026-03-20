from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

from maxc_cli.cli import run


MOCK_CONFIG = """
default_project: mock_project
default_format: json
state_dir: .maxc/state
skill_dirs:
  - .maxc/skills
backend:
  type: mock
allowed_operations:
  - SELECT
project_context: mock context
catalog:
  tables:
    - name: sample_table
      description: sample table
      row_count: 2
      owner: analytics
      created_at: "2026-03-01T00:00:00+00:00"
      updated_at: "2026-03-20T00:00:00+00:00"
      table_type: TABLE
      row_count_source: config
      size_bytes: 2048
      partition_columns:
        - name: ds
          type: string
          comment: biz date
      partitions:
        - ds=2026-03-20
      extra_metadata:
        lifecycle: 7
      columns:
        - name: id
          type: string
          comment: primary key
        - name: name
          type: string
          comment: display name
      sample_rows:
        - id: "1"
          name: alice
        - id: "2"
          name: bob
"""


def write_mock_files(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(MOCK_CONFIG, encoding="utf-8")

    skill_dir = tmp_path / ".maxc" / "skills"
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "maxc.data.query.yaml").write_text(
        """
skill:
  id: maxc.data.query
  name: query
  version: "0.1.0"
  input:
    schema:
      query:
        type: string
        required: true
  guards:
    allow_operations:
      - SELECT
  implementation:
    type: builtin
    target: query
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path


def run_json_command(tmp_path: Path, config_path: Path, argv: list[str]) -> tuple[int, dict[str, object], str]:
    stdout = StringIO()
    stderr = StringIO()

    code = run(
        ["--config", str(config_path), *argv],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    return code, json.loads(stdout.getvalue()), stderr.getvalue()


def test_query_json_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["query", "SELECT * FROM sample_table", "--json"],
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["metadata"]["tables_used"] == ["sample_table"]
    assert payload["data"]["returned_rows"] == 2


def test_query_pagination_with_cursor(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)

    code, first_page, _ = run_json_command(
        tmp_path,
        config_path,
        ["query", "SELECT * FROM sample_table", "--json", "--page-size", "1"],
    )

    assert code == 0
    assert first_page["data"]["returned_rows"] == 1
    assert first_page["data"]["has_more"] is True
    assert first_page["data"]["rows"][0]["name"] == "alice"

    code, second_page, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "query",
            "SELECT * FROM sample_table",
            "--json",
            "--page-size",
            "1",
            "--cursor",
            str(first_page["data"]["next_cursor"]),
        ],
    )

    assert code == 0
    assert second_page["data"]["returned_rows"] == 1
    assert second_page["data"]["has_more"] is False
    assert second_page["data"]["rows"][0]["name"] == "bob"
    assert second_page["metadata"]["current_offset"] == 1


def test_query_cost_and_explain_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)

    code, cost_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["query", "cost", "SELECT id FROM sample_table", "--json"],
    )

    assert code == 0
    assert cost_payload["command"] == "query.cost"
    assert cost_payload["data"]["cost_model"] == "maxc_derived"
    assert cost_payload["data"]["estimated_input_size_bytes"] >= 1024
    assert cost_payload["data"]["tables_used"] == ["sample_table"]

    code, explain_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["query", "--mode", "explain", "SELECT id FROM sample_table", "--json"],
    )

    assert code == 0
    assert explain_payload["command"] == "query.explain"
    assert explain_payload["data"]["analysis_mode"] == "explain"
    assert explain_payload["data"]["read_path"] is True


def test_meta_describe_json_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["meta", "describe", "sample_table", "--json"],
    )

    assert code == 0
    assert payload["data"]["table_name"] == "sample_table"
    assert payload["data"]["owner"] == "analytics"
    assert payload["data"]["table_type"] == "TABLE"
    assert payload["data"]["row_count_source"] == "config"
    assert payload["data"]["partition_columns"][0]["name"] == "ds"
    assert payload["data"]["extra_metadata"]["lifecycle"] == 7


def test_meta_search_columns_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["meta", "search-columns", "name", "--json"],
    )

    assert code == 0
    assert payload["command"] == "meta.search-columns"
    assert payload["data"]["total"] == 1
    assert payload["data"]["matches"][0]["column_name"] == "name"


def test_agent_skill_query_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "agent",
            "skill",
            "maxc.data.query",
            "--input",
            '{"query":"SELECT count(*) AS total_rows FROM sample_table"}',
            "--json",
        ],
    )
    assert code == 0
    assert payload["metadata"]["skill_id"] == "maxc.data.query"
    assert payload["data"]["output"]["rows"][0]["total_rows"] == 2
