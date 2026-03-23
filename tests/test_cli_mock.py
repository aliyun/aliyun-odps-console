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
        - ds=2026-03-18
        - ds=2026-03-19
        - ds=2026-03-20
      upstream_tables:
        - dim_table
      downstream_tables:
        - sample_table_v2
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
    - name: dim_table
      description: dimension table
      row_count: 1
      owner: analytics
      created_at: "2026-03-01T00:00:00+00:00"
      updated_at: "2026-03-19T12:00:00+00:00"
      table_type: TABLE
      row_count_source: config
      size_bytes: 512
      columns:
        - name: dim_id
          type: string
          comment: dimension id
      sample_rows:
        - dim_id: "d1"
    - name: sample_table_v2
      description: evolved sample table
      row_count: 2
      owner: analytics
      created_at: "2026-03-01T00:00:00+00:00"
      updated_at: "2026-03-20T00:00:00+00:00"
      table_type: TABLE
      row_count_source: config
      size_bytes: 4096
      partition_columns:
        - name: ds
          type: string
          comment: biz date
      partitions:
        - ds=2026-03-20
      columns:
        - name: id
          type: bigint
          comment: primary key
        - name: full_label
          type: string
          comment: display label
        - name: score
          type: bigint
          comment: quality score
      sample_rows:
        - id: 1
          full_label: alice
          score: 100
        - id: 2
          full_label: bob
          score: 80
    - name: sample_table_shadow
      description: shadow sample table
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
      columns:
        - name: id
          type: string
          comment: primary key
        - name: name
          type: string
          comment: display name
      sample_rows:
        - id: "1"
          name: alice_v2
        - id: "3"
          name: carol
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


def test_query_can_write_output_file(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    output_path = tmp_path / "query.csv"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "query",
            "SELECT * FROM sample_table",
            "--json",
            "--output",
            str(output_path),
            "--output-format",
            "csv",
        ],
    )

    assert code == 0
    assert payload["metadata"]["output_format"] == "csv"
    assert payload["metadata"]["output_path"] == str(output_path.resolve())
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "id,name",
        "1,alice",
        "2,bob",
    ]


def test_query_cost_rejects_output_file_args(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    output_path = tmp_path / "cost.json"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "query",
            "cost",
            "SELECT id FROM sample_table",
            "--json",
            "--output",
            str(output_path),
        ],
    )

    assert code == 1
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"


def test_data_sample_supports_partition_and_columns(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "data",
            "sample",
            "sample_table",
            "--partition",
            "ds=2026-03-20",
            "--columns",
            "id,ds",
            "--rows",
            "1",
            "--json",
        ],
    )

    assert code == 0
    assert payload["command"] == "data.sample"
    assert payload["data"]["returned_rows"] == 1
    assert payload["data"]["applied_partition"] == "ds=2026-03-20"
    assert payload["data"]["selected_columns"] == ["id", "ds"]
    assert payload["data"]["rows"][0] == {"id": "1", "ds": "2026-03-20"}
    assert payload["data"]["schema"][1]["name"] == "ds"


def test_data_sample_rejects_unknown_partition(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "data",
            "sample",
            "sample_table",
            "--partition",
            "ds=2026-03-01",
            "--json",
        ],
    )

    assert code == 1
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"


def test_data_profile_supports_partition_and_top_values(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["data", "profile", "sample_table", "--partition", "ds=2026-03-20", "--json"],
    )

    assert code == 0
    assert payload["command"] == "data.profile"
    assert payload["data"]["applied_partition"] == "ds=2026-03-20"
    assert payload["data"]["sampled_rows"] == 2
    name_profile = next(item for item in payload["data"]["columns"] if item["name"] == "name")
    assert name_profile["null_ratio_in_sample"] == 0.0
    assert name_profile["distinct_count_in_sample"] == 2
    assert name_profile["top_values_in_sample"][0]["value"] == "alice"
    assert name_profile["top_values_in_sample"][0]["count"] == 1


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
    assert payload["data"]["total"] == 2
    assert {item["table_name"] for item in payload["data"]["matches"]} == {
        "sample_table",
        "sample_table_shadow",
    }
    assert all(item["column_name"] == "name" for item in payload["data"]["matches"])


def test_meta_latest_partition_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["meta", "latest-partition", "sample_table", "--json"],
    )

    assert code == 0
    assert payload["command"] == "meta.latest-partition"
    assert payload["data"]["has_partitions"] is True
    assert payload["data"]["latest_partition"] == "ds=2026-03-20"
    assert payload["data"]["latest_partition_values"] == {"ds": "2026-03-20"}
    assert payload["data"]["latest_partition_source"] == "table_definition_partitions"


def test_meta_freshness_with_mock_backend(tmp_path: Path, monkeypatch) -> None:
    config_path = write_mock_files(tmp_path)
    monkeypatch.setattr("maxc_cli.backend.now_utc_iso", lambda: "2026-03-20T12:00:00+00:00")

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["meta", "freshness", "sample_table", "--json"],
    )

    assert code == 0
    assert payload["command"] == "meta.freshness"
    assert payload["data"]["freshness_source"] == "latest_partition"
    assert payload["data"]["freshness_status"] == "fresh"
    assert payload["data"]["reference_time"] == "2026-03-20T00:00:00+00:00"
    assert payload["data"]["age_hours"] == 12.0
    assert payload["data"]["latest_partition"] == "ds=2026-03-20"


def test_meta_freshness_falls_back_to_updated_at_for_non_partitioned_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = write_mock_files(tmp_path)
    monkeypatch.setattr("maxc_cli.backend.now_utc_iso", lambda: "2026-03-20T12:00:00+00:00")

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["meta", "freshness", "dim_table", "--json"],
    )

    assert code == 0
    assert payload["data"]["freshness_source"] == "table_updated_at"
    assert payload["data"]["freshness_status"] == "fresh"
    assert payload["data"]["reference_time"] == "2026-03-19T12:00:00+00:00"
    assert payload["data"]["latest_partition"] is None
    assert "该表不是分区表" in payload["agent_hints"]["warnings"][0]


def test_meta_lineage_exposes_supported_contract_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["meta", "lineage", "sample_table", "--json"],
    )

    assert code == 0
    assert payload["command"] == "meta.lineage"
    assert payload["data"]["supported"] is True
    assert payload["data"]["lineage_source"] == "table_definition"
    assert payload["data"]["coverage"] == "declared"
    assert payload["data"]["upstream_tables"] == ["dim_table"]
    assert payload["data"]["downstream_tables"] == ["sample_table_v2"]
    assert payload["data"]["limitation"] is None


def test_auth_whoami_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "whoami", "--json"],
    )

    assert code == 0
    assert payload["command"] == "auth.whoami"
    assert payload["data"]["backend"] == "mock"
    assert payload["data"]["principal_display"] == "mock_user"
    assert payload["data"]["allowed_operations"] == ["SELECT"]


def test_auth_can_i_allows_select_for_configured_mock_table(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "can-i", "--table", "sample_table", "--operation", "SELECT", "--json"],
    )

    assert code == 0
    assert payload["command"] == "auth.can-i"
    assert payload["data"]["allowed"] is True
    assert payload["data"]["check_mode"] == "mock_table_lookup"


def test_auth_can_i_denies_operation_outside_allowed_operations(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "can-i", "--table", "sample_table", "--operation", "INSERT", "--json"],
    )

    assert code == 0
    assert payload["data"]["allowed"] is False
    assert payload["data"]["check_mode"] == "config_allowed_operations"
    assert payload["data"]["check_error_code"] == "PERMISSION_DENIED"


def test_diff_schema_with_identical_tables_is_compatible(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["diff", "schema", "sample_table", "sample_table", "--json"],
    )

    assert code == 0
    assert payload["command"] == "diff.schema"
    assert payload["data"]["compatible"] is True
    assert payload["data"]["summary"]["breaking_changes"] == 0


def test_diff_schema_detects_breaking_changes(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["diff", "schema", "sample_table", "sample_table_v2", "--json"],
    )

    assert code == 0
    assert payload["command"] == "diff.schema"
    assert payload["data"]["compatible"] is False
    assert payload["data"]["summary"]["breaking_changes"] >= 2
    assert payload["data"]["summary"]["non_breaking_changes"] >= 2
    assert payload["data"]["columns"]["removed"][0]["name"] == "name"
    assert payload["data"]["breaking_changes"][0]["scope"] == "columns"


def test_diff_partition_with_identical_partitions_is_compatible(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["diff", "partition", "sample_table", "sample_table", "--json"],
    )

    assert code == 0
    assert payload["command"] == "diff.partition"
    assert payload["data"]["compatible"] is True
    assert payload["data"]["summary"]["common"] == 3


def test_diff_partition_detects_missing_partitions(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["diff", "partition", "sample_table", "dim_table", "--json"],
    )

    assert code == 0
    assert payload["data"]["compatible"] is False
    assert payload["data"]["summary"]["left_only"] == 3
    assert payload["data"]["summary"]["right_only"] == 0


def test_diff_data_with_identical_snapshots_is_compatible(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "diff",
            "data",
            "sample_table",
            "sample_table",
            "--keys",
            "id",
            "--columns",
            "name",
            "--partition",
            "ds=2026-03-20",
            "--rows",
            "5",
            "--json",
        ],
    )

    assert code == 0
    assert payload["command"] == "diff.data"
    assert payload["data"]["comparison_mode"] == "keyed_snapshot"
    assert payload["data"]["compatible"] is True
    assert payload["data"]["summary"]["matched_keys"] == 2
    assert payload["data"]["summary"]["mismatched_rows"] == 0
    assert payload["agent_hints"]["warnings"][0].startswith("当前 data diff 只比较每侧最多")


def test_diff_data_detects_key_and_value_gaps(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "diff",
            "data",
            "sample_table",
            "sample_table_shadow",
            "--keys",
            "id",
            "--columns",
            "name",
            "--partition",
            "ds=2026-03-20",
            "--rows",
            "5",
            "--json",
        ],
    )

    assert code == 0
    assert payload["data"]["compatible"] is False
    assert payload["data"]["summary"]["matched_keys"] == 1
    assert payload["data"]["summary"]["mismatched_rows"] == 1
    assert payload["data"]["summary"]["left_only_keys"] == 1
    assert payload["data"]["summary"]["right_only_keys"] == 1
    assert payload["data"]["left_only_keys"] == [{"id": "2"}]
    assert payload["data"]["right_only_keys"] == [{"id": "3"}]
    assert payload["data"]["mismatched_rows"][0]["key"] == {"id": "1"}
    assert payload["data"]["mismatched_rows"][0]["differing_columns"] == ["name"]


def test_job_status_includes_diagnostic_fields_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, submit_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["job", "submit", "SELECT * FROM sample_table", "--json"],
    )
    assert code == 0

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["job", "status", str(submit_payload["data"]["job_id"]), "--json"],
    )

    assert code == 0
    assert payload["data"]["stage"] == "queue"
    assert payload["data"]["retryable"] is None
    assert payload["data"]["task_summary"]["operation"] == "SELECT"


def test_job_wait_includes_diagnostic_metadata_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, submit_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["job", "submit", "SELECT * FROM sample_table", "--json"],
    )
    assert code == 0

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["job", "wait", str(submit_payload["data"]["job_id"]), "--json"],
    )

    assert code == 0
    assert payload["metadata"]["stage"] == "completed"
    assert payload["metadata"]["failure_reason"] is None
    assert payload["metadata"]["task_summary"]["operation"] == "SELECT"


def test_job_diagnose_reports_cancelled_mock_job(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, submit_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["job", "submit", "SELECT * FROM sample_table", "--json"],
    )
    assert code == 0
    job_id = str(submit_payload["data"]["job_id"])

    code, _, _ = run_json_command(
        tmp_path,
        config_path,
        ["job", "cancel", job_id, "--json"],
    )
    assert code == 0

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["job", "diagnose", job_id, "--json"],
    )

    assert code == 0
    assert payload["command"] == "job.diagnose"
    assert payload["data"]["status"] == "failure"
    assert payload["data"]["diagnosis_category"] == "client_cancelled"
    assert payload["data"]["retryable"] is False


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
