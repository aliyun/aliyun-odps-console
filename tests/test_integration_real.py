"""基于真实 MaxCompute 环境的集成测试。

运行前请确保环境变量已配置:
- ALIBABA_CLOUD_ACCESS_KEY_ID
- ALIBABA_CLOUD_ACCESS_KEY_SECRET
- MAXCOMPUTE_PROJECT
- MAXCOMPUTE_ENDPOINT

运行方式:
    PYTHONPATH=src python -m pytest tests/test_integration_real.py -v

注意:
- 这些测试会在真实 MaxCompute 项目上执行只读操作
- 测试会在临时目录中构建独立 metadata cache
"""


import json
import os
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

from maxc_cli.cli import run


INTEGRATION_TABLE_ENV = "MAXC_INTEGRATION_TABLE"
ALLOW_CACHE_BUILD_ENV = "MAXC_INTEGRATION_ALLOW_CACHE_BUILD"


@pytest.fixture(scope="module")
def require_env() -> 'dict[str, str]':
    """检查必需的环境变量。"""
    required = [
        "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "MAXCOMPUTE_PROJECT",
        "MAXCOMPUTE_ENDPOINT",
    ]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip(f"缺少环境变量: {', '.join(missing)}")
    return {name: os.environ[name] for name in required}


@pytest.fixture(scope="module")
def tmp_config_dir(tmp_path_factory: 'pytest.TempPathFactory') -> 'Path':
    """创建临时配置目录。"""
    return tmp_path_factory.mktemp("maxc_config")


@pytest.fixture(scope="module")
def config_path(tmp_config_dir: 'Path', require_env: 'dict[str, str]') -> 'Path':
    """创建使用真实 backend 的配置文件。"""
    config_content = f"""
default_project: {require_env['MAXCOMPUTE_PROJECT']}
default_format: json
state_dir: .maxc/state
cache_dir: .maxc/cache
backend:
  type: auto
allowed_operations:
  - SELECT
project_context: 集成测试项目
"""
    config_file = tmp_config_dir / "config.yaml"
    config_file.write_text(config_content, encoding="utf-8")
    return config_file


@pytest.fixture
def run_cmd(config_path: 'Path', tmp_config_dir: 'Path'):
    """辅助函数：运行 CLI 命令并返回结果。"""
    skip_reason = None

    def _run(argv: 'list[str]') -> 'tuple[int, dict, str]':
        if skip_reason is not None:
            pytest.skip(skip_reason)
        stdout = StringIO()
        stderr = StringIO()
        code = run(
            ["--config", str(config_path), *argv],
            cwd=tmp_config_dir,
            stdout=stdout,
            stderr=stderr,
        )
        output = stdout.getvalue()
        try:
            data = json.loads(output) if output else {}
        except json.JSONDecodeError:
            data = {"raw_output": output}
        return code, data, stderr.getvalue()

    preflight_code, preflight_payload, _ = _run(["auth", "whoami", "--json"])
    if _is_backend_preflight_problem(preflight_payload):
        skip_reason = (
            "真实 MaxCompute backend 当前不可用，跳过依赖远端连接的集成测试: "
            + _backend_preflight_reason(preflight_payload)
        )
    if preflight_code != 0:
        error = preflight_payload.get("error")
        if isinstance(error, dict) and error.get("code") == "BACKEND_CONNECTION_ERROR":
            skip_reason = (
                "真实 MaxCompute backend 当前不可用，跳过依赖远端连接的集成测试: "
                + _backend_preflight_reason(preflight_payload)
            )

    return _run


def _payload_data(payload: 'dict') -> 'dict':
    return payload.get("data", {})


def _payload_warnings(payload: 'dict') -> 'list[str]':
    agent_hints = payload.get("agent_hints")
    if not isinstance(agent_hints, dict):
        return []
    warnings = agent_hints.get("warnings")
    if not isinstance(warnings, list):
        return []
    return [str(item) for item in warnings if item]


def _is_backend_preflight_problem(payload: 'dict') -> 'bool':
    error = payload.get("error")
    if isinstance(error, dict) and error.get("code") == "BACKEND_CONNECTION_ERROR":
        return True

    identity = _payload_data(payload).get("identity")
    if not isinstance(identity, dict):
        return False
    if identity.get("configured") is not True:
        return False
    if identity.get("authenticated") is not False:
        return False
    if identity.get("validation_status") != "failed":
        return False

    warning_text = "\n".join(_payload_warnings(payload)).lower()
    preflight_markers = (
        "httpconnectionpool(",
        "nameresolutionerror",
        "max retries exceeded",
        "failed to resolve",
        "connection refused",
        "connection timed out",
        "temporarily unavailable",
        "nodename nor servname provided",
        "project not found",
        "odps-0420111",
    )
    return any(marker in warning_text for marker in preflight_markers)


def _backend_preflight_reason(payload: 'dict') -> 'str':
    error = payload.get("error")
    if isinstance(error, dict) and error.get("message"):
        return str(error["message"])

    for warning in _payload_warnings(payload):
        lowered = warning.lower()
        if (
            "httpconnectionpool(" in lowered
            or "nameresolutionerror" in lowered
            or "failed to resolve" in lowered
            or "connection refused" in lowered
            or "connection timed out" in lowered
            or "project not found" in lowered
            or "odps-0420111" in lowered
        ):
            return warning
    return "MaxCompute backend preflight failed in the current environment."


def _allow_cache_build() -> 'bool':
    return os.environ.get(ALLOW_CACHE_BUILD_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _list_tables_or_skip(run_cmd) -> 'list[dict]':
    code, payload, stderr = run_cmd(["meta", "list-tables", "--json"])
    assert code == 0, f"list-tables 失败: {stderr}"

    if payload.get("status") == "cache_miss":
        if not _allow_cache_build():
            pytest.skip(
                f"冷缓存需要执行 cache build；如需覆盖此路径，请设置 {ALLOW_CACHE_BUILD_ENV}=1。"
            )
        build_code, build_payload, build_stderr = run_cmd(["cache", "build", "--json"])
        assert build_code == 0, f"cache build 失败: {build_stderr}"
        assert build_payload["status"] in {"success", "partial"}
        code, payload, stderr = run_cmd(["meta", "list-tables", "--json"])
        assert code == 0, f"list-tables 失败: {stderr}"

    if payload.get("status") != "success":
        pytest.skip(f"无法获取表列表: {payload.get('status')}")

    tables = _payload_data(payload).get("tables", [])
    if not tables:
        pytest.skip("没有可用表，跳过依赖表元数据的测试")
    return tables


def _first_table_name_or_skip(run_cmd) -> 'str':
    configured_table = os.environ.get(INTEGRATION_TABLE_ENV)
    if configured_table:
        return configured_table
    return _list_tables_or_skip(run_cmd)[0]["table_name"]


def _describe_table_or_skip(run_cmd, table_name: 'str') -> 'dict':
    code, payload, stderr = run_cmd(["meta", "describe", table_name, "--json"])
    if code != 0 or payload.get("status") != "success":
        pytest.skip(f"无法获取表结构，跳过测试: {stderr}")
    table = _payload_data(payload).get("table")
    if not table:
        pytest.skip("描述结果为空，跳过测试")
    return table


class TestAuth:
    """认证相关测试。"""

    def test_auth_whoami(self, run_cmd):
        code, data, stderr = run_cmd(["auth", "whoami", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        identity = _payload_data(data)["identity"]
        assert identity["authenticated"] is True
        assert identity["configured"] is True
        assert identity["validation_status"] == "verified"
        assert identity["backend"] == "odps"
        assert identity["project"] == os.environ["MAXCOMPUTE_PROJECT"]

    def test_auth_can_i_select_allowed(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)

        code, data, stderr = run_cmd(
            [
                "auth",
                "can-i",
                "--table",
                test_table,
                "--operation",
                "SELECT",
                "--json",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        authorization = _payload_data(data)["authorization"]
        assert authorization["table_name"] == test_table
        assert "allowed" in authorization


class TestMeta:
    """元数据相关测试。"""

    def test_meta_list_tables(self, run_cmd):
        tables = _list_tables_or_skip(run_cmd)
        assert isinstance(tables, list)

    def test_meta_describe(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        code, data, stderr = run_cmd(["meta", "describe", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        table = _payload_data(data)["table"]
        assert table["table_name"] == test_table
        assert "schema" in table
        assert "owner" in table
        assert "table_type" in table

    def test_meta_search(self, run_cmd):
        code, data, stderr = run_cmd(["meta", "search", "test", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "matches" in _payload_data(data)["search"]

    def test_meta_search_columns(self, run_cmd):
        code, data, stderr = run_cmd(["meta", "search-columns", "id", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "matches" in _payload_data(data)["search"]

    def test_meta_partitions(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        code, data, stderr = run_cmd(["meta", "partitions", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "partitions" in _payload_data(data)

    def test_meta_latest_partition(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        code, data, stderr = run_cmd(["meta", "latest-partition", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "has_partitions" in _payload_data(data)["partition"]

    def test_meta_freshness(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        code, data, stderr = run_cmd(["meta", "freshness", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "freshness_status" in _payload_data(data)["freshness"]


class TestQuery:
    """查询相关测试。"""

    def test_query_simple(self, run_cmd):
        code, data, stderr = run_cmd(
            [
                "query",
                "SELECT 1 AS col1, 'test' AS col2",
                "--json",
                "--max-rows",
                "10",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        result = _payload_data(data)["result"]
        assert len(result["rows"]) == 1
        assert result["rows"][0]["col1"] == 1

    def test_query_cost(self, run_cmd):
        code, data, stderr = run_cmd(["query", "cost", "SELECT 1 AS col1", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        analysis = _payload_data(data)["analysis"]
        assert "cost_model" in analysis
        assert "estimated_input_size_bytes" in analysis

    def test_query_explain(self, run_cmd):
        code, data, stderr = run_cmd(["query", "explain", "SELECT 1 AS col1", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        analysis = _payload_data(data)["analysis"]
        assert analysis["analysis_mode"] == "explain"

    def test_query_pagination(self, run_cmd):
        code, data, stderr = run_cmd(
            [
                "query",
                "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5",
                "--json",
                "--page-size",
                "2",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        result = _payload_data(data)["result"]
        pagination = _payload_data(data)["pagination"]
        assert result["returned_rows"] == 2
        assert pagination["has_more"] is True
        assert "next_cursor" in pagination

    def test_query_dry_run(self, run_cmd):
        code, data, stderr = run_cmd(["query", "SELECT 1", "--dry-run", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        result = _payload_data(data)["result"]
        assert result["returned_rows"] == 0


class TestJob:
    """任务管理相关测试。"""

    def test_job_submit_and_status(self, run_cmd):
        code, submit_data, stderr = run_cmd(["job", "submit", "SELECT 1 AS test_col", "--json"])
        assert code == 0, f"提交失败: {stderr}"
        assert submit_data["status"] in {"pending", "running", "success"}
        assert "job_id" in _payload_data(submit_data)

        job_id = _payload_data(submit_data)["job_id"]
        code, status_data, stderr = run_cmd(["job", "status", job_id, "--json"])
        assert code == 0, f"状态查询失败: {stderr}"
        assert status_data["status"] in {"pending", "running", "success", "failure"}
        job = _payload_data(status_data)["job"]
        assert "stage" in job
        assert "task_summary" in job

    def test_job_wait(self, run_cmd):
        code, submit_data, _ = run_cmd(["job", "submit", "SELECT 1", "--json"])
        assert code == 0
        job_id = _payload_data(submit_data)["job_id"]

        code, wait_data, stderr = run_cmd(["job", "wait", job_id, "--json"])
        assert code == 0, f"等待失败: {stderr}"
        assert wait_data["status"] in {"success", "failure"}

    def test_job_diagnose(self, run_cmd):
        code, submit_data, _ = run_cmd(["job", "submit", "SELECT 1", "--json"])
        assert code == 0
        job_id = _payload_data(submit_data)["job_id"]

        run_cmd(["job", "wait", job_id, "--json"])

        code, diag_data, stderr = run_cmd(["job", "diagnose", job_id, "--json"])
        assert code == 0, f"诊断失败: {stderr}"
        assert diag_data["status"] == "success"
        assert "diagnosis_category" in _payload_data(diag_data)["diagnosis"]

    def test_job_list(self, run_cmd):
        code, data, stderr = run_cmd(["job", "list", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert isinstance(_payload_data(data)["jobs"], list)

    def test_job_result(self, run_cmd):
        code, submit_data, _ = run_cmd(["job", "submit", "SELECT 1 AS result_col", "--json"])
        assert code == 0, "提交任务失败"
        job_id = _payload_data(submit_data)["job_id"]

        run_cmd(["job", "wait", job_id, "--json"])

        code, result_data, stderr = run_cmd(["job", "result", job_id, "--json"])
        assert code == 0, f"获取结果失败: {stderr}"
        assert result_data["status"] == "success"
        assert "rows" in _payload_data(result_data)["result"]

    def test_job_cancel_running(self, run_cmd):
        code, submit_data, _ = run_cmd(
            [
                "job",
                "submit",
                "SELECT COUNT(*) FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t",
                "--json",
            ]
        )
        assert code == 0, "提交任务失败"
        job_id = _payload_data(submit_data)["job_id"]

        code, cancel_data, stderr = run_cmd(["job", "cancel", job_id, "--json"])
        assert code in [0, 1, 4], f"取消命令格式错误: {stderr}"
        if code == 4:
            assert cancel_data["error"]["code"] in {"FEATURE_UNAVAILABLE", "SQL_ERROR"}


class TestData:
    """数据探查相关测试。"""

    def test_data_sample(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)

        code, data, stderr = run_cmd(["data", "sample", test_table, "--rows", "3", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "rows" in _payload_data(data)["sample"]

    def test_data_profile(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)

        code, data, stderr = run_cmd(["data", "profile", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "columns" in _payload_data(data)["profile"]


class TestDiff:
    """差异对比相关测试。"""

    def test_diff_schema_self(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        code, data, stderr = run_cmd(["diff", "schema", test_table, test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert _payload_data(data)["diff"]["compatible"] is True

    def test_diff_partition_self(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        code, data, stderr = run_cmd(["diff", "partition", test_table, test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        diff = _payload_data(data)["diff"]
        assert "left_table" in diff
        assert "right_table" in diff
        assert "summary" in diff

    def test_diff_data_self(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        table = _describe_table_or_skip(run_cmd, test_table)
        columns = table.get("schema", [])
        if not columns:
            pytest.skip("表没有列，跳过 diff data 测试")

        key_column = columns[0]["name"]
        code, data, stderr = run_cmd(
            [
                "diff",
                "data",
                test_table,
                test_table,
                "--keys",
                key_column,
                "--rows",
                "5",
                "--json",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert _payload_data(data)["diff"]["compatible"] is True


class TestAgent:
    """Agent 相关测试。"""

    def test_agent_context(self, run_cmd):
        code, data, stderr = run_cmd(["agent", "context", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "project" in _payload_data(data)["context"]

    def test_auth_login_from_env_without_validation(self, tmp_config_dir: 'Path'):
        target_config = tmp_config_dir / "login.yaml"
        stdout = StringIO()
        stderr = StringIO()
        code = run(
            [
                "--config",
                str(target_config),
                "auth",
                "login",
                "--from-env",
                "--no-validate",
                "--json",
            ],
            cwd=tmp_config_dir,
            stdout=stdout,
            stderr=stderr,
        )
        data = json.loads(stdout.getvalue())
        assert code == 0, f"命令失败: {stderr.getvalue()}"
        assert data["status"] == "success"
        assert data["command"] == "auth login"
        assert target_config.exists()


class TestOutputFormats:
    """输出格式相关测试。"""

    def test_output_csv(self, run_cmd, tmp_config_dir: 'Path'):
        output_path = tmp_config_dir / "test_output.csv"
        code, _, stderr = run_cmd(
            [
                "query",
                "SELECT 1 AS a, 2 AS b",
                "--json",
                "--output",
                str(output_path),
                "--output-format",
                "csv",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "a,b" in content or "1,2" in content

    def test_output_ndjson(self, run_cmd, tmp_config_dir: 'Path'):
        output_path = tmp_config_dir / "test_output.ndjson"
        code, _, stderr = run_cmd(
            [
                "query",
                "SELECT 1 AS a",
                "--json",
                "--output",
                str(output_path),
                "--output-format",
                "ndjson",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8").strip()
        lines = content.split("\n")
        assert len(lines) >= 1
        json.loads(lines[0])


class TestSession:
    """会话相关测试。"""

    def test_session_show(self, run_cmd):
        code, data, stderr = run_cmd(["session", "show", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        payload = _payload_data(data)
        assert "project" in payload
        assert "schema" in payload

    def test_session_set_same_project(self, run_cmd):
        code, ctx_data, _ = run_cmd(["agent", "context", "--json"])
        assert code == 0
        current_project = _payload_data(ctx_data)["context"]["project"]

        code, data, stderr = run_cmd(["session", "set", "--project", current_project, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert _payload_data(data)["project"] == current_project


class TestCache:
    """缓存相关测试。"""

    def test_cache_status(self, run_cmd):
        code, data, stderr = run_cmd(["cache", "status", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "table_count" in _payload_data(data)

    def test_cache_build_small(self, run_cmd):
        if not _allow_cache_build():
            pytest.skip(f"该测试会触发整项目 cache build；如需执行，请设置 {ALLOW_CACHE_BUILD_ENV}=1。")
        run_cmd(["cache", "clear", "--json"])

        code, data, stderr = run_cmd(["cache", "build", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] in ["success", "partial"]
        assert "cached_tables" in _payload_data(data)
        assert "total_tables" in _payload_data(data)

    def test_cache_clear(self, run_cmd):
        code, data, stderr = run_cmd(["cache", "clear", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "deleted_tables" in _payload_data(data)


class TestMetaExtended:
    """元数据扩展测试。"""

    def test_meta_list_projects(self, run_cmd):
        code, data, stderr = run_cmd(["meta", "list-projects", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert isinstance(_payload_data(data)["projects"], list)

    def test_meta_list_schemas(self, run_cmd):
        code, data, stderr = run_cmd(["meta", "list-schemas", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert isinstance(_payload_data(data)["schemas"], list)


class TestQueryExtended:
    """查询扩展测试。"""

    def test_query_wait_0_mode(self, run_cmd):
        code, data, stderr = run_cmd(["query", "SELECT 1 AS async_test", "--wait", "0", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "pending"
        assert "job_id" in _payload_data(data)["job"]

    def test_query_with_file_output(self, run_cmd, tmp_config_dir: 'Path'):
        output_path = tmp_config_dir / "query_output.json"
        code, _, stderr = run_cmd(
            [
                "query",
                "SELECT 1 AS file_test, 2 AS file_test2",
                "--json",
                "--output",
                str(output_path),
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert output_path.exists()

    def test_query_with_specific_columns(self, run_cmd):
        code, data, stderr = run_cmd(
            [
                "query",
                "SELECT 1 AS col_a, 'test' AS col_b, 3.14 AS col_c",
                "--json",
                "--max-rows",
                "1",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        result = _payload_data(data)["result"]
        assert len(result["rows"]) == 1
        assert result["rows"][0]["col_a"] == 1
        assert result["rows"][0]["col_b"] == "test"


class TestDataExtended:
    """数据探查扩展测试。"""

    def test_data_sample_with_columns(self, run_cmd):
        test_table = _first_table_name_or_skip(run_cmd)
        table = _describe_table_or_skip(run_cmd, test_table)
        columns = table.get("schema", [])
        if not columns:
            pytest.skip("表没有列")

        col_names = [column["name"] for column in columns[:2]]
        code, data, stderr = run_cmd(
            [
                "data",
                "sample",
                test_table,
                "--rows",
                "3",
                "--columns",
                ",".join(col_names),
                "--json",
            ]
        )
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "rows" in _payload_data(data)["sample"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
