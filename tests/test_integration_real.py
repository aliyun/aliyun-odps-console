"""基于真实 MaxCompute 环境的集成测试。

运行前请确保环境变量已配置:
- ALIBABA_CLOUD_ACCESS_KEY_ID
- ALIBABA_CLOUD_ACCESS_KEY_SECRET
- MAXCOMPUTE_PROJECT
- MAXCOMPUTE_ENDPOINT

运行方式:
    PYTHONPATH=src python -m pytest tests/test_integration_real.py -v

注意: 这些测试会在真实 MaxCompute 项目上执行只读操作，请确保有相应权限。
"""

from __future__ import annotations

import json
import os
from io import StringIO
from pathlib import Path

import pytest

from maxc_cli.cli import run


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def require_env() -> dict[str, str]:
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
def tmp_config_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """创建临时配置目录。"""
    return tmp_path_factory.mktemp("maxc_config")


@pytest.fixture(scope="module")
def config_path(tmp_config_dir: Path, require_env: dict[str, str]) -> Path:
    """创建使用 auto backend 的配置文件。"""
    config_content = f"""
default_project: {require_env['MAXCOMPUTE_PROJECT']}
default_format: json
state_dir: .maxc/state
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
def run_cmd(config_path: Path, tmp_config_dir: Path):
    """辅助函数：运行 CLI 命令并返回结果。"""
    def _run(argv: list[str]) -> tuple[int, dict, str]:
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
    return _run


# =============================================================================
# Auth 测试
# =============================================================================

class TestAuth:
    """认证相关测试。"""

    def test_auth_whoami(self, run_cmd):
        """测试 whoami 命令。"""
        code, data, stderr = run_cmd(["auth", "whoami", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["data"]["authenticated"] is True
        assert data["data"]["backend"] == "odps"
        assert data["data"]["project"] == os.environ["MAXCOMPUTE_PROJECT"]

    def test_auth_can_i_select_allowed(self, run_cmd):
        """测试 can-i 对 SELECT 权限检查。"""
        # 先获取一个存在的表
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过权限检查测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd([
            "auth", "can-i",
            "--table", test_table,
            "--operation", "SELECT",
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "allowed" in data["data"]


# =============================================================================
# Meta 测试
# =============================================================================

class TestMeta:
    """元数据相关测试。"""

    def test_meta_list_tables(self, run_cmd):
        """测试 list-tables 命令。"""
        code, data, stderr = run_cmd(["meta", "list-tables", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "tables" in data["data"]
        assert isinstance(data["data"]["tables"], list)

    def test_meta_describe(self, run_cmd):
        """测试 describe 命令。"""
        # 先获取一个存在的表
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 describe 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd(["meta", "describe", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["data"]["table_name"] == test_table
        assert "schema" in data["data"]
        assert "owner" in data["data"]
        assert "table_type" in data["data"]

    def test_meta_search(self, run_cmd):
        """测试 search 命令。"""
        code, data, stderr = run_cmd(["meta", "search", "test", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "matches" in data["data"]

    def test_meta_search_columns(self, run_cmd):
        """测试 search-columns 命令。"""
        code, data, stderr = run_cmd(["meta", "search-columns", "id", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "matches" in data["data"]

    def test_meta_partitions(self, run_cmd):
        """测试 partitions 命令。"""
        # 先获取一个存在的表
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 partitions 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd(["meta", "partitions", test_table, "--json"])
        # 非分区表可能返回空列表，但命令应该成功
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"

    def test_meta_latest_partition(self, run_cmd):
        """测试 latest-partition 命令。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 latest-partition 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd(["meta", "latest-partition", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "has_partitions" in data["data"]

    def test_meta_freshness(self, run_cmd):
        """测试 freshness 命令。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 freshness 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd(["meta", "freshness", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "freshness_status" in data["data"]

    def test_meta_lineage(self, run_cmd):
        """测试 lineage 命令（预期返回 unsupported）。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 lineage 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd(["meta", "lineage", test_table, "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        # 真实 backend 应该返回 supported=False
        assert "supported" in data["data"]


# =============================================================================
# Query 测试
# =============================================================================

class TestQuery:
    """查询相关测试。"""

    def test_query_simple(self, run_cmd):
        """测试简单查询。"""
        code, data, stderr = run_cmd([
            "query", "SELECT 1 AS col1, 'test' AS col2",
            "--json", "--max-rows", "10"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "rows" in data["data"]
        assert len(data["data"]["rows"]) == 1
        assert data["data"]["rows"][0]["col1"] == 1

    def test_query_cost(self, run_cmd):
        """测试 query cost 命令。"""
        code, data, stderr = run_cmd([
            "query", "cost", "SELECT 1 AS col1",
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "cost_model" in data["data"]
        assert "estimated_input_size_bytes" in data["data"]

    def test_query_explain(self, run_cmd):
        """测试 query explain 命令。"""
        code, data, stderr = run_cmd([
            "query", "explain", "SELECT 1 AS col1",
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "analysis_mode" in data["data"]
        assert data["data"]["analysis_mode"] == "explain"

    def test_query_pagination(self, run_cmd):
        """测试查询分页。"""
        # 使用 UNION ALL 生成多行数据，避免依赖 VALUES 方言支持
        code, data, stderr = run_cmd([
            "query",
            "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5",
            "--json", "--page-size", "2"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["data"]["returned_rows"] == 2
        assert data["data"]["has_more"] is True
        assert "next_cursor" in data["data"]

    def test_query_dry_run(self, run_cmd):
        """测试 dry-run 模式。"""
        code, data, stderr = run_cmd([
            "query", "SELECT 1",
            "--dry-run", "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["data"]["returned_rows"] == 0


# =============================================================================
# Job 测试
# =============================================================================

class TestJob:
    """任务管理相关测试。"""

    def test_job_submit_and_status(self, run_cmd):
        """测试任务提交和状态查询。"""
        # 提交异步任务
        code, submit_data, stderr = run_cmd([
            "job", "submit", "SELECT 1 AS test_col",
            "--json"
        ])
        assert code == 0, f"提交失败: {stderr}"
        assert submit_data["status"] in {"pending", "running", "success"}
        assert "job_id" in submit_data["data"]

        job_id = submit_data["data"]["job_id"]

        # 查询任务状态
        code, status_data, stderr = run_cmd([
            "job", "status", job_id, "--json"
        ])
        assert code == 0, f"状态查询失败: {stderr}"
        assert status_data["status"] == "success"
        assert "stage" in status_data["data"]
        assert "task_summary" in status_data["data"]

    def test_job_wait(self, run_cmd):
        """测试 job wait 命令。"""
        # 提交异步任务
        code, submit_data, _ = run_cmd([
            "job", "submit", "SELECT 1",
            "--json"
        ])
        assert code == 0
        job_id = submit_data["data"]["job_id"]

        # 等待任务完成
        code, wait_data, stderr = run_cmd([
            "job", "wait", job_id, "--json"
        ])
        assert code == 0, f"等待失败: {stderr}"
        assert wait_data["status"] in ["success", "failure"]

    def test_job_diagnose(self, run_cmd):
        """测试 job diagnose 命令。"""
        # 提交并等待任务
        code, submit_data, _ = run_cmd([
            "job", "submit", "SELECT 1",
            "--json"
        ])
        assert code == 0
        job_id = submit_data["data"]["job_id"]

        # 等待完成
        run_cmd(["job", "wait", job_id, "--json"])

        # 诊断任务
        code, diag_data, stderr = run_cmd([
            "job", "diagnose", job_id, "--json"
        ])
        assert code == 0, f"诊断失败: {stderr}"
        assert diag_data["status"] == "success"
        assert "diagnosis_category" in diag_data["data"]

    def test_job_list(self, run_cmd):
        """测试 job list 命令。"""
        code, data, stderr = run_cmd(["job", "list", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "jobs" in data["data"]
        assert isinstance(data["data"]["jobs"], list)

    def test_job_result(self, run_cmd):
        """测试 job result 命令。"""
        # 提交并等待任务完成
        code, submit_data, _ = run_cmd([
            "job", "submit", "SELECT 1 AS result_col",
            "--json"
        ])
        assert code == 0, "提交任务失败"
        job_id = submit_data["data"]["job_id"]

        # 等待任务完成
        run_cmd(["job", "wait", job_id, "--json"])

        # 获取任务结果
        code, result_data, stderr = run_cmd([
            "job", "result", job_id, "--json"
        ])
        assert code == 0, f"获取结果失败: {stderr}"
        assert result_data["status"] == "success"
        assert "rows" in result_data["data"]

    def test_job_cancel_running(self, run_cmd):
        """测试 job cancel 命令。"""
        # 提交一个耗时较长的查询
        code, submit_data, _ = run_cmd([
            "job", "submit", "SELECT COUNT(*) FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t",
            "--json"
        ])
        assert code == 0, "提交任务失败"
        job_id = submit_data["data"]["job_id"]

        # 尝试取消任务（可能已完成，所以只验证命令格式）
        code, cancel_data, stderr = run_cmd([
            "job", "cancel", job_id, "--json"
        ])
        # 取消可能成功也可能失败（任务已完成），但命令格式应该正确
        assert code in [0, 1], f"取消命令格式错误: {stderr}"


# =============================================================================
# Data 测试
# =============================================================================

class TestData:
    """数据探查相关测试。"""

    def test_data_sample(self, run_cmd):
        """测试 data sample 命令。"""
        # 先获取一个存在的表
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 sample 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd([
            "data", "sample", test_table,
            "--rows", "3",
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "rows" in data["data"]

    def test_data_profile(self, run_cmd):
        """测试 data profile 命令。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 profile 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd([
            "data", "profile", test_table,
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "columns" in data["data"]


# =============================================================================
# Diff 测试
# =============================================================================

class TestDiff:
    """差异对比相关测试。"""

    def test_diff_schema_self(self, run_cmd):
        """测试 diff schema 命令（表与自身比较）。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 diff 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd([
            "diff", "schema", test_table, test_table,
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["data"]["compatible"] is True

    def test_diff_partition_self(self, run_cmd):
        """测试 diff partition 命令（表与自身比较）。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 diff partition 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        code, data, stderr = run_cmd([
            "diff", "partition", test_table, test_table,
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "left_table" in data["data"]
        assert "right_table" in data["data"]
        assert "summary" in data["data"]

    def test_diff_data_self(self, run_cmd):
        """测试 diff data 命令（表与自身比较）。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 diff data 测试")

        tables = list_data["data"]["tables"]
        test_table = tables[0]["table_name"]

        # 先获取表结构以确定可用列
        code, desc_data, _ = run_cmd(["meta", "describe", test_table, "--json"])
        if code != 0 or not desc_data.get("data", {}).get("schema"):
            pytest.skip("无法获取表结构，跳过 diff data 测试")

        columns = desc_data["data"]["schema"]
        if not columns:
            pytest.skip("表没有列，跳过 diff data 测试")

        key_column = columns[0]["name"]

        code, data, stderr = run_cmd([
            "diff", "data", test_table, test_table,
            "--keys", key_column,
            "--rows", "5",
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["data"]["compatible"] is True  # 自比较应该兼容


# =============================================================================
# Agent 测试
# =============================================================================

class TestAgent:
    """Agent 相关测试。"""

    def test_agent_context(self, run_cmd):
        """测试 agent context 命令。"""
        code, data, stderr = run_cmd(["agent", "context", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "project" in data["data"]

    def test_auth_login_from_env_without_validation(self, tmp_config_dir: Path):
        """测试 auth login 可以写入新的配置文件。"""
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
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["command"] == "auth.login"
        assert target_config.exists()


# =============================================================================
# 输出格式测试
# =============================================================================

class TestOutputFormats:
    """输出格式相关测试。"""

    def test_output_csv(self, run_cmd, tmp_config_dir: Path):
        """测试 CSV 输出。"""
        output_path = tmp_config_dir / "test_output.csv"
        code, data, stderr = run_cmd([
            "query", "SELECT 1 AS a, 2 AS b",
            "--json",
            "--output", str(output_path),
            "--output-format", "csv"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "a,b" in content or "1,2" in content

    def test_output_ndjson(self, run_cmd, tmp_config_dir: Path):
        """测试 NDJSON 输出。"""
        output_path = tmp_config_dir / "test_output.ndjson"
        code, data, stderr = run_cmd([
            "query", "SELECT 1 AS a",
            "--json",
            "--output", str(output_path),
            "--output-format", "ndjson"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8").strip()
        # NDJSON 每行是一个 JSON 对象
        lines = content.split("\n")
        assert len(lines) >= 1
        json.loads(lines[0])  # 验证是有效 JSON


# =============================================================================
# Project 测试
# =============================================================================

class TestProject:
    """项目相关测试。"""

    def test_project_info(self, run_cmd):
        """测试 project info 命令。"""
        code, data, stderr = run_cmd(["project", "info", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "name" in data["data"]
        assert "owner" in data["data"]

    def test_project_use_same_project(self, run_cmd):
        """测试 project use 命令（切换到当前项目）。"""
        # 获取当前项目名
        code, ctx_data, _ = run_cmd(["agent", "context", "--json"])
        assert code == 0
        current_project = ctx_data["data"]["project"]

        # 切换到当前项目（实际上是幂等操作）
        code, data, stderr = run_cmd([
            "project", "use", current_project, "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert data["data"]["project"] == current_project


# =============================================================================
# Cache 测试
# =============================================================================

class TestCache:
    """缓存相关测试。"""

    def test_cache_status(self, run_cmd):
        """测试 cache status 命令。"""
        code, data, stderr = run_cmd(["cache", "status", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "table_count" in data["data"]

    def test_cache_build_small(self, run_cmd):
        """测试 cache build 命令（构建缓存）。"""
        # 先清除缓存
        run_cmd(["cache", "clear", "--json"])

        # 构建缓存
        code, data, stderr = run_cmd(["cache", "build", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] in ["success", "partial"]
        assert "cached_tables" in data["data"]
        assert "total_tables" in data["data"]

    def test_cache_clear(self, run_cmd):
        """测试 cache clear 命令。"""
        code, data, stderr = run_cmd(["cache", "clear", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "deleted_tables" in data["data"]

    def test_cache_save_and_get_semantic(self, run_cmd):
        """测试 cache save-semantic 和 get-semantic 命令。"""
        # 获取一个表名
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 semantic 测试")

        test_table = list_data["data"]["tables"][0]["table_name"]

        # 保存语义元数据
        code, save_data, stderr = run_cmd([
            "cache", "save-semantic",
            "--table", test_table,
            "--semantic-desc", "测试表的语义描述",
            "--use-cases", '["数据分析", "报表统计"]',
            "--sample-questions", '["查询最近一天的数据"]',
            "--column-semantics", '[]',
            "--json"
        ])
        assert code == 0, f"保存语义失败: {stderr}"
        assert save_data["status"] == "success"
        assert save_data["data"]["table_name"] == test_table

        # 获取语义元数据
        code, get_data, stderr = run_cmd([
            "cache", "get-semantic",
            "--table", test_table,
            "--json"
        ])
        assert code == 0, f"获取语义失败: {stderr}"
        assert get_data["status"] == "success"
        assert get_data["data"]["semantic_desc"] == "测试表的语义描述"


# =============================================================================
# Meta 更多测试
# =============================================================================

class TestMetaExtended:
    """元数据扩展测试。"""

    def test_meta_list_projects(self, run_cmd):
        """测试 list-projects 命令。"""
        code, data, stderr = run_cmd(["meta", "list-projects", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "projects" in data["data"]
        assert isinstance(data["data"]["projects"], list)

    def test_meta_list_schemas(self, run_cmd):
        """测试 list-schemas 命令。"""
        code, data, stderr = run_cmd(["meta", "list-schemas", "--json"])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "schemas" in data["data"]
        assert isinstance(data["data"]["schemas"], list)


# =============================================================================
# Query 更多测试
# =============================================================================

class TestQueryExtended:
    """查询扩展测试。"""

    def test_query_async_mode(self, run_cmd):
        """测试异步查询模式。"""
        code, data, stderr = run_cmd([
            "query", "SELECT 1 AS async_test",
            "--async", "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] in ["pending", "running", "success"]
        assert "job_id" in data["data"]

    def test_query_with_file_output(self, run_cmd, tmp_config_dir: Path):
        """测试查询结果输出到文件。"""
        output_path = tmp_config_dir / "query_output.json"
        code, data, stderr = run_cmd([
            "query", "SELECT 1 AS file_test, 2 AS file_test2",
            "--json",
            "--output", str(output_path)
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert output_path.exists()

    def test_query_with_specific_columns(self, run_cmd):
        """测试查询指定列。"""
        # 简单查询验证返回结果结构
        code, data, stderr = run_cmd([
            "query", "SELECT 1 AS col_a, 'test' AS col_b, 3.14 AS col_c",
            "--json", "--max-rows", "1"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert len(data["data"]["rows"]) == 1
        assert data["data"]["rows"][0]["col_a"] == 1
        assert data["data"]["rows"][0]["col_b"] == "test"


# =============================================================================
# Data 更多测试
# =============================================================================

class TestDataExtended:
    """数据探查扩展测试。"""

    def test_data_sample_with_columns(self, run_cmd):
        """测试 data sample 指定列。"""
        code, list_data, _ = run_cmd(["meta", "list-tables", "--json"])
        if code != 0 or not list_data.get("data", {}).get("tables"):
            pytest.skip("无法获取表列表，跳过 sample columns 测试")

        test_table = list_data["data"]["tables"][0]["table_name"]

        # 获取表结构
        code, desc_data, _ = run_cmd(["meta", "describe", test_table, "--json"])
        if code != 0 or not desc_data.get("data", {}).get("schema"):
            pytest.skip("无法获取表结构")

        columns = desc_data["data"]["schema"]
        if len(columns) < 1:
            pytest.skip("表没有列")

        # 只选择前两列
        col_names = [c["name"] for c in columns[:2]]
        columns_str = ",".join(col_names)

        code, data, stderr = run_cmd([
            "data", "sample", test_table,
            "--rows", "3",
            "--columns", columns_str,
            "--json"
        ])
        assert code == 0, f"命令失败: {stderr}"
        assert data["status"] == "success"
        assert "rows" in data["data"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
