from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from difflib import get_close_matches
from itertools import islice
import os
from pathlib import Path
import re
from time import monotonic
from typing import Any, Iterable

from .config import MaxCConfig, TableColumn, TableDefinition
from .exceptions import (
    BackendConnectionError,
    FeatureUnavailableError,
    MaxCError,
    NotFoundError,
    PermissionDeniedError,
    SqlError,
    ValidationError,
)
from .utils import (
    detect_operation,
    encode_cursor,
    extract_table_names,
    normalize_sql,
    now_utc_iso,
    parse_select_projection,
    projection_alias,
)

try:
    from odps import ODPS
    from odps.errors import InvalidParameter as OdpsInvalidParameter
    from odps.errors import NoPermission as OdpsNoPermission
    from odps.errors import NoSuchObject as OdpsNoSuchObject
    from odps.errors import ODPSError
except Exception:  # pragma: no cover
    ODPS = None
    OdpsInvalidParameter = Exception
    OdpsNoPermission = Exception
    OdpsNoSuchObject = Exception
    ODPSError = Exception


ODPS_ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "access_id": (
        "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ODPS_ACCESS_ID",
        "ACCESS_KEY_ID",
    ),
    "secret_access_key": (
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "ODPS_ACCESS_KEY",
        "ODPS_ACCESS_KEY_SECRET",
        "ACCESS_KEY_SECRET",
    ),
    "project": ("MAXCOMPUTE_PROJECT", "ODPS_PROJECT"),
    "endpoint": ("MAXCOMPUTE_ENDPOINT", "ODPS_ENDPOINT", "odps_endpoint"),
    "region_name": ("MAXCOMPUTE_REGION", "ALIBABA_CLOUD_REGION"),
    "tunnel_endpoint": ("MAXCOMPUTE_TUNNEL_ENDPOINT", "ODPS_TUNNEL_ENDPOINT"),
}

PARTITION_PATH_RE = re.compile(r"/(?=[A-Za-z_][\w]*\s*=)")
FRESHNESS_FRESH_HOURS = 36
FRESHNESS_STALE_HOURS = 72


@dataclass(slots=True)
class QueryResult:
    rows: list[dict[str, Any]]
    schema: list[dict[str, Any]]
    total_rows: int
    returned_rows: int
    has_more: bool
    next_cursor: str | None
    elapsed_ms: int
    bytes_scanned: int | None
    cost_cu: float | None
    project: str
    sql_executed: str
    tables_used: list[str]
    warnings: list[str] = field(default_factory=list)
    job_id: str | None = None
    submitted_at: str | None = None
    completed_at: str | None = None
    extra_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobInfo:
    job_id: str
    status: str
    project: str
    progress: int
    stage: str | None = None
    retryable: bool | None = None
    failure_reason: str | None = None
    task_summary: dict[str, Any] = field(default_factory=dict)
    sql: str | None = None
    submitted_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None
    logview: str | None = None
    error_message: str | None = None
    warnings: list[str] = field(default_factory=list)


class BaseBackend:
    supports_remote_jobs = False
    supports_cost_check = True

    def estimate_query_cost(self, sql: str, *, project: str) -> dict[str, Any]:
        raise NotImplementedError

    def explain_query(self, sql: str, *, project: str) -> dict[str, Any]:
        raise NotImplementedError

    def search_columns(self, keyword: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def latest_partition_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        table = self.describe_table(table_name)
        return build_latest_partition_info(
            table,
            source="table_definition_partitions",
        )

    def freshness_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        table = self.describe_table(table_name)
        latest_payload, warnings = build_latest_partition_info(
            table,
            source="table_definition_partitions",
        )
        return build_freshness_info(table, latest_payload, warnings=warnings)

    def lineage_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        table = self.describe_table(table_name)
        return (
            {
                "table_name": table.name,
                "supported": True,
                "lineage_source": "table_definition",
                "coverage": "declared",
                "upstream_tables": table.upstream_tables,
                "downstream_tables": table.downstream_tables,
                "limitation": None,
            },
            [],
        )

    def whoami_info(self, *, project: str | None = None) -> tuple[dict[str, Any], list[str]]:
        raise NotImplementedError

    def can_i_info(
        self,
        *,
        table_name: str,
        operation: str,
        project: str | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        raise NotImplementedError

    def diagnose_job(self, job_id: str, *, project: str | None = None) -> dict[str, Any]:
        raise NotImplementedError


def create_backend(config: MaxCConfig) -> BaseBackend:
    backend_type = config.backend.type.lower()
    if backend_type == "auto":
        if odps_env_available():
            return OdpsBackend(config)
        return MockBackend(config)
    if backend_type in {"odps", "maxcompute"}:
        return OdpsBackend(config)
    if backend_type == "mock":
        return MockBackend(config)
    raise FeatureUnavailableError(f"不支持的 backend 类型: {config.backend.type}")


def odps_env_available() -> bool:
    settings = load_odps_env()
    return bool(
        settings.get("access_id")
        and settings.get("secret_access_key")
        and settings.get("project")
        and settings.get("endpoint")
    )


def load_odps_env() -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for field, aliases in ODPS_ENV_ALIASES.items():
        value = None
        for alias in aliases:
            candidate = os.environ.get(alias)
            if candidate:
                value = candidate
                break
        values[field] = value
    return values


class MockBackend(BaseBackend):
    def __init__(self, config: MaxCConfig) -> None:
        self.config = config

    def execute_query(
        self,
        sql: str,
        *,
        project: str,
        max_rows: int,
        dry_run: bool,
        offset: int = 0,
    ) -> QueryResult:
        self._validate_select(sql)

        tables = extract_table_names(sql)
        table_defs = [self._get_table(name) for name in tables] if tables else []
        projected_rows, schema, warnings, total_rows = self._materialize_rows(
            sql,
            table_defs,
            max_rows,
            offset,
        )
        returned_rows = 0 if dry_run else len(projected_rows)
        bytes_scanned = self._estimate_bytes_scanned(table_defs, projected_rows)
        cost_cu = round(max(bytes_scanned / 5_000_000, 0.01), 2)
        if dry_run:
            projected_rows = []
            warnings = warnings + ["dry-run 模式未真正执行 SQL，只返回预估信息。"]

        has_more = total_rows > (offset + returned_rows)
        next_cursor = encode_cursor(offset + returned_rows) if has_more and returned_rows else None
        elapsed_ms = 80 + len(tables) * 35 + len(projected_rows) * 5

        return QueryResult(
            rows=projected_rows,
            schema=schema,
            total_rows=total_rows,
            returned_rows=returned_rows,
            has_more=has_more,
            next_cursor=next_cursor,
            elapsed_ms=elapsed_ms,
            bytes_scanned=bytes_scanned,
            cost_cu=cost_cu,
            project=project,
            sql_executed=sql,
            tables_used=tables,
            warnings=warnings,
            extra_metadata={"current_offset": offset},
        )

    def estimate_query_cost(self, sql: str, *, project: str) -> dict[str, Any]:
        self._validate_select(sql)
        tables = extract_table_names(sql)
        table_defs = [self._get_table(name) for name in tables] if tables else []
        projected_columns = parse_select_projection(sql)
        bytes_scanned = self._estimate_bytes_scanned(table_defs, [])
        task_cost_cpu = max(1, bytes_scanned // 4096)
        task_cost_memory = max(64, max(len(projected_columns), 1) * 32)
        total_row_estimate = table_defs[0].row_count if table_defs else 1
        return {
            **build_query_outline(sql),
            "project": project,
            "estimated_input_size_bytes": bytes_scanned,
            "task_cost_cpu": task_cost_cpu,
            "task_cost_memory": task_cost_memory,
            "cost_cu": round(max(bytes_scanned / 5_000_000, 0.01), 2),
            "cost_model": "maxc_derived",
            "sql_complexity": round(1.0 + len(tables) * 0.5 + len(projected_columns) * 0.1, 2),
            "sql_udf_num": 0,
            "total_row_estimate": total_row_estimate,
            "warnings": ["mock backend 返回的是派生估算，不是 MaxCompute 原生计费。"],
        }

    def explain_query(self, sql: str, *, project: str) -> dict[str, Any]:
        estimate = self.estimate_query_cost(sql, project=project)
        warnings = list(estimate.pop("warnings", []))
        estimate["warnings"] = warnings
        estimate["analysis_mode"] = "explain"
        estimate["read_path"] = True
        return estimate

    def list_tables(self) -> list[TableDefinition]:
        return sorted(self.config.catalog.values(), key=lambda item: item.name)

    def describe_table(self, table_name: str) -> TableDefinition:
        return self._get_table(table_name)

    def search_tables(self, keyword: str) -> list[dict[str, Any]]:
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: list[dict[str, Any]] = []
        for table in self.list_tables():
            score = 0
            matched_columns: list[str] = []
            searchable = f"{table.name} {table.description}".lower()
            for token in tokens:
                if token in searchable:
                    score += 5
                for column in table.columns:
                    text = f"{column.name} {column.comment}".lower()
                    if token in text:
                        score += 2
                        matched_columns.append(column.name)
            if score:
                matches.append(
                    {
                        "table_name": table.name,
                        "description": table.description,
                        "score": score,
                        "matched_columns": sorted(set(matched_columns)),
                    }
                )
        return sorted(matches, key=lambda item: (-item["score"], item["table_name"]))

    def search_columns(self, keyword: str) -> list[dict[str, Any]]:
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: list[dict[str, Any]] = []
        for table in self.list_tables():
            for column in table.columns:
                score = 0
                text = f"{column.name} {column.comment}".lower()
                searchable = f"{table.name} {text}".lower()
                for token in tokens:
                    if token in column.name.lower():
                        score += 8
                    if token in text:
                        score += 4
                    if token in searchable:
                        score += 2
                if score:
                    matches.append(
                        {
                            "table_name": table.name,
                            "column_name": column.name,
                            "type": column.type,
                            "comment": column.comment,
                            "score": score,
                        }
                    )
        return sorted(matches, key=lambda item: (-item["score"], item["table_name"], item["column_name"]))

    def sample_table(
        self,
        table_name: str,
        rows: int,
        *,
        partition: str | None = None,
        columns: list[str] | None = None,
    ) -> tuple[TableDefinition, list[dict[str, Any]], dict[str, Any]]:
        table = self._get_table(table_name)
        selected_columns, applied_partition, partition_values = resolve_sample_request(
            table,
            partition=partition,
            columns=columns,
            strict_partition_check=True,
        )
        sample_rows = project_sample_rows(
            table.sample_rows[:rows],
            selected_columns,
            partition_values,
        )
        return table, sample_rows, {
            "schema": build_sample_schema(table, selected_columns),
            "applied_partition": applied_partition,
            "selected_columns": selected_columns,
        }

    def profile_table(self, table_name: str, *, partition: str | None = None) -> dict[str, Any]:
        table = self._get_table(table_name)
        _, sample_rows, sample_info = self.sample_table(
            table_name,
            rows=20,
            partition=partition,
            columns=None,
        )
        return build_profile(table, sample_rows, applied_partition=sample_info["applied_partition"])

    def whoami_info(self, *, project: str | None = None) -> tuple[dict[str, Any], list[str]]:
        return (
            {
                "authenticated": True,
                "backend": "mock",
                "auth_type": "mock",
                "identity_source": "local_config",
                "principal_display": "mock_user",
                "principal_masked": "mock_user",
                "project": project or self.config.default_project,
                "region": self.config.default_region,
                "endpoint": None,
                "project_owner": None,
                "allowed_operations": self.config.allowed_operations,
            },
            ["mock backend 返回的是本地配置身份，不代表真实 MaxCompute 账号。"],
        )

    def can_i_info(
        self,
        *,
        table_name: str,
        operation: str,
        project: str | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        normalized_operation = operation.upper().strip()
        target_project = project or self.config.default_project
        if normalized_operation not in self.config.allowed_operations:
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "config_allowed_operations",
                    "reason": f"当前配置仅允许 {', '.join(self.config.allowed_operations)}。",
                    "check_error_code": "PERMISSION_DENIED",
                },
                [],
            )

        try:
            self._get_table(table_name)
        except MaxCError as exc:
            if isinstance(exc, BackendConnectionError):
                raise
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "mock_table_lookup",
                    "reason": exc.message,
                    "check_error_code": exc.error_code,
                },
                [],
            )

        return (
            {
                "resource_type": "table",
                "table_name": table_name,
                "project": target_project,
                "operation": normalized_operation,
                "allowed": True,
                "check_mode": "mock_table_lookup",
                "reason": "mock backend 允许读取已配置表。",
                "check_error_code": None,
            },
            [],
        )

    def _estimate_bytes_scanned(
        self, tables: list[TableDefinition], projected_rows: list[dict[str, Any]]
    ) -> int:
        if tables:
            row_count = max(table.row_count for table in tables)
            avg_columns = max(len(table.columns) for table in tables)
            return max(row_count * max(avg_columns, 1) * 32, 1024)
        return max(
            len(projected_rows) * max(len(projected_rows[0]) if projected_rows else 1, 1) * 16,
            512,
        )

    def _get_table(self, table_name: str) -> TableDefinition:
        try:
            return self.config.catalog[table_name]
        except KeyError as exc:
            suggestions = get_close_matches(table_name, list(self.config.catalog), n=3)
            suggestion_text = None
            if suggestions:
                suggestion_text = f"可选表: {', '.join(suggestions)}"
            raise SqlError(
                f"表不存在: {table_name}",
                suggestion=suggestion_text or "请先执行 maxc meta list-tables 或 maxc meta search。",
            ) from exc

    def _materialize_rows(
        self,
        sql: str,
        tables: list[TableDefinition],
        max_rows: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], int]:
        warnings: list[str] = []
        projection = parse_select_projection(sql)

        if not tables:
            rows = [{"_c0": 1}]
            schema = [{"name": "_c0", "type": "bigint", "comment": "literal result"}]
            return rows[:max_rows], schema, warnings, len(rows)

        if len(tables) > 1:
            warnings.append("mock backend 只返回首张表的样例数据，JOIN 结果未真实计算。")
        table = tables[0]

        if any("count(" in item.lower() for item in projection):
            alias = projection_alias(projection[0], 0) if projection else "_c0"
            rows = [{alias: table.row_count}] if offset == 0 else []
            schema = [{"name": alias, "type": "bigint", "comment": "estimated row count"}]
            return rows, schema, warnings, 1

        source_rows = table.sample_rows[offset : offset + max_rows]
        if projection == ["*"] or not projection:
            schema = [
                {"name": column.name, "type": column.type, "comment": column.comment}
                for column in table.columns
            ]
            return source_rows, schema, warnings, table.row_count

        projected_rows: list[dict[str, Any]] = []
        schema: list[dict[str, Any]] = []
        for index, expression in enumerate(projection):
            alias = projection_alias(expression, index)
            schema_column = next(
                (column for column in table.columns if column.name == alias),
                None,
            )
            schema.append(
                {
                    "name": alias,
                    "type": schema_column.type if schema_column else "string",
                    "comment": schema_column.comment if schema_column else "",
                }
            )

        for row in source_rows:
            projected: dict[str, Any] = {}
            for index, expression in enumerate(projection):
                alias = projection_alias(expression, index)
                base_name = expression.split(".")[-1].strip()
                if " as " in expression.lower():
                    base_name = expression.rsplit(" ", 1)[0].split(".")[-1].strip()
                projected[alias] = row.get(alias, row.get(base_name))
            projected_rows.append(projected)
        return projected_rows, schema, warnings, table.row_count

    def _validate_select(self, sql: str) -> None:
        operation = detect_operation(sql)
        if operation not in self.config.allowed_operations:
            raise PermissionDeniedError(
                f"当前配置仅允许 {', '.join(self.config.allowed_operations)}，不允许执行 {operation}。",
                suggestion="如需支持写操作，请在 .maxc/config.yaml 中放宽 allowed_operations 并增加审批机制。",
            )
        if operation != "SELECT":
            raise PermissionDeniedError(f"当前 mock backend 暂不支持 {operation}。")


class OdpsBackend(BaseBackend):
    supports_remote_jobs = True
    supports_cost_check = False

    def __init__(self, config: MaxCConfig) -> None:
        if ODPS is None:  # pragma: no cover
            raise FeatureUnavailableError("当前环境未安装 pyodps，无法连接 MaxCompute。")
        self.config = config
        self.env = load_odps_env()
        missing = [
            name
            for name in ("access_id", "secret_access_key", "project", "endpoint")
            if not self.env.get(name)
        ]
        if missing:
            raise BackendConnectionError(
                f"缺少 MaxCompute 环境变量: {', '.join(missing)}",
                suggestion=(
                    "请设置 ALIBABA_CLOUD_ACCESS_KEY_ID、ALIBABA_CLOUD_ACCESS_KEY_SECRET、"
                    "MAXCOMPUTE_PROJECT、MAXCOMPUTE_ENDPOINT。"
                ),
            )

        self.project = self.env["project"] or config.default_project
        self.client = ODPS(
            access_id=self.env["access_id"],
            secret_access_key=self.env["secret_access_key"],
            project=self.project,
            endpoint=self.env["endpoint"],
            region_name=self.env.get("region_name") or None,
            tunnel_endpoint=self.env.get("tunnel_endpoint") or None,
        )

    def execute_query(
        self,
        sql: str,
        *,
        project: str,
        max_rows: int,
        dry_run: bool,
        offset: int = 0,
    ) -> QueryResult:
        self._validate_select(sql)

        started_at = now_utc_iso()
        started_monotonic = monotonic()

        if dry_run:
            try:
                sql_cost = self.client.execute_sql_cost(sql, project=project)
            except Exception as exc:
                raise translate_odps_error(exc) from exc
            elapsed_ms = int((monotonic() - started_monotonic) * 1000)
            return QueryResult(
                rows=[],
                schema=[],
                total_rows=0,
                returned_rows=0,
                has_more=False,
                next_cursor=None,
                elapsed_ms=elapsed_ms,
                bytes_scanned=int(sql_cost.input_size or 0),
                cost_cu=None,
                project=project,
                sql_executed=sql,
                tables_used=extract_table_names(sql),
                warnings=[
                    "MaxCompute dry-run 返回 SQLCost 元数据，不执行真实查询。",
                    "真实 backend 暂未提供 CU 口径成本，因此 cost_cu 返回 null。",
                ],
                submitted_at=started_at,
                completed_at=now_utc_iso(),
                extra_metadata={
                    "sql_complexity": sql_cost.complexity,
                    "sql_udf_num": sql_cost.udf_num,
                    "estimated_input_size_bytes": sql_cost.input_size,
                },
            )

        try:
            instance = self.client.execute_sql(sql, project=project)
            instance.wait_for_success()
        except Exception as exc:
            raise translate_odps_error(exc) from exc

        elapsed_ms = int((monotonic() - started_monotonic) * 1000)
        result = self._instance_to_query_result(
            instance,
            project=project,
            max_rows=max_rows,
            sql=sql,
            elapsed_ms=elapsed_ms,
            offset=offset,
        )
        result.submitted_at = started_at
        result.completed_at = now_utc_iso()
        return result

    def estimate_query_cost(self, sql: str, *, project: str) -> dict[str, Any]:
        self._validate_select(sql)
        started_monotonic = monotonic()
        try:
            sql_cost = self.client.execute_sql_cost(sql, project=project)
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return {
            **build_query_outline(sql),
            "project": project,
            "cost_model": "maxcompute_native_sql_cost",
            "estimated_input_size_bytes": int(sql_cost.input_size or 0),
            "task_cost_cpu": None,
            "task_cost_memory": None,
            "cost_cu": None,
            "sql_complexity": sql_cost.complexity,
            "sql_udf_num": sql_cost.udf_num,
            "total_row_estimate": None,
            "elapsed_ms": int((monotonic() - started_monotonic) * 1000),
            "warnings": [
                "MaxCompute SQLCost 只提供预估输入大小、复杂度和 UDF 数量。",
                "预执行阶段无法直接获得 task_cost_cpu / task_cost_memory。",
            ],
        }

    def explain_query(self, sql: str, *, project: str) -> dict[str, Any]:
        estimate = self.estimate_query_cost(sql, project=project)
        warnings = list(estimate.pop("warnings", []))
        estimate["warnings"] = warnings
        estimate["analysis_mode"] = "explain"
        estimate["read_path"] = True
        return estimate

    def submit_query(
        self,
        sql: str,
        *,
        project: str,
        idempotency_key: str | None = None,
    ) -> JobInfo:
        try:
            instance = self.client.execute_sql(
                sql,
                project=project,
                unique_identifier_id=idempotency_key,
            )
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return JobInfo(
            job_id=instance.id,
            status="pending",
            project=project,
            progress=0,
            sql=sql,
            submitted_at=now_utc_iso(),
            updated_at=now_utc_iso(),
            logview=self._safe_logview(instance),
            warnings=["真实 MaxCompute 实例已提交，可用 job.status / job.wait 跟踪。"],
        )

    def get_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        instance = self._get_instance(job_id, project=project)
        return self._instance_to_job_info(instance, project=project or self.project)

    def wait_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        instance = self._get_instance(job_id, project=project)
        try:
            instance.wait_for_success()
        except Exception:
            pass
        return self._instance_to_job_info(instance, project=project or self.project)

    def fetch_job_result(
        self,
        job_id: str,
        *,
        project: str | None = None,
        max_rows: int,
    ) -> QueryResult:
        instance = self._get_instance(job_id, project=project)
        info = self._instance_to_job_info(instance, project=project or self.project)
        if info.status != "success":
            raise FeatureUnavailableError(
                f"任务 {job_id} 当前状态为 {info.status}，结果尚不可读。",
                suggestion="请先执行 maxc job wait 或 maxc job status。",
            )
        sql = self._safe_sql(instance) or ""
        return self._instance_to_query_result(
            instance,
            project=project or self.project,
            max_rows=max_rows,
            sql=sql,
            elapsed_ms=_duration_ms(instance.start_time, instance.end_time),
        )

    def cancel_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        instance = self._get_instance(job_id, project=project)
        try:
            instance.stop()
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        sql = self._safe_sql(instance)
        return JobInfo(
            job_id=job_id,
            status="failure",
            project=project or self.project,
            progress=0,
            stage="cancel_requested",
            retryable=False,
            failure_reason="任务已请求取消。",
            task_summary=build_task_summary(sql),
            sql=sql,
            submitted_at=_dt_to_iso(getattr(instance, "start_time", None)),
            updated_at=now_utc_iso(),
            logview=self._safe_logview(instance),
            warnings=["任务已请求取消，最终状态请再次执行 job.status 确认。"],
        )

    def diagnose_job(self, job_id: str, *, project: str | None = None) -> dict[str, Any]:
        instance = self._get_instance(job_id, project=project)
        info = self._instance_to_job_info(instance, project=project or self.project)
        diagnosis = classify_failure_reason(info.failure_reason)
        task_statuses = self._safe_task_statuses(instance)
        task_results = self._safe_task_results(instance)
        return {
            "job_id": info.job_id,
            "status": info.status,
            "stage": info.stage,
            "retryable": info.retryable,
            "failure_reason": info.failure_reason,
            "diagnosis_category": diagnosis["category"],
            "diagnosis_summary": diagnosis["summary"],
            "logview": info.logview,
            "task_summary": info.task_summary,
            "task_statuses": [
                {
                    "task_name": name,
                    "status": str(getattr(task, "status", "")).split(".")[-1].lower(),
                    "type": str(getattr(task, "type", "") or ""),
                }
                for name, task in task_statuses.items()
            ],
            "task_results": task_results,
        }

    def list_jobs(self, *, project: str | None = None, limit: int = 20) -> list[JobInfo]:
        jobs: list[JobInfo] = []
        try:
            iterator = self.client.list_instances(project=project or self.project)
            for instance in islice(iterator, limit):
                jobs.append(self._instance_to_job_info(instance, project=project or self.project))
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return jobs

    def list_tables(self) -> list[TableDefinition]:
        tables: list[TableDefinition] = []
        try:
            for table in self.client.list_tables(project=self.project):
                tables.append(self._table_stub(table))
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return sorted(tables, key=lambda item: item.name)

    def describe_table(self, table_name: str) -> TableDefinition:
        table = self._get_table(table_name)
        partitions = self._list_partitions(table, limit=20)
        sample_rows = self._table_head(table, limit=2)
        definition = self._table_definition_from_table(table)
        definition.partitions = partitions
        definition.sample_rows = sample_rows
        return definition

    def search_tables(self, keyword: str) -> list[dict[str, Any]]:
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: list[dict[str, Any]] = []
        for table in self.list_tables():
            score = 0
            searchable = f"{table.name} {table.description}".lower()
            matched_columns: list[str] = []
            for token in tokens:
                if token in searchable:
                    score += 5
            if score == 0:
                for column in table.columns:
                    text = f"{column.name} {column.comment}".lower()
                    if any(token in text for token in tokens):
                        score += 2
                        matched_columns.append(column.name)
            if score:
                matches.append(
                    {
                        "table_name": table.name,
                        "description": table.description,
                        "score": score,
                        "matched_columns": matched_columns,
                    }
                )
        return sorted(matches, key=lambda item: (-item["score"], item["table_name"]))

    def search_columns(self, keyword: str) -> list[dict[str, Any]]:
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: list[dict[str, Any]] = []
        for table in self.list_tables():
            for column in table.columns:
                score = 0
                text = f"{column.name} {column.comment}".lower()
                searchable = f"{table.name} {text}".lower()
                for token in tokens:
                    if token in column.name.lower():
                        score += 8
                    if token in text:
                        score += 4
                    if token in searchable:
                        score += 2
                if score:
                    matches.append(
                        {
                            "table_name": table.name,
                            "column_name": column.name,
                            "type": column.type,
                            "comment": column.comment,
                            "score": score,
                        }
                    )
        return sorted(matches, key=lambda item: (-item["score"], item["table_name"], item["column_name"]))

    def sample_table(
        self,
        table_name: str,
        rows: int,
        *,
        partition: str | None = None,
        columns: list[str] | None = None,
    ) -> tuple[TableDefinition, list[dict[str, Any]], dict[str, Any]]:
        definition = self.describe_table(table_name)
        selected_columns, applied_partition, partition_values = resolve_sample_request(
            definition,
            partition=partition,
            columns=columns,
            strict_partition_check=False,
        )
        select_list = ", ".join(quote_table_name(name) for name in selected_columns)
        sql = f"SELECT {select_list} FROM {quote_table_name(table_name)}"
        if applied_partition:
            predicates = [
                f"{quote_table_name(column.name)} = {sql_string_literal(partition_values[column.name])}"
                for column in definition.partition_columns
            ]
            sql += " WHERE " + " AND ".join(predicates)
        sql += f" LIMIT {rows}"

        result = self.execute_query(
            sql,
            project=self.project,
            max_rows=rows,
            dry_run=False,
            offset=0,
        )
        return definition, result.rows, {
            "schema": result.schema,
            "applied_partition": applied_partition,
            "selected_columns": selected_columns,
        }

    def profile_table(self, table_name: str, *, partition: str | None = None) -> dict[str, Any]:
        definition, sample_rows, sample_info = self.sample_table(
            table_name,
            rows=20,
            partition=partition,
            columns=None,
        )
        return build_profile(
            definition,
            sample_rows,
            applied_partition=sample_info["applied_partition"],
        )

    def latest_partition_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        return self._latest_partition_info_from_table(table, definition)

    def freshness_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        latest_payload, warnings = self._latest_partition_info_from_table(table, definition)
        return build_freshness_info(definition, latest_payload, warnings=warnings)

    def lineage_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        return (
            {
                "table_name": definition.name,
                "supported": False,
                "lineage_source": "unavailable",
                "coverage": "unsupported",
                "upstream_tables": [],
                "downstream_tables": [],
                "limitation": "当前版本未接入 MaxCompute 血缘 API。",
            },
            ["当前版本未接入 MaxCompute 血缘 API，lineage 返回的是明确的 unsupported 占位结果。"],
        )

    def whoami_info(self, *, project: str | None = None) -> tuple[dict[str, Any], list[str]]:
        target_project = project or self.project
        owner = None
        try:
            owner = getattr(self.client.get_project(target_project), "owner", None)
        except Exception:
            owner = None
        access_id = getattr(self.client.account, "access_id", None)
        warnings = []
        if access_id:
            warnings.append("真实 backend 当前无法直接解析 RAM 用户名，principal_display 使用 access_id 脱敏值。")
        return (
            {
                "authenticated": True,
                "backend": "odps",
                "auth_type": "access_key",
                "identity_source": "environment",
                "principal_display": mask_access_id(access_id) if access_id else None,
                "principal_masked": mask_access_id(access_id) if access_id else None,
                "project": target_project,
                "region": self.env.get("region_name"),
                "endpoint": self.env.get("endpoint"),
                "project_owner": owner,
                "allowed_operations": self.config.allowed_operations,
            },
            warnings,
        )

    def can_i_info(
        self,
        *,
        table_name: str,
        operation: str,
        project: str | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        normalized_operation = operation.upper().strip()
        target_project = project or self.project
        if normalized_operation not in self.config.allowed_operations:
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "config_allowed_operations",
                    "reason": f"当前配置仅允许 {', '.join(self.config.allowed_operations)}。",
                    "check_error_code": "PERMISSION_DENIED",
                },
                [],
            )
        if normalized_operation != "SELECT":
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "cli_supported_operations",
                    "reason": "当前版本只支持 SELECT 读路径权限探测。",
                    "check_error_code": "FEATURE_UNAVAILABLE",
                },
                [],
            )

        safe_table_name = quote_table_name(table_name)
        sql = f"SELECT * FROM {safe_table_name} LIMIT 0"
        try:
            self._get_table(table_name, project=target_project)
            self.client.execute_sql_cost(sql, project=target_project)
        except MaxCError as exc:
            if isinstance(exc, BackendConnectionError):
                raise
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "odps_sql_cost_limit_0",
                    "reason": exc.message,
                    "check_error_code": exc.error_code,
                },
                [],
            )
        except Exception as exc:
            translated = translate_odps_error(exc)
            if isinstance(translated, BackendConnectionError):
                raise translated
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "odps_sql_cost_limit_0",
                    "reason": translated.message,
                    "check_error_code": translated.error_code,
                },
                [],
            )

        return (
            {
                "resource_type": "table",
                "table_name": table_name,
                "project": target_project,
                "operation": normalized_operation,
                "allowed": True,
                "check_mode": "odps_sql_cost_limit_0",
                "reason": "已通过元数据访问和 LIMIT 0 读路径预检。",
                "check_error_code": None,
            },
            [],
        )

    def _get_table(self, table_name: str, *, project: str | None = None):
        try:
            return self.client.get_table(table_name, project=project or self.project)
        except Exception as exc:
            raise translate_odps_error(exc) from exc

    def _get_instance(self, job_id: str, *, project: str | None = None):
        try:
            return self.client.get_instance(job_id, project=project or self.project)
        except Exception as exc:
            raise translate_odps_error(exc) from exc

    def _table_stub(self, table) -> TableDefinition:
        return self._table_definition_from_table(table)

    def _table_definition_from_table(self, table) -> TableDefinition:
        row_count = int(getattr(table, "record_num", -1) or -1)
        columns = [
            TableColumn(
                name=column.name,
                type=str(column.type),
                comment=getattr(column, "comment", "") or "",
            )
            for column in getattr(table.table_schema, "columns", [])
        ]
        partition_columns = [
            TableColumn(
                name=column.name,
                type=str(column.type),
                comment=getattr(column, "comment", "") or "",
            )
            for column in getattr(table.table_schema, "partitions", [])
        ]
        return TableDefinition(
            name=table.name,
            description=getattr(table, "comment", "") or "",
            row_count=row_count,
            columns=columns,
            sample_rows=[],
            partitions=[],
            upstream_tables=[],
            downstream_tables=[],
            partition_columns=partition_columns,
            owner=getattr(table, "owner", None),
            created_at=_dt_to_iso(getattr(table, "creation_time", None)),
            updated_at=_dt_to_iso(getattr(table, "last_data_modified_time", None)),
            table_type="VIRTUAL_VIEW" if getattr(table, "is_virtual_view", False) else "TABLE",
            row_count_source="odps_record_num" if row_count >= 0 else "unavailable",
            size_bytes=(
                int(getattr(table, "size", 0))
                if getattr(table, "size", None) is not None
                else None
            ),
            extra_metadata={"lifecycle": getattr(table, "lifecycle", None)},
        )

    def _table_head(self, table, *, limit: int) -> list[dict[str, Any]]:
        try:
            reader = table.head(limit)
            rows = list(islice(reader, limit))
        except Exception:
            return []
        columns = [column.name for column in table.table_schema.columns]
        return [record_to_dict(columns, record.values) for record in rows]

    def _list_partitions(self, table, *, limit: int) -> list[str]:
        try:
            partitions = list(islice(table.iterate_partitions(), limit))
        except OdpsInvalidParameter:
            return []
        except Exception:
            return []
        return [str(partition.partition_spec) for partition in partitions]

    def _latest_partition_info_from_table(
        self,
        table,
        definition: TableDefinition,
    ) -> tuple[dict[str, Any], list[str]]:
        latest_partition = self._max_partition_spec(table)
        if latest_partition:
            return build_latest_partition_info(
                definition,
                source="odps_get_max_partition",
                latest_partition_override=latest_partition,
                visible_partition_count=None,
            )

        partitions = self._list_partitions(table, limit=200)
        payload, warnings = build_latest_partition_info(
            definition,
            source="odps_iterate_partitions",
            partitions=partitions,
            visible_partition_count=len(partitions),
        )
        if definition.partition_columns and len(partitions) == 200:
            warnings.append("当前只遍历前 200 个可见分区，超大表建议结合控制台进一步核对。")
        return payload, warnings

    def _max_partition_spec(self, table) -> str | None:
        getter = getattr(table, "get_max_partition", None)
        if callable(getter):
            for kwargs in ({"skip_empty": True}, {}):
                try:
                    partition = getter(**kwargs)
                except TypeError:
                    continue
                except Exception:
                    partition = None
                text = partition_spec_text(partition)
                if text:
                    return text
        return None

    def _safe_task_statuses(self, instance) -> dict[str, Any]:
        try:
            return dict(instance.get_task_statuses())
        except Exception:
            return {}

    def _safe_task_results(self, instance) -> dict[str, str]:
        try:
            results = instance.get_task_results()
        except Exception:
            return {}
        return {
            str(name): str(value)
            for name, value in dict(results).items()
        }

    def _first_failure_reason(self, instance) -> str | None:
        task_results = self._safe_task_results(instance)
        for value in task_results.values():
            text = str(value).strip()
            if text:
                return text
        return None

    def _instance_to_job_info(self, instance, *, project: str) -> JobInfo:
        try:
            instance.reload(blocking=False)
        except Exception:
            pass

        status_name = str(getattr(instance, "status", "")).split(".")[-1]
        sql = self._safe_sql(instance)
        logview = self._safe_logview(instance)
        submitted_at = _dt_to_iso(getattr(instance, "start_time", None))
        completed_at = _dt_to_iso(getattr(instance, "end_time", None))
        task_statuses = self._safe_task_statuses(instance)
        task_names = sorted(task_statuses)
        task_types = {
            name: str(getattr(task, "type", "") or "")
            for name, task in task_statuses.items()
        }
        task_summary = build_task_summary(sql, task_names=task_names, task_types=task_types)

        if status_name == "RUNNING":
            return JobInfo(
                job_id=instance.id,
                status="running",
                project=project,
                progress=50,
                stage="running",
                retryable=None,
                task_summary=task_summary,
                sql=sql,
                submitted_at=submitted_at,
                updated_at=now_utc_iso(),
                completed_at=completed_at,
                logview=logview,
            )

        if status_name == "TERMINATED":
            try:
                succeeded = instance.is_successful()
            except Exception as exc:
                return JobInfo(
                    job_id=instance.id,
                    status="failure",
                    project=project,
                    progress=100,
                    stage="failed",
                    retryable=False,
                    failure_reason=str(exc),
                    task_summary=task_summary,
                    sql=sql,
                    submitted_at=submitted_at,
                    updated_at=now_utc_iso(),
                    completed_at=completed_at,
                    logview=logview,
                    error_message=str(exc),
                )
            failure_reason = None if succeeded else self._first_failure_reason(instance)
            diagnosis = classify_failure_reason(failure_reason)
            return JobInfo(
                job_id=instance.id,
                status="success" if succeeded else "failure",
                project=project,
                progress=100,
                stage="completed" if succeeded else "failed",
                retryable=False if succeeded else diagnosis["retryable"],
                failure_reason=failure_reason,
                task_summary=task_summary,
                sql=sql,
                submitted_at=submitted_at,
                updated_at=now_utc_iso(),
                completed_at=completed_at,
                logview=logview,
            )

        return JobInfo(
            job_id=instance.id,
            status="pending",
            project=project,
            progress=0,
            stage="queue",
            retryable=None,
            task_summary=task_summary,
            sql=sql,
            submitted_at=submitted_at,
            updated_at=now_utc_iso(),
            completed_at=completed_at,
            logview=logview,
        )

    def _instance_to_query_result(
        self,
        instance,
        *,
        project: str,
        max_rows: int,
        sql: str,
        elapsed_ms: int,
        offset: int = 0,
    ) -> QueryResult:
        try:
            with instance.open_reader() as reader:
                schema = [
                    {
                        "name": column.name,
                        "type": str(column.type),
                        "comment": "",
                    }
                    for column in reader.schema.columns
                ]
                rows = [
                    record_to_dict(
                        [column["name"] for column in schema],
                        record.values,
                    )
                    for record in islice(reader, offset, offset + max_rows)
                ]
                total_rows = int(getattr(reader, "count", len(rows)) or len(rows))
        except Exception as exc:
            raise translate_odps_error(exc) from exc

        bytes_scanned, extra_metadata = self._task_cost(instance)
        returned_rows = len(rows)
        has_more = total_rows > (offset + returned_rows)
        next_cursor = encode_cursor(offset + returned_rows) if has_more and returned_rows else None
        warnings: list[str] = []
        if extra_metadata.get("task_cost_cpu") is not None:
            warnings.append("真实 MaxCompute backend 暂未返回 CU 口径成本，cost_cu 为 null。")
        extra_metadata["current_offset"] = offset

        return QueryResult(
            rows=rows,
            schema=schema,
            total_rows=total_rows,
            returned_rows=returned_rows,
            has_more=has_more,
            next_cursor=next_cursor,
            elapsed_ms=elapsed_ms,
            bytes_scanned=bytes_scanned,
            cost_cu=None,
            project=project,
            sql_executed=sql.rstrip(";"),
            tables_used=extract_table_names(sql),
            warnings=warnings,
            job_id=instance.id,
            submitted_at=_dt_to_iso(getattr(instance, "start_time", None)),
            completed_at=_dt_to_iso(getattr(instance, "end_time", None)),
            extra_metadata=extra_metadata,
        )

    def _validate_select(self, sql: str) -> None:
        operation = detect_operation(sql)
        if operation not in self.config.allowed_operations:
            raise PermissionDeniedError(
                f"当前配置仅允许 {', '.join(self.config.allowed_operations)}，不允许执行 {operation}。",
                suggestion="如需支持写操作，请调整 .maxc/config.yaml 中的 allowed_operations。",
            )
        if operation != "SELECT":
            raise PermissionDeniedError(f"当前版本仅支持 SELECT，实际收到 {operation}。")

    def _task_cost(self, instance) -> tuple[int | None, dict[str, Any]]:
        try:
            task_cost = instance.get_task_cost()
        except Exception:
            return None, {}
        return (
            int(task_cost.input_size or 0),
            {
                "task_cost_cpu": task_cost.cpu_cost,
                "task_cost_memory": task_cost.memory_cost,
                "estimated_input_size_bytes": task_cost.input_size,
            },
        )

    def _safe_sql(self, instance) -> str | None:
        try:
            sql = instance.get_sql_query()
        except Exception:
            return None
        return sql.rstrip(";") if sql else None

    def _safe_logview(self, instance) -> str | None:
        try:
            return instance.get_logview_address()
        except Exception:
            return None


def build_latest_partition_info(
    table: TableDefinition,
    *,
    source: str,
    partitions: list[str] | None = None,
    latest_partition_override: str | None = None,
    visible_partition_count: int | None = None,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    partition_columns = [column.name for column in table.partition_columns]
    has_partitions = bool(partition_columns)
    candidates = list(partitions) if partitions is not None else list(table.partitions)
    latest_partition = latest_partition_override or select_latest_partition(
        candidates,
        partition_columns,
    )

    if not has_partitions:
        warnings.append("该表不是分区表，latest-partition 返回 null。")
    elif not latest_partition:
        warnings.append("当前未读取到可见分区，latest-partition 返回 null。")

    partition_values = normalize_partition_values(
        parse_partition_spec(latest_partition) if latest_partition else {},
        partition_columns,
    )
    if latest_partition and not partition_values:
        warnings.append("最新分区已返回，但当前无法稳定解析分区规格。")

    payload = {
        "table_name": table.name,
        "has_partitions": has_partitions,
        "partition_columns": partition_columns,
        "latest_partition": latest_partition,
        "latest_partition_values": partition_values,
        "reference_time": partition_reference_time(partition_values, partition_columns),
        "latest_partition_source": source,
        "visible_partition_count": visible_partition_count if visible_partition_count is not None else len(candidates),
        "observed_at": now_utc_iso(),
    }
    return payload, warnings


def build_freshness_info(
    table: TableDefinition,
    latest_partition_payload: dict[str, Any],
    *,
    warnings: list[str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    payload_warnings = list(warnings or [])
    observed_at = now_utc_iso()
    observed_dt = parse_time_value(observed_at)
    partition_time = latest_partition_payload.get("reference_time")
    latest_partition = latest_partition_payload.get("latest_partition")
    freshness_source = "latest_partition"
    reference_time = partition_time

    if reference_time is None and table.updated_at:
        freshness_source = "table_updated_at"
        reference_time = normalize_time_value(table.updated_at)
        if latest_partition_payload.get("has_partitions"):
            payload_warnings.append("未能从最新分区解析时间，freshness 已回退到 updated_at。")
        else:
            payload_warnings.append("该表不是分区表，freshness 基于 updated_at 推断。")
    elif reference_time is None:
        freshness_source = "unknown"
        if latest_partition:
            payload_warnings.append("最新分区存在，但分区值不是可解析时间格式。")
        else:
            payload_warnings.append("当前缺少可解析的分区时间和 updated_at，freshness 返回 unknown。")

    age_seconds: int | None = None
    age_hours: float | None = None
    age_days: float | None = None
    freshness_status = "unknown"
    reference_dt = parse_time_value(reference_time) if reference_time else None

    if observed_dt and reference_dt:
        delta_seconds = int((observed_dt - reference_dt).total_seconds())
        if delta_seconds < 0:
            payload_warnings.append("参考时间晚于当前时间，freshness 按 0 秒处理。")
            delta_seconds = 0
        age_seconds = delta_seconds
        age_hours = round(delta_seconds / 3600, 2)
        age_days = round(delta_seconds / 86400, 2)
        if age_hours <= FRESHNESS_FRESH_HOURS:
            freshness_status = "fresh"
        elif age_hours <= FRESHNESS_STALE_HOURS:
            freshness_status = "lagging"
        else:
            freshness_status = "stale"

    payload = {
        "table_name": table.name,
        "freshness_source": freshness_source,
        "freshness_status": freshness_status,
        "reference_time": reference_time,
        "observed_at": observed_at,
        "age_seconds": age_seconds,
        "age_hours": age_hours,
        "age_days": age_days,
        "latest_partition": latest_partition,
        "latest_partition_values": latest_partition_payload.get("latest_partition_values", {}),
        "partition_columns": latest_partition_payload.get("partition_columns", []),
        "latest_partition_source": latest_partition_payload.get("latest_partition_source"),
        "visible_partition_count": latest_partition_payload.get("visible_partition_count"),
        "status_thresholds": {
            "fresh_hours": FRESHNESS_FRESH_HOURS,
            "stale_hours": FRESHNESS_STALE_HOURS,
        },
    }
    return payload, payload_warnings


def select_latest_partition(partitions: list[str], partition_columns: list[str]) -> str | None:
    if not partitions:
        return None
    return max(
        partitions,
        key=lambda item: partition_sort_key(item, partition_columns),
    )


def partition_sort_key(spec: str, partition_columns: list[str]) -> tuple[Any, ...]:
    values = normalize_partition_values(parse_partition_spec(spec), partition_columns)
    ordered_keys = partition_columns or list(values)
    if not ordered_keys:
        return (spec,)
    return tuple(sortable_partition_value(values.get(key)) for key in ordered_keys)


def parse_partition_spec(spec: str | None) -> dict[str, str]:
    if not spec:
        return {}
    normalized = PARTITION_PATH_RE.sub(",", spec.strip())
    values: dict[str, str] = {}
    for item in normalized.split(","):
        segment = item.strip()
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


def normalize_partition_values(
    values: dict[str, str],
    partition_columns: list[str],
) -> dict[str, str]:
    ordered: dict[str, str] = {}
    for key in partition_columns:
        if key in values:
            ordered[key] = values[key]
    for key, value in values.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def partition_reference_time(
    partition_values: dict[str, str],
    partition_columns: list[str],
) -> str | None:
    if not partition_values:
        return None

    ordered_keys = partition_columns or list(partition_values)
    base_key = next((key for key in ordered_keys if key in partition_values), None)
    if base_key is not None:
        base_date = parse_date_value(partition_values.get(base_key))
        if base_date is not None:
            hour = parse_partition_component(partition_values, {"hh", "hour", "hr"})
            minute = parse_partition_component(partition_values, {"mi", "minute", "mm"})
            second = parse_partition_component(partition_values, {"ss", "second"})
            if hour is not None or minute is not None or second is not None:
                return datetime(
                    base_date.year,
                    base_date.month,
                    base_date.day,
                    hour or 0,
                    minute or 0,
                    second or 0,
                    tzinfo=timezone.utc,
                ).isoformat()

    for key in reversed(ordered_keys):
        candidate = normalize_time_value(partition_values.get(key))
        if candidate is not None:
            return candidate
    for value in partition_values.values():
        candidate = normalize_time_value(value)
        if candidate is not None:
            return candidate
    return None


def parse_partition_component(values: dict[str, str], names: set[str]) -> int | None:
    for key, value in values.items():
        if key.lower() not in names:
            continue
        text = str(value).strip()
        if not re.fullmatch(r"\d{1,2}", text):
            continue
        return int(text)
    return None


def sortable_partition_value(value: Any) -> tuple[int, Any]:
    if value is None:
        return (0, "")
    text = str(value).strip().strip("'").strip('"')
    if re.fullmatch(r"-?\d+", text):
        return (1, int(text))
    if re.fullmatch(r"-?\d+\.\d+", text):
        return (2, float(text))
    normalized_time = normalize_time_value(text)
    if normalized_time is not None:
        dt = parse_time_value(normalized_time)
        if dt is not None:
            return (3, int(dt.timestamp()))
    return (4, text.lower())


def parse_date_value(value: Any) -> date | None:
    text = str(value).strip().strip("'").strip('"')
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def normalize_time_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().strip("'").strip('"')
    if not text:
        return None
    parsed = parse_time_value(text)
    return parsed.isoformat() if parsed is not None else None


def parse_time_value(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip().strip("'").strip('"')
    if not text:
        return None

    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        parsed = None
    if parsed is not None:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None


def partition_spec_text(partition: Any) -> str | None:
    if partition is None:
        return None
    spec = getattr(partition, "partition_spec", None)
    target = spec if spec is not None else partition
    text = str(target).strip()
    return text or None


def mask_access_id(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}***{value[-4:]}"


def quote_table_name(table_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][\w]*(\.[A-Za-z_][\w]*)*", table_name):
        raise ValidationError(
            f"非法表名: {table_name}",
            suggestion="请使用普通表名或 project.table 形式，不要传入 SQL 片段。",
        )
    return ".".join(f"`{part}`" for part in table_name.split("."))


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_task_summary(
    sql: str | None,
    *,
    task_names: list[str] | None = None,
    task_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    normalized_sql = (sql or "").strip().rstrip(";")
    return {
        "operation": detect_operation(normalized_sql) if normalized_sql else "UNKNOWN",
        "tables_used": extract_table_names(normalized_sql) if normalized_sql else [],
        "task_names": task_names or [],
        "task_types": task_types or {},
        "sql_excerpt": normalized_sql[:200] if normalized_sql else None,
    }


def classify_failure_reason(reason: str | None) -> dict[str, Any]:
    text = (reason or "").strip()
    if not text:
        return {
            "category": "not_failed",
            "retryable": None,
            "summary": "当前没有可诊断的失败原因。",
        }
    lowered = text.lower()
    if any(token in lowered for token in ("semantic analysis exception", "parse exception", "syntax")):
        return {
            "category": "sql_semantic_error",
            "retryable": False,
            "summary": "SQL 语义或语法错误。",
        }
    if any(token in lowered for token in ("permission", "access denied", "no privilege")):
        return {
            "category": "permission_error",
            "retryable": False,
            "summary": "权限不足或访问被拒绝。",
        }
    if any(token in lowered for token in ("cancel", "取消")):
        return {
            "category": "client_cancelled",
            "retryable": False,
            "summary": "任务被主动取消。",
        }
    if any(token in lowered for token in ("quota", "resource", "timeout", "timed out", "memory")):
        return {
            "category": "resource_or_timeout_error",
            "retryable": True,
            "summary": "资源、配额或超时问题，可考虑重试。",
        }
    return {
        "category": "unknown_failure",
        "retryable": False,
        "summary": "暂时无法归类的失败原因。",
    }


def resolve_sample_request(
    table: TableDefinition,
    *,
    partition: str | None,
    columns: list[str] | None,
    strict_partition_check: bool,
) -> tuple[list[str], str | None, dict[str, str]]:
    column_lookup = {
        column.name: column
        for column in [*table.columns, *table.partition_columns]
    }
    selected_columns = columns or [column.name for column in table.columns]
    missing_columns = [column for column in selected_columns if column not in column_lookup]
    if missing_columns:
        raise ValidationError(
            f"不存在的列: {', '.join(missing_columns)}",
            suggestion="请先执行 maxc meta describe 查看可用 schema。",
        )

    applied_partition = None
    partition_values: dict[str, str] = {}
    if partition:
        partition_column_names = [column.name for column in table.partition_columns]
        if not partition_column_names:
            raise ValidationError(
                f"表 {table.name} 不是分区表，不能使用 --partition。",
                suggestion="请先执行 maxc meta describe 或 maxc meta latest-partition。",
            )
        partition_values = normalize_partition_values(
            parse_partition_spec(partition),
            partition_column_names,
        )
        if not partition_values:
            raise ValidationError(
                "--partition 无法解析。",
                suggestion="示例: --partition ds=2026-03-20",
            )
        if set(partition_values) != set(partition_column_names):
            raise ValidationError(
                f"--partition 需要完整指定分区列: {', '.join(partition_column_names)}。",
                suggestion=f"示例: --partition {canonical_partition_spec(dict.fromkeys(partition_column_names, '...'))}",
            )
        applied_partition = canonical_partition_spec(
            {name: partition_values[name] for name in partition_column_names}
        )
        if strict_partition_check and table.partitions:
            normalized_partitions = {
                canonical_partition_spec(normalize_partition_values(parse_partition_spec(item), partition_column_names))
                for item in table.partitions
            }
            if applied_partition not in normalized_partitions:
                raise ValidationError(
                    f"分区不存在: {applied_partition}",
                    suggestion="请先执行 maxc meta latest-partition 或 maxc meta partitions。",
                )

    deduped_columns: list[str] = []
    for column in selected_columns:
        if column not in deduped_columns:
            deduped_columns.append(column)
    return deduped_columns, applied_partition, partition_values


def canonical_partition_spec(values: dict[str, str]) -> str:
    return ",".join(f"{key}={value}" for key, value in values.items())


def build_sample_schema(table: TableDefinition, selected_columns: list[str]) -> list[dict[str, Any]]:
    column_lookup = {
        column.name: column
        for column in [*table.columns, *table.partition_columns]
    }
    return [
        {
            "name": column_lookup[name].name,
            "type": column_lookup[name].type,
            "comment": column_lookup[name].comment,
        }
        for name in selected_columns
    ]


def project_sample_rows(
    rows: list[dict[str, Any]],
    selected_columns: list[str],
    partition_values: dict[str, str],
) -> list[dict[str, Any]]:
    projected_rows: list[dict[str, Any]] = []
    for row in rows:
        projected: dict[str, Any] = {}
        for column in selected_columns:
            if column in row:
                projected[column] = row[column]
            else:
                projected[column] = partition_values.get(column)
        projected_rows.append(projected)
    return projected_rows


def top_values(values: list[Any], limit: int = 5) -> list[dict[str, Any]]:
    counter = Counter(str(value) for value in values)
    return [
        {"value": value, "count": count}
        for value, count in counter.most_common(limit)
    ]


def build_profile(
    table: TableDefinition,
    sample_rows: list[dict[str, Any]],
    *,
    applied_partition: str | None = None,
) -> dict[str, Any]:
    profiles = []
    for column in table.columns:
        values = [row.get(column.name) for row in sample_rows]
        non_null = [value for value in values if value is not None]
        profile: dict[str, Any] = {
            "name": column.name,
            "type": column.type,
            "comment": column.comment,
            "null_count_in_sample": len(values) - len(non_null),
            "null_ratio_in_sample": round((len(values) - len(non_null)) / len(values), 4) if values else None,
            "sample_size": len(values),
            "sample_values": non_null[:3],
            "distinct_count_in_sample": len({str(item) for item in non_null}),
            "top_values_in_sample": top_values(non_null),
        }
        if non_null and all(isinstance(item, (int, float, Decimal)) for item in non_null):
            numeric = [float(item) for item in non_null]
            profile["min"] = min(numeric)
            profile["max"] = max(numeric)
        profiles.append(profile)
    return {
        "table_name": table.name,
        "row_count": table.row_count,
        "partition_count": len(table.partitions),
        "applied_partition": applied_partition,
        "sampled_rows": len(sample_rows),
        "columns": profiles,
    }


def build_query_outline(sql: str) -> dict[str, Any]:
    return {
        "operation": detect_operation(sql),
        "normalized_sql": normalize_sql(sql),
        "tables_used": extract_table_names(sql),
        "projected_columns": parse_select_projection(sql),
    }


def translate_odps_error(exc: Exception) -> MaxCError:
    if isinstance(exc, ModuleNotFoundError) and exc.name == "pandas":
        return FeatureUnavailableError(
            "当前环境缺少 pandas，无法读取包含 TIMESTAMP 等类型的 MaxCompute 结果。",
            suggestion="请安装 pandas，或在 data sample / data profile 时先只选择不依赖 pandas 的列。",
        )
    if isinstance(exc, OdpsNoPermission):
        return PermissionDeniedError(str(exc))
    if isinstance(exc, OdpsNoSuchObject):
        return NotFoundError(
            str(exc),
            suggestion="请先执行 maxc meta list-tables 或 maxc meta search 确认对象是否存在。",
        )
    if isinstance(exc, ODPSError):
        message = str(exc)
        lowered = message.lower()
        if "permission" in lowered or "access denied" in lowered:
            return PermissionDeniedError(message)
        if "parse exception" in lowered or "semantic analysis exception" in lowered:
            return SqlError(message)
        if "connection" in lowered or "failed to resolve" in lowered:
            return BackendConnectionError(
                message,
                suggestion="请检查网络连通性、Endpoint 配置和环境变量。",
            )
        return SqlError(message)
    return BackendConnectionError(
        str(exc),
        suggestion="请检查 MaxCompute 网络连通性和环境变量。",
    )


def record_to_dict(columns: list[str], values: Iterable[Any]) -> dict[str, Any]:
    return {
        column: json_safe(value)
        for column, value in zip(columns, values)
    }


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()
    return value


def _dt_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).isoformat()
    return value.isoformat()


def _duration_ms(start: datetime | None, end: datetime | None) -> int:
    if start is None or end is None:
        return 0
    return int((end - start).total_seconds() * 1000)
