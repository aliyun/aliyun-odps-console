from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from difflib import get_close_matches
from itertools import islice
import os
from pathlib import Path
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

    def sample_table(self, table_name: str, rows: int) -> tuple[TableDefinition, list[dict[str, Any]]]:
        table = self._get_table(table_name)
        return table, table.sample_rows[:rows]

    def profile_table(self, table_name: str) -> dict[str, Any]:
        table = self._get_table(table_name)
        return build_profile(table, table.sample_rows)

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
        except Exception as exc:
            raise translate_odps_error(exc) from exc
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
        return JobInfo(
            job_id=job_id,
            status="failure",
            project=project or self.project,
            progress=0,
            sql=self._safe_sql(instance),
            submitted_at=_dt_to_iso(getattr(instance, "start_time", None)),
            updated_at=now_utc_iso(),
            logview=self._safe_logview(instance),
            warnings=["任务已请求取消，最终状态请再次执行 job.status 确认。"],
        )

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

    def sample_table(self, table_name: str, rows: int) -> tuple[TableDefinition, list[dict[str, Any]]]:
        table = self._get_table(table_name)
        sample_rows = self._table_head(table, limit=rows)
        definition = self.describe_table(table_name)
        return definition, sample_rows

    def profile_table(self, table_name: str) -> dict[str, Any]:
        definition, sample_rows = self.sample_table(table_name, rows=20)
        return build_profile(definition, sample_rows)

    def _get_table(self, table_name: str):
        try:
            return self.client.get_table(table_name, project=self.project)
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

        if status_name == "RUNNING":
            return JobInfo(
                job_id=instance.id,
                status="running",
                project=project,
                progress=50,
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
                    sql=sql,
                    submitted_at=submitted_at,
                    updated_at=now_utc_iso(),
                    completed_at=completed_at,
                    logview=logview,
                    error_message=str(exc),
                )
            return JobInfo(
                job_id=instance.id,
                status="success" if succeeded else "failure",
                project=project,
                progress=100,
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


def build_profile(table: TableDefinition, sample_rows: list[dict[str, Any]]) -> dict[str, Any]:
    profiles = []
    for column in table.columns:
        values = [row.get(column.name) for row in sample_rows]
        non_null = [value for value in values if value is not None]
        profile: dict[str, Any] = {
            "name": column.name,
            "type": column.type,
            "comment": column.comment,
            "null_count_in_sample": len(values) - len(non_null),
            "sample_values": non_null[:3],
        }
        if non_null and all(isinstance(item, (int, float, Decimal)) for item in non_null):
            numeric = [float(item) for item in non_null]
            profile["min"] = min(numeric)
            profile["max"] = max(numeric)
        else:
            profile["distinct_count_in_sample"] = len({str(item) for item in non_null})
        profiles.append(profile)
    return {
        "table_name": table.name,
        "row_count": table.row_count,
        "partition_count": len(table.partitions),
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
