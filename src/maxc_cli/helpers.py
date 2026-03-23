"""Helper functions for MaxCompute backend operations."""

from collections import Counter
from datetime import date, datetime, time, timezone
from decimal import Decimal
from difflib import get_close_matches
import os
import re
from typing import Any, Iterable

from .config import TableDefinition
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
    extract_table_names,
    normalize_sql,
    now_utc_iso,
    parse_select_projection,
)

try:
    from odps.errors import NoPermission as OdpsNoPermission
    from odps.errors import NoSuchObject as OdpsNoSuchObject
    from odps.errors import ODPSError
except Exception:  # pragma: no cover
    OdpsNoPermission = Exception
    OdpsNoSuchObject = Exception
    ODPSError = Exception


# Constants
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


def load_odps_env() -> dict[str, str | None]:
    """Load ODPS settings from environment variables."""
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


def validate_login_settings(
    *,
    settings: dict[str, str | None],
    allowed_operations: list[str],
) -> tuple[dict[str, Any], list[str]]:
    """Validate login settings by attempting to create an ODPS client.
    
    Args:
        settings: ODPS connection settings (access_id, secret_access_key, project, endpoint, etc.)
        allowed_operations: List of allowed operations for this connection
        
    Returns:
        Tuple of (identity_payload, warnings)
    """
    try:
        from odps import ODPS
    except ImportError:
        raise FeatureUnavailableError("当前环境未安装 pyodps，无法连接 MaxCompute。")
    
    missing = missing_odps_settings(settings)
    if missing:
        raise ValidationError(
            f"auth login 缺少必填字段: {', '.join(missing)}。",
            suggestion="请通过参数、--from-env 或交互输入补齐 access_id、secret_access_key、project、endpoint。",
        )

    try:
        client = ODPS(
            access_id=settings["access_id"],
            secret_access_key=settings["secret_access_key"],
            project=settings["project"],
            endpoint=settings["endpoint"],
            region_name=settings.get("region_name") or None,
            tunnel_endpoint=settings.get("tunnel_endpoint") or None,
        )
    except Exception as exc:
        raise translate_odps_error(exc) from exc

    return build_odps_identity_payload(
        client=client,
        settings=settings,
        allowed_operations=allowed_operations,
        identity_source="config_file",
    )


def resolve_odps_settings(
    config,
) -> tuple[dict[str, str | None], dict[str, str]]:
    values = config.auth.to_mapping()
    settings: dict[str, str | None] = {
        "access_id": values.get("access_id"),
        "secret_access_key": values.get("secret_access_key"),
        "project": values.get("project"),
        "endpoint": values.get("endpoint"),
        "region_name": values.get("region_name"),
        "tunnel_endpoint": values.get("tunnel_endpoint"),
    }
    sources: dict[str, str] = {
        key: "config_file" if value else "unset"
        for key, value in settings.items()
    }

    env_settings = load_odps_env()
    for field, env_value in env_settings.items():
        if env_value:
            settings[field] = env_value
            sources[field] = "environment"

    return settings, sources


def odps_identity_source(sources: dict[str, str]) -> str:
    required_sources = {
        value for key, value in sources.items()
        if key in {"access_id", "secret_access_key", "project", "endpoint"} and value != "unset"
    }
    if required_sources == {"environment"}:
        return "environment"
    if required_sources == {"config_file"}:
        return "config_file"
    if required_sources:
        return "mixed"
    return "unknown"


def missing_odps_settings(settings: dict[str, str | None]) -> list[str]:
    return [
        name
        for name in ("access_id", "secret_access_key", "project", "endpoint")
        if not settings.get(name)
    ]


def build_odps_identity_payload(
    *,
    client: Any,
    settings: dict[str, str | None],
    allowed_operations: list[str],
    identity_source: str,
    project: str | None = None,
    owner_display_name: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    target_project = project or settings.get("project")
    access_id = getattr(client.account, "access_id", None) or settings.get("access_id")
    masked_access_id = mask_access_id(access_id) if access_id else None

    # principal_display 优先使用 owner_display_name（通过 whoami API 获取）
    # 否则使用脱敏后的 access_id
    principal_display = owner_display_name or masked_access_id

    warnings = []
    if not owner_display_name and access_id:
        warnings.append("无法获取用户显示名，principal_display 使用 access_id 脱敏值。")
    if identity_source == "mixed":
        warnings.append("当前连接信息同时来自环境变量和配置文件；环境变量优先。")

    return (
        {
            "authenticated": True,
            "backend": "odps",
            "auth_type": "access_key",
            "identity_source": identity_source,
            "principal_display": principal_display,
            "principal_masked": masked_access_id,
            "project": target_project,
            "region": settings.get("region_name"),
            "endpoint": settings.get("endpoint"),
            "project_owner": owner_display_name,
            "allowed_operations": allowed_operations,
        },
        warnings,
    )


# Partition helpers
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


# String helpers
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


# Task and job helpers
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


# Sample and profile helpers
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


# Error handling
def translate_odps_error(exc: Exception, context: str = "") -> MaxCError:
    """统一处理 ODPS 错误，提取资源信息给出精准提示。
    
    Args:
        exc: 原始异常
        context: 操作上下文，如 "list_projects", "list_schemas", "get_project_info" 等
    """
    message = str(exc)
    
    # 从错误信息中提取资源名称
    project_name = _extract_resource_name(message, "projects")
    table_name = _extract_resource_name(message, "tables")
    schema_name = _extract_resource_name(message, "schemas")
    
    if isinstance(exc, ModuleNotFoundError) and exc.name == "pandas":
        return FeatureUnavailableError(
            "当前环境缺少 pandas，无法读取包含 TIMESTAMP 等类型的 MaxCompute 结果。",
            suggestion="请安装 pandas，或在 data sample / data profile 时先只选择不依赖 pandas 的列。",
        )
    
    if isinstance(exc, OdpsNoPermission):
        return _build_permission_error(message, context, project_name, table_name, schema_name)
    
    if isinstance(exc, OdpsNoSuchObject):
        return NotFoundError(
            message,
            suggestion="请先执行 maxc meta list-tables 或 maxc meta search 确认对象是否存在。",
        )
    
    if isinstance(exc, ODPSError):
        lowered = message.lower()
        if "permission" in lowered or "access denied" in lowered or "nopermission" in lowered:
            return _build_permission_error(message, context, project_name, table_name, schema_name)
        if "parse exception" in lowered or "semantic analysis exception" in lowered:
            return SqlError(message)
        if "connection" in lowered or "failed to resolve" in lowered:
            return BackendConnectionError(
                message,
                suggestion="请检查网络连通性、Endpoint 配置和环境变量。",
            )
        return SqlError(message)
    
    return BackendConnectionError(
        message,
        suggestion="请检查 MaxCompute 网络连通性和环境变量。",
    )


def _extract_resource_name(message: str, resource_type: str) -> str | None:
    """从错误信息中提取资源名称。
    
    支持格式: {acs:odps:*:projects/xxx}, projects/xxx, tables/xxx 等
    """
    # 匹配 {acs:odps:*:projects/xxx} 或 projects/xxx
    patterns = [
        rf"\{{acs:odps:[^:]*:{resource_type}/([^}}\s]+)\}}",
        rf"{resource_type}/([^}}\s/]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _build_permission_error(
    message: str,
    context: str,
    project_name: str | None,
    table_name: str | None,
    schema_name: str | None,
) -> PermissionDeniedError:
    """构建权限错误，给出精准提示。"""
    
    # 根据上下文和提取的资源名构建提示
    if context == "list_projects" and project_name:
        return PermissionDeniedError(
            f"列出项目失败: 对项目 {project_name} 没有 Read 权限",
            suggestion=f"请确认您的账号对项目 {project_name} 有 odps:Read 权限，或联系项目管理员。",
        )
    
    if context == "list_schemas":
        target = project_name or "当前项目"
        return PermissionDeniedError(
            f"列出 schemas 失败: 对项目 {target} 没有 Read 权限",
            suggestion=f"请确认您的账号对项目 {target} 有 odps:Read 权限。",
        )
    
    if context == "get_project_info" and project_name:
        return PermissionDeniedError(
            f"获取项目信息失败: 对项目 {project_name} 没有 Read 权限",
            suggestion=f"请确认您的账号对项目 {project_name} 有 odps:Read 权限。",
        )
    
    if table_name:
        return PermissionDeniedError(
            f"操作失败: 对表 {table_name} 没有相应权限",
            suggestion=f"请确认您的账号对表 {table_name} 有操作权限，或联系项目管理员。",
        )
    
    if project_name:
        return PermissionDeniedError(
            f"操作失败: 对项目 {project_name} 没有相应权限",
            suggestion=f"请确认您的账号对项目 {project_name} 有操作权限，或联系项目管理员。",
        )
    
    # 默认错误
    return PermissionDeniedError(message)


# Data conversion helpers
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
