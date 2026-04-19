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
    ReadOnlyError,
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
ODPS_ENV_ALIASES: 'dict[str, tuple[str, ...]]' = {
    "access_id": (
        "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ODPS_ACCESS_ID",
        "ODPS_STS_ACCESS_KEY_ID",
        "ACCESS_KEY_ID",
    ),
    "secret_access_key": (
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "ODPS_ACCESS_KEY",
        "ODPS_ACCESS_KEY_SECRET",
        "ODPS_STS_ACCESS_KEY_SECRET",
        "ACCESS_KEY_SECRET",
    ),
    "security_token": (
        "ALIBABA_CLOUD_SECURITY_TOKEN",
        "ODPS_STS_TOKEN",
        "SECURITY_TOKEN",
    ),
    "project": ("MAXCOMPUTE_PROJECT", "ODPS_PROJECT"),
    "endpoint": ("MAXCOMPUTE_ENDPOINT", "ODPS_ENDPOINT", "odps_endpoint"),
    "region_name": ("MAXCOMPUTE_REGION", "ALIBABA_CLOUD_REGION"),
    "tunnel_endpoint": ("MAXCOMPUTE_TUNNEL_ENDPOINT", "ODPS_TUNNEL_ENDPOINT"),
    "catalog_endpoint": ("MAXCOMPUTE_CATALOG_ENDPOINT",),
    "external_process_command": ("MAXCOMPUTE_EXTERNAL_PROCESS_COMMAND",),
    "external_process_timeout": ("MAXCOMPUTE_EXTERNAL_PROCESS_TIMEOUT",),
}

PARTITION_PATH_RE = re.compile(r"/(?=[A-Za-z_][\w]*\s*=)")
ISO_TIMEZONE_COLON_RE = re.compile(r"([+-]\d{2}):(\d{2})$")
FRESHNESS_FRESH_HOURS = 36
FRESHNESS_STALE_HOURS = 72


def load_odps_env() -> 'dict[str, str | None]':
    """Load ODPS settings from environment variables."""
    values: 'dict[str, str | None]' = {}
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
    settings: 'dict[str, str | None]',
    allowed_operations: 'list[str]',
) -> 'tuple[dict[str, Any], list[str]]':
    """Validate login settings by attempting to create an ODPS client.
    
    Args:
        settings: ODPS connection settings (access_id, secret_access_key, project, endpoint, etc.)
        allowed_operations: List of allowed operations for this connection
        
    Returns:
        Tuple of (identity_payload, warnings)
    """
    try:
        from odps import ODPS
        from odps.accounts import StsAccount
    except ImportError:
        raise FeatureUnavailableError("pyodps is not installed in the current environment.")
    
    auth_type = "sts_token" if settings.get("security_token") else "access_key"
    missing = missing_odps_settings(settings, auth_type=auth_type)
    if missing:
        raise ValidationError(
            f"auth login is missing required fields: {', '.join(missing)}.",
            suggestion="Provide the missing values via flags, --from-env, or interactive prompts.",
        )

    try:
        if auth_type == "sts_token":
            account = StsAccount(
                settings["access_id"],
                settings["secret_access_key"],
                settings["security_token"],
            )
            client = ODPS(
                account,
                project=settings["project"],
                endpoint=settings["endpoint"],
                region_name=settings.get("region_name") or None,
                tunnel_endpoint=settings.get("tunnel_endpoint") or None,
            )
        else:
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
        auth_type=auth_type,
        token_expires_at=settings.get("token_expires_at"),
    )


def _build_ncs_process_command_from_settings(settings: 'dict[str, str | None]') -> 'str | None':
    """Derive an ``ncs create credential …`` command from the flat settings
    dict produced by :func:`resolve_odps_settings`.  Used during the
    transparent NCS→External migration so that old ``provider: ncs``
    configs continue to work without a separate code path.
    """
    import shlex
    account_type = (settings.get("ncs_account_type") or "").strip().lower()
    if account_type == "user":
        eid = settings.get("ncs_employee_id")
        if not eid:
            return None
        return "ncs create credential odpsuser --employee-id {id} -o template -t odpscmd".format(
            id=shlex.quote(eid)
        )
    if account_type == "account":
        name = settings.get("ncs_account_name")
        if not name:
            return None
        return "ncs create credential odpsaccount --account-name {name} -o template -t odpscmd".format(
            name=shlex.quote(name)
        )
    if account_type == "app":
        name = settings.get("ncs_app_name")
        if not name:
            return None
        return "ncs create credential odpsapp --app-name {name} -o template -t odpscmd".format(
            name=shlex.quote(name)
        )
    return None


def resolve_odps_settings(
    config,
    auth_override=None,
) -> 'tuple[dict[str, str | None], dict[str, str], list[str]]':
    """Resolve ODPS settings from config and environment variables.

    Returns (settings, sources, suppressed_env_vars).

    suppressed_env_vars lists env var names that were present but ignored
    because an explicit auth provider is configured in the config file.
    When a provider is explicitly configured, env vars do not override any
    auth settings — use ``auth login --from-env`` or ``session set`` instead.
    """
    auth = auth_override or config.auth
    values = auth.to_mapping()
    settings: 'dict[str, str | None]' = {
        "provider": values.get("provider"),
        "access_id": values.get("access_id"),
        "secret_access_key": values.get("secret_access_key"),
        "security_token": values.get("security_token"),
        "token_expires_at": values.get("token_expires_at"),
        "project": values.get("project"),
        "endpoint": values.get("endpoint"),
        "region_name": values.get("region_name"),
        "tunnel_endpoint": values.get("tunnel_endpoint"),
        "catalog_endpoint": values.get("catalog_endpoint"),
        "ncs_process_command": values.get("ncs", {}).get("process_command") if isinstance(values.get("ncs"), dict) else None,
        "ncs_account_type": values.get("ncs", {}).get("account_type") if isinstance(values.get("ncs"), dict) else None,
        "ncs_employee_id": values.get("ncs", {}).get("employee_id") if isinstance(values.get("ncs"), dict) else None,
        "ncs_account_name": values.get("ncs", {}).get("account_name") if isinstance(values.get("ncs"), dict) else None,
        "ncs_app_name": values.get("ncs", {}).get("app_name") if isinstance(values.get("ncs"), dict) else None,
        "ncs_process_timeout": str(values.get("ncs", {}).get("process_timeout")) if isinstance(values.get("ncs"), dict) and values.get("ncs", {}).get("process_timeout") is not None else None,
        "external_process_command": values.get("external", {}).get("process_command") if isinstance(values.get("external"), dict) else None,
        "external_process_timeout": str(values.get("external", {}).get("process_timeout")) if isinstance(values.get("external"), dict) and values.get("external", {}).get("process_timeout") is not None else None,
    }

    # ------------------------------------------------------------------
    # NCS → External migration (runtime, transparent)
    # ------------------------------------------------------------------
    # Old configs with `provider: ncs` are normalized to `external` so
    # that a single code path (ExternalCredentialProvider + kv_store
    # cache) handles both.  This block runs before any downstream
    # provider inference, so nothing else needs to know about "ncs".
    _provider_raw = (settings.get("provider") or "").strip().lower()
    if _provider_raw == "ncs":
        ncs_cmd = settings.get("ncs_process_command")
        # If ncs_process_command is absent but we have account_type +
        # identifier, derive the command (same logic as the old
        # build_ncs_process_command_from_config).
        if not ncs_cmd and settings.get("ncs_account_type"):
            ncs_cmd = _build_ncs_process_command_from_settings(settings)
        if ncs_cmd:
            settings["external_process_command"] = ncs_cmd
            ncs_timeout = settings.get("ncs_process_timeout")
            if ncs_timeout and not settings.get("external_process_timeout"):
                settings["external_process_timeout"] = ncs_timeout
        settings["provider"] = "external"

    sources: 'dict[str, str]' = {
        key: "config_file" if value else "unset"
        for key, value in settings.items()
    }

    env_settings = load_odps_env()
    suppressed_env_vars: 'list[str]' = []

    if sources.get("provider") == "config_file":
        # User explicitly configured an auth provider via `auth login`.
        # Env vars must not silently override the saved
        # config — only env-var-based auth (Path B) is supposed to use them.
        # Collect which env vars are set so callers can surface a warning.
        for field, env_value in env_settings.items():
            if env_value:
                suppressed_env_vars.append(field)
    else:
        for field, env_value in env_settings.items():
            if env_value:
                settings[field] = env_value
                sources[field] = "environment"

    return settings, sources, suppressed_env_vars


def odps_identity_source(sources: 'dict[str, str]') -> 'str':
    required_sources = {
        value for key, value in sources.items()
        if key in {"access_id", "secret_access_key", "security_token", "project", "endpoint"} and value != "unset"
    }
    if required_sources == {"environment"}:
        return "environment"
    if required_sources == {"config_file"}:
        return "config_file"
    if required_sources:
        return "mixed"
    return "unknown"


def missing_odps_settings(
    settings: 'dict[str, str | None]',
    *,
    auth_type: 'str' = "access_key",
) -> 'list[str]':
    required = ["access_id", "secret_access_key", "project", "endpoint"]
    if auth_type == "sts_token":
        required.append("security_token")
    if auth_type == "external":
        missing = [f for f in ("project", "endpoint") if not settings.get(f)]
        if not settings.get("external_process_command"):
            missing.append("external_process_command")
        return missing
    return [name for name in required if not settings.get(name)]


def build_odps_identity_payload(
    *,
    client: 'Any | None',
    settings: 'dict[str, str | None]',
    allowed_operations: 'list[str]',
    identity_source: 'str',
    auth_type: 'str' = "access_key",
    token_expires_at: 'str | None' = None,
    project: 'str | None' = None,
    owner_display_name: 'str | None' = None,
    authenticated: 'bool' = True,
    configured: 'bool' = True,
    validation_status: 'str' = "verified",
) -> 'tuple[dict[str, Any], list[str]]':
    target_project = project or settings.get("project")
    access_id = (
        getattr(getattr(client, "account", None), "access_id", None)
        if client is not None
        else None
    ) or settings.get("access_id")
    masked_access_id = mask_access_id(access_id) if access_id else None

    # Prefer the owner display name when available from the whoami API.
    # Fall back to a masked access key id when the display name is unavailable.
    principal_display = owner_display_name or masked_access_id

    warnings = []
    if not owner_display_name and access_id:
        warnings.append("Principal display name was unavailable; falling back to a masked access key id.")
    if identity_source == "mixed":
        warnings.append("Connection settings came from both environment variables and config files; environment values take precedence.")

    payload = {
        "authenticated": authenticated,
        "configured": configured,
        "validation_status": validation_status,
        "backend": "odps",
        "auth_type": auth_type,
        "identity_source": identity_source,
        "principal_display": principal_display,
        "principal_masked": masked_access_id,
        "project": target_project,
        "region": settings.get("region_name"),
        "endpoint": settings.get("endpoint"),
        "project_owner": owner_display_name,
        "allowed_operations": allowed_operations,
    }
    if token_expires_at:
        payload["token_expires_at"] = token_expires_at

    return payload, warnings


# Partition helpers
def build_latest_partition_info(
    table: 'TableDefinition',
    *,
    source: 'str',
    partitions: 'list[str] | None' = None,
    latest_partition_override: 'str | None' = None,
    visible_partition_count: 'int | None' = None,
) -> 'tuple[dict[str, Any], list[str]]':
    warnings: 'list[str]' = []
    partition_columns = [column.name for column in table.partition_columns]
    has_partitions = bool(partition_columns)
    candidates = list(partitions) if partitions is not None else list(table.partitions)
    latest_partition = latest_partition_override or select_latest_partition(
        candidates,
        partition_columns,
    )

    if not has_partitions:
        warnings.append("The table is not partitioned, so `latest_partition` is `null`.")
    elif not latest_partition:
        warnings.append("No visible partitions were returned, so `latest_partition` is `null`.")

    partition_values = normalize_partition_values(
        parse_partition_spec(latest_partition) if latest_partition else {},
        partition_columns,
    )
    if latest_partition and not partition_values:
        warnings.append("A latest partition was returned, but its partition spec could not be parsed reliably.")

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
    table: 'TableDefinition',
    latest_partition_payload: 'dict[str, Any]',
    *,
    warnings: 'list[str] | None' = None,
) -> 'tuple[dict[str, Any], list[str]]':
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
            payload_warnings.append("The latest partition could not be converted to a timestamp, so freshness fell back to `updated_at`.")
        else:
            payload_warnings.append("The table is not partitioned, so freshness was inferred from `updated_at`.")
    elif reference_time is None:
        freshness_source = "unknown"
        if latest_partition:
            payload_warnings.append("A latest partition exists, but its values do not form a parseable timestamp.")
        else:
            payload_warnings.append("No parseable partition timestamp or `updated_at` value was available, so freshness is `unknown`.")

    age_seconds: 'int | None' = None
    age_hours: 'float | None' = None
    age_days: 'float | None' = None
    freshness_status = "unknown"
    reference_dt = parse_time_value(reference_time) if reference_time else None

    if observed_dt and reference_dt:
        delta_seconds = int((observed_dt - reference_dt).total_seconds())
        if delta_seconds < 0:
            payload_warnings.append("The reference time is in the future, so freshness age was clamped to 0 seconds.")
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


def select_latest_partition(partitions: 'list[str]', partition_columns: 'list[str]') -> 'str | None':
    if not partitions:
        return None
    return max(
        partitions,
        key=lambda item: partition_sort_key(item, partition_columns),
    )


def partition_sort_key(spec: 'str', partition_columns: 'list[str]') -> 'tuple[Any, ...]':
    values = normalize_partition_values(parse_partition_spec(spec), partition_columns)
    ordered_keys = partition_columns or list(values)
    if not ordered_keys:
        return (spec,)
    return tuple(sortable_partition_value(values.get(key)) for key in ordered_keys)


def parse_partition_spec(spec: 'str | None') -> 'dict[str, str]':
    if not spec:
        return {}
    normalized = PARTITION_PATH_RE.sub(",", spec.strip())
    values: 'dict[str, str]' = {}
    for item in normalized.split(","):
        segment = item.strip()
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


def normalize_partition_values(
    values: 'dict[str, str]',
    partition_columns: 'list[str]',
) -> 'dict[str, str]':
    ordered: 'dict[str, str]' = {}
    for key in partition_columns:
        if key in values:
            ordered[key] = values[key]
    for key, value in values.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def partition_reference_time(
    partition_values: 'dict[str, str]',
    partition_columns: 'list[str]',
) -> 'str | None':
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


def parse_partition_component(values: 'dict[str, str]', names: 'set[str]') -> 'int | None':
    for key, value in values.items():
        if key.lower() not in names:
            continue
        text = str(value).strip()
        if not re.fullmatch(r"\d{1,2}", text):
            continue
        return int(text)
    return None


def sortable_partition_value(value: 'Any') -> 'tuple[int, Any]':
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


def parse_date_value(value: 'Any') -> 'date | None':
    text = str(value).strip().strip("'").strip('"')
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def normalize_time_value(value: 'Any') -> 'str | None':
    if value is None:
        return None
    text = str(value).strip().strip("'").strip('"')
    if not text:
        return None
    parsed = parse_time_value(text)
    return parsed.isoformat() if parsed is not None else None


def parse_time_value(value: 'Any') -> 'datetime | None':
    if value is None:
        return None
    text = str(value).strip().strip("'").strip('"')
    if not text:
        return None

    candidate = text.replace("Z", "+00:00")
    fromisoformat = getattr(datetime, "fromisoformat", None)
    parsed = None
    if fromisoformat is not None:
        try:
            parsed = fromisoformat(candidate)
        except ValueError:
            parsed = None
    if parsed is not None:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    normalized_candidate = ISO_TIMEZONE_COLON_RE.sub(r"\1\2", candidate)
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y%m%d",
    ):
        try:
            source = normalized_candidate if "%z" in fmt else text
            parsed = datetime.strptime(source, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


def partition_spec_text(partition: 'Any') -> 'str | None':
    if partition is None:
        return None
    spec = getattr(partition, "partition_spec", None)
    target = spec if spec is not None else partition
    text = str(target).strip()
    return text or None


# String helpers
def mask_access_id(value: 'str | None') -> 'str | None':
    if not value:
        return None
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}***{value[-4:]}"


def quote_table_name(table_name: 'str') -> 'str':
    if not re.fullmatch(r"[A-Za-z_][\w]*(\.[A-Za-z_][\w]*)*", table_name):
        raise ValidationError(
            f"Invalid table name: {table_name}",
            suggestion="Use a plain table name or `project.table` form instead of passing a SQL fragment.",
        )
    return ".".join(f"`{part}`" for part in table_name.split("."))


def sql_string_literal(value: 'str') -> 'str':
    return "'" + value.replace("'", "''") + "'"


# Task and job helpers
def build_task_summary(
    sql: 'str | None',
    *,
    task_names: 'list[str] | None' = None,
    task_types: 'dict[str, str] | None' = None,
) -> 'dict[str, Any]':
    normalized_sql = (sql or "").strip().rstrip(";")
    return {
        "operation": detect_operation(normalized_sql) if normalized_sql else "UNKNOWN",
        "tables_used": extract_table_names(normalized_sql) if normalized_sql else [],
        "task_names": task_names or [],
        "task_types": task_types or {},
        "sql_excerpt": normalized_sql[:200] if normalized_sql else None,
    }


def classify_failure_reason(reason: 'str | None') -> 'dict[str, Any]':
    text = (reason or "").strip()
    if not text:
        return {
            "category": "not_failed",
            "retryable": None,
            "summary": "No diagnosable failure reason is currently available.",
        }
    lowered = text.lower()
    if any(token in lowered for token in ("semantic analysis exception", "parse exception", "syntax")):
        return {
            "category": "sql_semantic_error",
            "retryable": False,
            "summary": "SQL semantic or syntax error.",
        }
    if any(token in lowered for token in ("permission", "access denied", "no privilege")):
        return {
            "category": "permission_error",
            "retryable": False,
            "summary": "Permission was denied or access was rejected.",
        }
    if any(token in lowered for token in ("cancel", "取消")):
        return {
            "category": "client_cancelled",
            "retryable": False,
            "summary": "The job was cancelled explicitly.",
        }
    if any(token in lowered for token in ("quota", "resource", "timeout", "timed out", "memory")):
        return {
            "category": "resource_or_timeout_error",
            "retryable": True,
            "summary": "Resource, quota, or timeout issue; retrying may help.",
        }
    return {
        "category": "unknown_failure",
        "retryable": False,
        "summary": "The failure reason could not be classified.",
    }


# Sample and profile helpers
def resolve_sample_request(
    table: 'TableDefinition',
    *,
    partition: 'str | None',
    columns: 'list[str] | None',
    strict_partition_check: 'bool',
) -> 'tuple[list[str], str | None, dict[str, str]]':
    column_lookup = {
        column.name: column
        for column in [*table.columns, *table.partition_columns]
    }
    selected_columns = columns or [column.name for column in table.columns]
    missing_columns = [column for column in selected_columns if column not in column_lookup]
    if missing_columns:
        raise ValidationError(
            f"Unknown column(s): {', '.join(missing_columns)}",
            suggestion="Run `maxc meta describe` to inspect the available schema.",
        )

    applied_partition = None
    partition_values: 'dict[str, str]' = {}
    if partition:
        partition_column_names = [column.name for column in table.partition_columns]
        if not partition_column_names:
            raise ValidationError(
                f"Table `{table.name}` is not partitioned, so `--partition` cannot be used.",
                suggestion="Run `maxc meta describe` or `maxc meta latest-partition` first.",
            )
        partition_values = normalize_partition_values(
            parse_partition_spec(partition),
            partition_column_names,
        )
        if not partition_values:
            raise ValidationError(
                "`--partition` could not be parsed.",
                suggestion="Example: `--partition ds=2026-03-20`",
            )
        if set(partition_values) != set(partition_column_names):
            raise ValidationError(
                f"`--partition` must provide all partition columns: {', '.join(partition_column_names)}.",
                suggestion=f"Example: `--partition {canonical_partition_spec(dict.fromkeys(partition_column_names, '...'))}`",
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
                    f"Partition does not exist: {applied_partition}",
                    suggestion="Run `maxc meta latest-partition` or `maxc meta partitions` first.",
                )

    deduped_columns: 'list[str]' = []
    for column in selected_columns:
        if column not in deduped_columns:
            deduped_columns.append(column)
    return deduped_columns, applied_partition, partition_values


def canonical_partition_spec(values: 'dict[str, str]') -> 'str':
    return ",".join(f"{key}={value}" for key, value in values.items())


def build_sample_schema(table: 'TableDefinition', selected_columns: 'list[str]') -> 'list[dict[str, Any]]':
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
    rows: 'list[dict[str, Any]]',
    selected_columns: 'list[str]',
    partition_values: 'dict[str, str]',
) -> 'list[dict[str, Any]]':
    projected_rows: 'list[dict[str, Any]]' = []
    for row in rows:
        projected: 'dict[str, Any]' = {}
        for column in selected_columns:
            if column in row:
                projected[column] = row[column]
            else:
                projected[column] = partition_values.get(column)
        projected_rows.append(projected)
    return projected_rows


def top_values(values: 'list[Any]', limit: 'int' = 5) -> 'list[dict[str, Any]]':
    counter = Counter(str(value) for value in values)
    return [
        {"value": value, "count": count}
        for value, count in counter.most_common(limit)
    ]


def build_profile(
    table: 'TableDefinition',
    sample_rows: 'list[dict[str, Any]]',
    *,
    applied_partition: 'str | None' = None,
) -> 'dict[str, Any]':
    profiles = []
    for column in table.columns:
        values = [row.get(column.name) for row in sample_rows]
        non_null = [value for value in values if value is not None]
        profile: 'dict[str, Any]' = {
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
        "partition_count": len(table.partitions),
        "applied_partition": applied_partition,
        "sampled_rows": len(sample_rows),
        "columns": profiles,
    }


def build_query_outline(sql: 'str') -> 'dict[str, Any]':
    return {
        "operation": detect_operation(sql),
        "normalized_sql": normalize_sql(sql),
        "tables_used": extract_table_names(sql),
        "projected_columns": parse_select_projection(sql),
    }


def translate_odps_error(exc: 'Exception', context: 'str' = "") -> 'MaxCError':
    """Translate ODPS errors into structured CLI errors."""
    message = str(exc)

    project_name = _extract_resource_name(message, "projects")
    table_name = _extract_resource_name(message, "tables")
    schema_name = _extract_resource_name(message, "schemas")

    if isinstance(exc, ModuleNotFoundError) and exc.name == "pandas":
        return FeatureUnavailableError(
            "pandas is required to read MaxCompute result sets that contain TIMESTAMP-like types.",
            suggestion="Install pandas, or limit sampling / profiling to columns that do not require pandas.",
        )

    if isinstance(exc, OdpsNoPermission):
        return _build_permission_error(message, context, project_name, table_name, schema_name)

    if isinstance(exc, OdpsNoSuchObject):
        return NotFoundError(
            message,
            suggestion="Run `maxc meta list-tables` or `maxc meta search` to verify the object exists.",
        )

    if isinstance(exc, ODPSError):
        lowered = message.lower()
        if "permission" in lowered or "access denied" in lowered or "nopermission" in lowered:
            return _build_permission_error(message, context, project_name, table_name, schema_name)
        if "readonly mode" in lowered or "read.only" in lowered:
            return ReadOnlyError(
                message,
                suggestion=(
                    "maxc-cli enforces server-side read-only mode. "
                    "DDL/DML operations are not supported. "
                    "Use odpscmd, pyodps SDK, or DataWorks for write operations."
                ),
            )
        if "parse exception" in lowered or "semantic analysis exception" in lowered:
            return SqlError(message)
        if "connection" in lowered or "failed to resolve" in lowered:
            return BackendConnectionError(
                message,
                suggestion="Check network connectivity, the configured endpoint, and environment variables.",
            )
        return SqlError(message)

    return BackendConnectionError(
        message,
        suggestion="Check MaxCompute network connectivity and environment configuration.",
    )


def _extract_resource_name(message: 'str', resource_type: 'str') -> 'str | None':
    """Extract a resource name from an ODPS error message."""
    patterns = [
        rf"\{{acs:odps:[^:]*:{resource_type}/([^}}\s]+)\}}",
        rf"{resource_type}/([^}}\s/]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


# ODPS error patterns for SQL error classification
_COLUMN_ERROR_PATTERNS = [
    re.compile(r"column\s+(\S+)\s+cannot be resolved", re.IGNORECASE),
    re.compile(r"Invalid column reference\s+['\"]?([^'\"]+?)['\"]?(?:\s|$)", re.IGNORECASE),
    re.compile(r"Unknown column\s+['\"]?([^'\"]+?)['\"]?(?:\s|$)", re.IGNORECASE),
]

_TABLE_ERROR_PATTERNS = [
    re.compile(r"Table not found.*?table\s+(\S+)\s+cannot be resolved", re.IGNORECASE),
    re.compile(r"table\s+(\S+)\s+does not exist", re.IGNORECASE),
    re.compile(r"Table\s+['\"]?([^'\"]+?)['\"]?\s+doesn't exist", re.IGNORECASE),
    re.compile(r"Table not found\s*[:\-]?\s*(\S+)", re.IGNORECASE),
]


def classify_sql_error(message: str) -> dict[str, Any]:
    """Classify an ODPS SQL error message for self-correction context.

    Returns dict with 'error_type' and optionally 'column_name' or 'table_name'.
    """
    for pattern in _COLUMN_ERROR_PATTERNS:
        match = pattern.search(message)
        if match:
            return {"error_type": "column_not_found", "column_name": match.group(1)}

    for pattern in _TABLE_ERROR_PATTERNS:
        match = pattern.search(message)
        if match:
            return {"error_type": "table_not_found", "table_name": match.group(1)}

    lowered = message.lower()
    if "semantic analysis exception" in lowered or "parse exception" in lowered:
        return {"error_type": "generic_sql_error"}

    return {"error_type": "unknown"}


def _build_permission_error(
    message: 'str',
    context: 'str',
    project_name: 'str | None',
    table_name: 'str | None',
    schema_name: 'str | None',
) -> 'PermissionDeniedError':
    """Build permission-denied errors with more precise suggestions."""
    if context == "list_projects" and project_name:
        return PermissionDeniedError(
            f"Failed to list projects: missing Read permission on project {project_name}.",
            suggestion=f"Verify that your account has `odps:Read` on project {project_name}, or contact the project owner.",
        )

    if context == "list_schemas":
        target = project_name or "the current project"
        return PermissionDeniedError(
            f"Failed to list schemas: missing Read permission on project {target}.",
            suggestion=f"Verify that your account has `odps:Read` on project {target}.",
        )

    if context == "get_project_info" and project_name:
        return PermissionDeniedError(
            f"Failed to read project info: missing Read permission on project {project_name}.",
            suggestion=f"Verify that your account has `odps:Read` on project {project_name}.",
        )

    if table_name:
        return PermissionDeniedError(
            f"Operation failed: missing required permission on table {table_name}.",
            suggestion=f"Verify that your account has the required permission on table {table_name}, or contact the project owner.",
        )

    if project_name:
        return PermissionDeniedError(
            f"Operation failed: missing required permission on project {project_name}.",
            suggestion=f"Verify that your account has the required permission on project {project_name}, or contact the project owner.",
        )

    return PermissionDeniedError(message)


# Data conversion helpers
def record_to_dict(columns: 'list[str]', values: 'Iterable[Any]') -> 'dict[str, Any]':
    return {
        column: json_safe(value)
        for column, value in zip(columns, values)
    }


def json_safe(value: 'Any') -> 'Any':
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


def _dt_to_iso(value: 'datetime | None') -> 'str | None':
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).isoformat()
    return value.isoformat()


def _duration_ms(start: 'datetime | None', end: 'datetime | None') -> 'int':
    if start is None or end is None:
        return 0
    return int((end - start).total_seconds() * 1000)
