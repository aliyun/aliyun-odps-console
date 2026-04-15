
from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Any

import yaml

from .exceptions import ValidationError
from .utils import deep_merge, resolve_path


@dataclass
class TableColumn:
    name: 'str'
    type: 'str'
    comment: 'str' = ""

    @classmethod
    def from_mapping(cls, payload: 'dict[str, Any]') -> "TableColumn":
        return cls(
            name=str(payload["name"]),
            type=str(payload.get("type", "string")),
            comment=str(payload.get("comment", "")),
        )


@dataclass
class TableDefinition:
    name: 'str'
    description: 'str'
    columns: 'list[TableColumn]' = field(default_factory=list)
    sample_rows: 'list[dict[str, Any]]' = field(default_factory=list)
    partitions: 'list[str]' = field(default_factory=list)
    upstream_tables: 'list[str]' = field(default_factory=list)
    downstream_tables: 'list[str]' = field(default_factory=list)
    partition_columns: 'list[TableColumn]' = field(default_factory=list)
    owner: 'str | None' = None
    created_at: 'str | None' = None
    updated_at: 'str | None' = None
    table_type: 'str | None' = None
    size_bytes: 'int | None' = None
    extra_metadata: 'dict[str, Any]' = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: 'dict[str, Any]') -> "TableDefinition":
        return cls(
            name=str(payload["name"]),
            description=str(payload.get("description", "")),
            columns=[TableColumn.from_mapping(item) for item in payload.get("columns", [])],
            sample_rows=list(payload.get("sample_rows", [])),
            partitions=[str(item) for item in payload.get("partitions", [])],
            upstream_tables=[str(item) for item in payload.get("upstream_tables", [])],
            downstream_tables=[str(item) for item in payload.get("downstream_tables", [])],
            partition_columns=[
                TableColumn.from_mapping(item)
                for item in payload.get("partition_columns", [])
            ],
            owner=str(payload["owner"]) if payload.get("owner") is not None else None,
            created_at=(
                str(payload["created_at"]) if payload.get("created_at") is not None else None
            ),
            updated_at=(
                str(payload["updated_at"]) if payload.get("updated_at") is not None else None
            ),
            table_type=(
                str(payload["table_type"]) if payload.get("table_type") is not None else None
            ),
            size_bytes=(
                int(payload["size_bytes"]) if payload.get("size_bytes") is not None else None
            ),
            extra_metadata=dict(payload.get("extra_metadata", {})),
        )


@dataclass
class AgentConfig:
    auto_approve_cost_cu: 'float' = 10
    safety_mode: 'str' = "strict"
    audit_log: 'Path | None' = None


# BackendConfig removed - only ODPS backend is supported


@dataclass
class NcsAuthConfig:
    account_type: 'str | None' = None
    employee_id: 'str | None' = None
    account_name: 'str | None' = None
    app_name: 'str | None' = None
    process_command: 'str | None' = None
    process_timeout: 'int' = 20

    @classmethod
    def from_mapping(cls, payload: 'dict[str, Any] | None') -> "NcsAuthConfig":
        payload = payload or {}
        return cls(
            account_type=_optional_string(payload.get("account_type")),
            employee_id=_optional_string(payload.get("employee_id")),
            account_name=_optional_string(payload.get("account_name")),
            app_name=_optional_string(payload.get("app_name")),
            process_command=_optional_string(
                payload.get("process_command") or payload.get("command")
            ),
            process_timeout=int(payload.get("process_timeout", 20)),
        )

    def to_mapping(self) -> 'dict[str, Any]':
        payload: 'dict[str, Any]' = {}
        if self.account_type:
            payload["account_type"] = self.account_type
        if self.employee_id:
            payload["employee_id"] = self.employee_id
        if self.account_name:
            payload["account_name"] = self.account_name
        if self.app_name:
            payload["app_name"] = self.app_name
        if self.process_command:
            payload["process_command"] = self.process_command
        if payload:
            payload["process_timeout"] = self.process_timeout
        return payload

    def is_configured(self) -> 'bool':
        return bool(
            self.process_command
            or self.employee_id
            or self.account_name
            or self.app_name
        )


@dataclass
class AuthConfig:
    provider: 'str | None' = None
    access_id: 'str | None' = None
    secret_access_key: 'str | None' = None
    security_token: 'str | None' = None
    token_expires_at: 'str | None' = None
    project: 'str | None' = None
    endpoint: 'str | None' = None
    region_name: 'str | None' = None
    tunnel_endpoint: 'str | None' = None
    catalog_endpoint: 'str | None' = None
    ncs: 'NcsAuthConfig' = field(default_factory=NcsAuthConfig)

    @classmethod
    def from_mapping(cls, payload: 'dict[str, Any]') -> "AuthConfig":
        return cls(
            provider=_optional_string(payload.get("provider")),
            access_id=_optional_string(
                payload.get("access_id") or payload.get("access_key_id")
            ),
            secret_access_key=_optional_string(
                payload.get("secret_access_key") or payload.get("access_key_secret")
            ),
            security_token=_optional_string(
                payload.get("security_token") or payload.get("sts_token")
            ),
            token_expires_at=_optional_string(payload.get("token_expires_at")),
            project=_optional_string(payload.get("project")),
            endpoint=_optional_string(payload.get("endpoint")),
            region_name=_optional_string(
                payload.get("region_name") or payload.get("region")
            ),
            tunnel_endpoint=_optional_string(payload.get("tunnel_endpoint")),
            catalog_endpoint=_optional_string(payload.get("catalog_endpoint")),
            ncs=NcsAuthConfig.from_mapping(payload.get("ncs") if isinstance(payload.get("ncs"), dict) else None),
        )

    def to_mapping(self) -> 'dict[str, Any]':
        payload: 'dict[str, Any]' = {}
        if self.provider:
            payload["provider"] = self.provider
        if self.access_id:
            payload["access_id"] = self.access_id
        if self.secret_access_key:
            payload["secret_access_key"] = self.secret_access_key
        if self.security_token:
            payload["security_token"] = self.security_token
        if self.token_expires_at:
            payload["token_expires_at"] = self.token_expires_at
        if self.project:
            payload["project"] = self.project
        if self.endpoint:
            payload["endpoint"] = self.endpoint
        if self.region_name:
            payload["region_name"] = self.region_name
        if self.tunnel_endpoint:
            payload["tunnel_endpoint"] = self.tunnel_endpoint
        if self.catalog_endpoint:
            payload["catalog_endpoint"] = self.catalog_endpoint
        if self.ncs.is_configured():
            payload["ncs"] = self.ncs.to_mapping()
        return payload


@dataclass
class MaxCConfig:
    default_project: 'str'
    default_schema: 'str | None'
    default_format: 'str'
    default_region: 'str'
    project_context: 'str'
    allowed_operations: 'list[str]'
    cost_threshold_cu: 'float'
    sensitive_columns: 'list[str]'
    agent: 'AgentConfig'
    auth: 'AuthConfig'
    state_dir: 'Path'
    cache_dir: 'Path'
    catalog: 'dict[str, TableDefinition]'
    sources: 'list[Path]'


def _optional_string(value: 'Any') -> 'str | None':
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _load_yaml_file(path: 'Path') -> 'dict[str, Any]':
    if not path.exists() or path.is_dir():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ValidationError(
            f"Configuration file contains invalid YAML: {path}",
            suggestion=f"Fix the syntax in {path}, or delete it and re-run `maxc auth login` to create a fresh config.",
        ) from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"Invalid configuration file format: {path}")
    return payload


def default_global_config_path() -> 'Path':
    return Path.home() / ".maxc" / "config.yaml"


def session_override_path() -> 'Path':
    """Path to session override file (highest priority for project/schema)."""
    return Path.home() / ".maxc" / "session_override.yaml"


def load_config_mapping(path: 'Path') -> 'dict[str, Any]':
    return _load_yaml_file(path)


def save_config_mapping(path: 'Path', payload: 'dict[str, Any]') -> 'None':
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    try:
        path.chmod(0o600)
    except OSError:
        pass


def persist_login_config(
    target_path: 'Path',
    *,
    auth: 'AuthConfig',
) -> 'dict[str, Any]':
    payload = load_config_mapping(target_path) if target_path.exists() else {}

    payload["auth"] = auth.to_mapping()

    if auth.project:
        payload["default_project"] = auth.project
    if auth.region_name:
        payload["default_region"] = auth.region_name

    save_config_mapping(target_path, payload)
    return payload


def discover_config_files(cwd: 'Path', explicit_path: 'Path | None' = None) -> 'list[Path]':
    if explicit_path is not None:
        if not explicit_path.exists():
            raise ValidationError(f"Configuration file does not exist: {explicit_path}")
        return [explicit_path.resolve()]

    candidates = [
        Path.home() / ".maxc" / "config.yaml",
        cwd / ".maxc" / "config.yaml",
        cwd / ".maxc.yaml",
        cwd / ".maxc",
    ]
    paths: 'list[Path]' = []
    for candidate in candidates:
        if candidate.exists() and not candidate.is_dir():
            paths.append(candidate.resolve())
    return paths


def load_config(cwd: 'Path', explicit_path: 'Path | None' = None) -> 'MaxCConfig':
    sources = discover_config_files(cwd, explicit_path)
    merged: 'dict[str, Any]' = {}
    for source in sources:
        merged = deep_merge(merged, _load_yaml_file(source))

    # Load session override (highest priority for project/schema)
    override = _load_yaml_file(session_override_path())

    env_project = (
        os.environ.get("MAXCOMPUTE_PROJECT")
        or os.environ.get("ODPS_PROJECT")
        or None
    )
    env_region = (
        os.environ.get("MAXCOMPUTE_REGION")
        or os.environ.get("ALIBABA_CLOUD_REGION")
        or None
    )

    auth_payload = merged.get("auth", {}) or {}
    if not isinstance(auth_payload, dict):
        raise ValidationError("The `auth` configuration must be a mapping.")
    auth = AuthConfig.from_mapping(auth_payload)

    # Priority: session override > env var > config file > auth
    # Exception: when auth.provider is explicitly configured, auth.project takes
    # priority over env vars — env vars must not silently reroute to a different
    # project when the user has committed to a specific auth configuration.
    has_explicit_auth_provider = bool(auth.provider)
    default_project_value = merged.get("default_project")
    if override.get("project"):
        default_project = str(override["project"])
    elif env_project and not has_explicit_auth_provider:
        default_project = env_project
    elif default_project_value is not None:
        default_project = str(default_project_value)
    elif auth.project:
        default_project = auth.project
    elif env_project:
        # Fallback: env var still used when no auth.project is available
        default_project = env_project
    else:
        default_project = "demo_project"

    # Priority: session override > config file
    default_schema = _optional_string(override.get("schema")) or _optional_string(merged.get("default_schema"))

    default_format = str(merged.get("default_format", "json"))
    default_region_value = merged.get("default_region")
    if env_region:
        default_region = str(env_region)
    elif auth.region_name:
        default_region = auth.region_name
    elif default_region_value is not None:
        default_region = str(default_region_value)
    else:
        default_region = "local"
    project_context = str(merged.get("project_context", "")).strip()
    allowed_operations = [
        str(item).upper() for item in merged.get("allowed_operations", ["SELECT"])
    ]
    cost_threshold_cu = float(merged.get("cost_threshold_cu", 50))
    sensitive_columns = [str(item) for item in merged.get("sensitive_columns", [])]

    agent_payload = merged.get("agent", {}) or {}
    if not isinstance(agent_payload, dict):
        raise ValidationError("The `agent` configuration must be a mapping.")
    state_dir = resolve_path(
        merged.get("state_dir", "~/.maxc/state"),
        base_dir=cwd,
    )
    cache_dir = resolve_path(
        merged.get("cache_dir", "~/.maxc/cache"),
        base_dir=cwd,
    )
    audit_log = resolve_path(
        agent_payload.get("audit_log", str(state_dir / "audit.log")),
        base_dir=cwd,
    )
    agent = AgentConfig(
        auto_approve_cost_cu=float(agent_payload.get("auto_approve_cost_cu", 10)),
        safety_mode=str(agent_payload.get("safety_mode", "strict")),
        audit_log=audit_log,
    )

    tables = {}
    catalog_payload = merged.get("catalog", {}) or {}
    table_items = catalog_payload.get("tables", []) if isinstance(catalog_payload, dict) else []
    for item in table_items:
        table = TableDefinition.from_mapping(item)
        tables[table.name] = table


    return MaxCConfig(
        default_project=default_project,
        default_schema=default_schema,
        default_format=default_format,
        default_region=default_region,
        project_context=project_context,
        allowed_operations=allowed_operations,
        cost_threshold_cu=cost_threshold_cu,
        sensitive_columns=sensitive_columns,
        agent=agent,
        auth=auth,
        state_dir=state_dir,
        cache_dir=cache_dir,
        catalog=tables,
        sources=sources,
    )
