from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Any

import yaml

from .exceptions import ValidationError
from .utils import deep_merge, resolve_path


@dataclass(slots=True)
class TableColumn:
    name: str
    type: str
    comment: str = ""

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "TableColumn":
        return cls(
            name=str(payload["name"]),
            type=str(payload.get("type", "string")),
            comment=str(payload.get("comment", "")),
        )


@dataclass(slots=True)
class TableDefinition:
    name: str
    description: str
    row_count: int
    columns: list[TableColumn] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    partitions: list[str] = field(default_factory=list)
    upstream_tables: list[str] = field(default_factory=list)
    downstream_tables: list[str] = field(default_factory=list)
    partition_columns: list[TableColumn] = field(default_factory=list)
    owner: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    table_type: str | None = None
    row_count_source: str | None = None
    size_bytes: int | None = None
    extra_metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "TableDefinition":
        return cls(
            name=str(payload["name"]),
            description=str(payload.get("description", "")),
            row_count=int(payload.get("row_count", len(payload.get("sample_rows", [])))),
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
            row_count_source=(
                str(payload["row_count_source"])
                if payload.get("row_count_source") is not None
                else "config"
            ),
            size_bytes=(
                int(payload["size_bytes"]) if payload.get("size_bytes") is not None else None
            ),
            extra_metadata=dict(payload.get("extra_metadata", {})),
        )


@dataclass(slots=True)
class AgentConfig:
    auto_approve_cost_cu: float = 10
    safety_mode: str = "strict"
    audit_log: Path | None = None


@dataclass(slots=True)
class BackendConfig:
    type: str = "auto"


@dataclass(slots=True)
class MaxCConfig:
    default_project: str
    default_format: str
    default_region: str
    project_context: str
    allowed_operations: list[str]
    cost_threshold_cu: float
    sensitive_columns: list[str]
    agent: AgentConfig
    backend: BackendConfig
    state_dir: Path
    skill_dirs: list[Path]
    catalog: dict[str, TableDefinition]
    sources: list[Path]


def _load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists() or path.is_dir():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError(f"配置文件格式错误: {path}")
    return payload


def discover_config_files(cwd: Path, explicit_path: Path | None = None) -> list[Path]:
    if explicit_path is not None:
        if not explicit_path.exists():
            raise ValidationError(f"配置文件不存在: {explicit_path}")
        return [explicit_path.resolve()]

    candidates = [
        Path.home() / ".maxc" / "config.yaml",
        cwd / ".maxc" / "config.yaml",
        cwd / ".maxc.yaml",
        cwd / ".maxc",
    ]
    paths: list[Path] = []
    for candidate in candidates:
        if candidate.exists() and not candidate.is_dir():
            paths.append(candidate.resolve())
    return paths


def load_config(cwd: Path, explicit_path: Path | None = None) -> MaxCConfig:
    sources = discover_config_files(cwd, explicit_path)
    merged: dict[str, Any] = {}
    for source in sources:
        merged = deep_merge(merged, _load_yaml_file(source))

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

    backend_payload = merged.get("backend", {}) or {}
    if not isinstance(backend_payload, dict):
        raise ValidationError("backend 配置必须是对象。")
    backend_type = str(backend_payload.get("type", "auto")).lower()

    default_project_value = merged.get("default_project")
    if backend_type in {"auto", "odps", "maxcompute"} and env_project:
        default_project = env_project
    else:
        default_project = str(default_project_value or "demo_project")

    default_format = str(merged.get("default_format", "table"))
    default_region = str(
        env_region
        if backend_type in {"auto", "odps", "maxcompute"} and env_region
        else merged.get("default_region", "local")
    )
    project_context = str(merged.get("project_context", "")).strip()
    allowed_operations = [
        str(item).upper() for item in merged.get("allowed_operations", ["SELECT"])
    ]
    cost_threshold_cu = float(merged.get("cost_threshold_cu", 50))
    sensitive_columns = [str(item) for item in merged.get("sensitive_columns", [])]

    agent_payload = merged.get("agent", {}) or {}
    if not isinstance(agent_payload, dict):
        raise ValidationError("agent 配置必须是对象。")
    state_dir = resolve_path(
        merged.get("state_dir", ".maxc/state"),
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

    backend = BackendConfig(type=backend_type)

    raw_skill_dirs = merged.get("skill_dirs", [".maxc/skills"])
    if not isinstance(raw_skill_dirs, list):
        raise ValidationError("skill_dirs 必须是数组。")
    skill_dirs = [resolve_path(str(item), base_dir=cwd) for item in raw_skill_dirs]

    tables = {}
    catalog_payload = merged.get("catalog", {}) or {}
    table_items = catalog_payload.get("tables", []) if isinstance(catalog_payload, dict) else []
    for item in table_items:
        table = TableDefinition.from_mapping(item)
        tables[table.name] = table

    return MaxCConfig(
        default_project=default_project,
        default_format=default_format,
        default_region=default_region,
        project_context=project_context,
        allowed_operations=allowed_operations,
        cost_threshold_cu=cost_threshold_cu,
        sensitive_columns=sensitive_columns,
        agent=agent,
        backend=backend,
        state_dir=state_dir,
        skill_dirs=skill_dirs,
        catalog=tables,
        sources=sources,
    )
