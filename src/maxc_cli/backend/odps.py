"""Main OdpsBackend class combining all mixins."""

from itertools import islice
from typing import Any

from ..config import MaxCConfig
from ..exceptions import BackendConnectionError, FeatureUnavailableError, PermissionDeniedError
from ..helpers import (
    _dt_to_iso,
    missing_odps_settings,
    record_to_dict,
    resolve_odps_settings,
    translate_odps_error,
)
from ..models import QueryResult
from ..utils import detect_operation, extract_table_names
from .auth import AuthMixin
from .base import BaseBackend
from .data import DataMixin
from .job import JobMixin
from .meta import MetaMixin


class OdpsBackend(
    JobMixin,  # JobMixin extends QueryMixin
    MetaMixin,
    DataMixin,
    AuthMixin,
    BaseBackend,
):
    """MaxCompute backend for production use."""

    supports_remote_jobs = True
    supports_cost_check = False

    def __init__(self, config: MaxCConfig) -> None:
        """Initialize OdpsBackend with configuration."""
        try:
            from odps import ODPS
        except ImportError:
            raise FeatureUnavailableError("当前环境未安装 pyodps，无法连接 MaxCompute。")

        self.config = config
        self.settings, self.setting_sources = resolve_odps_settings(config)
        missing = missing_odps_settings(self.settings)
        if missing:
            raise BackendConnectionError(
                f"缺少 MaxCompute 连接配置: {', '.join(missing)}",
                suggestion=(
                    "请先执行 maxc auth login，或设置 ALIBABA_CLOUD_ACCESS_KEY_ID、"
                    "ALIBABA_CLOUD_ACCESS_KEY_SECRET、MAXCOMPUTE_PROJECT、MAXCOMPUTE_ENDPOINT。"
                ),
            )

        self.project = self.settings["project"] or config.default_project
        self.client = ODPS(
            access_id=self.settings["access_id"],
            secret_access_key=self.settings["secret_access_key"],
            project=self.project,
            endpoint=self.settings["endpoint"],
            region_name=self.settings.get("region_name") or None,
            tunnel_endpoint=self.settings.get("tunnel_endpoint") or None,
        )
        # 延迟获取 owner display name，避免不必要的 API 调用
        self._owner_display_name: str | None = None

    def _validate_select(self, sql: str) -> None:
        """Validate that SQL is a SELECT statement and allowed by config."""
        operation = detect_operation(sql)
        if operation not in self.config.allowed_operations:
            raise PermissionDeniedError(
                f"当前配置仅允许 {', '.join(self.config.allowed_operations)}，不允许执行 {operation}。",
                suggestion="如需支持写操作，请调整 .maxc/config.yaml 中的 allowed_operations。",
            )
        if operation != "SELECT":
            raise PermissionDeniedError(f"当前版本仅支持 SELECT，实际收到 {operation}。")

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
        """Convert ODPS instance to QueryResult."""
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
        extra_metadata["current_offset"] = offset

        return QueryResult(
            rows=rows,
            schema=schema,
            total_rows=total_rows,
            returned_rows=returned_rows,
            has_more=has_more,
            next_cursor=None,  # cursor 由 app 层生成
            elapsed_ms=elapsed_ms,
            bytes_scanned=bytes_scanned,
            project=project,
            sql_executed=sql.rstrip(";"),
            tables_used=extract_table_names(sql),
            job_id=instance.id,
            submitted_at=_dt_to_iso(getattr(instance, "start_time", None)),
            completed_at=_dt_to_iso(getattr(instance, "end_time", None)),
            extra_metadata=extra_metadata,
        )

    def _task_cost(self, instance) -> tuple[int | None, dict[str, Any]]:
        """Get task cost from ODPS instance."""
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
