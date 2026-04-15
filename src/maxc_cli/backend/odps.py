"""Main OdpsBackend class combining all mixins."""

from itertools import islice
from typing import Any

from ..auth_providers import resolve_auth_connection
from ..config import MaxCConfig
from ..exceptions import PermissionDeniedError
from ..helpers import (
    _dt_to_iso,
    record_to_dict,
    translate_odps_error,
)
from ..models import QueryResult
from ..utils import detect_operation, extract_table_names
from .auth import AuthMixin
from .catalog import CatalogMixin
from .data import DataMixin
from .job import JobMixin
from .meta import MetaMixin


class OdpsBackend(
    JobMixin,  # JobMixin extends QueryMixin
    CatalogMixin,
    MetaMixin,
    DataMixin,
    AuthMixin,
):
    """MaxCompute backend for production use."""

    supports_remote_jobs = True
    supports_cost_check = False

    def __init__(self, config: 'MaxCConfig') -> 'None':
        """Initialize OdpsBackend with configuration."""
        self.config = config
        resolved = resolve_auth_connection(config)
        self.resolved_auth = resolved
        self.settings = resolved.settings
        self.setting_sources = resolved.setting_sources
        # Priority: config.default_project (includes session_override) > resolved.project
        self.project = config.default_project or resolved.project
        # Update resolved settings with the actual project being used
        self.settings = dict(resolved.settings)
        self.settings["project"] = self.project
        self.client = resolved.create_client()
        # 延迟获取 owner display name，避免不必要的 API 调用
        self._owner_display_name: 'str | None' = None

    def _validate_select(self, sql: 'str') -> 'None':
        """Validate that SQL is a SELECT statement and allowed by config."""
        operation = detect_operation(sql)
        if operation not in self.config.allowed_operations:
            raise PermissionDeniedError(
                f"Configured allowed operations are limited to {', '.join(self.config.allowed_operations)}; received {operation}.",
                suggestion="Update `allowed_operations` if you intentionally want to permit this operation.",
            )
        if operation != "SELECT":
            raise PermissionDeniedError(f"This CLI currently supports only SELECT statements; received {operation}.")

    def _instance_to_query_result(
        self,
        instance,
        *,
        project: 'str',
        max_rows: 'int',
        sql: 'str',
        elapsed_ms: 'int',
        offset: 'int' = 0,
    ) -> 'QueryResult':
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

    def _task_cost(self, instance) -> 'tuple[int | None, dict[str, Any]]':
        """Get task cost from ODPS instance."""
        try:
            task_cost = instance.get_task_cost()
        except Exception:
            return None, {}
        if task_cost is None:
            return None, {}
        return (
            int(getattr(task_cost, "input_size", 0) or 0),
            {
                "task_cost_cpu": getattr(task_cost, "cpu_cost", None),
                "task_cost_memory": getattr(task_cost, "memory_cost", None),
                "estimated_input_size_bytes": getattr(task_cost, "input_size", None),
            },
        )
