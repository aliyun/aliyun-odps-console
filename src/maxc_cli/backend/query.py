"""Query-related mixin for OdpsBackend."""

from time import monotonic
from typing import Any

from ..helpers import (
    build_query_outline,
    translate_odps_error,
)
from ..models import QueryResult
from ..utils import extract_table_names, now_utc_iso


class QueryMixin:
    """Mixin providing query execution methods."""

    def execute_query(
        self,
        sql: str,
        *,
        project: str,
        max_rows: int,
        dry_run: bool,
        offset: int = 0,
        timeout: int | None = None,
    ) -> QueryResult:
        """Execute a SQL query and return results.
        
        Args:
            sql: SQL query to execute
            project: Project name
            max_rows: Maximum rows to return
            dry_run: If True, only estimate cost without executing
            offset: Row offset for pagination
            timeout: Timeout in seconds (default: 300s / 5 minutes)
        """
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
                project=project,
                sql_executed=sql,
                tables_used=extract_table_names(sql),
                warnings=["MaxCompute dry-run returned SQLCost metadata and did not execute the query."],
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
            # Default timeout: 300 seconds (5 minutes) to prevent indefinite blocking
            instance.wait_for_success(timeout=timeout or 300)
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
        """Estimate the cost of a query."""
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
            "sql_complexity": sql_cost.complexity,
            "sql_udf_num": sql_cost.udf_num,
            "total_row_estimate": None,
            "elapsed_ms": int((monotonic() - started_monotonic) * 1000),
        }

    def explain_query(self, sql: str, *, project: str) -> dict[str, Any]:
        """Explain a query execution plan."""
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
    ):
        """Submit a query for async execution."""
        from ..models import JobInfo

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
            warnings=["The MaxCompute instance has been submitted; use job.status or job.wait to track it."],
        )
