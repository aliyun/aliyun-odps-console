"""Query-related mixin for OdpsBackend."""

from time import monotonic
from typing import Any

from ..exceptions import ValidationError, WriteOperationRequiresForceError
from ..helpers import (
    build_query_outline,
    translate_odps_error,
)
from ..models import QueryResult
from ..setting_parser import SettingParser
from ..utils import detect_operation, extract_table_names, now_utc_iso

def _parse_sql_with_hints(sql: 'str', *, force: 'bool' = False) -> 'tuple[str, dict[str, str]]':
    """Extract SET statements from *sql* and enforce client-side read-only mode.

    Returns ``(remaining_sql, merged_hints)`` where ``merged_hints``
    contains only user-supplied SET values.  Write operations
    (INSERT, CREATE, DROP, etc.) are blocked unless *force* is ``True``.
    """
    parsed = SettingParser.parse(sql)
    if parsed.errors:
        raise ValidationError(
            f"Invalid SET statement in SQL: {'; '.join(parsed.errors)}",
            suggestion="Check SET syntax: SET key=value; must end with semicolon.",
        )
    hints = dict(parsed.settings)
    remaining = parsed.remaining_query.strip()

    # Client-side write detection (replaces server-side odps.sql.read.only hint)
    if not force:
        operation = detect_operation(remaining)
        if operation.upper() not in {"SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN"}:
            raise WriteOperationRequiresForceError(
                f"Write operation '{operation}' blocked by read-only mode. "
                f"Use --force to override.",
                suggestion="Re-run with --force to execute write operations.",
            )

    return remaining, hints


class QueryMixin:
    """Mixin providing query execution methods."""

    def execute_query(
        self,
        sql: 'str',
        *,
        project: 'str',
        max_rows: 'int',
        dry_run: 'bool',
        offset: 'int' = 0,
        timeout: 'int | None' = None,
        force: 'bool' = False,
    ) -> 'QueryResult':
        """Execute a SQL query and return results.

        Parses any leading ``SET key=value;`` statements from the SQL
        and passes them as execution hints to the MaxCompute backend.
        Write operations (INSERT, CREATE, DROP, etc.) are blocked
        client-side unless *force* is True.

        Args:
            sql: SQL query, optionally prefixed with SET statements.
            project: ODPS project name.
            max_rows: Maximum rows to return in the result set.
            dry_run: If True, only estimate cost without executing (uses
                ``client.execute_sql_cost()``).
            offset: Row offset for cursor-based pagination.
            timeout: Timeout in seconds (default: 300s / 5 minutes).
            force: If True, skip client-side write detection (allows DDL/DML).

        Raises:
            ValidationError: If SET syntax is invalid.
            BackendConnectionError: If ODPS connection fails.
        """
        actual_sql, hints = _parse_sql_with_hints(sql, force=force)

        started_at = now_utc_iso()
        started_monotonic = monotonic()

        if dry_run:
            try:
                sql_cost = self.client.execute_sql_cost(
                    actual_sql, project=project, hints=hints,
                )
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
                tables_used=extract_table_names(actual_sql),
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
            instance = self.client.execute_sql(
                actual_sql, project=project, hints=hints,
            )
        except Exception as exc:
            raise translate_odps_error(exc) from exc

        try:
            # Default timeout: 300 seconds (5 minutes) to prevent indefinite blocking
            instance.wait_for_success(timeout=timeout or 300)
        except Exception as exc:
            err = translate_odps_error(exc)
            err.instance_id = instance.id
            err.logview = self._safe_logview(instance)
            raise err from exc

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

    def estimate_query_cost(self, sql: 'str', *, project: 'str', force: 'bool' = False) -> 'dict[str, Any]':
        """Estimate the cost of a query using ODPS dry-run.

        Calls ``client.execute_sql_cost()`` which returns ``SQLCost`` metadata
        without actually executing the query. Provides input size, complexity,
        and UDF count estimates.

        Args:
            sql: SQL query, optionally prefixed with SET statements.
            project: ODPS project name.
            force: If True, skip read-only hint injection.

        Returns:
            Dict with estimated_input_size_bytes, sql_complexity, sql_udf_num, etc.
        """
        actual_sql, hints = _parse_sql_with_hints(sql, force=force)
        started_monotonic = monotonic()
        try:
            sql_cost = self.client.execute_sql_cost(
                actual_sql, project=project, hints=hints,
            )
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return {
            **build_query_outline(actual_sql),
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

    def explain_query(self, sql: 'str', *, project: 'str', force: 'bool' = False) -> 'dict[str, Any]':
        """Explain a query execution plan using ODPS dry-run.

        Similar to ``estimate_query_cost`` but focused on the execution
        plan outline rather than cost metrics.

        Args:
            sql: SQL query, optionally prefixed with SET statements.
            project: ODPS project name.
            force: If True, skip read-only hint injection.

        Returns:
            Dict with query outline and cost metadata.
        """
        estimate = self.estimate_query_cost(sql, project=project, force=force)
        warnings = list(estimate.pop("warnings", []))
        estimate["warnings"] = warnings
        estimate["analysis_mode"] = "explain"
        estimate["read_path"] = True
        return estimate

    def submit_query(
        self,
        sql: 'str',
        *,
        project: 'str',
        idempotency_key: 'str | None' = None,
        force: 'bool' = False,
    ):
        """Submit a query for async execution without waiting.

        Calls ``client.execute_sql()`` but does not call
        ``instance.wait_for_success()``. Returns immediately with a
        job ID that can be polled via ``wait_job`` / ``get_job``.

        Args:
            sql: SQL query, optionally prefixed with SET statements.
            project: ODPS project name.
            idempotency_key: Optional unique ID for deduplication.
            force: If True, skip read-only hint injection.

        Returns:
            JobInfo with status and job_id.
        """
        from ..models import JobInfo

        actual_sql, hints = _parse_sql_with_hints(sql, force=force)

        try:
            instance = self.client.execute_sql(
                actual_sql,
                project=project,
                hints=hints,
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
