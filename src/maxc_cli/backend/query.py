"""Query-related mixin for OdpsBackend."""

import re
from time import monotonic
from typing import Any

from ..exceptions import (
    ValidationError,
    WriteOperationRequiresForceError,
)
from ..helpers import (
    build_query_outline,
    translate_odps_error,
)
from ..models import QueryResult
from ..setting_parser import SettingParser
from ..utils import detect_operation, extract_table_names, now_utc_iso

_COMMENT_LINE_RE = re.compile(r"--[^\n]*")
_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


# Operations that mutate state. Detection is leading-keyword based, so an
# unrecognized leading word ("SELEKT" typo, vendor extension) is allowed
# through to MaxCompute, which will surface a proper SQL parser error
# rather than being misclassified as a write.
_WRITE_OPERATIONS = frozenset({
    "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE",
    "CREATE", "DROP", "ALTER", "RENAME", "TRUNCATE",
    "GRANT", "REVOKE",
    "ANALYZE", "OPTIMIZE", "COMPACT", "VACUUM",
    "USE", "ADD", "REMOVE", "PURGE", "INSTALL", "UNINSTALL", "LOAD",
})


def _strip_sql_comments(sql: 'str') -> 'str':
    """Remove SQL comments so statement-counting isn't fooled by ``;`` inside comments."""
    sql = _COMMENT_BLOCK_RE.sub("", sql)
    sql = _COMMENT_LINE_RE.sub("", sql)
    return sql


def _count_statements(sql: 'str') -> 'int':
    """Count non-empty SQL statements separated by ``;``, ignoring comments."""
    cleaned = _strip_sql_comments(sql)
    return sum(1 for part in cleaned.split(";") if part.strip())


def _parse_sql_with_hints(
    sql: 'str', *, force: 'bool' = False,
) -> 'tuple[str, dict[str, str], int | None]':
    """Extract SET statements from *sql* and enforce client-side read-only mode.

    Returns ``(remaining_sql, merged_hints, priority)``. ``merged_hints``
    contains user-supplied SET values minus ``odps.instance.priority``,
    which is lifted out into ``priority`` so callers can pass it as the
    ``priority=`` kwarg of ``run_sql`` / ``execute_sql``.

    Write operations (INSERT, CREATE, DROP, etc.) are blocked unless
    *force* is ``True``. Empty SQL raises ``ValidationError``.
    Multi-statement SQL automatically receives
    ``odps.sql.submit.mode=script`` unless the user already set it.
    """
    parsed = SettingParser.parse(sql)
    if parsed.errors:
        raise ValidationError(
            f"Invalid SET statement in SQL: {'; '.join(parsed.errors)}",
            suggestion="Check SET syntax: SET key=value; must end with semicolon.",
        )
    hints = dict(parsed.settings)
    remaining = parsed.remaining_query.strip()

    if not remaining:
        raise ValidationError(
            "SQL query is empty.",
            suggestion="Provide a SELECT statement via inline text, --file, or --stdin.",
        )

    # Client-side write detection (replaces server-side odps.sql.read.only hint).
    # Block only known-write keywords; pass typos / unknown leading words
    # through so MaxCompute returns a proper SQL parser error.
    if not force:
        operation = detect_operation(remaining).upper()
        if operation in _WRITE_OPERATIONS:
            raise WriteOperationRequiresForceError(
                f"Write operation '{operation}' blocked by read-only mode. "
                f"Use --force to override.",
                suggestion="Re-run with --force to execute write operations.",
            )

    # Multi-statement SQL needs script mode for MaxCompute to accept it.
    if _count_statements(remaining) >= 2:
        hints.setdefault("odps.sql.submit.mode", "script")

    # odps.instance.priority is not a SQL hint — it's a top-level kwarg on
    # run_sql/execute_sql. Lift it out so the caller can thread it through.
    priority = _pop_priority(hints)

    return remaining, hints, priority


def _pop_priority(hints: 'dict[str, str]') -> 'int | None':
    """Pop ``odps.instance.priority`` from *hints* and parse as int.

    Match is case-insensitive on the key. Returns ``None`` if absent.
    Raises ``ValidationError`` if the value isn't an integer.
    """
    matched_key: str | None = None
    for k in hints:
        if k.lower() == "odps.instance.priority":
            matched_key = k
            break
    if matched_key is None:
        return None
    raw = hints.pop(matched_key)
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValidationError(
            f"Invalid odps.instance.priority value {raw!r}: must be an integer.",
            suggestion="Use SET odps.instance.priority=N; where N is an integer.",
        ) from None


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
        actual_sql, hints, priority = _parse_sql_with_hints(sql, force=force)
        priority_kwargs = {"priority": priority} if priority is not None else {}

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
            instance = self.client.run_sql(
                actual_sql, project=project, hints=hints, **priority_kwargs,
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
        actual_sql, hints, _priority = _parse_sql_with_hints(sql, force=force)
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
        """Explain a query execution plan.

        Runs MaxCompute ``EXPLAIN <sql>`` to get the actual textual execution
        plan, then attaches cost-estimate metadata from ``execute_sql_cost``
        for context.

        Args:
            sql: SQL query, optionally prefixed with SET statements.
            project: ODPS project name.
            force: If True, skip read-only hint injection.

        Returns:
            Dict with query outline, cost metadata, and ``execution_plan`` text.
        """
        actual_sql, hints, priority = _parse_sql_with_hints(sql, force=force)
        priority_kwargs = {"priority": priority} if priority is not None else {}
        # script-mode auto-hint doesn't apply to EXPLAIN itself; remove if present.
        explain_hints = {k: v for k, v in hints.items() if k != "odps.sql.submit.mode"}
        started_monotonic = monotonic()

        plan_text: str | None = None
        plan_warning: str | None = None
        try:
            instance = self.client.execute_sql(
                f"EXPLAIN {actual_sql}",
                project=project,
                hints=explain_hints,
                **priority_kwargs,
            )
            try:
                results = instance.get_task_results()
                if results:
                    plan_text = "\n".join(
                        text for text in results.values() if text
                    ).strip() or None
            except Exception as inner:
                plan_warning = f"Could not retrieve EXPLAIN output: {inner}"
        except Exception as exc:
            plan_warning = f"EXPLAIN failed: {exc}"

        # Cost estimate alongside the plan
        try:
            sql_cost = self.client.execute_sql_cost(
                actual_sql, project=project, hints=hints,
            )
        except Exception:
            sql_cost = None

        out: dict[str, Any] = {
            **build_query_outline(actual_sql),
            "project": project,
            "cost_model": "maxcompute_native_sql_cost",
            "estimated_input_size_bytes": int(sql_cost.input_size or 0) if sql_cost else None,
            "sql_complexity": sql_cost.complexity if sql_cost else None,
            "sql_udf_num": sql_cost.udf_num if sql_cost else None,
            "execution_plan": plan_text,
            "analysis_mode": "explain",
            "read_path": True,
            "elapsed_ms": int((monotonic() - started_monotonic) * 1000),
        }
        warnings: list[str] = []
        if plan_warning:
            warnings.append(plan_warning)
        if plan_text is None and not plan_warning:
            warnings.append(
                "EXPLAIN returned no plan text; only cost estimate is available."
            )
        out["warnings"] = warnings
        return out

    def submit_query(
        self,
        sql: 'str',
        *,
        project: 'str',
        idempotency_key: 'str | None' = None,
        force: 'bool' = False,
    ):
        """Submit a query for async execution without waiting.

        Calls ``client.run_sql()`` to create the instance without waiting
        for completion. Returns immediately with a job ID that can be
        polled via ``wait_job`` / ``get_job``.

        Args:
            sql: SQL query, optionally prefixed with SET statements.
            project: ODPS project name.
            idempotency_key: Optional unique ID for deduplication.
            force: If True, skip read-only hint injection.

        Returns:
            JobInfo with status and job_id.
        """
        from ..models import JobInfo

        actual_sql, hints, priority = _parse_sql_with_hints(sql, force=force)
        priority_kwargs = {"priority": priority} if priority is not None else {}
        idem_kwargs = {"unique_identifier_id": idempotency_key} if idempotency_key is not None else {}

        try:
            instance = self.client.run_sql(
                actual_sql,
                project=project,
                hints=hints,
                **idem_kwargs,
                **priority_kwargs,
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
