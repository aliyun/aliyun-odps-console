"""Job-related mixin for OdpsBackend."""

import re
from itertools import islice
from time import monotonic, sleep
from typing import Any

from ..exceptions import BackendConnectionError, JobTimeoutError, ValidationError
from ..helpers import (
    OdpsNoSuchObject,
    _dt_to_iso,
    _duration_ms,
    build_task_summary,
    classify_failure_reason,
    translate_odps_error,
)
from ..models import JobInfo, QueryResult
from ..utils import now_utc_iso
from .query import QueryMixin

_SQLRT_SUBQUERY_ID_RE = re.compile(r"(?:^|_)session_query_(\d+)(?:_|$)")


def _extract_sqlrt_subquery_id(detail: 'Any') -> 'int | None':
    explicit_match: int | None = None
    fallback_match: int | None = None

    def _walk(value: 'Any') -> 'None':
        nonlocal explicit_match, fallback_match
        if isinstance(value, dict):
            for key in ("subQueryId", "subqueryId", "subquery_id"):
                candidate = value.get(key)
                if candidate is None:
                    continue
                text = str(candidate)
                if text.startswith("session_query_"):
                    text = text[len("session_query_"):]
                try:
                    explicit_match = int(text)
                except (TypeError, ValueError):
                    pass
            for nested in value.values():
                _walk(nested)
            return

        if isinstance(value, (list, tuple)):
            for nested in value:
                _walk(nested)
            return

        if isinstance(value, str):
            match = _SQLRT_SUBQUERY_ID_RE.search(value)
            if match:
                fallback_match = int(match.group(1))

    _walk(detail)
    return explicit_match if explicit_match is not None else fallback_match


class JobMixin(QueryMixin):
    """Mixin providing job management methods."""

    def get_job(
        self,
        job_id: 'str',
        *,
        project: 'str | None' = None,
        session_context: 'dict[str, Any] | None' = None,
    ) -> 'JobInfo':
        """Get job status by ID.

        Calls ``instance.reload()`` to fetch the latest status from ODPS.

        Args:
            job_id: ODPS instance/job ID.
            project: Optional project override.

        Returns:
            JobInfo with status, progress, stage, and error details.
        """
        instance = self._get_instance(job_id, project=project, session_context=session_context)
        return self._instance_to_job_info(instance, project=project or self.project)

    def wait_job(
        self,
        job_id: 'str',
        *,
        project: 'str | None' = None,
        timeout: 'int | None' = None,
        poll_interval: 'int' = 3,
        session_context: 'dict[str, Any] | None' = None,
    ) -> 'JobInfo':
        """Wait for job completion with polling and timeout.

        Polls ``instance.reload()`` every ``poll_interval`` seconds until
        the job reaches a terminal state (succeeded/failed/cancelled) or
        the timeout expires. Detects consecutive network errors and
        raises ``BackendConnectionError`` after 5 consecutive failures.

        Args:
            job_id: ODPS instance/job ID.
            project: Optional project override.
            timeout: Timeout in seconds (default: 300s / 5 minutes).
            poll_interval: Seconds between status checks (default: 3s).

        Raises:
            JobTimeoutError: If job does not complete within timeout.
            BackendConnectionError: After 5 consecutive reload() failures.
        """
        instance = self._get_instance(job_id, project=project, session_context=session_context)
        start_time = monotonic()
        default_timeout = timeout if timeout is not None else 300
        consecutive_errors = 0

        while True:
            elapsed = monotonic() - start_time
            if elapsed > default_timeout:
                raise JobTimeoutError(
                    f"Job {job_id} did not complete within {default_timeout} seconds"
                )

            try:
                instance.reload(blocking=False)
                consecutive_errors = 0
            except Exception as exc:
                # SQLRT session instances can return an empty inner `status`
                # payload even though their outer MaxCompute instance is
                # healthy and authoritative. Refresh that outer instance
                # before classifying the failure as lost backend contact.
                outer = self._outer_instance(instance)
                if outer is not None:
                    try:
                        outer.reload(blocking=False)
                    except Exception:
                        pass
                    else:
                        consecutive_errors = 0
                        if self._safe_status_name(instance) == "TERMINATED":
                            break
                        sleep(poll_interval)
                        continue
                consecutive_errors += 1
                if consecutive_errors >= 5:
                    raise BackendConnectionError(
                        f"Lost contact with backend after 5 consecutive errors: {exc}",
                        suggestion="Check network connectivity and retry.",
                    ) from exc
                sleep(poll_interval)
                continue

            status_name = self._safe_status_name(instance)
            if status_name == "TERMINATED":
                break

            sleep(poll_interval)

        return self._instance_to_job_info(instance, project=project or self.project)

    def fetch_job_result(
        self,
        job_id: 'str',
        *,
        project: 'str | None' = None,
        max_rows: 'int',
        offset: 'int' = 0,
        session_context: 'dict[str, Any] | None' = None,
    ) -> 'QueryResult':
        """Fetch job results with cursor-based pagination.

        Reads results from a completed ODPS instance. Only works when
        job status is ``success``.

        Args:
            job_id: ODPS instance/job ID.
            project: Optional project override.
            max_rows: Maximum rows to return.
            offset: Row offset for pagination.

        Raises:
            FeatureUnavailableError: If job is not in ``success`` state.
        """
        from ..exceptions import FeatureUnavailableError

        instance = self._get_instance(job_id, project=project, session_context=session_context)
        info = self._instance_to_job_info(instance, project=project or self.project)
        if info.status != "success":
            raise FeatureUnavailableError(
                f"Job {job_id} is currently {info.status}; results are not readable yet.",
                suggestion="Run `maxc job wait` or `maxc job status` first.",
            )
        result_instance = self._rehydrate_sqlrt_result_instance(instance)
        sql = self._safe_sql(result_instance) or self._safe_sql(instance) or ""
        return self._instance_to_query_result(
            result_instance,
            project=project or self.project,
            max_rows=max_rows,
            sql=sql,
            elapsed_ms=_duration_ms(instance.start_time, instance.end_time),
            offset=offset,
        )

    def cancel_job(self, job_id: 'str', *, project: 'str | None' = None) -> 'JobInfo':
        """Cancel a running job.

        Calls ``instance.stop()`` on the ODPS instance. If the job has
        already reached a terminal state (success / failure / cancelled),
        the server rejects the stop with an "Invalid state setting" error;
        we treat that as a no-op and return the current job info instead
        of surfacing a confusing error.

        Args:
            job_id: ODPS instance/job ID.
            project: Optional project override.

        Returns:
            JobInfo with updated status after cancellation attempt.
        """
        instance = self._get_instance(job_id, project=project)
        try:
            instance.stop()
        except Exception as exc:
            msg = str(exc)
            if "Invalid state setting" in msg or "not allowed to set status" in msg:
                # Already terminal — return current state with a note.
                info = self._instance_to_job_info(
                    instance, project=project or self.project,
                )
                note = (
                    f"Job `{job_id}` is already in terminal state "
                    f"`{info.status}`; cancellation is a no-op."
                )
                existing = list(info.warnings or [])
                existing.append(note)
                info.warnings = existing
                return info
            raise translate_odps_error(exc) from exc
        sql = self._safe_sql(instance)
        return JobInfo(
            job_id=job_id,
            status="running",
            project=project or self.project,
            progress=0,
            stage="cancel_requested",
            retryable=None,
            failure_reason=None,
            task_summary=build_task_summary(sql),
            sql=sql,
            submitted_at=_dt_to_iso(getattr(instance, "start_time", None)),
            updated_at=now_utc_iso(),
            logview=self._safe_logview(instance),
            warnings=["Cancellation has been requested. Run `job status` again to confirm the final state."],
        )

    def diagnose_job(
        self,
        job_id: 'str',
        *,
        project: 'str | None' = None,
        session_context: 'dict[str, Any] | None' = None,
    ) -> 'dict[str, Any]':
        """Diagnose a failed or problematic job.

        Assembles diagnostic information from instance status, task summary,
        logview URL, and failure reason classification. No dedicated ODPS
        diagnose API exists — this is a composite analysis.

        Limitations:
            - Some failure patterns may not be correctly classified.
            - Relies on available instance metadata only.

        Args:
            job_id: ODPS instance/job ID.
            project: Optional project override.

        Returns:
            Dict with status, failure_reason, retryable, logview, task_summary.
        """
        instance = self._get_instance(job_id, project=project, session_context=session_context)
        info = self._instance_to_job_info(instance, project=project or self.project)
        diagnosis = classify_failure_reason(info.failure_reason)
        task_statuses = self._safe_task_statuses(instance)
        task_results = self._safe_task_results(instance)
        return {
            "job_id": info.job_id,
            "status": info.status,
            "stage": info.stage,
            "retryable": info.retryable,
            "failure_reason": info.failure_reason,
            "diagnosis_category": diagnosis["category"],
            "diagnosis_summary": diagnosis["summary"],
            "logview": info.logview,
            "task_summary": info.task_summary,
            "task_statuses": [
                {
                    "task_name": name,
                    "status": str(getattr(task, "status", "")).split(".")[-1].lower(),
                    "type": str(getattr(task, "type", "") or ""),
                }
                for name, task in task_statuses.items()
            ],
            "task_results": task_results,
        }

    def list_jobs(
        self, *, project: 'str | None' = None, limit: 'int' = 20
    ) -> 'tuple[list[JobInfo], bool]':
        """List recent jobs in the project.

        Calls ``client.list_instances()`` to retrieve recent job history.
        Results are ordered by creation time (newest first). Iteration takes
        ``limit + 1`` items so ``has_more`` can be reported without a second
        pass — same pattern as ``list_tables``.

        Args:
            project: Optional project override.
            limit: Maximum number of jobs to return (default 20).

        Returns:
            ``(jobs, has_more)`` — ``jobs`` is the requested window;
            ``has_more`` is ``True`` if at least one job exists past the window.
        """
        jobs: list[JobInfo] = []
        has_more = False
        try:
            iterator = self.client.list_instances(project=project or self.project)
            window = list(islice(iterator, limit + 1))
            has_more = len(window) > limit
            for instance in window[:limit]:
                jobs.append(self._instance_to_job_info(instance, project=project or self.project))
        except Exception as exc:
            raise translate_odps_error(exc, context="job") from exc
        return jobs, has_more

    # Private methods for job handling

    def _get_instance(
        self,
        job_id: 'str',
        *,
        project: 'str | None' = None,
        session_context: 'dict[str, Any] | None' = None,
    ):
        """Get ODPS instance by job ID."""
        try:
            instance = self.client.get_instance(job_id, project=project or self.project)
        except Exception as exc:
            raise translate_odps_error(exc, context="job") from exc
        if session_context:
            return self._rehydrate_saved_sqlrt_instance(instance, session_context=session_context)
        return instance

    def _safe_task_statuses(self, instance) -> 'dict[str, Any]':
        """Safely get task statuses from instance."""
        try:
            return dict(instance.get_task_statuses())
        except Exception:
            return {}

    def _safe_task_results(self, instance) -> 'dict[str, str]':
        """Safely get task results from instance."""
        try:
            results = instance.get_task_results()
        except Exception:
            return {}
        return {
            str(name): str(value)
            for name, value in dict(results).items()
        }

    def _safe_task_detail2(self, instance, task_name: 'str') -> 'Any | None':
        try:
            return instance.get_task_detail2(task_name)
        except Exception:
            return None

    def _build_session_result_instance(
        self,
        instance,
        *,
        session_task_name: 'str',
        session_subquery_id: 'int',
        session_project_name: 'str | None' = None,
        session_is_select: 'bool' = True,
    ):
        session_project = getattr(instance, "project", None)
        project_name = session_project_name or getattr(session_project, "name", None)
        client = getattr(instance, "_client", None)
        parent = getattr(instance, "parent", None)
        if session_project is None or project_name is None or client is None or parent is None:
            return instance

        try:
            from odps.models.session.v1 import InSessionInstance, SessionInstance

            session_instance = SessionInstance.from_instance(
                instance,
                session_task_name=session_task_name,
                session_project=session_project,
            )
            session_result = InSessionInstance(
                session_project_name=project_name,
                session_task_name=session_task_name,
                name=instance.id,
                session_subquery_id=session_subquery_id,
                session_instance=session_instance,
                parent=parent,
                session_is_select=session_is_select,
                client=client,
            )
            object.__setattr__(session_result, "_maxc_outer_instance", instance)
            object.__setattr__(session_result, "start_time", getattr(instance, "start_time", None))
            object.__setattr__(session_result, "end_time", getattr(instance, "end_time", None))
            return session_result
        except Exception:
            return instance

    def _rehydrate_saved_sqlrt_instance(self, instance, *, session_context: 'dict[str, Any]'):
        session_subquery_id = session_context.get("session_subquery_id")
        if session_subquery_id is None:
            return instance
        session_task_name = session_context.get("session_task_name")
        if not session_task_name:
            return self._rehydrate_inferred_sqlrt_instance(
                instance,
                session_subquery_id=int(session_subquery_id),
                session_context=session_context,
            )
        session_result = self._build_session_result_instance(
            instance,
            session_task_name=str(session_task_name),
            session_subquery_id=int(session_subquery_id),
            session_project_name=session_context.get("session_project_name"),
            session_is_select=bool(session_context.get("session_is_select", True)),
        )
        if session_result is instance:
            raise ValidationError("Composite MCQA job IDs could not be resolved to a SQLRT session job.")
        return session_result

    def _rehydrate_inferred_sqlrt_instance(
        self,
        instance,
        *,
        session_subquery_id: 'int',
        session_context: 'dict[str, Any] | None' = None,
    ):
        task_statuses = self._safe_task_statuses(instance)
        sqlrt_task_names = [
            name
            for name, task in task_statuses.items()
            if str(getattr(task, "type", "") or "").upper() == "SQLRT"
        ]
        if not sqlrt_task_names:
            raise ValidationError("Composite MCQA job IDs must target a SQLRT session job.")
        if len(sqlrt_task_names) != 1:
            raise ValidationError("Composite MCQA job IDs require exactly one resolvable SQLRT task.")
        project_name = None
        if session_context is not None:
            project_name = session_context.get("session_project_name")
        if project_name is None:
            project_name = getattr(getattr(instance, "project", None), "name", None)
        if project_name is None:
            raise ValidationError("Composite MCQA job IDs require SQLRT session metadata to resolve the session project.")
        session_result = self._build_session_result_instance(
            instance,
            session_task_name=sqlrt_task_names[0],
            session_subquery_id=session_subquery_id,
            session_project_name=str(project_name),
            session_is_select=bool((session_context or {}).get("session_is_select", True)),
        )
        if session_result is instance:
            raise ValidationError("Composite MCQA job IDs could not be resolved to a SQLRT session job.")
        return session_result

    def _rehydrate_sqlrt_result_instance(self, instance):
        if getattr(instance, "subquery_id", None) is not None:
            return instance

        task_statuses = self._safe_task_statuses(instance)
        sqlrt_task_name = next(
            (
                name
                for name, task in task_statuses.items()
                if str(getattr(task, "type", "") or "").upper() == "SQLRT"
            ),
            None,
        )
        if sqlrt_task_name is None:
            return instance

        detail = self._safe_task_detail2(instance, sqlrt_task_name)
        subquery_id = _extract_sqlrt_subquery_id(detail)
        if subquery_id is None:
            return instance

        return self._build_session_result_instance(
            instance,
            session_task_name=sqlrt_task_name,
            session_subquery_id=subquery_id,
            session_is_select=True,
        )

    def _first_failure_reason(self, instance) -> 'str | None':
        """Get first non-empty failure reason from task results."""
        task_results = self._safe_task_results(instance)
        for value in task_results.values():
            text = str(value).strip()
            if text:
                return text
        return None

    def _raw_attr(self, instance, name: 'str', default: 'Any' = None) -> 'Any':
        try:
            return object.__getattribute__(instance, name)
        except AttributeError:
            return default
        except Exception:
            return default

    def _outer_instance(self, instance):
        return self._raw_attr(instance, "_maxc_outer_instance")

    def _safe_status_name(self, instance) -> 'str':
        try:
            return str(instance.status).split(".")[-1]
        except Exception:
            pass

        outer = self._outer_instance(instance)
        if outer is not None:
            try:
                return str(outer.status).split(".")[-1]
            except Exception:
                pass
            raw_outer_status = self._raw_attr(outer, "_status")
            if raw_outer_status is not None:
                return str(raw_outer_status).split(".")[-1]

        raw_status = self._raw_attr(instance, "_status")
        if raw_status is not None:
            return str(raw_status).split(".")[-1]
        return ""

    def _session_query_sql(self, instance, outer) -> 'str | None':
        session_task_name = self._raw_attr(instance, "_session_task_name")
        subquery_id = self._raw_attr(instance, "_subquery_id")
        if not session_task_name or subquery_id is None:
            return None
        detail = self._safe_task_detail2(outer, str(session_task_name))
        if not isinstance(detail, dict):
            return None
        map_reduce = detail.get("mapReduce")
        if not isinstance(map_reduce, dict):
            return None
        needle = f"session_query_{subquery_id}_"
        for plan in map_reduce.get("plans", []):
            if not isinstance(plan, dict):
                continue
            job_name = str(plan.get("jobName") or "")
            if needle not in job_name:
                continue
            query = plan.get("query")
            if query:
                return str(query).rstrip(";")
        return None

    def _session_success_from_outer(self, instance, outer) -> 'bool | None':
        session_task_name = self._raw_attr(instance, "_session_task_name")
        if session_task_name:
            task = self._safe_task_statuses(outer).get(str(session_task_name))
            if task is not None:
                task_status = str(getattr(task, "status", "") or "").split(".")[-1]
                if task_status == "SUCCESS":
                    return True
                if task_status in {"FAILED", "CANCELLED"}:
                    return False
        try:
            return bool(outer.is_successful())
        except Exception:
            return None

    def _session_job_info_from_outer(self, instance, *, project: 'str', reload_error: 'Exception | None' = None) -> 'JobInfo | None':
        outer = self._outer_instance(instance)
        if outer is None:
            return None

        status_name = self._safe_status_name(instance)
        sql = self._safe_sql(instance) or self._session_query_sql(instance, outer) or self._safe_sql(outer)
        logview = self._safe_logview(instance) or self._safe_logview(outer)
        submitted_at = _dt_to_iso(getattr(outer, "start_time", None))
        completed_at = _dt_to_iso(getattr(outer, "end_time", None))
        task_statuses = self._safe_task_statuses(outer)
        task_names = sorted(task_statuses)
        task_types = {
            name: str(getattr(task, "type", "") or "")
            for name, task in task_statuses.items()
        }
        task_summary = build_task_summary(sql, task_names=task_names, task_types=task_types)

        if status_name == "RUNNING":
            return JobInfo(
                job_id=instance.id,
                status="running",
                project=project,
                progress=50,
                stage="running",
                retryable=None,
                task_summary=task_summary,
                sql=sql,
                submitted_at=submitted_at,
                updated_at=now_utc_iso(),
                completed_at=completed_at,
                logview=logview,
            )

        if status_name == "TERMINATED":
            succeeded = self._session_success_from_outer(instance, outer)
            if succeeded is None:
                return None
            failure_reason = None if succeeded else self._first_failure_reason(outer)
            if not succeeded and reload_error is not None and not failure_reason:
                failure_reason = str(reload_error)
            diagnosis = classify_failure_reason(failure_reason)
            return JobInfo(
                job_id=instance.id,
                status="success" if succeeded else "failure",
                project=project,
                progress=100,
                stage="completed" if succeeded else "failed",
                retryable=False if succeeded else diagnosis["retryable"],
                failure_reason=failure_reason,
                task_summary=task_summary,
                sql=sql,
                submitted_at=submitted_at,
                updated_at=now_utc_iso(),
                completed_at=completed_at,
                logview=logview,
                error_message=None if succeeded else failure_reason,
            )

        return JobInfo(
            job_id=instance.id,
            status="pending",
            project=project,
            progress=0,
            stage="queue",
            retryable=None,
            task_summary=task_summary,
            sql=sql,
            submitted_at=submitted_at,
            updated_at=now_utc_iso(),
            completed_at=completed_at,
            logview=logview,
        )

    def _instance_to_job_info(self, instance, *, project: 'str') -> 'JobInfo':
        """Convert ODPS instance to JobInfo."""
        reload_error: Exception | None = None
        try:
            instance.reload(blocking=False)
        except OdpsNoSuchObject as exc:
            # The job ID has been purged or never existed. Do NOT swallow:
            # we'd otherwise return a JobInfo with status='pending' for a
            # non-existent job, masking the real error. Translate so the CLI
            # surfaces a NOT_FOUND envelope with a non-zero exit code.
            raise translate_odps_error(exc, context="job") from exc
        except Exception as exc:
            try:
                from odps.errors import InvalidArgument, InvalidParameter
            except ImportError:  # pragma: no cover - remote jobs require PyODPS
                invalid_job_errors: tuple[type[Exception], ...] = ()
            else:
                invalid_job_errors = (InvalidArgument, InvalidParameter)
            if isinstance(exc, invalid_job_errors):
                raise ValidationError(
                    f"MaxCompute rejected job ID `{instance.id}`: {exc}",
                    suggestion=(
                        "Use the exact `metadata.job_id` returned by query/job submit, "
                        "or list visible jobs with `maxc job list --json`."
                    ),
                ) from exc
            # Other reload failures (transient network, partial server errors)
            # fall through — downstream attribute reads (status, start_time,
            # task_statuses) are best-effort and degrade gracefully.
            reload_error = exc

        fallback_info = self._session_job_info_from_outer(instance, project=project, reload_error=reload_error)
        if reload_error is not None and fallback_info is not None:
            return fallback_info

        status_name = self._safe_status_name(instance)
        sql = self._safe_sql(instance)
        logview = self._safe_logview(instance)
        submitted_at = _dt_to_iso(getattr(instance, "start_time", None))
        completed_at = _dt_to_iso(getattr(instance, "end_time", None))
        task_statuses = self._safe_task_statuses(instance)
        task_names = sorted(task_statuses)
        task_types = {
            name: str(getattr(task, "type", "") or "")
            for name, task in task_statuses.items()
        }
        task_summary = build_task_summary(sql, task_names=task_names, task_types=task_types)

        if status_name == "RUNNING":
            return JobInfo(
                job_id=instance.id,
                status="running",
                project=project,
                progress=50,
                stage="running",
                retryable=None,
                task_summary=task_summary,
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
                fallback_info = self._session_job_info_from_outer(instance, project=project, reload_error=exc)
                if fallback_info is not None:
                    return fallback_info
                return JobInfo(
                    job_id=instance.id,
                    status="failure",
                    project=project,
                    progress=100,
                    stage="failed",
                    retryable=False,
                    failure_reason=str(exc),
                    task_summary=task_summary,
                    sql=sql,
                    submitted_at=submitted_at,
                    updated_at=now_utc_iso(),
                    completed_at=completed_at,
                    logview=logview,
                    error_message=str(exc),
                )
            failure_reason = None if succeeded else self._first_failure_reason(instance)
            diagnosis = classify_failure_reason(failure_reason)
            return JobInfo(
                job_id=instance.id,
                status="success" if succeeded else "failure",
                project=project,
                progress=100,
                stage="completed" if succeeded else "failed",
                retryable=False if succeeded else diagnosis["retryable"],
                failure_reason=failure_reason,
                task_summary=task_summary,
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
            stage="queue",
            retryable=None,
            task_summary=task_summary,
            sql=sql,
            submitted_at=submitted_at,
            updated_at=now_utc_iso(),
            completed_at=completed_at,
            logview=logview,
        )

    def _safe_sql(self, instance) -> 'str | None':
        """Safely get SQL from instance."""
        try:
            sql = instance.get_sql_query()
        except Exception:
            return None
        return sql.rstrip(";") if sql else None

    def _safe_logview(self, instance) -> 'str | None':
        """Safely get logview URL from instance."""
        try:
            return instance.get_logview_address()
        except Exception:
            return None
