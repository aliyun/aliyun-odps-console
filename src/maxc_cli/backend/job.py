"""Job-related mixin for OdpsBackend."""

from itertools import islice
from typing import Any

from ..helpers import (
    _dt_to_iso,
    _duration_ms,
    build_task_summary,
    classify_failure_reason,
    translate_odps_error,
)
from ..models import JobInfo, QueryResult
from ..utils import now_utc_iso
from .query import QueryMixin


class JobMixin(QueryMixin):
    """Mixin providing job management methods."""

    def get_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        """Get job status."""
        instance = self._get_instance(job_id, project=project)
        return self._instance_to_job_info(instance, project=project or self.project)

    def wait_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        """Wait for job completion."""
        instance = self._get_instance(job_id, project=project)
        try:
            instance.wait_for_success()
        except Exception:
            pass
        return self._instance_to_job_info(instance, project=project or self.project)

    def fetch_job_result(
        self,
        job_id: str,
        *,
        project: str | None = None,
        max_rows: int,
        offset: int = 0,
    ) -> QueryResult:
        """Fetch job results."""
        from ..exceptions import FeatureUnavailableError

        instance = self._get_instance(job_id, project=project)
        info = self._instance_to_job_info(instance, project=project or self.project)
        if info.status != "success":
            raise FeatureUnavailableError(
                f"Job {job_id} is currently {info.status}; results are not readable yet.",
                suggestion="Run `maxc job wait` or `maxc job status` first.",
            )
        sql = self._safe_sql(instance) or ""
        return self._instance_to_query_result(
            instance,
            project=project or self.project,
            max_rows=max_rows,
            sql=sql,
            elapsed_ms=_duration_ms(instance.start_time, instance.end_time),
            offset=offset,
        )

    def cancel_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        """Cancel a job."""
        instance = self._get_instance(job_id, project=project)
        try:
            instance.stop()
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        sql = self._safe_sql(instance)
        return JobInfo(
            job_id=job_id,
            status="failure",
            project=project or self.project,
            progress=0,
            stage="cancel_requested",
            retryable=False,
            failure_reason="Cancellation has been requested.",
            task_summary=build_task_summary(sql),
            sql=sql,
            submitted_at=_dt_to_iso(getattr(instance, "start_time", None)),
            updated_at=now_utc_iso(),
            logview=self._safe_logview(instance),
            warnings=["Cancellation has been requested. Run `job status` again to confirm the final state."],
        )

    def diagnose_job(self, job_id: str, *, project: str | None = None) -> dict[str, Any]:
        """Diagnose a job failure."""
        instance = self._get_instance(job_id, project=project)
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

    def list_jobs(self, *, project: str | None = None, limit: int = 20) -> list[JobInfo]:
        """List jobs."""
        jobs: list[JobInfo] = []
        try:
            iterator = self.client.list_instances(project=project or self.project)
            for instance in islice(iterator, limit):
                jobs.append(self._instance_to_job_info(instance, project=project or self.project))
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return jobs

    # Private methods for job handling

    def _get_instance(self, job_id: str, *, project: str | None = None):
        """Get ODPS instance by job ID."""
        try:
            return self.client.get_instance(job_id, project=project or self.project)
        except Exception as exc:
            raise translate_odps_error(exc) from exc

    def _safe_task_statuses(self, instance) -> dict[str, Any]:
        """Safely get task statuses from instance."""
        try:
            return dict(instance.get_task_statuses())
        except Exception:
            return {}

    def _safe_task_results(self, instance) -> dict[str, str]:
        """Safely get task results from instance."""
        try:
            results = instance.get_task_results()
        except Exception:
            return {}
        return {
            str(name): str(value)
            for name, value in dict(results).items()
        }

    def _first_failure_reason(self, instance) -> str | None:
        """Get first non-empty failure reason from task results."""
        task_results = self._safe_task_results(instance)
        for value in task_results.values():
            text = str(value).strip()
            if text:
                return text
        return None

    def _instance_to_job_info(self, instance, *, project: str) -> JobInfo:
        """Convert ODPS instance to JobInfo."""
        try:
            instance.reload(blocking=False)
        except Exception:
            pass

        status_name = str(getattr(instance, "status", "")).split(".")[-1]
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

    def _safe_sql(self, instance) -> str | None:
        """Safely get SQL from instance."""
        try:
            sql = instance.get_sql_query()
        except Exception:
            return None
        return sql.rstrip(";") if sql else None

    def _safe_logview(self, instance) -> str | None:
        """Safely get logview URL from instance."""
        try:
            return instance.get_logview_address()
        except Exception:
            return None
