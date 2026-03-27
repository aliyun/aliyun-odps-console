
from datetime import datetime, timezone
import getpass
import json
import sys
from pathlib import Path
from time import monotonic
from typing import Any, Callable

from .audit import AuditLogger
from .auth_providers import (
    build_auth_options,
    build_ncs_auth_config,
    list_ncs_accounts,
    resolve_auth_connection,
)
from .backend import OdpsBackend
from .cache import LocalCache
from .config import (
    AuthConfig,
    TableDefinition,
    default_global_config_path,
    load_config,
    load_config_mapping,
    persist_login_config,
    save_config_mapping,
)
from .exceptions import (
    CostLimitExceededError,
    ErrorPayload,
    FeatureUnavailableError,
    MaxCError,
    PermissionDeniedError,
    ValidationError,
)
from .helpers import (
    build_odps_identity_payload,
    build_task_summary,
    classify_failure_reason,
    load_odps_env,
    mask_access_id,
    missing_odps_settings,
    parse_time_value,
)
from .models import AgentHints, Envelope, JobInfo, QueryResult
from .store import JobStore
from .utils import decode_cursor, detect_operation, encode_cursor, now_utc_iso


class MaxCApp:
    def __init__(
        self,
        *,
        cwd: 'Path',
        config_path: 'Path | None' = None,
        load_backend: 'bool' = True,
    ) -> 'None':
        self.cwd = cwd
        self.config = load_config(cwd, config_path)
        self.backend = OdpsBackend(self.config) if load_backend else None
        self.remote_jobs = getattr(self.backend, "supports_remote_jobs", False) if self.backend else False
        self.jobs: 'JobStore | None' = None
        self._audit: 'AuditLogger | None' = None
        self._audit_path = self.config.agent.audit_log or self.config.state_dir / "audit.log"
        self._cache: 'LocalCache | None' = None

    @property
    def cache(self) -> 'LocalCache':
        if self._cache is None:
            self._cache = LocalCache(self.config.cache_dir)
        return self._cache

    def _ensure_job_store(self) -> 'JobStore':
        if self.remote_jobs:
            raise FeatureUnavailableError("Local job storage is not initialized for the current backend.")
        if self.jobs is None:
            self.jobs = JobStore(self.config.state_dir)
        return self.jobs

    def _whoami_validation_failed_envelope(
        self,
        *,
        settings: 'dict[str, str | None]',
        auth_type: 'str',
        identity_source: 'str',
        warnings: 'list[str]',
    ) -> 'Envelope':
        payload, base_warnings = build_odps_identity_payload(
            client=None,
            settings=settings,
            allowed_operations=self.config.allowed_operations,
            identity_source=identity_source,
            auth_type=auth_type,
            token_expires_at=settings.get("token_expires_at"),
            project=self.config.default_project,
            owner_display_name=None,
            authenticated=False,
            configured=True,
            validation_status="failed",
        )
        payload["auth_options"] = build_auth_options(default_global_config_path())
        return Envelope(
            command="auth.whoami",
            status="success",
            data=payload,
            metadata={
                "project": self.config.default_project,
                "config_sources": [str(p) for p in self.config.sources],
            },
            agent_hints=AgentHints(
                next_actions=["auth.login", "auth.login-ncs"],
                warnings=base_warnings + warnings,
            ),
        )

    def _auth_settings_from_config(self, auth: 'AuthConfig') -> 'dict[str, str | None]':
        return {
            "provider": auth.provider,
            "access_id": auth.access_id,
            "secret_access_key": auth.secret_access_key,
            "security_token": auth.security_token,
            "token_expires_at": auth.token_expires_at,
            "project": auth.project,
            "endpoint": auth.endpoint,
            "region_name": auth.region_name,
            "tunnel_endpoint": auth.tunnel_endpoint,
            "ncs_process_command": auth.ncs.process_command,
            "ncs_account_type": auth.ncs.account_type,
            "ncs_employee_id": auth.ncs.employee_id,
            "ncs_account_name": auth.ncs.account_name,
            "ncs_app_name": auth.ncs.app_name,
            "ncs_process_timeout": str(auth.ncs.process_timeout) if auth.ncs.process_timeout else None,
        }

    def _validate_auth_config_shape(self, auth: 'AuthConfig') -> 'None':
        settings = self._auth_settings_from_config(auth)
        provider = (auth.provider or "").strip().lower()
        if provider not in {"access_key", "sts_token", "ncs"}:
            provider = "ncs" if settings.get("ncs_process_command") else "sts_token" if settings.get("security_token") else "access_key"

        missing = missing_odps_settings(settings, auth_type=provider)
        if not missing:
            return

        if provider == "ncs":
            raise ValidationError(
                f"ncs authentication is missing required fields: {', '.join(missing)}.",
                suggestion="Provide project, endpoint, and ncs account configuration before using the ncs provider.",
            )
        if provider == "sts_token":
            raise ValidationError(
                f"STS authentication is missing required fields: {', '.join(missing)}.",
                suggestion="Provide access_id, secret_access_key, security_token, project, and endpoint.",
            )
        raise ValidationError(
            f"MaxCompute connection settings are incomplete: {', '.join(missing)}.",
            suggestion="Run `maxc auth login` or set the required environment variables.",
        )

    def _cache_age_seconds(self, updated_at: 'str | None') -> 'int | None':
        if not updated_at:
            return None
        parsed = parse_time_value(updated_at)
        if parsed is None:
            return None
        return max(int((datetime.now(timezone.utc) - parsed).total_seconds()), 0)

    def _cache_metadata(
        self,
        *,
        project: 'str',
        source: 'str',
        query_time_ms: 'int | None' = None,
        schema_name: 'str | None' = None,
    ) -> 'dict[str, Any]':
        cache_stats = self.cache.get_cache_stats(project, schema_name)
        cache_age_seconds = self._cache_age_seconds(cache_stats.get("newest"))
        metadata: 'dict[str, Any]' = {
            "project": project,
            "source": source,
            "cache_available": cache_stats["table_count"] > 0,
            "cache_age_seconds": cache_age_seconds,
            "cache_stale": bool(cache_age_seconds is not None and cache_age_seconds > 3600),
            "refresh_command": "cache build",
        }
        if query_time_ms is not None:
            metadata["query_time_ms"] = query_time_ms
        return metadata

    def query(
        self,
        *,
        command: 'str',
        sql: 'str',
        project: 'str | None' = None,
        max_rows: 'int' = 100,
        cursor: 'str | None' = None,
        dry_run: 'bool' = False,
        async_mode: 'bool' = False,
        cost_check: 'float | None' = None,
        idempotency_key: 'str | None' = None,
        retry_on: 'list[str] | None' = None,
        max_retries: 'int' = 0,
    ) -> 'Envelope':
        if dry_run and async_mode:
            raise ValidationError("Do not combine `--dry-run` with `--async`.")
        if max_rows <= 0:
            raise ValidationError("`--max-rows` and `--page-size` must be greater than 0.")
        if cursor and async_mode:
            raise ValidationError("Do not combine `--cursor` with `--async`.")
        if cursor and dry_run:
            raise ValidationError("Do not combine `--cursor` with `--dry-run`.")

        target_project = project or self.config.default_project
        offset, session_id = decode_cursor(cursor)

        # 如果 cursor 包含 session_id，从缓存获取 job_id，直接读取结果而不重新执行 SQL
        if session_id is not None and self.remote_jobs:
            session = self.cache.get_session(session_id)
            if session and session.get("job_id"):
                result = self.backend.fetch_job_result(
                    session["job_id"],
                    project=session.get("project") or target_project,
                    max_rows=max_rows,
                    offset=offset,
                )
                envelope = self._build_query_envelope(
                    command=command,
                    result=result,
                    dry_run=False,
                    session_id=session_id,
                )
                self.log(command, envelope.status, envelope.metadata)
                return envelope

        if async_mode and self.remote_jobs:
            job = self._submit_remote_job(
                sql=sql,
                project=target_project,
                cost_check=cost_check,
                idempotency_key=idempotency_key,
            )
            envelope = Envelope(
                command=command,
                status=job.status,
                data={"job_id": job.job_id},
                metadata={
                    "job_id": job.job_id,
                    "project": job.project,
                    "submitted_at": job.submitted_at,
                    "logview": job.logview,
                    "sql_executed": sql,
                    "idempotency_key": idempotency_key,
                },
                agent_hints=AgentHints(
                    next_actions=["job.status", "job.wait"],
                    warnings=job.warnings,
                ),
            )
            self.log(command, envelope.status, envelope.metadata)
            return envelope

        result = self._execute_query(
            sql=sql,
            project=target_project,
            max_rows=max_rows,
            offset=offset,
            dry_run=dry_run,
            cost_check=cost_check,
            retry_on=retry_on or [],
            max_retries=max_retries,
            strict_cost_check=True,
        )

        if async_mode:
            jobs = self._ensure_job_store()
            job = jobs.create_job(
                sql=sql,
                project=result.project,
                result=self._query_result_payload(result),
                idempotency_key=idempotency_key,
            )
            envelope = Envelope(
                command=command,
                status="pending",
                data={"job_id": job["job_id"]},
                metadata={
                    "job_id": job["job_id"],
                    "project": result.project,
                    "elapsed_ms": result.elapsed_ms,
                    "bytes_scanned": result.bytes_scanned,
                    "sql_executed": sql,
                    "idempotency_key": idempotency_key,
                },
                agent_hints=AgentHints(
                    next_actions=["job.status", "job.wait"],
                    warnings=result.warnings,
                ),
            )
        else:
            envelope = self._build_query_envelope(
                command=command,
                result=result,
                dry_run=dry_run,
            )
            if idempotency_key:
                envelope.metadata["idempotency_key"] = idempotency_key

        self.log(command, envelope.status, envelope.metadata)
        return envelope

    def query_cost(
        self,
        *,
        sql: 'str',
        project: 'str | None' = None,
        command: 'str' = "query.cost",
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        analysis = self._analyze_query(
            sql=sql,
            project=target_project,
            explain=False,
        )
        envelope = self._build_analysis_envelope(
            command=command,
            sql=sql,
            analysis=analysis,
        )
        self.log(command, envelope.status, envelope.metadata)
        return envelope

    def query_explain(
        self,
        *,
        sql: 'str',
        project: 'str | None' = None,
        command: 'str' = "query.explain",
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        analysis = self._analyze_query(
            sql=sql,
            project=target_project,
            explain=True,
        )
        envelope = self._build_analysis_envelope(
            command=command,
            sql=sql,
            analysis=analysis,
        )
        self.log(command, envelope.status, envelope.metadata)
        return envelope

    def submit_job(
        self,
        *,
        sql: 'str',
        project: 'str | None' = None,
        max_rows: 'int' = 100,
        cost_check: 'float | None' = None,
        idempotency_key: 'str | None' = None,
    ) -> 'Envelope':
        return self.query(
            command="job.submit",
            sql=sql,
            project=project,
            max_rows=max_rows,
            async_mode=True,
            cost_check=cost_check,
            idempotency_key=idempotency_key,
        )

    def job_status(self, job_id: 'str') -> 'Envelope':
        if self.remote_jobs:
            info = self.backend.get_job(job_id, project=self.config.default_project)
            envelope = self._job_info_envelope("job.status", info)
            self.log("job.status", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        info = self._local_job_info(job)
        envelope = self._job_info_envelope("job.status", info)
        self.log("job.status", envelope.status, envelope.metadata)
        return envelope

    def job_wait(self, job_id: 'str', *, timeout: 'int | None' = None) -> 'tuple[Envelope, list[dict[str, Any]]]':
        # TODO：目前等待作业结束，是直接静默的 wait 知道 Success，我希望是每若干秒（3s？）打印一条作业状态的 ND JSON
        if self.remote_jobs:
            before = self.backend.get_job(job_id, project=self.config.default_project)
            after = self.backend.wait_job(job_id, project=self.config.default_project, timeout=timeout)
            if after.status != "success":
                envelope = self._job_info_envelope("job.wait", after)
                events = [
                    {"type": "started", "ts": before.submitted_at or now_utc_iso(), "job_id": before.job_id},
                    {
                        "type": "failed",
                        "ts": after.completed_at or now_utc_iso(),
                        "job_id": after.job_id,
                        "reason": after.failure_reason,
                        "retryable": after.retryable,
                    },
                ]
                self.log("job.wait", envelope.status, envelope.metadata)
                return envelope, events
            result = self.backend.fetch_job_result(
                job_id,
                project=self.config.default_project,
                max_rows=100,
            )
            envelope = self._build_query_envelope(
                command="job.wait",
                result=result,
                dry_run=False,
            )
            envelope.metadata.update(
                {
                    "job_id": job_id,
                    "submitted_at": after.submitted_at,
                    "completed_at": after.completed_at,
                    "logview": after.logview,
                    "stage": after.stage,
                    "retryable": after.retryable,
                    "failure_reason": after.failure_reason,
                    "task_summary": after.task_summary,
                }
            )
            events = self._remote_job_events(before, after, result)
            self.log("job.wait", envelope.status, envelope.metadata)
            return envelope, events

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        events = self._job_events(job)
        final_job = jobs.update_job(
            job_id,
            status="success",
            progress=100,
            started_at=job.get("started_at") or now_utc_iso(),
            completed_at=now_utc_iso(),
        )
        stored = final_job["result"]
        info = self._local_job_info(final_job)
        envelope = Envelope(
            command="job.wait",
            status="success",
            data=stored["data"],
            metadata={
                **stored["metadata"],
                "job_id": job_id,
                "submitted_at": final_job["submitted_at"],
                "completed_at": final_job["completed_at"],
                "stage": info.stage,
                "retryable": info.retryable,
                "failure_reason": info.failure_reason,
                "logview": info.logview,
                "task_summary": info.task_summary,
            },
            agent_hints=AgentHints(
                next_actions=["job.result", "meta.describe"],
                warnings=stored.get("agent_hints", {}).get("warnings", []),
            ),
        )
        self.log("job.wait", envelope.status, envelope.metadata)
        return envelope, events

    def job_result(self, job_id: 'str') -> 'Envelope':
        if self.remote_jobs:
            info = self.backend.get_job(job_id, project=self.config.default_project)
            if info.status != "success":
                envelope = self._job_info_envelope("job.result", info)
                self.log("job.result", envelope.status, envelope.metadata)
                return envelope
            result = self.backend.fetch_job_result(
                job_id,
                project=self.config.default_project,
                max_rows=100,
            )
            envelope = self._build_query_envelope(
                command="job.result",
                result=result,
                dry_run=False,
            )
            envelope.metadata.update(
                {
                    "job_id": job_id,
                    "submitted_at": info.submitted_at,
                    "completed_at": info.completed_at,
                    "logview": info.logview,
                }
            )
            self.log("job.result", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        if job["status"] != "success":
            info = self._local_job_info(job)
            envelope = self._job_info_envelope("job.result", info)
            self.log("job.result", envelope.status, envelope.metadata)
            return envelope

        stored = job["result"]
        info = self._local_job_info(job)
        envelope = Envelope(
            command="job.result",
            status="success",
            data=stored["data"],
            metadata={
                **stored["metadata"],
                "job_id": job_id,
                "submitted_at": job["submitted_at"],
                "completed_at": job.get("completed_at", job["updated_at"]),
                "stage": info.stage,
                "retryable": info.retryable,
                "failure_reason": info.failure_reason,
                "logview": info.logview,
                "task_summary": info.task_summary,
            },
            agent_hints=AgentHints(
                next_actions=["meta.describe"],
                warnings=stored.get("agent_hints", {}).get("warnings", []),
            ),
        )
        self.log("job.result", envelope.status, envelope.metadata)
        return envelope

    def cancel_job(self, job_id: 'str') -> 'Envelope':
        if self.remote_jobs:
            info = self.backend.cancel_job(job_id, project=self.config.default_project)
            envelope = Envelope(
                command="job.cancel",
                status=info.status,
                data={"job_id": job_id, "cancelled": True},
                metadata={
                    "project": info.project,
                    "updated_at": info.updated_at,
                    "logview": info.logview,
                },
                agent_hints=AgentHints(
                    next_actions=["job.status"],
                    warnings=info.warnings,
                ),
            )
            self.log("job.cancel", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        if job["status"] == "success":
            raise ValidationError("The job is already complete and cannot be cancelled.")
        updated = jobs.update_job(job_id, status="failure", progress=0, cancelled=True)
        envelope = Envelope(
            command="job.cancel",
            status="failure",
            data={"job_id": job_id, "cancelled": True},
            metadata={"project": updated["project"], "updated_at": updated["updated_at"]},
            agent_hints=AgentHints(next_actions=["job.submit"]),
        )
        self.log("job.cancel", envelope.status, envelope.metadata)
        return envelope

    def job_diagnose(self, job_id: 'str') -> 'Envelope':
        if self.remote_jobs:
            payload = self.backend.diagnose_job(job_id, project=self.config.default_project)
            envelope = Envelope(
                command="job.diagnose",
                status="success",
                data=payload,
                metadata={"project": self.config.default_project},
                agent_hints=AgentHints(next_actions=["job.status", "job.result"]),
            )
            self.log("job.diagnose", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        info = self._local_job_info(job)
        diagnosis = classify_failure_reason(info.failure_reason)
        envelope = Envelope(
            command="job.diagnose",
            status="success",
            data={
                "job_id": info.job_id,
                "status": info.status,
                "stage": info.stage,
                "retryable": info.retryable,
                "failure_reason": info.failure_reason,
                "diagnosis_category": diagnosis["category"],
                "diagnosis_summary": diagnosis["summary"],
                "logview": info.logview,
                "task_summary": info.task_summary,
                "task_statuses": [],
                "task_results": {},
            },
            metadata={"project": info.project},
            agent_hints=AgentHints(next_actions=["job.status", "job.result"]),
        )
        self.log("job.diagnose", envelope.status, envelope.metadata)
        return envelope

    def list_jobs(self) -> 'Envelope':
        if self.remote_jobs:
            jobs = self.backend.list_jobs(project=self.config.default_project, limit=20)
            rows = [
                {
                    "job_id": item.job_id,
                    "status": item.status,
                    "progress": item.progress,
                    "project": item.project,
                    "submitted_at": item.submitted_at,
                }
                for item in jobs
            ]
            envelope = Envelope(
                command="job.list",
                status="success",
                data={"jobs": rows, "total": len(rows)},
                metadata={"backend": "odps", "project": self.config.default_project},
                agent_hints=AgentHints(next_actions=["job.status", "job.wait"]),
            )
            self.log("job.list", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        stored_jobs = jobs.list_jobs()
        rows = [
            {
                "job_id": item["job_id"],
                "status": item["status"],
                "progress": item["progress"],
                "project": item["project"],
                "submitted_at": item["submitted_at"],
            }
            for item in stored_jobs
        ]
        envelope = Envelope(
            command="job.list",
            status="success",
            data={"jobs": rows, "total": len(rows)},
            metadata={"state_file": str(jobs.path)},
            agent_hints=AgentHints(next_actions=["job.status", "job.wait"]),
        )
        self.log("job.list", envelope.status, envelope.metadata)
        return envelope

    def meta_list_tables(self) -> 'Envelope':
        started = monotonic()
        
        # Try to get from cache first
        cached_tables = self.cache.get_all_cached_tables(self.config.default_project)
        
        if cached_tables:
            # Use cached data (returns list of dicts)
            tables = cached_tables
            source = "cache"
            rows = [
                {
                    "table_name": table.get("table_name"),
                    "table_type": table.get("table_type", "TABLE"),
                    "size_bytes": table.get("size_bytes"),
                    "owner": table.get("owner"),
                    "description": table.get("description"),
                    "partition_columns": [
                        c.get("name") if isinstance(c, dict) else str(c)
                        for c in table.get("partition_columns", [])
                    ],
                }
                for table in tables
            ]
        else:
            # No cache - return guidance to build cache first
            return Envelope(
                command="meta.list-tables",
                status="cache_miss",
                data={"tables": [], "total": 0},
                metadata=self._cache_metadata(
                    project=self.config.default_project,
                    source="none",
                    query_time_ms=int((monotonic() - started) * 1000),
                ),
                agent_hints=AgentHints(
                    next_actions=["cache.build"],
                    insights=["No metadata cache found for this project. Building the cache first will significantly improve performance."],
                    warnings=["Cache miss: Run `maxc cache build` to populate the metadata cache."],
                ),
            )
        
        metadata = self._cache_metadata(
            project=self.config.default_project,
            source=source,
            query_time_ms=int((monotonic() - started) * 1000),
        )
        
        envelope = Envelope(
            command="meta.list-tables",
            status="success",
            data={"tables": rows, "total": len(rows)},
            metadata=metadata,
            agent_hints=AgentHints(
                next_actions=["meta.describe", "data.sample"],
                insights=[f"Table list served from {source}."],
            ),
        )
        self.log("meta.list-tables", envelope.status, envelope.metadata)
        return envelope

    def meta_describe(self, table_name: 'str', full: 'bool' = False) -> 'Envelope':
        started = monotonic()

        # Try to get from cache first
        cached_table = self.cache.get_cached_table(
            self.config.default_project,
            table_name,
            schema_name=self.config.default_schema or "default"
        )

        if cached_table:
            # Use cached metadata for schema, fetch sample rows from API
            from .config import TableDefinition, TableColumn

            # Build TableDefinition from cache
            columns = [
                TableColumn(name=c["name"], type=c["type"], comment=c.get("comment", ""))
                for c in cached_table.get("columns", [])
            ]
            partition_columns = [
                TableColumn(name=p, type="string", comment="")
                for p in cached_table.get("partitions", [])
            ]

            table = TableDefinition(
                name=table_name,
                description=cached_table.get("description", ""),
                columns=columns,
                sample_rows=[],  # Will fetch from API if needed
                partitions=[],  # Will fetch from API if needed
                partition_columns=partition_columns,
                owner=cached_table.get("owner"),
                size_bytes=cached_table.get("size_bytes"),
                table_type="TABLE",
            )
            source = "cache"

            # Optionally fetch additional metadata from API (description, owner, size, sample rows, partitions)
            try:
                api_table = self.backend.describe_table(table_name)
                # Update with API data (API has priority over cache for these fields)
                table.description = api_table.description or table.description
                table.owner = api_table.owner or table.owner
                table.size_bytes = api_table.size_bytes or table.size_bytes
                table.created_at = api_table.created_at
                table.updated_at = api_table.updated_at
                table.table_type = api_table.table_type or table.table_type
                table.sample_rows = api_table.sample_rows
                table.partitions = api_table.partitions
            except Exception:
                # If API fails, still return cached schema
                pass
        else:
            # Fall back to live API
            table = self.backend.describe_table(table_name)
            source = "live"

        warnings = []

        # Get semantic metadata from cache
        semantic = self.cache.get_semantic(
            project=self.config.default_project,
            table_name=table_name,
            schema_name=self.config.default_schema or "default",
        )

        if not semantic:
            warnings.append(
                "Missing semantic metadata. Agent should generate it using its own LLM and save with: maxc meta semantic set"
            )

        payload = self._table_payload(table, full=full)
        
        # Add hint about --full flag in summary mode
        if not full and payload.get("has_more_columns"):
            remaining = payload.get("remaining_columns", 0)
            warnings.append(
                f"Showing first 10 columns only. Use --full to see all {payload['column_count']} columns."
            )
        
        # Add semantic information to the payload
        payload["semantic"] = semantic

        envelope = Envelope(
            command="meta.describe",
            status="success",
            data=payload,
            metadata={
                "project": self.config.default_project,
                "source": source,
                "query_time_ms": int((monotonic() - started) * 1000) if source == "live" else None,
            },
            agent_hints=AgentHints(
                next_actions=["data.sample", "data.profile", "query"],
                warnings=warnings,
            ),
        )
        self.log("meta.describe", envelope.status, envelope.metadata)
        return envelope

    def meta_search(self, keyword: 'str') -> 'Envelope':
        started = monotonic()
        cached_tables = self.cache.get_all_cached_tables(self.config.default_project)
        if cached_tables:
            matches = self._search_in_cache(keyword, cached_tables)
            source = "cache"
        else:
            matches = self.backend.search_tables(keyword)
            source = "live"
        envelope = Envelope(
            command="meta.search",
            status="success",
            data={"keyword": keyword, "matches": matches, "total": len(matches)},
            metadata=self._cache_metadata(
                project=self.config.default_project,
                source=source,
                query_time_ms=int((monotonic() - started) * 1000) if source == "live" else None,
            ),
            agent_hints=AgentHints(
                next_actions=["meta.describe", "data.sample"],
                warnings=[] if cached_tables else ["No metadata cache was used. Run `maxc cache build` to speed up future lookups."],
            ),
        )
        self.log("meta.search", envelope.status, envelope.metadata)
        return envelope

    def meta_search_columns(self, keyword: 'str') -> 'Envelope':
        started = monotonic()
        cached_tables = self.cache.get_all_cached_tables(self.config.default_project)
        if cached_tables:
            matches = self._search_columns_in_cache(keyword, cached_tables)
            source = "cache"
        else:
            matches = self.backend.search_columns(keyword)
            source = "live"
        envelope = Envelope(
            command="meta.search-columns",
            status="success",
            data={"keyword": keyword, "matches": matches, "total": len(matches)},
            metadata=self._cache_metadata(
                project=self.config.default_project,
                source=source,
                query_time_ms=int((monotonic() - started) * 1000) if source == "live" else None,
            ),
            agent_hints=AgentHints(
                next_actions=["meta.describe", "query"],
                warnings=[] if cached_tables else ["No metadata cache was used. Run `maxc cache build` to speed up future lookups."],
            ),
        )
        self.log("meta.search-columns", envelope.status, envelope.metadata)
        return envelope

    def _search_in_cache(
        self, keyword: 'str', cached_tables: 'list[dict]'
    ) -> 'list[dict]':
        """Search tables in cache."""
        tokens = [t.lower() for t in keyword.split() if t.strip()] or [keyword.lower()]
        matches = []
        for table in cached_tables:
            score = 0
            matched_columns = []
            searchable = f"{table['table_name']} {table.get('description', '')}".lower()
            for token in tokens:
                if token in searchable:
                    score += 5
                for col in table.get("columns", []):
                    text = f"{col.get('name', '')} {col.get('comment', '')}".lower()
                    if token in text:
                        score += 2
                        matched_columns.append(col["name"])
            if score:
                matches.append({
                    "table_name": table["table_name"],
                    "description": table.get("description"),
                    "score": score,
                    "matched_columns": list(set(matched_columns))[:5],
                })
        return sorted(matches, key=lambda x: -x["score"])[:20]

    def _search_columns_in_cache(
        self, keyword: 'str', cached_tables: 'list[dict]'
    ) -> 'list[dict]':
        """Search columns in cache."""
        tokens = [t.lower() for t in keyword.split() if t.strip()] or [keyword.lower()]
        matches = []
        for table in cached_tables:
            for col in table.get("columns", []):
                text = f"{col.get('name', '')} {col.get('comment', '')}".lower()
                score = sum(2 for token in tokens if token in text)
                if score:
                    matches.append({
                        "table_name": table["table_name"],
                        "column_name": col["name"],
                        "column_type": col.get("type"),
                        "column_comment": col.get("comment"),
                        "score": score,
                    })
        return sorted(matches, key=lambda x: -x["score"])[:50]

    # ========== Semantic Metadata Methods ==========

    def semantic_set(
        self,
        table_name: 'str',
        semantic_desc: 'str | None' = None,
        use_cases: 'list[str] | None' = None,
        sample_questions: 'list[str] | None' = None,
        column_semantics: 'list[dict[str, Any]] | None' = None,
        relations: 'list[dict[str, Any]] | None' = None,
        stats: 'dict[str, Any] | None' = None,
    ) -> 'Envelope':
        """Set semantic metadata for a table (data provided by Agent)."""
        try:
            self.cache.save_semantic(
                project=self.config.default_project,
                table_name=table_name,
                semantic_desc=semantic_desc or "",
                use_cases=use_cases or [],
                sample_questions=sample_questions or [],
                column_semantics=column_semantics or [],
                schema_name=self.config.default_schema or "default",
                relations=relations,
                stats=stats,
            )

            envelope = Envelope(
                command="meta.semantic.set",
                status="success",
                data={
                    "action": "set_semantic",
                    "table_name": table_name,
                    "has_description": bool(semantic_desc),
                    "use_cases_count": len(use_cases) if use_cases else 0,
                    "sample_questions_count": len(sample_questions) if sample_questions else 0,
                    "column_semantics_count": len(column_semantics) if column_semantics else 0,
                },
                metadata={
                    "project": self.config.default_project,
                    "schema": self.config.default_schema or "default",
                },
                agent_hints=AgentHints(
                    next_actions=[f"meta.describe {table_name} --json"],
                    insights=["Semantic metadata has been saved to local cache."],
                ),
            )
        except Exception as exc:
            envelope = Envelope(
                command="meta.semantic.set",
                status="failure",
                data=None,
                metadata={
                    "project": self.config.default_project,
                },
                error=ErrorPayload(
                    code="SEMANTIC_SET_ERROR",
                    message=str(exc),
                    recoverable=False,
                    suggestion="Check the error message and try again.",
                ),
            )

        self.log("meta.semantic.set", envelope.status, envelope.metadata)
        return envelope

    def semantic_get(
        self,
        table_name: 'str',
    ) -> 'Envelope':
        """Get semantic metadata for a table."""
        try:
            semantic = self.cache.get_semantic(
                project=self.config.default_project,
                table_name=table_name,
                schema_name=self.config.default_schema or "default",
            )

            if semantic:
                envelope = Envelope(
                    command="meta.semantic.get",
                    status="success",
                    data={
                        "table_name": table_name,
                        "semantic": semantic,
                    },
                    metadata={
                        "project": self.config.default_project,
                        "schema": self.config.default_schema or "default",
                    },
                )
            else:
                envelope = Envelope(
                    command="meta.semantic.get",
                    status="success",
                    data={
                        "table_name": table_name,
                        "semantic": None,
                    },
                    metadata={
                        "project": self.config.default_project,
                        "schema": self.config.default_schema or "default",
                    },
                    agent_hints=AgentHints(
                        warnings=["No semantic metadata found. Use `maxc meta semantic set` to add metadata."],
                        next_actions=[f"meta semantic set {table_name}"],
                    ),
                )
        except Exception as exc:
            envelope = Envelope(
                command="meta.semantic.get",
                status="failure",
                data=None,
                metadata={
                    "project": self.config.default_project,
                },
                error=ErrorPayload(
                    code="SEMANTIC_GET_ERROR",
                    message=str(exc),
                    recoverable=False,
                    suggestion="Check the error message and try again.",
                ),
            )

        self.log("meta.semantic.get", envelope.status, envelope.metadata)
        return envelope

    def semantic_list_missing(
        self,
    ) -> 'Envelope':
        """List tables without semantic metadata."""
        try:
            # Get all cached tables
            all_tables = self.cache.get_all_cached_tables(
                project=self.config.default_project
            )

            # Get tables with semantic metadata
            semantic_tables = self.cache.get_all_semantics(
                project=self.config.default_project
            )
            semantic_table_names = {t["table_name"] for t in semantic_tables}

            # Find tables missing semantic metadata
            missing = [
                t for t in all_tables
                if t["table_name"] not in semantic_table_names
            ]

            envelope = Envelope(
                command="meta.semantic.list-missing",
                status="success",
                data={
                    "total_cached_tables": len(all_tables),
                    "with_semantic": len(semantic_tables),
                    "missing_semantic": len(missing),
                    "tables": [
                        {
                            "table_name": t["table_name"],
                            "schema_name": t["schema_name"],
                            "description": t.get("description", ""),
                            "column_count": len(t.get("columns", [])),
                        }
                        for t in missing[:50]  # Limit to 50 tables
                    ],
                },
                metadata={
                    "project": self.config.default_project,
                },
                agent_hints=AgentHints(
                    insights=[
                        f"{len(missing)} tables lack semantic metadata.",
                        "Run `maxc meta semantic set <table>` to add metadata for each table."
                    ],
                    next_actions=[
                        f"meta semantic set {missing[0]['table_name']}"
                    ] if missing else [],
                ),
            )
        except Exception as exc:
            envelope = Envelope(
                command="meta.semantic.list-missing",
                status="failure",
                data=None,
                metadata={
                    "project": self.config.default_project,
                },
                error=ErrorPayload(
                    code="SEMANTIC_LIST_MISSING_ERROR",
                    message=str(exc),
                    recoverable=False,
                    suggestion="Check the error message and try again.",
                ),
            )

        self.log("meta.semantic.list-missing", envelope.status, envelope.metadata)
        return envelope

    def meta_latest_partition(self, table_name: 'str') -> 'Envelope':
        payload, warnings = self.backend.latest_partition_info(table_name)
        next_actions = ["meta.freshness", "data.sample", "query"]
        if not payload.get("has_partitions"):
            next_actions = ["meta.describe", "data.sample"]
        envelope = Envelope(
            command="meta.latest-partition",
            status="success",
            data=payload,
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=next_actions, warnings=warnings),
        )
        self.log("meta.latest-partition", envelope.status, envelope.metadata)
        return envelope

    def meta_freshness(self, table_name: 'str') -> 'Envelope':
        payload, warnings = self.backend.freshness_info(table_name)
        next_actions = ["meta.latest-partition", "data.sample", "query"]
        if payload.get("freshness_status") == "stale":
            next_actions.insert(0, "job.submit")
        envelope = Envelope(
            command="meta.freshness",
            status="success",
            data=payload,
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=next_actions, warnings=warnings),
        )
        self.log("meta.freshness", envelope.status, envelope.metadata)
        return envelope

    def meta_lineage(self, table_name: 'str') -> 'Envelope':
        payload, warnings = self.backend.lineage_info(table_name)
        envelope = Envelope(
            command="meta.lineage",
            status="success",
            data=payload,
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["meta.describe"], warnings=warnings),
        )
        self.log("meta.lineage", envelope.status, envelope.metadata)
        return envelope

    def cache_build(
        self,
        *,
        project: 'str | None' = None,
        max_workers: 'int' = 8,
        schema_name: 'str | None' = None,
        async_mode: 'bool' = False,
        progress_callback: 'Callable[[dict[str, Any]], None] | None' = None,
    ) -> 'Envelope':
        """Build metadata cache for all tables in the project.

        Args:
            project: Project name
            max_workers: Number of concurrent workers
            schema_name: Specific schema to build (None = all schemas)
            async_mode: If True, return immediately with build_id for async tracking
            progress_callback: Optional callback for build progress events
        """
        import uuid

        target_project = project or self.config.default_project
        if progress_callback is not None:
            progress_callback(
                {
                    "type": "listing_start",
                    "project": target_project,
                    "schema_name": schema_name,
                }
            )

        all_tables = self.backend.list_tables()
        if schema_name:
            tables = all_tables
        else:
            tables = all_tables

        if progress_callback is not None:
            progress_callback(
                {
                    "type": "listing_complete",
                    "project": target_project,
                    "schema_name": schema_name,
                    "total_tables": len(tables),
                }
            )

        build_id = str(uuid.uuid4())[:8]

        if async_mode:
            self.cache.start_build(target_project, build_id, len(tables))
            envelope = Envelope(
                command="cache.build",
                status="running",
                data={
                    "action": "build",
                    "build_id": build_id,
                    "mode": "async",
                    "scope": "schema" if schema_name else "project",
                    "schema_name": schema_name,
                    "tables_scanned": len(tables),
                    "total_tables": len(tables),
                    "cache_location": str(self.cache.db_path),
                    "message": "Cache build started. Use `cache build-status` to track progress.",
                },
                metadata={"project": target_project, "build_id": build_id},
                agent_hints=AgentHints(
                    next_actions=["cache.build-status"],
                    insights=["The metadata cache build is running in the background."],
                ),
            )
            import threading
            thread = threading.Thread(
                target=self._build_cache_background,
                args=(target_project, build_id, tables, max_workers, schema_name, False),
                daemon=True,
            )
            thread.start()
            self.log("cache.build", "running", envelope.metadata)
            return envelope

        return self._build_cache_sync(
            target_project, build_id, tables, max_workers, schema_name, progress_callback=progress_callback
        )

    def _build_cache_sync(
        self,
        project: 'str',
        build_id: 'str',
        tables: 'list',
        max_workers: 'int',
        schema_name: 'str | None' = None,
        initialize_status: 'bool' = True,
        progress_callback: 'Callable[[dict[str, Any]], None] | None' = None,
    ) -> 'Envelope':
        """Synchronous cache build with progress tracking."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        started = monotonic()
        cached_count = 0
        created_count = 0
        updated_count = 0
        errors: 'list[str]' = []
        lock = threading.Lock()

        if initialize_status:
            self.cache.start_build(project, build_id, len(tables))
        if progress_callback is not None:
            progress_callback(
                {
                    "type": "build_start",
                    "project": project,
                    "schema_name": schema_name,
                    "build_id": build_id,
                    "total_tables": len(tables),
                }
            )

        def fetch_and_cache(
            table_name: 'str',
            table_schema: 'str' = "default",
        ) -> 'tuple[str, str | None]':
            try:
                full_table = self.backend.describe_table(table_name)
                existing = self.cache.get_cached_table(project, full_table.name, table_schema)
                columns = [
                    {"name": c.name, "type": c.type, "comment": c.comment}
                    for c in full_table.columns
                ]
                self.cache.cache_table(
                    project=project,
                    table_name=full_table.name,
                    description=full_table.description,
                    columns=columns,
                    partitions=full_table.partitions,
                    schema_name=table_schema,
                )
                return ("updated" if existing else "created"), None
            except Exception as exc:
                return "error", f"{table_name}: {exc}"

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_and_cache, t.name): t.name for t in tables}
            for future in as_completed(futures):
                outcome, error = future.result()
                with lock:
                    if error:
                        errors.append(error)
                    else:
                        cached_count += 1
                        if outcome == "updated":
                            updated_count += 1
                        else:
                            created_count += 1
                    self.cache.update_build_progress(
                        project, build_id, cached_count, len(errors)
                    )
                    if progress_callback is not None:
                        progress_callback(
                            {
                                "type": "progress",
                                "project": project,
                                "schema_name": schema_name,
                                "build_id": build_id,
                                "cached_tables": cached_count,
                                "failed_tables": len(errors),
                                "total_tables": len(tables),
                            }
                        )

        if errors:
            self.cache.complete_build(project, build_id, error_message=f"{len(errors)} errors")
        else:
            self.cache.complete_build(project, build_id)

        stats = self.cache.get_cache_stats(project)
        elapsed_ms = int((monotonic() - started) * 1000)
        envelope = Envelope(
            command="cache.build",
            status="success" if not errors else "partial",
            data={
                "action": "build",
                "build_id": build_id,
                "mode": "sync",
                "scope": "schema" if schema_name else "project",
                "schema_name": schema_name,
                "tables_scanned": len(tables),
                "cache_entries_created": created_count,
                "cache_entries_updated": updated_count,
                "cached_tables": cached_count,
                "total_tables": len(tables),
                "tables_failed": len(errors),
                "elapsed_ms": elapsed_ms,
                "cache_location": str(self.cache.db_path),
                "errors": errors[:10] if errors else [],
                "stats": stats,
            },
            metadata={"project": project, "build_id": build_id},
            agent_hints=AgentHints(
                next_actions=["meta.search", "meta.search-columns"],
                warnings=[f"Failed to cache {len(errors)} table(s)."] if errors else [],
            ),
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "type": "completed",
                    "project": project,
                    "schema_name": schema_name,
                    "build_id": build_id,
                    "cached_tables": cached_count,
                    "failed_tables": len(errors),
                    "total_tables": len(tables),
                    "status": envelope.status,
                }
            )
        self.log("cache.build", envelope.status, envelope.metadata)
        return envelope

    def _build_cache_background(
        self,
        project: 'str',
        build_id: 'str',
        tables: 'list',
        max_workers: 'int',
        schema_name: 'str | None' = None,
        initialize_status: 'bool' = False,
    ) -> 'None':
        """Background cache build (async mode)."""
        try:
            self._build_cache_sync(
                project,
                build_id,
                tables,
                max_workers,
                schema_name,
                initialize_status=initialize_status,
            )
        except Exception as exc:
            self.cache.complete_build(project, build_id, error_message=str(exc))

    def cache_build_status(
        self, *, project: 'str | None' = None, build_id: 'str | None' = None
    ) -> 'Envelope':
        """Get cache build status."""
        target_project = project or self.config.default_project
        status = self.cache.get_build_status(target_project, build_id)

        if status:
            envelope = Envelope(
                command="cache.build-status",
                status="success",
                data=status,
                metadata={"project": target_project},
                agent_hints=AgentHints(
                    next_actions=(
                        ["cache.build"]
                        if status["status"] in ["failed", "completed"]
                        else ["cache.build-status"]
                    ),
                    insights=[
                        f"Build progress: {status['progress_percent']}% ({status['processed_tables']}/{status['total_tables']})"
                    ]
                    if status["status"] == "running"
                    else [],
                ),
            )
        else:
            envelope = Envelope(
                command="cache.build-status",
                status="not_found",
                data={"message": "No cache build record was found."},
                metadata={"project": target_project},
                agent_hints=AgentHints(next_actions=["cache.build"]),
            )
        return envelope

    def cache_status(self, *, project: 'str | None' = None, schema_name: 'str | None' = None) -> 'Envelope':
        """Get cache status."""
        target_project = project or self.config.default_project
        stats = self.cache.get_cache_stats(target_project, schema_name)
        schemas = self.cache.get_schemas(target_project)

        envelope = Envelope(
            command="cache.status",
            status="success",
            data={
                **stats,
                "schemas": schemas,
            },
            metadata={"project": target_project},
            agent_hints=AgentHints(
                next_actions=["cache.build"] if stats["table_count"] == 0 else ["meta.search"],
            ),
        )
        return envelope

    def cache_clear(self, *, project: 'str | None' = None, schema_name: 'str | None' = None) -> 'Envelope':
        """Clear metadata cache."""
        target_project = project or self.config.default_project
        deleted = self.cache.clear_table_cache(target_project, schema_name)
        envelope = Envelope(
            command="cache.clear",
            status="success",
            data={"deleted_tables": deleted},
            metadata={"project": target_project},
            agent_hints=AgentHints(next_actions=["cache.build"]),
        )
        return envelope

    def cache_save_semantic(
        self,
        *,
        table_name: 'str',
        semantic_desc: 'str',
        use_cases: 'list[str]',
        sample_questions: 'list[str]',
        column_semantics: 'list[dict]',
        project: 'str | None' = None,
        schema_name: 'str' = "default",
    ) -> 'Envelope':
        """Save AI-generated semantic metadata for NL2SQL."""
        target_project = project or self.config.default_project
        self.cache.save_semantic(
            project=target_project,
            table_name=table_name,
            semantic_desc=semantic_desc,
            use_cases=use_cases,
            sample_questions=sample_questions,
            column_semantics=column_semantics,
            schema_name=schema_name,
        )
        envelope = Envelope(
            command="cache.save-semantic",
            status="success",
            data={
                "table_name": table_name,
                "schema_name": schema_name,
                "semantic_desc": semantic_desc,
                "use_cases_count": len(use_cases),
                "sample_questions_count": len(sample_questions),
                "column_semantics_count": len(column_semantics),
            },
            metadata={"project": target_project},
            agent_hints=AgentHints(
                next_actions=["meta.search", "cache.get-semantic"],
                insights=["Semantic metadata has been saved and can now support NL2SQL workflows."],
            ),
        )
        return envelope

    def cache_get_semantic(
        self, *, table_name: 'str', project: 'str | None' = None, schema_name: 'str' = "default"
    ) -> 'Envelope':
        """Get semantic metadata for a table."""
        target_project = project or self.config.default_project
        semantic = self.cache.get_semantic(target_project, table_name, schema_name)
        if semantic:
            envelope = Envelope(
                command="cache.get-semantic",
                status="success",
                data={"table_name": table_name, "schema_name": schema_name, **semantic},
                metadata={"project": target_project},
                agent_hints=AgentHints(next_actions=["query"]),
            )
        else:
            envelope = Envelope(
                command="cache.get-semantic",
                status="not_found",
                data={"table_name": table_name, "schema_name": schema_name},
                metadata={"project": target_project},
                agent_hints=AgentHints(
                    next_actions=["cache.save-semantic"],
                    warnings=["No semantic metadata was found. Run the semantic-index skill first."],
                ),
            )
        return envelope

    def meta_partitions(self, table_name: 'str') -> 'Envelope':
        table = self.backend.describe_table(table_name)
        envelope = Envelope(
            command="meta.partitions",
            status="success",
            data={"table_name": table.name, "partitions": table.partitions},
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["query"]),
        )
        self.log("meta.partitions", envelope.status, envelope.metadata)
        return envelope

    def meta_list_projects(self) -> 'Envelope':
        """List all projects owned by the current user."""
        projects = self.backend.list_projects()
        envelope = Envelope(
            command="meta.list-projects",
            status="success",
            data={"projects": projects, "total": len(projects)},
            metadata={"backend": "odps"},
            agent_hints=AgentHints(
                next_actions=["session.set", "meta.list-schemas"],
                insights=[
                    f"Discovered {len(projects)} project(s) owned by the current identity.",
                    "Choose a project, then run `maxc session set --project <project_name> --json` before listing schemas or tables.",
                ],
            ),
        )
        self.log("meta.list-projects", envelope.status, envelope.metadata)
        return envelope

    def meta_list_schemas(self, *, project: 'str | None' = None) -> 'Envelope':
        """List all schemas in a project."""
        target_project = project or self.config.default_project
        schemas = self.backend.list_schemas(project=target_project)
        rows = [{"name": s["name"]} for s in schemas]
        envelope = Envelope(
            command="meta.list-schemas",
            status="success",
            data={"schemas": rows, "total": len(rows)},
            metadata={"project": target_project},
            agent_hints=AgentHints(
                next_actions=["meta.list-tables", "meta.search"],
                warnings=[] if rows else ["No schemas were returned. Schema namespaces may not be enabled for this project."],
            ),
        )
        self.log("meta.list-schemas", envelope.status, envelope.metadata)
        return envelope

    def session_set(self, project: 'str | None' = None, schema: 'str | None' = None) -> 'Envelope':
        """Set default project and/or schema.
        
        Saves to session override file (~/.maxc/session_override.yaml) which has
        highest priority (above environment variables).
        """
        from .config import session_override_path, save_config_mapping, _load_yaml_file
        
        override_path = session_override_path()
        override = _load_yaml_file(override_path)
        
        changes = []
        warnings: 'list[str]' = []
        
        if project:
            if self.backend is not None:
                try:
                    self.backend.get_project_info(project)
                except Exception as exc:
                    raise ValidationError(
                        f"Unable to access project `{project}`: {exc}",
                        suggestion="Verify the project name and that the current identity has access.",
                    ) from exc
            else:
                warnings.append(
                    "Project override was saved without remote validation because no authenticated backend session is active."
                )
            override["project"] = project
            changes.append(f"project set to `{project}`")
            # Warn if session override project differs from the project saved in auth config
            if self.config.auth.project and project != self.config.auth.project:
                warnings.append(
                    f"Session project override (`{project}`) differs from the project saved in auth config "
                    f"(`{self.config.auth.project}`). Operations will use `{project}`, but credentials "
                    f"were configured for `{self.config.auth.project}`. Run `auth whoami` to verify access."
                )
        
        if schema:
            override["schema"] = schema
            changes.append(f"schema set to `{schema}`")
        elif schema is not None:  # explicitly cleared with --schema=""
            if "schema" in override:
                del override["schema"]
            changes.append("schema cleared")
        
        save_config_mapping(override_path, override)
        
        # Update current config in memory
        if project:
            self.config.default_project = project
            # Update backend project if available
            if self.backend is not None:
                self.backend.project = project
        if schema:
            self.config.default_schema = schema
        elif schema is not None:
            self.config.default_schema = None
        
        envelope = Envelope(
            command="session.set",
            status="success",
            data={
                "project": self.config.default_project,
                "schema": self.config.default_schema,
                "override_path": str(override_path),
                "changes": changes,
            },
            metadata={},
            agent_hints=AgentHints(
                next_actions=["meta.list-tables", "meta.list-schemas", "session.show"],
                insights=[f"Session override set: {', '.join(changes)}. Overrides environment variables."],
                warnings=warnings,
            ),
        )
        self.log("session.set", envelope.status, {"changes": changes})
        return envelope
    
    def session_unset(self) -> 'Envelope':
        """Clear session override, reverting to environment variables and config file."""
        from .config import session_override_path
        
        override_path = session_override_path()
        cleared = []
        
        if override_path.exists():
            override_path.unlink()
            cleared = ["project", "schema"]
        
        envelope = Envelope(
            command="session.unset",
            status="success",
            data={
                "cleared": cleared,
                "override_path": str(override_path),
            },
            metadata={},
            agent_hints=AgentHints(
                next_actions=["session.show", "session.set"],
                insights=["Session override cleared. Configuration will use environment variables and config file."],
            ),
        )
        self.log("session.unset", envelope.status, {})
        return envelope
    
    def session_show(self) -> 'Envelope':
        """Show current session settings with source information."""
        from .config import session_override_path, default_global_config_path, _load_yaml_file
        import os
        
        override_path = session_override_path()
        config_path = default_global_config_path()
        override = _load_yaml_file(override_path)
        
        # Determine source of project
        env_project = os.environ.get("MAXCOMPUTE_PROJECT") or os.environ.get("ODPS_PROJECT")
        if override.get("project"):
            project_source = "session_override"
        elif env_project:
            project_source = "environment"
        else:
            project_source = "config_file"
        
        # Determine source of schema
        if override.get("schema"):
            schema_source = "session_override"
        else:
            schema_source = "config_file"
        
        # Get project info if available
        project_info = None
        if self.backend is not None:
            try:
                raw_info = self.backend.get_project_info(self.config.default_project)
                project_info = {k: (str(v) if v is not None else None) for k, v in raw_info.items()}
            except Exception:
                pass
        
        envelope = Envelope(
            command="session.show",
            status="success",
            data={
                "project": {
                    "value": self.config.default_project,
                    "source": project_source,
                },
                "schema": {
                    "value": self.config.default_schema,
                    "source": schema_source,
                },
                "override_path": str(override_path),
                "config_path": str(config_path) if config_path.exists() else None,
                "project_info": project_info,
                "config_sources": [str(p) for p in self.config.sources],
            },
            metadata={},
            agent_hints=AgentHints(
                next_actions=["session.set", "session.unset", "meta.list-tables"],
                insights=[
                    f"Project `{self.config.default_project}` from {project_source}",
                    f"Schema `{self.config.default_schema or 'default'}` from {schema_source}",
                ],
            ),
        )
        self.log("session.show", envelope.status, {})
        return envelope

    def data_sample(
        self,
        table_name: 'str',
        rows: 'int' = 5,
        *,
        partition: 'str | None' = None,
        columns: 'list[str] | None' = None,
    ) -> 'Envelope':
        if rows <= 0:
            raise ValidationError("`--rows` must be greater than 0.")
        table, sample_rows, sample_info = self.backend.sample_table(
            table_name,
            rows,
            partition=partition,
            columns=columns,
        )
        envelope = Envelope(
            command="data.sample",
            status="success",
            data={
                "table_name": table.name,
                "rows": sample_rows,
                "returned_rows": len(sample_rows),
                "schema": sample_info["schema"],
                "applied_partition": sample_info["applied_partition"],
                "selected_columns": sample_info["selected_columns"],
            },
            metadata={
                "project": self.config.default_project,
                "requested_rows": rows,
                "requested_partition": partition,
                "requested_columns": columns or [],
            },
            agent_hints=AgentHints(next_actions=["data.profile", "query"]),
        )
        self.log("data.sample", envelope.status, envelope.metadata)
        return envelope

    def data_profile(self, table_name: 'str', *, partition: 'str | None' = None) -> 'Envelope':
        profile = self.backend.profile_table(table_name, partition=partition)
        envelope = Envelope(
            command="data.profile",
            status="success",
            data=profile,
            metadata={"project": self.config.default_project, "requested_partition": partition},
            agent_hints=AgentHints(next_actions=["query"]),
        )
        self.log("data.profile", envelope.status, envelope.metadata)
        return envelope

    def auth_login(
        self,
        *,
        access_id: 'str | None' = None,
        secret_access_key: 'str | None' = None,
        security_token: 'str | None' = None,
        project: 'str | None' = None,
        endpoint: 'str | None' = None,
        region_name: 'str | None' = None,
        tunnel_endpoint: 'str | None' = None,
        from_env: 'bool' = False,
        no_validate: 'bool' = False,
        target_config_path: 'Path | None' = None,
    ) -> 'Envelope':
        target_path = target_config_path or default_global_config_path()
        existing_payload = load_config_mapping(target_path) if target_path.exists() else {}
        existing_auth = AuthConfig.from_mapping(existing_payload.get("auth", {}) or {})
        env_settings = load_odps_env()

        resolved_auth = AuthConfig(
            access_id=self._resolve_login_value(
                provided=access_id,
                env_value=env_settings.get("access_id"),
                existing_value=existing_auth.access_id,
                prompt="Access Key ID",
                required=True,
                secret=False,
                use_env=from_env,
            ),
            secret_access_key=self._resolve_login_value(
                provided=secret_access_key,
                env_value=env_settings.get("secret_access_key"),
                existing_value=existing_auth.secret_access_key,
                prompt="Access Key Secret",
                required=True,
                secret=True,
                use_env=from_env,
            ),
            security_token=self._resolve_login_value(
                provided=security_token,
                env_value=env_settings.get("security_token"),
                existing_value=existing_auth.security_token,
                prompt="STS Security Token (optional)",
                required=False,
                secret=True,
                use_env=from_env,
            ),
            project=self._resolve_login_value(
                provided=project,
                env_value=env_settings.get("project"),
                existing_value=existing_auth.project,
                prompt="MaxCompute Project",
                required=True,
                secret=False,
                use_env=from_env,
            ),
            endpoint=self._resolve_login_value(
                provided=endpoint,
                env_value=env_settings.get("endpoint"),
                existing_value=existing_auth.endpoint,
                prompt="MaxCompute Endpoint",
                required=True,
                secret=False,
                use_env=from_env,
            ),
            region_name=self._resolve_login_value(
                provided=region_name,
                env_value=env_settings.get("region_name"),
                existing_value=existing_auth.region_name,
                prompt="MaxCompute Region (optional)",
                required=False,
                secret=False,
                use_env=from_env,
            ),
            tunnel_endpoint=self._resolve_login_value(
                provided=tunnel_endpoint,
                env_value=env_settings.get("tunnel_endpoint"),
                existing_value=existing_auth.tunnel_endpoint,
                prompt="MaxCompute Tunnel Endpoint (optional)",
                required=False,
                secret=False,
                use_env=from_env,
            ),
        )
        resolved_auth.provider = "sts_token" if resolved_auth.security_token else "access_key"
        if no_validate:
            self._validate_auth_config_shape(resolved_auth)
        else:
            resolve_auth_connection(self.config, auth_override=resolved_auth)

        persist_login_config(
            target_path,
            auth=resolved_auth,
        )

        warnings: 'list[str]' = []
        if any(
            env_settings.get(name)
            for name in ("access_id", "secret_access_key", "security_token", "project", "endpoint")
        ):
            warnings.append(
                "MaxCompute-related environment variables are set in the current shell; they may override the config you just saved."
            )

        if no_validate:
            payload = {
                "authenticated": None,
                "configured": True,
                "validation_status": "configuration_only",
                "backend": "odps",
                "auth_type": resolved_auth.provider,
                "identity_source": "config_file",
                "principal_display": mask_access_id(resolved_auth.access_id),
                "principal_masked": mask_access_id(resolved_auth.access_id),
                "project": resolved_auth.project,
                "region": resolved_auth.region_name,
                "endpoint": resolved_auth.endpoint,
                "project_owner": None,
                "allowed_operations": self.config.allowed_operations,
                "saved": True,
                "validated": False,
            }
            if resolved_auth.security_token:
                payload["token_expires_at"] = resolved_auth.token_expires_at
            warnings.append("Authentication settings were saved without remote validation.")
        else:
            payload, validate_warnings = self._validate_auth_config(resolved_auth)
            payload["saved"] = True
            payload["validated"] = True
            warnings.extend(validate_warnings)

        envelope = Envelope(
            command="auth.login",
            status="success",
            data=payload,
            metadata={
                "config_path": str(target_path),
                "written_fields": sorted(resolved_auth.to_mapping().keys()),
                "auth_storage": "config_file",
            },
            agent_hints=AgentHints(
                next_actions=["auth.whoami", "meta.list-tables"],
                warnings=warnings,
            ),
        )
        self.log("auth.login", envelope.status, envelope.metadata)
        return envelope

    def auth_login_ncs(
        self,
        *,
        account_type: 'str | None' = None,
        employee_id: 'str | None' = None,
        account_name: 'str | None' = None,
        app_name: 'str | None' = None,
        project: 'str | None' = None,
        endpoint: 'str | None' = None,
        region_name: 'str | None' = None,
        tunnel_endpoint: 'str | None' = None,
        interactive: 'bool' = False,
        list_accounts_mode: 'bool' = False,
        no_validate: 'bool' = False,
        target_config_path: 'Path | None' = None,
    ) -> 'Envelope':
        target_path = target_config_path or default_global_config_path()
        existing_payload = load_config_mapping(target_path) if target_path.exists() else {}
        existing_auth = AuthConfig.from_mapping(existing_payload.get("auth", {}) or {})

        if list_accounts_mode:
            normalized_type = account_type or existing_auth.ncs.account_type or "user"
            payload = list_ncs_accounts(normalized_type)
            envelope = Envelope(
                command="auth.login-ncs",
                status="success",
                data=payload,
                metadata={"config_path": str(target_path)},
                agent_hints=AgentHints(next_actions=["auth.login-ncs"]),
            )
            self.log("auth.login-ncs", envelope.status, envelope.metadata)
            return envelope

        if interactive:
            account_type = account_type or self._prompt_text(
                "ncs account type (user/account/app)",
                default=existing_auth.ncs.account_type,
            )
            normalized_type = (account_type or "").strip().lower()
            if normalized_type == "user":
                employee_id = employee_id or self._prompt_text(
                    "Employee ID", default=existing_auth.ncs.employee_id
                )
            elif normalized_type == "account":
                account_name = account_name or self._prompt_text(
                    "Account name", default=existing_auth.ncs.account_name
                )
            elif normalized_type == "app":
                app_name = app_name or self._prompt_text(
                    "App name", default=existing_auth.ncs.app_name
                )
            project = project or self._prompt_text(
                "MaxCompute Project", default=existing_auth.project
            )
            endpoint = endpoint or self._prompt_text(
                "MaxCompute Endpoint", default=existing_auth.endpoint
            )
            region_name = region_name or self._prompt_text(
                "MaxCompute Region (optional)",
                required=False,
                default=existing_auth.region_name,
            )
            tunnel_endpoint = tunnel_endpoint or self._prompt_text(
                "MaxCompute Tunnel Endpoint (optional)",
                required=False,
                default=existing_auth.tunnel_endpoint,
            )

        normalized_type = (
            account_type
            or existing_auth.ncs.account_type
            or "user"
        )
        ncs_config = build_ncs_auth_config(
            account_type=normalized_type,
            employee_id=employee_id or existing_auth.ncs.employee_id,
            account_name=account_name or existing_auth.ncs.account_name,
            app_name=app_name or existing_auth.ncs.app_name,
            process_timeout=existing_auth.ncs.process_timeout,
        )
        resolved_auth = AuthConfig(
            provider="ncs",
            project=project or existing_auth.project,
            endpoint=endpoint or existing_auth.endpoint,
            region_name=region_name or existing_auth.region_name,
            tunnel_endpoint=tunnel_endpoint or existing_auth.tunnel_endpoint,
            ncs=ncs_config,
        )
        if no_validate:
            self._validate_auth_config_shape(resolved_auth)
        else:
            resolve_auth_connection(self.config, auth_override=resolved_auth)

        persist_login_config(
            target_path,
            auth=resolved_auth,
        )

        warnings: 'list[str]' = []
        env_settings = load_odps_env()
        overriding_env_fields = [
            name for name in ("project", "endpoint")
            if env_settings.get(name)
        ]
        if overriding_env_fields:
            warnings.append(
                f"Environment variable(s) for {', '.join(overriding_env_fields)} are set and will override "
                f"the values you just saved at runtime. Unset them or they will take precedence over this ncs config."
            )

        if no_validate:
            payload = {
                "authenticated": None,
                "configured": True,
                "validation_status": "configuration_only",
                "backend": "odps",
                "auth_type": "ncs",
                "identity_source": "config_file",
                "principal_display": None,
                "principal_masked": None,
                "project": resolved_auth.project,
                "region": resolved_auth.region_name,
                "endpoint": resolved_auth.endpoint,
                "project_owner": None,
                "allowed_operations": self.config.allowed_operations,
                "saved": True,
                "validated": False,
                "ncs": {
                    "account_type": resolved_auth.ncs.account_type,
                    "process_command": resolved_auth.ncs.process_command,
                },
            }
            warnings.append("ncs authentication settings were saved without remote validation.")
        else:
            payload, validate_warnings = self._validate_auth_config(resolved_auth)
            payload["saved"] = True
            payload["validated"] = True
            warnings.extend(validate_warnings)

        envelope = Envelope(
            command="auth.login-ncs",
            status="success",
            data=payload,
            metadata={
                "config_path": str(target_path),
                "written_fields": sorted(resolved_auth.to_mapping().keys()),
                "auth_storage": "config_file",
            },
            agent_hints=AgentHints(
                next_actions=["auth.whoami", "meta.list-tables"],
                warnings=warnings,
            ),
        )
        self.log("auth.login-ncs", envelope.status, envelope.metadata)
        return envelope

    def auth_whoami(self) -> 'Envelope':
        if self.backend is None:
            try:
                self.backend = OdpsBackend(self.config)
                self.remote_jobs = getattr(self.backend, "supports_remote_jobs", False)
            except ValidationError as exc:
                envelope = self._unauthenticated_whoami_envelope(warnings=[exc.message])
                self.log("auth.whoami", envelope.status, envelope.metadata)
                return envelope

        try:
            payload, warnings = self.backend.whoami_info(project=self.config.default_project)
        except MaxCError as exc:
            suppressed = getattr(getattr(self.backend, "resolved_auth", None), "suppressed_env_vars", [])
            extra_warnings = [exc.message]
            if suppressed:
                extra_warnings.append(
                    f"{len(suppressed)} environment variable(s) are set but ignored because an explicit "
                    f"auth provider is configured ({', '.join(suppressed)}). "
                    f"To use environment variables, run `auth login --from-env` or unset the auth provider in config."
                )
            envelope = self._whoami_validation_failed_envelope(
                settings=getattr(self.backend, "settings", {}),
                auth_type=getattr(getattr(self.backend, "resolved_auth", None), "auth_type", "access_key"),
                identity_source=getattr(getattr(self.backend, "resolved_auth", None), "identity_source", "unknown"),
                warnings=extra_warnings,
            )
            self.log("auth.whoami", envelope.status, envelope.metadata)
            return envelope

        suppressed = getattr(getattr(self.backend, "resolved_auth", None), "suppressed_env_vars", [])
        if suppressed:
            warnings = list(warnings) + [
                f"{len(suppressed)} environment variable(s) are set but ignored because an explicit "
                f"auth provider is configured ({', '.join(suppressed)}). "
                f"To use environment variables, run `auth login --from-env` or unset the auth provider in config."
            ]
        envelope = Envelope(
            command="auth.whoami",
            status="success",
            data=payload,
            metadata={
                "project": self.config.default_project,
                "config_sources": [str(p) for p in self.config.sources],
            },
            agent_hints=AgentHints(
                next_actions=["auth.can-i", "meta.list-tables"],
                warnings=warnings,
            ),
        )
        self.log("auth.whoami", envelope.status, envelope.metadata)
        return envelope

    def auth_can_i(
        self,
        *,
        table_name: 'str',
        operation: 'str',
        project: 'str | None' = None,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        payload, warnings = self.backend.can_i_info(
            table_name=table_name,
            operation=operation,
            project=target_project,
        )
        next_actions = ["query.cost", "query.explain"] if payload.get("allowed") else ["auth.whoami", "meta.describe"]
        envelope = Envelope(
            command="auth.can-i",
            status="success",
            data=payload,
            metadata={"project": target_project},
            agent_hints=AgentHints(next_actions=next_actions, warnings=warnings),
        )
        self.log("auth.can-i", envelope.status, envelope.metadata)
        return envelope

    def _validate_auth_config(
        self,
        auth: 'AuthConfig',
    ) -> 'tuple[dict[str, Any], list[str]]':
        resolved = resolve_auth_connection(self.config, auth_override=auth)
        client = resolved.create_client()

        owner_display_name = None
        try:
            project = client.get_project(resolved.project)
            owner_display_name = getattr(project, "owner", None)
        except Exception:
            owner_display_name = None

        return build_odps_identity_payload(
            client=client,
            settings=resolved.settings,
            allowed_operations=self.config.allowed_operations,
            identity_source=resolved.identity_source,
            auth_type=resolved.auth_type,
            token_expires_at=resolved.token_expires_at,
            project=resolved.project,
            owner_display_name=owner_display_name,
        )

    def _unauthenticated_whoami_envelope(
        self,
        *,
        warnings: 'list[str] | None' = None,
    ) -> 'Envelope':
        payload = {
            "authenticated": False,
            "configured": False,
            "validation_status": "missing_configuration",
            "backend": "odps",
            "auth_type": None,
            "identity_source": "unknown",
            "principal_display": None,
            "principal_masked": None,
            "project": self.config.default_project or None,
            "region": self.config.default_region or None,
            "endpoint": self.config.auth.endpoint,
            "project_owner": None,
            "allowed_operations": self.config.allowed_operations,
            "auth_options": build_auth_options(default_global_config_path()),
        }
        return Envelope(
            command="auth.whoami",
            status="success",
            data=payload,
            metadata={
                "project": self.config.default_project,
                "config_sources": [str(p) for p in self.config.sources],
            },
            agent_hints=AgentHints(
                next_actions=["auth.login", "auth.login-ncs"],
                warnings=(warnings or ["No active MaxCompute credentials are configured."]),
            ),
        )

    def _resolve_login_value(
        self,
        *,
        provided: 'str | None',
        env_value: 'str | None',
        existing_value: 'str | None',
        prompt: 'str',
        required: 'bool',
        secret: 'bool',
        use_env: 'bool',
    ) -> 'str | None':
        if provided is not None and provided.strip():
            return provided.strip()
        if use_env and env_value:
            return env_value.strip()
        if existing_value:
            return existing_value.strip()
        if not required:
            return None
        if not sys.stdin.isatty():
            return None

        if secret:
            value = getpass.getpass(f"{prompt}: ").strip()
        else:
            value = input(f"{prompt}: ").strip()
        return value or None

    def _prompt_text(
        self,
        prompt: 'str',
        *,
        required: 'bool' = True,
        default: 'str | None' = None,
    ) -> 'str | None':
        if not sys.stdin.isatty():
            return default
        display_prompt = f"{prompt} [current: {default}]" if default else prompt
        value = input(f"{display_prompt}: ").strip()
        if value:
            return value
        if default:
            return default
        if required:
            raise ValidationError(f"{prompt} is required.")
        return None

    def schema_diff(self, left_table: 'str', right_table: 'str') -> 'Envelope':
        left = self.backend.describe_table(left_table)
        right = self.backend.describe_table(right_table)
        payload = build_schema_diff_payload(left, right)
        next_actions = ["meta.describe", "auth.can-i"]
        if payload.get("compatible"):
            next_actions = ["query.explain", "auth.can-i"]
        envelope = Envelope(
            command="diff.schema",
            status="success",
            data=payload,
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=next_actions),
        )
        self.log("diff.schema", envelope.status, envelope.metadata)
        return envelope

    def partition_diff(self, left_table: 'str', right_table: 'str') -> 'Envelope':
        left = self.backend.describe_table(left_table)
        right = self.backend.describe_table(right_table)
        payload = build_partition_diff_payload(left, right)
        envelope = Envelope(
            command="diff.partition",
            status="success",
            data=payload,
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["meta.partitions", "meta.latest-partition"]),
        )
        self.log("diff.partition", envelope.status, envelope.metadata)
        return envelope

    def data_diff(
        self,
        left_table: 'str',
        right_table: 'str',
        *,
        keys: 'list[str]',
        columns: 'list[str] | None' = None,
        rows: 'int' = 100,
        partition: 'str | None' = None,
        left_partition: 'str | None' = None,
        right_partition: 'str | None' = None,
    ) -> 'Envelope':
        if rows <= 0:
            raise ValidationError("`--rows` must be greater than 0.")
        if not keys:
            raise ValidationError("`--keys` requires at least one column.")

        left = self.backend.describe_table(left_table)
        right = self.backend.describe_table(right_table)
        compared_columns = resolve_data_diff_columns(
            left,
            right,
            keys=keys,
            requested_columns=columns,
        )
        fetch_columns = dedupe_preserve_order([*keys, *compared_columns])
        resolved_left_partition = left_partition or partition
        resolved_right_partition = right_partition or partition
        _, left_rows, _ = self.backend.sample_table(
            left_table,
            rows,
            partition=resolved_left_partition,
            columns=fetch_columns,
        )
        _, right_rows, _ = self.backend.sample_table(
            right_table,
            rows,
            partition=resolved_right_partition,
            columns=fetch_columns,
        )

        payload, warnings = build_data_diff_payload(
            left=left,
            right=right,
            left_rows=left_rows,
            right_rows=right_rows,
            keys=keys,
            compared_columns=compared_columns,
            rows_limit=rows,
            left_partition=resolved_left_partition,
            right_partition=resolved_right_partition,
        )
        envelope = Envelope(
            command="diff.data",
            status="success",
            data=payload,
            metadata={
                "project": self.config.default_project,
                "requested_rows": rows,
                "requested_columns": columns or [],
                "requested_keys": keys,
                "requested_partition": partition,
                "left_requested_partition": resolved_left_partition,
                "right_requested_partition": resolved_right_partition,
            },
            agent_hints=AgentHints(
                next_actions=["data.profile", "diff.schema", "meta.describe"],
                warnings=warnings,
            ),
        )
        self.log("diff.data", envelope.status, envelope.metadata)
        return envelope

    def agent_context(self) -> 'Envelope':
        """Return project context without listing tables (too slow on large projects)."""
        envelope = Envelope(
            command="agent.context",
            status="success",
            data={
                "project": self.config.default_project,
                "region": self.config.default_region,
                "backend": "odps",
                "project_context": self.config.project_context,
                "allowed_operations": self.config.allowed_operations,
                "cost_threshold_cu": self.config.cost_threshold_cu,
                "sensitive_columns": self.config.sensitive_columns,
            },
            metadata={
                "config_sources": [str(path) for path in self.config.sources],
                "state_dir": str(self.config.state_dir),
                "job_mode": "remote" if self.remote_jobs else "local" if self.backend is not None else "unknown",
            },
            agent_hints=AgentHints(
                next_actions=["meta.search", "meta.list-tables"],
                insights=["Use `meta list-tables` to enumerate tables and `meta search` to locate relevant datasets."],
            ),
        )
        self.log("agent.context", envelope.status, envelope.metadata)
        return envelope

    def feature_unavailable(self, command: 'str', message: 'str') -> 'Envelope':
        raise FeatureUnavailableError(
            message,
            suggestion="Run `maxc --help` to inspect the currently supported commands.",
        )

    def log(
        self,
        command: 'str',
        status: 'str',
        metadata: 'dict[str, Any] | None' = None,
        *,
        error: 'dict[str, Any] | None' = None,
    ) -> 'None':
        try:
            if self._audit is None:
                self._audit = AuditLogger(self._audit_path)
            self._audit.log(
                {
                    "command": command,
                    "status": status,
                    "metadata": metadata or {},
                    "error": error,
                }
            )
        except OSError:
            # Command execution should not fail solely because the audit path is unavailable.
            return

    def _submit_remote_job(
        self,
        *,
        sql: 'str',
        project: 'str',
        cost_check: 'float | None',
        idempotency_key: 'str | None',
    ) -> 'JobInfo':
        if cost_check is not None:
            raise FeatureUnavailableError(
                "The real MaxCompute backend does not yet support CU-based `--cost-check` validation.",
                suggestion="Run `--dry-run` first to inspect SQLCost metadata, or remove `--cost-check`.",
            )
        return self.backend.submit_query(
            sql,
            project=project,
            idempotency_key=idempotency_key,
        )

    def _execute_query(
        self,
        *,
        sql: 'str',
        project: 'str',
        max_rows: 'int',
        offset: 'int',
        dry_run: 'bool',
        cost_check: 'float | None',
        retry_on: 'list[str]',
        max_retries: 'int',
        strict_cost_check: 'bool',
        timeout: 'int | None' = None,
    ) -> 'QueryResult':
        if sql.startswith("@natural"):
            raise FeatureUnavailableError(
                "`@natural` is a roadmap feature and is not available in the current MVP.",
                suggestion="Use `maxc meta search` or `maxc meta describe` to inspect tables, then submit plain SQL.",
            )

        attempts = 0
        while True:
            try:
                if cost_check is not None and strict_cost_check and not self.backend.supports_cost_check:
                    raise FeatureUnavailableError(
                        "The current backend does not provide CU-based cost validation.",
                        suggestion="Remove `--cost-check`, or use `--dry-run` to inspect SQLCost metadata.",
                    )

                result = self.backend.execute_query(
                    sql,
                    project=project,
                    max_rows=max_rows,
                    dry_run=dry_run,
                    offset=offset,
                    timeout=timeout,
                )
                return result
            except MaxCError as exc:
                attempts += 1
                can_retry = (
                    attempts <= max_retries
                    and exc.recoverable
                    and exc.error_code in retry_on
                )
                if not can_retry:
                    raise

    def _analyze_query(
        self,
        *,
        sql: 'str',
        project: 'str',
        explain: 'bool',
    ) -> 'dict[str, Any]':
        if sql.startswith("@natural"):
            raise FeatureUnavailableError(
                "`@natural` is a roadmap feature and is not available in the current MVP.",
                suggestion="Use `maxc meta search` or `maxc meta describe` to inspect tables, then submit plain SQL.",
            )
        if explain:
            return self.backend.explain_query(sql, project=project)
        return self.backend.estimate_query_cost(sql, project=project)

    def _build_query_envelope(
        self,
        *,
        command: 'str',
        result: 'QueryResult',
        dry_run: 'bool',
        session_id: 'int | None' = None,
    ) -> 'Envelope':
        insights = []
        next_actions = ["meta.describe"] if result.tables_used else []
        if result.has_more:
            next_actions.append("query.paginate")
        if dry_run:
            next_actions.append("job.submit")
            insights.append("Dry-run returned estimated cost and SQLCost metadata so you can decide whether to continue.")
        elif not result.rows:
            insights.append("The result set is empty. Check filters, partitions, and table selection.")

        # 如果有 job_id 且 has_more，创建或复用 session，生成短 cursor
        next_cursor = None
        if result.has_more and result.returned_rows > 0:
            current_offset = result.extra_metadata.get("current_offset", 0)
            next_offset = current_offset + result.returned_rows
            if result.job_id and self.remote_jobs:
                # 远程 backend: 用 session_id 生成短 cursor
                if session_id is None:
                    session_id = self.cache.create_session(
                        job_id=result.job_id,
                        project=result.project,
                        sql=result.sql_executed,
                    )
                next_cursor = encode_cursor(next_offset, session_id=session_id)
            else:
                # Mock backend: 只包含 offset
                next_cursor = encode_cursor(next_offset)

        metadata = {
            "project": result.project,
            "elapsed_ms": result.elapsed_ms,
            "bytes_scanned": result.bytes_scanned,
            "sql_executed": result.sql_executed,
            "tables_used": result.tables_used,
        }
        if result.job_id:
            metadata["job_id"] = result.job_id
        if result.submitted_at:
            metadata["submitted_at"] = result.submitted_at
        if result.completed_at:
            metadata["completed_at"] = result.completed_at
        metadata.update(result.extra_metadata)

        return Envelope(
            command=command,
            status="success",
            data={
                "rows": result.rows,
                "schema": result.schema,
                "total_rows": result.total_rows,
                "returned_rows": result.returned_rows,
                "has_more": result.has_more,
                "next_cursor": next_cursor,
            },
            metadata=metadata,
            agent_hints=AgentHints(
                next_actions=next_actions,
                warnings=result.warnings,
                insights=insights,
            ),
        )

    def _build_analysis_envelope(
        self,
        *,
        command: 'str',
        sql: 'str',
        analysis: 'dict[str, Any]',
    ) -> 'Envelope':
        warnings = list(analysis.get("warnings", []))
        next_actions = ["query"]
        if analysis.get("tables_used"):
            next_actions.append("meta.describe")
        if command == "query.cost":
            next_actions.insert(0, "query.explain")
        insights = []
        if analysis.get("estimated_input_size_bytes") == 0:
            insights.append("The estimated scan input is 0 bytes. This is often a constant query or a plan that avoids scanning data.")

        metadata = {
            "project": analysis.get("project"),
            "sql_executed": sql.rstrip(";"),
        }
        if analysis.get("elapsed_ms") is not None:
            metadata["elapsed_ms"] = analysis["elapsed_ms"]

        return Envelope(
            command=command,
            status="success",
            data=analysis,
            metadata=metadata,
            agent_hints=AgentHints(
                next_actions=next_actions,
                warnings=warnings,
                insights=insights,
            ),
        )

    def _job_info_envelope(self, command: 'str', info: 'JobInfo') -> 'Envelope':
        next_actions = ["job.wait", "job.result"] if info.status in {"pending", "running"} else ["job.result"]
        if info.status == "failure":
            next_actions = ["job.diagnose", "job.status"]
        return Envelope(
            command=command,
            status=info.status,
            data={
                "job_id": info.job_id,
                "status": info.status,
                "progress": info.progress,
                "stage": info.stage,
                "retryable": info.retryable,
                "failure_reason": info.failure_reason,
                "logview": info.logview,
                "task_summary": info.task_summary,
                "sql": info.sql,
            },
            metadata={
                "project": info.project,
                "submitted_at": info.submitted_at,
                "updated_at": info.updated_at,
                "completed_at": info.completed_at,
                "logview": info.logview,
                "error_message": info.error_message,
            },
            agent_hints=AgentHints(
                next_actions=next_actions,
                warnings=info.warnings,
            ),
        )

    def _local_job_info(self, job: 'dict[str, Any]') -> 'JobInfo':
        status = job["status"]
        stage = "queue" if status == "pending" else "completed" if status == "success" else "failed"
        failure_reason = "The job was cancelled." if job.get("cancelled") else None
        diagnosis = classify_failure_reason(failure_reason)
        task_summary = build_task_summary(job.get("sql"))
        return JobInfo(
            job_id=job["job_id"],
            status=status,
            project=job["project"],
            progress=job["progress"],
            stage=stage,
            retryable=diagnosis["retryable"] if status == "failure" else None,
            failure_reason=failure_reason,
            task_summary=task_summary,
            sql=job.get("sql"),
            submitted_at=job.get("submitted_at"),
            updated_at=job.get("updated_at"),
            completed_at=job.get("completed_at"),
            logview=None,
        )

    def _query_result_payload(self, result: 'QueryResult') -> 'dict[str, Any]':
        envelope = self._build_query_envelope(
            command="query",
            result=result,
            dry_run=False,
        )
        return envelope.to_dict(normalize=False)

    def _job_events(self, job: 'dict[str, Any]') -> 'list[dict[str, Any]]':
        if job["status"] == "success":
            return [
                {
                    "type": "completed",
                    "ts": now_utc_iso(),
                    "job_id": job["job_id"],
                    "rows": job["result"]["data"]["returned_rows"],
                }
            ]
        if job.get("cancelled"):
            raise ValidationError("The job was cancelled and can no longer be waited on.")

        return [
            {"type": "started", "ts": now_utc_iso(), "job_id": job["job_id"]},
            {
                "type": "progress",
                "ts": now_utc_iso(),
                "job_id": job["job_id"],
                "percent": 20,
                "stage": "queue",
            },
            {
                "type": "progress",
                "ts": now_utc_iso(),
                "job_id": job["job_id"],
                "percent": 60,
                "stage": "scan",
            },
            {
                "type": "progress",
                "ts": now_utc_iso(),
                "job_id": job["job_id"],
                "percent": 90,
                "stage": "finalize",
            },
            {
                "type": "completed",
                "ts": now_utc_iso(),
                "job_id": job["job_id"],
                "rows": job["result"]["data"]["returned_rows"],
            },
        ]

    def _remote_job_events(
        self,
        before: 'JobInfo',
        after: 'JobInfo',
        result: 'QueryResult',
    ) -> 'list[dict[str, Any]]':
        events = [{"type": "started", "ts": before.submitted_at or now_utc_iso(), "job_id": before.job_id}]
        if before.status in {"pending", "running"}:
            events.append(
                {
                    "type": "progress",
                    "ts": now_utc_iso(),
                    "job_id": before.job_id,
                    "percent": before.progress or 50,
                    "stage": before.status,
                }
            )
        events.append(
            {
                "type": "completed",
                "ts": after.completed_at or now_utc_iso(),
                "job_id": after.job_id,
                "rows": result.returned_rows,
            }
        )
        return events

    def _table_payload(self, table: 'TableDefinition', full: 'bool' = False) -> 'dict[str, Any]':
        # Calculate size in MB
        size_mb = (table.size_bytes / (1024 * 1024)) if table.size_bytes else 0
        
        # Identify primary key with better heuristics:
        # 1. Look for explicit primary key indicators: *_id (but not ending with _sk), pk_*, id
        # 2. Exclude foreign key patterns and common FK suffixes
        foreign_key_suffixes = ('_date_sk', '_time_sk', '_dim_sk', '_demo_sk', '_hdemo_sk', 
                                '_cdemo_sk', '_addr_sk', '_promo_sk', '_item_sk', '_customer_sk',
                                '_store_sk', '_bill_sk', '_ship_sk', '_reason_sk')
        primary_key = None
        
        # First pass: look for actual primary keys (not ending with _sk)
        for col in table.columns:
            col_lower = col.name.lower()
            # Primary key candidates: ends with _id but not _sk, or explicitly named 'id'/'pk_*'
            if (col_lower.endswith('_id') and not col_lower.endswith('_sk')) or \
               col_lower == 'id' or col_lower.startswith('pk_'):
                primary_key = col.name
                break
        
        # Second pass: if no clear PK, check for ticket numbers or other business keys
        if not primary_key:
            for col in table.columns:
                col_lower = col.name.lower()
                if 'ticket_number' in col_lower or 'order_number' in col_lower:
                    primary_key = col.name
                    break
        
        payload = {
            "table_name": table.name,
            "description": table.description,
            "row_count": None,  # Not available from TableDefinition
            "size_mb": round(size_mb, 2),
            "column_count": len(table.columns),
            "table_type": table.table_type,
            "owner": table.owner,
            "created_at": table.created_at,
            "updated_at": table.updated_at,
        }
        
        if full:
            # Full mode: return all columns
            payload["schema"] = [
                {"name": column.name, "type": column.type, "comment": column.comment}
                for column in table.columns
            ]
            payload["has_more_columns"] = False
            # Full mode: include complete sample rows
            payload["sample_preview"] = table.sample_rows[:2]
        else:
            # Summary mode: return first 10 columns only
            display_columns = table.columns[:10]
            payload["schema"] = [
                {"name": column.name, "type": column.type, "comment": column.comment}
                for column in display_columns
            ]
            payload["has_more_columns"] = len(table.columns) > 10
            payload["remaining_columns"] = max(0, len(table.columns) - 10)
            
            # Summary mode: no sample preview (keep it lightweight)
            payload["sample_preview"] = []
        
        # Add primary key info (None if no clear primary key found)
        payload["primary_key"] = primary_key
        
        # Add partition columns
        payload["partition_columns"] = [
            {"name": column.name, "type": column.type, "comment": column.comment}
            for column in table.partition_columns
        ]
        
        # Add other metadata
        payload["partitions"] = table.partitions
        payload["lineage"] = {
            "upstream_tables": table.upstream_tables,
            "downstream_tables": table.downstream_tables,
        }
        payload["extra_metadata"] = table.extra_metadata
        
        return payload


def build_schema_diff_payload(left: 'TableDefinition', right: 'TableDefinition') -> 'dict[str, Any]':
    columns = compare_columns(left.columns, right.columns, scope="columns")
    partition_columns = compare_columns(
        left.partition_columns,
        right.partition_columns,
        scope="partition_columns",
        added_is_breaking=True,
    )

    breaking_changes = list(columns["breaking_changes"])
    non_breaking_changes = list(columns["non_breaking_changes"])
    breaking_changes.extend(partition_columns["breaking_changes"])
    non_breaking_changes.extend(partition_columns["non_breaking_changes"])

    table_type_changed = left.table_type != right.table_type
    if table_type_changed:
        breaking_changes.append(
            {
                "kind": "table_type_changed",
                "scope": "table_attributes",
                "field": "table_type",
                "left": left.table_type,
                "right": right.table_type,
            }
        )

    return {
        "left_table": left.name,
        "right_table": right.name,
        "compatible": not breaking_changes,
        "summary": {
            "added_columns": len(columns["added"]),
            "removed_columns": len(columns["removed"]),
            "changed_columns": len(columns["changed"]),
            "unchanged_columns": len(columns["unchanged"]),
            "added_partition_columns": len(partition_columns["added"]),
            "removed_partition_columns": len(partition_columns["removed"]),
            "changed_partition_columns": len(partition_columns["changed"]),
            "breaking_changes": len(breaking_changes),
            "non_breaking_changes": len(non_breaking_changes),
        },
        "breaking_changes": breaking_changes,
        "non_breaking_changes": non_breaking_changes,
        "columns": {
            "added": columns["added"],
            "removed": columns["removed"],
            "changed": columns["changed"],
            "unchanged": columns["unchanged"],
        },
        "partition_columns": {
            "added": partition_columns["added"],
            "removed": partition_columns["removed"],
            "changed": partition_columns["changed"],
            "unchanged": partition_columns["unchanged"],
        },
        "table_attributes": {
            "table_type": {
                "left": left.table_type,
                "right": right.table_type,
                "changed": table_type_changed,
            }
        },
    }


def build_partition_diff_payload(left: 'TableDefinition', right: 'TableDefinition') -> 'dict[str, Any]':
    left_partitions = set(left.partitions)
    right_partitions = set(right.partitions)
    left_only = sorted(left_partitions - right_partitions)
    right_only = sorted(right_partitions - left_partitions)
    common = sorted(left_partitions & right_partitions)
    return {
        "left_table": left.name,
        "right_table": right.name,
        "compatible": not left_only and not right_only,
        "summary": {
            "left_partition_count": len(left.partitions),
            "right_partition_count": len(right.partitions),
            "left_only": len(left_only),
            "right_only": len(right_only),
            "common": len(common),
        },
        "left_only": left_only,
        "right_only": right_only,
        "common": common,
    }


def build_data_diff_payload(
    *,
    left: 'TableDefinition',
    right: 'TableDefinition',
    left_rows: 'list[dict[str, Any]]',
    right_rows: 'list[dict[str, Any]]',
    keys: 'list[str]',
    compared_columns: 'list[str]',
    rows_limit: 'int',
    left_partition: 'str | None',
    right_partition: 'str | None',
) -> 'tuple[dict[str, Any], list[str]]':
    left_by_key = index_rows_by_key(left_rows, keys=keys, table_name=left.name)
    right_by_key = index_rows_by_key(right_rows, keys=keys, table_name=right.name)
    left_keys = set(left_by_key)
    right_keys = set(right_by_key)
    common_keys = sorted(left_keys & right_keys)
    left_only_keys = sorted(left_keys - right_keys)
    right_only_keys = sorted(right_keys - left_keys)

    mismatched_rows: 'list[dict[str, Any]]' = []
    for key in common_keys:
        left_row = left_by_key[key]
        right_row = right_by_key[key]
        differing_columns = [
            column
            for column in compared_columns
            if normalize_diff_value(left_row.get(column)) != normalize_diff_value(right_row.get(column))
        ]
        if differing_columns:
            mismatched_rows.append(
                {
                    "key": key_to_payload(keys, key),
                    "differing_columns": differing_columns,
                    "left_row": left_row,
                    "right_row": right_row,
                }
            )

    warnings = [
        (
            f"Data diff currently compares at most {rows_limit} read-only snapshot row(s) per side. "
            "Increase `--rows` and narrow the partition scope if you need higher coverage."
        )
    ]
    if not compared_columns:
        warnings.append("Only key presence was compared. Non-key column values were not compared.")

    payload = {
        "left_table": left.name,
        "right_table": right.name,
        "comparison_mode": "keyed_snapshot",
        "compatible": not left_only_keys and not right_only_keys and not mismatched_rows,
        "keys": keys,
        "compared_columns": compared_columns,
        "left_partition": left_partition,
        "right_partition": right_partition,
        "left_sampled_rows": len(left_rows),
        "right_sampled_rows": len(right_rows),
        "summary": {
            "matched_keys": len(common_keys),
            "mismatched_rows": len(mismatched_rows),
            "left_only_keys": len(left_only_keys),
            "right_only_keys": len(right_only_keys),
            "left_sampled_rows": len(left_rows),
            "right_sampled_rows": len(right_rows),
        },
        "left_only_keys": [key_to_payload(keys, key) for key in left_only_keys],
        "right_only_keys": [key_to_payload(keys, key) for key in right_only_keys],
        "mismatched_rows": mismatched_rows,
    }
    return payload, warnings


def compare_columns(
    left_columns: 'list[Any]',
    right_columns: 'list[Any]',
    *,
    scope: 'str',
    added_is_breaking: 'bool' = False,
) -> 'dict[str, Any]':
    left_by_name = {column.name: column for column in left_columns}
    right_by_name = {column.name: column for column in right_columns}
    left_names = set(left_by_name)
    right_names = set(right_by_name)

    added = [column_payload(right_by_name[name]) for name in sorted(right_names - left_names)]
    removed = [column_payload(left_by_name[name]) for name in sorted(left_names - right_names)]
    unchanged: 'list[str]' = []
    changed: 'list[dict[str, Any]]' = []
    breaking_changes: 'list[dict[str, Any]]' = []
    non_breaking_changes: 'list[dict[str, Any]]' = []

    for name in sorted(left_names & right_names):
        left = left_by_name[name]
        right = right_by_name[name]
        field_changes: 'dict[str, dict[str, Any]]' = {}
        if left.type != right.type:
            field_changes["type"] = {"left": left.type, "right": right.type}
            breaking_changes.append(
                {
                    "kind": "column_type_changed",
                    "scope": scope,
                    "column": name,
                    "left": left.type,
                    "right": right.type,
                }
            )
        if left.comment != right.comment:
            field_changes["comment"] = {"left": left.comment, "right": right.comment}
            non_breaking_changes.append(
                {
                    "kind": "column_comment_changed",
                    "scope": scope,
                    "column": name,
                    "left": left.comment,
                    "right": right.comment,
                }
            )
        if field_changes:
            changed.append(
                {
                    "name": name,
                    "left": column_payload(left),
                    "right": column_payload(right),
                    "changes": field_changes,
                }
            )
        else:
            unchanged.append(name)

    for column in removed:
        breaking_changes.append(
            {
                "kind": "column_removed",
                "scope": scope,
                "column": column["name"],
                "left": column,
                "right": None,
            }
        )
    for column in added:
        target = breaking_changes if added_is_breaking else non_breaking_changes
        target.append(
            {
                "kind": "column_added",
                "scope": scope,
                "column": column["name"],
                "left": None,
                "right": column,
            }
        )

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "unchanged": unchanged,
        "breaking_changes": breaking_changes,
        "non_breaking_changes": non_breaking_changes,
    }


def column_payload(column: 'Any') -> 'dict[str, Any]':
    return {
        "name": column.name,
        "type": column.type,
        "comment": column.comment,
    }


def resolve_data_diff_columns(
    left: 'TableDefinition',
    right: 'TableDefinition',
    *,
    keys: 'list[str]',
    requested_columns: 'list[str] | None',
) -> 'list[str]':
    left_columns = all_table_column_names(left)
    right_columns = all_table_column_names(right)
    deduped_keys = dedupe_preserve_order(keys)
    missing_keys = [
        key
        for key in deduped_keys
        if key not in left_columns or key not in right_columns
    ]
    if missing_keys:
        raise ValidationError(
            f"Data diff key columns are incomplete across both tables: {', '.join(missing_keys)}",
            suggestion="Run `maxc meta describe` or `maxc diff schema` to inspect the available columns.",
        )

    if requested_columns:
        compared_columns = [
            column
            for column in dedupe_preserve_order(requested_columns)
            if column not in deduped_keys
        ]
        missing_columns = [
            column
            for column in compared_columns
            if column not in left_columns or column not in right_columns
        ]
        if missing_columns:
            raise ValidationError(
                f"Data diff comparison columns are incomplete across both tables: {', '.join(missing_columns)}",
                suggestion="Run `maxc meta describe` or `maxc diff schema` to inspect the available columns.",
            )
        return compared_columns

    return [
        column
        for column in left_columns
        if column in right_columns and column not in deduped_keys
    ]


def all_table_column_names(table: 'TableDefinition') -> 'list[str]':
    return [column.name for column in [*table.columns, *table.partition_columns]]


def dedupe_preserve_order(values: 'list[str]') -> 'list[str]':
    deduped: 'list[str]' = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped


def index_rows_by_key(
    rows: 'list[dict[str, Any]]',
    *,
    keys: 'list[str]',
    table_name: 'str',
) -> 'dict[tuple[Any, ...], dict[str, Any]]':
    indexed: 'dict[tuple[Any, ...], dict[str, Any]]' = {}
    duplicate_keys: 'list[dict[str, Any]]' = []
    for row in rows:
        key = tuple(normalize_diff_value(row.get(column)) for column in keys)
        if key in indexed:
            duplicate_keys.append(key_to_payload(keys, key))
            continue
        indexed[key] = row
    if duplicate_keys:
        examples = ", ".join(str(item) for item in duplicate_keys[:3])
        raise ValidationError(
            f"Data diff found duplicate keys in the `{table_name}` snapshot and cannot align rows reliably: {examples}",
            suggestion="Use more selective `--keys`, or narrow the partition scope before retrying.",
        )
    return indexed


def key_to_payload(columns: 'list[str]', values: 'tuple[Any, ...]') -> 'dict[str, Any]':
    return {
        column: value
        for column, value in zip(columns, values)
    }


def normalize_diff_value(value: 'Any') -> 'Any':
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return value
    return value


def read_stdin() -> 'str':
    return sys.stdin.read()
