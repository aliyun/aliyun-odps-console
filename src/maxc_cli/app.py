
import getpass
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable

from . import __version__
from . import catalog_bootstrap as _catalog_bootstrap
from .audit import AuditLogger
from .auth_providers import (
    ResolvedAuthConnection,
    build_auth_options,
    resolve_auth_connection,
)
from .backend import OdpsBackend
from .cache import LocalCache
from .config import (
    AuthConfig,
    ExternalAuthConfig,
    TableDefinition,
    default_global_config_path,
    load_config,
    load_config_mapping,
    persist_login_config,
    save_config_mapping,
)
from .exceptions import (
    BackendConnectionError,
    CostLimitExceededError,
    ErrorPayload,
    FeatureUnavailableError,
    JobTimeoutError,
    MaxCError,
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
from .masking import mask_rows
from .models import (
    AgentHints,
    Envelope,
    JobInfo,
    QueryResult,
    SuggestedAction,
    action,
    build_safety_block,
)
from .store import JobStore
from .utils import (
    decode_cursor,
    encode_cursor,
    now_utc_iso,
    sql_has_limit,
)

_SKILL_IF_BLOCK = re.compile(
    r"<!--\s*@if\s+(\w[\w\s]*?)\s*-->(.*?)<!--\s*@endif\s*-->",
    flags=re.DOTALL,
)
_SKILL_BLANK_RUN = re.compile(r"\n{3,}")


def render_skill_template(content: 'str', *, cli: 'str', cli_module: 'str') -> 'str':
    """Render a SKILL template, evaluating conditional blocks and placeholders.

    Skill source files use two layers of customization:

    * ``{{cli}}`` and ``{{cli_module}}`` placeholders, substituted with the
      target invocation's command strings.
    * ``<!-- @if cli_module_differs -->...<!-- @endif -->`` blocks, kept when
      ``cli`` and ``cli_module`` resolve to different strings, dropped when
      they collapse to the same string (e.g., ``aliyun-maxc`` invocation
      where there is no separate module form). Unknown conditions are kept
      verbatim — the test suite enforces no leftover markers.
    """
    cli_module_differs = cli != cli_module

    def _eval(condition: 'str') -> 'bool':
        condition = condition.strip()
        if condition == "cli_module_differs":
            return cli_module_differs
        if condition == "not cli_module_differs":
            return not cli_module_differs
        return True

    def _sub(match: 're.Match') -> 'str':
        return match.group(2) if _eval(match.group(1)) else ""

    content = _SKILL_IF_BLOCK.sub(_sub, content)
    content = _SKILL_BLANK_RUN.sub("\n\n", content)
    return content.replace("{{cli}}", cli).replace("{{cli_module}}", cli_module)


@dataclass
class _PickerInputs:
    """Bundled inputs for ``MaxCApp._resolve_project_via_picker``.

    Grouping these keeps the helper's signature stable as new flags are
    added (e.g. Task 7's ``--no-picker`` argparse wiring) and makes the
    call site in ``auth_login`` readable.
    """
    provided_project: 'str | None'
    provided_endpoint: 'str | None'
    provided_region: 'str | None'
    provided_tunnel: 'str | None'
    access_id: 'str | None'
    secret: 'str | None'
    security_token: 'str | None'
    catalog_endpoint: 'str | None'
    no_picker: 'bool'
    from_env: 'bool'
    env_settings: 'dict[str, str]'
    existing_auth: 'AuthConfig'
    reselect: 'bool' = False


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
        self._cache: LocalCache | None = None
        self.backend = OdpsBackend(self.config, cache=self.cache) if load_backend else None
        self.remote_jobs = getattr(self.backend, "supports_remote_jobs", False) if self.backend else False
        self.jobs: JobStore | None = None
        self._audit: AuditLogger | None = None
        self._audit_path = self.config.agent.audit_log or self.config.state_dir / "audit.log"

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
                actions=[
                    action("auth.login", data=payload, metadata={"project": self.config.default_project}),
                    action("auth.login-external", data=payload, metadata={"project": self.config.default_project}),
                ],
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

    def _find_shadowing_sources(
        self, target_path: 'Path', keys: 'list[str]'
    ) -> 'list[tuple[str, str]]':
        """Return (source_path, key) pairs that override ``target_path`` for the
        given keys.

        Walks ``self.config.sources`` (the chain that was actually loaded for this
        invocation), looking at sources that have higher precedence than
        ``target_path``. If a source defines one of ``keys``, it wins over the
        edit we just made to ``target_path`` and the user should be told.
        """
        from .config import _load_yaml_file

        target_resolved = target_path.resolve()
        try:
            target_index = self.config.sources.index(target_resolved)
        except ValueError:
            # Target wasn't loaded this invocation — typically because we just
            # created it (e.g. `session set` writing to `~/.maxc/config.yaml`).
            # Since the user-level file is the lowest-priority slot, every
            # already-loaded source shadows it.
            target_index = -1
        result: list[tuple[str, str]] = []
        for src in self.config.sources[target_index + 1:]:
            if src == target_resolved:
                continue
            payload = _load_yaml_file(src)
            for key in keys:
                if payload.get(key) is not None:
                    result.append((str(src), key))
        return result

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
        metadata: dict[str, Any] = {
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
        wait: 'int' = 10,
        cost_check: 'float | None' = None,
        idempotency_key: 'str | None' = None,
        retry_on: 'list[str] | None' = None,
        max_retries: 'int' = 0,
        force: 'bool' = False,
    ) -> 'Envelope':
        if max_rows <= 0:
            raise ValidationError("`--max-rows` and `--page-size` must be greater than 0.")
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
                    force=force,
                    session_id=session_id,
                )
                self.log(command, envelope.status, envelope.metadata)
                return envelope

        # Remote branch — always submit, then poll up to `wait` seconds
        if self.remote_jobs and not dry_run:
            job = self._submit_remote_job(
                sql=sql,
                project=target_project,
                cost_check=cost_check,
                idempotency_key=idempotency_key,
                force=force,
            )
            retry_warnings = []
            if retry_on:
                retry_warnings = ["`--retry-on` and `--max-retries` are not applied on the remote job path; the job runs as submitted."]
            if wait == 0:
                # Return pending envelope immediately, no polling
                envelope = Envelope(
                    command=command,
                    status="pending",
                    data={
                        "job_id": job.job_id,
                        "safety": build_safety_block(force=force, sql=sql),
                    },
                    metadata={
                        "job_id": job.job_id,
                        "project": job.project,
                        "submitted_at": job.submitted_at,
                        "logview": job.logview,
                        "wait_seconds": 0,
                        "sql_executed": sql,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.wait", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project, "sql_executed": sql}),
                            action("job.status", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project}),
                        ],
                        warnings=(job.warnings or []) + retry_warnings,
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            # Poll
            try:
                job_info = self.backend.wait_job(
                    job.job_id,
                    project=target_project,
                    timeout=wait,
                    poll_interval=1,
                )
            except JobTimeoutError:
                envelope = Envelope(
                    command=command,
                    status="pending",
                    data={
                        "job_id": job.job_id,
                        "safety": build_safety_block(force=force, sql=sql),
                    },
                    metadata={
                        "job_id": job.job_id,
                        "project": job.project,
                        "submitted_at": job.submitted_at,
                        "logview": job.logview,
                        "wait_seconds": wait,
                        "sql_executed": sql,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.wait", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project, "sql_executed": sql}),
                            action("job.status", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project}),
                        ],
                        warnings=(job.warnings or []) + retry_warnings,
                        insights=[f"Query promoted to async after {wait}s."],
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            except BackendConnectionError as exc:
                envelope = Envelope(
                    command=command,
                    status="failure",
                    data=None,
                    error=exc.to_payload(),
                    metadata={
                        "job_id": job.job_id,
                        "project": target_project,
                        "sql_executed": sql,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.status", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": target_project, "sql_executed": sql}),
                            action("job.diagnose", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": target_project}),
                        ],
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            # Job ended — check outcome
            if job_info.status == "failure":
                error_msg = job_info.failure_reason or job_info.error_message or "Job failed"
                envelope = Envelope(
                    command=command,
                    status="failure",
                    data={"job_id": job_info.job_id},
                    metadata={
                        "job_id": job_info.job_id,
                        "project": job_info.project,
                        "submitted_at": job_info.submitted_at,
                        "logview": job_info.logview,
                        "sql_executed": sql,
                    },
                    error=ErrorPayload(
                        code="EXECUTION_FAILED",
                        message=error_msg,
                        suggestion=None,
                        recoverable=False,
                    ),
                    agent_hints=AgentHints(
                        actions=[
                            action("job.diagnose", data={"job_id": job_info.job_id}, metadata={"job_id": job_info.job_id, "project": job_info.project, "sql_executed": sql}),
                            action("job.status", data={"job_id": job_info.job_id}, metadata={"job_id": job_info.job_id, "project": job_info.project}),
                        ],
                        warnings=job_info.warnings or [],
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            # status == "success" — fetch rows
            try:
                result = self.backend.fetch_job_result(
                    job_info.job_id,
                    project=target_project,
                    max_rows=max_rows,
                    offset=offset,
                )
            except Exception as exc:
                fetch_err = MaxCError(str(exc))
                envelope = Envelope(
                    command=command,
                    status="failure",
                    data=None,
                    error=fetch_err.to_payload(),
                    metadata={
                        "job_id": job_info.job_id,
                        "project": target_project,
                        "sql_executed": sql,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.result", data={"job_id": job_info.job_id}, metadata={"job_id": job_info.job_id, "project": target_project, "sql_executed": sql}),
                            action("job.status", data={"job_id": job_info.job_id}, metadata={"job_id": job_info.job_id, "project": target_project}),
                        ],
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            envelope = self._build_query_envelope(
                command=command,
                result=result,
                dry_run=False,
                force=force,
            )
            envelope.metadata.update({
                "job_id": job_info.job_id,
                "submitted_at": job_info.submitted_at,
                "logview": job_info.logview,
            })
            if idempotency_key:
                envelope.metadata["idempotency_key"] = idempotency_key
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
            force=force,
        )

        envelope = self._build_query_envelope(
            command=command,
            result=result,
            dry_run=dry_run,
            force=force,
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
        force: 'bool' = False,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        analysis = self._analyze_query(
            sql=sql,
            project=target_project,
            explain=False,
            force=force,
        )
        envelope = self._build_analysis_envelope(
            command=command,
            sql=sql,
            analysis=analysis,
            force=force,
        )
        self.log(command, envelope.status, envelope.metadata)
        return envelope

    def query_explain(
        self,
        *,
        sql: 'str',
        project: 'str | None' = None,
        command: 'str' = "query.explain",
        force: 'bool' = False,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        analysis = self._analyze_query(
            sql=sql,
            project=target_project,
            explain=True,
            force=force,
        )
        envelope = self._build_analysis_envelope(
            command=command,
            sql=sql,
            analysis=analysis,
            force=force,
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
        force: 'bool' = False,
    ) -> 'Envelope':
        return self.query(
            command="job.submit",
            sql=sql,
            project=project,
            max_rows=max_rows,
            wait=0,
            cost_check=cost_check,
            idempotency_key=idempotency_key,
            force=force,
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
            try:
                after = self.backend.wait_job(job_id, project=self.config.default_project, timeout=timeout)
            except JobTimeoutError:
                envelope = Envelope(
                    command="job.wait",
                    status="pending",
                    data={"job_id": job_id},
                    metadata={
                        "job_id": job_id,
                        "project": self.config.default_project,
                        "submitted_at": before.submitted_at,
                        "logview": before.logview,
                        "wait_seconds": timeout,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.wait", data={"job_id": job_id}, metadata={"job_id": job_id, "project": self.config.default_project}),
                            action("job.status", data={"job_id": job_id}, metadata={"job_id": job_id, "project": self.config.default_project}),
                        ],
                        insights=[f"Job still running after {timeout}s."],
                    ),
                )
                self.log("job.wait", envelope.status, envelope.metadata)
                return envelope, []
            except BackendConnectionError as exc:
                envelope = Envelope(
                    command="job.wait",
                    status="failure",
                    data=None,
                    error=ErrorPayload(
                        code="BACKEND_CONNECTION_ERROR",
                        message=str(exc),
                        recoverable=True,
                        suggestion=getattr(exc, "suggestion", None),
                    ),
                    metadata={
                        "job_id": job_id,
                        "project": self.config.default_project,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.status", data={"job_id": job_id}, metadata={"job_id": job_id, "project": self.config.default_project}),
                        ],
                        warnings=[f"Lost contact with backend while waiting for job {job_id}."],
                    ),
                )
                self.log("job.wait", envelope.status, envelope.metadata)
                return envelope, []
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
                actions=[
                    action("job.result", data={"job_id": job_id}, metadata={"job_id": job_id, "project": info.project}),
                    action("meta.describe", data=stored["data"], metadata=stored["metadata"]),
                ],
                warnings=stored.get("agent_hints", {}).get("warnings", []),
            ),
        )
        self.log("job.wait", envelope.status, envelope.metadata)
        return envelope, events

    def job_result(self, job_id: 'str', *, max_rows: 'int' = 100, cursor: 'str | None' = None) -> 'Envelope':
        if self.remote_jobs:
            info = self.backend.get_job(job_id, project=self.config.default_project)
            if info.status != "success":
                envelope = self._job_info_envelope("job.result", info)
                self.log("job.result", envelope.status, envelope.metadata)
                return envelope
            offset, _ = decode_cursor(cursor)
            result = self.backend.fetch_job_result(
                job_id,
                project=self.config.default_project,
                max_rows=max_rows,
                offset=offset,
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
        all_rows = stored["data"].get("rows", [])
        schema = stored["data"].get("schema", [])
        total_rows = stored["data"].get("total_rows", len(all_rows))

        offset, _ = decode_cursor(cursor)  # session_id ignored for local jobs
        page_rows = all_rows[offset:offset + max_rows]
        returned_rows = len(page_rows)
        has_more = (offset + returned_rows) < total_rows
        next_cursor = encode_cursor(offset + returned_rows) if has_more else None

        # Sensitive field masking
        local_warnings = list(stored.get("agent_hints", {}).get("warnings", []))
        if self.config.masking_enabled and page_rows:
            page_rows, masked_columns = mask_rows(
                page_rows, schema,
                extra_sensitive_columns=self.config.sensitive_columns or None,
            )
            if masked_columns:
                local_warnings.append(f"Sensitive columns masked: {', '.join(masked_columns)}")

        envelope = Envelope(
            command="job.result",
            status="success",
            data={
                "rows": page_rows,
                "schema": schema,
                "total_rows": total_rows,
                "returned_rows": returned_rows,
                "has_more": has_more,
                "next_cursor": next_cursor,
            },
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
                actions=[
                    action("meta.describe", data={"table_name": None}, metadata=stored["metadata"]),
                ],
                warnings=local_warnings,
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
                    actions=[
                        action("job.status", data={"job_id": job_id}, metadata={"project": info.project}),
                    ],
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
            agent_hints=AgentHints(
                actions=[
                    action("job.status", data={"job_id": job_id}, metadata={"project": updated["project"]}),
                ],
            ),
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
                agent_hints=AgentHints(
                    actions=[
                        action("job.status", data={"job_id": job_id}, metadata={"project": self.config.default_project}),
                        action("job.result", data={"job_id": job_id}, metadata={"project": self.config.default_project}),
                    ],
                ),
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
            agent_hints=AgentHints(
                actions=[
                    action("job.status", data={"job_id": info.job_id}, metadata={"project": info.project}),
                    action("job.result", data={"job_id": info.job_id}, metadata={"project": info.project}),
                ],
            ),
        )
        self.log("job.diagnose", envelope.status, envelope.metadata)
        return envelope

    def list_jobs(self, *, limit: 'int' = 20) -> 'Envelope':
        if self.remote_jobs:
            jobs, has_more = self.backend.list_jobs(
                project=self.config.default_project, limit=limit
            )
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
                data={"jobs": rows, "total": len(rows), "has_more": has_more},
                metadata={"backend": "odps", "project": self.config.default_project},
                agent_hints=AgentHints(
                    actions=[
                        action("job.status", data={}, metadata={"project": self.config.default_project}),
                        action("job.wait", data={}, metadata={"project": self.config.default_project}),
                    ],
                ),
            )
            self.log("job.list", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        all_stored_jobs = jobs.list_jobs()
        stored_jobs = all_stored_jobs[:limit]
        has_more = len(all_stored_jobs) > limit
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
            data={"jobs": rows, "total": len(rows), "has_more": has_more},
            metadata={"state_file": str(jobs.path)},
            agent_hints=AgentHints(
                actions=[
                    action("job.status", data={}, metadata={"state_file": str(jobs.path)}),
                    action("job.wait", data={}, metadata={"state_file": str(jobs.path)}),
                ],
            ),
        )
        self.log("job.list", envelope.status, envelope.metadata)
        return envelope

    def meta_list_tables(
        self,
        *,
        schema: 'str | None' = None,
        project: 'str | None' = None,
        limit: 'int | None' = None,
        cursor: 'str | None' = None,
    ) -> 'Envelope':
        started = monotonic()
        target_project = project or self.config.default_project
        effective_schema = schema or self.config.default_schema

        # Decode cursor (offset token, mirrors cli.py pagination scheme)
        offset = 0
        if cursor:
            try:
                offset = max(0, int(cursor))
            except (TypeError, ValueError):
                raise ValidationError(
                    f"Invalid --cursor value: {cursor!r}",
                    suggestion="Pass the `next_cursor` value returned by the previous call.",
                )

        # Try to get from cache first (cache pagination is in-memory slicing)
        cached_tables = self.cache.get_all_cached_tables(
            target_project,
            schema_name=effective_schema,
        )

        has_more = False
        next_cursor: str | None = None

        if cached_tables:
            # Use cached data (returns list of dicts)
            window = cached_tables[offset:]
            if limit is not None:
                has_more = len(window) > limit
                window = window[:limit]
            tables = window
            source = "cache"
            rows = [
                {
                    "table_name": table.get("table_name"),
                    "schema_name": effective_schema or table.get("schema_name", "default"),
                    "table_type": table.get("table_type", "TABLE"),
                    "description": table.get("description"),
                    "partition_columns": [
                        c.get("name") if isinstance(c, dict) else str(c)
                        for c in table.get("partition_columns", [])
                    ],
                }
                for table in tables
            ]
        else:
            # Cache miss — fall back to live backend query (now paginated)
            live_tables, has_more = self.backend.list_tables(
                schema=effective_schema,
                project=project,
                limit=limit,
                offset=offset,
            )
            source = "backend"
            rows = [
                {
                    "table_name": t.name,
                    "schema_name": effective_schema or "default",
                    "table_type": t.table_type or "TABLE",
                    "description": t.description,
                    "partition_columns": [c.name for c in (t.partition_columns or [])],
                }
                for t in live_tables
            ]

        if has_more and limit is not None:
            next_cursor = str(offset + limit)

        metadata = self._cache_metadata(
            project=target_project,
            source=source,
            query_time_ms=int((monotonic() - started) * 1000),
        )

        schema_label = effective_schema or "default"
        insights = [f"Table list served from {source}."]
        if effective_schema and effective_schema != "default":
            insights.append(f"Use schema-qualified names in SQL: `{schema_label}.<table_name>`")

        data = {
            "tables": rows,
            "total": len(rows),
            "schema": schema_label,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "limit": limit,
            "offset": offset,
        }
        envelope = Envelope(
            command="meta.list-tables",
            status="success",
            data=data,
            metadata=metadata,
            agent_hints=AgentHints(
                actions=[
                    action("meta.describe", data=data, metadata=metadata),
                    action("data.sample", data=data, metadata=metadata),
                ],
                insights=insights,
            ),
        )
        self.log("meta.list-tables", envelope.status, envelope.metadata)
        return envelope

    def meta_describe(
        self,
        table_name: 'str',
        full: 'bool' = False,
        project: 'str | None' = None,
        *,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        started = monotonic()
        target_project = project or self.config.default_project
        effective_schema = schema or self.config.default_schema or "default"

        # Try to get from cache first
        cached_table = self.cache.get_cached_table(
            target_project,
            table_name,
            schema_name=effective_schema,
        )

        if cached_table:
            # Use cached metadata for schema, fetch sample rows from API
            from .config import TableColumn, TableDefinition

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

            warnings = []
            # Optionally fetch additional metadata from API (description, owner, size, sample rows, partitions)
            try:
                api_table = self.backend.describe_table(
                    table_name, project=project, schema=schema,
                )
                # Update with API data (API has priority over cache for these fields)
                table.description = api_table.description or table.description
                table.owner = api_table.owner or table.owner
                table.size_bytes = api_table.size_bytes or table.size_bytes
                table.created_at = api_table.created_at
                table.updated_at = api_table.updated_at
                table.table_type = api_table.table_type or table.table_type
                table.sample_rows = api_table.sample_rows
                table.partitions = api_table.partitions
                # The cache writes partition column *names* with no type info, so
                # the live API is the only source of truth for partition_columns.
                if api_table.partition_columns:
                    table.partition_columns = api_table.partition_columns
            except Exception:
                # If API fails, still return cached schema
                warnings.append("Backend API unavailable, showing cached schema only")
        else:
            # Fall back to live API
            table = self.backend.describe_table(
                table_name, project=project, schema=schema,
            )
            source = "live"
            warnings = []

        # Get semantic metadata from cache
        semantic = self.cache.get_semantic(
            project=target_project,
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
            warnings.append(
                f"Showing first 10 columns only. Use --full to see all {payload['column_count']} columns."
            )
        
        # Add semantic information to the payload
        payload["semantic"] = semantic

        meta_metadata = {
                "project": target_project,
                "source": source,
                "query_time_ms": int((monotonic() - started) * 1000) if source == "live" else None,
            }
        envelope = Envelope(
            command="meta.describe",
            status="success",
            data=payload,
            metadata=meta_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("data.sample", data=payload, metadata=meta_metadata),
                    action("data.profile", data=payload, metadata=meta_metadata),
                    action("query", data=payload, metadata=meta_metadata),
                ],
                warnings=warnings,
            ),
        )
        self.log("meta.describe", envelope.status, envelope.metadata)
        return envelope

    def meta_search(
        self,
        keyword: 'str',
        *,
        schema: 'str | None' = None,
        project: 'str | None' = None,
        limit: 'int | None' = None,
    ) -> 'Envelope':
        started = monotonic()
        target_project = project or self.config.default_project
        effective_schema = schema or self.config.default_schema

        # Priority: Catalog API → cache → live scan
        matches: list[dict[str, Any]] = []
        source = "live"
        catalog_available = False

        # Empty keyword is not a search — skip Catalog API (which would
        # return a random page of tables) and go straight to list-tables.
        use_catalog = bool(keyword and keyword.strip())

        if use_catalog and self.backend is not None:
            catalog_matches = self.backend.catalog_search_tables(
                keyword, schema=effective_schema,
            )
            if catalog_matches is not None:
                matches = catalog_matches
                source = "catalog"
                catalog_available = True

        if not catalog_available:
            cached_tables = self.cache.get_all_cached_tables(
                target_project, schema_name=effective_schema,
            )
            if cached_tables:
                matches = self._search_in_cache(keyword, cached_tables)
                source = "cache"
            else:
                matches = self.backend.search_tables(keyword, schema=effective_schema, project=project)
                source = "live"

        original_total = len(matches)
        truncated = False
        if limit is not None and len(matches) > limit:
            matches = matches[:limit]
            truncated = True

        search_data = {
            "keyword": keyword,
            "matches": matches,
            "total": original_total,
            "has_more": truncated,
            "limit": limit,
            "truncated": truncated,
        }
        search_metadata = self._cache_metadata(
                project=target_project,
                source=source,
                query_time_ms=int((monotonic() - started) * 1000) if source in ("live", "catalog") else None,
            )
        envelope = Envelope(
            command="meta.search",
            status="success",
            data=search_data,
            metadata=search_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("meta.describe", data=search_data, metadata=search_metadata),
                    action("data.sample", data=search_data, metadata=search_metadata),
                ],
                warnings=[] if source == "catalog" or cached_tables else ["No metadata cache was used. Run `maxc cache build` to speed up future lookups."],
            ),
        )
        self.log("meta.search", envelope.status, envelope.metadata)
        return envelope

    def meta_search_columns(
        self,
        keyword: 'str',
        *,
        schema: 'str | None' = None,
        project: 'str | None' = None,
        limit: 'int | None' = None,
    ) -> 'Envelope':
        started = monotonic()
        target_project = project or self.config.default_project
        effective_schema = schema or self.config.default_schema
        cached_tables = self.cache.get_all_cached_tables(
            target_project, schema_name=effective_schema,
        )
        if cached_tables:
            matches = self._search_columns_in_cache(keyword, cached_tables)
            source = "cache"
            warnings: list[str] = []
        else:
            # search-columns without cache iterates all tables client-side,
            # which is extremely slow (N API calls for N tables).  Return
            # empty results with a strong warning instead of silently
            # timing out or returning partial results.
            matches = []
            source = "cache_required"
            warnings = [
                "Column search requires a metadata cache. "
                "Run `maxc cache build` first, then retry `maxc meta search-columns`.",
            ]

        original_total = len(matches)
        truncated = False
        if limit is not None and len(matches) > limit:
            matches = matches[:limit]
            truncated = True

        sc_data = {
            "keyword": keyword,
            "matches": matches,
            "total": original_total,
            "has_more": truncated,
            "limit": limit,
            "truncated": truncated,
        }
        sc_metadata = self._cache_metadata(
                project=target_project,
                source=source,
                query_time_ms=int((monotonic() - started) * 1000) if source not in ("cache", "cache_required") else None,
            )
        envelope = Envelope(
            command="meta.search-columns",
            status="success",
            data=sc_data,
            metadata=sc_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("meta.describe", data=sc_data, metadata=sc_metadata),
                    action("meta.search", data=sc_data, metadata=sc_metadata),
                ],
                warnings=warnings,
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
                    actions=[
                        action("meta.describe", data={"table_name": table_name}, metadata={"project": self.config.default_project}),
                    ],
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
                agent_hints=None,
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
                    agent_hints=AgentHints(
                        actions=[
                            action("meta.describe", data={"table_name": table_name}, metadata={"project": self.config.default_project}),
                            action("meta.semantic.set", data={"table_name": table_name}, metadata={"project": self.config.default_project}),
                        ],
                    ),
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
                        actions=[
                            action("meta.semantic.set", data={"table_name": table_name}, metadata={"project": self.config.default_project}),
                        ],
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
                agent_hints=None,
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

            warnings: list[str] = []
            if len(all_tables) == 0:
                warnings.append(
                    "Cache is empty — no tables to analyze. Run "
                    "`maxc cache build` first to populate metadata."
                )

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
                    insights=[f"{len(missing)} tables lack semantic metadata."],
                    warnings=warnings,
                    actions=[
                        action("meta.semantic.set", data={"table_name": missing[0]["table_name"]}, metadata={"project": self.config.default_project})
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
                agent_hints=None,
            )

        self.log("meta.semantic.list-missing", envelope.status, envelope.metadata)
        return envelope

    def meta_latest_partition(
        self,
        table_name: 'str',
        project: 'str | None' = None,
        *,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        payload, warnings = self.backend.latest_partition_info(
            table_name, project=project, schema=schema,
        )
        lp_metadata = {"project": target_project}
        if payload.get("has_partitions"):
            lp_actions = [
                action("meta.freshness", data=payload, metadata=lp_metadata),
                action("data.sample", data=payload, metadata=lp_metadata),
                action("query", data=payload, metadata=lp_metadata),
            ]
        else:
            lp_actions = [
                action("meta.describe", data=payload, metadata=lp_metadata),
                action("data.sample", data=payload, metadata=lp_metadata),
            ]
        envelope = Envelope(
            command="meta.latest-partition",
            status="success",
            data=payload,
            metadata=lp_metadata,
            agent_hints=AgentHints(actions=lp_actions, warnings=warnings),
        )
        self.log("meta.latest-partition", envelope.status, envelope.metadata)
        return envelope

    def meta_freshness(
        self,
        table_name: 'str',
        project: 'str | None' = None,
        *,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        payload, warnings = self.backend.freshness_info(
            table_name, project=project, schema=schema,
        )
        fresh_metadata = {"project": target_project}
        fresh_actions = []
        if payload.get("freshness_status") == "stale":
            fresh_actions.append(action("job.submit", data=payload, metadata=fresh_metadata))
        fresh_actions.extend([
            action("meta.latest-partition", data=payload, metadata=fresh_metadata),
            action("data.sample", data=payload, metadata=fresh_metadata),
            action("query", data=payload, metadata=fresh_metadata),
        ])
        envelope = Envelope(
            command="meta.freshness",
            status="success",
            data=payload,
            metadata=fresh_metadata,
            agent_hints=AgentHints(actions=fresh_actions, warnings=warnings),
        )
        self.log("meta.freshness", envelope.status, envelope.metadata)
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

        all_tables, _ = self.backend.list_tables(schema=schema_name)
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
                    actions=[
                        action("cache.build-status", data={"build_id": build_id}, metadata={"project": target_project, "build_id": build_id}),
                    ],
                    insights=["The metadata cache build is running in the background."],
                ),
            )
            import threading
            # daemon=False so the parent process waits for the build before
            # exiting. The envelope above is already flushed to stdout so a
            # wrapping script gets the build_id immediately; the user's shell
            # blocks until the build completes — which is the only way to
            # avoid silently dropping the build when `cache build --async`
            # is invoked from a short-lived CLI invocation. True
            # fire-and-forget would require a detached subprocess, which is
            # tracked separately.
            thread = threading.Thread(
                target=self._build_cache_background,
                args=(target_project, build_id, tables, max_workers, schema_name, False),
                daemon=False,
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
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        started = monotonic()
        cached_count = 0
        created_count = 0
        updated_count = 0
        errors: list[str] = []
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
                actions=[
                    action("meta.search", data={}, metadata={"project": project}),
                    action("meta.search-columns", data={}, metadata={"project": project}),
                ],
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
            bs_metadata = {"project": target_project}
            if status["status"] in ["failed", "completed"]:
                bs_actions = [action("cache.build", data=status, metadata=bs_metadata)]
            else:
                bs_actions = [action("cache.build-status", data=status, metadata=bs_metadata)]
            envelope = Envelope(
                command="cache.build-status",
                status="success",
                data=status,
                metadata=bs_metadata,
                agent_hints=AgentHints(
                    actions=bs_actions,
                    insights=[
                        f"Build progress: {status['progress_percent']}% ({status['processed_tables']}/{status['total_tables']})"
                    ]
                    if status["status"] == "running"
                    else [],
                ),
            )
        else:
            bs_metadata = {"project": target_project}
            envelope = Envelope(
                command="cache.build-status",
                status="not_found",
                data={"message": "No cache build record was found."},
                metadata=bs_metadata,
                agent_hints=AgentHints(
                    actions=[action("cache.build", data={}, metadata=bs_metadata)],
                ),
            )
        return envelope

    def cache_status(self, *, project: 'str | None' = None, schema_name: 'str | None' = None) -> 'Envelope':
        """Get cache status."""
        target_project = project or self.config.default_project
        stats = self.cache.get_cache_stats(target_project, schema_name)
        schemas = self.cache.get_schemas(target_project)

        cs_data = {
                **stats,
                "schemas": schemas,
            }
        cs_metadata = {"project": target_project}
        if stats["table_count"] == 0:
            cs_actions = [action("cache.build", data=cs_data, metadata=cs_metadata)]
        else:
            cs_actions = [action("meta.search", data=cs_data, metadata=cs_metadata)]
        envelope = Envelope(
            command="cache.status",
            status="success",
            data=cs_data,
            metadata=cs_metadata,
            agent_hints=AgentHints(
                actions=cs_actions,
            ),
        )
        return envelope

    def cache_clear(
        self,
        *,
        project: 'str | None' = None,
        schema_name: 'str | None' = None,
        force: 'bool' = False,
        dry_run: 'bool' = False,
    ) -> 'Envelope':
        """Clear metadata cache.

        Default behavior is dry-run: count the cached entries that would be
        cleared, return them as ``would_delete`` and a warning, and do not
        touch the cache. Pass ``force=True`` to actually delete. Passing
        ``dry_run=True`` is equivalent to the default but makes intent
        explicit (and remains a dry-run even if the user also passes
        ``force=True``).
        """
        target_project = project or self.config.default_project
        cache_stats = self.cache.get_cache_stats(target_project, schema_name)
        table_count = int(cache_stats.get("table_count", 0))
        cc_metadata = {"project": target_project}
        scope = f"project `{target_project}`"
        if schema_name:
            scope += f", schema `{schema_name}`"

        if dry_run or not force:
            if dry_run:
                warning = (
                    f"Dry run: {table_count} cached table entries in {scope} would be cleared. "
                    "No changes were made."
                )
            else:
                warning = (
                    f"{table_count} cached table entries in {scope} would be cleared. "
                    "Re-run with `--force` to apply, or `--dry-run` to acknowledge explicitly."
                )
            cc_data = {
                "deleted_tables": 0,
                "would_delete": table_count,
                "dry_run": True,
            }
            actions = []
            if not dry_run and table_count > 0:
                actions.append(action("cache.clear", data=cc_data, metadata=cc_metadata))
            envelope = Envelope(
                command="cache.clear",
                status="success",
                data=cc_data,
                metadata=cc_metadata,
                agent_hints=AgentHints(
                    actions=actions,
                    warnings=[warning],
                ),
            )
            return envelope

        deleted = self.cache.clear_table_cache(target_project, schema_name)
        cc_data = {"deleted_tables": deleted, "dry_run": False}
        envelope = Envelope(
            command="cache.clear",
            status="success",
            data=cc_data,
            metadata=cc_metadata,
            agent_hints=AgentHints(
                actions=[action("cache.build", data=cc_data, metadata=cc_metadata)],
            ),
        )
        return envelope

    def meta_partitions(
        self,
        table_name: 'str',
        project: 'str | None' = None,
        *,
        limit: 'int' = 100,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        payload, warnings = self.backend.list_partitions(
            table_name, limit=limit, project=project, schema=schema,
        )
        mp_metadata = {"project": target_project}
        envelope = Envelope(
            command="meta.partitions",
            status="success",
            data=payload,
            metadata=mp_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("query", data=payload, metadata=mp_metadata),
                    action("meta.latest-partition", data=payload, metadata=mp_metadata),
                ],
                warnings=warnings,
            ),
        )
        self.log("meta.partitions", envelope.status, envelope.metadata)
        return envelope

    def meta_list_projects(self) -> 'Envelope':
        """List all projects owned by the current user."""
        projects = self.backend.list_projects()
        lp_data = {"projects": projects, "total": len(projects)}
        lp_metadata = {"backend": "odps"}
        envelope = Envelope(
            command="meta.list-projects",
            status="success",
            data=lp_data,
            metadata=lp_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("session.set", data=lp_data, metadata=lp_metadata),
                    action("meta.list-schemas", data=lp_data, metadata=lp_metadata),
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
        ls_data = {"schemas": rows, "total": len(rows)}
        ls_metadata = {"project": target_project}
        envelope = Envelope(
            command="meta.list-schemas",
            status="success",
            data=ls_data,
            metadata=ls_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("meta.list-tables", data=ls_data, metadata=ls_metadata),
                    action("meta.search", data=ls_data, metadata=ls_metadata),
                ],
                warnings=[] if rows else ["No schemas were returned. Schema namespaces may not be enabled for this project."],
            ),
        )
        self.log("meta.list-schemas", envelope.status, envelope.metadata)
        return envelope

    def session_set(self, project: 'str | None' = None, schema: 'str | None' = None) -> 'Envelope':
        """Set default project and/or schema by writing to ~/.maxc/config.yaml.

        Mirrors `gcloud config set project` / `kubectl config use-context`: the
        change persists in the global config file. If a higher-precedence config
        (e.g., ./.maxc/config.yaml) shadows the value, a warning is emitted but
        the write still happens — the in-memory value is updated for the current
        invocation.
        """
        target_path = default_global_config_path()
        config_payload = load_config_mapping(target_path) if target_path.exists() else {}

        changes: list[str] = []
        warnings: list[str] = []

        if project:
            if self.backend is not None:
                try:
                    self.backend.get_project_info(project)
                except Exception as exc:
                    raise ValidationError(
                        f"Unable to access project `{project}`: {exc}",
                        suggestion="Verify the project name and that the current identity has access.",
                    ) from exc
            config_payload["default_project"] = project
            changes.append(f"project set to `{project}`")
            if self.config.auth.project and project != self.config.auth.project:
                warnings.append(
                    f"Project (`{project}`) differs from the project saved in auth config "
                    f"(`{self.config.auth.project}`). Operations will use `{project}`, but credentials "
                    f"were configured for `{self.config.auth.project}`. Run `auth whoami` to verify access."
                )

        if schema:
            config_payload["default_schema"] = schema
            changes.append(f"schema set to `{schema}`")
        elif schema is not None:
            config_payload.pop("default_schema", None)
            changes.append("schema cleared")

        save_config_mapping(target_path, config_payload)

        if project:
            self.config.default_project = project
            if self.backend is not None:
                self.backend.project = project
        if schema:
            self.config.default_schema = schema
        elif schema is not None:
            self.config.default_schema = None

        shadowing = self._find_shadowing_sources(
            target_path,
            keys=[k for k, v in [("default_project", project), ("default_schema", schema)] if v],
        )
        for src_path, key in shadowing:
            warnings.append(
                f"`{key}` is also set in `{src_path}` (higher precedence than `{target_path}`); "
                f"that file will continue to shadow this change. Edit it directly or remove the entry."
            )

        ss_data = {
            "project": self.config.default_project,
            "schema": self.config.default_schema,
            "config_path": str(target_path),
            "changes": changes,
        }
        ss_metadata: dict[str, Any] = {}
        envelope = Envelope(
            command="session.set",
            status="success",
            data=ss_data,
            metadata=ss_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("meta.list-tables", data=ss_data, metadata=ss_metadata),
                    action("meta.list-schemas", data=ss_data, metadata=ss_metadata),
                    action("session.show", data=ss_data, metadata=ss_metadata),
                ],
                warnings=warnings,
            ),
        )
        self.log("session.set", envelope.status, {"changes": changes})
        return envelope
    
    def session_unset(self) -> 'Envelope':
        """Remove default_project / default_schema from ~/.maxc/config.yaml.

        Project-level config files in the working directory are NOT modified, since
        they may be checked into version control. Edit those by hand if needed.
        """
        target_path = default_global_config_path()
        cleared: list[str] = []

        if target_path.exists():
            payload = load_config_mapping(target_path)
            for key in ("default_project", "default_schema"):
                if key in payload:
                    payload.pop(key)
                    cleared.append(key)
            if cleared:
                save_config_mapping(target_path, payload)

        su_data = {
            "cleared": cleared,
            "config_path": str(target_path),
        }
        su_metadata: dict[str, Any] = {}
        envelope = Envelope(
            command="session.unset",
            status="success",
            data=su_data,
            metadata=su_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("session.show", data=su_data, metadata=su_metadata),
                    action("session.set", data=su_data, metadata=su_metadata),
                ],
            ),
        )
        self.log("session.unset", envelope.status, {})
        return envelope
    
    def session_show(self) -> 'Envelope':
        """Show current session settings with source information."""
        config_path = default_global_config_path()

        env_project = os.environ.get("MAXCOMPUTE_PROJECT") or os.environ.get("ODPS_PROJECT")
        has_explicit_auth_provider = bool(self.config.auth.provider)
        if env_project and not has_explicit_auth_provider:
            project_source = "environment"
        else:
            project_source = "config_file"
        schema_source = "config_file"

        project_info = None
        project_info_warning = None
        if self.backend is not None:
            try:
                raw_info = self.backend.get_project_info(self.config.default_project)
                project_info = {k: (str(v) if v is not None else None) for k, v in raw_info.items()}
            except Exception:
                project_info_warning = "Could not fetch project info from backend"

        show_data = {
                "project": {
                    "value": self.config.default_project,
                    "source": project_source,
                },
                "schema": {
                    "value": self.config.default_schema,
                    "source": schema_source,
                },
                "config_path": str(config_path) if config_path.exists() else None,
                "project_info": project_info,
                "config_sources": [str(p) for p in self.config.sources],
            }
        show_metadata: dict[str, Any] = {}
        envelope = Envelope(
            command="session.show",
            status="success",
            data=show_data,
            metadata=show_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("session.set", data=show_data, metadata=show_metadata),
                    action("session.unset", data=show_data, metadata=show_metadata),
                    action("meta.list-tables", data=show_data, metadata=show_metadata),
                ],
                warnings=[project_info_warning] if project_info_warning else [],
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
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        if rows <= 0:
            raise ValidationError("`--rows` must be greater than 0.")
        table, sample_rows, sample_info = self.backend.sample_table(
            table_name,
            rows,
            partition=partition,
            columns=columns,
            project=project,
            schema=schema,
        )
        ds_data = {
                "table_name": table.name,
                "rows": sample_rows,
                "returned_rows": len(sample_rows),
                "schema": sample_info["schema"],
                "applied_partition": sample_info["applied_partition"],
                "selected_columns": sample_info["selected_columns"],
            }
        ds_metadata = {
                "project": target_project,
                "requested_rows": rows,
                "requested_partition": partition,
                "requested_columns": columns or [],
            }
        envelope = Envelope(
            command="data.sample",
            status="success",
            data=ds_data,
            metadata=ds_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("data.profile", data=ds_data, metadata=ds_metadata),
                    action("query", data=ds_data, metadata=ds_metadata),
                ],
                warnings=list(sample_info.get("warnings") or []),
            ),
        )
        self.log("data.sample", envelope.status, envelope.metadata)
        return envelope

    def data_profile(
        self,
        table_name: 'str',
        *,
        partition: 'str | None' = None,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        # Take the underlying sample_table call so we can surface the same
        # auto-partition warning that data.sample emits — profile_table
        # otherwise swallows it inside `sample_info`.
        _table, _rows, sample_info = self.backend.sample_table(
            table_name,
            rows=20,
            partition=partition,
            columns=None,
            project=project,
            schema=schema,
        )
        from .helpers import build_profile
        profile = build_profile(
            _table,
            _rows,
            applied_partition=sample_info["applied_partition"],
        )
        dp_metadata = {"project": target_project, "requested_partition": partition}
        envelope = Envelope(
            command="data.profile",
            status="success",
            data=profile,
            metadata=dp_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("query", data=profile, metadata=dp_metadata),
                    action("meta.describe", data=profile, metadata=dp_metadata),
                ],
                warnings=list(sample_info.get("warnings") or []),
            ),
        )
        self.log("data.profile", envelope.status, envelope.metadata)
        return envelope

    def data_upload(
        self,
        table_name: 'str',
        file_path: 'str',
        *,
        partition: 'str | None' = None,
        overwrite: 'bool' = False,
        delimiter: 'str' = ",",
        has_header: 'bool' = True,
        null_marker: 'str' = r"\N",
        block_size: 'int' = 10000,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        target_project = project or self.config.default_project
        result = self.backend.upload_table(
            table_name, file_path,
            partition=partition, overwrite=overwrite,
            delimiter=delimiter, has_header=has_header,
            null_marker=null_marker, block_size=block_size,
            project=project, schema=schema,
        )
        metadata = {
            "project": target_project,
            "requested_partition": partition,
            "delimiter": delimiter,
            "block_size": block_size,
        }
        envelope = Envelope(
            command="data.upload",
            status="success",
            data=result,
            metadata=metadata,
            agent_hints=AgentHints(
                actions=[
                    action("data.sample", data=result, metadata=metadata),
                ],
                warnings=result.get("warnings", []),
            ),
        )
        self.log("data.upload", envelope.status, envelope.metadata)
        return envelope

    def data_download(
        self,
        table_name: 'str',
        output_path: 'str',
        *,
        partition: 'str | None' = None,
        columns: 'list[str] | None' = None,
        limit: 'int | None' = None,
        delimiter: 'str' = ",",
        write_header: 'bool' = True,
        null_marker: 'str' = "",
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        """Download a table or partition to a local CSV/TSV file via Tunnel.

        Args:
            table_name: Table name (schema.table or table).
            output_path: Local file path to write.
            partition: Required when table is partitioned.
            columns: Optional column subset; default = all columns in schema order.
            limit: Optional max rows; default = full partition / table.
            delimiter: Field delimiter (default ",").
            write_header: When False, suppress header row.
            null_marker: Token written for SQL NULL (default empty string).
            project: Target project; default = config's default_project.

        Returns:
            Envelope with table, applied_partition, output_path, rows_written,
            bytes_written, columns, truncated, warnings.
        """
        target_project = project or self.config.default_project
        result = self.backend.download_table(
            table_name, output_path,
            partition=partition, columns=columns, limit=limit,
            delimiter=delimiter, write_header=write_header,
            null_marker=null_marker, project=project, schema=schema,
        )
        metadata = {
            "project": target_project,
            "requested_partition": partition,
            "requested_columns": columns or [],
            "requested_limit": limit,
            "delimiter": delimiter,
        }
        envelope = Envelope(
            command="data.download",
            status="success",
            data=result,
            metadata=metadata,
            agent_hints=AgentHints(
                actions=[],
                warnings=result.get("warnings", []),
            ),
        )
        self.log("data.download", envelope.status, envelope.metadata)
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
        catalog_endpoint: 'str | None' = None,
        no_picker: 'bool' = False,
        reselect: 'bool' = False,
    ) -> 'Envelope':
        target_path = target_config_path or default_global_config_path()
        existing_payload = load_config_mapping(target_path) if target_path.exists() else {}
        existing_auth = AuthConfig.from_mapping(existing_payload.get("auth", {}) or {})
        env_settings = load_odps_env()

        # Resolve credentials first — the picker needs the AK/secret/STS in hand.
        resolved_access_id = self._resolve_login_value(
            provided=access_id,
            env_value=env_settings.get("access_id"),
            existing_value=existing_auth.access_id,
            prompt="Access Key ID",
            required=True,
            secret=False,
            use_env=from_env,
        )
        resolved_secret = self._resolve_login_value(
            provided=secret_access_key,
            env_value=env_settings.get("secret_access_key"),
            existing_value=existing_auth.secret_access_key,
            prompt="Access Key Secret",
            required=True,
            secret=True,
            use_env=from_env,
        )
        resolved_token = self._resolve_login_value(
            provided=security_token,
            env_value=env_settings.get("security_token"),
            existing_value=existing_auth.security_token,
            prompt="STS Security Token (optional)",
            required=False,
            secret=True,
            use_env=from_env,
        )

        # Project / endpoint / region / tunnel — try the interactive Catalog
        # picker when the user did not pin a project explicitly.
        (
            picked_project,
            derived_endpoint,
            derived_region,
            derived_tunnel,
            picker_warnings,
        ) = self._resolve_project_via_picker(
            _PickerInputs(
                provided_project=project,
                provided_endpoint=endpoint,
                provided_region=region_name,
                provided_tunnel=tunnel_endpoint,
                access_id=resolved_access_id,
                secret=resolved_secret,
                security_token=resolved_token,
                catalog_endpoint=catalog_endpoint,
                no_picker=no_picker,
                from_env=from_env,
                env_settings=env_settings,
                existing_auth=existing_auth,
                reselect=reselect,
            )
        )

        resolved_auth = AuthConfig(
            access_id=resolved_access_id,
            secret_access_key=resolved_secret,
            security_token=resolved_token,
            project=picked_project,
            endpoint=self._resolve_login_value(
                provided=derived_endpoint,
                env_value=env_settings.get("endpoint"),
                existing_value=existing_auth.endpoint,
                prompt="MaxCompute Endpoint",
                required=True,
                secret=False,
                use_env=from_env,
            ),
            region_name=self._resolve_login_value(
                provided=derived_region,
                env_value=env_settings.get("region_name"),
                existing_value=existing_auth.region_name,
                prompt="MaxCompute Region (optional)",
                required=False,
                secret=False,
                use_env=from_env,
            ),
            tunnel_endpoint=self._resolve_login_value(
                provided=derived_tunnel,
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

        warnings: list[str] = []
        warnings.extend(picker_warnings)
        # Always remind callers that AK/SK is stored in plaintext YAML (chmod
        # 0600) — flagged in CLAUDE.md as a known limitation. Skip for STS
        # tokens since those are short-lived and self-expiring.
        if not resolved_auth.security_token:
            warnings.append(
                f"AccessKey saved in plaintext at `{target_path}` (file mode 0600). "
                f"For shared/CI environments prefer `auth login-external` with a credential helper, "
                f"or scope the AccessKey to a least-privilege RAM user."
            )
        if from_env:
            warnings.append(
                "Credentials were imported from environment variables (--from-env) and saved to config."
            )
        elif any(
            env_settings.get(name)
            for name in ("access_id", "secret_access_key", "security_token", "endpoint", "region_name", "tunnel_endpoint")
        ):
            warnings.append(
                "Detected MaxCompute environment variables in the current shell; values not passed as flags were sourced from the environment and saved to config."
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

        login_metadata = {
                "config_path": str(target_path),
                "written_fields": sorted(resolved_auth.to_mapping().keys()),
                "auth_storage": "config_file",
            }
        envelope = Envelope(
            command="auth.login",
            status="success",
            data=payload,
            metadata=login_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("auth.whoami", data=payload, metadata=login_metadata),
                    action("meta.list-tables", data=payload, metadata=login_metadata),
                ],
                warnings=warnings,
            ),
        )
        self.log("auth.login", envelope.status, envelope.metadata)
        return envelope

    def auth_login_external(
        self,
        *,
        process_command: 'str',
        process_timeout: 'int' = 60,
        project: 'str | None' = None,
        endpoint: 'str | None' = None,
        region_name: 'str | None' = None,
        tunnel_endpoint: 'str | None' = None,
        no_validate: 'bool' = False,
        target_config_path: 'Path | None' = None,
    ) -> 'Envelope':
        """Save external-process-based login configuration.

        The *process_command* is a shell command that outputs credential
        JSON to stdout.  See :class:`ExternalCredentialProvider` for the
        expected JSON format.
        """
        target_path = target_config_path or default_global_config_path()
        existing_payload = load_config_mapping(target_path) if target_path.exists() else {}
        existing_auth = AuthConfig.from_mapping(existing_payload.get("auth", {}) or {})

        external_cfg = ExternalAuthConfig(
            process_command=process_command,
            process_timeout=min(max(process_timeout, 1), 600),
        )

        # Merge with existing auth
        new_auth = AuthConfig(
            provider="external",
            project=project or existing_auth.project,
            endpoint=endpoint or existing_auth.endpoint,
            region_name=region_name or existing_auth.region_name,
            tunnel_endpoint=tunnel_endpoint or existing_auth.tunnel_endpoint,
            catalog_endpoint=existing_auth.catalog_endpoint,
            ncs=existing_auth.ncs,
            external=external_cfg,
        )

        # Write to config file
        persist_login_config(target_path, auth=new_auth)

        # Validate
        warnings: list[str] = []
        resolved_auth: ResolvedAuthConnection | None = None
        try:
            new_config = load_config(Path.cwd())
            resolved_auth = resolve_auth_connection(new_config, auth_override=new_auth)

            if not no_validate:
                try:
                    from odps import ODPS
                    odps = ODPS(
                        auth=resolved_auth.account,
                        project=resolved_auth.project,
                        endpoint=resolved_auth.endpoint,
                    )
                    _ = odps.project
                except Exception as exc:
                    warnings.append(f"Validation probe failed: {exc}")

        except ValidationError as exc:
            warnings.append(f"Configuration saved but validation failed: {exc.message}")

        env_settings = load_odps_env()
        overriding_env_fields = [
            name for name in ("project", "endpoint")
            if env_settings.get(name)
        ]
        if overriding_env_fields:
            warnings.append(
                f"Environment variable(s) for {', '.join(overriding_env_fields)} are set and will override "
                f"the values you just saved at runtime. Unset them or they will take precedence over this external config."
            )

        if no_validate or resolved_auth is None:
            payload = {
                "authenticated": None,
                "configured": True,
                "validation_status": "configuration_only",
                "backend": "odps",
                "auth_type": "external",
                "identity_source": "config_file",
                "principal_display": None,
                "principal_masked": None,
                "project": new_auth.project,
                "region": new_auth.region_name,
                "endpoint": new_auth.endpoint,
                "process_command": process_command,
            }
        else:
            payload = {
                "authenticated": True,
                "configured": True,
                "validation_status": "verified" if not any("failed" in w for w in warnings) else "validation_failed",
                "backend": "odps",
                "auth_type": "external",
                "identity_source": "config_file",
                "principal_display": mask_access_id(resolved_auth.access_id) or "external-process",
                "principal_masked": mask_access_id(resolved_auth.access_id),
                "project": resolved_auth.project,
                "region": resolved_auth.region_name,
                "endpoint": resolved_auth.endpoint,
                "process_command": process_command,
            }

        ext_metadata = self._cache_metadata(
                project=new_auth.project or self.config.default_project,
                source="config",
            )
        envelope = Envelope(
            command="auth.login-external",
            status="success",
            data=payload,
            metadata=ext_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("auth.whoami", data=payload, metadata=ext_metadata),
                    action("meta.list-tables", data=payload, metadata=ext_metadata),
                ],
                warnings=warnings,
            ),
        )
        self.log("auth.login-external", envelope.status, envelope.metadata)
        return envelope

    def auth_whoami(self) -> 'Envelope':
        if self.backend is None:
            try:
                self.backend = OdpsBackend(self.config, cache=self.cache)
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
        whoami_metadata = {
                "project": self.config.default_project,
                "config_sources": [str(p) for p in self.config.sources],
            }
        envelope = Envelope(
            command="auth.whoami",
            status="success",
            data=payload,
            metadata=whoami_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("auth.can-i", data=payload, metadata=whoami_metadata),
                    action("meta.list-tables", data=payload, metadata=whoami_metadata),
                ],
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
        cani_metadata = {"project": target_project}
        if payload.get("allowed"):
            cani_actions = [
                action("query.cost", data=payload, metadata=cani_metadata),
                action("query.explain", data=payload, metadata=cani_metadata),
            ]
        else:
            cani_actions = [
                action("auth.whoami", data=payload, metadata=cani_metadata),
                action("meta.describe", data=payload, metadata=cani_metadata),
            ]
        envelope = Envelope(
            command="auth.can-i",
            status="success",
            data=payload,
            metadata=cani_metadata,
            agent_hints=AgentHints(actions=cani_actions, warnings=warnings),
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
        unauth_metadata = {
                "project": self.config.default_project,
                "config_sources": [str(p) for p in self.config.sources],
            }
        return Envelope(
            command="auth.whoami",
            status="success",
            data=payload,
            metadata=unauth_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("auth.login", data=payload, metadata=unauth_metadata),
                    action("auth.login-external", data=payload, metadata=unauth_metadata),
                ],
                warnings=(warnings or ["No active MaxCompute credentials are configured."]),
            ),
        )

    def _resolve_project_via_picker(
        self,
        inputs: '_PickerInputs',
    ) -> 'tuple[str | None, str | None, str | None, str | None, list[str]]':
        """Resolve (project, endpoint, region, tunnel, warnings) for auth login.

        Precedence (highest first):
          1. Explicit ``--project`` flag, ``MAXCOMPUTE_PROJECT`` env (only
             when ``--from-env`` was passed — gated like
             ``_resolve_login_value`` to avoid silent re-routing), or value
             already in the target config file. Picker is skipped.
          2. ``no_picker=True`` or non-TTY stdin → reuse the existing
             ``_resolve_login_value`` prompt path (today's behavior).
          3. TTY + missing project + picker viable → call the Catalog API
             via ``catalog_bootstrap`` and render an interactive picker. On
             any exception, fall back to the prompt path with a warning.

        Endpoint / region / tunnel are derived from the picked project's
        region ONLY when the user did not pass them explicitly — explicit
        user values always win.
        """
        provided_project = inputs.provided_project
        provided_endpoint = inputs.provided_endpoint
        provided_region = inputs.provided_region
        provided_tunnel = inputs.provided_tunnel
        env_settings = inputs.env_settings
        existing_auth = inputs.existing_auth
        from_env = inputs.from_env

        # 1. Explicit / env (gated on --from-env) / existing-config wins.
        #    --reselect bypasses the existing-config short-circuit so the
        #    picker re-opens even when a prior login saved auth.project.
        #    Explicit --project and --from-env env still win over --reselect.
        env_project = env_settings.get("project") if from_env else None
        existing_project_for_skip = None if inputs.reselect else existing_auth.project
        explicit_project = (
            (provided_project.strip() if provided_project and provided_project.strip() else None)
            or env_project
            or existing_project_for_skip
        )
        if explicit_project:
            return (
                explicit_project,
                provided_endpoint,
                provided_region,
                provided_tunnel,
                [],
            )

        # 2. Picker not viable → today's behavior (prompt or fail).
        if inputs.no_picker or not sys.stdin.isatty():
            prompted = self._resolve_login_value(
                provided=None,
                env_value=env_settings.get("project"),
                existing_value=existing_auth.project,
                prompt="MaxCompute Project",
                required=True,
                secret=False,
                use_env=from_env,
            )
            return (
                prompted,
                provided_endpoint,
                provided_region,
                provided_tunnel,
                [],
            )

        # 3. Try the catalog picker.
        warnings: list[str] = []
        try:
            bootstrap_odps = _catalog_bootstrap.build_bootstrap_odps(
                access_id=inputs.access_id,
                secret_access_key=inputs.secret,
                security_token=inputs.security_token,
                endpoint=inputs.catalog_endpoint or provided_endpoint,
            )
            projects = _catalog_bootstrap.list_all_projects(bootstrap_odps)
            if not projects:
                raise _catalog_bootstrap.NoProjectsError(
                    "Catalog returned 0 projects for this AccessKey."
                )
            picked = _catalog_bootstrap.pick_project(projects, input_fn=input)
        except Exception as exc:  # noqa: BLE001 — any failure → manual fallback
            warnings.append(
                f"Could not list projects via Catalog API "
                f"({type(exc).__name__}: {exc}). Falling back to manual entry."
            )
            prompted = self._resolve_login_value(
                provided=None,
                env_value=None,
                existing_value=None,
                prompt="MaxCompute Project",
                required=True,
                secret=False,
                use_env=False,
            )
            return (
                prompted,
                provided_endpoint,
                provided_region,
                provided_tunnel,
                warnings,
            )

        # 4. Successful pick — derive endpoint/region/tunnel ONLY if user
        #    did not provide them explicitly.
        derived_endpoint = provided_endpoint or _catalog_bootstrap.region_to_endpoint(picked.region)
        derived_region = provided_region or picked.region
        derived_tunnel = provided_tunnel or _catalog_bootstrap.region_to_tunnel_endpoint(picked.region)
        # Only warn when no fallback (env/config) exists — otherwise the
        # downstream _resolve_login_value chain will fill it in silently.
        if not derived_endpoint and not env_settings.get("endpoint") and not existing_auth.endpoint:
            warnings.append(
                f"Picked project '{picked.project_id}' is in region "
                f"'{picked.region}', which is not in the known endpoint table. "
                "Please provide --endpoint."
            )
            # Leave derived_endpoint as None — _resolve_login_value will prompt.
        return (
            picked.project_id,
            derived_endpoint,
            derived_region,
            derived_tunnel,
            warnings,
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
        # Env vars are honored unconditionally — login reflects the current
        # shell's MaxCompute environment without the user having to opt in.
        # --from-env (use_env=True) becomes a hard assertion: if the env is
        # missing a required value, fail loudly rather than fall through to a
        # stale config or interactive prompt.
        if env_value:
            return env_value.strip()
        if use_env and required:
            raise ValidationError(
                f"--from-env was specified but the environment variable for '{prompt}' is not set.",
                suggestion="Set the required environment variables (ALIBABA_CLOUD_ACCESS_KEY_ID, "
                "ALIBABA_CLOUD_ACCESS_KEY_SECRET, MAXCOMPUTE_PROJECT, MAXCOMPUTE_ENDPOINT) "
                "or provide the values as CLI flags.",
            )
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

    def agent_context(self) -> 'Envelope':
        """Return environment context for Agent readiness check.

        Provides version, auth status, backend reachability, skill path,
        full command list, and backend capability matrix — everything an
        Agent needs to determine whether it can use maxc-cli.
        """
        # Determine auth status
        auth_status = "unknown"
        backend_reachable = None
        if self.backend is not None:
            try:
                _ = self.backend.client.project  # lightweight connectivity probe
                backend_reachable = True
                auth_status = "authenticated"
            except Exception:
                backend_reachable = False
                auth_status = "unreachable"
        else:
            # No backend loaded — infer from config without network calls
            auth_cfg = self.config.auth
            has_provider = bool(auth_cfg.provider)
            has_project = bool(self.config.default_project)
            has_endpoint = bool(auth_cfg.endpoint or auth_cfg.region_name)
            if has_provider and has_project and has_endpoint:
                # Config looks complete — verify without hitting the network
                # by checking if credential fields are populated.
                if auth_cfg.provider == "access_key":
                    has_creds = bool(auth_cfg.access_id and auth_cfg.secret_access_key)
                elif auth_cfg.provider == "sts_token":
                    has_creds = bool(auth_cfg.access_id and auth_cfg.secret_access_key and auth_cfg.security_token)
                elif auth_cfg.provider == "ncs":
                    has_creds = bool(getattr(auth_cfg.ncs, "process_command", None))
                elif auth_cfg.provider == "external":
                    has_creds = bool(getattr(auth_cfg.external, "process_command", None))
                else:
                    has_creds = False

                if has_creds:
                    auth_status = "configured"
                    backend_reachable = None  # unknown until actual probe
                else:
                    auth_status = "incomplete"
                    backend_reachable = False
            elif has_project:
                auth_status = "not_configured"
                backend_reachable = False
            else:
                auth_status = "not_configured"
                backend_reachable = False

        # Determine backend capabilities
        capabilities = {
            "remote_jobs": getattr(self.backend, "supports_remote_jobs", False) if self.backend else False,
            "cost_check": getattr(self.backend, "supports_cost_check", False) if self.backend else False,
            "lineage": False,  # Always false for current ODPS backend
        }

        # For catalog_search, we need backend; if not loaded, do a lightweight probe
        if self.backend is not None:
            capabilities["catalog_search"] = self.backend.catalog_available
        else:
            # Lightweight probe: check kv_store cache first, then ODPS auto-routing
            catalog_search = False
            try:
                # 1. Check LocalCache kv_store (instant, no network)
                from .cache import LocalCache
                cache = LocalCache(self.config.cache_dir)
                cached_ep = cache.get_kv(
                    f"catalog_endpoint:{self.config.default_project}",
                    max_age_hours=24,
                )
                if cached_ep is not None:
                    catalog_search = True
                else:
                    # 2. No cache — trigger auto-routing and cache for next time
                    from .auth_providers import resolve_auth_connection
                    resolved = resolve_auth_connection(self.config, cache=cache)
                    odps = resolved.create_client()
                    ep = odps.catalog_endpoint
                    if ep is not None:
                        cache.set_kv(
                            f"catalog_endpoint:{self.config.default_project}", ep,
                        )
                        catalog_search = True
            except Exception:
                pass
            capabilities["catalog_search"] = catalog_search

        ac_data = {
                "version": __version__,
                "python_version": f"{__import__('sys').version_info.major}.{__import__('sys').version_info.minor}.{__import__('sys').version_info.micro}",
                "entry_point": "maxc",
                "project": self.config.default_project,
                "region": self.config.default_region,
                "backend": "odps",
                "backend_reachable": backend_reachable,
                "auth_status": auth_status,
                "project_context": self.config.project_context,
                "allowed_operations": self.config.allowed_operations,
                "cost_threshold_cu": self.config.cost_threshold_cu,
                "sensitive_columns": self.config.sensitive_columns,
                "capabilities": capabilities,
            }
        ac_metadata = {
                "config_sources": [str(path) for path in self.config.sources],
                "state_dir": str(self.config.state_dir),
                "job_mode": "remote" if self.remote_jobs else "local" if self.backend is not None else "unknown",
            }
        envelope = Envelope(
            command="agent.context",
            status="success",
            data=ac_data,
            metadata=ac_metadata,
            agent_hints=AgentHints(
                actions=[
                    action("agent.skill", data=ac_data, metadata=ac_metadata),
                    action("meta.search", data=ac_data, metadata=ac_metadata),
                    action("meta.list-tables", data=ac_data, metadata=ac_metadata),
                ],
            ),
        )
        self.log("agent.context", envelope.status, envelope.metadata)
        return envelope

    def agent_skill(self) -> 'Envelope':
        """Return SKILL.md path and metadata for Agent discoverability."""
        import importlib.resources
        try:
            skill_path = importlib.resources.files("maxc_cli") / "skills" / "SKILL.md"
            skill_path_str = str(skill_path)
            skill_exists = skill_path.is_file()
        except Exception:
            skill_path_str = ""
            skill_exists = False

        envelope = Envelope(
            command="agent.skill",
            status="success",
            data={
                "skill_path": skill_path_str,
                "skill_exists": skill_exists,
                "name": "maxcompute-cli-guidance",
                "version": __version__,
                "min_cli_version": "0.1.3",
                "entry_point": "maxc",
                "category": "database",
                "description": (
                    "Agent-native CLI for MaxCompute/ODPS — auth bootstrap, "
                    "metadata discovery, SQL execution, job tracking, and data profiling."
                ),
            },
            agent_hints=AgentHints(
                insights=[
                    "The SKILL.md file contains the full Agent-readable skill definition. "
                    "Read it to understand all available commands and workflows."
                ] if skill_exists else [
                    "SKILL.md not found in the installed package. "
                    "Reinstall maxc-cli or check package integrity."
                ],
            ),
        )
        self.log("agent.skill", envelope.status)
        return envelope

    def feature_unavailable(self, command: 'str', message: 'str') -> 'Envelope':
        raise FeatureUnavailableError(
            message,
            suggestion="Run `maxc --help` to inspect the currently supported commands.",
        )

    # ── agent skill {install,update,uninstall,list,diff,path} ────────────
    # All platform metadata lives in agent_platforms.REGISTRY; invocation
    # templates in agent_platforms.INVOCATIONS. See agent_platforms.py for
    # the single source of truth.

    def _resolve_skill_target(
        self,
        platform_name: 'str',
        dir_override: 'Path | None',
    ) -> 'tuple[Any, Path]':
        from . import agent_platforms
        try:
            platform = agent_platforms.resolve(platform_name)
        except KeyError as exc:
            raise ValidationError(str(exc))
        target = agent_platforms.effective_target(platform, dir_override)
        return platform, target

    def _locate_skills_source(self) -> 'Path':
        import importlib.resources
        try:
            skills_dir = importlib.resources.files("maxc_cli") / "skills"
            if not skills_dir.is_dir():
                raise MaxCError("Skills directory not found in installed package")
            return Path(str(skills_dir))
        except MaxCError:
            raise
        except Exception as exc:
            raise MaxCError(f"Cannot locate skills directory: {exc}")

    def _render_skill_into(
        self,
        skills_src: 'Path',
        target_dir: 'Path',
        platform: 'Any',
        invocation_map: 'dict[str, str]',
        force: 'bool',
    ) -> 'list[str]':
        """Render SKILL.md + references/ + extra_files into target_dir."""
        import shutil

        EXCLUDED_NAMES = {
            ".git", "__pycache__", ".DS_Store", "nohup.out",
            ".gitignore", ".pytest_cache", ".mypy_cache", ".ruff_cache",
        }
        EXCLUDED_SUFFIXES = (".pyc", ".pyo", ".log")
        TEMPLATED_SUFFIXES = (".md", ".yaml", ".yml")

        cli_str = invocation_map["cli"]
        cli_module_str = invocation_map["cli_module"]

        def _is_excluded(name: 'str') -> 'bool':
            if name in EXCLUDED_NAMES:
                return True
            return any(name.endswith(suf) for suf in EXCLUDED_SUFFIXES)

        def _render_or_copy(src: 'Path', dst: 'Path') -> 'None':
            if not force and dst.exists():
                return
            if src.suffix.lower() in TEMPLATED_SUFFIXES:
                content = render_skill_template(
                    src.read_text(encoding="utf-8"),
                    cli=cli_str,
                    cli_module=cli_module_str,
                )
                dst.write_text(content, encoding="utf-8")
                try:
                    shutil.copystat(str(src), str(dst))
                except OSError:
                    pass
            else:
                shutil.copy2(str(src), str(dst))

        def _render_tree(src_dir: 'Path', dst_dir: 'Path') -> 'None':
            dst_dir.mkdir(parents=True, exist_ok=True)
            for child in src_dir.iterdir():
                if _is_excluded(child.name):
                    continue
                target = dst_dir / child.name
                if child.is_file():
                    _render_or_copy(child, target)
                elif child.is_dir():
                    _render_tree(child, target)

        target_dir.mkdir(parents=True, exist_ok=True)

        files_copied: list[str] = []
        for item in skills_src.iterdir():
            if _is_excluded(item.name):
                continue
            dst = target_dir / item.name
            if item.is_file():
                _render_or_copy(item, dst)
                files_copied.append(item.name)
            elif item.is_dir():
                if force and dst.exists():
                    shutil.rmtree(str(dst))
                _render_tree(item, dst)
                files_copied.append(item.name + "/")

        from . import agent_platforms
        for ef in platform.extra_files:
            render_fn = agent_platforms.get_render_fn(ef.render_fn_name)
            render_fn(target_dir, cli_str, cli_module_str)
            files_copied.append(ef.relative_path)

        return sorted(files_copied)

    def skill_install(
        self,
        *,
        platform: 'str',
        invocation: 'str' = "maxc",
        dir_override: 'Path | None' = None,
        force: 'bool' = False,
    ) -> 'Envelope':
        from . import agent_platforms
        if invocation not in agent_platforms.INVOCATIONS:
            raise ValidationError(
                f"Unsupported invocation: {invocation}. "
                f"Supported: {', '.join(agent_platforms.INVOCATIONS)}"
            )
        platform_spec, target = self._resolve_skill_target(platform, dir_override)
        invocation_map = agent_platforms.INVOCATIONS[invocation]
        skills_src = self._locate_skills_source()
        version_marker = f"{__version__}+{invocation}"
        marker_path = target / ".maxc-skill-version"
        if not force and marker_path.is_file() and marker_path.read_text().strip() == version_marker:
            return Envelope(
                command="agent.skill.install",
                status="success",
                data={
                    "platform": platform_spec.name,
                    "invocation": invocation,
                    "install_path": str(target),
                    "installed_version": __version__,
                    "upgraded": False,
                    "files_copied": [],
                    "next_step": "Skill is already up to date",
                },
            )
        files = self._render_skill_into(
            skills_src, target, platform_spec, invocation_map, force=True
        )
        marker_path.write_text(version_marker)
        return Envelope(
            command="agent.skill.install",
            status="success",
            data={
                "platform": platform_spec.name,
                "invocation": invocation,
                "install_path": str(target),
                "installed_version": __version__,
                "upgraded": True,
                "files_copied": files,
                "next_step": platform_spec.next_step_hint,
            },
        )

    def skill_update(
        self,
        *,
        platform: 'str | None',
        all_platforms: 'bool',
        invocation: 'str' = "maxc",
    ) -> 'Envelope':
        from . import agent_platforms
        if platform is None and not all_platforms:
            raise ValidationError(
                "agent skill update requires either a <platform> argument or --all"
            )
        if platform is not None and all_platforms:
            raise ValidationError(
                "agent skill update accepts either <platform> or --all, not both"
            )
        if platform is not None:
            env = self.skill_install(platform=platform, invocation=invocation, force=True)
            env.command = "agent.skill.update"
            return env
        updated: list[str] = []
        for p in agent_platforms.all_platforms():
            target = agent_platforms.effective_target(p, None)
            if (target / ".maxc-skill-version").is_file():
                self.skill_install(platform=p.name, invocation=invocation, force=True)
                updated.append(p.name)
        return Envelope(
            command="agent.skill.update",
            status="success",
            data={"platforms_updated": updated, "invocation": invocation},
        )

    def skill_uninstall(
        self,
        *,
        platform: 'str',
        dir_override: 'Path | None' = None,
    ) -> 'Envelope':
        import shutil
        _, target = self._resolve_skill_target(platform, dir_override)
        removed = False
        if target.exists():
            shutil.rmtree(str(target))
            removed = True
        return Envelope(
            command="agent.skill.uninstall",
            status="success",
            data={"platform": platform, "install_path": str(target), "removed": removed},
        )

    def skill_list(self) -> 'Envelope':
        from . import agent_platforms
        installed: list[dict[str, Any]] = []
        for p in agent_platforms.all_platforms():
            target = agent_platforms.effective_target(p, None)
            marker = target / ".maxc-skill-version"
            if marker.is_file():
                installed.append({
                    "platform": p.name,
                    "install_path": str(target),
                    "installed_version_marker": marker.read_text().strip(),
                })
        hints = AgentHints(warnings=[
            "agent skill list only inspects default install paths. "
            "If you installed with --dir <CUSTOM>, that copy is not shown — "
            "pass --platform <name> --dir <CUSTOM> to skill_path to verify."
        ])
        return Envelope(
            command="agent.skill.list",
            status="success",
            data={"installed": installed},
            agent_hints=hints,
        )

    def skill_diff(
        self,
        *,
        platform: 'str',
        unified: 'bool' = False,
        dir_override: 'Path | None' = None,
    ) -> 'Envelope':
        import difflib

        from . import agent_platforms
        platform_spec, target = self._resolve_skill_target(platform, dir_override)
        skills_src = self._locate_skills_source()
        differences: list[dict[str, Any]] = []
        invocation_map = agent_platforms.INVOCATIONS["maxc"]
        for src in skills_src.rglob("*"):
            if not src.is_file():
                continue
            rel = src.relative_to(skills_src)
            dst = target / rel
            if not dst.exists():
                differences.append({"path": str(rel), "kind": "missing"})
                continue
            src_text = src.read_text(encoding="utf-8", errors="replace")
            dst_text = dst.read_text(encoding="utf-8", errors="replace")
            if src.suffix.lower() in (".md", ".yaml", ".yml"):
                src_text = render_skill_template(
                    src_text,
                    cli=invocation_map["cli"],
                    cli_module=invocation_map["cli_module"],
                )
            if src_text != dst_text:
                entry: dict[str, Any] = {"path": str(rel), "kind": "modified"}
                if unified:
                    entry["diff"] = "".join(difflib.unified_diff(
                        dst_text.splitlines(keepends=True),
                        src_text.splitlines(keepends=True),
                        fromfile=f"local/{rel}",
                        tofile=f"wheel/{rel}",
                    ))
                differences.append(entry)
        return Envelope(
            command="agent.skill.diff",
            status="success",
            data={
                "platform": platform_spec.name,
                "install_path": str(target),
                "differences": differences,
            },
        )

    def skill_path(
        self,
        *,
        platform: 'str | None' = None,
        source: 'bool' = False,
        dir_override: 'Path | None' = None,
    ) -> 'Envelope':
        if source:
            return Envelope(
                command="agent.skill.path",
                status="success",
                data={"path": str(self._locate_skills_source()), "kind": "source"},
            )
        if platform is None:
            raise ValidationError(
                "agent skill path requires --platform <name> unless --source is given"
            )
        _, target = self._resolve_skill_target(platform, dir_override)
        return Envelope(
            command="agent.skill.path",
            status="success",
            data={"path": str(target), "kind": "target", "platform": platform},
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
        force: 'bool' = False,
    ) -> 'JobInfo':
        if cost_check is not None:
            self._enforce_cost_check(sql=sql, project=project, cost_check=cost_check, force=force)
        return self.backend.submit_query(
            sql,
            project=project,
            idempotency_key=idempotency_key,
            force=force,
        )

    # ------------------------------------------------------------------
    # CU-based cost check helpers
    # ------------------------------------------------------------------
    # Conversion rule used for `--cost-check`:
    # MaxCompute SQLCost reports `input_size` in bytes scanned. The
    # rule-of-thumb conversion is 1 CU ≈ 1 GB of scanned input.
    _BYTES_PER_CU = 1024 ** 3

    def _enforce_cost_check(
        self,
        *,
        sql: 'str',
        project: 'str',
        cost_check: 'float',
        force: 'bool',
    ) -> 'None':
        """Estimate query cost and abort if it exceeds *cost_check* CU.

        Raises:
            CostLimitExceededError: If estimated CU exceeds the threshold.
            FeatureUnavailableError: If the backend doesn't expose
                ``estimate_query_cost``.
        """
        if not hasattr(self.backend, "estimate_query_cost"):
            raise FeatureUnavailableError(
                "The current backend does not provide CU-based cost validation.",
                suggestion="Remove `--cost-check`, or use `--dry-run` to inspect SQLCost metadata.",
            )
        try:
            estimate = self.backend.estimate_query_cost(sql, project=project, force=force)
        except MaxCError:
            raise
        except Exception as exc:
            raise FeatureUnavailableError(
                f"Could not estimate cost for `--cost-check`: {exc}",
                suggestion="Remove `--cost-check` or run `--dry-run` to inspect cost manually.",
            ) from exc
        bytes_scanned = int(estimate.get("estimated_input_size_bytes") or 0)
        estimated_cu = bytes_scanned / self._BYTES_PER_CU
        if estimated_cu > cost_check:
            raise CostLimitExceededError(
                (
                    f"Estimated query cost {estimated_cu:.2f} CU exceeds "
                    f"--cost-check threshold of {cost_check:.2f} CU "
                    f"({bytes_scanned:,} bytes scanned, 1 CU ≈ 1 GB)."
                ),
                suggestion=(
                    "Tighten the WHERE clause (e.g., add partition filter) or "
                    "raise the --cost-check threshold."
                ),
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
        force: 'bool' = False,
    ) -> 'QueryResult':
        if sql.startswith("@natural"):
            raise FeatureUnavailableError(
                "`@natural` is a roadmap feature and is not available in the current MVP.",
                suggestion="Use `maxc meta search` or `maxc meta describe` to inspect tables, then submit plain SQL.",
            )

        attempts = 0
        while True:
            try:
                if cost_check is not None and strict_cost_check:
                    if not getattr(self.backend, "supports_cost_check", False):
                        raise FeatureUnavailableError(
                            "The current backend does not provide CU-based cost validation.",
                            suggestion="Remove `--cost-check`, or use `--dry-run` to inspect SQLCost metadata.",
                        )
                    self._enforce_cost_check(
                        sql=sql, project=project, cost_check=cost_check, force=force,
                    )

                result = self.backend.execute_query(
                    sql,
                    project=project,
                    max_rows=max_rows,
                    dry_run=dry_run,
                    offset=offset,
                    timeout=timeout,
                    force=force,
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
        force: 'bool' = False,
    ) -> 'dict[str, Any]':
        if sql.startswith("@natural"):
            raise FeatureUnavailableError(
                "`@natural` is a roadmap feature and is not available in the current MVP.",
                suggestion="Use `maxc meta search` or `maxc meta describe` to inspect tables, then submit plain SQL.",
            )
        if explain:
            return self.backend.explain_query(sql, project=project, force=force)
        return self.backend.estimate_query_cost(sql, project=project, force=force)

    def _build_query_envelope(
        self,
        *,
        command: 'str',
        result: 'QueryResult',
        dry_run: 'bool',
        force: 'bool' = False,
        session_id: 'int | None' = None,
    ) -> 'Envelope':
        insights = []
        warnings = list(result.warnings)
        if dry_run:
            insights.append("Dry-run returned estimated cost and SQLCost metadata so you can decide whether to continue.")
        elif not result.rows:
            insights.append("The result set is empty. Check filters, partitions, and table selection.")

        # LIMIT truncation warning
        if result.has_more and not dry_run and not sql_has_limit(result.sql_executed):
            warnings.append(
                f"Results truncated to {result.returned_rows} rows. "
                f"Add LIMIT to your SQL or use --max-rows to adjust."
            )

        # Sensitive field masking
        rows = result.rows
        if self.config.masking_enabled and rows:
            rows, masked_columns = mask_rows(
                rows, result.schema,
                extra_sensitive_columns=self.config.sensitive_columns or None,
            )
            if masked_columns:
                warnings.append(f"Sensitive columns masked: {', '.join(masked_columns)}")

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

        data = {
                "rows": rows,
                "schema": result.schema,
                "total_rows": result.total_rows,
                "returned_rows": result.returned_rows,
                "has_more": result.has_more,
                "next_cursor": next_cursor,
            }

        # Build actions
        qe_actions: list[SuggestedAction] = []
        if result.tables_used:
            qe_actions.append(action("meta.describe", data=data, metadata=metadata))
        if result.has_more:
            qe_actions.append(action("query.next_page", data=data, metadata=metadata))
        if dry_run:
            qe_actions.append(action("job.submit", data=data, metadata=metadata))

        # Add safety block
        data["safety"] = build_safety_block(force=force, sql=result.sql_executed)

        return Envelope(
            command=command,
            status="success",
            data=data,
            metadata=metadata,
            agent_hints=AgentHints(
                actions=qe_actions,
                warnings=warnings,
                insights=insights,
            ),
        )

    def _build_analysis_envelope(
        self,
        *,
        command: 'str',
        sql: 'str',
        analysis: 'dict[str, Any]',
        force: 'bool' = False,
    ) -> 'Envelope':
        warnings = list(analysis.get("warnings", []))
        insights = []
        if analysis.get("estimated_input_size_bytes") == 0:
            insights.append("The estimated scan input is 0 bytes. This is often a constant query or a plan that avoids scanning data.")

        metadata = {
            "project": analysis.get("project"),
            "sql_executed": sql.rstrip(";"),
        }
        if analysis.get("elapsed_ms") is not None:
            metadata["elapsed_ms"] = analysis["elapsed_ms"]

        # Build actions
        ae_actions: list[SuggestedAction] = []
        if command == "query.cost":
            ae_actions.append(action("query.explain", data=analysis, metadata=metadata))
        ae_actions.append(action("query", data=analysis, metadata=metadata))
        if analysis.get("tables_used"):
            ae_actions.append(action("meta.describe", data=analysis, metadata=metadata))

        # Add safety block
        analysis["safety"] = build_safety_block(force=force, sql=sql)

        return Envelope(
            command=command,
            status="success",
            data=analysis,
            metadata=metadata,
            agent_hints=AgentHints(
                actions=ae_actions,
                warnings=warnings,
                insights=insights,
            ),
        )

    def _job_info_envelope(self, command: 'str', info: 'JobInfo') -> 'Envelope':
        ji_data = {
                "job_id": info.job_id,
                "status": info.status,
                "progress": info.progress,
                "stage": info.stage,
                "retryable": info.retryable,
                "failure_reason": info.failure_reason,
                "logview": info.logview,
                "task_summary": info.task_summary,
                "sql": info.sql,
            }
        ji_metadata = {
                "project": info.project,
                "submitted_at": info.submitted_at,
                "updated_at": info.updated_at,
                "completed_at": info.completed_at,
                "logview": info.logview,
                "error_message": info.error_message,
            }
        if info.status in {"pending", "running"}:
            ji_actions = [
                action("job.wait", data=ji_data, metadata=ji_metadata),
                action("job.result", data=ji_data, metadata=ji_metadata),
            ]
        elif info.status == "failure":
            ji_actions = [
                action("job.diagnose", data=ji_data, metadata=ji_metadata),
                action("job.status", data=ji_data, metadata=ji_metadata),
            ]
        else:
            ji_actions = [
                action("job.result", data=ji_data, metadata=ji_metadata),
            ]
        return Envelope(
            command=command,
            status=info.status,
            data=ji_data,
            metadata=ji_metadata,
            agent_hints=AgentHints(
                actions=ji_actions,
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


def read_stdin() -> 'str':
    return sys.stdin.read()
