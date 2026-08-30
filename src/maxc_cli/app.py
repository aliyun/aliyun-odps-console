
import getpass
import os
import re
import shlex
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable

from . import __version__
from .audit import AuditLogger
from .auth_providers import (
    ResolvedAuthConnection,
    build_auth_options,
    infer_auth_provider,
    resolve_auth_connection,
)
from .cache import CacheSnapshotBusyError, LocalCache
from .config import (
    AuthConfig,
    ExternalAuthConfig,
    OAuthAuthConfig,
    TableDefinition,
    default_global_config_path,
    load_config,
    load_config_mapping,
    migrate_legacy_session_override,
    persist_login_config,
    update_config_mapping,
)
from .exceptions import (
    BackendConnectionError,
    ColumnNotFoundError,
    CostLimitExceededError,
    ErrorPayload,
    FeatureUnavailableError,
    JobTimeoutError,
    MaxCError,
    SchemaNotFoundError,
    TableNotFoundError,
    TwoTierNamespaceError,
    ValidationError,
)
from .helpers import (
    build_odps_identity_payload,
    build_task_summary,
    classify_failure_reason,
    classify_sql_error,
    load_odps_env,
    mask_access_id,
    missing_odps_settings,
    parse_time_value,
    quote_table_name,
    resolve_odps_settings,
    translate_odps_error,
)
from .job_ids import COMPOSITE_METADATA_MESSAGE, format_job_id, parse_job_id
from .masking import mask_rows
from .models import (
    AgentHints,
    Envelope,
    JobInfo,
    QueryResult,
    SuggestedAction,
    action,
    build_observational_safety_block,
    build_safety_block,
)
from .store import JobStore
from .utils import (
    current_cli_entry_point,
    decode_cursor,
    encode_cursor,
    enforce_read_only_sql,
    known_write_operations,
    now_utc_iso,
    sql_has_limit,
    validate_csv_delimiter,
    validate_download_output_path,
    validate_upload_input_path,
)

_SKILL_IF_BLOCK = re.compile(
    r"<!--\s*@if\s+(\w[\w\s]*?)\s*-->(.*?)<!--\s*@endif\s*-->",
    flags=re.DOTALL,
)
_SKILL_BLANK_RUN = re.compile(r"\n{3,}")


def OdpsBackend(*args: Any, **kwargs: Any) -> Any:
    """Construct the remote backend without importing PyODPS for local commands."""
    from .backend import OdpsBackend as Backend

    return Backend(*args, **kwargs)


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
class _McqaExecutionSettings:
    enabled: 'bool'
    version: 'str'
    quota_name: 'str | None'
    fallback: 'bool'
    requested_mode: 'str'


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


@dataclass(frozen=True)
class _ResolvedExternalJobId:
    external_job_id: str
    instance_id: str
    subquery_id: 'int | None'
    project: str
    record: 'dict[str, Any] | None'
    session_context: 'dict[str, Any] | None'


class ProjectPickerPending(Exception):
    """Raised when non-TTY auth login can list projects but needs the caller to pick one."""
    def __init__(self, projects: list):
        self.projects = projects


class _LazyLocalCache:
    """Proxy that preserves backend cache support without startup writes."""

    def __init__(self, factory: 'Callable[[], LocalCache]') -> 'None':
        self._factory = factory

    def __getattr__(self, name: 'str') -> 'Any':
        return getattr(self._factory(), name)


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
        # Credential/catalog providers may need the cache later, but ordinary
        # authenticated commands must not create or migrate SQLite merely by
        # constructing their backend. The proxy materializes LocalCache only
        # when a provider actually reads or writes it.
        self._lazy_cache = _LazyLocalCache(lambda: self.cache)
        self.backend = (
            OdpsBackend(self.config, cache=self._lazy_cache)
            if load_backend
            else None
        )
        self.remote_jobs = getattr(self.backend, "supports_remote_jobs", False) if self.backend else False
        self.jobs: JobStore | None = None
        self._audit: AuditLogger | None = None
        self._audit_invocation_id: str | None = None
        self._audit_path = self.config.agent.audit_log or self.config.state_dir / "audit.log"

    def _mask_sensitive_rows(
        self,
        rows: 'list[dict[str, Any]]',
        schema: 'list[dict[str, Any]]',
    ) -> 'tuple[list[dict[str, Any]], list[str]]':
        """Apply the configured masking policy to any row-bearing output."""
        if not self.config.masking_enabled or not rows:
            return rows, []
        return mask_rows(
            rows,
            schema,
            extra_sensitive_columns=self.config.sensitive_columns or None,
        )

    @property
    def cache(self) -> 'LocalCache':
        if self._cache is None:
            self._cache = LocalCache(self.config.cache_dir)
        return self._cache

    def _read_only_cache(self) -> 'LocalCache':
        """Return a non-creating, non-migrating view of the local cache."""
        return LocalCache(self.config.cache_dir, read_only=True)

    def _ensure_job_store(self) -> 'JobStore':
        if self.jobs is None:
            self.jobs = JobStore(self.config.state_dir)
        return self.jobs

    def _persist_remote_session_context(
        self,
        *,
        job_id: 'str',
        project: 'str',
        session_task_name: 'str | None',
        session_subquery_id: 'int | None',
        session_project_name: 'str | None',
        session_is_select: 'bool | None',
        require_composite: 'bool' = False,
    ) -> 'str':
        external_job_id, context = self._build_remote_session_context(
            job_id=job_id,
            project=project,
            session_task_name=session_task_name,
            session_subquery_id=session_subquery_id,
            session_project_name=session_project_name,
            session_is_select=session_is_select,
            require_composite=require_composite,
        )
        self._ensure_job_store().save_remote_job_context(external_job_id, context)
        return external_job_id

    @staticmethod
    def _build_remote_session_context(
        *,
        job_id: 'str',
        project: 'str',
        session_task_name: 'str | None',
        session_subquery_id: 'int | None',
        session_project_name: 'str | None',
        session_is_select: 'bool | None',
        require_composite: 'bool' = False,
    ) -> 'tuple[str, dict[str, Any]]':
        parsed_job_id = parse_job_id(job_id)
        subquery_id = parsed_job_id.subquery_id
        if require_composite:
            if session_subquery_id is None or not session_task_name:
                raise ValidationError(COMPOSITE_METADATA_MESSAGE)
            if subquery_id is None:
                subquery_id = int(session_subquery_id)
        external_job_id = format_job_id(parsed_job_id.instance_id, subquery_id)
        context: dict[str, Any] = {
            "instance_id": parsed_job_id.instance_id,
            "project": project,
        }
        if subquery_id is not None:
            context.update(
                {
                    "subquery_id": subquery_id,
                    "session_task_name": session_task_name,
                    "session_subquery_id": subquery_id,
                    "session_project_name": session_project_name or project,
                    "session_is_select": (
                        True if session_is_select is None else session_is_select
                    ),
                }
            )
        return external_job_id, context

    def _persist_remote_job_context(self, job: 'JobInfo', *, require_composite: 'bool' = False) -> 'None':
        submitted_job_id = job.job_id
        try:
            external_job_id, context = self._build_remote_session_context(
                job_id=submitted_job_id,
                project=job.project,
                session_task_name=job.session_task_name,
                session_subquery_id=job.session_subquery_id,
                session_project_name=job.session_project_name,
                session_is_select=job.session_is_select,
                require_composite=require_composite,
            )
        except Exception:
            job.warnings.append(
                f"Remote job `{submitted_job_id}` was submitted, but the CLI could not "
                "derive its MCQA follow-up context. Do not resubmit it; use "
                f"`job status {submitted_job_id} --project {job.project}` and retain the "
                "returned job ID."
            )
            return

        # Publish the usable ID before touching local state. A disk or lock
        # failure after remote submission must never hide the server-side job
        # or encourage the caller to submit it again.
        job.job_id = external_job_id
        try:
            self._ensure_job_store().save_remote_job_context(external_job_id, context)
        except Exception:
            job.warnings.append(
                f"Remote job `{external_job_id}` was submitted, but its local follow-up "
                "context could not be saved. Do not resubmit it; pass "
                f"`--project {job.project}` to subsequent job commands."
            )

    def _persist_remote_query_result_context(
        self,
        result: 'QueryResult',
        *,
        require_composite: 'bool' = False,
    ) -> 'None':
        if result.job_id is None:
            return
        submitted_job_id = result.job_id
        try:
            external_job_id, context = self._build_remote_session_context(
                job_id=submitted_job_id,
                project=result.project,
                session_task_name=result.session_task_name,
                session_subquery_id=result.session_subquery_id,
                session_project_name=result.session_project_name,
                session_is_select=result.session_is_select,
                require_composite=require_composite,
            )
        except Exception:
            result.warnings.append(
                f"Remote query `{submitted_job_id}` completed, but the CLI could not "
                "derive its MCQA follow-up context. Do not rerun the query solely for "
                "this local bookkeeping failure."
            )
            return

        result.job_id = external_job_id
        # Follow-up result/wait paths already have an authoritative submission
        # record. Do not overwrite its richer SQLRT session fields with a
        # QueryResult that may only carry the external composite ID. A local
        # read failure must not hide a result that already completed remotely.
        try:
            if self._remote_job_record(external_job_id) is not None:
                return
        except Exception:
            result.warnings.append(
                f"Remote query `{external_job_id}` completed, but existing local "
                "follow-up context could not be read. Do not rerun it solely for "
                f"this local failure; pass `--project {result.project}` to job commands."
            )
            return

        try:
            self._ensure_job_store().save_remote_job_context(external_job_id, context)
        except Exception:
            result.warnings.append(
                f"Remote query `{external_job_id}` completed, but its local follow-up "
                "context could not be saved. Do not rerun it solely for this local "
                f"failure; pass `--project {result.project}` to job commands."
            )

    def _requires_composite_job_id(
        self,
        execution_settings: '_McqaExecutionSettings | None' = None,
    ) -> 'bool':
        return bool(
            execution_settings
            and execution_settings.enabled
            and execution_settings.version == "v1"
        )

    def _query_result_uses_composite_job_id(self, result: 'QueryResult') -> 'bool':
        if result.job_id and "@" in result.job_id:
            return True
        execution_requested = result.extra_metadata.get("execution_requested")
        execution_mode = result.extra_metadata.get("execution_mode")
        return execution_requested == "mcqa_v1" or execution_mode == "mcqa_v1"

    def _remote_job_record(self, job_id: 'str') -> 'dict[str, Any] | None':
        return self._ensure_job_store().get_remote_job_context(job_id)

    def _remote_job_context(self, job_id: 'str') -> 'dict[str, Any] | None':
        return self._resolve_remote_job_id(job_id).session_context

    def _session_context_from_record(
        self, record: 'dict[str, Any]'
    ) -> 'dict[str, Any] | None':
        context = {
            "session_task_name": record.get("session_task_name"),
            "session_subquery_id": record.get("session_subquery_id", record.get("subquery_id")),
            "session_project_name": record.get("session_project_name"),
            "session_is_select": record.get("session_is_select"),
        }
        if not context["session_task_name"] or context["session_subquery_id"] is None:
            return None
        return context

    def _resolve_remote_job_id(
        self,
        raw_job_id: 'str',
        *,
        project: 'str | None' = None,
    ) -> '_ResolvedExternalJobId':
        parsed = parse_job_id(raw_job_id)
        external_job_id = format_job_id(parsed.instance_id, parsed.subquery_id)
        try:
            record = self._remote_job_record(external_job_id)
            if record is None and parsed.subquery_id is None:
                record = self._remote_job_record(parsed.instance_id)
        except Exception:
            if project is None:
                # Without an explicit project the local submission record is
                # the only authoritative routing context; do not silently
                # guess when it cannot be read.
                raise
            # An explicit project is the documented recovery path when the
            # state directory is unavailable. Continue without local context.
            record = None
        session_context = None
        if record is not None:
            session_context = self._session_context_from_record(record)
        elif parsed.subquery_id is not None:
            session_context = {
                "session_subquery_id": parsed.subquery_id,
            }
        recorded_project = (record or {}).get("project")
        if project and recorded_project and project != recorded_project:
            raise ValidationError(
                f"Job `{external_job_id}` was submitted in project `{recorded_project}`, not `{project}`.",
                suggestion="Omit --project to use the stored submission context, or pass the recorded project.",
            )
        return _ResolvedExternalJobId(
            external_job_id=external_job_id,
            instance_id=parsed.instance_id,
            subquery_id=parsed.subquery_id,
            project=project or recorded_project or self.config.default_project,
            record=record,
            session_context=session_context,
        )

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

    def _resolve_mcqa_settings(
        self,
        *,
        command: 'str',
        mcqa: 'bool | None' = None,
        maxqa: 'bool' = False,
        no_mcqa: 'bool' = False,
        mcqa_version: 'str | None' = None,
        quota: 'str | None' = None,
        mcqa_fallback: 'bool | None' = None,
    ) -> '_McqaExecutionSettings':
        if no_mcqa and (
            mcqa is True
            or maxqa
            or mcqa_version is not None
            or quota is not None
            or mcqa_fallback is True
        ):
            raise ValidationError("`--no-mcqa` cannot be combined with other MCQA options.")
        if mcqa is True and maxqa:
            raise ValidationError("`--mcqa` and `--maxqa` cannot be combined.")

        config_mcqa = self.config.mcqa
        # Fallback modifies an already selected MCQA execution; it must never
        # enable MCQA by itself. In particular, `--no-mcqa-fallback` is a
        # negative override and cannot require a v2 quota on an offline query.
        explicit_mcqa_options = maxqa or any(
            value is not None for value in (mcqa_version, quota)
        )
        if no_mcqa:
            enabled = False
        elif maxqa:
            enabled = True
        elif mcqa is not None:
            enabled = mcqa
        else:
            enabled = config_mcqa.enabled or explicit_mcqa_options

        if mcqa_fallback is True and not enabled:
            raise ValidationError(
                "`--mcqa-fallback` requires MCQA to be enabled.",
                suggestion="Add --mcqa, --maxqa with --quota, or configure MCQA first.",
            )

        if mcqa_version is not None:
            version = str(mcqa_version)
        elif maxqa:
            version = "v2"
        elif mcqa is True:
            version = "v1"
        else:
            version = str(config_mcqa.version or "v2")

        if quota is not None:
            quota_name = quota
        elif version == "v2":
            quota_name = config_mcqa.quota_name
        else:
            quota_name = None

        if command == "job.submit":
            if mcqa_fallback is True:
                raise ValidationError("MCQA fallback is not supported with job submit; use query for fallbackable execution.")
            fallback = False
        else:
            fallback = config_mcqa.fallback if mcqa_fallback is None else mcqa_fallback

        if not enabled:
            return _McqaExecutionSettings(
                enabled=False,
                version=version,
                quota_name=None,
                fallback=False,
                requested_mode="offline",
            )

        if version not in {"v1", "v2"}:
            raise ValidationError("`--mcqa-version` must be `v1` or `v2`.")
        if version == "v1" and quota_name:
            raise ValidationError("Quota name applies only to MCQA v2.")
        if version == "v2" and not quota_name:
            raise ValidationError("MCQA v2 requires a quota name.")

        return _McqaExecutionSettings(
            enabled=True,
            version=version,
            quota_name=quota_name,
            fallback=fallback,
            requested_mode=f"mcqa_{version}",
        )

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
        mcqa: 'bool | None' = None,
        maxqa: 'bool' = False,
        no_mcqa: 'bool' = False,
        mcqa_version: 'str | None' = None,
        quota: 'str | None' = None,
        mcqa_fallback: 'bool | None' = None,
    ) -> 'Envelope':
        if max_rows <= 0:
            raise ValidationError("`--max-rows` and `--page-size` must be greater than 0.")
        if cursor and dry_run:
            raise ValidationError("Do not combine `--cursor` with `--dry-run`.")
        if not cursor:
            enforce_read_only_sql(sql, force=force)

        target_project = project or self.config.default_project
        if cursor and self.remote_jobs:
            submission_only_options: list[str] = []
            if cost_check is not None:
                submission_only_options.append("--cost-check")
            if idempotency_key:
                submission_only_options.append("--idempotency-key")
            if wait != 10:
                submission_only_options.append("--wait")
            if force:
                submission_only_options.append("--force")
            if mcqa is not None:
                submission_only_options.append("--mcqa")
            if maxqa:
                submission_only_options.append("--maxqa")
            if no_mcqa:
                submission_only_options.append("--no-mcqa")
            if mcqa_version is not None:
                submission_only_options.append("--mcqa-version")
            if quota is not None:
                submission_only_options.append("--quota")
            if mcqa_fallback is not None:
                submission_only_options.append("--mcqa-fallback/--no-mcqa-fallback")
            if submission_only_options:
                raise ValidationError(
                    f"{', '.join(submission_only_options)} only apply to a new "
                    "query submission and cannot be combined with --cursor.",
                    suggestion=(
                        "Remove the submission-only flags. A cursor always fetches "
                        "the existing job using its persisted execution context."
                    ),
                )
        write_operations = known_write_operations(sql)
        if write_operations and ((retry_on or []) or max_retries):
            raise ValidationError(
                "Automatic retries are not supported for mutating SQL "
                f"({', '.join(write_operations)}).",
                suggestion=(
                    "Remove --retry-on and --max-retries. Inspect the original "
                    "job/result before deciding whether a manual retry is safe."
                ),
            )
        if self.remote_jobs and ((retry_on or []) or max_retries):
            raise ValidationError(
                "Automatic query retries are not supported by resumable remote execution.",
                suggestion=(
                    "Submit once, retain the returned job_id, and inspect that job before "
                    "deciding whether a manual retry is safe."
                ),
            )
        if idempotency_key and (
            dry_run
            or not self.remote_jobs
        ):
            raise ValidationError(
                "`--idempotency-key` is only applied by asynchronous remote job submission.",
                suggestion=(
                    "Use `--wait 0` (or `job submit`) so the key is sent with the "
                    "submission, or remove --idempotency-key."
                ),
            )
        offset, session_id = decode_cursor(cursor)

        # A remote cursor must resolve to the original submitted job. Losing
        # local cursor state is never permission to submit the SQL again: that
        # could duplicate cost or side effects while appearing to be a read of
        # the next page.
        if cursor and self.remote_jobs:
            if session_id is None:
                raise ValidationError(
                    "The remote cursor does not contain resumable job context.",
                    suggestion=(
                        "Use the next_cursor from the latest remote response. If "
                        "its local state was cleared, retain the original job_id "
                        "and use `job result --max-rows <larger-value>`; do not "
                        "rerun the query just to paginate."
                    ),
                )
            try:
                session = self._read_only_cache().get_session(session_id)
            except Exception as exc:
                raise ValidationError(
                    "The remote pagination context could not be read locally.",
                    suggestion=(
                        "Use the original job_id with `job result --max-rows "
                        "<larger-value>`; do not rerun the query."
                    ),
                ) from exc
            if not session or not session.get("job_id"):
                raise ValidationError(
                    "The remote pagination context no longer exists.",
                    suggestion=(
                        "The cache may have been cleared. Use the original job_id "
                        "with `job result --max-rows <larger-value>`; do not rerun "
                        "the query."
                    ),
                )
            session_project = str(session.get("project") or "").strip()
            if project is not None and project.strip() != session_project:
                raise ValidationError(
                    "The pagination cursor belongs to a different project than --project.",
                    suggestion=(
                        f"Use `--project {session_project}` with this cursor, or "
                        "start a new query intentionally."
                    ),
                )
            session_sql = session.get("sql")
            if not isinstance(session_sql, str) or not session_sql.strip():
                raise ValidationError(
                    "The pagination cursor is missing its original SQL identity.",
                    suggestion=(
                        "Use the original job_id with `job result --max-rows "
                        "<larger-value>`; do not rerun the query."
                    ),
                )
            if sql != session_sql:
                raise ValidationError(
                    "The pagination cursor belongs to different SQL than this request.",
                    suggestion=(
                        "Use the exact original SQL with this cursor, or use the "
                        "original job_id with `job result`."
                    ),
                )
            resolved = self._resolve_remote_job_id(
                session["job_id"],
                project=session.get("project") or None,
            )
            result = self.backend.fetch_job_result(
                resolved.instance_id,
                project=resolved.project,
                max_rows=max_rows,
                offset=offset,
                session_context=resolved.session_context,
            )
            envelope = self._build_query_envelope(
                command=command,
                result=result,
                dry_run=False,
                force=force,
                session_id=session_id,
                external_job_id=resolved.external_job_id,
            )
            self.log(command, envelope.status, envelope.metadata)
            return envelope

        # Resolve execution settings only after a remote cursor continuation
        # has returned. Existing jobs must not become unreadable because the
        # current MCQA defaults drifted or are incomplete.
        execution_settings = self._resolve_mcqa_settings(
            command=command,
            mcqa=mcqa,
            maxqa=maxqa,
            no_mcqa=no_mcqa,
            mcqa_version=mcqa_version,
            quota=quota,
            mcqa_fallback=mcqa_fallback,
        )

        # Remote branch — always submit, then poll up to `wait` seconds
        if self.remote_jobs and not dry_run:
            submitted_execution_settings = execution_settings
            execution_warnings: list[str] = []
            if execution_settings.enabled:
                # PyODPS cannot safely auto-fallback an MCQA v1 query while
                # also returning the submitted interactive instance before it
                # completes.  A resumable job ID is the stronger contract:
                # submit once, poll that exact job, and never hide it on
                # timeout.  Keep the requested mode visible and state the
                # fallback limitation explicitly.
                submitted_execution_settings = _McqaExecutionSettings(
                    enabled=True,
                    version=execution_settings.version,
                    quota_name=execution_settings.quota_name,
                    fallback=False,
                    requested_mode=execution_settings.requested_mode,
                )
                if execution_settings.fallback:
                    execution_warnings.append(
                        "MCQA fallback is disabled for resumable query execution. "
                        "The CLI submitted one trackable MCQA job and will not "
                        "silently create a second offline job."
                    )
            job = self._submit_remote_job(
                sql=sql,
                project=target_project,
                cost_check=cost_check,
                idempotency_key=idempotency_key,
                force=force,
                execution_settings=submitted_execution_settings,
            )
            common_warnings = (
                list(job.warnings or []) + execution_warnings
            )
            execution_metadata = {
                "execution_requested": submitted_execution_settings.requested_mode,
                "execution_mode": submitted_execution_settings.requested_mode,
                "mcqa_fallback_enabled": submitted_execution_settings.fallback,
                "mcqa_fallback_used": False,
                "mcqa_quota_name": submitted_execution_settings.quota_name,
            }
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
                        "requested_max_rows": max_rows,
                        "sql_executed": sql,
                        **execution_metadata,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.wait", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project, "sql_executed": sql}),
                            action("job.status", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project}),
                            action(
                                "job.result",
                                data={"job_id": job.job_id, "max_rows": max_rows},
                                metadata={"job_id": job.job_id, "project": job.project},
                            ),
                        ],
                        warnings=common_warnings,
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            resolved = self._resolve_remote_job_id(job.job_id, project=job.project)
            # Poll the authoritative outer instance and, for MCQA v1, retain
            # the persisted SQLRT subquery context for status/result routing.
            try:
                job_info = self.backend.wait_job(
                    resolved.instance_id,
                    project=resolved.project,
                    timeout=wait,
                    poll_interval=1,
                    session_context=resolved.session_context,
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
                        "requested_max_rows": max_rows,
                        "sql_executed": sql,
                        **execution_metadata,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.wait", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project, "sql_executed": sql}),
                            action("job.status", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project}),
                            action(
                                "job.result",
                                data={"job_id": job.job_id, "max_rows": max_rows},
                                metadata={"job_id": job.job_id, "project": job.project},
                            ),
                        ],
                        warnings=common_warnings,
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
                        "job_id": resolved.external_job_id,
                        "project": resolved.project,
                        "sql_executed": sql,
                        **execution_metadata,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.status", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project, "sql_executed": sql}),
                            action("job.diagnose", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project}),
                        ],
                        warnings=common_warnings,
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            # Job ended — check outcome
            if job_info.status == "failure":
                error_msg = job_info.failure_reason or job_info.error_message or "Job failed"
                error_payload = self._query_job_failure_payload(error_msg)
                envelope = Envelope(
                    command=command,
                    status="failure",
                    data={"job_id": resolved.external_job_id},
                    metadata={
                        "job_id": resolved.external_job_id,
                        "project": job_info.project,
                        "submitted_at": job_info.submitted_at,
                        "logview": job_info.logview,
                        "sql_executed": sql,
                        **execution_metadata,
                    },
                    error=error_payload,
                    agent_hints=AgentHints(
                        actions=[
                            action("job.diagnose", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": job_info.project, "sql_executed": sql}),
                            action("job.status", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": job_info.project}),
                        ],
                        warnings=common_warnings + list(job_info.warnings or []),
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            # status == "success" — fetch rows
            fetch_error = None
            try:
                result = self.backend.fetch_job_result(
                    resolved.instance_id,
                    project=resolved.project,
                    max_rows=max_rows,
                    offset=offset,
                    session_context=resolved.session_context,
                )
            except MaxCError as exc:
                fetch_error = exc.to_payload()
            except Exception as exc:
                fetch_error = translate_odps_error(exc).to_payload()
            if fetch_error is not None:
                envelope = Envelope(
                    command=command,
                    status="failure",
                    data=None,
                    error=fetch_error,
                    metadata={
                        "job_id": resolved.external_job_id,
                        "project": resolved.project,
                        "logview": job_info.logview,
                        "sql_executed": sql,
                        **execution_metadata,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.result", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project, "sql_executed": sql}),
                            action("job.status", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project}),
                        ],
                        warnings=common_warnings,
                    ),
                )
                if idempotency_key:
                    envelope.metadata["idempotency_key"] = idempotency_key
                self.log(command, envelope.status, envelope.metadata)
                return envelope
            result.extra_metadata.update(execution_metadata)
            for warning in common_warnings:
                if warning not in result.warnings:
                    result.warnings.append(warning)
            envelope = self._build_query_envelope(
                command=command,
                result=result,
                dry_run=False,
                force=force,
                external_job_id=resolved.external_job_id,
            )
            envelope.metadata.update({
                "job_id": resolved.external_job_id,
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
            execution_settings=execution_settings,
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
        enforce_read_only_sql(sql, force=force)
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
        enforce_read_only_sql(sql, force=force)
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
        dry_run: 'bool' = False,
        mcqa: 'bool | None' = None,
        maxqa: 'bool' = False,
        no_mcqa: 'bool' = False,
        mcqa_version: 'str | None' = None,
        quota: 'str | None' = None,
        mcqa_fallback: 'bool | None' = None,
    ) -> 'Envelope':
        enforce_read_only_sql(sql, force=force)
        if not self.remote_jobs:
            return self.query(
                command="job.submit",
                sql=sql,
                project=project,
                max_rows=max_rows,
                wait=0,
                cost_check=cost_check,
                idempotency_key=idempotency_key,
                force=force,
                dry_run=dry_run,
                mcqa=mcqa,
                maxqa=maxqa,
                no_mcqa=no_mcqa,
                mcqa_version=mcqa_version,
                quota=quota,
                mcqa_fallback=mcqa_fallback,
            )

        execution_settings = self._resolve_mcqa_settings(
            command="job.submit",
            mcqa=mcqa,
            maxqa=maxqa,
            no_mcqa=no_mcqa,
            mcqa_version=mcqa_version,
            quota=quota,
            mcqa_fallback=mcqa_fallback,
        )
        target_project = project or self.config.default_project
        if dry_run:
            return self.query(
                command="job.submit",
                sql=sql,
                project=project,
                max_rows=max_rows,
                wait=0,
                cost_check=cost_check,
                idempotency_key=idempotency_key,
                force=force,
                dry_run=dry_run,
                mcqa=mcqa,
                maxqa=maxqa,
                no_mcqa=no_mcqa,
                mcqa_version=mcqa_version,
                quota=quota,
                mcqa_fallback=mcqa_fallback,
            )

        if cost_check is not None:
            self._enforce_cost_check(sql=sql, project=target_project, cost_check=cost_check, force=force)

        job = self._submit_remote_job(
            sql=sql,
            project=target_project,
            cost_check=None,
            idempotency_key=idempotency_key,
            force=force,
            execution_settings=execution_settings,
        )
        envelope = Envelope(
            command="job.submit",
            status="pending",
            data={"job_id": job.job_id, "safety": build_safety_block(force=force, sql=sql)},
            metadata={
                "job_id": job.job_id,
                "project": job.project,
                "submitted_at": job.submitted_at,
                "logview": job.logview,
                "sql_executed": sql,
                "execution_requested": execution_settings.requested_mode,
                "execution_mode": execution_settings.requested_mode,
                "mcqa_fallback_enabled": execution_settings.fallback,
                "mcqa_fallback_used": False,
                "mcqa_quota_name": execution_settings.quota_name,
            },
            agent_hints=AgentHints(
                actions=[
                    action("job.wait", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project, "sql_executed": sql}),
                    action("job.status", data={"job_id": job.job_id}, metadata={"job_id": job.job_id, "project": job.project}),
                ],
                warnings=job.warnings or [],
            ),
        )
        if idempotency_key:
            envelope.metadata["idempotency_key"] = idempotency_key
        self.log("job.submit", envelope.status, envelope.metadata)
        return envelope

    def job_status(self, job_id: 'str', *, project: 'str | None' = None) -> 'Envelope':
        if self.remote_jobs:
            resolved = self._resolve_remote_job_id(job_id, project=project)
            info = self.backend.get_job(
                resolved.instance_id,
                project=resolved.project,
                session_context=resolved.session_context,
            )
            envelope = self._job_info_envelope(
                "job.status",
                info,
                external_job_id=resolved.external_job_id,
            )
            self.log("job.status", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        if project and project != job["project"]:
            raise ValidationError(
                f"Job `{job_id}` belongs to project `{job['project']}`, not `{project}`."
            )
        info = self._local_job_info(job)
        envelope = self._job_info_envelope("job.status", info)
        self.log("job.status", envelope.status, envelope.metadata)
        return envelope

    def job_wait(
        self,
        job_id: 'str',
        *,
        timeout: 'int | None' = None,
        project: 'str | None' = None,
    ) -> 'tuple[Envelope, list[dict[str, Any]]]':
        effective_timeout = timeout if timeout is not None else 300
        if self.remote_jobs:
            resolved = self._resolve_remote_job_id(job_id, project=project)
            before = self.backend.get_job(
                resolved.instance_id,
                project=resolved.project,
                session_context=resolved.session_context,
            )
            try:
                after = self.backend.wait_job(
                    resolved.instance_id,
                    project=resolved.project,
                    timeout=effective_timeout,
                    session_context=resolved.session_context,
                )
            except JobTimeoutError:
                envelope = Envelope(
                    command="job.wait",
                    status="pending",
                    data={"job_id": resolved.external_job_id},
                    metadata={
                        "job_id": resolved.external_job_id,
                        "project": resolved.project,
                        "submitted_at": before.submitted_at,
                        "logview": before.logview,
                        "wait_seconds": effective_timeout,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.wait", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project}),
                            action("job.status", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project}),
                        ],
                        insights=[f"Job still running after {effective_timeout}s."],
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
                        "job_id": resolved.external_job_id,
                        "project": resolved.project,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("job.status", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project}),
                        ],
                        warnings=[f"Lost contact with backend while waiting for job {resolved.external_job_id}."],
                    ),
                )
                self.log("job.wait", envelope.status, envelope.metadata)
                return envelope, []
            if after.status != "success":
                envelope = self._job_info_envelope(
                    "job.wait",
                    after,
                    external_job_id=resolved.external_job_id,
                )
                normalized_after = str(after.status or "").lower()
                if normalized_after == "cancelled":
                    terminal_type = "cancelled"
                elif normalized_after in {"failure", "failed"}:
                    terminal_type = "failed"
                else:
                    terminal_type = "unknown"
                terminal_payload = envelope.to_dict()
                events = [
                    {"type": "started", "ts": before.submitted_at or now_utc_iso(), "job_id": resolved.external_job_id},
                    {
                        "type": terminal_type,
                        "ts": after.completed_at or now_utc_iso(),
                        "status": envelope.status,
                        "job_id": resolved.external_job_id,
                        "data": terminal_payload.get("data"),
                        "metadata": terminal_payload.get("metadata"),
                        "error": terminal_payload.get("error"),
                        "agent_hints": terminal_payload.get("agent_hints"),
                    },
                ]
                self.log("job.wait", envelope.status, envelope.metadata)
                return envelope, events
            result = self.backend.fetch_job_result(
                resolved.instance_id,
                project=resolved.project,
                max_rows=100,
                session_context=resolved.session_context,
            )
            envelope = self._build_query_envelope(
                command="job.wait",
                result=result,
                dry_run=False,
                external_job_id=resolved.external_job_id,
            )
            envelope.metadata.update(
                {
                    "job_id": resolved.external_job_id,
                    "submitted_at": after.submitted_at,
                    "completed_at": after.completed_at,
                    "logview": after.logview,
                    "stage": after.stage,
                    "retryable": after.retryable,
                    "failure_reason": after.failure_reason,
                    "task_summary": after.task_summary,
                }
            )
            events = self._remote_job_events(
                before,
                after,
                result,
                external_job_id=resolved.external_job_id,
            )
            self.log("job.wait", envelope.status, envelope.metadata)
            return envelope, events

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        if project and project != job["project"]:
            raise ValidationError(
                f"Job `{job_id}` belongs to project `{job['project']}`, not `{project}`."
            )
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

    def job_result(
        self,
        job_id: 'str',
        *,
        max_rows: 'int' = 100,
        cursor: 'str | None' = None,
        project: 'str | None' = None,
    ) -> 'Envelope':
        if self.remote_jobs:
            resolved = self._resolve_remote_job_id(job_id, project=project)
            offset, cursor_session_id = self._bound_job_result_cursor(
                cursor,
                job_id=resolved.external_job_id,
                project=resolved.project,
            )
            info = self.backend.get_job(
                resolved.instance_id,
                project=resolved.project,
                session_context=resolved.session_context,
            )
            if info.status != "success":
                envelope = self._job_info_envelope(
                    "job.result",
                    info,
                    external_job_id=resolved.external_job_id,
                )
                self.log("job.result", envelope.status, envelope.metadata)
                return envelope
            result = self.backend.fetch_job_result(
                resolved.instance_id,
                project=resolved.project,
                max_rows=max_rows,
                offset=offset,
                session_context=resolved.session_context,
            )
            envelope = self._build_query_envelope(
                command="job.result",
                result=result,
                dry_run=False,
                session_id=cursor_session_id,
                external_job_id=resolved.external_job_id,
            )
            envelope.metadata.update(
                {
                    "job_id": resolved.external_job_id,
                    "submitted_at": info.submitted_at,
                    "completed_at": info.completed_at,
                    "logview": info.logview,
                }
            )
            self.log("job.result", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        if project and project != job["project"]:
            raise ValidationError(
                f"Job `{job_id}` belongs to project `{job['project']}`, not `{project}`."
            )
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

        offset, cursor_session_id = self._bound_job_result_cursor(
            cursor,
            job_id=job_id,
            project=job["project"],
        )
        page_rows = all_rows[offset:offset + max_rows]
        returned_rows = len(page_rows)
        has_more = (offset + returned_rows) < total_rows
        # Sensitive field masking
        local_warnings = list(stored.get("agent_hints", {}).get("warnings", []))
        next_cursor = None
        if has_more and returned_rows > 0:
            if cursor_session_id is None:
                try:
                    cursor_session_id = self.cache.create_session(
                        job_id=job_id,
                        project=job["project"],
                        sql=job.get("sql"),
                    )
                except Exception:
                    local_warnings.append(
                        "The result succeeded, but a job-bound pagination cursor could not be saved. "
                        "Retry `job result` with a larger --max-rows value instead of reusing an old cursor."
                    )
            if cursor_session_id is not None:
                next_cursor = encode_cursor(
                    offset + returned_rows,
                    session_id=cursor_session_id,
                )
        page_rows, masked_columns = self._mask_sensitive_rows(page_rows, schema)
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

    def _bound_job_result_cursor(
        self,
        cursor: 'str | None',
        *,
        job_id: 'str',
        project: 'str',
    ) -> 'tuple[int, int | None]':
        offset, session_id = decode_cursor(cursor)
        if cursor is None:
            return offset, session_id
        if session_id is None:
            raise ValidationError(
                "The job-result cursor is not bound to a job.",
                suggestion="Use the next_cursor returned by the latest `job result` response.",
            )
        try:
            session = self._read_only_cache().get_session(session_id)
        except Exception as exc:
            raise ValidationError(
                "The job-result pagination context could not be read locally.",
                suggestion="Start from `job result <job_id>` without --cursor.",
            ) from exc
        if not session:
            raise ValidationError(
                "The job-result pagination context no longer exists.",
                suggestion="Start from `job result <job_id>` without --cursor.",
            )
        if str(session.get("job_id") or "") != job_id:
            raise ValidationError(
                "The pagination cursor belongs to a different job.",
                suggestion=f"Use a cursor returned for job `{job_id}`, or omit --cursor.",
            )
        if str(session.get("project") or "") != project:
            raise ValidationError(
                "The pagination cursor belongs to a different project.",
                suggestion=f"Use a cursor returned for project `{project}`, or omit --cursor.",
            )
        return offset, session_id

    def cancel_job(self, job_id: 'str', *, project: 'str | None' = None) -> 'Envelope':
        if self.remote_jobs:
            resolved = self._resolve_remote_job_id(job_id, project=project)
            if resolved.subquery_id is not None:
                raise ValidationError(
                    "Composite MCQA cancellation is not yet supported; refusing to cancel the outer session instance."
                )
            info = self.backend.cancel_job(resolved.instance_id, project=resolved.project)
            cancel_requested = info.stage == "cancel_requested"
            already_terminal = (
                not cancel_requested
                and info.status in {"success", "failure", "cancelled", "completed", "failed"}
            )
            cancelled = info.status == "cancelled" or (
                bool(info.failure_reason)
                and "cancel" in info.failure_reason.lower()
                and not cancel_requested
            )
            if cancelled:
                outcome = "cancelled"
            elif cancel_requested:
                outcome = "cancel_requested"
            elif already_terminal:
                outcome = "already_terminal"
            else:
                outcome = "state_observed"
            observed_job_status = "running" if cancel_requested else info.status
            envelope = Envelope(
                command="job.cancel",
                status="success",
                data={
                    "job_id": resolved.external_job_id,
                    "cancelled": cancelled,
                    "cancel_requested": cancel_requested,
                    "already_terminal": already_terminal,
                    "outcome": outcome,
                    "job_status": observed_job_status,
                },
                metadata={
                    "job_id": resolved.external_job_id,
                    "project": info.project,
                    "updated_at": info.updated_at,
                    "logview": info.logview,
                },
                agent_hints=AgentHints(
                    actions=[
                        action("job.status", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": info.project}),
                    ],
                    warnings=info.warnings,
                ),
            )
            self.log("job.cancel", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        if project and project != job["project"]:
            raise ValidationError(
                f"Job `{job_id}` belongs to project `{job['project']}`, not `{project}`."
            )
        if job["status"] == "success":
            raise ValidationError("The job is already complete and cannot be cancelled.")
        if job.get("cancelled"):
            envelope = Envelope(
                command="job.cancel",
                status="success",
                data={
                    "job_id": job_id,
                    "cancelled": True,
                    "cancel_requested": False,
                    "already_terminal": True,
                    "outcome": "already_cancelled",
                    "job_status": job["status"],
                },
                metadata={"project": job["project"], "updated_at": job["updated_at"]},
                agent_hints=AgentHints(
                    actions=[
                        action(
                            "job.status",
                            data={"job_id": job_id},
                            metadata={"project": job["project"]},
                        ),
                    ],
                    warnings=["The job was already cancelled; no new cancellation was sent."],
                ),
            )
            self.log("job.cancel", envelope.status, envelope.metadata)
            return envelope
        updated = jobs.update_job(job_id, status="cancelled", progress=0, cancelled=True)
        envelope = Envelope(
            command="job.cancel",
            status="success",
            data={
                "job_id": job_id,
                "cancelled": True,
                "cancel_requested": False,
                "already_terminal": False,
                "outcome": "cancelled",
                "job_status": updated["status"],
            },
            metadata={"project": updated["project"], "updated_at": updated["updated_at"]},
            agent_hints=AgentHints(
                actions=[
                    action("job.status", data={"job_id": job_id}, metadata={"project": updated["project"]}),
                ],
            ),
        )
        self.log("job.cancel", envelope.status, envelope.metadata)
        return envelope

    def job_diagnose(self, job_id: 'str', *, project: 'str | None' = None) -> 'Envelope':
        if self.remote_jobs:
            resolved = self._resolve_remote_job_id(job_id, project=project)
            payload = self.backend.diagnose_job(
                resolved.instance_id,
                project=resolved.project,
                session_context=resolved.session_context,
            )
            payload = dict(payload)
            payload["job_id"] = resolved.external_job_id
            envelope = Envelope(
                command="job.diagnose",
                status="success",
                data=payload,
                metadata={"project": resolved.project, "job_id": resolved.external_job_id},
                agent_hints=AgentHints(
                    actions=[
                        action("job.status", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project}),
                        action("job.result", data={"job_id": resolved.external_job_id}, metadata={"job_id": resolved.external_job_id, "project": resolved.project}),
                    ],
                ),
            )
            self.log("job.diagnose", envelope.status, envelope.metadata)
            return envelope

        jobs = self._ensure_job_store()
        job = jobs.get_job(job_id)
        if project and project != job["project"]:
            raise ValidationError(
                f"Job `{job_id}` belongs to project `{job['project']}`, not `{project}`."
            )
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

    def list_jobs(
        self,
        *,
        limit: 'int' = 20,
        project: 'str | None' = None,
    ) -> 'Envelope':
        if self.remote_jobs:
            target_project = project or self.config.default_project
            jobs, has_more = self.backend.list_jobs(
                project=target_project, limit=limit
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
                metadata={"backend": "odps", "project": target_project},
                agent_hints=AgentHints(
                    actions=[
                        action("job.status", data={}, metadata={"project": target_project}),
                        action("job.wait", data={}, metadata={"project": target_project}),
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
        namespace_model = "3-tier" if effective_schema else "unknown"
        namespace_warnings: list[str] = []

        # A missing schema argument does not prove that the project is 2-tier.
        # Schema-enabled projects also default to the ``default`` schema when
        # callers omit --schema. Probe the schema API so the response does not
        # mislabel those projects or emit unsafe bare qualified names.
        if effective_schema is None:
            try:
                schemas = self.backend.list_schemas(project=target_project)
            except TwoTierNamespaceError:
                # This subtype is emitted only when MaxCompute explicitly says
                # that the project is not using the 3-tier namespace model.
                namespace_model = "2-tier"
            except Exception:
                # Permission, network, and unsupported backend failures are not
                # evidence of a 2-tier namespace. Keep listing tables, but mark
                # the namespace as unresolved instead of guessing.
                namespace_warnings.append(
                    "Could not verify the project's namespace model. Run "
                    "`maxc meta list-schemas --project "
                    f"{target_project} --json` and inspect its envelope before "
                    "choosing a table-name shape."
                )
            else:
                namespace_model = "3-tier"
                schema_names = {
                    str(item.get("name"))
                    for item in schemas
                    if isinstance(item, dict) and item.get("name")
                }
                if "default" in schema_names:
                    effective_schema = "default"
                else:
                    namespace_warnings.append(
                        "The project uses 3-tier namespaces, but no active "
                        "schema was resolved. Pass `--schema <name>` before "
                        "using a returned table name."
                    )

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

        # Try the cache only when its schema key can be resolved exactly. The
        # cache stores 2-tier tables under the legacy ``default`` key; passing
        # None would mix every cached schema and recreate ambiguous bare names.
        cache_schema = effective_schema
        if namespace_model == "2-tier":
            cache_schema = "default"
        use_cache = cache_schema is not None
        cached_tables = (
            self.cache.get_all_cached_tables(target_project, schema_name=cache_schema)
            if use_cache
            else []
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
                    "schema_name": effective_schema if namespace_model == "3-tier" else None,
                    "qualified_name": (
                        f"{effective_schema}.{table.get('table_name')}"
                        if effective_schema
                        else table.get("table_name") if namespace_model == "2-tier" else None
                    ),
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
                project=target_project,
                limit=limit,
                offset=offset,
            )
            source = "backend"
            rows = [
                {
                    "table_name": t.name,
                    "schema_name": effective_schema if namespace_model == "3-tier" else None,
                    "qualified_name": (
                        f"{effective_schema}.{t.name}"
                        if effective_schema
                        else t.name if namespace_model == "2-tier" else None
                    ),
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

        schema_label = effective_schema
        insights = [f"Table list served from {source}."]
        if namespace_model == "3-tier" and effective_schema:
            insights.append(f"Use schema-qualified names in SQL: `{schema_label}.<table_name>`")
        elif namespace_model == "3-tier":
            insights.append("Project uses 3-tier namespaces; pass --schema before using a table name.")
        elif namespace_model == "2-tier":
            insights.append("Project uses 2-tier namespaces; use unqualified table names in the current project.")
        else:
            insights.append("Project namespace model is unresolved; do not assume a 2-tier or 3-tier table-name shape.")

        data = {
            "tables": rows,
            "total": len(rows),
            "schema": schema_label,
            "namespace_model": namespace_model,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "limit": limit,
            "offset": offset,
        }
        if namespace_model == "unknown" or (namespace_model == "3-tier" and not effective_schema):
            next_actions = [action("meta.list-schemas", data=data, metadata=metadata)]
        else:
            next_actions = [
                action("meta.describe", data=data, metadata=metadata),
                action("data.sample", data=data, metadata=metadata),
            ]

        envelope = Envelope(
            command="meta.list-tables",
            status="success",
            data=data,
            metadata=metadata,
            agent_hints=AgentHints(
                actions=next_actions,
                insights=insights,
                warnings=namespace_warnings,
            ),
        )
        self.log("meta.list-tables", envelope.status, envelope.metadata)
        return envelope

    def _resolve_table_scope(
        self,
        table_name: 'str',
        *,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'tuple[str, str | None, str, str, str]':
        """Resolve one table reference into backend and cache coordinates.

        The backend needs an unqualified table plus independent project/schema
        arguments, while the local cache always uses a schema key (``default``
        for two-tier projects). Keeping those representations separate avoids
        sending ``sales.orders`` as a literal table name to PyODPS and avoids
        looking up a cross-schema table in the active schema's cache row.
        """
        value = table_name.strip()
        quote_table_name(value)
        parts = value.split(".")
        explicit_project = (project or "").strip() or None
        explicit_schema = (schema or "").strip() or None
        target_project = (explicit_project or self.config.default_project or "").strip()
        target_schema = (
            explicit_schema or (self.config.default_schema or "").strip() or None
        )
        target_table = parts[-1]
        explicit_project_qualification = False

        if len(parts) < 3 and not target_project:
            raise ValidationError(
                "Table metadata requires an active project.",
                suggestion=(
                    "Pass --project or set an explicit project with `session set "
                    "--project <project>`."
                ),
            )

        def probe_namespace() -> 'tuple[str, set[str]]':
            try:
                schemas = self.backend.list_schemas(project=target_project)
            except TwoTierNamespaceError:
                return "2-tier", set()
            except Exception as exc:
                raise ValidationError(
                    f"Could not verify the namespace model for project `{target_project}`.",
                    suggestion=(
                        "Run `maxc meta list-schemas --project "
                        f"{target_project} --json`, then pass an explicit "
                        "--project/--schema instead of relying on a two-part name."
                    ),
                ) from exc
            return (
                "3-tier",
                {
                    str(item.get("name"))
                    for item in schemas
                    if isinstance(item, dict) and item.get("name")
                },
            )

        if len(parts) == 3:
            parsed_project, parsed_schema, target_table = parts
            if project and parsed_project != project:
                raise ValidationError(
                    "The table project conflicts with --project.",
                    suggestion="Use one verified project identity for the metadata operation.",
                )
            if schema and parsed_schema != schema:
                raise ValidationError(
                    "The table schema conflicts with --schema.",
                    suggestion="Use one verified schema identity for the metadata operation.",
                )
            target_project = parsed_project
            target_schema = parsed_schema
        elif len(parts) == 2:
            qualifier, target_table = parts
            if explicit_schema is not None:
                if qualifier != explicit_schema:
                    raise ValidationError(
                        "The table schema conflicts with --schema.",
                        suggestion="Use one verified schema identity for the metadata operation.",
                    )
                target_schema = explicit_schema
            elif explicit_project is not None and qualifier == explicit_project:
                # Preserve the established two-tier ``project.table`` form.
                target_schema = None
                explicit_project_qualification = True
            elif explicit_project is not None:
                # --project fixes the project coordinate, so the remaining
                # qualifier can only be the schema coordinate. The backend
                # will validate whether that schema exists.
                target_schema = qualifier
            else:
                namespace_model, schema_names = probe_namespace()
                if namespace_model == "2-tier":
                    if qualifier == target_project:
                        target_schema = None
                        explicit_project_qualification = True
                    else:
                        raise ValidationError(
                            f"Two-part table name `{value}` is ambiguous in a 2-tier project.",
                            suggestion=(
                                f"Pass `--project {qualifier}` if `{qualifier}` is a project, "
                                "or use the unqualified table name for the active project."
                            ),
                        )
                    schema_names = set()
                    # The namespace has been fully resolved as project.table.
                    # Skip the 3-tier schema validation below.
                    namespace_model = "2-tier-resolved"
                if schema_names and qualifier not in schema_names:
                    raise ValidationError(
                        f"Schema `{qualifier}` was not returned for project `{target_project}`.",
                        suggestion=(
                            f"Run `maxc meta list-schemas --project {target_project} "
                            "--json` and use an exact returned schema name."
                        ),
                    )
                if namespace_model != "2-tier-resolved":
                    target_schema = qualifier
        if not target_project:
            raise ValidationError(
                "Table metadata requires an active project.",
                suggestion="Pass --project or set an explicit project with `session set --project <project>`.",
            )

        cache_schema = target_schema or "default"
        if target_schema:
            qualified_name = f"{target_schema}.{target_table}"
        elif explicit_project_qualification:
            qualified_name = f"{target_project}.{target_table}"
        else:
            qualified_name = target_table
        return target_project, target_schema, cache_schema, target_table, qualified_name

    def meta_describe(
        self,
        table_name: 'str',
        full: 'bool' = False,
        project: 'str | None' = None,
        *,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        started = monotonic()
        (
            target_project,
            effective_schema,
            cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)

        # Try to get from cache first
        cached_table = self.cache.get_cached_table(
            target_project,
            target_table,
            schema_name=cache_schema,
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
                name=target_table,
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
                    target_table, project=target_project, schema=effective_schema,
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
                target_table, project=target_project, schema=effective_schema,
            )
            source = "live"
            warnings = []

        # Get semantic metadata from cache
        semantic = self.cache.get_semantic(
            project=target_project,
            table_name=target_table,
            schema_name=cache_schema,
        )

        if not semantic:
            warnings.append(
                "Missing semantic metadata. Agent should generate it using its own LLM and save with: maxc meta semantic set"
            )

        payload = self._table_payload(table, full=full)
        payload["table_name"] = target_table
        payload["qualified_name"] = qualified_name
        payload["schema_name"] = effective_schema
        
        # Add hint about --full flag in summary mode
        if not full and payload.get("has_more_columns"):
            warnings.append(
                f"Showing first 10 columns only. Use --full to see all {payload['column_count']} columns."
            )
        
        # Add semantic information to the payload
        payload["semantic"] = semantic

        meta_metadata = {
                "project": target_project,
                "schema": effective_schema,
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
        namespace_model = "3-tier" if effective_schema else "unknown"
        if effective_schema is None:
            try:
                self.backend.list_schemas(project=target_project)
            except TwoTierNamespaceError:
                namespace_model = "2-tier"
            except Exception:
                namespace_model = "unknown"
            else:
                namespace_model = "3-tier"

        # Priority: Catalog API → cache → live scan
        matches: list[dict[str, Any]] = []
        source = "live"
        catalog_available = False

        # Empty keyword is not a search — skip Catalog API (which would
        # return a random page of tables) and go straight to list-tables.
        use_catalog = bool(keyword and keyword.strip())

        if use_catalog and self.backend is not None:
            catalog_matches = self.backend.catalog_search_tables(
                keyword, schema=effective_schema, project=target_project,
            )
            if catalog_matches is not None:
                matches = catalog_matches
                source = "catalog"
                catalog_available = True

        if not catalog_available:
            cached_tables = self.cache.get_all_cached_tables(
                target_project,
                schema_name=(
                    "default"
                    if namespace_model == "2-tier" and effective_schema is None
                    else effective_schema
                ),
            )
            if cached_tables:
                matches = self._search_in_cache(
                    keyword,
                    cached_tables,
                    namespace_model=namespace_model,
                )
                source = "cache"
            else:
                matches = self.backend.search_tables(
                    keyword,
                    schema=effective_schema,
                    project=target_project,
                )
                source = "live"

        matches = self._normalize_table_matches(
            matches,
            namespace_model=namespace_model,
            schema=effective_schema,
        )

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
            "namespace_model": namespace_model,
        }
        search_metadata = self._cache_metadata(
                project=target_project,
                source=source,
                query_time_ms=int((monotonic() - started) * 1000) if source in ("live", "catalog") else None,
                schema_name=effective_schema,
            )
        if effective_schema:
            search_metadata["schema"] = effective_schema
        if namespace_model == "unknown":
            search_actions = [
                action("meta.list-schemas", data=search_data, metadata=search_metadata)
            ]
            search_warnings = [
                "The project namespace model could not be verified; inspect schemas before choosing a table-name shape."
            ]
        else:
            search_actions = [
                action("meta.describe", data=search_data, metadata=search_metadata),
                action("data.sample", data=search_data, metadata=search_metadata),
            ]
            search_warnings = []
        if source != "catalog" and not cached_tables:
            search_warnings.append(
                "No metadata cache was used. Run `maxc cache build` to speed up future lookups."
            )
        envelope = Envelope(
            command="meta.search",
            status="success",
            data=search_data,
            metadata=search_metadata,
            agent_hints=AgentHints(
                actions=search_actions,
                warnings=search_warnings,
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
        namespace_model = "3-tier" if effective_schema else "unknown"
        if effective_schema is None:
            try:
                self.backend.list_schemas(project=target_project)
            except TwoTierNamespaceError:
                namespace_model = "2-tier"
            except Exception:
                namespace_model = "unknown"
            else:
                namespace_model = "3-tier"
        cached_tables = self.cache.get_all_cached_tables(
            target_project,
            schema_name=(
                "default"
                if namespace_model == "2-tier" and effective_schema is None
                else effective_schema
            ),
        )
        if cached_tables:
            matches = self._search_columns_in_cache(
                keyword,
                cached_tables,
                namespace_model=namespace_model,
            )
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
            "namespace_model": namespace_model,
        }
        sc_metadata = self._cache_metadata(
                project=target_project,
                source=source,
                query_time_ms=int((monotonic() - started) * 1000) if source not in ("cache", "cache_required") else None,
                schema_name=effective_schema,
            )
        if effective_schema:
            sc_metadata["schema"] = effective_schema
        if namespace_model == "unknown":
            column_actions = [
                action("meta.list-schemas", data=sc_data, metadata=sc_metadata)
            ]
            warnings.append(
                "The project namespace model could not be verified; inspect schemas before choosing a table-name shape."
            )
        else:
            column_actions = [
                action("meta.describe", data=sc_data, metadata=sc_metadata),
                action("meta.search", data=sc_data, metadata=sc_metadata),
            ]
        envelope = Envelope(
            command="meta.search-columns",
            status="success",
            data=sc_data,
            metadata=sc_metadata,
            agent_hints=AgentHints(
                actions=column_actions,
                warnings=warnings,
            ),
        )
        self.log("meta.search-columns", envelope.status, envelope.metadata)
        return envelope

    def _search_in_cache(
        self,
        keyword: 'str',
        cached_tables: 'list[dict]',
        *,
        namespace_model: 'str' = "unknown",
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
                schema_name = (
                    None if namespace_model == "2-tier" else table.get("schema_name")
                )
                matches.append({
                    "table_name": table["table_name"],
                    "schema_name": schema_name,
                    "qualified_name": (
                        f"{schema_name}.{table['table_name']}"
                        if schema_name
                        else table["table_name"]
                    ),
                    "description": table.get("description"),
                    "score": score,
                    "matched_columns": list(set(matched_columns))[:5],
                })
        return sorted(matches, key=lambda x: -x["score"])[:20]

    def _search_columns_in_cache(
        self,
        keyword: 'str',
        cached_tables: 'list[dict]',
        *,
        namespace_model: 'str' = "unknown",
    ) -> 'list[dict]':
        """Search columns in cache."""
        tokens = [t.lower() for t in keyword.split() if t.strip()] or [keyword.lower()]
        matches = []
        for table in cached_tables:
            for col in table.get("columns", []):
                text = f"{col.get('name', '')} {col.get('comment', '')}".lower()
                score = sum(2 for token in tokens if token in text)
                if score:
                    schema_name = (
                        None if namespace_model == "2-tier" else table.get("schema_name")
                    )
                    matches.append({
                        "table_name": table["table_name"],
                        "schema_name": schema_name,
                        "qualified_name": (
                            f"{schema_name}.{table['table_name']}"
                            if schema_name
                            else table["table_name"]
                        ),
                        "column_name": col["name"],
                        "column_type": col.get("type"),
                        "column_comment": col.get("comment"),
                        "score": score,
                    })
        return sorted(matches, key=lambda x: -x["score"])[:50]

    def _normalize_table_matches(
        self,
        matches: 'list[dict[str, Any]]',
        *,
        namespace_model: 'str',
        schema: 'str | None',
    ) -> 'list[dict[str, Any]]':
        """Give every discovery match one explicit, non-invented identity."""
        normalized: list[dict[str, Any]] = []
        for raw in matches:
            item = dict(raw)
            table_name = item.get("table_name") or item.get("name")
            if not table_name:
                continue
            schema_name = item.get("schema_name") or item.get("schema") or schema
            if namespace_model == "2-tier":
                schema_name = None
                qualified_name = str(table_name)
            elif namespace_model == "3-tier" and schema_name:
                qualified_name = f"{schema_name}.{table_name}"
            else:
                # Unknown namespace is evidence that qualification has not yet
                # been established. Do not turn the cache's internal `default`
                # sentinel into a claimed public schema identity.
                schema_name = None
                qualified_name = None
            item["table_name"] = str(table_name)
            item["schema_name"] = schema_name
            item["qualified_name"] = qualified_name
            if "description" not in item and "comment" in item:
                item["description"] = item.get("comment")
            normalized.append(item)
        return normalized

    # ========== Semantic Metadata Methods ==========

    def _resolve_semantic_table_scope(
        self,
        table_name: 'str',
        *,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'tuple[str, str, str]':
        """Resolve one local semantic record without guessing a project/schema.

        A two-part name is interpreted as ``schema.table``. Cross-project access
        should use ``--project`` or an unambiguous ``project.schema.table`` name.
        """
        value = table_name.strip()
        parts = value.split(".") if value else []
        if not parts or any(not part.strip() for part in parts) or len(parts) > 3:
            raise ValidationError(
                "Semantic metadata requires a table, schema.table, or project.schema.table name."
            )

        parsed_project: str | None = None
        parsed_schema: str | None = None
        if len(parts) == 3:
            parsed_project, parsed_schema, value = parts
        elif len(parts) == 2:
            parsed_schema, value = parts
        else:
            value = parts[0]

        if project and parsed_project and project != parsed_project:
            raise ValidationError(
                "The table project conflicts with --project.",
                suggestion="Use one verified project identity for the semantic operation.",
            )
        if schema and parsed_schema and schema != parsed_schema:
            raise ValidationError(
                "The table schema conflicts with --schema.",
                suggestion="Use one verified schema identity for the semantic operation.",
            )

        target_project = (parsed_project or project or self.config.default_project or "").strip()
        target_schema = (parsed_schema or schema or self.config.default_schema or "default").strip()
        if not target_project:
            raise ValidationError(
                "Semantic metadata requires an active project.",
                suggestion="Pass --project or set an explicit project with `session set --project <project>`.",
            )
        if not target_schema:
            raise ValidationError("Semantic metadata requires a non-empty schema.")
        return target_project, target_schema, value.strip()

    def _resolve_semantic_collection_scope(
        self,
        *,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'tuple[str, str | None]':
        target_project = (project or self.config.default_project or "").strip()
        target_schema = schema.strip() if schema is not None else None
        if not target_project:
            raise ValidationError(
                "Semantic metadata requires an active project.",
                suggestion="Pass --project or set an explicit project with `session set --project <project>`.",
            )
        if schema is not None and not target_schema:
            raise ValidationError("--schema must not be empty.")
        return target_project, target_schema

    def semantic_set(
        self,
        table_name: 'str',
        semantic_desc: 'str | None' = None,
        use_cases: 'list[str] | None' = None,
        sample_questions: 'list[str] | None' = None,
        column_semantics: 'list[dict[str, Any]] | None' = None,
        relations: 'list[dict[str, Any]] | None' = None,
        stats: 'dict[str, Any] | None' = None,
        *,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        """Set semantic metadata for a table (data provided by Agent)."""
        if not any(
            (
                semantic_desc,
                use_cases,
                sample_questions,
                column_semantics,
                relations,
                stats,
            )
        ):
            raise ValidationError(
                "Semantic metadata is required; an empty semantic set would erase useful context.",
                suggestion=(
                    "Provide at least one of --desc, --use-cases, --sample-questions, "
                    "--column-semantics, --relations, or --stats. Use `meta semantic "
                    "clear` when removal is intentional."
                ),
            )
        target_project, target_schema, target_table = self._resolve_semantic_table_scope(
            table_name,
            project=project,
            schema=schema,
        )
        try:
            self.cache.save_semantic(
                project=target_project,
                table_name=target_table,
                semantic_desc=semantic_desc or "",
                use_cases=use_cases or [],
                sample_questions=sample_questions or [],
                column_semantics=column_semantics or [],
                schema_name=target_schema,
                relations=relations,
                stats=stats,
            )

            envelope = Envelope(
                command="meta.semantic.set",
                status="success",
                data={
                    "action": "set_semantic",
                    "table_name": target_table,
                    "qualified_name": f"{target_schema}.{target_table}",
                    "has_description": bool(semantic_desc),
                    "use_cases_count": len(use_cases) if use_cases else 0,
                    "sample_questions_count": len(sample_questions) if sample_questions else 0,
                    "column_semantics_count": len(column_semantics) if column_semantics else 0,
                },
                metadata={
                    "project": target_project,
                    "schema": target_schema,
                },
                agent_hints=AgentHints(
                    actions=[
                        action(
                            "meta.describe",
                            data={"table_name": target_table},
                            metadata={"project": target_project, "schema": target_schema},
                        ),
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
                    "project": target_project,
                    "schema": target_schema,
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
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        """Get semantic metadata for a table."""
        target_project, target_schema, target_table = self._resolve_semantic_table_scope(
            table_name,
            project=project,
            schema=schema,
        )
        try:
            semantic = self._read_only_cache().get_semantic(
                project=target_project,
                table_name=target_table,
                schema_name=target_schema,
            )

            if semantic:
                envelope = Envelope(
                    command="meta.semantic.get",
                    status="success",
                    data={
                        "table_name": target_table,
                        "qualified_name": f"{target_schema}.{target_table}",
                        "semantic": semantic,
                    },
                    metadata={
                        "project": target_project,
                        "schema": target_schema,
                    },
                    agent_hints=AgentHints(
                        actions=[
                            action("meta.describe", data={"table_name": target_table}, metadata={"project": target_project, "schema": target_schema}),
                        ],
                    ),
                )
            else:
                envelope = Envelope(
                    command="meta.semantic.get",
                    status="success",
                    data={
                        "table_name": target_table,
                        "qualified_name": f"{target_schema}.{target_table}",
                        "semantic": None,
                    },
                    metadata={
                        "project": target_project,
                        "schema": target_schema,
                    },
                    agent_hints=AgentHints(
                        warnings=["No semantic metadata found. Use `maxc meta semantic set` to add metadata."],
                        actions=[
                            action(
                                "meta.semantic.set",
                                data={"table_name": target_table},
                                metadata={"project": target_project, "schema": target_schema},
                                confirmation_required=True,
                                agent_allowed=False,
                            ),
                        ],
                    ),
                )
        except CacheSnapshotBusyError as exc:
            envelope = Envelope(
                command="meta.semantic.get",
                status="failure",
                data=None,
                metadata={
                    "project": target_project,
                    "schema": target_schema,
                },
                error=exc.to_payload(),
                agent_hints=None,
            )
        except Exception as exc:
            envelope = Envelope(
                command="meta.semantic.get",
                status="failure",
                data=None,
                metadata={
                    "project": target_project,
                    "schema": target_schema,
                },
                error=ErrorPayload(
                    code="SEMANTIC_GET_ERROR",
                    message=str(exc),
                    recoverable=False,
                    suggestion="Check the error message and try again.",
                ),
                agent_hints=None,
            )

        return envelope

    def semantic_clear(
        self,
        *,
        table_name: 'str | None' = None,
        schema_name: 'str | None' = None,
        project: 'str | None' = None,
        all_semantics: 'bool' = False,
    ) -> 'Envelope':
        """Remove local semantic annotations from an explicit project scope."""
        if bool(table_name) == bool(all_semantics):
            raise ValidationError("Provide exactly one table name or all_semantics=True.")
        target_table: str | None
        if table_name:
            target_project, effective_schema, target_table = self._resolve_semantic_table_scope(
                table_name,
                project=project,
                schema=schema_name,
            )
        else:
            target_project, effective_schema = self._resolve_semantic_collection_scope(
                project=project,
                schema=schema_name,
            )
            target_table = None
        cleared = self.cache.clear_semantic(
            project=target_project,
            table_name=target_table,
            schema_name=effective_schema,
        )
        scope = "project" if all_semantics and effective_schema is None else (
            "schema" if all_semantics else "table"
        )
        data = {
            "cleared": cleared,
            "scope": scope,
            "project": target_project,
            "schema": effective_schema,
            "table_name": target_table,
        }
        warnings = [] if cleared else [
            "No semantic metadata matched the requested project/schema/table scope."
        ]
        envelope = Envelope(
            command="meta.semantic.clear",
            status="success",
            data=data,
            metadata={"project": target_project, "schema": effective_schema},
            agent_hints=AgentHints(
                actions=[
                    action(
                        "meta.semantic.list-missing",
                        metadata={"project": target_project, "schema": effective_schema},
                    )
                ],
                warnings=warnings,
            ),
        )
        self.log(
            "meta.semantic.clear",
            envelope.status,
            {
                "project": target_project,
                "schema": effective_schema,
                "table_name": target_table,
                "cleared": cleared,
            },
        )
        return envelope

    def semantic_list_missing(
        self,
        *,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        """List tables without semantic metadata."""
        target_project, target_schema = self._resolve_semantic_collection_scope(
            project=project,
            schema=schema,
        )
        try:
            cache = self._read_only_cache()
            # Get all cached tables
            all_tables = cache.get_all_cached_tables(
                project=target_project,
                schema_name=target_schema,
            )

            # Get tables with semantic metadata
            semantic_tables = cache.get_all_semantics(
                project=target_project,
                schema_name=target_schema,
            )
            semantic_table_keys = {
                (t["schema_name"], t["table_name"]) for t in semantic_tables
            }

            # Find tables missing semantic metadata
            missing = [
                t for t in all_tables
                if (t["schema_name"], t["table_name"]) not in semantic_table_keys
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
                            "qualified_name": f'{t["schema_name"]}.{t["table_name"]}',
                            "schema_name": t["schema_name"],
                            "description": t.get("description", ""),
                            "column_count": len(t.get("columns", [])),
                        }
                        for t in missing[:50]  # Limit to 50 tables
                    ],
                },
                metadata={
                    "project": target_project,
                    "schema": target_schema,
                },
                agent_hints=AgentHints(
                    insights=[f"{len(missing)} tables lack semantic metadata."],
                    warnings=warnings,
                    actions=[
                        action(
                            "meta.semantic.set",
                            data={"table_name": missing[0]["table_name"]},
                            metadata={
                                "project": target_project,
                                "schema": missing[0]["schema_name"],
                            },
                            confirmation_required=True,
                            agent_allowed=False,
                        )
                    ] if missing else [],
                ),
            )
        except CacheSnapshotBusyError as exc:
            envelope = Envelope(
                command="meta.semantic.list-missing",
                status="failure",
                data=None,
                metadata={
                    "project": target_project,
                    "schema": target_schema,
                },
                error=exc.to_payload(),
                agent_hints=None,
            )
        except Exception as exc:
            envelope = Envelope(
                command="meta.semantic.list-missing",
                status="failure",
                data=None,
                metadata={
                    "project": target_project,
                    "schema": target_schema,
                },
                error=ErrorPayload(
                    code="SEMANTIC_LIST_MISSING_ERROR",
                    message=str(exc),
                    recoverable=False,
                    suggestion="Check the error message and try again.",
                ),
                agent_hints=None,
            )

        return envelope

    def meta_latest_partition(
        self,
        table_name: 'str',
        project: 'str | None' = None,
        *,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        (
            target_project,
            effective_schema,
            _cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)
        payload, warnings = self.backend.latest_partition_info(
            target_table, project=target_project, schema=effective_schema,
        )
        payload = dict(payload)
        payload.update(
            {
                "table_name": target_table,
                "schema_name": effective_schema,
                "qualified_name": qualified_name,
            }
        )
        lp_metadata = {"project": target_project, "schema": effective_schema}
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
        (
            target_project,
            effective_schema,
            _cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)
        payload, warnings = self.backend.freshness_info(
            target_table, project=target_project, schema=effective_schema,
        )
        payload = dict(payload)
        payload.update(
            {
                "table_name": target_table,
                "schema_name": effective_schema,
                "qualified_name": qualified_name,
            }
        )
        fresh_metadata = {"project": target_project, "schema": effective_schema}
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
        """Build metadata cache for a verified namespace.

        Args:
            project: Project name
            max_workers: Number of concurrent workers
            schema_name: Specific schema to build. Required for 3-tier projects.
            async_mode: Deprecated compatibility flag. Builds complete synchronously.
            progress_callback: Optional callback for build progress events
        """
        import uuid

        target_project = project or self.config.default_project
        effective_schema = schema_name or self.config.default_schema
        namespace_model = "3-tier" if effective_schema else "unknown"
        if effective_schema is None:
            try:
                self.backend.list_schemas(project=target_project)
            except TwoTierNamespaceError:
                namespace_model = "2-tier"
            except Exception as exc:
                raise ValidationError(
                    "Could not verify whether the project uses a 2-tier or "
                    "3-tier namespace.",
                    suggestion=(
                        f"Run `maxc meta list-schemas --project {target_project} "
                        "--json`, then pass --schema for a 3-tier project."
                    ),
                ) from exc
            else:
                raise ValidationError(
                    f"Project `{target_project}` uses 3-tier namespaces; cache "
                    "build requires an explicit schema.",
                    suggestion=(
                        f"Run `maxc meta list-schemas --project {target_project} "
                        "--json`, then rerun cache build with --schema <name>."
                    ),
                )
        if progress_callback is not None:
            progress_callback(
                {
                    "type": "listing_start",
                    "project": target_project,
                    "schema_name": effective_schema,
                }
            )

        all_tables, _ = self.backend.list_tables(
            schema=effective_schema,
            project=target_project,
        )
        tables = all_tables

        if progress_callback is not None:
            progress_callback(
                {
                    "type": "listing_complete",
                    "project": target_project,
                    "schema_name": effective_schema,
                    "total_tables": len(tables),
                }
            )

        build_id = str(uuid.uuid4())[:8]

        envelope = self._build_cache_sync(
            target_project,
            build_id,
            tables,
            max_workers,
            effective_schema,
            namespace_model=namespace_model,
            progress_callback=progress_callback,
        )
        if async_mode:
            # A non-daemon thread does not make a short-lived CLI command
            # asynchronous: Python waits for it during interpreter shutdown.
            # Keep accepting the historical flag, but report the behavior
            # truthfully until a supervised detached-worker design exists.
            envelope.data["async_requested"] = True
            envelope.agent_hints = envelope.agent_hints or AgentHints()
            envelope.agent_hints.warnings.insert(
                0,
                "`--async` is deprecated and no detached worker is available; "
                "this cache build completed synchronously.",
            )
        return envelope

    def _build_cache_sync(
        self,
        project: 'str',
        build_id: 'str',
        tables: 'list',
        max_workers: 'int',
        schema_name: 'str | None' = None,
        namespace_model: 'str | None' = None,
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

        resolved_namespace_model = namespace_model or (
            "3-tier" if schema_name else "2-tier"
        )
        write_schema = schema_name or "default"

        def fetch_and_cache(
            table_name: 'str',
        ) -> 'tuple[str, str | None]':
            try:
                describe_metadata = getattr(
                    self.backend,
                    "describe_table_metadata",
                    self.backend.describe_table,
                )
                full_table = describe_metadata(
                    table_name, project=project, schema=schema_name,
                )
                existing = self.cache.get_cached_table(project, full_table.name, write_schema)
                columns = [
                    {"name": c.name, "type": c.type, "comment": c.comment}
                    for c in full_table.columns
                ]
                self.cache.cache_table(
                    project=project,
                    table_name=full_table.name,
                    description=full_table.description,
                    columns=columns,
                    partitions=[column.name for column in full_table.partition_columns],
                    schema_name=write_schema,
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
                    processed_count = cached_count + len(errors)
                    self.cache.update_build_progress(
                        project, build_id, processed_count, len(errors)
                    )
                    if progress_callback is not None:
                        progress_callback(
                            {
                                "type": "progress",
                                "project": project,
                                "schema_name": schema_name,
                                "build_id": build_id,
                                "cached_tables": cached_count,
                                "processed_tables": processed_count,
                                "failed_tables": len(errors),
                                "total_tables": len(tables),
                            }
                        )

        all_failed = bool(tables) and cached_count == 0 and bool(errors)
        if all_failed:
            build_status = "failed"
            self.cache.complete_build(
                project,
                build_id,
                error_message=f"{len(errors)} errors",
                status=build_status,
            )
        elif errors:
            build_status = "completed_with_errors"
            self.cache.complete_build(
                project,
                build_id,
                error_message=f"{len(errors)} errors",
                status=build_status,
            )
        else:
            build_status = "completed" if tables else "empty"
            self.cache.complete_build(project, build_id)

        stats = self.cache.get_cache_stats(project, write_schema)
        elapsed_ms = int((monotonic() - started) * 1000)
        build_metadata = {
            "project": project,
            "schema": schema_name,
            "build_id": build_id,
            "namespace_model": resolved_namespace_model,
        }
        build_data = {
            "action": "build",
            "build_id": build_id,
            "mode": "sync",
            "build_status": build_status,
            "scope": "project" if resolved_namespace_model == "2-tier" else "schema",
            "schema_name": schema_name,
            "namespace_model": resolved_namespace_model,
            "tables_scanned": len(tables),
            "cache_entries_created": created_count,
            "cache_entries_updated": updated_count,
            "cached_tables": cached_count,
            "processed_tables": cached_count + len(errors),
            "total_tables": len(tables),
            "tables_failed": len(errors),
            "elapsed_ms": elapsed_ms,
            "cache_location": str(self.cache.db_path),
            "errors": errors[:10] if errors else [],
            "stats": stats,
        }
        if all_failed:
            build_actions = [action("cache.build", data=build_data, metadata=build_metadata)]
            build_warnings = ["No table metadata was cached."]
            build_error = ErrorPayload(
                code="CACHE_BUILD_FAILED",
                message=f"Failed to cache all {len(tables)} table(s).",
                suggestion=(
                    "Inspect data.errors, verify project/schema access, and retry cache build."
                ),
                recoverable=True,
                context={"errors": errors[:10]},
            )
        elif not tables:
            build_actions = [
                action("meta.list-tables", data=build_data, metadata=build_metadata),
                action("cache.build", data=build_data, metadata=build_metadata),
            ]
            build_warnings = [
                "No tables were found. Verify the project and schema before treating the cache as ready."
            ]
            build_error = None
        else:
            build_actions = [
                action("meta.search", data={}, metadata=build_metadata),
                action("meta.search-columns", data={}, metadata=build_metadata),
            ]
            build_warnings = (
                [f"Failed to cache {len(errors)} table(s)."] if errors else []
            )
            build_error = None

        envelope = Envelope(
            command="cache.build",
            status="failure" if all_failed else "success",
            data=build_data,
            metadata=build_metadata,
            error=build_error,
            agent_hints=AgentHints(
                actions=build_actions,
                warnings=build_warnings,
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
                    "processed_tables": cached_count + len(errors),
                    "failed_tables": len(errors),
                    "total_tables": len(tables),
                    "status": envelope.status,
                    "build_status": envelope.data["build_status"],
                }
            )
        self.log("cache.build", envelope.status, envelope.metadata)
        return envelope

    def cache_build_status(
        self, *, project: 'str | None' = None, build_id: 'str | None' = None
    ) -> 'Envelope':
        """Get cache build status."""
        target_project = project or self.config.default_project
        try:
            status = self._read_only_cache().get_build_status(target_project, build_id)
        except CacheSnapshotBusyError as exc:
            metadata = {"project": target_project}
            if build_id:
                metadata["build_id"] = build_id
            return Envelope(
                command="cache.build-status",
                status="failure",
                data=None,
                metadata=metadata,
                error=exc.to_payload(),
            )

        if status:
            status = {"found": True, **status}
            bs_metadata = {"project": target_project, "build_id": status["build_id"]}
            if status["status"] in ["failed", "completed", "completed_with_errors"]:
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
            if build_id:
                bs_metadata["build_id"] = build_id
            envelope = Envelope(
                command="cache.build-status",
                status="success",
                data={
                    "found": False,
                    "project": target_project,
                    "build_id": build_id,
                    "status": "not_found",
                    "message": "No cache build record was found.",
                },
                metadata=bs_metadata,
                agent_hints=AgentHints(
                    actions=[action("cache.build", data={}, metadata=bs_metadata)],
                ),
            )
        return envelope

    def cache_status(self, *, project: 'str | None' = None, schema_name: 'str | None' = None) -> 'Envelope':
        """Get cache status."""
        target_project = project or self.config.default_project
        cache = self._read_only_cache()
        try:
            stats = cache.get_cache_stats(target_project, schema_name)
            schemas = cache.get_schemas(target_project)
            semantic_count = cache.get_semantic_count(target_project, schema_name)
            fts_count = cache.get_fts_count(target_project, schema_name)
        except CacheSnapshotBusyError as exc:
            return Envelope(
                command="cache.status",
                status="failure",
                data=None,
                metadata={"project": target_project, "schema": schema_name},
                error=exc.to_payload(),
            )

        cs_data = {
                **stats,
                "schemas": schemas,
                "schema_name": schema_name,
                "semantic_count": semantic_count,
                "fts_available": cache.fts_available,
                "fts_entries": fts_count,
            }
        cs_metadata = {"project": target_project, "schema": schema_name}
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
        # Counting a dry-run must be observational: do not create/migrate the
        # SQLite database merely to report that an empty cache would delete
        # zero rows. The same stable snapshot also supplies the pre-delete
        # counts for an explicit --force operation.
        read_cache = self._read_only_cache()
        cache_stats = read_cache.get_cache_stats(target_project, schema_name)
        table_count = int(cache_stats.get("table_count", 0))
        semantic_count = read_cache.get_semantic_count(target_project, schema_name)
        fts_count = read_cache.get_fts_count(target_project, schema_name)
        cc_metadata = {"project": target_project, "schema": schema_name}
        scope = f"project `{target_project}`"
        if schema_name:
            scope += f", schema `{schema_name}`"
        semantic_note = ""
        if semantic_count:
            semantic_note = (
                f" {semantic_count} semantic annotation(s) are preserved; "
                "clear them explicitly with `meta semantic clear`."
            )

        if dry_run or not force:
            if dry_run:
                warning = (
                    f"Dry run: {table_count} cached table entries in {scope} would be cleared. "
                    f"No changes were made.{semantic_note}"
                )
            else:
                warning = (
                    f"{table_count} cached table entries in {scope} would be cleared. "
                    "Re-run with `--force` to apply, or `--dry-run` to acknowledge explicitly."
                    f"{semantic_note}"
                )
            cc_data = {
                "deleted_tables": 0,
                "would_delete": table_count,
                "would_delete_tables": table_count,
                "would_delete_fts_entries": fts_count,
                "preserved_semantics": semantic_count,
                "schema_name": schema_name,
                "dry_run": True,
            }
            actions = []
            if not dry_run and table_count > 0:
                actions.append(
                    action(
                        "cache.clear",
                        data=cc_data,
                        metadata=cc_metadata,
                        effect="local_write",
                        confirmation_required=True,
                        agent_allowed=False,
                    )
                )
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

        if table_count or (fts_count or 0):
            deleted = self.cache.clear_table_cache(target_project, schema_name)
        else:
            deleted = 0
        cc_data = {
            "deleted_tables": deleted,
            "deleted_fts_entries": fts_count,
            "preserved_semantics": semantic_count,
            "schema_name": schema_name,
            "dry_run": False,
        }
        envelope = Envelope(
            command="cache.clear",
            status="success",
            data=cc_data,
            metadata=cc_metadata,
            agent_hints=AgentHints(
                actions=[action("cache.build", data=cc_data, metadata=cc_metadata)],
                warnings=[semantic_note.strip()] if semantic_note else [],
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
        (
            target_project,
            effective_schema,
            _cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)
        payload, warnings = self.backend.list_partitions(
            target_table,
            limit=limit,
            project=target_project,
            schema=effective_schema,
        )
        payload = dict(payload)
        payload.update(
            {
                "table_name": target_table,
                "schema_name": effective_schema,
                "qualified_name": qualified_name,
            }
        )
        mp_metadata = {"project": target_project, "schema": effective_schema}
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
                warnings=[] if rows else [
                    "The project supports 3-tier namespaces, but no schemas were returned. "
                    "Verify schema visibility before choosing a table-name shape."
                ],
            ),
        )
        self.log("meta.list-schemas", envelope.status, envelope.metadata)
        return envelope

    def session_set(
        self,
        project: 'str | None' = None,
        schema: 'str | None' = None,
        *,
        target_config_path: 'Path | None' = None,
    ) -> 'Envelope':
        """Set default project and/or schema by writing to ~/.maxc/config.yaml.

        Mirrors `gcloud config set project` / `kubectl config use-context`: the
        change persists in the global config file. If a higher-precedence config
        (e.g., ./.maxc/config.yaml) shadows the value, a warning is emitted but
        the write still happens — the in-memory value is updated for the current
        invocation.

        When ``target_config_path`` is given (i.e. the user passed ``--config``),
        the write goes to that file instead, so a subsequent ``session show
        --config <same>`` round-trips correctly.
        """
        target_path = target_config_path or default_global_config_path()

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
            changes.append(f"project set to `{project}`")
            if self.config.auth.project and project != self.config.auth.project:
                warnings.append(
                    f"Project (`{project}`) differs from the project saved in auth config "
                    f"(`{self.config.auth.project}`). Operations will use `{project}`, but credentials "
                    f"were configured for `{self.config.auth.project}`. Run `auth whoami` to verify access."
                )

        if schema:
            changes.append(f"schema set to `{schema}`")
        elif schema is not None:
            changes.append("schema cleared")

        def update_session(config_payload: 'dict[str, Any]') -> 'None':
            if project:
                config_payload["default_project"] = project
            if schema:
                config_payload["default_schema"] = schema
            elif schema is not None:
                config_payload.pop("default_schema", None)
            return None

        migrate_legacy_session_override(target_path)
        update_config_mapping(target_path, update_session)

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
    
    def session_unset(
        self, *, target_config_path: 'Path | None' = None
    ) -> 'Envelope':
        """Remove default_project / default_schema from ~/.maxc/config.yaml.

        Project-level config files in the working directory are NOT modified, since
        they may be checked into version control. Edit those by hand if needed.

        When ``target_config_path`` is given (``--config``), unset operates on
        that file instead of the global one.
        """
        target_path = target_config_path or default_global_config_path()
        migrate_legacy_session_override(target_path)
        cleared: list[str] = []

        if target_path.exists():
            def clear_session(payload: 'dict[str, Any]') -> 'bool | None':
                for key in ("default_project", "default_schema"):
                    if key in payload:
                        payload.pop(key)
                        cleared.append(key)
                return None if cleared else False

            update_config_mapping(target_path, clear_session)

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
    
    def session_show(
        self, *, target_config_path: 'Path | None' = None
    ) -> 'Envelope':
        """Show current session settings with source information.

        When ``target_config_path`` is given (``--config``), the reported
        ``config_path`` reflects that file, matching what ``session set
        --config <same>`` would write to.
        """
        config_path = target_config_path or default_global_config_path()

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
        (
            target_project,
            effective_schema,
            _cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)
        if rows <= 0:
            raise ValidationError("`--rows` must be greater than 0.")
        table, sample_rows, sample_info = self.backend.sample_table(
            target_table,
            rows,
            partition=partition,
            columns=columns,
            project=target_project,
            schema=effective_schema,
        )
        sample_rows, masked_columns = self._mask_sensitive_rows(
            sample_rows,
            sample_info["schema"],
        )
        sample_warnings = list(sample_info.get("warnings") or [])
        if masked_columns:
            sample_warnings.append(
                f"Sensitive columns masked: {', '.join(masked_columns)}"
            )
        ds_data = {
                "table_name": target_table,
                "schema_name": effective_schema,
                "qualified_name": qualified_name,
                "rows": sample_rows,
                "returned_rows": len(sample_rows),
                "schema": sample_info["schema"],
                "applied_partition": sample_info["applied_partition"],
                "selected_columns": sample_info["selected_columns"],
            }
        ds_metadata = {
                "project": target_project,
                "schema": effective_schema,
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
                warnings=sample_warnings,
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
        (
            target_project,
            effective_schema,
            _cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)
        # Take the underlying sample_table call so we can surface the same
        # auto-partition warning that data.sample emits — profile_table
        # otherwise swallows it inside `sample_info`.
        _table, _rows, sample_info = self.backend.sample_table(
            target_table,
            rows=20,
            partition=partition,
            columns=None,
            project=target_project,
            schema=effective_schema,
        )
        _rows, masked_columns = self._mask_sensitive_rows(
            _rows,
            sample_info["schema"],
        )
        from .helpers import build_profile
        profile = build_profile(
            _table,
            _rows,
            applied_partition=sample_info["applied_partition"],
        )
        profile["table_name"] = target_table
        profile["schema_name"] = effective_schema
        profile["qualified_name"] = qualified_name
        profile_warnings = list(sample_info.get("warnings") or [])
        if masked_columns:
            profile_warnings.append(
                f"Sensitive columns masked: {', '.join(masked_columns)}"
            )
        dp_metadata = {
            "project": target_project,
            "schema": effective_schema,
            "requested_partition": partition,
        }
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
                warnings=profile_warnings,
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
        create_partition: 'bool' = False,
        overwrite: 'bool' = False,
        delimiter: 'str' = ",",
        has_header: 'bool' = True,
        null_marker: 'str' = r"\N",
        block_size: 'int' = 10000,
        project: 'str | None' = None,
        schema: 'str | None' = None,
        dry_run: 'bool' = False,
    ) -> 'Envelope':
        local_file = Path(file_path).expanduser()
        if not local_file.is_absolute():
            local_file = self.cwd / local_file
        validate_csv_delimiter(delimiter)
        local_file = validate_upload_input_path(local_file)
        if block_size < 1:
            raise ValidationError("`--block-size` must be greater than 0.")
        if create_partition and not partition:
            raise ValidationError("`--create-partition` requires --partition.")
        (
            target_project,
            effective_schema,
            _cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)
        result = self.backend.upload_table(
            target_table, str(local_file),
            partition=partition, create_partition=create_partition,
            overwrite=overwrite,
            delimiter=delimiter, has_header=has_header,
            null_marker=null_marker, block_size=block_size,
            project=target_project, schema=effective_schema,
            dry_run=dry_run,
        )
        result = dict(result)
        result.update(
            {
                "table_name": target_table,
                "schema_name": effective_schema,
                "qualified_name": qualified_name,
            }
        )
        metadata = {
            "project": target_project,
            "schema": effective_schema,
            "requested_partition": partition,
            "file_path": str(local_file),
            "overwrite": overwrite,
            "create_partition": create_partition,
            "delimiter": delimiter,
            "has_header": has_header,
            "null_marker": null_marker,
            "block_size": block_size,
        }
        if dry_run:
            actions = [
                action(
                    "data.upload",
                    data=result,
                    metadata=metadata,
                    effect="remote_write",
                    confirmation_required=True,
                    agent_allowed=False,
                )
            ]
            insights = [
                "Dry-run validated the table schema, every CSV row width, and "
                "all mapped value types without creating an upload session. "
                "Re-run without --dry-run to upload."
            ]
        else:
            actions = [action("data.sample", data=result, metadata=metadata)]
            insights = []
        envelope = Envelope(
            command="data.upload",
            status="success",
            data=result,
            metadata=metadata,
            agent_hints=AgentHints(
                actions=actions,
                warnings=result.get("warnings", []),
                insights=insights,
            ),
        )
        self.log("data.upload", envelope.status, envelope.metadata)
        return envelope

    def data_download(
        self,
        table_name: 'str',
        output_path: 'str',
        *,
        overwrite: 'bool' = False,
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
            overwrite: Replace an existing output file when True.
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
        local_output = Path(output_path).expanduser()
        if not local_output.is_absolute():
            local_output = self.cwd / local_output
        validate_csv_delimiter(delimiter)
        local_output = validate_download_output_path(
            local_output,
            overwrite=overwrite,
        )
        if limit is not None and limit < 1:
            raise ValidationError("`--limit` must be greater than 0.")
        (
            target_project,
            effective_schema,
            _cache_schema,
            target_table,
            qualified_name,
        ) = self._resolve_table_scope(table_name, project=project, schema=schema)
        result = self.backend.download_table(
            target_table, str(local_output),
            overwrite=overwrite,
            partition=partition, columns=columns, limit=limit,
            delimiter=delimiter, write_header=write_header,
            null_marker=null_marker,
            project=target_project,
            schema=effective_schema,
        )
        result = dict(result)
        result.update(
            {
                "table_name": target_table,
                "schema_name": effective_schema,
                "qualified_name": qualified_name,
            }
        )
        metadata = {
            "project": target_project,
            "schema": effective_schema,
            "requested_partition": partition,
            "requested_columns": columns or [],
            "requested_limit": limit,
            "output_path": str(local_output),
            "overwrite": overwrite,
            "delimiter": delimiter,
            "write_header": write_header,
            "null_marker": null_marker,
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

    @staticmethod
    def _auth_action_prefix(target_path: 'Path') -> 'list[str]':
        tokens = shlex.split(current_cli_entry_point())
        tokens.extend(["--config", str(target_path)])
        from .odps_runtime import current_agent_user_agent

        user_agent = current_agent_user_agent()
        if user_agent:
            tokens.extend(["--user-agent", user_agent])
        return tokens

    @staticmethod
    def _project_selection_actions(
        base_tokens: 'list[str]',
        projects: 'list[dict[str, Any]]',
        *,
        title_prefix: 'str',
    ) -> 'list[SuggestedAction]':
        actions: list[SuggestedAction] = []
        for project_data in projects:
            project_id = str(project_data.get("project_id") or "")
            if not project_id:
                continue
            tokens = [*base_tokens, "--project", project_id]
            endpoint_value = project_data.get("endpoint")
            endpoint_placeholder = endpoint_value is None
            tokens.extend(["--endpoint", str(endpoint_value or "__MAXC_ENDPOINT__")])
            if project_data.get("region"):
                tokens.extend(["--region", str(project_data["region"])])
            if project_data.get("tunnel_endpoint"):
                tokens.extend(
                    ["--tunnel-endpoint", str(project_data["tunnel_endpoint"])]
                )
            tokens.append("--json")
            command_text = shlex.join(tokens).replace(
                "__MAXC_ENDPOINT__", "<endpoint>"
            )
            actions.append(
                SuggestedAction(
                    id="auth.login",
                    title=f"{title_prefix} {project_id}",
                    command=command_text,
                    executable=False,
                    placeholders=(
                        {"endpoint": "<endpoint>"} if endpoint_placeholder else {}
                    ),
                    effect="local_write",
                    confirmation_required=True,
                    agent_allowed=False,
                )
            )
        return actions

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
        _oauth_config: 'OAuthAuthConfig | None' = None,
        continuation_id: 'str | None' = None,
    ) -> 'Envelope':
        target_path = target_config_path or default_global_config_path()
        existing_payload = load_config_mapping(target_path) if target_path.exists() else {}
        existing_auth = AuthConfig.from_mapping(existing_payload.get("auth", {}) or {})
        env_settings = load_odps_env()

        # Resolve credentials first — the picker needs the AK/secret/STS in hand.
        if continuation_id:
            if any((access_id, secret_access_key, security_token)) or from_env:
                raise ValidationError(
                    "Do not combine an auth continuation with credential flags or --from-env.",
                    suggestion="Run the exact project-selection action returned by auth login.",
                )
            from .auth_continuation import load_auth_continuation

            continuation_payload = load_auth_continuation(
                self.config.state_dir,
                continuation_id,
                kind="access_key",
                target_config_path=target_path,
            )
            resolved_access_id = str(continuation_payload.get("access_id") or "")
            resolved_secret = str(
                continuation_payload.get("secret_access_key") or ""
            )
            resolved_token = continuation_payload.get("security_token")
            if not resolved_access_id or not resolved_secret:
                raise ValidationError(
                    "The auth continuation does not contain complete credentials.",
                    suggestion="Restart auth login.",
                )
            if resolved_token is not None:
                resolved_token = str(resolved_token)
        else:
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
        try:
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
        except ProjectPickerPending as exc:
            from . import catalog_bootstrap as _catalog_bootstrap

            projects_data = [
                {
                    "project_id": p.project_id,
                    "region": region_name or p.region,
                    "endpoint": endpoint or _catalog_bootstrap.region_to_endpoint(p.region),
                    "tunnel_endpoint": (
                        tunnel_endpoint
                        or _catalog_bootstrap.region_to_tunnel_endpoint(p.region)
                    ),
                    "owner": p.owner,
                    "schema_enabled": p.schema_enabled,
                    "description": p.description,
                }
                for p in exc.projects
            ]
            pending_envelope = Envelope(
                command="auth.login",
                status="pending",
                data={
                    "reason": "project_selection_required",
                    "projects": projects_data,
                    "count": len(projects_data),
                },
                agent_hints=AgentHints(
                    actions=[
                        SuggestedAction(
                            id="auth.login",
                            title="Complete login with selected project",
                            command=(
                                f"{current_cli_entry_point()} auth login "
                                "--project <project_id> --json"
                            ),
                            executable=False,
                            placeholders={"project_id": "<project_id>"},
                            effect="local_write",
                            confirmation_required=True,
                            agent_allowed=False,
                        ),
                    ],
                    warnings=[],
                ),
            )
            if _oauth_config is not None:
                return pending_envelope
            if continuation_id:
                raise ValidationError(
                    "Auth continuation did not resolve project selection.",
                    suggestion="Restart auth login and use one returned project action exactly.",
                )
            from .auth_continuation import save_auth_continuation

            continuation_id, expires_at = save_auth_continuation(
                self.config.state_dir,
                kind="access_key",
                target_config_path=target_path,
                secret_payload={
                    "access_id": resolved_access_id,
                    "secret_access_key": resolved_secret,
                    "security_token": resolved_token,
                },
            )
            base_tokens = self._auth_action_prefix(target_path)
            base_tokens.extend(
                ["auth", "login", "--login-continuation", continuation_id]
            )
            if catalog_endpoint:
                base_tokens.extend(["--catalog-endpoint", catalog_endpoint])
            if no_validate:
                base_tokens.append("--no-validate")
            pending_envelope.agent_hints.actions = self._project_selection_actions(
                base_tokens,
                projects_data,
                title_prefix="Complete login with",
            )
            pending_envelope.agent_hints.warnings.append(
                "Credential candidates are preserved in owner-only local state for "
                "10 minutes; choose one project action before it expires."
            )
            pending_envelope.metadata["continuation_expires_at_unix"] = expires_at
            return pending_envelope

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
            validated_payload: dict[str, Any] | None = None
            validate_warnings: list[str] = []
        else:
            validated_payload, validate_warnings = self._validate_auth_config(resolved_auth)

        # Commit only after shape/remote validation succeeds. OAuth validates
        # with its exchanged STS but persists the refreshable OAuth identity in
        # the same atomic config write.
        persisted_auth = resolved_auth
        if _oauth_config is not None:
            persisted_auth.provider = "oauth"
            persisted_auth.oauth = _oauth_config
        migrate_legacy_session_override(target_path)
        persist_login_config(
            target_path,
            auth=persisted_auth,
        )

        continuation_cleanup_warning: str | None = None
        if continuation_id:
            try:
                from .auth_continuation import delete_auth_continuation

                delete_auth_continuation(self.config.state_dir, continuation_id)
            except Exception as exc:
                continuation_cleanup_warning = (
                    "Login succeeded, but the expired one-time continuation could "
                    f"not be removed ({type(exc).__name__})."
                )

        warnings: list[str] = []
        warnings.extend(picker_warnings)
        if continuation_cleanup_warning:
            warnings.append(continuation_cleanup_warning)
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
            assert validated_payload is not None
            payload = validated_payload
            payload["saved"] = True
            payload["validated"] = True
            warnings.extend(validate_warnings)

        login_metadata = {
                "config_path": str(target_path),
                "written_fields": sorted(persisted_auth.to_mapping().keys()),
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

    def auth_login_oauth(
        self,
        *,
        site_type: 'str' = "CN",
        no_browser: 'bool' = False,
        on_url: 'Any | None' = None,
        project: 'str | None' = None,
        endpoint: 'str | None' = None,
        region_name: 'str | None' = None,
        tunnel_endpoint: 'str | None' = None,
        catalog_endpoint: 'str | None' = None,
        no_validate: 'bool' = False,
        target_config_path: 'Path | None' = None,
        no_picker: 'bool' = False,
        reselect: 'bool' = False,
        continuation_id: 'str | None' = None,
    ) -> 'Envelope':
        """Browser OAuth login (aliyun CLI ``--mode OAuth`` equivalent).

        Runs Authorization Code + PKCE, exchanges the OAuth token for a
        temporary STS triple, then delegates to the standard login flow
        (project picker, validation, persistence). OAuth tokens are persisted
        so the oauth provider can refresh/exchange on later invocations.
        """
        target_path = target_config_path or default_global_config_path()
        from .oauth import (
            delete_oauth_continuation,
            exchange_sts,
            load_oauth_continuation,
            save_oauth_continuation,
            start_oauth_flow,
        )

        if continuation_id:
            site_type, tokens, sts = load_oauth_continuation(
                self.config.state_dir,
                continuation_id,
                target_config_path=target_path,
            )
        else:
            tokens = start_oauth_flow(
                site_type,
                open_browser=not no_browser,
                on_url=on_url,
            )
            sts = exchange_sts(site_type, tokens.access_token)

        oauth_config = OAuthAuthConfig(
            site_type=site_type,
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            access_token_expire=tokens.expires_at,
        )

        envelope = self.auth_login(
            access_id=sts.access_key_id,
            secret_access_key=sts.access_key_secret,
            security_token=sts.security_token,
            project=project,
            endpoint=endpoint,
            region_name=region_name,
            tunnel_endpoint=tunnel_endpoint,
            catalog_endpoint=catalog_endpoint,
            no_validate=no_validate,
            target_config_path=target_path,
            no_picker=no_picker,
            reselect=reselect,
            _oauth_config=oauth_config,
        )

        if envelope.status == "success":
            if isinstance(envelope.data, dict):
                envelope.data["auth_type"] = "oauth"
                envelope.data["oauth_site_type"] = site_type
                envelope.data["token_expires_at"] = sts.expiration_iso
            if continuation_id:
                delete_oauth_continuation(self.config.state_dir, continuation_id)
        elif envelope.status == "pending" and envelope.agent_hints:
            if continuation_id:
                # An explicit project in a resume command should make another
                # selection round impossible.  Fail closed instead of creating
                # a chain of opaque continuation states.
                raise ValidationError(
                    "OAuth continuation did not resolve project selection.",
                    suggestion="Restart OAuth login and choose one project action exactly as returned.",
                )
            continuation_id, expires_at = save_oauth_continuation(
                self.config.state_dir,
                target_config_path=target_path,
                site_type=site_type,
                tokens=tokens,
                sts=sts,
            )
            base_tokens = self._auth_action_prefix(target_path)
            base_tokens.extend(
                [
                    "auth",
                    "login",
                    "--oauth",
                    "--oauth-continuation",
                    continuation_id,
                    "--site-type",
                    site_type,
                ]
            )
            if no_browser:
                base_tokens.append("--no-browser")
            if catalog_endpoint:
                base_tokens.extend(["--catalog-endpoint", catalog_endpoint])
            if no_validate:
                base_tokens.append("--no-validate")

            projects = envelope.data.get("projects", []) if isinstance(envelope.data, dict) else []
            envelope.agent_hints.actions = self._project_selection_actions(
                base_tokens,
                projects,
                title_prefix="Complete OAuth login with",
            )
            envelope.agent_hints.warnings.append(
                "OAuth authorization is preserved in owner-only local state for "
                "10 minutes; choose one project action before it expires."
            )
            envelope.metadata["continuation_expires_at_unix"] = expires_at
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

        # Resolve and validate the candidate entirely in memory. A failed
        # credential helper or remote identity probe must not replace a working
        # login already stored at ``target_path``.
        warnings: list[str] = []
        resolved_auth: ResolvedAuthConnection | None = None
        owner_display_name: str | None = None
        resolved_auth = resolve_auth_connection(self.config, auth_override=new_auth)
        if not no_validate:
            try:
                odps = resolved_auth.create_client()
                result = odps.execute_security_query(
                    "whoami",
                    project=resolved_auth.project,
                )
            except Exception as exc:
                raise translate_odps_error(exc, "whoami") from exc
            if isinstance(result, dict):
                owner_display_name = result.get("DisplayName")

        # Commit only after local shape resolution and, by default, the remote
        # identity probe have succeeded.
        migrate_legacy_session_override(target_path)
        persist_login_config(target_path, auth=new_auth)

        suppressed_env_fields = (
            getattr(resolved_auth, "suppressed_env_vars", []) if resolved_auth is not None else []
        )
        if suppressed_env_fields:
            warnings.append(
                f"{len(suppressed_env_fields)} environment variable(s) are set but ignored because "
                "an explicit external auth provider is configured."
            )

        if no_validate:
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
                "credential_process_configured": True,
                "process_timeout_seconds": external_cfg.process_timeout,
                "saved": True,
                "validated": False,
            }
        else:
            payload = {
                "authenticated": True,
                "configured": True,
                "validation_status": "verified",
                "backend": "odps",
                "auth_type": "external",
                "identity_source": "config_file",
                "principal_display": owner_display_name or mask_access_id(resolved_auth.access_id),
                "principal_masked": mask_access_id(resolved_auth.access_id),
                "project": resolved_auth.project,
                "region": resolved_auth.region_name,
                "endpoint": resolved_auth.endpoint,
                "credential_process_configured": True,
                "process_timeout_seconds": external_cfg.process_timeout,
                "saved": True,
                "validated": True,
            }

        try:
            ext_metadata = self._cache_metadata(
                project=new_auth.project or self.config.default_project,
                source="config",
            )
        except Exception as exc:
            # The auth config is already durably committed. Cache metadata is
            # only enrichment; it must never turn a successful login into a
            # failure that encourages the helper or remote probe to run again.
            ext_metadata = {
                "project": new_auth.project or self.config.default_project,
                "source": "config",
                "cache_available": None,
                "cache_age_seconds": None,
            }
            warnings.append(
                "Login succeeded, but local cache metadata was unavailable "
                f"({type(exc).__name__}). Authentication does not need to be repeated."
            )
        if not no_validate:
            ext_actions = [
                action("auth.whoami", data=payload, metadata=ext_metadata),
                action("meta.list-tables", data=payload, metadata=ext_metadata),
            ]
        else:
            ext_actions = [action("auth.whoami", data=payload, metadata=ext_metadata)]
        envelope = Envelope(
            command="auth.login-external",
            status="success",
            data=payload,
            metadata=ext_metadata,
            agent_hints=AgentHints(
                actions=ext_actions,
                warnings=warnings,
            ),
        )
        self.log("auth.login-external", envelope.status, envelope.metadata)
        return envelope

    def auth_logout(
        self,
        *,
        target_config_path: 'Path | None' = None,
    ) -> 'Envelope':
        """Remove persisted auth state without changing project preferences."""
        from .helpers import ODPS_ENV_ALIASES

        target_path = Path(
            os.path.abspath(
                os.fspath(target_config_path or default_global_config_path())
            )
        )
        auth_removed = False
        removed_fields: list[str] = []
        migrate_legacy_session_override(target_path)
        if target_path.exists():
            def clear_auth(payload: 'dict[str, Any]') -> 'bool | None':
                nonlocal auth_removed, removed_fields
                raw_auth = payload.get("auth")
                if isinstance(raw_auth, dict):
                    removed_fields = sorted(str(key) for key in raw_auth)
                    auth_removed = True
                    payload.pop("auth", None)
                return None if auth_removed else False

            update_config_mapping(target_path, clear_auth)

        cache_cleanup_error: str | None = None
        try:
            cached_credentials_removed: int | None = self.cache.delete_kv_prefix(
                "ext_creds:"
            )
        except Exception as exc:  # local cache cleanup is best-effort during logout
            cached_credentials_removed = None
            cache_cleanup_error = str(exc) or type(exc).__name__
        auth_continuations_removed: int | None = None
        expired_auth_continuations_removed: int | None = None
        auth_continuation_cleanup_failures: int | None = None
        auth_continuation_cleanup_error: str | None = None
        try:
            from .auth_continuation import clear_auth_continuations

            (
                auth_continuations_removed,
                expired_auth_continuations_removed,
                auth_continuation_cleanup_failures,
            ) = clear_auth_continuations(
                self.config.state_dir,
                target_config_path=target_path,
            )
        except Exception as exc:  # continuation cleanup is best-effort on logout
            auth_continuation_cleanup_error = str(exc) or type(exc).__name__
        environment_variables = sorted({
            name
            for field in (
                "access_id",
                "secret_access_key",
                "security_token",
                "external_process_command",
            )
            for name in ODPS_ENV_ALIASES[field]
            if os.environ.get(name)
        })
        remaining_auth_sources: list[str] = []
        for source in self.config.sources:
            resolved = source.resolve()
            if resolved == target_path or not resolved.exists():
                continue
            source_payload = load_config_mapping(resolved)
            if isinstance(source_payload.get("auth"), dict) and source_payload["auth"]:
                remaining_auth_sources.append(str(resolved))

        # Avoid retaining credentials in a long-lived in-process caller after
        # the persisted source and cache have been cleared.
        self.config.auth = AuthConfig()
        self.backend = None
        self.remote_jobs = False

        credentials_may_still_be_available = bool(
            environment_variables or remaining_auth_sources
        )
        warnings: list[str] = []
        if cache_cleanup_error:
            warnings.append(
                "Saved auth was removed, but cached temporary credentials could not be cleared: "
                + cache_cleanup_error
            )
        if auth_continuation_cleanup_error:
            warnings.append(
                "Saved auth was removed, but pending authentication continuations could not be cleared: "
                + auth_continuation_cleanup_error
            )
        elif auth_continuation_cleanup_failures:
            warnings.append(
                f"Saved auth was removed, but {auth_continuation_cleanup_failures} unreadable authentication continuation(s) could not be cleared."
            )
        if environment_variables:
            warnings.append(
                "Credential-related environment variables remain active in the current process: "
                + ", ".join(environment_variables)
                + ". The CLI cannot unset its parent shell environment."
            )
        if remaining_auth_sources:
            warnings.append(
                "Other loaded configuration files still contain auth settings: "
                + ", ".join(remaining_auth_sources)
                + ". Re-run auth logout with --config <path> for an exact file only after confirming it should be changed."
            )

        data = {
            "config_path": str(target_path),
            "config_auth_removed": auth_removed,
            "removed_fields": removed_fields,
            "cached_credentials_removed": cached_credentials_removed,
            "cache_cleanup_error": cache_cleanup_error,
            "auth_continuations_removed": auth_continuations_removed,
            "expired_auth_continuations_removed": expired_auth_continuations_removed,
            "auth_continuation_cleanup_failures": auth_continuation_cleanup_failures,
            "auth_continuation_cleanup_error": auth_continuation_cleanup_error,
            "environment_auth_variables": environment_variables,
            "remaining_auth_sources": remaining_auth_sources,
            "credentials_may_still_be_available": credentials_may_still_be_available,
        }
        envelope = Envelope(
            command="auth.logout",
            status="success",
            data=data,
            metadata={"config_sources": [str(path) for path in self.config.sources]},
            agent_hints=AgentHints(
                actions=[action("auth.whoami"), action("auth.login")],
                warnings=warnings,
            ),
        )
        self.log(
            "auth.logout",
            envelope.status,
            {
                "config_path": str(target_path),
                "config_auth_removed": auth_removed,
                "cached_credentials_removed": cached_credentials_removed,
                "auth_continuations_removed": auth_continuations_removed,
                "credentials_may_still_be_available": credentials_may_still_be_available,
            },
        )
        return envelope

    def auth_whoami(self) -> 'Envelope':
        if self.backend is None:
            try:
                self.backend = OdpsBackend(self.config, cache=self._lazy_cache)
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
        object_name: 'str',
        object_type: 'str' = "Table",
        operation: 'str',
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'Envelope':
        payload, warnings = self.backend.can_i_info(
            object_name=object_name,
            object_type=object_type,
            operation=operation,
            project=project or self.config.default_project,
            schema=schema,
        )
        cani_metadata = {
            "project": payload.get("project"),
            "schema": payload.get("schema"),
        }
        object_type = payload.get("object_type")
        if payload.get("allowed") and object_type == "Table":
            cani_actions = [
                action("query.cost", data=payload, metadata=cani_metadata),
                action("query.explain", data=payload, metadata=cani_metadata),
            ]
        elif payload.get("allowed") and object_type == "Project":
            cani_actions = [
                action("meta.list-schemas", data=payload, metadata=cani_metadata),
                action("meta.list-tables", data=payload, metadata=cani_metadata),
            ]
        elif payload.get("allowed") and object_type == "Schema":
            cani_actions = [
                action("meta.list-tables", data=payload, metadata=cani_metadata),
            ]
        elif payload.get("allowed"):
            cani_actions = []
        else:
            cani_actions = [action("auth.whoami", data=payload, metadata=cani_metadata)]
            if object_type == "Table":
                cani_actions.append(
                    action("meta.describe", data=payload, metadata=cani_metadata)
                )
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

        try:
            result = client.execute_security_query("whoami", project=resolved.project)
        except Exception as exc:
            raise translate_odps_error(exc, "whoami") from exc
        owner_display_name = result.get("DisplayName") if isinstance(result, dict) else None

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

        from . import catalog_bootstrap as _catalog_bootstrap

        # 2. Picker not viable (non-TTY or --no-picker).
        if inputs.no_picker or not sys.stdin.isatty():
            # Non-TTY + picker not disabled: list projects for structured output
            catalog_warning: str | None = None
            if not inputs.no_picker and not from_env:
                try:
                    bootstrap_odps = _catalog_bootstrap.build_bootstrap_odps(
                        access_id=inputs.access_id,
                        secret_access_key=inputs.secret,
                        security_token=inputs.security_token,
                        endpoint=inputs.catalog_endpoint or provided_endpoint,
                    )
                    projects = _catalog_bootstrap.list_all_projects(bootstrap_odps)
                    if projects:
                        raise ProjectPickerPending(projects)
                except ProjectPickerPending:
                    raise
                except Exception as exc:
                    catalog_warning = (
                        f"Could not list projects via Catalog API "
                        f"({type(exc).__name__}: {exc}). "
                        f"Falling back to saved/env config."
                    )

            prompted = self._resolve_login_value(
                provided=None,
                env_value=env_settings.get("project"),
                existing_value=existing_auth.project,
                prompt="MaxCompute Project",
                required=True,
                secret=False,
                use_env=from_env,
            )
            warnings_out: list[str] = []
            if catalog_warning:
                warnings_out.append(catalog_warning)
            return (
                prompted,
                provided_endpoint,
                provided_region,
                provided_tunnel,
                warnings_out,
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

        This command is intentionally local-only. It reports configuration
        readiness without claiming that credentials or the backend were
        checked; use ``agent doctor --online`` for a live probe.
        """
        # Determine local configuration readiness. Constructing a PyODPS client
        # or reading ``client.project`` does not prove authentication or network
        # reachability, so this command deliberately never labels either as
        # checked. ``agent doctor --online`` owns that live claim.
        auth_cfg = self.config.auth
        # `aliyun maxc` may pass this non-secret hint after validating its
        # selected profile locally. It deliberately does not contain or cause
        # credential resolution: context remains offline, while avoiding a
        # false "not_configured" result when the wrapper owns authentication.
        aliyun_profile_configured = (
            os.environ.get("ALIBABA_CLOUD_MAXC_PROFILE_CONFIGURED") == "1"
        )
        effective_settings, _setting_sources, suppressed_env = resolve_odps_settings(
            self.config
        )
        inferred_provider = infer_auth_provider(self.config, effective_settings)
        auth_signal = bool(
            auth_cfg.provider
            or auth_cfg.oauth.is_configured()
            or auth_cfg.external.is_configured()
            or auth_cfg.ncs.is_configured()
            or effective_settings.get("access_id")
            or effective_settings.get("secret_access_key")
            or effective_settings.get("security_token")
            or effective_settings.get("external_process_command")
            or aliyun_profile_configured
        )
        provider = inferred_provider if auth_signal else ""

        access_id = effective_settings.get("access_id")
        secret = effective_settings.get("secret_access_key")
        security_token = effective_settings.get("security_token")
        has_project = bool(effective_settings.get("project") or self.config.default_project)
        # A region is useful routing metadata, but it is not itself a usable
        # MaxCompute service endpoint. Do not report readiness until the same
        # endpoint requirement used by backend construction is satisfied.
        has_endpoint = bool(effective_settings.get("endpoint"))
        if provider == "access_key":
            has_creds = bool(access_id and secret) or aliyun_profile_configured
        elif provider in {"sts", "sts_token"}:
            has_creds = bool(access_id and secret and security_token)
        elif provider == "ncs":
            has_creds = auth_cfg.ncs.is_configured()
        elif provider == "external":
            has_creds = bool(effective_settings.get("external_process_command"))
        elif provider == "oauth":
            has_creds = auth_cfg.oauth.is_configured()
        else:
            has_creds = False

        if auth_signal and has_creds and has_project and has_endpoint:
            auth_status = "configured"
        elif auth_signal:
            auth_status = "incomplete"
        else:
            auth_status = "not_configured"
        backend_reachable = None

        # Determine backend capabilities
        capabilities = {
            "remote_jobs": getattr(self.backend, "supports_remote_jobs", True) if self.backend else True,
            "cost_check": getattr(self.backend, "supports_cost_check", True) if self.backend else True,
            "lineage": False,  # Always false for current ODPS backend
        }

        # Keep agent.context strictly local. Report Catalog search capability
        # from explicit configuration or the local routing cache; discovering
        # an endpoint through PyODPS may perform network I/O and belongs to a
        # real Catalog operation, not a readiness summary.
        if self.backend is not None:
            capabilities["catalog_search"] = bool(
                getattr(self.backend, "catalog_available", False)
            )
        else:
            capabilities["catalog_search"] = bool(
                self.config.auth.catalog_endpoint
            )

        ac_data = {
            "version": __version__,
            "min_cli_version": __version__,
            "min_python_version": "3.9",
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "entry_point": current_cli_entry_point(),
            "project": self.config.default_project,
            "region": self.config.default_region,
            "backend": "odps",
            "backend_reachable": backend_reachable,
            "network_checked": False,
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
                warnings=(
                    [
                        "Environment-based MaxCompute settings are present but ignored because a saved auth provider owns the connection."
                    ]
                    if suppressed_env
                    else []
                ),
            ),
        )
        return envelope

    def agent_doctor(self, *, online: 'bool' = False) -> 'Envelope':
        """Run local readiness checks and, optionally, a live identity probe."""
        import os
        import sys

        context = self.agent_context()
        context_data = dict(context.data)
        writable_probe = self.config.state_dir
        while not writable_probe.exists() and writable_probe != writable_probe.parent:
            writable_probe = writable_probe.parent
        state_writable = writable_probe.is_dir() and os.access(writable_probe, os.W_OK)
        checks: list[dict[str, Any]] = [
            {
                "id": "python.version",
                "status": "pass" if sys.version_info >= (3, 9) else "fail",
                "detail": context_data["python_version"],
                "required": ">=3.9",
            },
            {
                "id": "config.loaded",
                "status": "pass" if self.config.sources else "warn",
                "detail": [str(path) for path in self.config.sources],
            },
            {
                "id": "state.writable",
                "status": "pass" if state_writable else "fail",
                "detail": {
                    "target": str(self.config.state_dir),
                    "checked_existing_parent": str(writable_probe),
                },
            },
            {
                "id": "auth.configured",
                "status": (
                    "pass"
                    if context_data["auth_status"] in {"configured", "authenticated"}
                    else "fail"
                ),
                "detail": context_data["auth_status"],
            },
        ]

        identity: dict[str, Any] | None = None
        if online:
            whoami = self.auth_whoami()
            raw_identity = (
                whoami.data.get("identity")
                if isinstance(whoami.data, dict)
                else None
            )
            if isinstance(raw_identity, dict):
                identity = raw_identity
            elif isinstance(whoami.data, dict):
                # App methods hold pre-serialization data. auth.whoami is
                # wrapped under data.identity only when Envelope.to_dict()
                # normalizes the public JSON shape.
                identity = dict(whoami.data)
            else:
                identity = {}
            authenticated = bool(identity.get("authenticated"))
            checks.append({
                "id": "backend.identity",
                "status": "pass" if authenticated else "fail",
                "detail": {
                    "authenticated": authenticated,
                    "validation_status": identity.get("validation_status"),
                    "auth_type": identity.get("auth_type"),
                    "project": identity.get("project"),
                },
            })

        ready = all(check["status"] != "fail" for check in checks)
        local_ready = all(
            check["status"] != "fail"
            for check in checks
            if check["id"] != "backend.identity"
        )
        online_ready = bool(online and ready)
        failed_check_ids = [
            check["id"] for check in checks if check["status"] == "fail"
        ]
        command = current_cli_entry_point()
        if ready and not online:
            actions = [SuggestedAction(
                id="agent.doctor.online",
                title="Verify credentials and backend",
                command=f"{command} agent doctor --online --json",
            )]
        elif ready:
            actions = [action("meta.list-tables", data=context_data)]
        elif "auth.configured" in failed_check_ids:
            actions = [action("auth.login")]
        elif online and "backend.identity" in failed_check_ids:
            actions = [action("auth.whoami"), action("agent.context")]
        else:
            actions = [action("agent.context")]
        envelope = Envelope(
            command="agent.doctor",
            status="success",
            data={
                "ready": ready,
                "local_ready": local_ready,
                "online_ready": online_ready,
                "readiness": (
                    "online_ready"
                    if online_ready
                    else "locally_ready"
                    if local_ready
                    else "not_ready"
                ),
                "online": online,
                "failed_check_ids": failed_check_ids,
                "checks": checks,
                "context": context_data,
                "identity": identity,
            },
            metadata={"config_sources": [str(path) for path in self.config.sources]},
            agent_hints=AgentHints(actions=actions),
        )
        if online:
            self.log("agent.doctor", envelope.status, envelope.metadata)
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
                "name": "alibabacloud-maxcompute-cli",
                "version": __version__,
                "min_cli_version": __version__,
                "entry_point": current_cli_entry_point(),
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
        if dir_override is None and not platform.install_root.is_absolute():
            raise ValidationError(
                f"Platform {platform.name!r} has no default install path — "
                f"pass --dir <path> to specify where the skill should be installed."
            )
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

    _LEGACY_SKILL_DIRS = ("maxc-cli", "maxcompute-cli-guidance", "use-maxc-cli")

    def _cleanup_legacy_skill_dir(self, target: 'Path') -> None:
        """Remove legacy skill directories that have been superseded by the new path."""
        import shutil
        for old_name in self._LEGACY_SKILL_DIRS:
            old_dir = target.parent / old_name
            if old_dir.is_dir() and (old_dir / ".maxc-skill-version").is_file():
                shutil.rmtree(str(old_dir))

    def skill_install(
        self,
        *,
        platform: 'str',
        invocation: 'str' = "maxc",
        dir_override: 'Path | None' = None,
        force: 'bool' = False,
    ) -> 'Envelope':
        from . import agent_platforms
        # invocation is now the literal cli name (e.g. "maxc", "aliyun maxc")
        # For backwards compat, also accept legacy key "aliyun-maxc"
        if invocation in agent_platforms.INVOCATIONS:
            invocation_map = agent_platforms.INVOCATIONS[invocation]
        else:
            invocation_map = {"cli": invocation, "cli_module": invocation}
        platform_spec, target = self._resolve_skill_target(platform, dir_override)
        skills_src = self._locate_skills_source()
        if dir_override is None:
            self._cleanup_legacy_skill_dir(target)
        version_marker = f"{__version__}+{invocation}"
        marker_path = target / ".maxc-skill-version"
        invocation_path = target / ".maxc-skill-invocation"
        if not force and marker_path.is_file() and marker_path.read_text().strip() == version_marker:
            invocation_path.write_text(invocation, encoding="utf-8")
            return Envelope(
                command="agent.skill.install",
                status="success",
                data={
                    "platform": platform_spec.name,
                    "invocation": invocation,
                    "cli_invocation": invocation_map["cli"],
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
        marker_path.write_text(version_marker, encoding="utf-8")
        invocation_path.write_text(invocation, encoding="utf-8")
        return Envelope(
            command="agent.skill.install",
            status="success",
            data={
                "platform": platform_spec.name,
                "invocation": invocation,
                "cli_invocation": invocation_map["cli"],
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
        invocation: 'str | None' = None,
        dir_override: 'Path | None' = None,
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
        if all_platforms and dir_override is not None:
            raise ValidationError(
                "agent skill update --all cannot be combined with --dir; "
                "update one explicit platform for a custom location."
            )

        def _effective_invocation(target: 'Path') -> 'tuple[str, str]':
            if invocation is not None:
                return invocation, "explicit-override"

            invocation_path = target / ".maxc-skill-invocation"
            if invocation_path.is_file():
                installed_invocation = invocation_path.read_text(
                    encoding="utf-8"
                ).strip()
                if installed_invocation:
                    return installed_invocation, "installed-marker"

            # Legacy installs may have only the version marker. Do not infer a
            # command from that free-form value: use the same validated current
            # entry point as a fresh install.
            return current_cli_entry_point(), "current-default"

        if platform is not None:
            _, target = self._resolve_skill_target(platform, dir_override)
            effective_invocation, invocation_source = _effective_invocation(target)
            env = self.skill_install(
                platform=platform,
                invocation=effective_invocation,
                dir_override=dir_override,
                force=True,
            )
            env.command = "agent.skill.update"
            env.data["invocation_source"] = invocation_source
            return env
        updated: list[str] = []
        updates: list[dict[str, Any]] = []
        for p in agent_platforms.all_platforms():
            target = agent_platforms.effective_target(p, None)
            if (target / ".maxc-skill-version").is_file():
                effective_invocation, invocation_source = _effective_invocation(target)
                install_env = self.skill_install(
                    platform=p.name,
                    invocation=effective_invocation,
                    force=True,
                )
                updated.append(p.name)
                updates.append({
                    "platform": p.name,
                    "invocation": install_env.data["invocation"],
                    "cli_invocation": install_env.data["cli_invocation"],
                    "invocation_source": invocation_source,
                    "install_path": install_env.data["install_path"],
                })

        invocation_values = {item["invocation"] for item in updates}
        return Envelope(
            command="agent.skill.update",
            status="success",
            data={
                "platforms_updated": updated,
                "updates": updates,
                # Retain the legacy scalar when every target used one value;
                # mixed installs are represented without inventing a winner.
                "invocation": (
                    next(iter(invocation_values)) if len(invocation_values) == 1 else None
                ),
                "invocation_override": invocation,
            },
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
            "run `agent skill path <platform> --dir <CUSTOM>` to verify it."
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
        invocation = "maxc"
        invocation_path = target / ".maxc-skill-invocation"
        if invocation_path.is_file():
            invocation = invocation_path.read_text(encoding="utf-8").strip() or "maxc"
        else:
            marker_path = target / ".maxc-skill-version"
            marker = marker_path.read_text(encoding="utf-8").strip() if marker_path.is_file() else ""
            for known in ("aliyun-maxc", "aliyun maxc", "maxc"):
                if marker.endswith(f"+{known}"):
                    invocation = known
                    break
        invocation_map = agent_platforms.INVOCATIONS.get(
            invocation,
            {"cli": invocation, "cli_module": invocation},
        )
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
                "invocation": invocation,
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
            if self._audit_invocation_id is None:
                self._audit_invocation_id = os.urandom(16).hex()
            if self._audit is None:
                default_state_dir = default_global_config_path().parent / "state"
                self._audit = AuditLogger(
                    self._audit_path,
                    secure_parent=(
                        self._audit_path.parent.resolve()
                        == default_state_dir.resolve()
                    ),
                )
            self._audit.log(
                {
                    "invocation_id": self._audit_invocation_id,
                    "command": command,
                    "status": status,
                    "metadata": metadata or {},
                    "error": error,
                }
            )
        except Exception:
            # Audit is best-effort enrichment. Serialization, permissions, or
            # an unsafe local path must never rewrite a completed command.
            return

    def _submit_remote_job(
        self,
        *,
        sql: 'str',
        project: 'str',
        cost_check: 'float | None',
        idempotency_key: 'str | None',
        force: 'bool' = False,
        execution_settings: '_McqaExecutionSettings | None' = None,
    ) -> 'JobInfo':
        if cost_check is not None:
            self._enforce_cost_check(sql=sql, project=project, cost_check=cost_check, force=force)
        job = self.backend.submit_query(
            sql,
            project=project,
            idempotency_key=idempotency_key,
            force=force,
            execution_settings=execution_settings,
        )
        self._persist_remote_job_context(
            job,
            require_composite=self._requires_composite_job_id(execution_settings),
        )
        return job

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
        execution_settings: '_McqaExecutionSettings | None' = None,
    ) -> 'QueryResult':
        if sql.startswith("@natural"):
            raise FeatureUnavailableError(
                "`@natural` is a roadmap feature and is not available in the current MVP.",
                suggestion="Use `maxc meta search` or `maxc meta describe` to inspect tables, then submit plain SQL.",
            )
        write_operations = known_write_operations(sql)
        if write_operations and (retry_on or max_retries):
            raise ValidationError(
                "Automatic retries are not supported for mutating SQL "
                f"({', '.join(write_operations)}).",
                suggestion="Remove retry flags and verify the first execution before retrying manually.",
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
                    execution_settings=execution_settings,
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

    def _query_job_failure_payload(self, message: str) -> ErrorPayload:
        classification = classify_sql_error(message)
        if classification["error_type"] == "schema_not_found":
            return SchemaNotFoundError(
                message,
                suggestion="Check schema name with `maxc meta list-schemas --json`.",
            ).to_payload()
        if classification["error_type"] == "table_not_found":
            return TableNotFoundError(
                message,
                suggestion="Run `maxc meta search <keyword> --json` or `maxc meta list-tables --json` to find the table.",
            ).to_payload()
        if classification["error_type"] == "column_not_found":
            return ColumnNotFoundError(
                message,
                suggestion="Run `maxc meta describe <table> --json` to inspect available columns.",
            ).to_payload()
        return ErrorPayload(
            code="EXECUTION_FAILED",
            message=message,
            suggestion=None,
            recoverable=False,
        )

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
        external_job_id: 'str | None' = None,
    ) -> 'Envelope':
        insights = []
        warnings = list(result.warnings)
        if dry_run:
            insights.append("Dry-run returned estimated cost and SQLCost metadata so you can decide whether to continue.")
        elif result.extra_metadata.get("result_kind") == "statement":
            operation = result.extra_metadata.get("statement_operation")
            subject = f"{operation} statement" if operation else "Statement"
            insights.append(f"{subject} completed successfully and returned no result set.")
        elif not result.rows:
            insights.append("The result set is empty. Check filters, partitions, and table selection.")

        # LIMIT truncation warning
        if result.has_more and not dry_run and not sql_has_limit(result.sql_executed):
            warnings.append(
                f"Results truncated to {result.returned_rows} rows. "
                f"Add LIMIT to your SQL or use --max-rows to adjust."
            )

        # Sensitive field masking
        rows, masked_columns = self._mask_sensitive_rows(result.rows, result.schema)
        if masked_columns:
            warnings.append(f"Sensitive columns masked: {', '.join(masked_columns)}")

        if external_job_id is not None:
            result.job_id = external_job_id
        if result.job_id and self.remote_jobs:
            self._persist_remote_query_result_context(
                result,
                require_composite=self._query_result_uses_composite_job_id(result),
            )
            for warning in result.warnings:
                if warning not in warnings:
                    warnings.append(warning)

        # 如果有 job_id 且 has_more，创建或复用 session，生成短 cursor
        next_cursor = None
        if result.has_more and result.returned_rows > 0:
            current_offset = result.extra_metadata.get("current_offset", 0)
            next_offset = current_offset + result.returned_rows
            if result.job_id and self.remote_jobs:
                # 远程 backend: 用 session_id 生成短 cursor
                if session_id is None:
                    try:
                        session_id = self.cache.create_session(
                            job_id=result.job_id,
                            project=result.project,
                            sql=result.sql_executed,
                        )
                    except Exception:
                        warnings.append(
                            "The remote result succeeded, but the local pagination "
                            "cursor could not be saved. Do not rerun the query solely "
                            "for this local failure; retain the job_id and use `job "
                            "result --max-rows <larger-value> --project "
                            f"{result.project}` if more rows are needed."
                        )
                if session_id is not None:
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
        if result.tables_used and result.extra_metadata.get("result_kind") != "statement":
            qe_actions.append(action("meta.describe", data=data, metadata=metadata))
        if result.has_more and next_cursor:
            qe_actions.append(action("query.paginate", data=data, metadata=metadata))
        elif result.has_more and result.job_id:
            qe_actions.append(action("job.result", data=data, metadata=metadata))
        if dry_run:
            qe_actions.append(action("job.submit", data=data, metadata=metadata))

        # Add safety block
        if command in {"job.wait", "job.result"}:
            data["safety"] = build_observational_safety_block(command)
        else:
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

    def _job_info_envelope(
        self,
        command: 'str',
        info: 'JobInfo',
        *,
        external_job_id: 'str | None' = None,
    ) -> 'Envelope':
        display_job_id = external_job_id or info.job_id
        ji_data = {
                "job_id": display_job_id,
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
                "job_id": display_job_id,
                "project": info.project,
                "submitted_at": info.submitted_at,
                "updated_at": info.updated_at,
                "completed_at": info.completed_at,
                "logview": info.logview,
                "error_message": info.error_message,
            }
        normalized_status = str(info.status or "").lower()
        warnings = list(info.warnings or [])
        if normalized_status in {"pending", "queued", "running", "suspended", "cancel_requested"}:
            ji_actions = [
                action("job.wait", data=ji_data, metadata=ji_metadata),
                action("job.status", data=ji_data, metadata=ji_metadata),
            ]
        elif normalized_status in {"failure", "failed"}:
            ji_actions = [
                action("job.diagnose", data=ji_data, metadata=ji_metadata),
                action("job.status", data=ji_data, metadata=ji_metadata),
            ]
        elif normalized_status in {"success", "completed"}:
            ji_actions = [
                action("job.result", data=ji_data, metadata=ji_metadata),
            ]
        elif normalized_status == "cancelled":
            ji_actions = [action("job.status", data=ji_data, metadata=ji_metadata)]
        else:
            ji_actions = [
                action("job.status", data=ji_data, metadata=ji_metadata),
                action("job.wait", data=ji_data, metadata=ji_metadata),
            ]
            warnings.append(
                f"Backend returned an unknown job status `{info.status}`; the CLI "
                "did not treat it as successful."
            )
        envelope_status = {
            "pending": "pending",
            "queued": "pending",
            "running": "pending",
            "suspended": "pending",
            "cancel_requested": "pending",
            "success": "success",
            "completed": "success",
            "cancelled": "success",
            "failure": "failure",
            "failed": "failure",
        }.get(normalized_status, "pending")
        error_payload = None
        if envelope_status == "failure":
            error_payload = self._query_job_failure_payload(
                info.failure_reason or info.error_message or "Job failed"
            )
            error_payload.instance_id = display_job_id
            error_payload.logview = info.logview
            error_payload.context = {
                "job_id": display_job_id,
                "project": info.project,
                "job_status": info.status,
            }
            if info.retryable is not None:
                error_payload.recoverable = bool(info.retryable)
        return Envelope(
            command=command,
            status=envelope_status,
            data=ji_data,
            metadata=ji_metadata,
            error=error_payload,
            agent_hints=AgentHints(
                actions=ji_actions,
                warnings=warnings,
            ),
        )

    def _local_job_info(self, job: 'dict[str, Any]') -> 'JobInfo':
        status = job["status"]
        stage = (
            "queue"
            if status == "pending"
            else "completed"
            if status == "success"
            else "cancelled"
            if status == "cancelled"
            else "failed"
        )
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
        *,
        external_job_id: 'str | None' = None,
    ) -> 'list[dict[str, Any]]':
        display_job_id = external_job_id or result.job_id or after.job_id or before.job_id
        events = [{"type": "started", "ts": before.submitted_at or now_utc_iso(), "job_id": display_job_id}]
        if before.status in {"pending", "running"}:
            events.append(
                {
                    "type": "progress",
                    "ts": now_utc_iso(),
                    "job_id": display_job_id,
                    "percent": before.progress or 50,
                    "stage": before.status,
                }
            )
        events.append(
            {
                "type": "completed",
                "ts": after.completed_at or now_utc_iso(),
                "job_id": display_job_id,
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
            columns_payload = [
                {"name": column.name, "type": column.type, "comment": column.comment}
                for column in table.columns
            ]
            payload["schema"] = columns_payload
            payload["columns"] = columns_payload
            payload["has_more_columns"] = False
            # Full mode: include complete sample rows
            payload["sample_preview"] = table.sample_rows[:2]
        else:
            # Summary mode: return first 10 columns only
            display_columns = table.columns[:10]
            columns_payload = [
                {"name": column.name, "type": column.type, "comment": column.comment}
                for column in display_columns
            ]
            payload["schema"] = columns_payload
            payload["columns"] = columns_payload
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
