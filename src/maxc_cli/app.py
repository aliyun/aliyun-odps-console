from __future__ import annotations

import getpass
import json
import sys
from pathlib import Path
from typing import Any

from .audit import AuditLogger
from .backend import create_backend
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
    FeatureUnavailableError,
    MaxCError,
    PermissionDeniedError,
    ValidationError,
)
from .helpers import (
    build_task_summary,
    classify_failure_reason,
    load_odps_env,
    mask_access_id,
    validate_login_settings,
)
from .models import AgentHints, Envelope, JobInfo, QueryResult
from .store import JobStore
from .utils import decode_cursor, detect_operation, encode_cursor, now_utc_iso


class MaxCApp:
    def __init__(
        self,
        *,
        cwd: Path,
        config_path: Path | None = None,
        load_backend: bool = True,
    ) -> None:
        self.cwd = cwd
        self.config = load_config(cwd, config_path)
        self.backend = create_backend(self.config) if load_backend else None
        self.remote_jobs = getattr(self.backend, "supports_remote_jobs", False) if self.backend else False
        self.jobs = None if self.remote_jobs else JobStore(self.config.state_dir)
        self.audit = AuditLogger(self.config.agent.audit_log or self.config.state_dir / "audit.log")
        self.cache = LocalCache(self.config.state_dir)

    def query(
        self,
        *,
        command: str,
        sql: str,
        project: str | None = None,
        max_rows: int = 100,
        cursor: str | None = None,
        dry_run: bool = False,
        async_mode: bool = False,
        cost_check: float | None = None,
        idempotency_key: str | None = None,
        retry_on: list[str] | None = None,
        max_retries: int = 0,
    ) -> Envelope:
        if dry_run and async_mode:
            raise ValidationError("--dry-run 和 --async 不能同时使用。")
        if max_rows <= 0:
            raise ValidationError("--max-rows 和 --page-size 必须大于 0。")
        if cursor and async_mode:
            raise ValidationError("--cursor 不能和 --async 同时使用。")
        if cursor and dry_run:
            raise ValidationError("--cursor 不能和 --dry-run 同时使用。")

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
            if self.jobs is None:
                raise FeatureUnavailableError("当前 backend 未初始化本地任务存储。")
            job = self.jobs.create_job(
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
        sql: str,
        project: str | None = None,
        command: str = "query.cost",
    ) -> Envelope:
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
        sql: str,
        project: str | None = None,
        command: str = "query.explain",
    ) -> Envelope:
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
        sql: str,
        project: str | None = None,
        max_rows: int = 100,
        cost_check: float | None = None,
        idempotency_key: str | None = None,
    ) -> Envelope:
        return self.query(
            command="job.submit",
            sql=sql,
            project=project,
            max_rows=max_rows,
            async_mode=True,
            cost_check=cost_check,
            idempotency_key=idempotency_key,
        )

    def job_status(self, job_id: str) -> Envelope:
        if self.remote_jobs:
            info = self.backend.get_job(job_id, project=self.config.default_project)
            envelope = self._job_info_envelope("job.status", info)
            self.log("job.status", envelope.status, envelope.metadata)
            return envelope

        if self.jobs is None:
            raise FeatureUnavailableError("当前 backend 未初始化本地任务存储。")
        job = self.jobs.get_job(job_id)
        info = self._local_job_info(job)
        envelope = self._job_info_envelope("job.status", info)
        self.log("job.status", envelope.status, envelope.metadata)
        return envelope

    def job_wait(self, job_id: str) -> tuple[Envelope, list[dict[str, Any]]]:
        if self.remote_jobs:
            before = self.backend.get_job(job_id, project=self.config.default_project)
            after = self.backend.wait_job(job_id, project=self.config.default_project)
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

        if self.jobs is None:
            raise FeatureUnavailableError("当前 backend 未初始化本地任务存储。")
        job = self.jobs.get_job(job_id)
        events = self._job_events(job)
        final_job = self.jobs.update_job(
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

    def job_result(self, job_id: str) -> Envelope:
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

        if self.jobs is None:
            raise FeatureUnavailableError("当前 backend 未初始化本地任务存储。")
        job = self.jobs.get_job(job_id)
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

    def cancel_job(self, job_id: str) -> Envelope:
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

        if self.jobs is None:
            raise FeatureUnavailableError("当前 backend 未初始化本地任务存储。")
        job = self.jobs.get_job(job_id)
        if job["status"] == "success":
            raise ValidationError("任务已完成，不能取消。")
        updated = self.jobs.update_job(job_id, status="failure", progress=0, cancelled=True)
        envelope = Envelope(
            command="job.cancel",
            status="failure",
            data={"job_id": job_id, "cancelled": True},
            metadata={"project": updated["project"], "updated_at": updated["updated_at"]},
            agent_hints=AgentHints(next_actions=["job.submit"]),
        )
        self.log("job.cancel", envelope.status, envelope.metadata)
        return envelope

    def job_diagnose(self, job_id: str) -> Envelope:
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

        if self.jobs is None:
            raise FeatureUnavailableError("当前 backend 未初始化本地任务存储。")
        job = self.jobs.get_job(job_id)
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

    def list_jobs(self) -> Envelope:
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

        if self.jobs is None:
            raise FeatureUnavailableError("当前 backend 未初始化本地任务存储。")
        jobs = self.jobs.list_jobs()
        rows = [
            {
                "job_id": item["job_id"],
                "status": item["status"],
                "progress": item["progress"],
                "project": item["project"],
                "submitted_at": item["submitted_at"],
            }
            for item in jobs
        ]
        envelope = Envelope(
            command="job.list",
            status="success",
            data={"jobs": rows, "total": len(rows)},
            metadata={"state_file": str(self.jobs.path)},
            agent_hints=AgentHints(next_actions=["job.status", "job.wait"]),
        )
        self.log("job.list", envelope.status, envelope.metadata)
        return envelope

    def meta_list_tables(self) -> Envelope:
        tables = self.backend.list_tables()
        rows = [
            {
                "table_name": table.name,
                "table_type": table.table_type,
                "row_count": table.row_count,
                "row_count_source": table.row_count_source,
                "size_bytes": table.size_bytes,
                "owner": table.owner,
                "description": table.description,
                "partition_columns": [column.name for column in table.partition_columns],
            }
            for table in tables
        ]
        envelope = Envelope(
            command="meta.list-tables",
            status="success",
            data={"tables": rows, "total": len(rows)},
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["meta.describe", "data.sample"]),
        )
        self.log("meta.list-tables", envelope.status, envelope.metadata)
        return envelope

    def meta_describe(self, table_name: str) -> Envelope:
        table = self.backend.describe_table(table_name)
        warnings = []
        if self.remote_jobs and not table.partitions:
            warnings.append("未读取到分区列表时，可能是非分区表，也可能是账号无分区读取权限。")
        if table.row_count_source == "unavailable":
            warnings.append("当前未读取到可靠 row_count，返回值可能为 -1。")
        envelope = Envelope(
            command="meta.describe",
            status="success",
            data=self._table_payload(table),
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(
                next_actions=["data.sample", "data.profile", "query"],
                warnings=warnings,
            ),
        )
        self.log("meta.describe", envelope.status, envelope.metadata)
        return envelope

    def meta_search(self, keyword: str) -> Envelope:
        # 优先使用缓存
        cached_tables = self.cache.get_all_cached_tables(self.config.default_project)
        if cached_tables:
            matches = self._search_in_cache(keyword, cached_tables)
            source = "cache"
        else:
            matches = self.backend.search_tables(keyword)
            source = "backend"
        envelope = Envelope(
            command="meta.search",
            status="success",
            data={"keyword": keyword, "matches": matches, "total": len(matches)},
            metadata={"project": self.config.default_project, "source": source},
            agent_hints=AgentHints(
                next_actions=["meta.describe", "data.sample"],
                warnings=[] if cached_tables else ["未使用缓存，建议执行 maxc cache build 加速后续查询"],
            ),
        )
        self.log("meta.search", envelope.status, envelope.metadata)
        return envelope

    def meta_search_columns(self, keyword: str) -> Envelope:
        # 优先使用缓存
        cached_tables = self.cache.get_all_cached_tables(self.config.default_project)
        if cached_tables:
            matches = self._search_columns_in_cache(keyword, cached_tables)
            source = "cache"
        else:
            matches = self.backend.search_columns(keyword)
            source = "backend"
        envelope = Envelope(
            command="meta.search-columns",
            status="success",
            data={"keyword": keyword, "matches": matches, "total": len(matches)},
            metadata={"project": self.config.default_project, "source": source},
            agent_hints=AgentHints(
                next_actions=["meta.describe", "query"],
                warnings=[] if cached_tables else ["未使用缓存，建议执行 maxc cache build 加速后续查询"],
            ),
        )
        self.log("meta.search-columns", envelope.status, envelope.metadata)
        return envelope

    def _search_in_cache(
        self, keyword: str, cached_tables: list[dict]
    ) -> list[dict]:
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
        self, keyword: str, cached_tables: list[dict]
    ) -> list[dict]:
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

    def meta_latest_partition(self, table_name: str) -> Envelope:
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

    def meta_freshness(self, table_name: str) -> Envelope:
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

    def meta_lineage(self, table_name: str) -> Envelope:
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

    def cache_build(self, *, project: str | None = None, max_workers: int = 8) -> Envelope:
        """Build metadata cache for all tables in the project."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        target_project = project or self.config.default_project
        tables = self.backend.list_tables()
        cached_count = 0
        errors: list[str] = []

        def fetch_and_cache(table_name: str) -> str | None:
            try:
                full_table = self.backend.describe_table(table_name)
                columns = [
                    {"name": c.name, "type": c.type, "comment": c.comment}
                    for c in full_table.columns
                ]
                self.cache.cache_table(
                    project=target_project,
                    table_name=full_table.name,
                    description=full_table.description,
                    columns=columns,
                    partitions=full_table.partitions,
                    row_count=full_table.row_count,
                )
                return None
            except Exception as e:
                return f"{table_name}: {e}"

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_and_cache, t.name): t.name for t in tables}
            for future in as_completed(futures):
                error = future.result()
                if error:
                    errors.append(error)
                else:
                    cached_count += 1

        stats = self.cache.get_cache_stats(target_project)
        envelope = Envelope(
            command="cache.build",
            status="success" if not errors else "partial",
            data={
                "cached_tables": cached_count,
                "total_tables": len(tables),
                "errors": errors[:10] if errors else [],
                "stats": stats,
            },
            metadata={"project": target_project},
            agent_hints=AgentHints(
                next_actions=["meta.search", "meta.search-columns"],
                warnings=[f"缓存失败 {len(errors)} 个表"] if errors else [],
            ),
        )
        self.log("cache.build", envelope.status, envelope.metadata)
        return envelope

    def cache_status(self, *, project: str | None = None) -> Envelope:
        """Get cache status."""
        target_project = project or self.config.default_project
        stats = self.cache.get_cache_stats(target_project)
        envelope = Envelope(
            command="cache.status",
            status="success",
            data=stats,
            metadata={"project": target_project},
            agent_hints=AgentHints(
                next_actions=["cache.build"] if stats["table_count"] == 0 else ["meta.search"],
            ),
        )
        return envelope

    def cache_clear(self, *, project: str | None = None) -> Envelope:
        """Clear metadata cache."""
        target_project = project or self.config.default_project
        deleted = self.cache.clear_table_cache(target_project)
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
        table_name: str,
        semantic_desc: str,
        use_cases: list[str],
        sample_questions: list[str],
        column_semantics: list[dict],
        project: str | None = None,
    ) -> Envelope:
        """Save AI-generated semantic metadata for NL2SQL."""
        target_project = project or self.config.default_project
        self.cache.save_semantic(
            project=target_project,
            table_name=table_name,
            semantic_desc=semantic_desc,
            use_cases=use_cases,
            sample_questions=sample_questions,
            column_semantics=column_semantics,
        )
        envelope = Envelope(
            command="cache.save-semantic",
            status="success",
            data={
                "table_name": table_name,
                "semantic_desc": semantic_desc,
                "use_cases_count": len(use_cases),
                "sample_questions_count": len(sample_questions),
                "column_semantics_count": len(column_semantics),
            },
            metadata={"project": target_project},
            agent_hints=AgentHints(
                next_actions=["meta.search", "cache.get-semantic"],
                insights=["语义元数据已保存，可用于 NL2SQL 场景"],
            ),
        )
        return envelope

    def cache_get_semantic(
        self, *, table_name: str, project: str | None = None
    ) -> Envelope:
        """Get semantic metadata for a table."""
        target_project = project or self.config.default_project
        semantic = self.cache.get_semantic(target_project, table_name)
        if semantic:
            envelope = Envelope(
                command="cache.get-semantic",
                status="success",
                data={"table_name": table_name, **semantic},
                metadata={"project": target_project},
                agent_hints=AgentHints(next_actions=["query"]),
            )
        else:
            envelope = Envelope(
                command="cache.get-semantic",
                status="not_found",
                data={"table_name": table_name},
                metadata={"project": target_project},
                agent_hints=AgentHints(
                    next_actions=["cache.save-semantic"],
                    warnings=["未找到语义元数据，请先执行 semantic-index skill"],
                ),
            )
        return envelope

    def meta_partitions(self, table_name: str) -> Envelope:
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

    def meta_list_projects(self) -> Envelope:
        """List all projects owned by the current user."""
        projects = self.backend.list_projects()
        envelope = Envelope(
            command="meta.list-projects",
            status="success",
            data={"projects": projects, "total": len(projects)},
            metadata={"backend": self.config.backend.type},
            agent_hints=AgentHints(
                next_actions=["project.use", "project.info"],
                insights=[f"共发现 {len(projects)} 个您拥有的项目", "使用 maxc project info <project_name> 获取项目详细信息"],
            ),
        )
        self.log("meta.list-projects", envelope.status, envelope.metadata)
        return envelope

    def meta_list_schemas(self, *, project: str | None = None) -> Envelope:
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
                warnings=[] if rows else ["该项目没有 schemas，可能未启用 schema namespace 功能"],
            ),
        )
        self.log("meta.list-schemas", envelope.status, envelope.metadata)
        return envelope

    def project_use(self, project_name: str, *, schema: str | None = None) -> Envelope:
        """Switch to a different project (and optionally schema)."""
        # 验证项目是否存在
        try:
            project_info = self.backend.get_project_info(project_name)
        except Exception as exc:
            raise ValidationError(
                f"无法访问项目 {project_name}: {exc}",
                suggestion="请确认项目名称正确且有访问权限。",
            ) from exc

        # 更新配置文件
        config_path = default_global_config_path()
        existing = load_config_mapping(config_path) if config_path.exists() else {}
        existing["default_project"] = project_name
        if schema:
            existing["default_schema"] = schema
        elif "default_schema" in existing:
            del existing["default_schema"]
        save_config_mapping(config_path, existing)

        warnings = []
        if schema:
            warnings.append(f"已切换到项目 {project_name}，默认 schema: {schema}")
        else:
            warnings.append(f"已切换到项目 {project_name}")

        envelope = Envelope(
            command="project.use",
            status="success",
            data={
                "project": project_name,
                "schema": schema,
                "project_info": project_info,
                "config_path": str(config_path),
            },
            metadata={"previous_project": self.config.default_project},
            agent_hints=AgentHints(
                next_actions=["meta.list-tables", "meta.search", "project.info"],
                warnings=warnings,
            ),
        )
        self.log("project.use", envelope.status, envelope.metadata)
        return envelope

    def project_info(self, *, project_name: str | None = None) -> Envelope:
        """Get detailed information about a project."""
        target = project_name or self.config.default_project
        info = self.backend.get_project_info(target)
        envelope = Envelope(
            command="project.info",
            status="success",
            data=info,
            metadata={"project": target},
            agent_hints=AgentHints(
                next_actions=["meta.list-tables", "meta.list-schemas"],
                insights=[f"当前项目: {target}"],
            ),
        )
        self.log("project.info", envelope.status, envelope.metadata)
        return envelope

    def data_sample(
        self,
        table_name: str,
        rows: int = 5,
        *,
        partition: str | None = None,
        columns: list[str] | None = None,
    ) -> Envelope:
        if rows <= 0:
            raise ValidationError("--rows 必须大于 0。")
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

    def data_profile(self, table_name: str, *, partition: str | None = None) -> Envelope:
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
        access_id: str | None = None,
        secret_access_key: str | None = None,
        project: str | None = None,
        endpoint: str | None = None,
        region_name: str | None = None,
        tunnel_endpoint: str | None = None,
        from_env: bool = False,
        no_validate: bool = False,
        target_config_path: Path | None = None,
    ) -> Envelope:
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

        missing = [
            name
            for name, value in (
                ("access_id", resolved_auth.access_id),
                ("secret_access_key", resolved_auth.secret_access_key),
                ("project", resolved_auth.project),
                ("endpoint", resolved_auth.endpoint),
            )
            if not value
        ]
        if missing:
            raise ValidationError(
                f"auth login 缺少必填字段: {', '.join(missing)}。",
                suggestion="请通过参数、--from-env，或在交互终端中运行 auth login 补齐缺失字段。",
            )

        persist_login_config(
            target_path,
            auth=resolved_auth,
            backend_type="auto",
        )

        warnings: list[str] = []
        if any(
            env_settings.get(name)
            for name in ("access_id", "secret_access_key", "project", "endpoint")
        ):
            warnings.append("当前 shell 中存在 MaxCompute 环境变量；后续命令会优先使用环境变量，可能覆盖刚保存的登录配置。")

        if no_validate:
            payload = {
                "authenticated": None,
                "backend": "odps",
                "auth_type": "access_key",
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
            warnings.append("登录信息已保存，但未执行远程校验。")
        else:
            payload, validate_warnings = validate_login_settings(
                settings={
                    "access_id": resolved_auth.access_id,
                    "secret_access_key": resolved_auth.secret_access_key,
                    "project": resolved_auth.project,
                    "endpoint": resolved_auth.endpoint,
                    "region_name": resolved_auth.region_name,
                    "tunnel_endpoint": resolved_auth.tunnel_endpoint,
                },
                allowed_operations=self.config.allowed_operations,
            )
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

    def auth_whoami(self) -> Envelope:
        payload, warnings = self.backend.whoami_info(project=self.config.default_project)
        envelope = Envelope(
            command="auth.whoami",
            status="success",
            data=payload,
            metadata={"project": self.config.default_project},
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
        table_name: str,
        operation: str,
        project: str | None = None,
    ) -> Envelope:
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

    def _resolve_login_value(
        self,
        *,
        provided: str | None,
        env_value: str | None,
        existing_value: str | None,
        prompt: str,
        required: bool,
        secret: bool,
        use_env: bool,
    ) -> str | None:
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

    def schema_diff(self, left_table: str, right_table: str) -> Envelope:
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

    def partition_diff(self, left_table: str, right_table: str) -> Envelope:
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
        left_table: str,
        right_table: str,
        *,
        keys: list[str],
        columns: list[str] | None = None,
        rows: int = 100,
        partition: str | None = None,
        left_partition: str | None = None,
        right_partition: str | None = None,
    ) -> Envelope:
        if rows <= 0:
            raise ValidationError("--rows 必须大于 0。")
        if not keys:
            raise ValidationError("--keys 至少要指定一个列。")

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

    def agent_context(self) -> Envelope:
        """Return project context without listing tables (too slow on large projects)."""
        envelope = Envelope(
            command="agent.context",
            status="success",
            data={
                "project": self.config.default_project,
                "region": self.config.default_region,
                "backend": self.config.backend.type,
                "project_context": self.config.project_context,
                "allowed_operations": self.config.allowed_operations,
                "cost_threshold_cu": self.config.cost_threshold_cu,
                "sensitive_columns": self.config.sensitive_columns,
            },
            metadata={
                "config_sources": [str(path) for path in self.config.sources],
                "state_dir": str(self.config.state_dir),
                "job_mode": "remote" if self.remote_jobs else "local",
            },
            agent_hints=AgentHints(
                next_actions=["meta.search", "meta.list-tables"],
                insights=["使用 meta list-tables 获取表列表，使用 meta search 搜索表"],
            ),
        )
        self.log("agent.context", envelope.status, envelope.metadata)
        return envelope

    def feature_unavailable(self, command: str, message: str) -> Envelope:
        raise FeatureUnavailableError(
            message,
            suggestion="请查看 maxc --help 了解当前支持的命令。",
        )

    def log(
        self,
        command: str,
        status: str,
        metadata: dict[str, Any] | None = None,
        *,
        error: dict[str, Any] | None = None,
    ) -> None:
        self.audit.log(
            {
                "command": command,
                "status": status,
                "metadata": metadata or {},
                "error": error,
            }
        )

    def _submit_remote_job(
        self,
        *,
        sql: str,
        project: str,
        cost_check: float | None,
        idempotency_key: str | None,
    ) -> JobInfo:
        if cost_check is not None:
            raise FeatureUnavailableError(
                "真实 MaxCompute backend 暂未支持 CU 口径的 --cost-check。",
                suggestion="请先执行 --dry-run 查看 SQLCost 元数据，或去掉 --cost-check。",
            )
        return self.backend.submit_query(
            sql,
            project=project,
            idempotency_key=idempotency_key,
        )

    def _execute_query(
        self,
        *,
        sql: str,
        project: str,
        max_rows: int,
        offset: int,
        dry_run: bool,
        cost_check: float | None,
        retry_on: list[str],
        max_retries: int,
        strict_cost_check: bool,
    ) -> QueryResult:
        if sql.startswith("@natural"):
            raise FeatureUnavailableError(
                "@natural 属于路线图 Q2 范围，当前 MVP 只支持直接 SQL。",
                suggestion="请先用 maxc meta search / describe 理清表，再提交 SQL。",
            )

        attempts = 0
        while True:
            try:
                if cost_check is not None and strict_cost_check and not self.backend.supports_cost_check:
                    raise FeatureUnavailableError(
                        "当前真实 backend 暂未提供 CU 口径成本检查。",
                        suggestion="请去掉 --cost-check，或使用 --dry-run 查看 SQLCost 元数据。",
                    )

                result = self.backend.execute_query(
                    sql,
                    project=project,
                    max_rows=max_rows,
                    dry_run=dry_run,
                    offset=offset,
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
        sql: str,
        project: str,
        explain: bool,
    ) -> dict[str, Any]:
        if sql.startswith("@natural"):
            raise FeatureUnavailableError(
                "@natural 属于路线图 Q2 范围，当前 MVP 只支持直接 SQL。",
                suggestion="请先用 maxc meta search / describe 理清表，再提交 SQL。",
            )
        if explain:
            return self.backend.explain_query(sql, project=project)
        return self.backend.estimate_query_cost(sql, project=project)

    def _build_query_envelope(
        self,
        *,
        command: str,
        result: QueryResult,
        dry_run: bool,
        session_id: int | None = None,
    ) -> Envelope:
        insights = []
        next_actions = ["meta.describe"] if result.tables_used else []
        if result.has_more:
            next_actions.append("query.paginate")
        if dry_run:
            next_actions.append("job.submit")
            insights.append("dry-run 已返回预估成本或 SQLCost 元数据，可据此决定是否继续执行。")
        elif not result.rows:
            insights.append("结果为空，建议检查过滤条件、分区或表选择。")

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
        command: str,
        sql: str,
        analysis: dict[str, Any],
    ) -> Envelope:
        warnings = list(analysis.get("warnings", []))
        next_actions = ["query"]
        if analysis.get("tables_used"):
            next_actions.append("meta.describe")
        if command == "query.cost":
            next_actions.insert(0, "query.explain")
        insights = []
        if analysis.get("estimated_input_size_bytes") == 0:
            insights.append("预估扫描输入为 0，可能是常量查询或优化后无需扫描数据。")

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

    def _job_info_envelope(self, command: str, info: JobInfo) -> Envelope:
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

    def _local_job_info(self, job: dict[str, Any]) -> JobInfo:
        status = job["status"]
        stage = "queue" if status == "pending" else "completed" if status == "success" else "failed"
        failure_reason = "任务已取消。" if job.get("cancelled") else None
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

    def _query_result_payload(self, result: QueryResult) -> dict[str, Any]:
        envelope = self._build_query_envelope(
            command="query",
            result=result,
            dry_run=False,
        )
        return envelope.to_dict()

    def _job_events(self, job: dict[str, Any]) -> list[dict[str, Any]]:
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
            raise ValidationError("任务已取消，不能继续等待。")

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
        before: JobInfo,
        after: JobInfo,
        result: QueryResult,
    ) -> list[dict[str, Any]]:
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

    def _table_payload(self, table: TableDefinition) -> dict[str, Any]:
        return {
            "table_name": table.name,
            "description": table.description,
            "row_count": table.row_count,
            "row_count_source": table.row_count_source,
            "size_bytes": table.size_bytes,
            "owner": table.owner,
            "table_type": table.table_type,
            "created_at": table.created_at,
            "updated_at": table.updated_at,
            "schema": [
                {"name": column.name, "type": column.type, "comment": column.comment}
                for column in table.columns
            ],
            "partition_columns": [
                {"name": column.name, "type": column.type, "comment": column.comment}
                for column in table.partition_columns
            ],
            "partitions": table.partitions,
            "lineage": {
                "upstream_tables": table.upstream_tables,
                "downstream_tables": table.downstream_tables,
            },
            "sample_preview": table.sample_rows[:2],
            "extra_metadata": table.extra_metadata,
        }


def build_schema_diff_payload(left: TableDefinition, right: TableDefinition) -> dict[str, Any]:
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


def build_partition_diff_payload(left: TableDefinition, right: TableDefinition) -> dict[str, Any]:
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
    left: TableDefinition,
    right: TableDefinition,
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    keys: list[str],
    compared_columns: list[str],
    rows_limit: int,
    left_partition: str | None,
    right_partition: str | None,
) -> tuple[dict[str, Any], list[str]]:
    left_by_key = index_rows_by_key(left_rows, keys=keys, table_name=left.name)
    right_by_key = index_rows_by_key(right_rows, keys=keys, table_name=right.name)
    left_keys = set(left_by_key)
    right_keys = set(right_by_key)
    common_keys = sorted(left_keys & right_keys)
    left_only_keys = sorted(left_keys - right_keys)
    right_only_keys = sorted(right_keys - left_keys)

    mismatched_rows: list[dict[str, Any]] = []
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
            f"当前 data diff 只比较每侧最多 {rows_limit} 行的只读快照；"
            "如需提高覆盖率，请调大 --rows 并尽量缩小分区范围。"
        )
    ]
    if not compared_columns:
        warnings.append("当前只比较 key 存在性，未比较非 key 列值。")

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
    left_columns: list[Any],
    right_columns: list[Any],
    *,
    scope: str,
    added_is_breaking: bool = False,
) -> dict[str, Any]:
    left_by_name = {column.name: column for column in left_columns}
    right_by_name = {column.name: column for column in right_columns}
    left_names = set(left_by_name)
    right_names = set(right_by_name)

    added = [column_payload(right_by_name[name]) for name in sorted(right_names - left_names)]
    removed = [column_payload(left_by_name[name]) for name in sorted(left_names - right_names)]
    unchanged: list[str] = []
    changed: list[dict[str, Any]] = []
    breaking_changes: list[dict[str, Any]] = []
    non_breaking_changes: list[dict[str, Any]] = []

    for name in sorted(left_names & right_names):
        left = left_by_name[name]
        right = right_by_name[name]
        field_changes: dict[str, dict[str, Any]] = {}
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


def column_payload(column: Any) -> dict[str, Any]:
    return {
        "name": column.name,
        "type": column.type,
        "comment": column.comment,
    }


def resolve_data_diff_columns(
    left: TableDefinition,
    right: TableDefinition,
    *,
    keys: list[str],
    requested_columns: list[str] | None,
) -> list[str]:
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
            f"data diff key 列在两侧表中不完整: {', '.join(missing_keys)}",
            suggestion="请先执行 maxc meta describe 或 maxc diff schema 检查可用列。",
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
                f"data diff 对比列在两侧表中不完整: {', '.join(missing_columns)}",
                suggestion="请先执行 maxc meta describe 或 maxc diff schema 检查可用列。",
            )
        return compared_columns

    return [
        column
        for column in left_columns
        if column in right_columns and column not in deduped_keys
    ]


def all_table_column_names(table: TableDefinition) -> list[str]:
    return [column.name for column in [*table.columns, *table.partition_columns]]


def dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped


def index_rows_by_key(
    rows: list[dict[str, Any]],
    *,
    keys: list[str],
    table_name: str,
) -> dict[tuple[Any, ...], dict[str, Any]]:
    indexed: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicate_keys: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(normalize_diff_value(row.get(column)) for column in keys)
        if key in indexed:
            duplicate_keys.append(key_to_payload(keys, key))
            continue
        indexed[key] = row
    if duplicate_keys:
        examples = ", ".join(str(item) for item in duplicate_keys[:3])
        raise ValidationError(
            f"data diff 在表 {table_name} 的快照中发现重复 key，无法稳定对齐: {examples}",
            suggestion="请改用更细粒度的 --keys，或缩小分区范围后再重试。",
        )
    return indexed


def key_to_payload(columns: list[str], values: tuple[Any, ...]) -> dict[str, Any]:
    return {
        column: value
        for column, value in zip(columns, values, strict=False)
    }


def normalize_diff_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return value
    return value


def read_stdin() -> str:
    return sys.stdin.read()
