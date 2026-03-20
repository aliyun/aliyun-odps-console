from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from .audit import AuditLogger
from .backend import JobInfo, QueryResult, create_backend
from .config import TableDefinition, load_config
from .exceptions import (
    CostLimitExceededError,
    FeatureUnavailableError,
    MaxCError,
    PermissionDeniedError,
    ValidationError,
)
from .models import AgentHints, Envelope
from .skills import SkillRegistry
from .store import JobStore
from .utils import decode_cursor, detect_operation, now_utc_iso


class MaxCApp:
    def __init__(self, *, cwd: Path, config_path: Path | None = None) -> None:
        self.cwd = cwd
        self.config = load_config(cwd, config_path)
        self.backend = create_backend(self.config)
        self.remote_jobs = getattr(self.backend, "supports_remote_jobs", False)
        self.jobs = None if self.remote_jobs else JobStore(self.config.state_dir)
        self.skills = SkillRegistry(self.config.skill_dirs)
        self.audit = AuditLogger(self.config.agent.audit_log or self.config.state_dir / "audit.log")

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
        offset = decode_cursor(cursor)
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
                    "cost_cu": result.cost_cu,
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
        envelope = Envelope(
            command="job.status",
            status=job["status"],
            data={
                "job_id": job["job_id"],
                "status": job["status"],
                "progress": job["progress"],
                "sql": job["sql"],
            },
            metadata={
                "project": job["project"],
                "submitted_at": job["submitted_at"],
                "updated_at": job["updated_at"],
            },
            agent_hints=AgentHints(next_actions=["job.wait", "job.result"]),
        )
        self.log("job.status", envelope.status, envelope.metadata)
        return envelope

    def job_wait(self, job_id: str) -> tuple[Envelope, list[dict[str, Any]]]:
        if self.remote_jobs:
            before = self.backend.get_job(job_id, project=self.config.default_project)
            after = self.backend.wait_job(job_id, project=self.config.default_project)
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
        envelope = Envelope(
            command="job.wait",
            status="success",
            data=stored["data"],
            metadata={
                **stored["metadata"],
                "job_id": job_id,
                "submitted_at": final_job["submitted_at"],
                "completed_at": final_job["completed_at"],
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
            envelope = Envelope(
                command="job.result",
                status=job["status"],
                data={"job_id": job_id},
                metadata={
                    "project": job["project"],
                    "submitted_at": job["submitted_at"],
                    "updated_at": job["updated_at"],
                },
                agent_hints=AgentHints(next_actions=["job.wait"]),
            )
            self.log("job.result", envelope.status, envelope.metadata)
            return envelope

        stored = job["result"]
        envelope = Envelope(
            command="job.result",
            status="success",
            data=stored["data"],
            metadata={
                **stored["metadata"],
                "job_id": job_id,
                "submitted_at": job["submitted_at"],
                "completed_at": job.get("completed_at", job["updated_at"]),
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
        matches = self.backend.search_tables(keyword)
        envelope = Envelope(
            command="meta.search",
            status="success",
            data={"keyword": keyword, "matches": matches, "total": len(matches)},
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["meta.describe", "data.sample"]),
        )
        self.log("meta.search", envelope.status, envelope.metadata)
        return envelope

    def meta_search_columns(self, keyword: str) -> Envelope:
        matches = self.backend.search_columns(keyword)
        envelope = Envelope(
            command="meta.search-columns",
            status="success",
            data={"keyword": keyword, "matches": matches, "total": len(matches)},
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["meta.describe", "query"]),
        )
        self.log("meta.search-columns", envelope.status, envelope.metadata)
        return envelope

    def meta_lineage(self, table_name: str) -> Envelope:
        table = self.backend.describe_table(table_name)
        warnings = []
        if self.remote_jobs:
            warnings.append("当前版本未接入 MaxCompute 血缘 API，lineage 先返回空数组占位。")
        envelope = Envelope(
            command="meta.lineage",
            status="success",
            data={
                "table_name": table.name,
                "upstream_tables": table.upstream_tables,
                "downstream_tables": table.downstream_tables,
            },
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["meta.describe"], warnings=warnings),
        )
        self.log("meta.lineage", envelope.status, envelope.metadata)
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

    def data_sample(self, table_name: str, rows: int = 5) -> Envelope:
        table, sample_rows = self.backend.sample_table(table_name, rows)
        envelope = Envelope(
            command="data.sample",
            status="success",
            data={
                "table_name": table.name,
                "rows": sample_rows,
                "returned_rows": len(sample_rows),
                "schema": [
                    {"name": column.name, "type": column.type, "comment": column.comment}
                    for column in table.columns
                ],
            },
            metadata={"project": self.config.default_project, "requested_rows": rows},
            agent_hints=AgentHints(next_actions=["data.profile", "query"]),
        )
        self.log("data.sample", envelope.status, envelope.metadata)
        return envelope

    def data_profile(self, table_name: str) -> Envelope:
        profile = self.backend.profile_table(table_name)
        envelope = Envelope(
            command="data.profile",
            status="success",
            data=profile,
            metadata={"project": self.config.default_project},
            agent_hints=AgentHints(next_actions=["query"]),
        )
        self.log("data.profile", envelope.status, envelope.metadata)
        return envelope

    def agent_context(self) -> Envelope:
        skills = [item.summary() for item in self.skills.list()]
        tables = self.backend.list_tables()
        table_names = [table.name for table in tables[:50]]
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
                "catalog": {
                    "table_count": len(tables),
                    "tables": table_names,
                    "truncated": len(tables) > len(table_names),
                },
                "skills": skills,
            },
            metadata={
                "config_sources": [str(path) for path in self.config.sources],
                "state_dir": str(self.config.state_dir),
                "skill_dirs": [str(path) for path in self.config.skill_dirs],
                "job_mode": "remote" if self.remote_jobs else "local",
            },
            agent_hints=AgentHints(next_actions=["meta.search", "agent.skill"]),
        )
        self.log("agent.context", envelope.status, envelope.metadata)
        return envelope

    def list_skills(self) -> Envelope:
        skills = [item.summary() for item in self.skills.list()]
        envelope = Envelope(
            command="skill.list",
            status="success",
            data={"skills": skills, "total": len(skills)},
            metadata={"skill_dirs": [str(path) for path in self.config.skill_dirs]},
            agent_hints=AgentHints(next_actions=["skill.info", "agent.skill"]),
        )
        self.log("skill.list", envelope.status, envelope.metadata)
        return envelope

    def skill_info(self, skill_id: str) -> Envelope:
        skill = self.skills.get(skill_id)
        envelope = Envelope(
            command="skill.info",
            status="success",
            data={
                "skill": skill.summary(),
                "input_schema": skill.input_schema,
                "guards": skill.guards,
                "implementation": skill.implementation,
            },
            metadata={"path": str(skill.path)},
            agent_hints=AgentHints(next_actions=["agent.skill"]),
        )
        self.log("skill.info", envelope.status, envelope.metadata)
        return envelope

    def execute_skill(self, skill_id: str, raw_input: dict[str, Any]) -> Envelope:
        skill = self.skills.get(skill_id)
        resolved_input = skill.resolve_input(raw_input, self.config)
        target = skill.implementation.get("target")

        if target == "query":
            operation = detect_operation(str(resolved_input["query"]))
            allowed = [item.upper() for item in skill.guards.get("allow_operations", [])]
            if allowed and operation not in allowed:
                raise PermissionDeniedError(
                    f"Skill {skill_id} 只允许操作: {', '.join(allowed)}，实际收到 {operation}。"
                )
            max_cost = skill.guards.get("max_cost_cu")
            result = self._execute_query(
                sql=str(resolved_input["query"]),
                project=str(resolved_input.get("project", self.config.default_project)),
                max_rows=int(resolved_input.get("max_rows", 100)),
                offset=0,
                dry_run=False,
                cost_check=float(max_cost) if max_cost is not None else None,
                retry_on=[],
                max_retries=0,
                strict_cost_check=False,
            )
            skill_output = self._build_query_envelope(
                command="agent.skill",
                result=result,
                dry_run=False,
            )
            metadata = skill_output.metadata
            hints = skill_output.agent_hints or AgentHints()
            status = skill_output.status
            output_payload = skill_output.data
        elif target == "meta.describe":
            detail = self.meta_describe(str(resolved_input["table"]))
            output_payload = detail.data
            metadata = detail.metadata
            hints = detail.agent_hints or AgentHints()
            status = detail.status
        elif target == "data.sample":
            sample = self.data_sample(
                str(resolved_input["table"]),
                rows=int(resolved_input.get("rows", 5)),
            )
            output_payload = sample.data
            metadata = sample.metadata
            hints = sample.agent_hints or AgentHints()
            status = sample.status
        else:
            raise FeatureUnavailableError(f"Skill target 未实现: {target}")

        wrapped = Envelope(
            command="agent.skill",
            status=status,
            data={
                "skill": skill.summary(),
                "input": resolved_input,
                "output": output_payload,
            },
            metadata={"skill_id": skill.skill_id, "skill_version": skill.version, **metadata},
            agent_hints=hints,
        )
        self.log("agent.skill", wrapped.status, wrapped.metadata)
        return wrapped

    def feature_unavailable(self, command: str, message: str) -> Envelope:
        raise FeatureUnavailableError(
            message,
            suggestion="请先使用当前已实现的 query / job / meta / data / agent context / agent skill。",
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

                if cost_check is not None and result.cost_cu is not None and result.cost_cu > cost_check:
                    raise CostLimitExceededError(
                        f"预估成本 {result.cost_cu} CU 超过阈值 {cost_check} CU。",
                        suggestion="请缩小分区范围、减少扫描列，或上调 --cost-check。",
                    )
                if cost_check is not None and result.cost_cu is None and not strict_cost_check:
                    result.warnings.append(
                        "backend 未返回 CU 口径成本，已跳过 Skill 内部 max_cost_cu 校验。"
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

        metadata = {
            "project": result.project,
            "elapsed_ms": result.elapsed_ms,
            "bytes_scanned": result.bytes_scanned,
            "cost_cu": result.cost_cu,
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
                "next_cursor": result.next_cursor,
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
            next_actions = ["job.status"]
        return Envelope(
            command=command,
            status=info.status,
            data={
                "job_id": info.job_id,
                "status": info.status,
                "progress": info.progress,
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
                    "cost_cu": job["result"]["metadata"]["cost_cu"],
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
                "cost_cu": job["result"]["metadata"]["cost_cu"],
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
                "cost_cu": result.cost_cu,
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


def load_skill_input(raw_input: str | None) -> dict[str, Any]:
    if not raw_input:
        return {}
    try:
        payload = json.loads(raw_input)
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"--input 不是合法 JSON: {exc.msg}",
            suggestion='示例: --input \'{"query":"SELECT * FROM your_table LIMIT 10"}\'',
        ) from exc
    if not isinstance(payload, dict):
        raise ValidationError("--input 必须是 JSON 对象。")
    return payload


def read_stdin() -> str:
    return sys.stdin.read()
