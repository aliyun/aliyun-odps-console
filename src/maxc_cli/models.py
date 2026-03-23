"""Data models for MaxCompute CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AgentHints:
    next_actions: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    insights: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.next_actions:
            payload["next_actions"] = self.next_actions
        if self.warnings:
            payload["warnings"] = self.warnings
        if self.insights:
            payload["insights"] = self.insights
        return payload


@dataclass(slots=True)
class Envelope:
    command: str
    status: str
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: Any | None = None
    agent_hints: AgentHints | None = None
    version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "version": self.version,
            "command": self.command,
            "status": self.status,
            "data": self.data,
            "metadata": self.metadata,
        }
        payload["error"] = self.error.to_dict() if self.error else None
        payload["agent_hints"] = (
            self.agent_hints.to_dict() if self.agent_hints else None
        )
        return payload


@dataclass(slots=True)
class QueryResult:
    """Result of a query execution."""

    rows: list[dict[str, Any]]
    schema: list[dict[str, Any]]
    total_rows: int
    returned_rows: int
    has_more: bool
    next_cursor: str | None
    elapsed_ms: int
    bytes_scanned: int | None
    project: str
    sql_executed: str
    tables_used: list[str]
    warnings: list[str] = field(default_factory=list)
    job_id: str | None = None
    submitted_at: str | None = None
    completed_at: str | None = None
    extra_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobInfo:
    """Information about a job."""

    job_id: str
    status: str
    project: str
    progress: int
    stage: str | None = None
    retryable: bool | None = None
    failure_reason: str | None = None
    task_summary: dict[str, Any] = field(default_factory=dict)
    sql: str | None = None
    submitted_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None
    logview: str | None = None
    error_message: str | None = None
    warnings: list[str] = field(default_factory=list)
