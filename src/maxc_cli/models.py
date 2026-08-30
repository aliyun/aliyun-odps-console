"""Data models for MaxCompute CLI."""


import re
import shlex
from dataclasses import dataclass, field
from typing import Any

from .utils import current_cli_entry_point, distribution_cli_text

_PLACEHOLDER_RE = re.compile(r'<(\w+)>')


@dataclass
class SuggestedAction:
    id: str
    title: str
    command: str
    executable: bool = True
    placeholders: 'dict[str, str]' = field(default_factory=dict)
    args_schema: 'dict[str, Any]' = field(default_factory=dict)
    effect: 'str' = "read"
    confirmation_required: 'bool' = False
    agent_allowed: 'bool' = True

    def to_dict(self) -> 'dict[str, Any]':
        return {
            "id": self.id,
            "title": self.title,
            "command": distribution_cli_text(self.command),
            "executable": self.executable,
            "placeholders": self.placeholders,
            "args_schema": self.args_schema,
            "effect": self.effect,
            "confirmation_required": self.confirmation_required,
            "agent_allowed": self.agent_allowed,
        }


def suggested_action_is_safe(action: 'SuggestedAction') -> 'bool':
    """Whether an action may be presented as immediately copyable/executable."""
    return bool(
        getattr(action, "executable", False)
        and getattr(action, "agent_allowed", False)
        and not getattr(action, "confirmation_required", True)
    )


@dataclass
class AgentHints:
    actions: 'list[SuggestedAction]' = field(default_factory=list)
    warnings: 'list[str]' = field(default_factory=list)
    insights: 'list[str]' = field(default_factory=list)

    def to_dict(self) -> 'dict[str, Any]':
        payload: dict[str, Any] = {}
        if self.actions:
            rendered_actions = [a.to_dict() for a in self.actions]
            payload["actions"] = rendered_actions
            payload["action_ids"] = [a.id for a in self.actions]
            safe_next_actions = [
                item["command"]
                for action, item in zip(self.actions, rendered_actions)
                if suggested_action_is_safe(action)
            ]
            if safe_next_actions:
                payload["next_actions"] = safe_next_actions
        if self.warnings:
            payload["warnings"] = [distribution_cli_text(item) for item in self.warnings]
        if self.insights:
            payload["insights"] = [distribution_cli_text(item) for item in self.insights]
        return payload


@dataclass
class Envelope:
    command: 'str'
    status: 'str'
    data: 'dict[str, Any]' = field(default_factory=dict)
    metadata: 'dict[str, Any]' = field(default_factory=dict)
    error: 'Any | None' = None
    agent_hints: 'AgentHints | None' = None
    version: 'str' = "2.0"

    def to_dict(self, *, normalize: 'bool' = True) -> 'dict[str, Any]':
        command = _format_command_path(self.command) if normalize else self.command
        data = _normalize_data(self.command, self.data) if normalize else self.data
        payload = {
            "version": self.version,
            "command": command,
            "status": self.status,
            "data": data,
            "metadata": self.metadata,
        }
        payload["error"] = self.error.to_dict() if self.error else None
        payload["agent_hints"] = _render_agent_hints(self)
        return payload


@dataclass
class QueryResult:
    """Result of a query execution."""

    rows: 'list[dict[str, Any]]'
    schema: 'list[dict[str, Any]]'
    total_rows: 'int'
    returned_rows: 'int'
    has_more: 'bool'
    next_cursor: 'str | None'
    elapsed_ms: 'int'
    bytes_scanned: 'int | None'
    project: 'str'
    sql_executed: 'str'
    tables_used: 'list[str]'
    warnings: 'list[str]' = field(default_factory=list)
    job_id: 'str | None' = None
    submitted_at: 'str | None' = None
    completed_at: 'str | None' = None
    session_task_name: 'str | None' = None
    session_subquery_id: 'int | None' = None
    session_project_name: 'str | None' = None
    session_is_select: 'bool | None' = None
    extra_metadata: 'dict[str, Any]' = field(default_factory=dict)


@dataclass
class JobInfo:
    """Information about a job."""

    job_id: 'str'
    status: 'str'
    project: 'str'
    progress: 'int'
    stage: 'str | None' = None
    retryable: 'bool | None' = None
    failure_reason: 'str | None' = None
    task_summary: 'dict[str, Any]' = field(default_factory=dict)
    sql: 'str | None' = None
    submitted_at: 'str | None' = None
    updated_at: 'str | None' = None
    completed_at: 'str | None' = None
    logview: 'str | None' = None
    error_message: 'str | None' = None
    warnings: 'list[str]' = field(default_factory=list)
    session_task_name: 'str | None' = None
    session_subquery_id: 'int | None' = None
    session_project_name: 'str | None' = None
    session_is_select: 'bool | None' = None


def _format_command_path(command: 'str') -> 'str':
    if "." not in command:
        return command
    return command.replace(".", " ")


def _normalize_data(command: 'str', data: 'dict[str, Any]') -> 'dict[str, Any]':
    if not isinstance(data, dict):
        return {"value": data}

    if _already_normalized(command, data):
        return data

    if command in {"query", "job.wait", "job.result"}:
        if "rows" in data or "total_rows" in data or "next_cursor" in data:
            normalized = {
                "result": {
                    "rows": data.get("rows", []),
                    "schema": data.get("schema", []),
                    "row_count": data.get("total_rows"),
                    "returned_rows": data.get("returned_rows"),
                },
                "pagination": {
                    "has_more": data.get("has_more", False),
                    "next_cursor": data.get("next_cursor"),
                },
            }
            if "safety" in data:
                normalized["safety"] = data["safety"]
            return normalized
        if data.get("job_id") is not None:
            return {"job": dict(data)}
    if command in {"query.cost", "query.explain"}:
        analysis = dict(data)
        safety = analysis.pop("safety", None)
        result = {"analysis": analysis}
        if safety:
            result["safety"] = safety
        return result
    if command == "auth.whoami":
        options = data.get("auth_options")
        identity = {key: value for key, value in data.items() if key != "auth_options"}
        payload: dict[str, Any] = {"identity": identity}
        if options is not None:
            payload["auth_options"] = options
        return payload
    if command == "auth.login":
        identity = {
            key: value
            for key, value in data.items()
            if key not in {"saved", "validated"}
        }
        return {
            "identity": identity,
            "persistence": {
                "saved": data.get("saved"),
                "validated": data.get("validated"),
            },
        }
    if command in {"auth.login-ncs", "auth.login-external"}:
        if "raw_lines" in data or "raw_output" in data:
            return {"accounts": data}
        identity = {
            key: value
            for key, value in data.items()
            if key not in {"saved", "validated"}
        }
        return {
            "identity": identity,
            "persistence": {
                "saved": data.get("saved"),
                "validated": data.get("validated"),
            },
        }
    if command == "auth.can-i":
        return {"authorization": data}
    if command == "meta.list-tables":
        pagination: dict[str, Any] = {
            "total": data.get("total"),
            "has_more": data.get("has_more", False),
        }
        if data.get("next_cursor") is not None:
            pagination["next_cursor"] = data.get("next_cursor")
        if data.get("limit") is not None:
            pagination["limit"] = data.get("limit")
        if data.get("offset") is not None:
            pagination["offset"] = data.get("offset")
        result = {
            "tables": data.get("tables", []),
            "pagination": pagination,
        }
        if "schema" in data:
            result["schema"] = data.get("schema")
        if "namespace_model" in data:
            result["namespace_model"] = data.get("namespace_model")
        return result
    if command in {"meta.list-projects", "meta.list-schemas"}:
        collection_key = "projects" if command == "meta.list-projects" else "schemas"
        return {
            collection_key: data.get(collection_key, []),
            "pagination": {
                "total": data.get("total"),
                "has_more": False,
            },
        }
    if command in {"meta.search", "meta.search-columns"}:
        pagination: dict[str, Any] = {
            "total": data.get("total"),
            "has_more": data.get("has_more", False),
        }
        if data.get("limit") is not None:
            pagination["limit"] = data.get("limit")
        return {
            "search": {
                "keyword": data.get("keyword"),
                "matches": data.get("matches", []),
            },
            "pagination": pagination,
        }
    if command == "meta.describe":
        return {"table": data}
    if command == "meta.partitions":
        return {
            "table": {
                "table_name": data.get("table_name"),
                "schema_name": data.get("schema_name"),
                "qualified_name": data.get("qualified_name"),
            },
            "partitions": data.get("partitions", []),
        }
    if command in {"meta.latest-partition", "meta.freshness"}:
        key = {
            "meta.latest-partition": "partition",
            "meta.freshness": "freshness",
        }[command]
        return {key: data}
    if command == "data.sample":
        return {
            "sample": {
                "table_name": data.get("table_name"),
                "schema_name": data.get("schema_name"),
                "qualified_name": data.get("qualified_name"),
                "applied_partition": data.get("applied_partition"),
                "selected_columns": data.get("selected_columns"),
                "rows": data.get("rows", []),
                "schema": data.get("schema", []),
                "returned_rows": data.get("returned_rows"),
            }
        }
    if command == "data.profile":
        return {"profile": data}
    if command.startswith("project."):
        return {"project": data}
    if command == "job.list":
        return {
            "jobs": data.get("jobs", []),
            "pagination": {
                "total": data.get("total"),
                "has_more": data.get("has_more", False),
            },
        }
    if command in {"job.status", "job.cancel"}:
        return {"job": data}
    if command == "job.diagnose":
        return {"diagnosis": data}
    if command == "agent.context":
        return {"context": data}

    return data


def _already_normalized(command: 'str', data: 'dict[str, Any]') -> 'bool':
    if command in {"query", "job.wait", "job.result"}:
        return _has_mapping(data, "job") or _has_mapping(data, "result", "pagination")
    if command in {"query.cost", "query.explain"}:
        return _has_mapping(data, "analysis")
    if command == "auth.whoami":
        return _has_mapping(data, "identity")
    if command == "auth.login":
        return _has_mapping(data, "identity", "persistence")
    if command in {"auth.login-ncs", "auth.login-external"}:
        return _has_mapping(data, "identity", "persistence") or _has_mapping(data, "accounts")
    if command == "auth.can-i":
        return _has_mapping(data, "authorization")
    if command == "meta.list-tables":
        return _has_sequence(data, "tables") and _has_mapping(data, "pagination")
    if command == "meta.list-projects":
        return _has_sequence(data, "projects") and _has_mapping(data, "pagination")
    if command == "meta.list-schemas":
        return _has_sequence(data, "schemas") and _has_mapping(data, "pagination")
    if command in {"meta.search", "meta.search-columns"}:
        return _has_mapping(data, "search", "pagination")
    if command == "meta.describe":
        return _has_mapping(data, "table")
    if command == "meta.partitions":
        return _has_mapping(data, "table") and _has_sequence(data, "partitions")
    if command == "meta.latest-partition":
        return _has_mapping(data, "partition")
    if command == "meta.freshness":
        return _has_mapping(data, "freshness")
    if command == "data.sample":
        return _has_mapping(data, "sample")
    if command == "data.profile":
        return _has_mapping(data, "profile")
    if command == "job.list":
        return _has_sequence(data, "jobs") and _has_mapping(data, "pagination")
    if command in {"job.status", "job.cancel"}:
        return _has_mapping(data, "job")
    if command == "job.diagnose":
        return _has_mapping(data, "diagnosis")
    if command == "agent.context":
        return _has_mapping(data, "context")
    return False


def _has_mapping(data: 'dict[str, Any]', *keys: 'str') -> 'bool':
    return all(isinstance(data.get(key), dict) for key in keys)


def _has_sequence(data: 'dict[str, Any]', key: 'str') -> 'bool':
    return isinstance(data.get(key), list)


def _render_agent_hints(envelope: 'Envelope') -> 'dict[str, Any] | None':
    if envelope.agent_hints is None:
        return None
    payload = envelope.agent_hints.to_dict()
    return payload or None


_ACTION_TITLES: 'dict[str, str]' = {
    "query": "Run query",
    "query.cost": "Estimate query cost",
    "query.explain": "Explain query plan",
    "query.paginate": "Next page",
    "query.next_page": "Next page",
    "job.submit": "Submit async job",
    "job.status": "Check job status",
    "job.wait": "Wait for job",
    "job.result": "Fetch job results",
    "job.cancel": "Cancel job",
    "job.diagnose": "Diagnose job",
    "job.list": "List jobs",
    "meta.describe": "Describe table",
    "meta.search": "Search tables",
    "meta.search-columns": "Search columns",
    "meta.list-tables": "List tables",
    "meta.list-projects": "List projects",
    "meta.list-schemas": "List schemas",
    "meta.partitions": "List partitions",
    "meta.latest-partition": "Latest partition",
    "meta.freshness": "Check freshness",
    "meta.semantic.get": "Get semantic metadata",
    "meta.semantic.set": "Set semantic metadata",
    "meta.semantic.list-missing": "List missing semantics",
    "meta.semantic.clear": "Clear semantic metadata",
    "data.sample": "Sample table data",
    "data.profile": "Profile table data",
    "auth.login": "Login",
    "auth.login-external": "Login (external)",
    "auth.logout": "Remove saved credentials",
    "auth.whoami": "Show identity",
    "auth.can-i": "Check permissions",
    "cache.build": "Build cache",
    "cache.build-status": "Cache build status",
    "cache.status": "Cache status",
    "cache.clear": "Clear cache",
    "agent.context": "Agent context",
    "agent.doctor": "Diagnose CLI connectivity",
    "agent.skill": "Agent skill info",
    "agent.skill.list": "List installed skills",
    "agent.skill.install": "Install skill",
    "agent.skill.update": "Update skill",
    "agent.skill.uninstall": "Uninstall skill",
    "agent.skill.diff": "Show skill differences",
    "agent.skill.path": "Show skill path",
    "session.set": "Set session",
    "session.show": "Show session",
    "session.unset": "Clear session",
    "project.use": "Switch project",
    "project.info": "Project info",
}

_ACTION_EFFECTS: 'dict[str, str]' = {
    "query": "remote_compute",
    "query.cost": "remote_compute",
    "query.explain": "remote_compute",
    "job.submit": "remote_compute",
    "job.cancel": "remote_write",
    "data.upload": "remote_write",
    "data.download": "local_write",
    "cache.build": "local_write",
    "cache.clear": "local_write",
    "meta.semantic.set": "local_write",
    "meta.semantic.clear": "local_write",
    "session.set": "local_write",
    "session.unset": "local_write",
    "auth.login": "local_write",
    "auth.login-external": "local_write",
    "auth.logout": "local_write",
    "agent.skill.install": "local_write",
    "agent.skill.update": "local_write",
    "agent.skill.uninstall": "local_write",
}


def action(
    action_id: 'str',
    *,
    title: 'str' = "",
    data: 'dict[str, Any] | None' = None,
    metadata: 'dict[str, Any] | None' = None,
    effect: 'str | None' = None,
    confirmation_required: 'bool' = False,
    agent_allowed: 'bool' = True,
) -> 'SuggestedAction':
    if not title:
        title = _ACTION_TITLES.get(action_id, action_id.replace(".", " ").title())
    command = _format_next_action(
        action_id,
        data=data or {},
        metadata=metadata or {},
    )
    placeholders: dict[str, str] = {}
    for match in _PLACEHOLDER_RE.finditer(command):
        placeholders[match.group(1)] = match.group(0)
    executable = (
        len(placeholders) == 0
        and agent_allowed
        and not confirmation_required
    )
    return SuggestedAction(
        id=action_id,
        title=title,
        command=command,
        executable=executable,
        placeholders=placeholders,
        effect=effect or _ACTION_EFFECTS.get(action_id, "read"),
        confirmation_required=confirmation_required,
        agent_allowed=agent_allowed,
    )


def _format_next_action(
    action: 'str',
    *,
    data: 'dict[str, Any]',
    metadata: 'dict[str, Any]',
) -> 'str':
    # Strip "maxc " prefix for template matching
    if action.startswith("maxc "):
        action = action[len("maxc "):]

    # If action already has spaces (e.g. "query explain"), convert to dot-notation
    # for template matching
    if " " in action:
        action = action.replace(" ", ".")

    sql = _suggested_sql(data, metadata)
    next_cursor = _string_value(data.get("next_cursor"))
    job_id = _string_value(data.get("job_id")) or _string_value(metadata.get("job_id"))
    build_id = _string_value(data.get("build_id")) or _string_value(metadata.get("build_id"))
    table_name = (
        _string_value(data.get("qualified_name"))
        or _string_value(data.get("table_name"))
        or _string_value(data.get("table"))
        or _first_table_name(data.get("tables"))
        or _first_table_name(data.get("matches"))
        or _single_list_value(metadata.get("tables_used"))
    )
    applied_partition = (
        _string_value(data.get("applied_partition"))
        or _string_value(data.get("latest_partition"))
    )
    keyword = _string_value(data.get("keyword"))
    operation = _string_value(data.get("operation")) or "SELECT"
    project = _string_value(metadata.get("project")) or _string_value(data.get("project"))
    project_name = _string_value(data.get("name")) or project
    schema_name = (
        _string_value(metadata.get("schema"))
        or _string_value(data.get("schema_name"))
        or _string_value(data.get("schema"))
    )

    if action in {"query.paginate", "query.next_page"}:
        parts = [
            "query",
            _shell_arg(sql, "<sql>"),
            "--cursor",
            _shell_arg(next_cursor, "<next_cursor>"),
        ]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action in {"query", "query.cost", "query.explain"}:
        parts = ["query"]
        if action != "query":
            parts.append(action.split(".", 1)[1])
        parts.append(_shell_arg(sql, "<sql>"))
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "job.submit":
        parts = ["job", "submit", _shell_arg(sql, "<sql>")]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action in {"job.status", "job.wait", "job.result", "job.cancel", "job.diagnose"}:
        parts = [
            "job",
            action.split(".", 1)[1],
            _shell_arg(job_id, "<job_id>"),
        ]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if action == "job.result":
            max_rows = data.get("max_rows")
            if isinstance(max_rows, int) and not isinstance(max_rows, bool) and max_rows > 0:
                parts.extend(["--max-rows", str(max_rows)])
            output_path = _string_value(metadata.get("output_path"))
            output_format = _string_value(metadata.get("output_format"))
            if output_path:
                parts.extend(["--output", _shell_arg(output_path, "<output_path>")])
                if output_format:
                    parts.extend(["--output-format", _shell_arg(output_format, "<output_format>")])
                if metadata.get("output_overwrite") is True:
                    parts.append("--overwrite")
        parts.append("--json")
        return _cli_command(*parts)
    if action == "job.list":
        parts = ["job", "list"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "auth.can-i":
        parts = [
            "auth",
            "can-i",
            "--table",
            _shell_arg(table_name, "<table_name>"),
            "--operation",
            _shell_arg(operation, "SELECT"),
        ]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "auth.whoami":
        return _cli_command("auth", "whoami", "--json")
    if action == "auth.login":
        return _cli_command("auth", "login", "--oauth", "--json")
    if action == "auth.login-external":
        parts = [
            "auth",
            "login-external",
            "--process-command",
            _shell_arg(None, "<credential_helper>"),
        ]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        endpoint = _string_value(data.get("endpoint"))
        if endpoint:
            parts.extend(["--endpoint", _shell_arg(endpoint, "<endpoint>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "auth.logout":
        return _cli_command("auth", "logout", "--json")
    if action == "meta.list-tables":
        parts = ["meta", "list-tables"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action in {
        "meta.describe",
        "meta.partitions",
        "meta.latest-partition",
        "meta.freshness",
        "data.sample",
        "data.profile",
    }:
        parts = [
            action.split(".", 1)[0],
            action.split(".", 1)[1],
            _shell_arg(table_name, "<table_name>"),
        ]
        if action in {"data.sample", "data.profile"} and applied_partition:
            parts.extend(["--partition", _shell_arg(applied_partition, "<partition_spec>")])
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action in {"meta.search", "meta.search-columns"}:
        parts = [
            action.split(".", 1)[0],
            action.split(".", 1)[1],
            _shell_arg(keyword, "<keyword>"),
        ]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "meta.list-projects":
        return _cli_command("meta", "list-projects", "--json")
    if action == "meta.list-schemas":
        parts = ["meta", "list-schemas"]
        if project or data.get("projects") is not None:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "data.upload":
        file_path = _string_value(metadata.get("file_path"))
        parts = [
            "data",
            "upload",
            _shell_arg(table_name, "<table_name>"),
            "--file",
            _shell_arg(file_path, "<file_path>"),
        ]
        if applied_partition:
            parts.extend(["--partition", _shell_arg(applied_partition, "<partition_spec>")])
        if metadata.get("create_partition") is True:
            parts.append("--create-partition")
        if metadata.get("overwrite") is True:
            parts.append("--overwrite")
        if metadata.get("has_header") is False:
            parts.append("--no-header")
        delimiter = metadata.get("delimiter")
        if isinstance(delimiter, str) and delimiter != ",":
            parts.extend(["--delimiter", _shell_arg(delimiter, "<delimiter>")])
        null_marker = metadata.get("null_marker")
        if isinstance(null_marker, str) and null_marker != r"\N":
            parts.extend(["--null-marker", _shell_arg(null_marker, "<null_marker>")])
        block_size = metadata.get("block_size")
        if isinstance(block_size, int) and block_size != 10000:
            parts.extend(["--block-size", str(block_size)])
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "data.download":
        output_path = _string_value(metadata.get("output_path"))
        parts = [
            "data",
            "download",
            _shell_arg(table_name, "<table_name>"),
            "--output",
            _shell_arg(output_path, "<output_path>"),
        ]
        if applied_partition:
            parts.extend(["--partition", _shell_arg(applied_partition, "<partition_spec>")])
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "session.set":
        parts = ["session", "set"]
        if project_name:
            parts.extend(["--project", _shell_arg(project_name, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        if not project_name and not schema_name:
            parts.extend(["--project", "<project>"])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "cache.build":
        parts = ["cache", "build"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "cache.build-status":
        parts = ["cache", "build-status"]
        if build_id:
            parts.extend(["--build-id", _shell_arg(build_id, "<build_id>")])
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "cache.status":
        parts = ["cache", "status"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "cache.clear":
        parts = ["cache", "clear"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--force")
        parts.append("--json")
        return _cli_command(*parts)
    if action in {"meta.semantic.get", "meta.semantic.set", "meta.semantic.clear"}:
        parts = [
            "meta", "semantic", action.rsplit(".", 1)[1],
            _shell_arg(table_name, "<table_name>"),
        ]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        if action == "meta.semantic.set":
            semantic_desc = _string_value(data.get("semantic_desc"))
            parts.extend(
                [
                    "--desc",
                    _shell_arg(
                        semantic_desc,
                        shlex.quote("<semantic_description>"),
                    ),
                ]
            )
        parts.append("--json")
        return _cli_command(*parts)
    if action == "meta.semantic.list-missing":
        parts = ["meta", "semantic", "list-missing"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "agent.context":
        return _cli_command("agent", "context", "--json")
    if action == "agent.doctor":
        parts = ["agent", "doctor"]
        if data.get("online") is True:
            parts.append("--online")
        parts.append("--json")
        return _cli_command(*parts)
    if action == "agent.skill":
        return _cli_command("agent", "skill", "--json")
    if action == "agent.skill.list":
        return _cli_command("agent", "skill", "list", "--json")
    if action == "agent.skill.install":
        return _cli_command("agent", "skill", "install", "--json")

    if "." not in action:
        return _cli_command(action, "--json")
    group, subcommand = action.split(".", 1)
    return _cli_command(group, subcommand, "--json")


def _suggested_sql(data: 'dict[str, Any]', metadata: 'dict[str, Any]') -> 'str | None':
    sql = _string_value(metadata.get("sql_executed"))
    if sql:
        return sql

    partitioned = (
        data.get("has_partitions") is True
        or data.get("is_partitioned") is True
        or bool(data.get("partition_columns"))
        or bool(data.get("applied_partition"))
        or bool(data.get("latest_partition"))
    )
    if partitioned:
        return None

    table_name = (
        _string_value(data.get("qualified_name"))
        or _string_value(data.get("table_name"))
        or _first_table_name(data.get("tables"))
    )
    if table_name:
        return f"SELECT * FROM {table_name} LIMIT 20"
    return None


def _string_value(value: 'Any') -> 'str | None':
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        return None
    text = str(value).strip()
    return text or None


def _single_list_value(value: 'Any') -> 'str | None':
    if not isinstance(value, list) or len(value) != 1:
        return None
    return _string_value(value[0])


def _first_table_name(value: 'Any') -> 'str | None':
    # Collection actions are executable only when discovery resolved exactly
    # one object. Picking the first of several matches silently turns an
    # ambiguous search/list result into an arbitrary data operation.
    if not isinstance(value, list) or len(value) != 1:
        return None
    first = value[0]
    if not isinstance(first, dict):
        return _string_value(first)
    return _string_value(first.get("qualified_name")) or _string_value(first.get("table_name"))


def _shell_arg(value: 'str | None', placeholder: 'str') -> 'str':
    if value is None:
        return placeholder
    return shlex.quote(value)


def _cli_command(*parts: 'str', prefix: 'str | None' = None) -> 'str':
    if prefix is not None:
        command_prefix = prefix
    else:
        command_prefix = f"{current_cli_entry_point()} "
        from .odps_runtime import current_agent_user_agent

        user_agent = current_agent_user_agent()
        if user_agent:
            command_prefix += f"--user-agent {shlex.quote(user_agent)} "
    return command_prefix + " ".join(part for part in parts if part)


def build_safety_block(
    force: 'bool' = False,
    sql: 'str | None' = None,
) -> 'dict[str, Any]':
    """Build a safety block describing the read-only policy state."""
    from .utils import executable_operations, known_write_operations, statement_operations

    if sql:
        operations = executable_operations(sql) or statement_operations(sql)
    else:
        operations = ["SELECT"]
    write_operations = known_write_operations(sql) if sql else []
    operations = list(dict.fromkeys(operations or ["SELECT"]))
    is_write = bool(write_operations)

    if force:
        return {
            "mode": "force",
            "force": True,
            "allowed_operations": operations,
            "effective_hints": {},
            "policy_decision": "allowed",
        }
    if is_write:
        return {
            "mode": "read_only",
            "force": False,
            "policy_decision": "blocked",
            "reason": "WRITE_OPERATION_REQUIRES_FORCE",
        }
    return {
        "mode": "read_only",
        "force": False,
        "allowed_operations": operations,
        "effective_hints": {},
        "policy_decision": "allowed",
    }


def build_observational_safety_block(command: 'str') -> 'dict[str, Any]':
    """Describe a non-mutating follow-up without reclassifying the original SQL."""
    operation = command.replace(".", "_").upper()
    return {
        "mode": "read_only",
        "force": False,
        "allowed_operations": [operation],
        "effective_hints": {},
        "policy_decision": "allowed",
        "scope": "result_observation",
    }
