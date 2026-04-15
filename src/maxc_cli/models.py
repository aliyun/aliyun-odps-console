"""Data models for MaxCompute CLI."""


from dataclasses import dataclass, field
import shlex
from typing import Any


@dataclass
class AgentHints:
    next_actions: 'list[str]' = field(default_factory=list)
    warnings: 'list[str]' = field(default_factory=list)
    insights: 'list[str]' = field(default_factory=list)

    def to_dict(self) -> 'dict[str, Any]':
        payload: 'dict[str, Any]' = {}
        if self.next_actions:
            payload["next_actions"] = self.next_actions
        if self.warnings:
            payload["warnings"] = self.warnings
        if self.insights:
            payload["insights"] = self.insights
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
            "command_id": self.command,
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
        if set(data) == {"job_id"}:
            return {"job": {"job_id": data["job_id"]}}
        return {
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
    if command in {"query.cost", "query.explain"}:
        return {"analysis": data}
    if command == "auth.whoami":
        options = data.get("auth_options")
        identity = {key: value for key, value in data.items() if key != "auth_options"}
        payload: 'dict[str, Any]' = {"identity": identity}
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
    if command == "auth.login-ncs":
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
        return {
            "tables": data.get("tables", []),
            "pagination": {
                "total": data.get("total"),
                "has_more": False,
            },
        }
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
        return {
            "search": {
                "keyword": data.get("keyword"),
                "matches": data.get("matches", []),
            },
            "pagination": {
                "total": data.get("total"),
                "has_more": False,
            },
        }
    if command == "meta.describe":
        return {"table": data}
    if command == "meta.partitions":
        return {
            "table": {"table_name": data.get("table_name")},
            "partitions": data.get("partitions", []),
        }
    if command in {"meta.latest-partition", "meta.freshness", "meta.lineage"}:
        key = {
            "meta.latest-partition": "partition",
            "meta.freshness": "freshness",
            "meta.lineage": "lineage",
        }[command]
        return {key: data}
    if command == "data.sample":
        return {
            "sample": {
                "table_name": data.get("table_name"),
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
    if command.startswith("diff."):
        return {"diff": data}
    if command == "job.list":
        return {
            "jobs": data.get("jobs", []),
            "pagination": {
                "total": data.get("total"),
                "has_more": False,
            },
        }
    if command in {"job.status", "job.cancel"}:
        return {"job": data}
    if command == "job.diagnose":
        return {"diagnosis": data}
    if command in {"cache.get-semantic", "cache.save-semantic"}:
        return {"semantic": data}
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
    if command == "auth.login-ncs":
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
    if command == "meta.lineage":
        return _has_mapping(data, "lineage")
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
    if command in {"cache.get-semantic", "cache.save-semantic"}:
        return _has_mapping(data, "semantic")
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
    if envelope.agent_hints.next_actions:
        # Generate action_ids in dot-notation for programmatic use
        # e.g. "maxc query explain" → "query.explain", "maxc meta describe" → "meta.describe"
        payload["action_ids"] = [
            _to_action_id(action) for action in envelope.agent_hints.next_actions
        ]
        # Generate next_actions as fully-templated CLI commands
        payload["next_actions"] = [
            _format_next_action(
                action,
                data=envelope.data,
                metadata=envelope.metadata,
            )
            for action in envelope.agent_hints.next_actions
        ]
    return payload


def _to_action_id(action: 'str') -> 'str':
    """Convert a next_actions entry to dot-notation action_id.

    "maxc meta describe" → "meta.describe"
    "maxc job wait" → "job.wait"
    "meta.describe" → "meta.describe"  (already dot-notation)
    """
    if action.startswith("maxc "):
        parts = action[len("maxc "):].split()
        return ".".join(parts)
    return action


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
    table_name = _string_value(data.get("table_name")) or _single_list_value(metadata.get("tables_used"))
    keyword = _string_value(data.get("keyword"))
    operation = _string_value(data.get("operation")) or "SELECT"
    project = _string_value(metadata.get("project")) or _string_value(data.get("project"))
    project_name = _string_value(data.get("name")) or project
    schema_name = _string_value(data.get("schema_name"))
    left_table = _string_value(data.get("left_table"))
    right_table = _string_value(data.get("right_table"))

    if action in {"query.paginate", "query.next_page"}:
        return _cli_command(
            "query",
            _shell_arg(sql, "<sql>"),
            "--cursor",
            _shell_arg(next_cursor, "<next_cursor>"),
            "--json",
        )
    if action in {"query", "query.cost", "query.explain"}:
        parts = ["query"]
        if action != "query":
            parts.append(action.split(".", 1)[1])
        parts.extend([_shell_arg(sql, "<sql>"), "--json"])
        return _cli_command(*parts)
    if action == "job.submit":
        return _cli_command("job", "submit", _shell_arg(sql, "<sql>"), "--json")
    if action in {"job.status", "job.wait", "job.result", "job.cancel", "job.diagnose"}:
        return _cli_command(
            "job",
            action.split(".", 1)[1],
            _shell_arg(job_id, "<job_id>"),
            "--json",
        )
    if action == "job.list":
        return _cli_command("job", "list", "--json")
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
    if action == "meta.list-tables":
        return _cli_command("meta", "list-tables", "--json")
    if action in {
        "meta.describe",
        "meta.partitions",
        "meta.latest-partition",
        "meta.freshness",
        "meta.lineage",
        "data.sample",
        "data.profile",
    }:
        return _cli_command(
            action.split(".", 1)[0],
            action.split(".", 1)[1],
            _shell_arg(table_name, "<table_name>"),
            "--json",
        )
    if action in {"meta.search", "meta.search-columns"}:
        return _cli_command(
            action.split(".", 1)[0],
            action.split(".", 1)[1],
            _shell_arg(keyword, "<keyword>"),
            "--json",
        )
    if action == "meta.list-projects":
        return _cli_command("meta", "list-projects", "--json")
    if action == "meta.list-schemas":
        parts = ["meta", "list-schemas"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "project.use":
        parts = ["project", "use", _shell_arg(project_name, "<project_name>")]
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "project.info":
        parts = ["project", "info"]
        if project_name:
            parts.append(_shell_arg(project_name, "<project_name>"))
        parts.append("--json")
        return _cli_command(*parts)
    if action in {"diff.schema", "diff.partition"}:
        return _cli_command(
            action.split(".", 1)[0],
            action.split(".", 1)[1],
            _shell_arg(left_table, "<left_table>"),
            _shell_arg(right_table, "<right_table>"),
            "--json",
        )
    if action == "diff.data":
        return _cli_command(
            "diff",
            "data",
            _shell_arg(left_table, "<left_table>"),
            _shell_arg(right_table, "<right_table>"),
            "--keys",
            "<key_columns>",
            "--json",
        )
    if action == "cache.build":
        parts = ["cache", "build"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
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
        parts.append("--json")
        return _cli_command(*parts)
    if action == "cache.clear":
        parts = ["cache", "clear"]
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "cache.get-semantic":
        parts = [
            "cache",
            "get-semantic",
            "--table",
            _shell_arg(table_name, "<table_name>"),
        ]
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "cache.save-semantic":
        parts = [
            "cache",
            "save-semantic",
            "--table",
            _shell_arg(table_name, "<table_name>"),
            "--semantic-desc",
            "<semantic_desc>",
        ]
        if schema_name:
            parts.extend(["--schema", _shell_arg(schema_name, "<schema_name>")])
        if project:
            parts.extend(["--project", _shell_arg(project, "<project>")])
        parts.append("--json")
        return _cli_command(*parts)
    if action == "agent.context":
        return _cli_command("agent", "context", "--json")

    if "." not in action:
        return _cli_command(action, "--json")
    group, subcommand = action.split(".", 1)
    return _cli_command(group, subcommand, "--json")


def _suggested_sql(data: 'dict[str, Any]', metadata: 'dict[str, Any]') -> 'str | None':
    sql = _string_value(metadata.get("sql_executed"))
    if sql:
        return sql

    table_name = _string_value(data.get("table_name"))
    if table_name:
        return f"SELECT * FROM {table_name} LIMIT 20"
    return None


def _string_value(value: 'Any') -> 'str | None':
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _single_list_value(value: 'Any') -> 'str | None':
    if not isinstance(value, list) or len(value) != 1:
        return None
    return _string_value(value[0])


def _shell_arg(value: 'str | None', placeholder: 'str') -> 'str':
    if value is None:
        return placeholder
    return shlex.quote(value)


def _cli_command(*parts: 'str', prefix: 'str' = "maxc ") -> 'str':
    return prefix + " ".join(part for part in parts if part)
