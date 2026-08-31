from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, TextIO

from .models import suggested_action_is_safe
from .utils import distribution_cli_text

if TYPE_CHECKING:
    from .models import Envelope, SuggestedAction


def emit_json(payload: dict[str, Any], stdout: TextIO) -> None:
    stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def emit_ndjson(events: list[dict[str, Any]], stdout: TextIO) -> None:
    for event in events:
        stdout.write(json.dumps(event, ensure_ascii=False) + "\n")


def render_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "(no rows)"

    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)

    widths = {
        column: max(
            len(str(column)),
            max(len(_stringify(row.get(column, ""))) for row in rows),
        )
        for column in columns
    }
    header = "| " + " | ".join(str(column).ljust(widths[column]) for column in columns) + " |"
    separator = "|" + "|".join("-" * (widths[column] + 2) for column in columns) + "|"
    lines = [header, separator]
    for row in rows:
        line = (
            "| "
            + " | ".join(
                _escape_md_cell(_stringify(row.get(column, ""))).ljust(widths[column])
                for column in columns
            )
            + " |"
        )
        lines.append(line)
    return "\n".join(lines)


def render_key_values(mapping: dict[str, Any]) -> str:
    if not mapping:
        return ""
    key_width = max(max(len(str(k)) for k in mapping), 3)
    val_width = max(max(len(_stringify(v)) for v in mapping.values()), 5)
    header = f"| {'Key'.ljust(key_width)} | {'Value'.ljust(val_width)} |"
    separator = f"|{'-' * (key_width + 2)}|{'-' * (val_width + 2)}|"
    lines = [header, separator]
    for key, value in mapping.items():
        lines.append(
            f"| {str(key).ljust(key_width)} | {_escape_md_cell(_stringify(value)).ljust(val_width)} |"
        )
    return "\n".join(lines)


def render_error(code: str, message: str, suggestion: str | None = None) -> str:
    parts = [f"**Error** [`{code}`]: {distribution_cli_text(message)}"]
    if suggestion:
        parts.append("")
        parts.append(f"> **Suggestion**: {distribution_cli_text(suggestion)}")
    return "\n".join(parts)


def _escape_md_cell(text: str) -> str:
    return text.replace("|", "\\|")


def _stringify(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


# ---------------------------------------------------------------------------
# render_markdown / render_brief
# ---------------------------------------------------------------------------


def _safe_suggested_actions(envelope: Envelope) -> list[SuggestedAction]:
    """Return only actions that are safe to present as immediately copyable."""
    hints = envelope.agent_hints
    if hints is None:
        return []
    return [
        action
        for action in hints.actions
        if suggested_action_is_safe(action)
    ]


def _rendered_action_command(action: SuggestedAction) -> str:
    """Return the command after distribution and User-Agent normalization."""
    return str(action.to_dict()["command"])


def _user_agent_action_templates(envelope: Envelope) -> list[dict[str, Any]]:
    """Return actions made non-executable only by a missing session User-Agent."""
    hinted = envelope.agent_hints.actions if envelope.agent_hints else []
    templates: list[dict[str, Any]] = []
    for candidate in hinted:
        rendered = candidate.to_dict()
        if (
            candidate.executable
            and candidate.agent_allowed
            and not candidate.confirmation_required
            and not rendered["executable"]
            and "user_agent" in rendered["placeholders"]
        ):
            templates.append(rendered)
    return templates


def _pending_continuation_templates(
    envelope: Envelope,
    job_id: Any,
) -> list[dict[str, Any]]:
    """Return agent-allowed pending templates without inventing an unsafe command."""
    hinted = envelope.agent_hints.actions if envelope.agent_hints else []
    rendered = [
        candidate.to_dict()
        for candidate in hinted
        if candidate.agent_allowed and not candidate.confirmation_required
    ]
    if rendered:
        return rendered

    from .models import action

    data = {"job_id": str(job_id)} if job_id else {}
    return [
        action("job.wait", data=data).to_dict(),
        action("job.status", data=data).to_dict(),
    ]


def _render_pending_md(envelope: Envelope) -> str:
    """Render a pending/async envelope so the user sees status and how to wait."""
    parts: list[str] = ["## Pending", ""]
    parts.append("The query has not finished yet — it is still running asynchronously.")
    parts.append("")

    data = envelope.data or {}
    metadata = envelope.metadata or {}
    kv: dict[str, Any] = {}
    job_id = data.get("job_id") or metadata.get("job_id")
    if job_id:
        kv["Job ID"] = job_id
    if metadata.get("project"):
        kv["Project"] = metadata["project"]
    if metadata.get("wait_seconds") is not None:
        kv["Waited"] = f"{metadata['wait_seconds']}s"
    if metadata.get("logview"):
        from .utils import sanitize_logview_url

        kv["Logview"] = sanitize_logview_url(metadata["logview"])
    if kv:
        parts.append(render_key_values(kv))
        parts.append("")

    parts.append("Wait for it to complete with:")
    parts.append("")
    if not _safe_suggested_actions(envelope):
        templates = _pending_continuation_templates(envelope, job_id)
        if any(not item["executable"] for item in templates):
            parts.append(
                "Fill every placeholder (including the current session User-Agent) "
                "before running a continuation:"
            )
            parts.append("")
        for item in templates:
            parts.append(f"- `{item['command']}`")
        parts.append("")
        return "\n".join(parts)
    return _append_agent_hints_md(parts, envelope)


def _pending_job_id(envelope: Envelope) -> Any:
    data = envelope.data if isinstance(envelope.data, dict) else {}
    metadata = envelope.metadata if isinstance(envelope.metadata, dict) else {}
    return data.get("job_id") or metadata.get("job_id")


def _is_async_job_pending(envelope: Envelope) -> bool:
    return bool(
        _pending_job_id(envelope)
        and (envelope.command == "query" or envelope.command.startswith("job."))
    )


def _render_generic_pending_md(envelope: Envelope) -> str:
    """Render non-job continuations without inventing a query or job ID."""
    data = envelope.data if isinstance(envelope.data, dict) else {}
    parts = ["## Pending", ""]
    parts.append(
        f"`{envelope.command.replace('.', ' ')}` requires additional input or user action."
    )
    reason = data.get("reason")
    if reason:
        parts.extend(["", f"Reason: **{_stringify(reason)}**"])

    scalar_values = {
        str(key): value
        for key, value in data.items()
        if key not in {"reason", "projects"}
        and not isinstance(value, (dict, list, tuple))
    }
    if scalar_values:
        parts.extend(["", render_key_values(scalar_values)])

    projects = data.get("projects")
    if isinstance(projects, list) and all(
        isinstance(project, dict) for project in projects
    ):
        parts.extend(["", "### Available Projects", "", render_table(projects)])

    safe_actions = _safe_suggested_actions(envelope)
    if not safe_actions and envelope.agent_hints and envelope.agent_hints.actions:
        parts.extend(["", "### User Action Required", ""])
        for candidate in envelope.agent_hints.actions:
            parts.append(f"- {candidate.title} (explicit user selection required)")
        parts.append("")
    return _append_agent_hints_md(parts, envelope)


def render_markdown(envelope: Envelope) -> str:
    """Render an Envelope as human-readable markdown."""
    parts: list[str] = []

    # --- Error envelopes ------------------------------------------------
    if envelope.error is not None:
        err = envelope.error
        parts.append(f"## Error [{err.code}]")
        parts.append("")
        parts.append(distribution_cli_text(err.message))
        if err.suggestion:
            parts.append("")
            parts.append(
                f"> **Suggestion**: {distribution_cli_text(err.suggestion)}"
            )
        parts.append("")
        return _append_agent_hints_md(parts, envelope)

    # --- Pending / async envelopes --------------------------------------
    if envelope.status == "pending":
        if _is_async_job_pending(envelope):
            return _render_pending_md(envelope)
        return _render_generic_pending_md(envelope)

    command = envelope.command
    data = envelope.data
    metadata = envelope.metadata or {}

    # --- query ----------------------------------------------------------
    if command in {"query", "job.wait", "job.result"}:
        parts.append("## Query Result")
        parts.append("")
        meta_items: list[str] = []
        if metadata.get("project"):
            meta_items.append(f"Project: **{metadata['project']}**")
        if metadata.get("elapsed_ms") is not None:
            meta_items.append(f"Elapsed: **{metadata['elapsed_ms']}ms**")
        total = data.get("total_rows")
        if total is not None:
            meta_items.append(f"Total rows: **{total}**")
        if data.get("has_more"):
            meta_items.append("*(has more rows)*")
        if meta_items:
            parts.append(" | ".join(meta_items))
            parts.append("")
        rows = data.get("rows")
        if rows:
            parts.append(render_table(rows))
        elif total == 0 or not rows:
            parts.append("(no rows)")
        parts.append("")

    # --- meta.describe --------------------------------------------------
    elif command == "meta.describe":
        table_name = data.get("table_name", "unknown")
        parts.append(f"## Table: {table_name}")
        parts.append("")
        if data.get("description"):
            parts.append(f"_{data['description']}_")
            parts.append("")
        columns = data.get("columns")
        if columns:
            parts.append("### Columns")
            parts.append("")
            parts.append(render_table(columns))
            parts.append("")
        partitions = data.get("partitions")
        if partitions:
            parts.append("### Partitions")
            parts.append("")
            parts.append(render_table(partitions))
            parts.append("")

    # --- meta.search / meta.search-columns ------------------------------
    elif command in {"meta.search", "meta.search-columns"}:
        keyword = data.get("keyword", "")
        total = data.get("total", 0)
        parts.append(f'## Search: "{keyword}" ({total} match{"es" if total != 1 else ""})')
        parts.append("")
        matches = data.get("matches")
        if matches:
            parts.append(render_table(matches))
        else:
            parts.append("(no matches)")
        parts.append("")

    # --- meta.list-tables -----------------------------------------------
    elif command == "meta.list-tables":
        tables = data.get("tables", [])
        parts.append(f"## Tables ({len(tables)})")
        parts.append("")
        if tables:
            parts.append(render_table(tables))
        else:
            parts.append("(no tables)")
        parts.append("")

    # --- job.* ----------------------------------------------------------
    elif command.startswith("job."):
        parts.append("## Job Info")
        parts.append("")
        kv = {k: v for k, v in data.items() if v is not None}
        if kv:
            parts.append(render_key_values(kv))
        parts.append("")

    # --- data.sample ----------------------------------------------------
    elif command == "data.sample":
        table_name = data.get("table_name", "unknown")
        parts.append(f"## Sample: {table_name}")
        parts.append("")
        rows = data.get("rows")
        if rows:
            parts.append(render_table(rows))
        else:
            parts.append("(no rows)")
        parts.append("")

    # --- query.cost / query.explain -------------------------------------
    elif command in {"query.cost", "query.explain"}:
        label = "Cost Estimate" if command == "query.cost" else "Query Plan"
        parts.append(f"## {label}")
        parts.append("")
        kv = {k: v for k, v in data.items() if v is not None}
        if kv:
            parts.append(render_key_values(kv))
        parts.append("")

    # --- Fallback -------------------------------------------------------
    else:
        parts.append(f"## {command}")
        parts.append("")
        kv = {k: v for k, v in data.items() if v is not None}
        if kv:
            parts.append(render_key_values(kv))
        else:
            parts.append("(no data)")
        parts.append("")

    return _append_agent_hints_md(parts, envelope)


def _append_agent_hints_md(parts: list[str], envelope: Envelope) -> str:
    """Append agent hints section and return the final markdown string."""
    safe_actions = _safe_suggested_actions(envelope)
    if safe_actions:
        parts.append("### Next Actions")
        parts.append("")
        for act in safe_actions:
            parts.append(
                f"- **{act.title}**: `{_rendered_action_command(act)}`"
            )
        parts.append("")
    templates = _user_agent_action_templates(envelope)
    if templates:
        parts.append("### Next Actions (fill required placeholders)")
        parts.append("")
        for item in templates:
            parts.append(f"- `{item['command']}`")
        parts.append("")
    return "\n".join(parts)


def render_brief(envelope: Envelope) -> str:
    """Render a minimal one-line summary of an Envelope."""
    command = envelope.command.replace(".", " ")

    # Determine first suggested action command
    next_cmd = ""
    next_label = "next"
    safe_actions = _safe_suggested_actions(envelope)
    if safe_actions:
        next_cmd = _rendered_action_command(safe_actions[0])
    else:
        templates = _user_agent_action_templates(envelope)
        if templates:
            next_cmd = str(templates[0]["command"])
            next_label = "next template"

    # --- Error envelopes ------------------------------------------------
    if envelope.error is not None:
        err = envelope.error
        suggestion = distribution_cli_text(err.suggestion or err.message)
        line = f"{command} | ERROR [{err.code}] | {suggestion}"
        return line

    data = envelope.data

    # --- pending / async ------------------------------------------------
    if envelope.status == "pending":
        if _is_async_job_pending(envelope):
            job_id = _pending_job_id(envelope)
            line = f"{command} | pending | job {job_id}"
            if next_cmd:
                line += f" | {next_label}: {next_cmd}"
            else:
                templates = _pending_continuation_templates(envelope, job_id)
                line += f" | next template: {templates[0]['command']}"
            return line
        reason = data.get("reason") or "additional input required"
        line = f"{command} | pending | {reason}"
        count = data.get("count")
        if isinstance(count, int):
            line += f" | {count} options"
        if next_cmd:
            line += f" | {next_label}: {next_cmd}"
        return line

    # --- query ----------------------------------------------------------
    if envelope.command in {"query", "job.wait", "job.result"}:
        total = data.get("total_rows", "?")
        line = f"{command} | success | {total} rows"
        rows = data.get("rows") or []
        preview_lines: list[str] = []
        for row in rows[:3]:
            if isinstance(row, dict):
                preview_lines.append(",".join(_stringify(row.get(col, "")) for col in row))
            else:
                preview_lines.append(_stringify(row))
        if next_cmd:
            line += f" | {next_label}: {next_cmd}"
        if preview_lines:
            line += "\n" + "\n".join(preview_lines)
        return line

    # --- meta.describe --------------------------------------------------
    if envelope.command == "meta.describe":
        table_name = data.get("table_name", "?")
        col_count = len(data.get("columns", []))
        line = f"{command} | success | {table_name} ({col_count} columns)"
        if next_cmd:
            line += f" | {next_label}: {next_cmd}"
        return line

    # --- meta.search / meta.search-columns ------------------------------
    if envelope.command in {"meta.search", "meta.search-columns"}:
        total = data.get("total", 0)
        line = f"{command} | success | {total} matches"
        if next_cmd:
            line += f" | {next_label}: {next_cmd}"
        return line

    # --- job.* ----------------------------------------------------------
    if envelope.command.startswith("job."):
        job_id = data.get("job_id", "?")
        job_status = data.get("status", "?")
        line = f"{command} | {envelope.status} | {job_id} {job_status}"
        if next_cmd:
            line += f" | {next_label}: {next_cmd}"
        return line

    # --- Fallback -------------------------------------------------------
    line = f"{command} | {envelope.status}"
    if next_cmd:
        line += f" | {next_label}: {next_cmd}"
    return line
