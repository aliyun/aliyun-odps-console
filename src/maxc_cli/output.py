from __future__ import annotations

import json
from typing import Any, TextIO, TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Envelope


def emit_json(payload: 'dict[str, Any]', stdout: 'TextIO') -> 'None':
    stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def emit_ndjson(events: 'list[dict[str, Any]]', stdout: 'TextIO') -> 'None':
    for event in events:
        stdout.write(json.dumps(event, ensure_ascii=False) + "\n")


def render_table(rows: 'list[dict[str, Any]]') -> 'str':
    if not rows:
        return "(no rows)"

    columns: 'list[str]' = []
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
        line = "| " + " | ".join(
            _escape_md_cell(_stringify(row.get(column, ""))).ljust(widths[column]) for column in columns
        ) + " |"
        lines.append(line)
    return "\n".join(lines)


def render_key_values(mapping: 'dict[str, Any]') -> 'str':
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


def render_error(code: 'str', message: 'str', suggestion: 'str | None' = None) -> 'str':
    parts = [f"**Error** [`{code}`]: {message}"]
    if suggestion:
        parts.append("")
        parts.append(f"> **Suggestion**: {suggestion}")
    return "\n".join(parts)


def _escape_md_cell(text: 'str') -> 'str':
    return text.replace("|", "\\|")


def _stringify(value: 'Any') -> 'str':
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


# ---------------------------------------------------------------------------
# render_markdown / render_brief
# ---------------------------------------------------------------------------


def render_markdown(envelope: Envelope) -> str:
    """Render an Envelope as human-readable markdown."""
    parts: list[str] = []

    # --- Error envelopes ------------------------------------------------
    if envelope.error is not None:
        err = envelope.error
        parts.append(f"## Error [{err.code}]")
        parts.append("")
        parts.append(err.message)
        if err.suggestion:
            parts.append("")
            parts.append(f"> **Suggestion**: {err.suggestion}")
        if getattr(err, "recovery_steps", None):
            parts.append("")
            parts.append("**Recovery steps**:")
            for step in err.recovery_steps:
                parts.append(f"- {step}")
        parts.append("")
        return _append_agent_hints_md(parts, envelope)

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
        parts.append(f"## Search: \"{keyword}\" ({total} match{'es' if total != 1 else ''})")
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
    hints = envelope.agent_hints
    if hints is not None and hints.actions:
        parts.append("### Next Actions")
        parts.append("")
        for act in hints.actions:
            parts.append(f"- **{act.title}**: `{act.command}`")
        parts.append("")
    return "\n".join(parts)


def render_brief(envelope: Envelope) -> str:
    """Render a minimal one-line summary of an Envelope."""
    command = envelope.command.replace(".", " ")

    # Determine first suggested action command
    next_cmd = ""
    hints = envelope.agent_hints
    if hints is not None and hints.actions:
        next_cmd = hints.actions[0].command

    # --- Error envelopes ------------------------------------------------
    if envelope.error is not None:
        err = envelope.error
        suggestion = err.suggestion or err.message
        line = f"{command} | ERROR [{err.code}] | {suggestion}"
        return line

    data = envelope.data
    metadata = envelope.metadata or {}

    # --- query ----------------------------------------------------------
    if envelope.command in {"query", "job.wait", "job.result"}:
        total = data.get("total_rows", "?")
        line = f"{command} | success | {total} rows"
        if next_cmd:
            line += f" | next: {next_cmd}"
        return line

    # --- meta.describe --------------------------------------------------
    if envelope.command == "meta.describe":
        table_name = data.get("table_name", "?")
        col_count = len(data.get("columns", []))
        line = f"{command} | success | {table_name} ({col_count} columns)"
        if next_cmd:
            line += f" | next: {next_cmd}"
        return line

    # --- meta.search / meta.search-columns ------------------------------
    if envelope.command in {"meta.search", "meta.search-columns"}:
        total = data.get("total", 0)
        line = f"{command} | success | {total} matches"
        if next_cmd:
            line += f" | next: {next_cmd}"
        return line

    # --- job.* ----------------------------------------------------------
    if envelope.command.startswith("job."):
        job_id = data.get("job_id", "?")
        job_status = data.get("status", "?")
        line = f"{command} | {envelope.status} | {job_id} {job_status}"
        if next_cmd:
            line += f" | next: {next_cmd}"
        return line

    # --- Fallback -------------------------------------------------------
    line = f"{command} | {envelope.status}"
    if next_cmd:
        line += f" | next: {next_cmd}"
    return line
