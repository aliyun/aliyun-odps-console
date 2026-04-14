
import json
from typing import Any, TextIO


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
