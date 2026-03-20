from __future__ import annotations

import json
from typing import Any, TextIO


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
    border = "+" + "+".join("-" * (widths[column] + 2) for column in columns) + "+"
    header = "| " + " | ".join(str(column).ljust(widths[column]) for column in columns) + " |"
    lines = [border, header, border]
    for row in rows:
        line = "| " + " | ".join(
            _stringify(row.get(column, "")).ljust(widths[column]) for column in columns
        ) + " |"
        lines.append(line)
    lines.append(border)
    return "\n".join(lines)


def render_key_values(mapping: dict[str, Any]) -> str:
    if not mapping:
        return ""
    width = max(len(str(key)) for key in mapping)
    return "\n".join(
        f"{str(key).ljust(width)} : {_stringify(value)}" for key, value in mapping.items()
    )


def _stringify(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)
