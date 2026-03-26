
import base64
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .exceptions import ValidationError


SQL_COMMENT_RE = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)
TABLE_NAME_RE = re.compile(
    r"(?i)\b(?:from|join|into|update|table)\s+([a-zA-Z_][\w.]*)"
)


def now_utc_iso() -> 'str':
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def deep_merge(base: 'dict[str, Any]', override: 'dict[str, Any]') -> 'dict[str, Any]':
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def resolve_path(raw_path: 'str | None', *, base_dir: 'Path') -> 'Path':
    if not raw_path:
        raise ValidationError("Configuration path cannot be empty.")
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def normalize_sql(sql: 'str') -> 'str':
    stripped = SQL_COMMENT_RE.sub(" ", sql)
    return " ".join(stripped.strip().split())


def detect_operation(sql: 'str') -> 'str':
    normalized = normalize_sql(sql)
    match = re.match(r"(?i)^([a-z]+)", normalized)
    return match.group(1).upper() if match else "UNKNOWN"


def extract_table_names(sql: 'str') -> 'list[str]':
    normalized = normalize_sql(sql)
    return list(dict.fromkeys(TABLE_NAME_RE.findall(normalized)))


def parse_select_projection(sql: 'str') -> 'list[str]':
    normalized = normalize_sql(sql)
    match = re.search(r"(?is)^select\s+(.*?)\s+from\b", normalized)
    if not match:
        match = re.search(r"(?is)^select\s+(.*)$", normalized)
    if not match:
        return []
    projection = match.group(1).strip()
    if projection == "*":
        return ["*"]
    return [part.strip() for part in projection.split(",") if part.strip()]


def projection_alias(expression: 'str', fallback_index: 'int') -> 'str':
    alias_match = re.search(r"(?i)\bas\s+([a-zA-Z_][\w]*)$", expression)
    if alias_match:
        return alias_match.group(1)
    bare = expression.split(".")[-1].strip()
    if bare == expression and "(" in expression:
        return f"_c{fallback_index}"
    return bare


def encode_cursor(offset: 'int', session_id: 'int | None' = None) -> 'str':
    """Encode cursor with short keys: s=session_id, o=offset."""
    payload: 'dict[str, int]' = {"o": offset}
    if session_id is not None:
        payload["s"] = session_id
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("utf-8")


def decode_cursor(cursor: 'str | None') -> 'tuple[int, int | None]':
    """Decode a cursor and return (offset, session_id)."""
    if not cursor:
        return 0, None
    try:
        payload = base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8")
        value = json.loads(payload)
    except Exception as exc:
        raise ValidationError(
            "The cursor could not be parsed.",
            suggestion="Use the `next_cursor` returned by the previous response.",
        ) from exc
    offset = value.get("o")
    if not isinstance(offset, int) or offset < 0:
        raise ValidationError(
            "The cursor contains an invalid offset.",
            suggestion="Use the `next_cursor` returned by the previous response.",
        )
    session_id = value.get("s")
    return offset, session_id


def read_sql_input(
    sql_parts: 'list[str]',
    *,
    file_path: 'str | None',
    use_stdin: 'bool',
    stdin_text: 'str | None',
) -> 'str':
    provided_sources = sum(bool(item) for item in [sql_parts, file_path, use_stdin])
    if provided_sources == 0:
        raise ValidationError("Provide SQL via inline text, `--file`, or `--stdin`.")
    if provided_sources > 1:
        raise ValidationError("SQL input must come from exactly one source: inline text, `--file`, or `--stdin`.")

    if sql_parts:
        return " ".join(sql_parts).strip()
    if file_path:
        return Path(file_path).read_text(encoding="utf-8").strip()
    if use_stdin:
        content = (stdin_text or "").strip()
        if not content:
            raise ValidationError("No SQL was read from stdin.")
        return content
    raise ValidationError("Unable to resolve SQL input.")


def short_json(value: 'Any') -> 'str':
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
