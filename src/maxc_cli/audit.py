
import json
import os
from hashlib import sha256
from pathlib import Path
from typing import Any

from .state_permissions import (
    close_private_directory,
    open_private_directory,
    open_private_file_at,
)
from .utils import now_utc_iso

_SENSITIVE_KEY_FRAGMENTS = (
    "access_id",
    "authorization",
    "continuation",
    "cookie",
    "credential",
    "password",
    "process_command",
    "refresh_token",
    "secret",
    "security_token",
    "token",
)
_SQL_KEYS = frozenset({"sql", "sql_executed", "statement"})


def _sanitize_value(value: 'Any', *, key: 'str' = "") -> 'Any':
    normalized_key = key.lower().replace("-", "_")
    if any(fragment in normalized_key for fragment in _SENSITIVE_KEY_FRAGMENTS):
        return "<redacted>"
    if normalized_key in _SQL_KEYS and isinstance(value, str):
        return {
            "redacted": True,
            "sha256": sha256(value.encode("utf-8", errors="replace")).hexdigest(),
        }
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {
            str(item_key): _sanitize_value(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    # Avoid repr(value): provider objects can embed credentials in reprs.
    return f"<{type(value).__name__}>"


def sanitize_audit_payload(payload: 'dict[str, Any]') -> 'dict[str, Any]':
    """Return a JSON-safe audit record without SQL literals or credentials."""
    sanitized = _sanitize_value(payload)
    if not isinstance(sanitized, dict):
        return {"payload": "<invalid>"}
    error = sanitized.get("error")
    if isinstance(error, dict):
        sanitized["error"] = {
            key: error[key]
            for key in ("code", "recoverable", "instance_id")
            if key in error
        }
    return sanitized


class AuditLogger:
    def __init__(self, path: 'Path', *, secure_parent: 'bool' = False) -> 'None':
        self.path = path
        parent_existed = os.path.lexists(self.path.parent)
        # The default state directory is dedicated to maxc and can be safely
        # repaired. For a custom audit path, only secure a newly created parent
        # so we never chmod an existing shared directory such as /tmp.
        directory = open_private_directory(
            self.path.parent,
            secure_existing=secure_parent or not parent_existed,
        )
        try:
            descriptor = open_private_file_at(
                directory,
                self.path.name,
                os.O_WRONLY | os.O_APPEND,
                create=True,
                display_path=self.path,
            )
            os.close(descriptor)
        finally:
            close_private_directory(directory)

    def log(self, payload: 'dict[str, Any]') -> 'None':
        record = sanitize_audit_payload(payload)
        record.setdefault("ts", now_utc_iso())
        directory = open_private_directory(
            self.path.parent,
            create=False,
        )
        try:
            descriptor = open_private_file_at(
                directory,
                self.path.name,
                os.O_WRONLY | os.O_APPEND,
                create=True,
                display_path=self.path,
            )
            with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        finally:
            close_private_directory(directory)
