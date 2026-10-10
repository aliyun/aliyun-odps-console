"""Scope-bound pagination for registered function discovery."""

import base64
import json
from typing import Any

from .exceptions import ValidationError


def function_cursor(offset: int, *, project: str, schema: 'str | None', prefix: 'str | None') -> str:
    payload = {"v": 1, "offset": offset, "project": project, "schema": schema, "prefix": prefix}
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()


def function_offset(cursor: 'str | None', *, project: str, schema: 'str | None', prefix: 'str | None') -> int:
    if not cursor:
        return 0
    try:
        if len(cursor) > 8192:
            raise ValueError("oversized cursor")
        payload: Any = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
        if not isinstance(payload, dict) or payload.get("v") != 1:
            raise ValueError("invalid version")
        offset = payload.get("offset")
        if type(offset) is not int or offset < 0:
            raise ValueError("invalid offset")
        if any(payload.get(key) != value for key, value in {
            "project": project, "schema": schema, "prefix": prefix,
        }.items()):
            raise ValueError("scope mismatch")
        return offset
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValidationError(
            "Invalid function cursor or cursor scope mismatch.",
            suggestion="Use the returned next_cursor with the same project, schema, and prefix.",
        ) from exc
