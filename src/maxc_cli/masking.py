
import re
from typing import Any


SENSITIVE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)(password|passwd|secret|pwd|api_key)"), "password"),
    (re.compile(r"(?i)(phone|mobile|tel|cellphone)"), "phone"),
    (re.compile(r"(?i)(email|e_mail|mail_addr)"), "email"),
    (re.compile(r"(?i)(id_card|idcard|ssn|identity_no|cert_no)"), "id_card"),
]


def _classify_column(column_name: str, extra_patterns: list[str] | None = None) -> str | None:
    """Return the masking type for a column name, or None if not sensitive."""
    for pattern, mask_type in SENSITIVE_PATTERNS:
        if pattern.search(column_name):
            return mask_type
    if extra_patterns:
        lower = column_name.lower()
        for pat in extra_patterns:
            if pat.lower() in lower:
                return "custom"
    return None


def _mask_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    if len(digits) >= 7:
        return digits[:3] + "****" + digits[-4:]
    return "***"


def _mask_email(value: str) -> str:
    if "@" in value:
        local, domain = value.rsplit("@", 1)
        return local[0] + "***@" + domain if local else "***@" + domain
    return "***"


def _mask_password(_value: str) -> str:
    return "******"


def _mask_id_card(value: str) -> str:
    cleaned = re.sub(r"\s", "", value)
    if len(cleaned) >= 7:
        return cleaned[:3] + "*" * (len(cleaned) - 7) + cleaned[-4:]
    return "***"


def _mask_custom(_value: str) -> str:
    return "***"


_MASKERS: dict[str, Any] = {
    "phone": _mask_phone,
    "email": _mask_email,
    "password": _mask_password,
    "id_card": _mask_id_card,
    "custom": _mask_custom,
}


def mask_rows(
    rows: list[dict[str, Any]],
    schema: list[dict[str, Any]],
    extra_sensitive_columns: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Mask sensitive columns in query result rows.

    Returns (masked_rows, masked_column_names).
    """
    if not rows:
        return rows, []

    # Determine which columns need masking
    column_names = [col.get("name", "") for col in schema] if schema else list(rows[0].keys())
    sensitive_map: dict[str, str] = {}  # column_name -> mask_type
    for col_name in column_names:
        mask_type = _classify_column(col_name, extra_sensitive_columns)
        if mask_type is not None:
            sensitive_map[col_name] = mask_type

    if not sensitive_map:
        return rows, []

    # Apply masking
    masked_rows = []
    for row in rows:
        new_row = dict(row)
        for col_name, mask_type in sensitive_map.items():
            if col_name in new_row and new_row[col_name] is not None:
                val = new_row[col_name]
                if isinstance(val, str):
                    masker = _MASKERS.get(mask_type, _mask_custom)
                    new_row[col_name] = masker(val)
                else:
                    # Convert to string, mask, keep as string
                    masker = _MASKERS.get(mask_type, _mask_custom)
                    new_row[col_name] = masker(str(val))
        masked_rows.append(new_row)

    return masked_rows, sorted(sensitive_map.keys())
