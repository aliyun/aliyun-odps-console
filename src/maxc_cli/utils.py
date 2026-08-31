
import base64
import ipaddress
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .exceptions import (
    UnsupportedSqlOperationError,
    ValidationError,
    WriteOperationRequiresForceError,
)

SQL_COMMENT_RE = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)
_CODE_BLOCK_START_RE = re.compile(r"(?i)#CODE\b")
_CODE_BLOCK_END_RE = re.compile(r"(?i)#END\s+CODE\b")
TABLE_NAME_RE = re.compile(
    r"(?i)\b(?:from|join|into|update|table)\s+([a-zA-Z0-9_][\w.]*)"
)
_CLI_ENTRY_POINT_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.-]*(?: [A-Za-z0-9][A-Za-z0-9_.-]*)*"
)
_SQL_IDENTIFIER_SEGMENT_RE = (
    r"(?:[A-Za-z_][A-Za-z0-9_$]*|`(?:``|[^`\r\n])+`)"
)
_SQL_QUALIFIED_IDENTIFIER_RE = (
    rf"{_SQL_IDENTIFIER_SEGMENT_RE}"
    rf"(?:\s*\.\s*{_SQL_IDENTIFIER_SEGMENT_RE}){{0,2}}"
)
_ALTER_VIEW_RENAME_RE = re.compile(
    rf"(?is)^\s*ALTER\s+VIEW\s+{_SQL_QUALIFIED_IDENTIFIER_RE}\s+"
    rf"RENAME\s+TO\s+{_SQL_QUALIFIED_IDENTIFIER_RE}\s*;?\s*$"
)
_ALTER_SNAPSHOT_OPTIONS_PREFIX_RE = re.compile(
    rf"(?is)^\s*ALTER\s+SNAPSHOT\s+TABLE\s+"
    rf"(?:IF\s+EXISTS\s+)?"
    rf"{_SQL_QUALIFIED_IDENTIFIER_RE}\s+SET\s+OPTIONS\s*"
)
_DROP_SCHEMA_RE = re.compile(
    rf"(?is)^\s*DROP\s+SCHEMA\s+(?:IF\s+EXISTS\s+)?"
    rf"{_SQL_IDENTIFIER_SEGMENT_RE}\s*;?\s*$"
)
_DROP_DATA_OBJECT_RE = re.compile(
    rf"(?is)^\s*DROP\s+(?:"
    rf"TABLE\s+(?:IF\s+EXISTS\s+)?{_SQL_QUALIFIED_IDENTIFIER_RE}|"
    rf"SNAPSHOT\s+TABLE\s+(?:IF\s+EXISTS\s+)?{_SQL_QUALIFIED_IDENTIFIER_RE}|"
    rf"VIEW\s+(?:IF\s+EXISTS\s+)?{_SQL_QUALIFIED_IDENTIFIER_RE}|"
    rf"MATERIALIZED\s+VIEW\s+(?:IF\s+EXISTS\s+)?"
    rf"{_SQL_QUALIFIED_IDENTIFIER_RE}(?:\s+PURGE)?|"
    rf"FUNCTION\s+{_SQL_QUALIFIED_IDENTIFIER_RE}"
    rf")\s*;?\s*$"
)
_ALTER_SCHEMA_COMMENT_RE = re.compile(
    rf"(?is)^\s*ALTER\s+SCHEMA\s+{_SQL_IDENTIFIER_SEGMENT_RE}\s+"
    r"SET\s+COMMENT\s+'(?:''|\\.|[^'\\])*'\s*;?\s*$"
)
WRITE_OPERATIONS = frozenset({
    "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE",
    "CREATE", "DROP", "ALTER", "RENAME", "TRUNCATE", "CLONE", "RESTORE",
    "KILL", "ALIAS", "MSCK", "UNLOAD",
    "GRANT", "REVOKE",
    "ANALYZE", "OPTIMIZE", "COMPACT", "VACUUM",
    "USE", "ADD", "REMOVE", "PURGE", "INSTALL", "UNINSTALL", "LOAD",
})
_FORCE_ALLOWED_DML_OPERATIONS = frozenset({
    "INSERT", "UPDATE", "DELETE", "MERGE",
})
_FORCE_ALLOWED_DDL_PATTERNS = {
    "CREATE": re.compile(
        r"(?is)^CREATE\s+(?:"
        r"(?:OR\s+REPLACE\s+)?TABLE\b|"
        r"EXTERNAL\s+TABLE\b|"
        r"OBJECT\s+TABLE\b|"
        r"ICEBERG\s+TABLE\b|"
        r"(?:OR\s+REPLACE\s+)?SNAPSHOT\s+TABLE\b|"
        r"(?:OR\s+REPLACE\s+)?VIEW\b|"
        r"MATERIALIZED\s+VIEW\b|"
        r"(?:SQL\s+)?FUNCTION\b|"
        r"(?:EXTERNAL\s+)?SCHEMA\b)"
    ),
    "ALTER": re.compile(
        r"(?is)^ALTER\s+(?:TABLE|SNAPSHOT\s+TABLE|VIEW|MATERIALIZED\s+VIEW|SCHEMA)\b"
    ),
    "DROP": re.compile(
        r"(?is)^DROP\s+(?:TABLE|SNAPSHOT\s+TABLE|VIEW|MATERIALIZED\s+VIEW|FUNCTION|SCHEMA)\b"
    ),
    "TRUNCATE": re.compile(r"(?is)^TRUNCATE\s+TABLE\b"),
    "CLONE": re.compile(r"(?is)^CLONE\s+TABLE\b"),
    "RESTORE": re.compile(r"(?is)^RESTORE\s+TABLE\b"),
    "MSCK": re.compile(r"(?is)^MSCK\s+REPAIR\s+TABLE\b"),
    "UNLOAD": re.compile(r"(?is)^UNLOAD\s+FROM\b"),
    "ANALYZE": re.compile(r"(?is)^ANALYZE\s+TABLE\b"),
    "LOAD": re.compile(r"(?is)^LOAD\s+(?:INTO|OVERWRITE)\s+TABLE\b"),
    "PURGE": re.compile(r"(?is)^PURGE\s+TABLE\b"),
}
RESULT_OPERATIONS = frozenset({"SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN"})
_CONTROL_FLOW_OPERATIONS = frozenset({
    "BEGIN", "DO", "ELSE", "ELSEIF", "END", "FOR", "IF", "LOOP", "THEN", "WHILE",
})
_SPECIAL_WRITE_OPERATIONS = frozenset({"SETPROJECT"})
_SCRIPT_OPERATION = "SCRIPT"

# Leading ``SET key=value;`` statements are removed from the SQL text and
# passed to PyODPS as execution hints. They are therefore part of the remote
# execution context even though they are not counted as executable SQL
# statements. These documented project-security and masking controls must
# never be accepted through the public query path.
_BLOCKED_SQL_SETTING_KEYS = frozenset({
    "checkpermissionusingacl",
    "checkpermissionusingpolicy",
    "labelsecurity",
    "objectcreatorhasaccesspermission",
    "objectcreatorhasgrantpermission",
    "odps.isolation.session.enable",
    "odps.output.field.formatter",
    "odps.forbid.fetch.result.by.bearertoken",
    "odps.security.enabledownloadprivilege",
    "odps.security.ip.whitelist",
    "odps.security.ip.whitelist.services",
    "odps.security.vpc.whitelist",
    "projectprotection",
})

# Mutating SQL is intentionally narrower than SELECT. A forced statement may
# use only execution hints that this client has reviewed as statement-local
# SQL/runtime controls; an unknown hint must not become a back door around the
# positive SQL-operation allowlist. Keep this list synchronized with the
# public Skill's documented SET examples.
_FORCE_ALLOWED_SQL_SETTING_KEYS = frozenset({
    "odps.ext.hive.lazy.simple.serde.native",
    "odps.function.strictmode",
    "odps.instance.priority",
    "odps.mcqa.disable",
    "odps.namespace.schema",
    "odps.optimizer.auto.mapjoin.threshold",
    "odps.sql.allow.cartesian",
    "odps.sql.allow.namespace.schema",
    "odps.sql.bigquery.compatible",
    "odps.sql.decimal.odps2",
    "odps.sql.default.zorder.type",
    "odps.sql.executionengine.batch.rowcount",
    "odps.sql.hive.compatible",
    "odps.sql.insert.acidtable.deduplicate.enable",
    "odps.sql.job.max.time.hours",
    "odps.sql.mapjoin.memory.max",
    "odps.sql.rcte.max.iterate.num",
    "odps.sql.submit.mode",
    "odps.sql.timestamp.function.ntz",
    "odps.sql.type.json.enable",
    "odps.sql.type.system.odps2",
    "odps.sql.udf.strict.mode",
    "odps.sql.udf.timeout",
    "odps.sql.validate.orderby.limit",
})

_REDACTED_SQL_HINT_VALUE = "<redacted>"
_BOOLEAN_SQL_HINT_KEYS = frozenset({
    "odps.ext.hive.lazy.simple.serde.native",
    "odps.function.strictmode",
    "odps.mcqa.disable",
    "odps.namespace.schema",
    "odps.sql.allow.cartesian",
    "odps.sql.allow.namespace.schema",
    "odps.sql.bigquery.compatible",
    "odps.sql.decimal.odps2",
    "odps.sql.hive.compatible",
    "odps.sql.insert.acidtable.deduplicate.enable",
    "odps.sql.timestamp.function.ntz",
    "odps.sql.type.json.enable",
    "odps.sql.type.system.odps2",
    "odps.sql.udf.strict.mode",
    "odps.sql.validate.orderby.limit",
})
_INTEGER_SQL_HINT_RANGES = {
    "odps.instance.priority": (0, 9),
    # The service documents this threshold in bytes but does not publish a
    # narrower upper bound. Limit disclosure to an unsigned 64-bit-safe
    # scalar; arbitrary strings and oversized digit payloads remain redacted.
    "odps.optimizer.auto.mapjoin.threshold": (0, 2**63 - 1),
    "odps.sql.executionengine.batch.rowcount": (1, 1024),
    "odps.sql.job.max.time.hours": (1, 72),
    "odps.sql.mapjoin.memory.max": (0, 8192),
    "odps.sql.rcte.max.iterate.num": (1, 100),
    "odps.sql.udf.timeout": (0, 3600),
}
_ENUM_SQL_HINT_VALUES = {
    "odps.sql.default.zorder.type": frozenset({"global", "local"}),
    "odps.sql.submit.mode": frozenset({"script"}),
}


def _safe_effective_sql_hint_value(normalized_key: 'str', value: 'str') -> 'str':
    """Return *value* only when it belongs to the key's expected domain."""
    text = str(value)
    if not text or text != text.strip():
        return _REDACTED_SQL_HINT_VALUE

    if normalized_key in _BOOLEAN_SQL_HINT_KEYS:
        return text if text.lower() in {"true", "false"} else _REDACTED_SQL_HINT_VALUE

    integer_range = _INTEGER_SQL_HINT_RANGES.get(normalized_key)
    if integer_range is not None:
        # Bound length before int conversion to avoid treating an arbitrary
        # digit payload as a harmless scalar (and to avoid oversized parsing).
        if re.fullmatch(r"[0-9]{1,19}", text) is None:
            return _REDACTED_SQL_HINT_VALUE
        number = int(text)
        lower, upper = integer_range
        return text if lower <= number <= upper else _REDACTED_SQL_HINT_VALUE

    enum_values = _ENUM_SQL_HINT_VALUES.get(normalized_key)
    if enum_values is not None:
        return text if text.lower() in enum_values else _REDACTED_SQL_HINT_VALUE

    # New allowlisted keys must receive an explicit output-value policy before
    # their values can be disclosed.
    return _REDACTED_SQL_HINT_VALUE


def effective_sql_hints_for_output(
    hints: 'dict[str, str]',
    *,
    priority: 'int | None' = None,
) -> 'dict[str, str]':
    """Return a safe, accurate summary of SQL execution hints.

    ``hints`` must be the validated mapping that is actually passed to
    PyODPS. A reviewed key is not sufficient to disclose its value: the value
    must also match that key's boolean, integer, or enum domain. Read-only
    SELECT keeps compatibility with unknown provider hints, whose values can
    contain arbitrary user input, so those values are deliberately redacted
    while retaining the effective key. The separately threaded instance
    priority is included only when it is actually supplied to PyODPS.
    """
    effective: dict[str, str] = {}
    for key, value in hints.items():
        normalized_key = key.strip().lower()
        effective[key] = _safe_effective_sql_hint_value(normalized_key, value)
    if priority is not None:
        effective["odps.instance.priority"] = _safe_effective_sql_hint_value(
            "odps.instance.priority",
            str(priority),
        )
    return effective


def current_cli_entry_point() -> 'str':
    """Return the safe, normalized command prefix for this distribution."""
    raw = os.environ.get("MAXC_CLI_NAME", "")
    normalized = " ".join(raw.split())
    if normalized and _CLI_ENTRY_POINT_RE.fullmatch(normalized):
        return normalized

    # The Alibaba Cloud CLI launcher executes the managed PyInstaller binary
    # from ``~/.aliyun/maxc/maxc``. Older launchers do not set MAXC_CLI_NAME,
    # so infer the public entry point from that stable install location.
    for candidate in (sys.executable, sys.argv[0] if sys.argv else ""):
        parts = Path(candidate).expanduser().parts
        if any(
            parts[index] == ".aliyun" and parts[index + 1] == "maxc"
            for index in range(max(0, len(parts) - 1))
        ):
            return "aliyun maxc"
    return "maxc"


def distribution_cli_text(text: 'str') -> 'str':
    """Render command-shaped ``maxc`` references without rewriting payloads."""
    cli = current_cli_entry_point()
    if cli == "maxc" or "maxc " not in text:
        return text
    command_group = re.compile(
        r"maxc(?=\s+(?:agent|auth|cache|data|job|meta|nl2sql|project|query|session)\b)"
    )
    rendered: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if quote is not None:
            rendered.append(char)
            if char == "\\" and index + 1 < len(text):
                rendered.append(next_char)
                index += 2
                continue
            if char == quote:
                if next_char == quote:
                    rendered.append(next_char)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            rendered.append(char)
            index += 1
            continue
        match = command_group.match(text, index)
        if (
            match is not None
            and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_"))
            and not text[max(0, index - len("aliyun ")):index].endswith("aliyun ")
        ):
            rendered.append(cli)
            index = match.end()
            continue
        rendered.append(char)
        index += 1
    return "".join(rendered)


def sanitize_logview_url(value: 'str | None') -> 'str | None':
    """Remove credentials while retaining a numeric MCQA subquery selector."""
    if not value:
        return value
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return "[redacted invalid LogView URL]"
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except (TypeError, ValueError):
        # A malformed authority (for example an invalid IPv6 literal) can make
        # urlsplit fail before userinfo is separated. Returning any substring
        # of the original value risks preserving a password or signed token.
        return "[redacted invalid LogView URL]"
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.netloc
        or not hostname
        or port == 0
    ):
        # Opaque, relative, local-file, and script URLs do not satisfy the
        # LogView transport contract. Never return part of an invalid value:
        # its path or scheme-specific payload may itself contain credentials.
        return "[redacted invalid LogView URL]"
    if ":" in hostname:
        try:
            ipaddress.IPv6Address(hostname)
        except ValueError:
            return "[redacted invalid LogView URL]"
        safe_host = f"[{hostname}]"
    else:
        labels = hostname.rstrip(".").split(".")
        if (
            len(hostname) > 253
            or not labels
            or any(
                not label
                or len(label) > 63
                or re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", label)
                is None
                for label in labels
            )
        ):
            return "[redacted invalid LogView URL]"
        safe_host = hostname
    safe_netloc = safe_host if port is None else f"{safe_host}:{port}"
    safe_query = urlencode([
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() == "subquery" and re.fullmatch(r"[0-9]+", item)
    ])
    if (
        safe_query == parsed.query
        and not parsed.fragment
        and safe_netloc == parsed.netloc
    ):
        return value
    return urlunsplit((parsed.scheme, safe_netloc, parsed.path, safe_query, ""))


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


def validate_csv_delimiter(delimiter: 'str') -> 'None':
    """Validate the stdlib CSV single-character delimiter contract."""
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValidationError(
            "`--delimiter` must be exactly one character.",
            suggestion="Use a single character such as `,`, `\\t`, or `|`.",
        )


def validate_upload_input_path(file_path: 'str | Path') -> 'Path':
    """Fail locally for missing, non-regular, or unreadable upload input."""
    path = Path(file_path).expanduser().absolute()
    if not path.exists():
        raise ValidationError(
            f"Upload input does not exist: {path}",
            suggestion="Choose an existing readable CSV/TSV file.",
        )
    if not path.is_file():
        raise ValidationError(
            f"Upload input is not a regular file: {path}",
            suggestion="Choose a regular CSV/TSV file.",
        )
    try:
        with path.open("rb") as stream:
            stream.read(0)
    except OSError as exc:
        raise ValidationError(
            f"Upload input is not readable: {path}: {exc}",
            suggestion="Check the file permissions and try again.",
        ) from exc
    return path


def validate_download_output_path(
    output_path: 'str | Path',
    *,
    overwrite: 'bool',
) -> 'Path':
    """Fail locally before a download session for an unusable destination."""
    path = Path(output_path).expanduser().absolute()
    if path.exists():
        if path.is_dir():
            raise ValidationError(f"Download output is a directory: {path}")
        if not overwrite:
            raise ValidationError(
                f"Download output already exists: {path}",
                suggestion="Choose a new path or pass --overwrite to replace the file.",
            )
    parent = path.parent
    if not parent.exists():
        raise ValidationError(
            f"Download output directory does not exist: {parent}",
            suggestion="Create the directory or choose an existing writable directory.",
        )
    if not parent.is_dir():
        raise ValidationError(f"Download output parent is not a directory: {parent}")
    if not os.access(parent, os.W_OK):
        raise ValidationError(
            f"Download output directory is not writable: {parent}",
            suggestion="Choose a writable directory or correct its permissions.",
        )
    return path


def _sql_raw_literal_end(sql: 'str', index: int) -> 'int | None':
    """Return the index after a MaxCompute raw literal starting at *index*.

    MaxCompute accepts both ``R\"(...)\"`` and ``R'(...)'`` (with a
    case-insensitive ``R``). Quotes and semicolons inside the parentheses are
    literal content; only ``)`` followed by the opening quote terminates the
    value. An unterminated raw literal consumes the remainder so the local
    scanners cannot reinterpret its contents as executable SQL.
    """
    if index + 2 >= len(sql) or sql[index] not in {"R", "r"}:
        return None
    if index > 0 and (sql[index - 1].isalnum() or sql[index - 1] in {"_", "$"}):
        return None
    quote = sql[index + 1]
    if quote not in {"'", '"'} or sql[index + 2] != "(":
        return None
    end = sql.find(")" + quote, index + 3)
    return len(sql) if end == -1 else end + 2


def _strip_sql_comments(sql: 'str') -> 'str':
    """Remove SQL comments without altering quoted literals or identifiers."""
    output: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""
        if quote is not None:
            output.append(char)
            if char == "\\" and index + 1 < len(sql):
                output.append(next_char)
                index += 2
                continue
            if char == quote:
                if next_char == quote:
                    output.append(next_char)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        raw_end = _sql_raw_literal_end(sql, index)
        if raw_end is not None:
            output.append(sql[index:raw_end])
            index = raw_end
            continue
        if char in {"'", '"', "`"}:
            quote = char
            output.append(char)
            index += 1
            continue
        if char == "-" and next_char == "-":
            end = sql.find("\n", index + 2)
            index = len(sql) if end == -1 else end + 1
            output.append(" ")
            continue
        if char == "/" and next_char == "*":
            end = sql.find("*/", index + 2)
            index = len(sql) if end == -1 else end + 2
            output.append(" ")
            continue
        output.append(char)
        index += 1
    return "".join(output)


def normalize_sql(sql: 'str') -> 'str':
    return " ".join(_strip_sql_comments(sql).strip().split())


def detect_operation(sql: 'str') -> 'str':
    normalized = normalize_sql(sql)
    match = re.match(r"(?i)^([a-z]+)", normalized)
    return match.group(1).upper() if match else "UNKNOWN"


def split_sql_statements(sql: 'str') -> 'list[str]':
    """Split top-level statements without treating quoted semicolons as separators."""
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    in_code_block = False
    compound_create = bool(re.match(
        r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:VIEW|SQL\s+FUNCTION)\b",
        normalize_sql(sql),
    ))
    saw_compound_as = False
    compound_stack: list[str] = []
    compound_finished = False
    index = 0

    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""

        if in_code_block:
            end_match = _CODE_BLOCK_END_RE.match(sql, index)
            if end_match is not None:
                current.append(sql[index:end_match.end()])
                index = end_match.end()
                in_code_block = False
            else:
                current.append(char)
                index += 1
            continue

        if quote is not None:
            current.append(char)
            if char == "\\" and index + 1 < len(sql):
                current.append(next_char)
                index += 2
                continue
            if char == quote:
                if next_char == quote:
                    current.append(next_char)
                    index += 2
                    continue
                quote = None
            index += 1
            continue

        raw_end = _sql_raw_literal_end(sql, index)
        if raw_end is not None:
            current.append(sql[index:raw_end])
            index = raw_end
            continue
        if char in {"'", '"', "`"}:
            quote = char
            current.append(char)
            index += 1
            continue
        code_match = _CODE_BLOCK_START_RE.match(sql, index)
        if code_match is not None:
            current.append(sql[index:code_match.end()])
            index = code_match.end()
            in_code_block = True
            continue
        if char == "-" and next_char == "-":
            end = sql.find("\n", index + 2)
            if end == -1:
                current.append(sql[index:])
                index = len(sql)
            else:
                current.append(sql[index:end + 1])
                index = end + 1
            continue
        if char == "/" and next_char == "*":
            end = sql.find("*/", index + 2)
            if end == -1:
                current.append(sql[index:])
                index = len(sql)
            else:
                current.append(sql[index:end + 2])
                index = end + 2
            continue
        if char.isalpha() or char == "_":
            end = index + 1
            while end < len(sql) and (sql[end].isalnum() or sql[end] in {"_", "$"}):
                end += 1
            word = sql[index:end].upper()
            current.append(sql[index:end])
            if compound_create and not compound_finished:
                if not compound_stack:
                    if word == "AS":
                        saw_compound_as = True
                    elif saw_compound_as and word == "BEGIN":
                        compound_stack.append("BEGIN")
                else:
                    if word in {"BEGIN", "CASE"}:
                        compound_stack.append(word)
                    elif word == "END":
                        compound_stack.pop()
                        if not compound_stack:
                            compound_finished = True
            index = end
            continue
        if char == ";":
            if compound_stack:
                current.append(char)
                index += 1
                continue
            statement = "".join(current).strip()
            if normalize_sql(statement):
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    statement = "".join(current).strip()
    if normalize_sql(statement):
        statements.append(statement)
    return statements


def _is_compound_create_sql(sql: 'str') -> 'bool':
    """Whether SQL is one permanent view/function DDL with an AS BEGIN body."""
    visible = _unquoted_sql_text(sql)
    return bool(re.match(
        r"(?is)^\s*CREATE\s+"
        r"(?:(?:OR\s+REPLACE\s+)?VIEW|SQL\s+FUNCTION)\b"
        r".*\bAS\s+BEGIN\b.*\bEND\s*;?\s*$",
        visible,
    ))


def statement_operations(sql: 'str') -> 'list[str]':
    """Return operations for each statement after removing leading ``SET`` hints."""
    return [detect_operation(statement) for statement in sql_statements(sql)]


def sql_statements(sql: 'str') -> 'list[str]':
    """Return executable statements after removing leading ``SET`` hints."""
    from .setting_parser import SettingParser

    parsed = SettingParser.parse(sql)
    candidate = parsed.remaining_query.strip() if not parsed.errors else sql.strip()
    return split_sql_statements(candidate)


def sql_keyword_tokens(sql: 'str') -> 'list[str]':
    """Return unquoted SQL keywords, excluding comments and literal contents."""
    return [
        token.upper()
        for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", _unquoted_sql_text(sql))
    ]


def _unquoted_sql_text(
    sql: 'str',
    *,
    quoted_placeholder: 'str' = " ",
) -> 'str':
    """Blank quoted and commented content while retaining SQL operators."""
    visible: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""
        if quote is not None:
            if char == "\\" and index + 1 < len(sql):
                index += 2
                continue
            if char == quote:
                if next_char == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        raw_end = _sql_raw_literal_end(sql, index)
        if raw_end is not None:
            visible.append(quoted_placeholder)
            index = raw_end
            continue
        if char in {"'", '"', "`"}:
            quote = char
            visible.append(quoted_placeholder)
            index += 1
            continue
        if char == "-" and next_char == "-":
            end = sql.find("\n", index + 2)
            index = len(sql) if end == -1 else end + 1
            visible.append(" ")
            continue
        if char == "/" and next_char == "*":
            end = sql.find("*/", index + 2)
            index = len(sql) if end == -1 else end + 2
            visible.append(" ")
            continue
        visible.append(char)
        index += 1
    return "".join(visible)


def _sql_lex_tokens(sql: 'str') -> 'list[tuple[str, int]]':
    """Return unquoted words and grouping symbols paired with nesting depth.

    This is intentionally a small lexer rather than a SQL parser. It lets the
    safety gate distinguish executable operation positions from identifiers or
    UDF names such as ``load(...)`` without importing PyODPS on local commands.
    """
    tokens: list[tuple[str, int]] = []
    depth = 0
    index = 0
    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""

        raw_end = _sql_raw_literal_end(sql, index)
        if raw_end is not None:
            tokens.append(("<QUOTED>", depth))
            index = raw_end
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            while index < len(sql):
                current = sql[index]
                following = sql[index + 1] if index + 1 < len(sql) else ""
                if current == "\\" and index + 1 < len(sql):
                    index += 2
                    continue
                if current == quote:
                    if following == quote:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            tokens.append(("<QUOTED>", depth))
            continue
        if char == "-" and next_char == "-":
            end = sql.find("\n", index + 2)
            index = len(sql) if end == -1 else end + 1
            continue
        if char == "/" and next_char == "*":
            end = sql.find("*/", index + 2)
            index = len(sql) if end == -1 else end + 2
            continue
        if char.isalpha() or char == "_":
            end = index + 1
            while end < len(sql) and (sql[end].isalnum() or sql[end] in {"_", "$"}):
                end += 1
            tokens.append((sql[index:end].upper(), depth))
            index = end
            continue
        if char == "(":
            tokens.append((char, depth))
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            tokens.append((char, depth))
            index += 1
            continue
        if char in {",", "=", "@"}:
            tokens.append((char, depth))
            index += 1
            continue
        if char == ":" and next_char == "=":
            tokens.append((":=", depth))
            index += 2
            continue
        index += 1
    return tokens


def _skip_token_group(tokens: 'list[tuple[str, int]]', index: int) -> 'int | None':
    """Return the index after a parenthesized token group, or ``None``."""
    if index >= len(tokens) or tokens[index][0] != "(":
        return None
    group_depth = tokens[index][1]
    for current in range(index + 1, len(tokens)):
        if tokens[current] == (")", group_depth):
            return current + 1
    return None


def _top_level_command_heads(
    tokens: 'list[tuple[str, int]]',
) -> 'list[tuple[int, str]]':
    """Locate command-shaped heads at the outer SQL nesting level.

    The service parser remains authoritative for full syntax. This local check
    prevents a valid mutation prefix from hiding a second command without a
    semicolon, while allowing clause keywords such as ALTER TABLE ... DROP
    COLUMN and MERGE ... THEN INSERT VALUES.
    """
    if not tokens:
        return []
    base_depth = tokens[0][1]
    words = [
        (index, token)
        for index, (token, depth) in enumerate(tokens)
        if depth == base_depth and token not in {"(", ")", ",", "=", ":=", "@"}
    ]
    heads: list[tuple[int, str]] = []
    create_objects = {
        "DATA", "DATABASE", "EXTERNAL", "FUNCTION", "ICEBERG", "MATERIALIZED",
        "OBJECT", "PACKAGE", "PROJECT", "RESOURCE", "ROLE", "SCHEMA",
        "ROW", "SECURITY", "SNAPSHOT", "SQL", "TABLE", "TENANT", "VIEW",
    }
    drop_objects = {
        "DATA", "DATABASE", "FUNCTION", "MATERIALIZED", "PACKAGE", "PROJECT",
        "QUOTA", "RESOURCE", "ROLE", "ROW", "SCHEMA", "SNAPSHOT", "TABLE", "VIEW",
    }
    alter_objects = {
        "CLUSTER", "FUNCTION", "MATERIALIZED", "PROJECT", "QUOTA", "SCHEMA",
        "SNAPSHOT", "SYSTEM", "TABLE", "USER", "VIEW",
    }
    standalone = {
        "ALLOW", "CALL", "CLEAR", "DISALLOW", "EXECUTE", "GRANT",
        "INSTALL", "KILL", "LIST", "PAI", "PUT", "REVOKE", "SETPROJECT",
        "UNINSTALL", "USE",
    }

    for position, (token_index, token) in enumerate(words):
        following = [item for _index, item in words[position + 1:position + 8]]
        raw_following = [
            item
            for item, depth in tokens[token_index + 1:]
            if depth == base_depth
        ][:4]
        next_word = following[0] if following else ""
        operation: str | None = None
        if token == "INSERT" and next_word in {"INTO", "OVERWRITE"}:
            operation = token
        elif token == "UPDATE" and next_word != "SET" and "SET" in following:
            operation = token
        elif token == "DELETE" and next_word == "FROM":
            operation = token
        elif token == "MERGE" and next_word == "INTO":
            operation = token
        elif token == "CREATE":
            object_word = next_word
            if following[:2] == ["OR", "REPLACE"] and len(following) >= 3:
                object_word = following[2]
            if object_word in create_objects:
                operation = token
        elif token == "DROP" and next_word in drop_objects:
            operation = token
        elif token == "DROP" and following[:2] == ["ALL", "ROW"]:
            operation = token
        elif token == "ALTER" and next_word in alter_objects:
            operation = token
        elif token == "RENAME" and next_word in {"TABLE", "VIEW"}:
            operation = token
        elif token == "TRUNCATE" and next_word == "TABLE":
            operation = token
        elif token == "CLONE" and next_word == "TABLE":
            operation = token
        elif token == "RESTORE" and next_word == "TABLE":
            operation = token
        elif token == "MSCK" and next_word == "REPAIR":
            operation = token
        elif token == "UNLOAD" and next_word == "FROM":
            operation = token
        elif token == "ANALYZE" and next_word == "TABLE":
            operation = token
        elif token == "LOAD" and next_word in {"INTO", "OVERWRITE"}:
            operation = token
        elif token == "PURGE" and next_word == "TABLE":
            operation = token
        elif token == "APPLY" and next_word == "DATA":
            operation = token
        elif token == "ADD" and next_word in {
            "ARCHIVE", "FILE", "JAR", "PY", "TABLE",
        }:
            operation = token
        elif (
            token == "ALIAS"
            and len(raw_following) >= 3
            and raw_following[1] == "="
            and raw_following[0] not in {"(", ")", ",", "=", ":=", "@"}
            and raw_following[2] not in {"(", ")", ",", "=", ":=", "@"}
        ):
            operation = token
        elif token in standalone:
            operation = token
        if operation is not None:
            heads.append((token_index, operation))
    return heads


def _with_main_operation_index(
    tokens: 'list[tuple[str, int]]',
    start: int,
) -> 'int | None':
    """Return the token index of the operation after a well-formed WITH clause."""
    base_depth = tokens[start][1]
    index = start + 1
    if index < len(tokens) and tokens[index] == ("RECURSIVE", base_depth):
        index += 1

    while index < len(tokens):
        # CTE name (quoted or unquoted).
        if tokens[index][1] != base_depth or tokens[index][0] in {"(", ")", ","}:
            return None
        index += 1

        # Optional CTE column list.
        if index < len(tokens) and tokens[index] == ("(", base_depth):
            index = _skip_token_group(tokens, index) or len(tokens)

        if index >= len(tokens) or tokens[index] != ("AS", base_depth):
            return None
        index += 1
        if index < len(tokens) and tokens[index] == ("NOT", base_depth):
            index += 1
        if index < len(tokens) and tokens[index] == ("MATERIALIZED", base_depth):
            index += 1
        if index >= len(tokens) or tokens[index] != ("(", base_depth):
            return None
        index = _skip_token_group(tokens, index) or len(tokens)

        if index < len(tokens) and tokens[index] == (",", base_depth):
            index += 1
            continue
        break

    while index < len(tokens) and tokens[index][1] != base_depth:
        index += 1
    if index >= len(tokens):
        return None
    operation = tokens[index][0]
    if operation == "WITH":
        return _with_main_operation_index(tokens, index)
    if operation == "FROM" or operation in RESULT_OPERATIONS or operation in WRITE_OPERATIONS:
        return index
    return None


def _multi_insert_operations(
    tokens: 'list[tuple[str, int]]',
    *,
    base_depth: int,
) -> 'list[str]':
    """Find operation-shaped INSERT clauses in a MaxCompute multi-insert."""
    operations: list[str] = []
    for index, (token, depth) in enumerate(tokens):
        if token != "INSERT" or depth != base_depth:
            continue
        for following, following_depth in tokens[index + 1:]:
            if following_depth != base_depth:
                continue
            if following in {"INTO", "OVERWRITE"}:
                operations.append("INSERT")
            break
    return operations


def _operation_at(
    tokens: 'list[tuple[str, int]]',
    index: int,
) -> 'list[str]':
    """Classify an operation token only when it appears at an execution boundary."""
    operation, base_depth = tokens[index]
    if operation == "@":
        # Script variables are job-local. Both declarations and assignments
        # are resultless even when an assignment's right side is SELECT.
        return [_SCRIPT_OPERATION]
    if operation == "FUNCTION":
        # Temporary SQL UDF declaration. Permanent functions start with
        # CREATE and are classified as a write before reaching this branch.
        return [_SCRIPT_OPERATION]
    if operation == "CREATE":
        outer_words = [
            token
            for token, depth in tokens[index:]
            if depth == base_depth and token not in {"(", ")", ",", "="}
        ]
        suffix = outer_words[1:]
        if suffix[:2] == ["TEMPORARY", "FUNCTION"] or suffix[:4] == [
            "OR", "REPLACE", "TEMPORARY", "FUNCTION",
        ]:
            # Code-embedded temporary functions exist only for this script and
            # are not persisted in MaxCompute metadata.
            return [_SCRIPT_OPERATION]
    if operation == "WITH":
        main_index = _with_main_operation_index(tokens, index)
        if main_index is None:
            return []
        main_operation, main_depth = tokens[main_index]
        if main_operation == "FROM":
            return _multi_insert_operations(
                tokens[main_index:],
                base_depth=main_depth,
            )
        return [main_operation]
    if operation == "FROM":
        return _multi_insert_operations(tokens[index:], base_depth=base_depth)
    if operation == "SETPROJECT":
        has_assignment = any(
            token == "=" and depth == base_depth
            for token, depth in tokens[index + 1:]
        )
        return [operation] if has_assignment else []
    if operation in RESULT_OPERATIONS or operation in WRITE_OPERATIONS:
        # A write-keyword UDF in a control condition is an identifier, not an
        # operation. Real statements such as LOAD DATA do not start with `(`.
        if (
            operation in WRITE_OPERATIONS
            and index + 1 < len(tokens)
            and tokens[index + 1] == ("(", base_depth)
        ):
            return []
        return [operation]
    return []


def _control_flow_operations(
    tokens: 'list[tuple[str, int]]',
    start: int,
) -> 'list[str]':
    """Return the single command head owned by this script control statement.

    MaxCompute IF conditions are parenthesized and the branch body follows the
    closing parenthesis. Looking for THEN / ELSE across the whole statement is
    unsafe because a branch query may contain a SQL CASE expression using the
    same words.
    """
    base_depth = tokens[start][1]
    leading = tokens[start][0]
    index = start + 1

    if leading in {"IF", "ELSEIF", "FOR", "WHILE"}:
        condition_start = next(
            (
                current
                for current in range(index, len(tokens))
                if tokens[current] == ("(", base_depth)
            ),
            None,
        )
        if condition_start is not None:
            index = _skip_token_group(tokens, condition_start) or len(tokens)
        else:
            # Retain support for loop-like forms using an explicit DO / THEN,
            # while refusing to scan arbitrary condition identifiers as verbs.
            boundary = next(
                (
                    current
                    for current in range(index, len(tokens))
                    if tokens[current][1] == base_depth
                    and tokens[current][0] in {"DO", "THEN"}
                ),
                None,
            )
            if boundary is None:
                return []
            index = boundary + 1

    while index < len(tokens) and tokens[index][1] != base_depth:
        index += 1
    while (
        index < len(tokens)
        and tokens[index][1] == base_depth
        and tokens[index][0] in {"DO", "THEN"}
    ):
        index += 1
    if index >= len(tokens) or tokens[index][1] != base_depth:
        return []

    operation = tokens[index][0]
    if operation in _CONTROL_FLOW_OPERATIONS:
        return _control_flow_operations(tokens, index)
    return _operation_at(tokens, index)


def executable_operations(sql: 'str') -> 'list[str]':
    """Return executable result/write operations without treating identifiers as verbs."""
    operations: list[str] = []
    for statement in sql_statements(sql):
        tokens = _sql_lex_tokens(statement)
        if not tokens:
            continue
        leading = detect_operation(statement)
        if (
            leading in RESULT_OPERATIONS
            or leading in WRITE_OPERATIONS
            or leading in {"WITH", "FROM", "FUNCTION", "SETPROJECT"}
            or tokens[0][0] == "@"
        ):
            operations.extend(_operation_at(tokens, 0))
        elif leading in _CONTROL_FLOW_OPERATIONS:
            operations.extend(_control_flow_operations(tokens, 0))
    return operations


def known_write_operations(sql: 'str') -> 'list[str]':
    """Find known mutations, including writes nested in script control flow."""
    return [
        operation
        for operation in executable_operations(sql)
        if operation in WRITE_OPERATIONS or operation in _SPECIAL_WRITE_OPERATIONS
    ]


def _force_statement_is_allowed(
    statement: 'str',
    *,
    leading_operation: 'str',
    operations: 'list[str]',
) -> 'bool':
    """Recognize one inspectable data-plane mutation and reject unknown admin SQL."""
    if len(operations) != 1:
        return False
    operation = operations[0]
    normalized = normalize_sql(statement)
    structural_statement = _strip_sql_comments(statement)
    tokens = _sql_lex_tokens(statement)
    if len(_top_level_command_heads(tokens)) != 1:
        return False

    if operation == "CREATE" and _is_compound_create_sql(statement):
        if not _force_compound_create_body_is_allowed(statement):
            return False

    if operation == "CREATE" and re.match(
        r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?SNAPSHOT\s+TABLE\b",
        structural_statement,
    ):
        return _force_snapshot_create_is_allowed(structural_statement)

    if operation in _FORCE_ALLOWED_DML_OPERATIONS:
        if _contains_force_admin_clause(statement, operation=operation):
            return False
        return _force_dml_shape_is_allowed(
            statement,
            leading_operation=leading_operation,
            operation=operation,
        )

    ddl_pattern = _FORCE_ALLOWED_DDL_PATTERNS.get(operation)
    if operation == "ALTER" and re.match(
        r"(?is)^\s*ALTER\s+VIEW\b",
        structural_statement,
    ):
        # MaxCompute ALTER VIEW supports RENAME and CHANGEOWNER. Ownership
        # transfer is administrative and blocked from this path, so only the
        # explicit rename form belongs to the public data-plane allowlist.
        # Updating a view definition uses CREATE OR REPLACE VIEW.
        return bool(_ALTER_VIEW_RENAME_RE.fullmatch(structural_statement))
    if operation == "ALTER" and re.match(
        r"(?is)^\s*ALTER\s+SNAPSHOT\s+TABLE\b",
        structural_statement,
    ):
        return _force_snapshot_alter_is_allowed(structural_statement)
    if operation == "ALTER" and re.match(
        r"(?is)^\s*ALTER\s+SCHEMA\b",
        structural_statement,
    ):
        return bool(_ALTER_SCHEMA_COMMENT_RE.fullmatch(structural_statement))
    if operation == "DROP" and re.match(
        r"(?is)^\s*DROP\s+SCHEMA\b",
        structural_statement,
    ):
        return bool(_DROP_SCHEMA_RE.fullmatch(structural_statement))
    if operation == "DROP":
        return bool(_DROP_DATA_OBJECT_RE.fullmatch(structural_statement))
    return bool(
        ddl_pattern is not None
        and leading_operation == operation
        and ddl_pattern.match(normalized)
        and not _contains_force_admin_clause(statement)
    )


def _force_compound_create_body_is_allowed(statement: 'str') -> 'bool':
    """Validate each expression in a permanent view/function BEGIN body.

    MaxCompute treats these semicolon-delimited expressions as one DDL object
    definition. Require every inner expression to be a variable assignment so
    an unrelated SQL command cannot hide inside that object definition.
    """
    visible = _unquoted_sql_text(
        statement,
        quoted_placeholder=" QUOTED_LITERAL ",
    )
    match = re.match(
        r"(?is)^\s*CREATE\s+"
        r"(?P<object_type>(?:OR\s+REPLACE\s+)?VIEW|SQL\s+FUNCTION)\b"
        r".*?\bAS\s+BEGIN\b"
        r"(?P<body>.*)\bEND\s*;?\s*$",
        visible,
    )
    if match is None:
        return False
    expressions = [
        normalize_sql(item)
        for item in match.group("body").split(";")
        if normalize_sql(item)
    ]
    if not expressions:
        return False
    for expression in expressions:
        assignment = re.match(
            r"(?is)^@[A-Za-z_][A-Za-z0-9_$]*\s*:=\s*(?P<rhs>.+)$",
            expression,
        )
        if assignment is None:
            return False
        rhs = assignment.group("rhs").strip()
        if match.group("object_type").upper().endswith("VIEW"):
            operations = executable_operations(rhs)
            if not operations or any(
                operation not in RESULT_OPERATIONS
                for operation in operations
            ):
                return False
        elif not _force_sql_function_expression_is_allowed(rhs):
            return False
    return True


def _force_sql_function_expression_is_allowed(rhs: 'str') -> 'bool':
    """Accept expression-shaped SQL UDF assignments and reject commands.

    This is deliberately a conservative lexer-level check, not a replacement
    for the MaxCompute parser. It recognizes literals, variables, arithmetic,
    CASE expressions and function calls while refusing query, mutation,
    dynamic-execution and script-control statement heads anywhere in the RHS.
    """
    visible = rhs.strip()
    if not visible or ";" in visible:
        return False

    balance = 0
    for char in visible:
        if char == "(":
            balance += 1
        elif char == ")":
            balance -= 1
            if balance < 0:
                return False
    if balance != 0:
        return False

    tokens = _sql_lex_tokens(visible)
    if any(token == ":=" for token, _depth in tokens):
        return False

    always_blocked = {"CALL", "EXECUTE", "PAI", "SETPROJECT"}
    command_words = (
        WRITE_OPERATIONS
        | RESULT_OPERATIONS
        | _SPECIAL_WRITE_OPERATIONS
        | {"BEGIN", "DO", "FOR", "FROM", "IF", "LOOP", "WHILE", "WITH"}
    )
    for index, (token, depth) in enumerate(tokens):
        if token in always_blocked:
            return False
        if token not in command_words:
            continue
        # Standard scalar functions such as TRIM and EXTRACT use FROM inside
        # their argument group. A real nested query or mutation still exposes
        # SELECT / WITH / DELETE (and is rejected independently).
        if token == "FROM" and depth > 0:
            continue
        # A name such as load(...) or if(...) is expression-shaped. Bare
        # operation keywords, including nested SELECT / DELETE, are commands.
        if index + 1 >= len(tokens) or tokens[index + 1] != ("(", depth):
            return False

    if not tokens:
        return bool(re.fullmatch(r"[0-9eE+\-*/%.\s]+", visible))
    if visible[0] in "+-(" or visible[0].isdigit():
        return True

    first, first_depth = tokens[0]
    if first == "@":
        return len(tokens) >= 2 and tokens[1][1] == first_depth
    if first in {
        "CASE",
        "CURRENT_DATE",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DATE",
        "FALSE",
        "INTERVAL",
        "NOT",
        "NULL",
        "QUOTED_LITERAL",
        "TIMESTAMP",
        "TRUE",
    }:
        return True
    return len(tokens) >= 2 and tokens[1] == ("(", first_depth)


def _force_snapshot_alter_is_allowed(statement: 'str') -> 'bool':
    """Allow snapshot metadata edits, but not access-permission changes."""
    if _ALTER_SNAPSHOT_OPTIONS_PREFIX_RE.match(statement) is None:
        return False
    tokens = _sql_lex_tokens(statement)
    if not tokens:
        return False
    base_depth = tokens[0][1]
    options_indices = [
        index
        for index, token in enumerate(tokens)
        if token == ("OPTIONS", base_depth)
    ]
    if len(options_indices) != 1:
        return False
    options_index = options_indices[0]
    group_index = options_index + 1
    if group_index >= len(tokens) or tokens[group_index] != ("(", base_depth):
        return False
    group_end = _skip_token_group(tokens, group_index)
    if group_end is None or group_end != len(tokens):
        return False

    option_keys = {
        tokens[index - 1][0]
        for index, token in enumerate(tokens)
        if token == ("=", base_depth + 1)
        and index > 0
        and tokens[index - 1][1] == base_depth + 1
    }
    return bool(
        option_keys
        and option_keys <= {"DESCRIPTION", "EXPIRATION_TIMESTAMP"}
    )


def _force_snapshot_create_is_allowed(statement: 'str') -> 'bool':
    """Allow snapshot creation options that do not change access policy."""
    tokens = _sql_lex_tokens(statement)
    if not tokens:
        return False
    base_depth = tokens[0][1]
    options_indices = [
        index
        for index, token in enumerate(tokens)
        if token == ("OPTIONS", base_depth)
    ]
    if not options_indices:
        return True
    if len(options_indices) != 1:
        return False
    group_index = options_indices[0] + 1
    if group_index >= len(tokens) or tokens[group_index] != ("(", base_depth):
        return False
    group_end = _skip_token_group(tokens, group_index)
    if group_end is None or group_end != len(tokens):
        return False
    option_keys = {
        tokens[index - 1][0]
        for index, token in enumerate(tokens)
        if token == ("=", base_depth + 1)
        and index > 0
        and tokens[index - 1][1] == base_depth + 1
    }
    return bool(
        option_keys
        and option_keys <= {"DESCRIPTION", "EXPIRATION_TIMESTAMP"}
    )


def _force_dml_shape_is_allowed(
    statement: 'str',
    *,
    leading_operation: 'str',
    operation: 'str',
) -> 'bool':
    """Validate the required outer clause of one direct or WITH-wrapped DML."""
    tokens = _sql_lex_tokens(statement)
    if not tokens:
        return False
    if leading_operation == "WITH":
        operation_index = _with_main_operation_index(tokens, 0)
        if operation_index is None:
            return False
        outer_operation, base_depth = tokens[operation_index]
        if outer_operation == "FROM":
            insert_indices = [
                index
                for index, (token, depth) in enumerate(tokens[operation_index:], operation_index)
                if token == "INSERT" and depth == base_depth
            ]
            return (
                operation == "INSERT"
                and len(insert_indices) == 1
                and _force_insert_shape_is_allowed(
                    tokens,
                    operation_index=insert_indices[0],
                    base_depth=base_depth,
                )
            )
    elif leading_operation == "FROM":
        base_depth = tokens[0][1]
        insert_indices = [
            index
            for index, (token, depth) in enumerate(tokens)
            if token == "INSERT" and depth == base_depth
        ]
        return (
            operation == "INSERT"
            and len(insert_indices) == 1
            and _force_insert_shape_is_allowed(
                tokens,
                operation_index=insert_indices[0],
                base_depth=base_depth,
            )
        )
    else:
        operation_index = 0

    actual_operation, base_depth = tokens[operation_index]
    if actual_operation != operation:
        return False
    outer_words = [
        token
        for token, depth in tokens[operation_index:]
        if depth == base_depth and token not in {"(", ")", ",", "="}
    ]
    if len(outer_words) < 2:
        return False
    if operation == "INSERT":
        return _force_insert_shape_is_allowed(
            tokens,
            operation_index=operation_index,
            base_depth=base_depth,
        )
    if operation == "UPDATE":
        return "SET" in outer_words[2:]
    if operation == "DELETE":
        return outer_words[1] == "FROM"
    if operation in {"MERGE", "UPSERT", "REPLACE"}:
        return outer_words[1] == "INTO"
    return False


def _force_insert_shape_is_allowed(
    tokens: 'list[tuple[str, int]]',
    *,
    operation_index: 'int',
    base_depth: 'int',
) -> 'bool':
    """Require a MaxCompute table target and reject Hive directory inserts."""
    words = [
        token
        for token, depth in tokens[operation_index:]
        if depth == base_depth and token not in {"(", ")", ",", "="}
    ]
    if len(words) < 3 or words[0] != "INSERT":
        return False
    mode = words[1]
    if mode == "OVERWRITE":
        return len(words) >= 4 and words[2] == "TABLE" and words[3] != "DIRECTORY"
    if mode == "INTO":
        target_index = 3 if words[2] == "TABLE" else 2
        return len(words) > target_index and words[target_index] != "DIRECTORY"
    return False


def _contains_force_admin_clause(
    statement: 'str',
    *,
    operation: 'str | None' = None,
) -> 'bool':
    """Reject ownership, authorization, and label-control subclauses.

    Some MaxCompute administrative operations share a data-plane DDL prefix,
    such as ``ALTER TABLE ... CHANGEOWNER``. Inspect unquoted, uncommented
    top-level tokens so a comment string containing these words is not
    mistaken for an administrative operation.
    """
    tokens = _sql_lex_tokens(statement)
    if not tokens:
        return False
    base_depth = tokens[0][1]
    words = [token for token, depth in tokens if depth == base_depth]
    # Match command signatures rather than isolated words. This keeps columns
    # such as authorization or changeowner usable in CTAS/view projections,
    # while still rejecting an appended GRANT or ADD USER after an AS body.
    blocked_pairs = {
        ("ADD", "ACCOUNTPROVIDER"),
        ("ADD", "ACCOUNTPROVIDERS"),
        ("ADD", "FILE"),
        ("ADD", "ROLE"),
        ("ADD", "ROLES"),
        ("ADD", "TRUSTEDPROJECT"),
        ("ADD", "TRUSTEDPROJECTS"),
        ("ADD", "USER"),
        ("ADD", "USERS"),
        ("ALLOW", "PROJECT"),
        ("ALTER", "CLUSTER"),
        ("ALTER", "PROJECT"),
        ("ALTER", "QUOTA"),
        ("ALTER", "ROLE"),
        ("ALTER", "SYSTEM"),
        ("ALTER", "USER"),
        ("CHANGEOWNER", "TO"),
        ("CREATE", "DATABASE"),
        ("CREATE", "PACKAGE"),
        ("CREATE", "PROJECT"),
        ("CREATE", "RESOURCE"),
        ("CREATE", "ROLE"),
        ("CREATE", "TENANT"),
        ("DELETE", "PACKAGE"),
        ("DISALLOW", "PROJECT"),
        ("DROP", "DATABASE"),
        ("DROP", "PACKAGE"),
        ("DROP", "PROJECT"),
        ("DROP", "QUOTA"),
        ("DROP", "RESOURCE"),
        ("DROP", "ROLE"),
        ("OWNER", "TO"),
        ("PUT", "POLICY"),
        ("REMOVE", "ACCOUNTPROVIDER"),
        ("REMOVE", "ACCOUNTPROVIDERS"),
        ("REMOVE", "ROLE"),
        ("REMOVE", "ROLES"),
        ("REMOVE", "TRUSTEDPROJECT"),
        ("REMOVE", "TRUSTEDPROJECTS"),
        ("REMOVE", "USER"),
        ("REMOVE", "USERS"),
        ("SET", "LABEL"),
        ("SET", "OWNER"),
        ("SET", "AUTHORIZATION"),
        ("SECURITY", "LABEL"),
        ("TO", "PACKAGE"),
        ("FROM", "PACKAGE"),
    }
    for index, pair in enumerate(zip(words, words[1:])):
        if pair not in blocked_pairs:
            continue
        if (
            operation in _FORCE_ALLOWED_DML_OPERATIONS
            and pair[0] == "SET"
            and index + 2 < len(words)
            and words[index + 2] == "="
        ):
            # UPDATE and MERGE assignments may legitimately target columns
            # named LABEL, OWNER, or AUTHORIZATION. The administrative
            # ``SET LABEL <level> ...`` command has no assignment operator.
            continue
        return True
    if any(
        token in {
            "CALL",
            "CLEAR",
            "EXECUTE",
            "GRANT",
            "INSTALL",
            "KILL",
            "PAI",
            "REVOKE",
            "SETPROJECT",
            "UNINSTALL",
            "USE",
        }
        for token in words[1:]
    ):
        return True
    if any(
        pair in {
            ("LIST", "ACCOUNTPROVIDERS"),
            ("LIST", "ROLES"),
            ("LIST", "TRUSTEDPROJECTS"),
            ("LIST", "USERS"),
        }
        for pair in zip(words, words[1:])
    ):
        return True
    if words[0] == "CREATE" and "SCHEMA" in words:
        schema_index = words.index("SCHEMA")
        return any(
            token == "AUTHORIZATION" and index > schema_index
            for index, token in enumerate(words)
        )
    return False


def _validate_sql_settings(
    settings: 'dict[str, str]',
    *,
    force: 'bool',
) -> 'None':
    """Validate leading SET hints independently of the SQL verb gate."""
    normalized_keys = {key.strip().lower() for key in settings}
    blocked = sorted(normalized_keys & _BLOCKED_SQL_SETTING_KEYS)
    if blocked:
        raise UnsupportedSqlOperationError(
            "SET parameter "
            f"'{blocked[0]}' controls project security or data masking and "
            "cannot be supplied through the public query path.",
            suggestion=(
                "Remove the parameter. Use a separately approved administrative "
                "workflow when a project security or masking policy must change."
            ),
        )

    if force:
        unreviewed = sorted(normalized_keys - _FORCE_ALLOWED_SQL_SETTING_KEYS)
        if unreviewed:
            raise UnsupportedSqlOperationError(
                "SET parameter "
                f"'{unreviewed[0]}' is not an audited execution hint for "
                "mutating SQL.",
                suggestion=(
                    "Remove the unreviewed SET parameter and submit only the exact "
                    "authorized DDL/DML statement, or use a dedicated approved "
                    "workflow for that execution control."
                ),
            )


def enforce_read_only_sql(sql: 'str', *, force: 'bool' = False) -> 'None':
    """Fail closed unless every statement is a proven read-only SQL shape.

    A mutation denylist is insufficient for an evolving SQL dialect: new
    commands such as procedure calls, dynamic SQL, or product-specific jobs
    could otherwise reach the service before this client recognizes them.
    ``force`` permits one recognized data-plane DDL/DML statement after the
    caller has obtained explicit user authorization. It never permits mixed
    executable statements or unknown and administrative SQL shapes.
    """
    from .setting_parser import SettingParser

    parsed = SettingParser.parse(sql)
    if parsed.errors:
        raise ValidationError(
            f"Invalid SET statement in SQL: {'; '.join(parsed.errors)}",
            suggestion="Check SET syntax: SET key=value; must end with semicolon.",
        )
    _validate_sql_settings(parsed.settings, force=force)
    remaining = parsed.remaining_query.strip()
    if not remaining:
        raise ValidationError(
            "SQL query is empty.",
            suggestion="Provide a SELECT statement via inline text, --file, or --stdin.",
        )

    statements = split_sql_statements(remaining)
    if force:
        if len(statements) != 1:
            raise ValidationError(
                "--force accepts exactly one executable SQL statement.",
                suggestion=(
                    "Separate the statements, verify the project, schema, target, "
                    "and effect, then execute only the exact statement the user authorized."
                ),
            )
        operation = detect_operation(statements[0])
        operations = executable_operations(statements[0])
        if not _force_statement_is_allowed(
            statements[0],
            leading_operation=operation,
            operations=operations,
        ):
            raise UnsupportedSqlOperationError(
                "--force accepts one recognized DDL/DML operation, not an unknown, "
                "procedural, permission, session-control, or multi-target SQL shape.",
                suggestion=(
                    "Use one directly inspectable DDL/DML statement, verify its "
                    "project, schema, target, and effect, and request a dedicated "
                    "approved workflow for other operation types."
                ),
            )
        return

    write_operations = known_write_operations(remaining)
    if write_operations:
        operation = write_operations[0]
        raise WriteOperationRequiresForceError(
            f"Write operation '{operation}' requires explicit authorization and --force.",
            suggestion=(
                "This error is not authorization. If the user explicitly requested "
                "this exact write, verify its project, schema, target, and effect, "
                "then retry that one statement with --force."
            ),
        )

    unsupported: list[str] = []
    for statement in statements:
        operation = detect_operation(statement)
        operations = executable_operations(statement)
        if operation in RESULT_OPERATIONS:
            continue
        if (
            operation == "WITH"
            and operations
            and all(item in RESULT_OPERATIONS for item in operations)
        ):
            continue
        # `SETPROJECT;` without an assignment is a MaxCompute inspection
        # statement. Assignment forms were classified as mutations above.
        if operation == "SETPROJECT" and re.fullmatch(
            r"(?is)\s*SETPROJECT\s*;?\s*", statement
        ):
            continue
        unsupported.append(operation)

    if unsupported:
        operation_list = ", ".join(dict.fromkeys(unsupported))
        raise UnsupportedSqlOperationError(
            f"SQL operation '{operation_list}' is not proven read-only and was blocked before submission.",
            suggestion=(
                "Use SELECT, SHOW, DESC, DESCRIBE, EXPLAIN, or a WITH query "
                "whose outer statement is read-only."
            ),
        )


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
    payload: dict[str, int] = {"o": offset}
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
    if session_id is not None and (
        not isinstance(session_id, int) or isinstance(session_id, bool) or session_id <= 0
    ):
        raise ValidationError(
            "The cursor contains an invalid session identifier.",
            suggestion="Use the `next_cursor` returned by the previous response.",
        )
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
        path = Path(file_path)
        try:
            return path.read_text(encoding="utf-8").strip()
        except FileNotFoundError as exc:
            raise ValidationError(
                f"SQL file not found: {file_path}",
                suggestion="Check the path; use an absolute path or one relative to the current working directory.",
            ) from exc
        except IsADirectoryError as exc:
            raise ValidationError(
                f"`{file_path}` is a directory, not a SQL file.",
                suggestion="Pass a path to a regular file containing the SQL query.",
            ) from exc
        except PermissionError as exc:
            raise ValidationError(
                f"Permission denied reading SQL file: {file_path}",
                suggestion="Adjust the file permissions and retry.",
            ) from exc
        except UnicodeDecodeError as exc:
            raise ValidationError(
                f"SQL file `{file_path}` is not valid UTF-8.",
                suggestion="Re-encode the file as UTF-8 and retry.",
            ) from exc
    if use_stdin:
        content = (stdin_text or "").strip()
        if not content:
            raise ValidationError("No SQL was read from stdin.")
        return content
    raise ValidationError("Unable to resolve SQL input.")


_LIMIT_RE = re.compile(r'\bLIMIT\s+\d+', re.IGNORECASE)


def sql_has_limit(sql: str) -> bool:
    """Check if SQL contains a LIMIT clause."""
    return bool(_LIMIT_RE.search(normalize_sql(sql)))


def short_json(value: 'Any') -> 'str':
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
