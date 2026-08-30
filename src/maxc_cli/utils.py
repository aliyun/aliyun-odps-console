
import base64
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .exceptions import ValidationError

SQL_COMMENT_RE = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)
_CODE_BLOCK_START_RE = re.compile(r"(?i)#CODE\b")
_CODE_BLOCK_END_RE = re.compile(r"(?i)#END\s+CODE\b")
TABLE_NAME_RE = re.compile(
    r"(?i)\b(?:from|join|into|update|table)\s+([a-zA-Z0-9_][\w.]*)"
)
WRITE_OPERATIONS = frozenset({
    "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE",
    "CREATE", "DROP", "ALTER", "RENAME", "TRUNCATE", "CLONE", "RESTORE",
    "KILL", "ALIAS", "MSCK", "UNLOAD",
    "GRANT", "REVOKE",
    "ANALYZE", "OPTIMIZE", "COMPACT", "VACUUM",
    "USE", "ADD", "REMOVE", "PURGE", "INSTALL", "UNINSTALL", "LOAD",
})
RESULT_OPERATIONS = frozenset({"SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN"})
_CONTROL_FLOW_OPERATIONS = frozenset({
    "BEGIN", "DO", "ELSE", "ELSEIF", "END", "FOR", "IF", "LOOP", "THEN", "WHILE",
})
_SPECIAL_WRITE_OPERATIONS = frozenset({"SETPROJECT"})
_SCRIPT_OPERATION = "SCRIPT"


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


def split_sql_statements(sql: 'str') -> 'list[str]':
    """Split top-level statements without treating quoted semicolons as separators."""
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    in_code_block = False
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
        if char == ";":
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


def _unquoted_sql_text(sql: 'str') -> 'str':
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
        if char in {"'", '"', "`"}:
            quote = char
            visible.append(" ")
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


def _skip_token_group(tokens: 'list[tuple[str, int]]', index: int) -> int | None:
    """Return the index after a parenthesized token group, or ``None``."""
    if index >= len(tokens) or tokens[index][0] != "(":
        return None
    group_depth = tokens[index][1]
    for current in range(index + 1, len(tokens)):
        if tokens[current] == (")", group_depth):
            return current + 1
    return None


def _with_main_operation(
    tokens: 'list[tuple[str, int]]',
    start: int,
) -> 'str | None':
    """Return the outer operation following a well-formed WITH clause."""
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
        return _with_main_operation(tokens, index)
    if operation == "FROM":
        multi_insert = _multi_insert_operations(
            tokens[index:],
            base_depth=base_depth,
        )
        return multi_insert[0] if multi_insert else None
    if operation in RESULT_OPERATIONS or operation in WRITE_OPERATIONS:
        return operation
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
        main_operation = _with_main_operation(tokens, index)
        return [main_operation] if main_operation else []
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
