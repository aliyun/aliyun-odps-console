# Data Upload / Download Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `maxc data upload` and `maxc data download` commands that move CSV/TSV data between local files and an existing MaxCompute table or partition, using the PyODPS Tunnel API.

**Architecture:** Two new `DataMixin` methods (`upload_table`, `download_table`) wrap `odps.tunnel.TableTunnel` upload/download sessions. CSV parsing/serialization lives in pure helpers in `helpers.py`. Two new `MaxCApp` methods build the JSON envelope; two new CLI handlers wire argparse → app. Fail-fast on bad rows, target table must exist, single-thread session.

**Tech Stack:** Python stdlib `csv` (no new deps), PyODPS `odps.tunnel.TableTunnel`, existing `argparse` + envelope/exception machinery.

**Spec:** `docs/superpowers/specs/2026-05-08-data-upload-download-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `src/maxc_cli/exceptions.py` | modify | Add `CsvParseError(ValidationError)` with `line` / `column` context |
| `src/maxc_cli/helpers.py` | modify | Add `csv_supported_type`, `csv_parse_value`, `csv_format_value` |
| `src/maxc_cli/backend/data.py` | modify | Add `DataMixin.upload_table`, `DataMixin.download_table` |
| `src/maxc_cli/app.py` | modify | Add `MaxCApp.data_upload`, `MaxCApp.data_download` (envelope assembly) |
| `src/maxc_cli/cli.py` | modify | Register `data upload` / `data download` subparsers + handlers |
| `tests/test_helpers_csv.py` | create | Unit tests for the three CSV helpers |
| `tests/test_cli_mock.py` | modify | Extend `FakeODPS` with tunnel stub; add CLI-level upload/download tests |
| `tests/test_integration_real.py` | modify | Add roundtrip integration test |

`OdpsBackend`'s MRO already includes `DataMixin`; no changes to `backend/odps.py`. There is no `backend/base.py` in this codebase — the mixins themselves are the de facto interface, and we follow that pattern.

## Test Strategy

The existing `tests/test_cli_mock.py` drives all CLI commands through `run_json_command(tmp_path, config_path, argv)` against a real argparse → `MaxCApp` → `OdpsBackend(FakeODPS)` stack. Helper utilities `clear_odps_env`, `isolate_home`, `_make_config_with_odps`, and class-level `monkeypatch.setattr("odps.ODPS", FakeODPS)` are the canonical setup. We follow that pattern for upload/download instead of constructing `OdpsBackend` directly (which would require a fully-populated `MaxCConfig` and a working `ResolvedAuthConnection`).

The new integration in `FakeODPS` is a `tunnel` attribute exposing `FakeTunnel`. Production code reaches it as `self.client.tunnel`. Schema is supplied by monkey-patching `DataMixin.describe_table` per test (the simplest seam, given that `describe_table` lives in `MetaMixin` and would otherwise need a real ODPS table).

---

## Task 1: Add `CsvParseError` exception

**Files:**
- Modify: `src/maxc_cli/exceptions.py`
- Test: `tests/test_cli_mock.py`

- [ ] **Step 1: Write the failing test**

Add this test anywhere after the existing imports section in `tests/test_cli_mock.py`:

```python
def test_csv_parse_error_carries_line_and_column():
    from maxc_cli.exceptions import CsvParseError, ValidationError

    err = CsvParseError(
        "could not parse 'abc' as bigint",
        line=42,
        column="user_id",
        suggestion="check the row format",
    )
    assert isinstance(err, ValidationError)
    assert err.line == 42
    assert err.column == "user_id"
    assert err.error_code == "CSV_PARSE_ERROR"
    payload = err.to_payload().to_dict()
    assert payload["code"] == "CSV_PARSE_ERROR"
    assert payload["suggestion"] == "check the row format"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli_mock.py::test_csv_parse_error_carries_line_and_column -v`
Expected: FAIL with `ImportError: cannot import name 'CsvParseError'`.

- [ ] **Step 3: Implement `CsvParseError`**

Append to `src/maxc_cli/exceptions.py`:

```python
class CsvParseError(ValidationError):
    error_code = "CSV_PARSE_ERROR"
    recoverable = False

    def __init__(
        self,
        message: 'str',
        *,
        line: 'int | None' = None,
        column: 'str | None' = None,
        suggestion: 'str | None' = None,
    ) -> 'None':
        super().__init__(message, suggestion=suggestion)
        self.line = line
        self.column = column
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli_mock.py::test_csv_parse_error_carries_line_and_column -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maxc_cli/exceptions.py tests/test_cli_mock.py
git commit -m "feat(exceptions): add CsvParseError with line/column context"
```

---

## Task 2: CSV helper — `csv_supported_type`

**Files:**
- Modify: `src/maxc_cli/helpers.py`
- Test: `tests/test_helpers_csv.py` (create)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_helpers_csv.py`:

```python
"""Tests for CSV value helpers in helpers.py."""

import pytest


@pytest.mark.parametrize("odps_type", [
    "bigint", "int", "smallint", "tinyint",
    "double", "float", "decimal", "decimal(10,2)",
    "boolean", "string", "varchar(255)", "char(10)",
    "date", "datetime", "timestamp",
    "BIGINT", "STRING",
])
def test_csv_supported_type_returns_true_for_primitives(odps_type: str):
    from maxc_cli.helpers import csv_supported_type
    assert csv_supported_type(odps_type) is True


@pytest.mark.parametrize("odps_type", [
    "array<bigint>",
    "map<string,bigint>",
    "struct<a:bigint,b:string>",
    "ARRAY<STRING>",
])
def test_csv_supported_type_returns_false_for_complex_types(odps_type: str):
    from maxc_cli.helpers import csv_supported_type
    assert csv_supported_type(odps_type) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_helpers_csv.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `csv_supported_type`**

Append to `src/maxc_cli/helpers.py`:

```python
def csv_supported_type(odps_type: 'str') -> 'bool':
    """Return True if a column of this ODPS type can be round-tripped through CSV.

    Primitives are supported; complex types (array/map/struct) are not.
    Type parameters like decimal(10,2), varchar(255) are stripped before checking.
    """
    base = odps_type.strip().lower().split("(", 1)[0].split("<", 1)[0]
    return base in {
        "bigint", "int", "smallint", "tinyint",
        "double", "float", "decimal",
        "boolean",
        "string", "varchar", "char",
        "date", "datetime", "timestamp",
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_helpers_csv.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maxc_cli/helpers.py tests/test_helpers_csv.py
git commit -m "feat(helpers): add csv_supported_type for column-type gatekeeping"
```

---

## Task 3: CSV helper — `csv_parse_value`

**Files:**
- Modify: `src/maxc_cli/helpers.py`
- Test: `tests/test_helpers_csv.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_helpers_csv.py`:

```python
from datetime import date, datetime
from decimal import Decimal


@pytest.mark.parametrize("text,odps_type,expected", [
    ("123", "bigint", 123),
    ("-7", "int", -7),
    ("3.14", "double", 3.14),
    ("1.5", "decimal(10,2)", Decimal("1.5")),
    ("true", "boolean", True),
    ("False", "boolean", False),
    ("hello", "string", "hello"),
    ("", "string", ""),  # empty string stays empty
    ("2026-05-08", "date", date(2026, 5, 8)),
    ("2026-05-08 12:34:56", "datetime", datetime(2026, 5, 8, 12, 34, 56)),
])
def test_csv_parse_value_happy_path(text, odps_type, expected):
    from maxc_cli.helpers import csv_parse_value
    assert csv_parse_value(text, odps_type, null_marker=r"\N") == expected


def test_csv_parse_value_null_marker_returns_none():
    from maxc_cli.helpers import csv_parse_value
    assert csv_parse_value(r"\N", "bigint", null_marker=r"\N") is None
    assert csv_parse_value(r"\N", "string", null_marker=r"\N") is None


def test_csv_parse_value_empty_null_marker_treats_empty_as_null_for_non_string():
    from maxc_cli.helpers import csv_parse_value
    assert csv_parse_value("", "bigint", null_marker="") is None
    assert csv_parse_value("", "string", null_marker="") == ""


def test_csv_parse_value_raises_csv_parse_error_on_bad_int():
    from maxc_cli.helpers import csv_parse_value
    from maxc_cli.exceptions import CsvParseError
    with pytest.raises(CsvParseError) as exc:
        csv_parse_value("abc", "bigint", null_marker=r"\N")
    assert "bigint" in str(exc.value)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_helpers_csv.py -v -k csv_parse_value`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `csv_parse_value`**

Append to `src/maxc_cli/helpers.py` (the `from datetime import ...` may already be present; only add what's missing):

```python
from datetime import date as _date, datetime as _datetime  # add near other datetime imports if not present


def csv_parse_value(text: 'str', odps_type: 'str', *, null_marker: 'str') -> 'Any':
    """Parse a CSV cell into a typed Python value matching an ODPS column type.

    Raises CsvParseError if the text cannot be parsed.
    """
    from decimal import Decimal, InvalidOperation
    from .exceptions import CsvParseError

    base = odps_type.strip().lower().split("(", 1)[0].split("<", 1)[0]

    # NULL handling. Explicit marker matches first; for non-string types an
    # empty cell with null_marker="" is also NULL (matches odpscmd default).
    if text == null_marker and not (text == "" and base == "string"):
        return None
    if text == "" and null_marker == "" and base != "string":
        return None

    try:
        if base in {"bigint", "int", "smallint", "tinyint"}:
            return int(text)
        if base in {"double", "float"}:
            return float(text)
        if base == "decimal":
            return Decimal(text)
        if base == "boolean":
            lowered = text.strip().lower()
            if lowered in {"true", "1", "t", "yes"}:
                return True
            if lowered in {"false", "0", "f", "no"}:
                return False
            raise ValueError(f"invalid boolean: {text!r}")
        if base in {"string", "varchar", "char"}:
            return text
        if base == "date":
            return _datetime.strptime(text, "%Y-%m-%d").date()
        if base in {"datetime", "timestamp"}:
            normalized = text.replace("T", " ").split(".", 1)[0]
            return _datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S")
    except (ValueError, InvalidOperation) as exc:
        raise CsvParseError(
            f"could not parse {text!r} as {odps_type}: {exc}",
        ) from exc

    raise CsvParseError(f"unsupported ODPS type for CSV parse: {odps_type}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_helpers_csv.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maxc_cli/helpers.py tests/test_helpers_csv.py
git commit -m "feat(helpers): add csv_parse_value for typed CSV cell parsing"
```

---

## Task 4: CSV helper — `csv_format_value`

**Files:**
- Modify: `src/maxc_cli/helpers.py`
- Test: `tests/test_helpers_csv.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_helpers_csv.py`:

```python
@pytest.mark.parametrize("value,odps_type,expected", [
    (123, "bigint", "123"),
    (3.14, "double", "3.14"),
    (Decimal("1.5"), "decimal(10,2)", "1.5"),
    (True, "boolean", "true"),
    (False, "boolean", "false"),
    ("hello", "string", "hello"),
    (date(2026, 5, 8), "date", "2026-05-08"),
    (datetime(2026, 5, 8, 12, 34, 56), "datetime", "2026-05-08 12:34:56"),
])
def test_csv_format_value_happy_path(value, odps_type, expected):
    from maxc_cli.helpers import csv_format_value
    assert csv_format_value(value, odps_type, null_marker="") == expected


def test_csv_format_value_none_returns_null_marker():
    from maxc_cli.helpers import csv_format_value
    assert csv_format_value(None, "bigint", null_marker=r"\N") == r"\N"
    assert csv_format_value(None, "string", null_marker="") == ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_helpers_csv.py -v -k csv_format_value`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `csv_format_value`**

Append to `src/maxc_cli/helpers.py`:

```python
def csv_format_value(value: 'Any', odps_type: 'str', *, null_marker: 'str') -> 'str':
    """Render a Python value back into a CSV cell string for an ODPS column type."""
    if value is None:
        return null_marker
    base = odps_type.strip().lower().split("(", 1)[0].split("<", 1)[0]

    if base == "boolean":
        return "true" if value else "false"
    if base == "date":
        if isinstance(value, _datetime):
            return value.date().isoformat()
        if isinstance(value, _date):
            return value.isoformat()
        return str(value)
    if base in {"datetime", "timestamp"}:
        if isinstance(value, _datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)
    return str(value)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_helpers_csv.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maxc_cli/helpers.py tests/test_helpers_csv.py
git commit -m "feat(helpers): add csv_format_value for typed CSV cell rendering"
```

---

## Task 5: Tunnel test doubles + `_install_data_doubles` helper

This task adds reusable test infrastructure. No production change yet; commit on its own so subsequent tasks have a clean dependency.

**Files:**
- Modify: `tests/test_cli_mock.py`

- [ ] **Step 1: Add tunnel stub classes after `FakeODPS`**

Insert this block in `tests/test_cli_mock.py` immediately after the `FakeODPS` class definition (before `BrokenWhoamiODPS`):

```python
class _FakeRecord(dict):
    """Behaves like an odps Record: indexable by column name."""


class FakeUploadSession:
    def __init__(self, table, partition, overwrite, store):
        self.table = table
        self.partition = partition
        self.overwrite = overwrite
        self._store = store
        self.committed_blocks: 'list[int]' = []
        self.aborted = False

    def new_record(self):
        return _FakeRecord()

    def open_record_writer(self, block_id: int):
        records: 'list[dict]' = []
        self._store.setdefault((self.table, self.partition), []).append(
            (block_id, records, self.overwrite)
        )

        class _Writer:
            def write(self_inner, record):
                records.append(dict(record))

            def close(self_inner):
                pass

        return _Writer()

    def commit(self, blocks):
        self.committed_blocks = list(blocks)

    def abort(self):
        self.aborted = True


class FakeDownloadSession:
    def __init__(self, table, partition, rows):
        self.table = table
        self.partition = partition
        self._rows = list(rows)
        self.count = len(self._rows)

    def open_record_reader(self, start: int, count: int):
        return iter(self._rows[start:start + count])


class FakeTunnel:
    """Stub for odps.tunnel.TableTunnel.

    Class-level `last_upload_session` and `download_rows` allow tests to
    inspect/seed state across the FakeODPS instance the CLI constructs.
    """

    last_upload_session: 'FakeUploadSession | None' = None
    download_rows: 'dict[tuple, list[_FakeRecord]]' = {}

    def __init__(self):
        self.upload_store: 'dict[tuple, list]' = {}

    def create_upload_session(self, table, partition_spec=None, overwrite=False):
        sess = FakeUploadSession(table, partition_spec, overwrite, self.upload_store)
        FakeTunnel.last_upload_session = sess
        return sess

    def create_download_session(self, table, partition_spec=None):
        rows = FakeTunnel.download_rows.get((table, partition_spec), [])
        return FakeDownloadSession(table, partition_spec, rows)
```

Then extend `FakeODPS.__init__` to expose a tunnel attribute. Find this line:

```python
        self.tunnel_endpoint = tunnel_endpoint
```

and add immediately after it:

```python
        self.tunnel = FakeTunnel()
```

- [ ] **Step 2: Add the `_install_data_doubles` helper**

Append at the bottom of `tests/test_cli_mock.py` (after the last existing helper):

```python
def _install_data_doubles(
    monkeypatch,
    *,
    columns: 'list[tuple[str, str]]',
    partition_columns: 'list[tuple[str, str]]' = (),
    download_rows: 'list[dict] | None' = None,
    download_table: 'str | None' = None,
    download_partition: 'str | None' = None,
):
    """Install FakeODPS + a fixed describe_table + optional download seed.

    Resets FakeTunnel class state so tests do not leak into each other.
    """
    import odps
    from maxc_cli.backend.data import DataMixin
    from maxc_cli.config import TableColumn, TableDefinition

    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    table_def = TableDefinition(
        name="proj.sch.tbl",
        description="",
        columns=[TableColumn(name=n, type=t) for n, t in columns],
        partition_columns=[TableColumn(name=n, type=t) for n, t in partition_columns],
    )
    monkeypatch.setattr(
        DataMixin, "describe_table",
        lambda self, name, project=None: table_def,
    )

    # Reset class-level FakeTunnel state.
    FakeTunnel.last_upload_session = None
    FakeTunnel.download_rows = {}
    if download_rows is not None:
        key = (download_table or table_def.name, download_partition)
        FakeTunnel.download_rows[key] = [_FakeRecord(r) for r in download_rows]
```

- [ ] **Step 3: Run the full mock suite to confirm no regressions**

Run: `pytest tests/test_cli_mock.py -q`
Expected: all existing tests still pass (no behavior depends on `tunnel` yet).

- [ ] **Step 4: Commit**

```bash
git add tests/test_cli_mock.py
git commit -m "test: add Fake tunnel sessions and _install_data_doubles helper"
```

---

## Task 6: Wire `data upload` (backend + app + CLI)

This task implements all three layers of `data upload` together because the test harness exercises everything through `run_json_command`. TDD here means: fail at CLI level → wire CLI → wire app → wire backend → green.

**Files:**
- Modify: `src/maxc_cli/backend/data.py`
- Modify: `src/maxc_cli/app.py`
- Modify: `src/maxc_cli/cli.py`
- Test: `tests/test_cli_mock.py`

- [ ] **Step 1: Write the failing happy-path test**

Append to `tests/test_cli_mock.py`:

```python
def test_cli_data_upload_appends_csv_to_partitioned_table(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
        partition_columns=[("ds", "string")],
    )

    csv_path = tmp_path / "in.csv"
    csv_path.write_text("user_id,name\n1,alice\n2,bob\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path),
         "--partition", "ds=20260508",
         "--json"],
    )

    assert code == 0, payload
    assert payload["command"] == "data.upload"
    assert payload["status"] == "success"
    assert payload["data"]["rows_written"] == 2
    assert payload["data"]["table"] == "proj.sch.tbl"
    assert payload["data"]["applied_partition"] == "ds=20260508"
    assert payload["data"]["overwrite"] is False
    assert payload["data"]["blocks"] == 1

    sess = FakeTunnel.last_upload_session
    assert sess.partition == "ds=20260508"
    assert sess.overwrite is False
    assert sess.committed_blocks == [0]
    [(_block_id, recs, _ow)] = sess._store[("proj.sch.tbl", "ds=20260508")]
    assert recs == [
        {"user_id": 1, "name": "alice"},
        {"user_id": 2, "name": "bob"},
    ]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_cli_mock.py::test_cli_data_upload_appends_csv_to_partitioned_table -v`
Expected: FAIL with argparse error like `invalid choice: 'upload'`.

- [ ] **Step 3: Add the CLI subparser**

In `src/maxc_cli/cli.py`, find the `data_subparsers` block (around line 270, immediately after `data_profile.set_defaults(handler=_handle_data_profile)`). Append:

```python
    data_upload = data_subparsers.add_parser("upload", help="Upload a CSV/TSV file into a table")
    data_upload.add_argument("table_name", help="Table name (schema.table or table)")
    data_upload.add_argument("--file", required=True, help="Path to local CSV/TSV file")
    data_upload.add_argument("--partition", help="Partition spec, e.g. ds=20260508")
    data_upload.add_argument("--overwrite", action="store_true",
                             help="Use INSERT OVERWRITE semantics for the partition/table")
    data_upload.add_argument("--delimiter", default=",", help="Field delimiter (default: ,)")
    data_upload.add_argument("--no-header", dest="has_header", action="store_false",
                             default=True, help="Treat the first row as data, not header")
    data_upload.add_argument("--null-marker", default=r"\N",
                             help=r"Token interpreted as SQL NULL (default: \N)")
    data_upload.add_argument("--block-size", type=int, default=10000,
                             help="Rows per Tunnel block (default: 10000)")
    data_upload.add_argument("--project", help="Target MaxCompute project")
    data_upload.add_argument("--json", action="store_true", help="Output as JSON envelope")
    data_upload.set_defaults(handler=_handle_data_upload)
```

- [ ] **Step 4: Add the CLI handler**

Add to `src/maxc_cli/cli.py`, immediately after `_handle_data_profile`:

```python
def _handle_data_upload(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.data_upload(
        args.table_name,
        args.file,
        partition=args.partition,
        overwrite=args.overwrite,
        delimiter=args.delimiter,
        has_header=args.has_header,
        null_marker=args.null_marker,
        block_size=args.block_size,
        project=args.project,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
```

- [ ] **Step 5: Add the app method**

Add to `MaxCApp` in `src/maxc_cli/app.py`, immediately after `data_profile`:

```python
def data_upload(
    self,
    table_name: 'str',
    file_path: 'str',
    *,
    partition: 'str | None' = None,
    overwrite: 'bool' = False,
    delimiter: 'str' = ",",
    has_header: 'bool' = True,
    null_marker: 'str' = r"\N",
    block_size: 'int' = 10000,
    project: 'str | None' = None,
) -> 'Envelope':
    target_project = project or self.config.default_project
    result = self.backend.upload_table(
        table_name, file_path,
        partition=partition, overwrite=overwrite,
        delimiter=delimiter, has_header=has_header,
        null_marker=null_marker, block_size=block_size,
        project=project,
    )
    metadata = {
        "project": target_project,
        "requested_partition": partition,
        "delimiter": delimiter,
        "block_size": block_size,
    }
    envelope = Envelope(
        command="data.upload",
        status="success",
        data=result,
        metadata=metadata,
        agent_hints=AgentHints(
            actions=[
                action("data.sample", data=result, metadata=metadata),
            ],
            warnings=result.get("warnings", []),
        ),
    )
    self.log("data.upload", envelope.status, envelope.metadata)
    return envelope
```

- [ ] **Step 6: Add the backend mixin method**

Add to `DataMixin` in `src/maxc_cli/backend/data.py`:

```python
def upload_table(
    self,
    table_name: 'str',
    file_path: 'str',
    *,
    partition: 'str | None' = None,
    overwrite: 'bool' = False,
    delimiter: 'str' = ",",
    has_header: 'bool' = True,
    null_marker: 'str' = r"\N",
    block_size: 'int' = 10000,
    project: 'str | None' = None,
) -> 'dict[str, Any]':
    """Upload a CSV/TSV file into an existing table or partition via Tunnel."""
    import csv
    import os

    from ..exceptions import CsvParseError, ValidationError
    from ..helpers import csv_parse_value, csv_supported_type, translate_odps_error

    if block_size < 1:
        raise ValidationError("`block_size` must be >= 1.")

    definition = self.describe_table(table_name, project=project)
    partition_columns = {c.name for c in definition.partition_columns}
    data_columns = [c for c in definition.columns if c.name not in partition_columns]
    name_to_type = {c.name: c.type for c in data_columns}

    if definition.partition_columns and not partition:
        keys = ", ".join(c.name for c in definition.partition_columns)
        raise ValidationError(
            f"Table `{definition.name}` is partitioned ({keys}); --partition is required.",
            suggestion=f"Pass --partition <{keys}=...>.",
        )
    if partition and not definition.partition_columns:
        raise ValidationError(
            f"Table `{definition.name}` is not partitioned; --partition is not allowed.",
        )

    unsupported = [c.name for c in data_columns if not csv_supported_type(c.type)]
    if unsupported:
        raise ValidationError(
            f"Columns {unsupported} have complex types not supported by CSV upload.",
            suggestion="Use INSERT ... SELECT via `maxc query` instead.",
        )

    bytes_read = os.path.getsize(file_path)
    block_ids: 'list[int]' = []
    rows_written = 0

    upload_session = self.client.tunnel.create_upload_session(
        definition.name, partition_spec=partition, overwrite=overwrite,
    )

    try:
        with open(file_path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter)

            if has_header:
                try:
                    header = next(reader)
                except StopIteration:
                    header = []
                column_order = _resolve_header_mapping(header, data_columns)
            else:
                column_order = [c.name for c in data_columns]

            current_block = 0
            writer = upload_session.open_record_writer(current_block)
            block_ids.append(current_block)
            in_block = 0
            line_no = 1 if not has_header else 2

            for row in reader:
                if not has_header and len(row) != len(column_order):
                    raise CsvParseError(
                        f"expected {len(column_order)} columns, got {len(row)}",
                        line=line_no,
                    )
                if has_header and len(row) < len(column_order):
                    raise CsvParseError(
                        f"row has {len(row)} columns, header has {len(column_order)}",
                        line=line_no,
                    )
                record = upload_session.new_record()
                for col_name, cell in zip(column_order, row):
                    try:
                        record[col_name] = csv_parse_value(
                            cell, name_to_type[col_name], null_marker=null_marker,
                        )
                    except CsvParseError as exc:
                        exc.line = line_no
                        exc.column = col_name
                        raise
                writer.write(record)
                rows_written += 1
                in_block += 1
                line_no += 1
                if in_block >= block_size:
                    writer.close()
                    current_block += 1
                    writer = upload_session.open_record_writer(current_block)
                    block_ids.append(current_block)
                    in_block = 0

            writer.close()
    except CsvParseError:
        upload_session.abort()
        raise
    except Exception as exc:
        upload_session.abort()
        raise translate_odps_error(exc) from exc

    upload_session.commit(block_ids)

    return {
        "table": definition.name,
        "applied_partition": partition,
        "rows_written": rows_written,
        "bytes_read": bytes_read,
        "blocks": len(block_ids),
        "overwrite": overwrite,
        "warnings": [],
    }
```

Add this module-level helper at the bottom of `src/maxc_cli/backend/data.py`:

```python
def _resolve_header_mapping(header: 'list[str]', data_columns: 'list') -> 'list[str]':
    from ..exceptions import ValidationError
    expected = {c.name for c in data_columns}
    seen = set(header)
    missing = expected - seen
    if missing:
        raise ValidationError(
            f"CSV header missing required columns: {sorted(missing)}",
        )
    return [name for name in header if name in expected]
```

- [ ] **Step 7: Run the happy-path test**

Run: `pytest tests/test_cli_mock.py::test_cli_data_upload_appends_csv_to_partitioned_table -v`
Expected: PASS.

- [ ] **Step 8: Add the rest of the upload tests**

Append to `tests/test_cli_mock.py`:

```python
def test_cli_data_upload_overwrite_partition(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("v", "bigint")],
        partition_columns=[("ds", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n42\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path),
         "--partition", "ds=20260508",
         "--overwrite", "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["overwrite"] is True
    assert FakeTunnel.last_upload_session.overwrite is True


def test_cli_data_upload_rejects_missing_partition_for_partitioned_table(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("v", "bigint")],
        partition_columns=[("ds", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n1\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code != 0
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "partition" in payload["error"]["message"].lower()


def test_cli_data_upload_rejects_unsupported_complex_type(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("a", "array<bigint>")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("a\n1\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code != 0
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "complex types" in payload["error"]["message"]


def test_cli_data_upload_fail_fast_on_bad_row_aborts_session(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(monkeypatch, columns=[("v", "bigint")])
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n1\nabc\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code != 0
    assert payload["error"]["code"] == "CSV_PARSE_ERROR"
    sess = FakeTunnel.last_upload_session
    assert sess.aborted is True
    assert sess.committed_blocks == []


def test_cli_data_upload_no_header_uses_ordinal_mapping(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
    )
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("1,alice\n2,bob\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--no-header", "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["rows_written"] == 2


def test_cli_data_upload_empty_file_commits_zero_rows(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(monkeypatch, columns=[("v", "bigint")])
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n", encoding="utf-8")
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "upload", "proj.sch.tbl",
         "--file", str(csv_path), "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["rows_written"] == 0
    assert payload["data"]["blocks"] == 1
    assert FakeTunnel.last_upload_session.committed_blocks == [0]
```

- [ ] **Step 9: Run all upload tests**

Run: `pytest tests/test_cli_mock.py -v -k data_upload`
Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git add src/maxc_cli/backend/data.py src/maxc_cli/app.py src/maxc_cli/cli.py tests/test_cli_mock.py
git commit -m "feat: add data upload command (backend + app + CLI)"
```

---

## Task 7: Wire `data download` (backend + app + CLI)

Same shape as Task 6.

**Files:**
- Modify: `src/maxc_cli/backend/data.py`
- Modify: `src/maxc_cli/app.py`
- Modify: `src/maxc_cli/cli.py`
- Test: `tests/test_cli_mock.py`

- [ ] **Step 1: Write the failing happy-path test**

Append to `tests/test_cli_mock.py`:

```python
def test_cli_data_download_writes_full_partition(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("user_id", "bigint"), ("name", "string")],
        partition_columns=[("ds", "string")],
        download_rows=[{"user_id": 1, "name": "alice"}, {"user_id": 2, "name": "bob"}],
        download_partition="ds=20260508",
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--partition", "ds=20260508",
         "--json"],
    )

    assert code == 0, payload
    assert payload["command"] == "data.download"
    assert payload["data"]["rows_written"] == 2
    assert payload["data"]["truncated"] is False
    assert payload["data"]["columns"] == ["user_id", "name"]
    assert payload["data"]["applied_partition"] == "ds=20260508"
    assert out.read_text(encoding="utf-8") == "user_id,name\n1,alice\n2,bob\n"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_cli_mock.py::test_cli_data_download_writes_full_partition -v`
Expected: FAIL with `invalid choice: 'download'`.

- [ ] **Step 3: Add the CLI subparser**

In `src/maxc_cli/cli.py`, in the `data_subparsers` block (right after the upload parser added in Task 6), append:

```python
    data_download = data_subparsers.add_parser("download", help="Download a table/partition to a CSV/TSV file")
    data_download.add_argument("table_name", help="Table name (schema.table or table)")
    data_download.add_argument("--output", required=True, help="Path to local CSV/TSV file to write")
    data_download.add_argument("--partition", help="Partition spec, e.g. ds=20260508")
    data_download.add_argument("--columns", help="Comma-separated subset of columns")
    data_download.add_argument("--limit", type=int, help="Maximum rows to download")
    data_download.add_argument("--delimiter", default=",", help="Field delimiter (default: ,)")
    data_download.add_argument("--no-header", dest="write_header", action="store_false",
                               default=True, help="Suppress header row in output")
    data_download.add_argument("--null-marker", default="",
                               help='Token written for SQL NULL (default: empty string)')
    data_download.add_argument("--project", help="Target MaxCompute project")
    data_download.add_argument("--json", action="store_true", help="Output as JSON envelope")
    data_download.set_defaults(handler=_handle_data_download)
```

- [ ] **Step 4: Add the CLI handler**

Add to `src/maxc_cli/cli.py`, immediately after `_handle_data_upload`:

```python
def _handle_data_download(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    columns = _csv_arg_list(args.columns)
    envelope = app.data_download(
        args.table_name,
        args.output,
        partition=args.partition,
        columns=columns or None,
        limit=args.limit,
        delimiter=args.delimiter,
        write_header=args.write_header,
        null_marker=args.null_marker,
        project=args.project,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
```

- [ ] **Step 5: Add the app method**

Add to `MaxCApp` in `src/maxc_cli/app.py`, immediately after `data_upload`:

```python
def data_download(
    self,
    table_name: 'str',
    output_path: 'str',
    *,
    partition: 'str | None' = None,
    columns: 'list[str] | None' = None,
    limit: 'int | None' = None,
    delimiter: 'str' = ",",
    write_header: 'bool' = True,
    null_marker: 'str' = "",
    project: 'str | None' = None,
) -> 'Envelope':
    target_project = project or self.config.default_project
    result = self.backend.download_table(
        table_name, output_path,
        partition=partition, columns=columns, limit=limit,
        delimiter=delimiter, write_header=write_header,
        null_marker=null_marker, project=project,
    )
    metadata = {
        "project": target_project,
        "requested_partition": partition,
        "requested_columns": columns or [],
        "requested_limit": limit,
        "delimiter": delimiter,
    }
    envelope = Envelope(
        command="data.download",
        status="success",
        data=result,
        metadata=metadata,
        agent_hints=AgentHints(
            actions=[],
            warnings=result.get("warnings", []),
        ),
    )
    self.log("data.download", envelope.status, envelope.metadata)
    return envelope
```

- [ ] **Step 6: Add the backend mixin method**

Add to `DataMixin` in `src/maxc_cli/backend/data.py`:

```python
def download_table(
    self,
    table_name: 'str',
    output_path: 'str',
    *,
    partition: 'str | None' = None,
    columns: 'list[str] | None' = None,
    limit: 'int | None' = None,
    delimiter: 'str' = ",",
    write_header: 'bool' = True,
    null_marker: 'str' = "",
    project: 'str | None' = None,
) -> 'dict[str, Any]':
    """Download a table or partition to a local CSV/TSV file via Tunnel."""
    import csv
    import os

    from ..exceptions import ValidationError
    from ..helpers import csv_format_value, translate_odps_error

    if limit is not None and limit < 1:
        raise ValidationError("`limit` must be >= 1.")

    definition = self.describe_table(table_name, project=project)
    partition_columns = {c.name for c in definition.partition_columns}
    data_columns = [c for c in definition.columns if c.name not in partition_columns]
    name_to_type = {c.name: c.type for c in data_columns}

    if definition.partition_columns and not partition:
        keys = ", ".join(c.name for c in definition.partition_columns)
        raise ValidationError(
            f"Table `{definition.name}` is partitioned ({keys}); --partition is required.",
            suggestion=f"Pass --partition <{keys}=...>.",
        )
    if partition and not definition.partition_columns:
        raise ValidationError(
            f"Table `{definition.name}` is not partitioned; --partition is not allowed.",
        )

    if columns:
        unknown = [c for c in columns if c not in name_to_type]
        if unknown:
            raise ValidationError(f"Unknown columns: {unknown}")
        selected = list(columns)
    else:
        selected = [c.name for c in data_columns]

    try:
        session = self.client.tunnel.create_download_session(
            definition.name, partition_spec=partition,
        )
        total = session.count
        count = min(total, limit) if limit is not None else total

        rows_written = 0
        try:
            with open(output_path, "w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh, delimiter=delimiter)
                if write_header:
                    writer.writerow(selected)
                for record in session.open_record_reader(0, count):
                    writer.writerow([
                        csv_format_value(
                            record[col], name_to_type[col],
                            null_marker=null_marker,
                        )
                        for col in selected
                    ])
                    rows_written += 1
        except Exception:
            try:
                os.remove(output_path)
            except OSError:
                pass
            raise
    except ValidationError:
        raise
    except Exception as exc:
        raise translate_odps_error(exc) from exc

    bytes_written = os.path.getsize(output_path)
    truncated = limit is not None and limit < total
    warnings: 'list[str]' = []
    if truncated:
        warnings.append(
            f"--limit reached; output may be partial (session has {total} rows)."
        )

    return {
        "table": definition.name,
        "applied_partition": partition,
        "output_path": os.path.abspath(output_path),
        "rows_written": rows_written,
        "bytes_written": bytes_written,
        "columns": selected,
        "truncated": truncated,
        "warnings": warnings,
    }
```

- [ ] **Step 7: Run the happy-path test**

Run: `pytest tests/test_cli_mock.py::test_cli_data_download_writes_full_partition -v`
Expected: PASS.

- [ ] **Step 8: Add the rest of the download tests**

Append to `tests/test_cli_mock.py`:

```python
def test_cli_data_download_respects_limit_and_marks_truncated(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("v", "bigint")],
        partition_columns=[("ds", "string")],
        download_rows=[{"v": i} for i in range(10)],
        download_partition="ds=1",
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--partition", "ds=1",
         "--limit", "3", "--json"],
    )
    assert code == 0, payload
    assert payload["data"]["rows_written"] == 3
    assert payload["data"]["truncated"] is True
    assert "limit reached" in payload["data"]["warnings"][0]


def test_cli_data_download_columns_subset_in_requested_order(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("a", "bigint"), ("b", "string"), ("c", "double")],
        download_rows=[{"a": 1, "b": "x", "c": 1.5}],
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, _payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--columns", "c,a", "--json"],
    )
    assert code == 0
    assert out.read_text(encoding="utf-8") == "c,a\n1.5,1\n"


def test_cli_data_download_rejects_unknown_column(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(monkeypatch, columns=[("a", "bigint")])
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--columns", "nope", "--json"],
    )
    assert code != 0
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "Unknown columns" in payload["error"]["message"]


def test_cli_data_download_null_marker_renders_none(tmp_path, monkeypatch):
    clear_odps_env(monkeypatch); isolate_home(monkeypatch, tmp_path)
    _install_data_doubles(
        monkeypatch,
        columns=[("a", "bigint"), ("b", "string")],
        download_rows=[{"a": None, "b": None}],
    )
    out = tmp_path / "out.csv"
    config_path = _make_config_with_odps(tmp_path)

    code, _payload, _ = run_json_command(
        tmp_path, config_path,
        ["data", "download", "proj.sch.tbl",
         "--output", str(out),
         "--null-marker", r"\N", "--json"],
    )
    assert code == 0
    assert out.read_text(encoding="utf-8") == "a,b\n\\N,\\N\n"
```

- [ ] **Step 9: Run all download tests + full mock suite for regressions**

Run: `pytest tests/test_cli_mock.py -v -k data_download`
Expected: all PASS.

Run: `pytest tests/test_cli_mock.py -q`
Expected: full file green.

- [ ] **Step 10: Commit**

```bash
git add src/maxc_cli/backend/data.py src/maxc_cli/app.py src/maxc_cli/cli.py tests/test_cli_mock.py
git commit -m "feat: add data download command (backend + app + CLI)"
```

---

## Task 8: Integration test (real backend, skip-when-no-creds)

**Files:**
- Modify: `tests/test_integration_real.py`

- [ ] **Step 1: Inspect the existing integration test file**

Run: `grep -n "skip\|fixture\|^def test\|MaxCApp\|load_config" tests/test_integration_real.py | head -40`

Note:
- The existing skip pattern (decorator name and what it gates on, e.g. credential env vars).
- How an `app` is constructed in existing tests — likely `MaxCApp(cwd=tmp_path, config_path=...)` because `MaxCApp.__init__` is keyword-only with `cwd: Path`, `config_path: Path | None = None`, `load_backend: bool = True`. Mirror this pattern exactly; do NOT use a positional config object.

- [ ] **Step 2: Write the roundtrip integration test**

Append to `tests/test_integration_real.py`. Follow the surrounding tests' patterns for skip decorator and app construction:

```python
def test_data_upload_download_roundtrip(tmp_path, monkeypatch):
    """Upload a CSV and download it back; assert row equality.

    Requires:
      - real ODPS credentials in the environment (the existing skip
        decorator on this file already gates this).
      - MAXC_TEST_TABLE: writable test table in the configured project.
      - MAXC_TEST_PARTITION: optional partition spec; required if the
        test table is partitioned.
    """
    import csv
    import os
    table = os.environ.get("MAXC_TEST_TABLE")
    if not table:
        pytest.skip("MAXC_TEST_TABLE not set")
    partition = os.environ.get("MAXC_TEST_PARTITION")  # may be None

    # Construct an app the same way the rest of this file does. If a fixture
    # like `real_app` exists, prefer it; otherwise use MaxCApp(cwd=..., config_path=...)
    # with a config file populated from env vars.
    from maxc_cli.app import MaxCApp
    app = MaxCApp(cwd=tmp_path)  # picks up env vars via load_config

    src = tmp_path / "in.csv"
    src.write_text("user_id,name\n1,alice\n2,bob\n", encoding="utf-8")

    upload_env = app.data_upload(
        table, str(src),
        partition=partition, overwrite=True,
        delimiter=",", has_header=True, null_marker=r"\N",
        block_size=1000, project=None,
    )
    assert upload_env.status == "success"
    assert upload_env.data["rows_written"] == 2

    out = tmp_path / "out.csv"
    download_env = app.data_download(
        table, str(out),
        partition=partition, columns=None, limit=None,
        delimiter=",", write_header=True, null_marker="",
        project=None,
    )
    assert download_env.status == "success"
    assert download_env.data["rows_written"] == 2

    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert {r["name"] for r in rows} == {"alice", "bob"}
```

If the existing integration tests in this file use a `@requires_real_odps`-style decorator (or `@pytest.mark.skipif(...)`), apply the same decorator to this test.

- [ ] **Step 3: Run the integration test (will be skipped without env vars)**

Run: `pytest tests/test_integration_real.py::test_data_upload_download_roundtrip -v`
Expected: SKIP (because env vars are not set in CI). If `MAXC_TEST_TABLE` and credentials are set locally, expect PASS.

- [ ] **Step 4: Run the whole suite once more**

Run: `pytest -q`
Expected: all PASS / SKIP, no failures.

- [ ] **Step 5: Commit**

```bash
git add tests/test_integration_real.py
git commit -m "test: add data upload/download roundtrip integration test"
```

---

## Done criteria

- `pytest -q` is green (with the integration test skipped when creds absent).
- `maxc data upload --help` and `maxc data download --help` show the documented flags.
- A manual exercise against a real test table round-trips a small CSV.
