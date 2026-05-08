# Design: `data upload` & `data download` Commands

**Date:** 2026-05-08
**Status:** Draft, pending implementation plan
**Owner:** dingxin

## 1. Background & Goals

`maxc-cli` already exposes `data sample` and `data profile` for inspecting
table contents, but has no way to move bulk data between local files and
MaxCompute tables. Agents and operators currently fall back to `odpscmd
tunnel upload/download` or hand-written PyODPS scripts.

This spec adds two commands:

- `maxc data upload <table>` — load a local CSV/TSV into an existing table
  (or one of its partitions).
- `maxc data download <table>` — dump a table or partition to a local CSV/TSV.

### Goals

- Cover the dominant use cases agreed during brainstorming:
  - Small files (<100 MB) for quick imports/exports.
  - Medium ETL (100 MB ~ a few GB) on a single thread.
  - Agent-driven small-batch round-trips that complement `data sample`.
- Reuse the existing CLI / envelope / mixin patterns so the surface stays
  coherent with `data sample`, `data profile`, etc.
- Preserve agent safety: refuse risky implicit behaviour (auto-create tables,
  unbounded full-table downloads on partitioned tables, silent row drops).

### Non-goals (v1)

- Parallel/multi-thread Tunnel sessions, resumable transfers,
  multi-partition (dynamic) writes — single session is enough for the
  targeted data sizes.
- JSONL / Parquet input/output formats — CSV/TSV (delimiter-only difference)
  is the v1 surface; other formats can be added later behind `--format`.
- `stdin`/`stdout` streaming or inlining row data into the JSON envelope —
  `data sample` already covers the inline-JSON case.
- Auto-create destination table — the table must exist; agents are expected
  to call `meta describe` or write SQL if they need a new table.
- Skip-bad-row mode with side-channel error files — fail-fast only in v1.

## 2. CLI Surface

```
maxc data upload <table>
    --file <path>                       # required
    [--partition ds=20260101]           # required iff table is partitioned
    [--overwrite]                       # INSERT OVERWRITE semantics
    [--delimiter ,]                     # default ","; use $'\t' for TSV
    [--no-header]                       # treat first row as data, map by ordinal
    [--null-marker \N]                  # token interpreted as SQL NULL
    [--block-size 10000]                # rows per tunnel block flush
    [--project <p>]
    [--json]

maxc data download <table>
    --output <path>                     # required
    [--partition ds=20260101]           # required iff table is partitioned
    [--columns col1,col2]               # default: all columns in schema order
    [--limit N]                         # default: full partition / table
    [--delimiter ,]
    [--no-header]                       # suppress header row in output
    [--null-marker ""]                  # token written for SQL NULL
    [--project <p>]
    [--json]
```

### CLI Conventions

- Wired into the existing `data` subparser group in `cli.py:build_parser`,
  next to `sample` / `profile`.
- `--json` toggles the structured envelope output, matching every other
  command.
- `<table>` accepts the same `[project.][schema.]table` form as
  `meta describe` and `data sample`.
- All flag names mirror odpscmd's `tunnel upload/download` where possible
  (`--delimiter`, `--null-marker`, `--block-size`) so users coming from
  odpscmd are not surprised.

### Validation rules surfaced as `ValidationError`

- `--partition` is required for a partitioned table; forbidden for a
  non-partitioned table.
- `--columns` names must all exist in the table schema.
- `--block-size` must be `>= 1`.
- `--limit` must be `>= 1` when given.
- `--overwrite` is rejected when `--partition` is omitted on a partitioned
  table (i.e. no implicit "overwrite the whole partitioned table").

## 3. Backend Layer

Both methods land in `src/maxc_cli/backend/data.py` on `DataMixin`,
keeping the existing mixin layout. No new backend module.

```python
class DataMixin:
    # Existing: sample_table, profile_table, _resolve_partition_for_sample

    def upload_table(
        self,
        table_name: str,
        file_path: str,
        *,
        partition: str | None = None,
        overwrite: bool = False,
        delimiter: str = ",",
        has_header: bool = True,
        null_marker: str = r"\N",
        block_size: int = 10000,
        project: str | None = None,
    ) -> dict[str, Any]:
        """Return a dict with rows_written, bytes_read, blocks, overwrite,
        applied_partition, and warnings."""

    def download_table(
        self,
        table_name: str,
        output_path: str,
        *,
        partition: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        delimiter: str = ",",
        write_header: bool = True,
        null_marker: str = "",
        project: str | None = None,
    ) -> dict[str, Any]:
        """Return a dict with rows_written, bytes_written, columns,
        applied_partition, truncated, and warnings."""
```

`BaseBackend` in `backend/base.py` gains the same two abstract methods so
the protocol stays explicit.

### Underlying ODPS API

Both methods use `odps.tunnel.TableTunnel` directly — the same path
`odpscmd tunnel upload/download` uses, and a sibling of
`client.read_table()` already used by `sample_table`. Tunnel does not
consume SQL CU and avoids the latency of going through the SQL engine.

- **Upload:** `tunnel.create_upload_session(table, partition_spec,
  overwrite=...)`, `session.open_record_writer(block_id)` (one block per
  `block_size` rows), `session.commit([...block_ids])`.
- **Download:** `tunnel.create_download_session(table, partition_spec)`,
  `session.open_record_reader(start, count)` over `session.count` records
  (or up to `--limit`).

### CSV helpers in `helpers.py`

A small group of `csv_*` helpers keeps the mixin focused on orchestration:

- `csv_parse_value(text: str, odps_type: str, *, null_marker: str)` —
  CSV string → typed Python value matching ODPS column type.
- `csv_format_value(value: Any, odps_type: str, *, null_marker: str)` —
  typed Python value → CSV string (used by download).
- `csv_supported_type(odps_type: str) -> bool` — True for all primitives
  (bigint, int, double, decimal, boolean, string, varchar, datetime,
  date, timestamp), False for `array`/`map`/`struct`.

Complex types raise `ValidationError` up-front (before any tunnel session
opens) with a suggestion to use SQL `INSERT ... SELECT` instead.

## 4. Application Layer

`MaxCApp` in `app.py` gets two new methods that wrap the backend calls
and build the envelope:

```python
def data_upload(self, table_name, file_path, *, partition, overwrite,
                delimiter, has_header, null_marker, block_size,
                project) -> Envelope: ...

def data_download(self, table_name, output_path, *, partition, columns,
                  limit, delimiter, write_header, null_marker,
                  project) -> Envelope: ...
```

The CLI handlers `_handle_data_upload` / `_handle_data_download` in
`cli.py` parse argparse args and call the app methods, identical in
shape to `_handle_data_sample`.

## 5. Data Flow

### Upload

1. `describe_table(table)` → `TableDefinition` with schema + partition keys.
2. Validate `--partition` against `definition.partition_columns`
   (required iff partitioned; rejected if extra keys; rejected if any
   value missing).
3. Validate every non-partition column type with
   `csv_supported_type`; raise `ValidationError` listing unsupported
   columns before opening any session.
4. Open the file with `csv.reader(file, delimiter=delimiter)`.
   - If `has_header`: first row is treated as the header, mapped against
     non-partition columns. Unknown header names → fail. Missing required
     columns → fail. Extra header columns → warning, ignored.
   - If `--no-header`: rows are mapped by ordinal to non-partition
     columns; row width must match.
5. `tunnel.create_upload_session(table, partition_spec, overwrite=...)`.
6. Open one `RecordWriter` for `block_id=0`. For each parsed row:
   - `csv_parse_value` per column; assemble `record`; write.
   - When `block_size` rows have been written, close the current writer,
     open a new block, increment `block_id`.
7. `session.commit([block_ids...])`. Return counts + applied partition +
   warnings.
8. **Failure mode (fail-fast):** any parse / type / IO error during steps
   4–6 aborts immediately. The current block writer is closed without
   commit; `session.commit([])` is *not* called, so the upload is fully
   rolled back. The error is raised as
   `CsvParseError(line=N, column=name, reason=...)`.

### Download

1. `describe_table(table)` → schema + partition keys.
2. Validate `--partition` and `--columns`. Default columns =
   `[c.name for c in definition.columns]`.
3. `tunnel.create_download_session(table, partition_spec)`. Compute
   `count = min(session.count, limit)` if `limit` is given else
   `session.count`.
4. Open `csv.writer(file, delimiter=delimiter)`. Write header row from
   the chosen columns unless `--no-header`.
5. `session.open_record_reader(0, count)`. For each record write
   `[csv_format_value(record[c], type, ...) for c in columns]`.
6. Track `rows_written` and `bytes_written` (file `.tell()` after close).
   Set `truncated = limit is not None and limit < session.count`.
7. Return counts + columns + applied partition + warnings.

## 6. Output Envelope

### Upload — success

```json
{
  "version": "1.0",
  "command": "data.upload",
  "status": "success",
  "data": {
    "table": "project.schema.tbl",
    "partition": "ds=20260101",
    "rows_written": 12345,
    "bytes_read": 2345678,
    "blocks": 2,
    "overwrite": false
  },
  "metadata": { "elapsed_ms": 4567, "project": "..." },
  "error": null,
  "agent_hints": {
    "next_actions": [
      "maxc data sample project.schema.tbl --partition ds=20260101 --rows 5"
    ],
    "warnings": []
  }
}
```

### Download — success

```json
{
  "version": "1.0",
  "command": "data.download",
  "status": "success",
  "data": {
    "table": "project.schema.tbl",
    "partition": "ds=20260101",
    "output_path": "/abs/path/out.csv",
    "rows_written": 10000,
    "bytes_written": 4567890,
    "columns": ["col1", "col2"],
    "truncated": true
  },
  "metadata": { "elapsed_ms": 3456, "project": "..." },
  "error": null,
  "agent_hints": {
    "next_actions": [],
    "warnings": [
      "--limit reached; output may be partial (session has 53210 rows)"
    ]
  }
}
```

### Errors

Reuse existing exception family; one new code is added:

| Exception | error_code | When |
|---|---|---|
| `ValidationError` | `validation_error` | bad flags, missing partition, unsupported column type, unknown column |
| `NotFoundError` | `not_found` | table or partition doesn't exist |
| `PermissionDeniedError` | `permission_denied` | tunnel session refused |
| `BackendConnectionError` | `backend_connection_error` | tunnel network failure |
| **new** `CsvParseError` | `csv_parse_error` | row N column C cannot be parsed into target type |

`CsvParseError` extends `ValidationError` and carries `line` and
`column` attributes; the envelope `error` block includes them under
`error.context = {"line": N, "column": "name", "value": "..."}`.

## 7. Testing

### Mock unit tests — `tests/test_cli_mock.py`

Extend the existing `FakeODPS` to provide a tunnel stub:

- `FakeTunnel.create_upload_session(table, partition, overwrite)` returns
  a `FakeUploadSession` collecting records into an in-memory list.
- `FakeTunnel.create_download_session(table, partition)` returns a
  `FakeDownloadSession` with a fixed `count` and a `RecordReader` that
  yields pre-seeded `Record` objects.

Tests:

- `test_data_upload_appends_csv_to_table`
- `test_data_upload_overwrite_partition`
- `test_data_upload_rejects_missing_partition_for_partitioned_table`
- `test_data_upload_rejects_unsupported_complex_type`
- `test_data_upload_fail_fast_on_bad_row_aborts_session`
- `test_data_upload_no_header_uses_ordinal_mapping`
- `test_data_download_writes_full_partition`
- `test_data_download_respects_limit_and_marks_truncated`
- `test_data_download_columns_subset_in_requested_order`
- `test_data_download_rejects_unknown_column`

### Integration test — `tests/test_integration_real.py`

`test_data_upload_download_roundtrip`: uploads a small CSV into a
disposable test partition, downloads it back, asserts row equality. Uses
the existing skip-when-no-credentials decorator.

## 8. Open Risks & Mitigations

- **Tunnel `BufferedWriter` vs explicit block writer.** PyODPS exposes
  both. The explicit block writer with manual `block_size` flushing is
  what odpscmd uses and gives us deterministic block counts to surface in
  the envelope. Implementation will pick this path.
- **`session.count` for download is the partition's row count, not the
  exact count returned to the user under `--limit`.** The envelope
  surfaces both `rows_written` and a `truncated` flag (plus an
  agent-hint warning) so this is unambiguous.
- **CSV type coverage.** Decimals and timestamps need careful round-trip
  handling. The `csv_*` helpers will be unit-tested per supported type
  before being wired into the mixin.
- **Partial files on download failure.** If the tunnel session errors
  mid-read, the partially-written CSV is left on disk. v1 deletes the
  output file on error; the envelope error message states this.
