"""Data-related mixin for OdpsBackend."""

from typing import Any

from ..config import TableDefinition
from ..exceptions import CsvParseError, ValidationError
from ..helpers import (
    build_profile,
    csv_format_value,
    csv_parse_value,
    csv_supported_type,
    quote_table_name,
    resolve_sample_request,
    sql_string_literal,
    translate_odps_error,
)


class DataMixin:
    """Mixin providing data sampling and profiling methods."""

    def _resolve_partition_for_sample(
        self,
        definition: 'TableDefinition',
        partition: 'str | None',
        *,
        project: 'str | None',
    ) -> 'tuple[str | None, list[str]]':
        """Resolve the partition spec to use, auto-detecting latest if needed.

        Returns (partition_spec, warnings).

        Raises ValidationError if the table is partitioned and no partition
        can be determined.
        """
        warnings: 'list[str]' = []
        if partition or not definition.partition_columns:
            return partition, warnings

        # Partitioned table without partition spec — try latest-partition.
        try:
            latest_payload, _latest_warnings = self.latest_partition_info(
                definition.name, project=project,
            )
            latest_spec = latest_payload.get("latest_partition")
        except Exception:
            latest_spec = None

        if latest_spec:
            warnings.append(
                f"No --partition specified; auto-selected latest partition "
                f"`{latest_spec}`. Pass --partition explicitly to pin a value."
            )
            return latest_spec, warnings

        partition_keys = ", ".join(c.name for c in definition.partition_columns)
        raise ValidationError(
            (
                f"Table `{definition.name}` is partitioned ({partition_keys}) "
                f"but no --partition was specified, and no latest partition "
                f"could be determined."
            ),
            suggestion=(
                f"Run `maxc meta latest-partition {definition.name}` to find a "
                f"valid partition, then re-run with --partition <spec>."
            ),
        )

    def sample_table(
        self,
        table_name: 'str',
        rows: 'int',
        *,
        partition: 'str | None' = None,
        columns: 'list[str] | None' = None,
        project: 'str | None' = None,
    ) -> 'tuple[TableDefinition, list[dict[str, Any]], dict[str, Any]]':
        """Sample data from a table.

        Uses ``client.read_table()`` for efficient row-level access with
        optional partition pruning and column selection. When the table is
        partitioned and *partition* is not provided, automatically selects
        the latest partition (and adds a warning to ``sample_info``).

        Args:
            table_name: Table name.
            rows: Maximum number of rows to return.
            partition: Optional partition spec (e.g. ``"ds=20260101"``).
            columns: Optional list of column names to select.

        Returns:
            Tuple of (table definition, sample rows as list of dicts,
            sample metadata with applied_partition, selected_columns,
            and warnings).
        """
        definition = self.describe_table(table_name, project=project)
        partition, auto_partition_warnings = self._resolve_partition_for_sample(
            definition, partition, project=project,
        )

        selected_columns, applied_partition, partition_values = resolve_sample_request(
            definition,
            partition=partition,
            columns=columns,
            strict_partition_check=False,
        )

        # Build column selection
        column_names = selected_columns if selected_columns else [c.name for c in definition.columns]

        # Build partition spec if needed
        partition_spec = None
        if applied_partition and partition_values:
            partition_spec = ",".join(
                f"{k}={v}" for k, v in partition_values.items()
            )

        # Read data using ODPS read_table method
        def _serialize_value(value):
            """Convert value to JSON-serializable format."""
            from datetime import datetime, date
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, date):
                return value.isoformat()
            return value

        try:
            records = self.client.read_table(
                table_name,
                limit=rows,
                partition=partition_spec,
                project=project or self.project,
            )
            sample_rows = [
                {column: _serialize_value(record[column]) for column in column_names}
                for record in records
            ]
        except Exception as exc:
            raise translate_odps_error(exc) from exc

        return definition, sample_rows, {
            "schema": [{"name": c.name, "type": c.type, "comment": c.comment} for c in definition.columns if c.name in column_names],
            "applied_partition": applied_partition,
            "selected_columns": selected_columns,
            "warnings": auto_partition_warnings,
        }

    def profile_table(self, table_name: 'str', *, partition: 'str | None' = None, project: 'str | None' = None) -> 'dict[str, Any]':
        """Profile data from a table by sampling and computing statistics.

        Samples up to 20 rows and computes per-column statistics (null count,
        distinct count, min/max, etc.) using heuristic analysis. Not a native
        ODPS profile feature — results are approximate.

        Limitations:
            - Based on a 20-row sample; not statistically representative.
            - No native ODPS ``PROFILE`` command is used.
            - For accurate statistics, run explicit aggregation SQL.

        Args:
            table_name: Table name.
            partition: Optional partition spec for partition pruning.

        Returns:
            Dict with table name, column profiles, and sample info.
        """
        definition, sample_rows, sample_info = self.sample_table(
            table_name,
            rows=20,
            partition=partition,
            columns=None,
            project=project,
        )
        return build_profile(
            definition,
            sample_rows,
            applied_partition=sample_info["applied_partition"],
        )

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
        """Upload a CSV/TSV file into an existing table or partition via Tunnel.

        Args:
            table_name: Target table name (schema.table or table).
            file_path: Path to the local CSV/TSV file to upload.
            partition: Optional partition spec (e.g. ``"ds=20260508"``); required
                for partitioned tables, forbidden for non-partitioned tables.
            overwrite: If True, use INSERT OVERWRITE semantics for the target.
            delimiter: Field delimiter (default ``","``).
            has_header: If True, the first row is treated as a header and
                columns are mapped by name; otherwise mapped by ordinal.
            null_marker: Token interpreted as SQL NULL (default ``"\\N"``).
            block_size: Rows per Tunnel block (default 10000).
            project: Optional MaxCompute project override.

        Returns:
            Dict with ``table``, ``applied_partition``, ``rows_written``,
            ``bytes_read``, ``blocks``, ``overwrite``, and ``warnings``.

        Raises:
            ValidationError: For invalid partitioning, unsupported column
                types, or invalid block sizes.
            CsvParseError: When a CSV row cannot be parsed; carries
                ``line`` / ``column`` context. The Tunnel session is aborted
                before the exception propagates.
        """
        import csv
        import os

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
        """Download a table or partition to a local CSV/TSV file via Tunnel.

        Args:
            table_name: Table name (schema.table or table).
            output_path: Local file path to write.
            partition: Required when table is partitioned.
            columns: Optional column subset; default = all columns in schema order.
            limit: Optional max rows; default = full partition / table.
            delimiter: Field delimiter (default ",").
            write_header: When False, suppress header row.
            null_marker: Token written for SQL NULL (default empty string).
            project: Target project; default = backend's default project.

        Returns:
            Dict with table, applied_partition, output_path, rows_written,
            bytes_written, columns, truncated, warnings.
        """
        import csv
        import os

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


def _resolve_header_mapping(header: 'list[str]', data_columns: 'list') -> 'list[str]':
    expected = {c.name for c in data_columns}
    seen = set(header)
    missing = expected - seen
    if missing:
        raise ValidationError(
            f"CSV header missing required columns: {sorted(missing)}",
        )
    return [name for name in header if name in expected]
