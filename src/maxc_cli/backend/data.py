"""Data-related mixin for OdpsBackend."""

from typing import Any

from ..config import TableDefinition
from ..exceptions import ValidationError
from ..helpers import (
    build_profile,
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
