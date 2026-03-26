"""Data-related mixin for OdpsBackend."""

from typing import Any

from ..config import TableDefinition
from ..helpers import (
    build_profile,
    quote_table_name,
    resolve_sample_request,
    sql_string_literal,
)


class DataMixin:
    """Mixin providing data sampling and profiling methods."""

    def sample_table(
        self,
        table_name: 'str',
        rows: 'int',
        *,
        partition: 'str | None' = None,
        columns: 'list[str] | None' = None,
    ) -> 'tuple[TableDefinition, list[dict[str, Any]], dict[str, Any]]':
        """Sample data from a table using ODPS read_table for better performance."""
        definition = self.describe_table(table_name)
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
                project=self.project,
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
        }

    def profile_table(self, table_name: 'str', *, partition: 'str | None' = None) -> 'dict[str, Any]':
        """Profile data from a table."""
        definition, sample_rows, sample_info = self.sample_table(
            table_name,
            rows=20,
            partition=partition,
            columns=None,
        )
        return build_profile(
            definition,
            sample_rows,
            applied_partition=sample_info["applied_partition"],
        )
