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
        table_name: str,
        rows: int,
        *,
        partition: str | None = None,
        columns: list[str] | None = None,
    ) -> tuple[TableDefinition, list[dict[str, Any]], dict[str, Any]]:
        """Sample data from a table."""
        definition = self.describe_table(table_name)
        selected_columns, applied_partition, partition_values = resolve_sample_request(
            definition,
            partition=partition,
            columns=columns,
            strict_partition_check=False,
        )
        select_list = ", ".join(quote_table_name(name) for name in selected_columns)
        sql = f"SELECT {select_list} FROM {quote_table_name(table_name)}"
        if applied_partition:
            predicates = [
                f"{quote_table_name(column.name)} = {sql_string_literal(partition_values[column.name])}"
                for column in definition.partition_columns
            ]
            sql += " WHERE " + " AND ".join(predicates)
        sql += f" LIMIT {rows}"

        result = self.execute_query(
            sql,
            project=self.project,
            max_rows=rows,
            dry_run=False,
            offset=0,
        )
        return definition, result.rows, {
            "schema": result.schema,
            "applied_partition": applied_partition,
            "selected_columns": selected_columns,
        }

    def profile_table(self, table_name: str, *, partition: str | None = None) -> dict[str, Any]:
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
