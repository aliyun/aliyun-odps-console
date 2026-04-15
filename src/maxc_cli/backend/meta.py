"""Meta-related mixin for OdpsBackend."""

from itertools import islice
from typing import Any

from ..config import TableColumn, TableDefinition
from ..helpers import (
    _dt_to_iso,
    build_freshness_info,
    build_latest_partition_info,
    partition_spec_text,
    record_to_dict,
    translate_odps_error,
)


class MetaMixin:
    """Mixin providing metadata methods."""

    def list_tables(self, *, schema: 'str | None' = None) -> 'list[TableDefinition]':
        """List tables in the current project, optionally filtered by schema.

        Uses ``client.list_tables()`` to iterate all tables in the project.
        Returns minimal ``TableDefinition`` stubs (name only, no schema access)
        to avoid triggering per-table ``reload()`` calls.

        Limitations:
            - Large projects (>10k tables) may take 30+ seconds on first call.
            - Consider running ``cache build`` first for faster lookups.

        Args:
            schema: Optional schema name to filter tables.

        Returns:
            Sorted list of TableDefinition stubs.
        """
        tables: 'list[TableDefinition]' = []
        kwargs: 'dict[str, Any]' = {"project": self.project}
        if schema:
            kwargs["schema"] = schema
        try:
            for table in self.client.list_tables(**kwargs):
                tables.append(self._table_stub(table))
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return sorted(tables, key=lambda item: item.name)

    def describe_table(self, table_name: 'str') -> 'TableDefinition':
        """Describe a table with full schema, partitions, and sample rows.

        Calls ``table.schema`` for column definitions, ``table.partitions``
        for partition list (capped at 20), and a ``SELECT * LIMIT 2``
        head query for sample rows.

        Args:
            table_name: Table name in ``schema.table`` or bare ``table`` format.

        Returns:
            Full TableDefinition with columns, partitions, and sample_rows.
        """
        table = self._get_table(table_name)
        partitions = self._list_partitions(table, limit=20)
        sample_rows = self._table_head(table, limit=2)
        definition = self._table_definition_from_table(table)
        definition.partitions = partitions
        definition.sample_rows = sample_rows
        return definition

    def search_tables(self, keyword: 'str', *, schema: 'str | None' = None) -> 'list[dict[str, Any]]':
        """Search tables by keyword using client-side substring match.

        Iterates ``project.tables`` and filters by case-insensitive substring
        match on table name and comment. No server-side FTS is available.

        Limitations:
            - Case-insensitive substring match only.
            - Does not search column names (use ``search_columns`` for that).
            - Consider ``cache build`` for faster repeated searches.

        Args:
            keyword: Search term (case-insensitive substring).
            schema: Optional schema scope.

        Returns:
            List of dicts with keys: name, schema, comment, owner.
        """
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: 'list[dict[str, Any]]' = []
        for table in self.list_tables(schema=schema):
            score = 0
            searchable = f"{table.name} {table.description}".lower()
            matched_columns: 'list[str]' = []
            for token in tokens:
                if token in searchable:
                    score += 5
            if score == 0:
                for column in table.columns:
                    text = f"{column.name} {column.comment}".lower()
                    if any(token in text for token in tokens):
                        score += 2
                        matched_columns.append(column.name)
            if score:
                matches.append(
                    {
                        "table_name": table.name,
                        "description": table.description,
                        "score": score,
                        "matched_columns": matched_columns,
                    }
                )
        return sorted(matches, key=lambda item: (-item["score"], item["table_name"]))

    def search_columns(self, keyword: 'str', *, schema: 'str | None' = None) -> 'list[dict[str, Any]]':
        """Search columns across all tables by keyword.

        Iterates all tables and their columns, scoring matches by
        column name, comment, and table+column context. No server-side
        column search API is available.

        Limitations:
            - Client-side iteration only; slow without cache.
            - Scoring is heuristic (column name match > comment match).
            - Consider ``cache build`` for faster repeated searches.

        Args:
            keyword: Search term (case-insensitive, space-separated tokens).
            schema: Optional schema scope.

        Returns:
            Sorted list of dicts with keys: table_name, column_name, type, comment, score.
        """
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: 'list[dict[str, Any]]' = []
        for table in self.list_tables(schema=schema):
            for column in table.columns:
                score = 0
                text = f"{column.name} {column.comment}".lower()
                searchable = f"{table.name} {text}".lower()
                for token in tokens:
                    if token in column.name.lower():
                        score += 8
                    if token in text:
                        score += 4
                    if token in searchable:
                        score += 2
                if score:
                    matches.append(
                        {
                            "table_name": table.name,
                            "column_name": column.name,
                            "type": column.type,
                            "comment": column.comment,
                            "score": score,
                        }
                    )
        return sorted(matches, key=lambda item: (-item["score"], item["table_name"], item["column_name"]))

    def latest_partition_info(self, table_name: 'str') -> 'tuple[dict[str, Any], list[str]]':
        """Get the latest partition info for a partitioned table.

        Uses ``table.partitions`` to find the most recent partition by
        creation time.

        Args:
            table_name: Table name.

        Returns:
            Tuple of (payload dict, warnings list). Payload contains
            partition key, value, creation time, and size info.
        """
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        return self._latest_partition_info_from_table(table, definition)

    def freshness_info(self, table_name: 'str') -> 'tuple[dict[str, Any], list[str]]':
        """Get data freshness info for a table.

        Derives freshness from the latest partition's modification time.
        Not a native ODPS API — the value is approximate.

        Limitations:
            - Based on partition metadata timestamps, not actual data writes.
            - Non-partitioned tables return limited freshness info.

        Args:
            table_name: Table name.

        Returns:
            Tuple of (payload dict, warnings list).
        """
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        latest_payload, warnings = self._latest_partition_info_from_table(table, definition)
        return build_freshness_info(definition, latest_payload, warnings=warnings)

    def lineage_info(self, table_name: 'str') -> 'tuple[dict[str, Any], list[str]]':
        """Get table lineage info.

        **Currently unsupported** — ODPS lineage API is not accessible via pyodps.
        Returns a ``supported=false`` placeholder with a clear contract.

        When the API becomes available, this method can be updated without
        changing the CLI interface.

        Args:
            table_name: Table name.

        Returns:
            Tuple of (payload dict, warnings list). Payload contains
            ``supported: false`` and a message explaining the limitation.
        """
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        return (
            {
                "table_name": definition.name,
                "supported": False,
                "lineage_source": "unavailable",
                "coverage": "unsupported",
                "upstream_tables": [],
                "downstream_tables": [],
                "limitation": "The current version does not integrate with the MaxCompute lineage API.",
            },
            ["The current version does not integrate with the MaxCompute lineage API, so lineage returns an explicit unsupported placeholder result."],
        )

    def list_projects(self) -> 'list[dict[str, Any]]':
        """List all projects owned by the current user.

        Note: This only returns basic info (name) to avoid triggering project.reload()
        which requires Read permission on each project. Use get_project_info() for details.
        """
        projects: 'list[dict[str, Any]]' = []
        try:
            # 获取当前用户的 display name 作为 owner 过滤条件
            owner = self._get_owner_display_name()
            for project in self.client.list_projects(owner=owner):
                # 只返回 list_projects 直接提供的基本信息
                # 不要访问 comment, owner, properties 等属性，会触发 reload 需要 Read 权限
                projects.append({
                    "name": project.name,
                })
        except Exception as exc:
            raise translate_odps_error(exc, "list_projects") from exc
        return sorted(projects, key=lambda item: item["name"])

    def list_schemas(self, *, project: 'str | None' = None) -> 'list[dict[str, Any]]':
        """List all schemas in a project.

        Args:
            project: Optional project override. Defaults to the configured project.

        Returns:
            List of schema info dicts.
        """
        target_project = project or self.project
        schemas: 'list[dict[str, Any]]' = []
        try:
            for schema in self.client.list_schemas(project=target_project):
                schemas.append({
                    "name": schema.name,
                })
        except Exception as exc:
            raise translate_odps_error(exc, "list_schemas") from exc
        return sorted(schemas, key=lambda item: item["name"])

    def get_project_info(self, project_name: 'str | None' = None) -> 'dict[str, Any]':
        """Get detailed information about a project.

        Calls ``project.reload()`` to fetch full metadata including
        owner, creation time, and cluster info.

        Args:
            project_name: Project name. Defaults to the configured project.

        Returns:
            Dict with project metadata.
        """
        target = project_name or self.project
        try:
            project = self.client.get_project(target)
            # get_project 返回的对象需要 reload 才能获取完整属性
            # 访问属性会自动触发 lazy loading
            props = getattr(project, "properties", {}) or {}
            extended_props = getattr(project, "extended_properties", {}) or {}

            return {
                "name": project.name,
                "project_type": getattr(project, "type", None),
                "comment": getattr(project, "comment", None),
                "owner": getattr(project, "owner", None),
                "state": getattr(project, "state", None) or getattr(project, "status", None),
                "creation_time": _dt_to_iso(getattr(project, "creation_time", None)),
                "last_modified_time": _dt_to_iso(getattr(project, "last_modified_time", None)),
                "region": getattr(project, "region_id", None),
                "allow_3_tier": props.get("allow3tier") or extended_props.get("allow3tier"),
                "is_external_catalog_bound": props.get("isExternalCatalogBound") or extended_props.get("isExternalCatalogBound"),
            }
        except Exception as exc:
            raise translate_odps_error(exc, "get_project_info") from exc

    # Private methods for metadata handling

    def _get_table(self, table_name: 'str', *, project: 'str | None' = None, schema: 'str | None' = None):
        """Get ODPS table by name."""
        kwargs: 'dict[str, Any]' = {"project": project or self.project}
        if schema:
            kwargs["schema"] = schema
        try:
            return self.client.get_table(table_name, **kwargs)
        except Exception as exc:
            raise translate_odps_error(exc) from exc

    def _table_stub(self, table) -> 'TableDefinition':
        """Create a minimal TableDefinition from table object (name only, no schema access)."""
        return TableDefinition(
            name=table.name,
            description="",
            columns=[],
            sample_rows=[],
            partitions=[],
            upstream_tables=[],
            downstream_tables=[],
            partition_columns=[],
            owner=None,
            created_at=None,
            updated_at=None,
            table_type="TABLE",
            size_bytes=None,
        )

    def _table_definition_from_table(self, table) -> 'TableDefinition':
        """Create a full TableDefinition from ODPS table object."""
        try:
            columns = [
                TableColumn(
                    name=column.name,
                    type=str(column.type),
                    comment=getattr(column, "comment", "") or "",
                )
                for column in getattr(table.table_schema, "columns", [])
            ]
            partition_columns = [
                TableColumn(
                    name=column.name,
                    type=str(column.type),
                    comment=getattr(column, "comment", "") or "",
                )
                for column in getattr(table.table_schema, "partitions", [])
            ]
            return TableDefinition(
                name=table.name,
                description=getattr(table, "comment", "") or "",
                columns=columns,
                sample_rows=[],
                partitions=[],
                upstream_tables=[],
                downstream_tables=[],
                partition_columns=partition_columns,
                owner=getattr(table, "owner", None),
                created_at=_dt_to_iso(getattr(table, "creation_time", None)),
                updated_at=_dt_to_iso(getattr(table, "last_data_modified_time", None)),
                table_type="VIRTUAL_VIEW" if getattr(table, "is_virtual_view", False) else "TABLE",
                size_bytes=(
                    int(getattr(table, "size", 0))
                    if getattr(table, "size", None) is not None
                    else None
                ),
                extra_metadata={"lifecycle": getattr(table, "lifecycle", None)},
            )
        except Exception as exc:
            raise translate_odps_error(exc) from exc

    def _table_head(self, table, *, limit: 'int') -> 'list[dict[str, Any]]':
        """Get first N rows from a table."""
        try:
            reader = table.head(limit)
            rows = list(islice(reader, limit))
        except Exception:
            return []
        columns = [column.name for column in table.table_schema.columns]
        return [record_to_dict(columns, record.values) for record in rows]

    def _list_partitions(self, table, *, limit: 'int') -> 'list[str]':
        """List partition specs for a table."""
        try:
            from odps.errors import InvalidParameter as OdpsInvalidParameter
            partitions = list(islice(table.iterate_partitions(), limit))
        except Exception:
            return []
        return [str(partition.partition_spec) for partition in partitions]

    def _latest_partition_info_from_table(
        self,
        table,
        definition: 'TableDefinition',
    ) -> 'tuple[dict[str, Any], list[str]]':
        """Get latest partition info from ODPS table object."""
        latest_partition = self._max_partition_spec(table)
        if latest_partition:
            return build_latest_partition_info(
                definition,
                source="odps_get_max_partition",
                latest_partition_override=latest_partition,
                visible_partition_count=None,
            )

        partitions = self._list_partitions(table, limit=200)
        payload, warnings = build_latest_partition_info(
            definition,
            source="odps_iterate_partitions",
            partitions=partitions,
            visible_partition_count=len(partitions),
        )
        if definition.partition_columns and len(partitions) == 200:
            warnings.append("Only the first 200 visible partitions were inspected. For very large tables, verify the result in the MaxCompute console as well.")
        return payload, warnings

    def _max_partition_spec(self, table) -> 'str | None':
        """Get max partition spec from table using get_max_partition if available."""
        getter = getattr(table, "get_max_partition", None)
        if callable(getter):
            for kwargs in ({"skip_empty": True}, {}):
                try:
                    partition = getter(**kwargs)
                except TypeError:
                    continue
                except Exception:
                    partition = None
                text = partition_spec_text(partition)
                if text:
                    return text
        return None
