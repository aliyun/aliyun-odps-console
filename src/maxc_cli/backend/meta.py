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

    def list_tables(self) -> list[TableDefinition]:
        """List tables in the current project."""
        tables: list[TableDefinition] = []
        try:
            for table in self.client.list_tables(project=self.project):
                tables.append(self._table_stub(table))
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return sorted(tables, key=lambda item: item.name)

    def describe_table(self, table_name: str) -> TableDefinition:
        """Describe a table with partitions and sample rows."""
        table = self._get_table(table_name)
        partitions = self._list_partitions(table, limit=20)
        sample_rows = self._table_head(table, limit=2)
        definition = self._table_definition_from_table(table)
        definition.partitions = partitions
        definition.sample_rows = sample_rows
        return definition

    def search_tables(self, keyword: str) -> list[dict[str, Any]]:
        """Search tables by keyword."""
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: list[dict[str, Any]] = []
        for table in self.list_tables():
            score = 0
            searchable = f"{table.name} {table.description}".lower()
            matched_columns: list[str] = []
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

    def search_columns(self, keyword: str) -> list[dict[str, Any]]:
        """Search columns by keyword."""
        tokens = [item.lower() for item in keyword.split() if item.strip()] or [keyword.lower()]
        matches: list[dict[str, Any]] = []
        for table in self.list_tables():
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

    def latest_partition_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        """Get latest partition info for a table."""
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        return self._latest_partition_info_from_table(table, definition)

    def freshness_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        """Get data freshness info for a table."""
        table = self._get_table(table_name)
        definition = self._table_definition_from_table(table)
        latest_payload, warnings = self._latest_partition_info_from_table(table, definition)
        return build_freshness_info(definition, latest_payload, warnings=warnings)

    def lineage_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        """Get table lineage info (placeholder - API not yet integrated)."""
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
                "limitation": "当前版本未接入 MaxCompute 血缘 API。",
            },
            ["当前版本未接入 MaxCompute 血缘 API，lineage 返回的是明确的 unsupported 占位结果。"],
        )

    def list_projects(self) -> list[dict[str, Any]]:
        """List all projects owned by the current user.

        Note: This only returns basic info (name) to avoid triggering project.reload()
        which requires Read permission on each project. Use get_project_info() for details.
        """
        projects: list[dict[str, Any]] = []
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

    def list_schemas(self, *, project: str | None = None) -> list[dict[str, Any]]:
        """List all schemas in a project."""
        target_project = project or self.project
        schemas: list[dict[str, Any]] = []
        try:
            for schema in self.client.list_schemas(project=target_project):
                schemas.append({
                    "name": schema.name,
                })
        except Exception as exc:
            raise translate_odps_error(exc, "list_schemas") from exc
        return sorted(schemas, key=lambda item: item["name"])

    def get_project_info(self, project_name: str | None = None) -> dict[str, Any]:
        """Get detailed information about a project."""
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

    def _get_table(self, table_name: str, *, project: str | None = None):
        """Get ODPS table by name."""
        try:
            return self.client.get_table(table_name, project=project or self.project)
        except Exception as exc:
            raise translate_odps_error(exc) from exc

    def _table_stub(self, table) -> TableDefinition:
        """Create a minimal TableDefinition from table object."""
        return self._table_definition_from_table(table)

    def _table_definition_from_table(self, table) -> TableDefinition:
        """Create a full TableDefinition from ODPS table object."""
        row_count = int(getattr(table, "record_num", -1) or -1)
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
            row_count=row_count,
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
            row_count_source="odps_record_num" if row_count >= 0 else "unavailable",
            size_bytes=(
                int(getattr(table, "size", 0))
                if getattr(table, "size", None) is not None
                else None
            ),
            extra_metadata={"lifecycle": getattr(table, "lifecycle", None)},
        )

    def _table_head(self, table, *, limit: int) -> list[dict[str, Any]]:
        """Get first N rows from a table."""
        try:
            reader = table.head(limit)
            rows = list(islice(reader, limit))
        except Exception:
            return []
        columns = [column.name for column in table.table_schema.columns]
        return [record_to_dict(columns, record.values) for record in rows]

    def _list_partitions(self, table, *, limit: int) -> list[str]:
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
        definition: TableDefinition,
    ) -> tuple[dict[str, Any], list[str]]:
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
            warnings.append("当前只遍历前 200 个可见分区，超大表建议结合控制台进一步核对。")
        return payload, warnings

    def _max_partition_spec(self, table) -> str | None:
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
