"""Meta-related mixin for OdpsBackend."""

from itertools import islice
from typing import Any

from odps.errors import ODPSError

from ..config import TableColumn, TableDefinition
from ..exceptions import ValidationError
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

    def list_tables(
        self,
        *,
        schema: 'str | None' = None,
        project: 'str | None' = None,
        limit: 'int | None' = None,
        offset: 'int' = 0,
    ) -> 'tuple[list[TableDefinition], bool]':
        """List tables in a project, optionally filtered by schema.

        Uses ``client.list_tables()`` to iterate all tables in the project.
        Returns minimal ``TableDefinition`` stubs (name only, no schema access)
        to avoid triggering per-table ``reload()`` calls.

        When *limit* is provided, iteration is bounded via ``itertools.islice``
        so very large projects don't drag the whole table list. The returned
        ``has_more`` flag is ``True`` if at least one table beyond the window
        exists.

        Limitations:
            - Without a limit, large projects (>10k tables) may take 30+ seconds.
            - Consider running ``cache build`` first for faster lookups.

        Args:
            schema: Optional schema name to filter tables.
            project: Optional project override. Defaults to the configured project.
            limit: Maximum number of tables to return. ``None`` means no limit.
            offset: Number of tables to skip from the start.

        Returns:
            ``(tables, has_more)`` — ``tables`` is sorted by name within the
            requested window; ``has_more`` indicates more tables exist past
            the window.
        """
        kwargs: 'dict[str, Any]' = {"project": project or self.project}
        if schema:
            kwargs["schema"] = schema
        try:
            iterator = iter(self.client.list_tables(**kwargs))
            if offset:
                # Drop *offset* items first.
                for _ in range(offset):
                    if next(iterator, None) is None:
                        return [], False
            if limit is None:
                tables = [self._table_stub(table) for table in iterator]
                has_more = False
            else:
                # Take limit + 1 to detect has_more without a second pass.
                window = list(islice(iterator, limit + 1))
                has_more = len(window) > limit
                tables = [self._table_stub(table) for table in window[:limit]]
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        return sorted(tables, key=lambda item: item.name), has_more

    def describe_table(self, table_name: 'str', *, project: 'str | None' = None) -> 'TableDefinition':
        """Describe a table with full schema, partitions, and sample rows.

        Args:
            table_name: Table name in ``schema.table`` or bare ``table`` format.
            project: Optional project override.

        Returns:
            Full TableDefinition with columns, partitions, and sample_rows.
        """
        table = self._get_table(table_name, project=project)
        partitions = self._list_partitions(table, limit=20)
        sample_rows = self._table_head(table, limit=2)
        definition = self._table_definition_from_table(table)
        definition.partitions = partitions
        definition.sample_rows = sample_rows
        return definition

    def search_tables(self, keyword: 'str', *, schema: 'str | None' = None, project: 'str | None' = None) -> 'list[dict[str, Any]]':
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
        all_tables, _ = self.list_tables(schema=schema, project=project)
        for table in all_tables:
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

    def search_columns(self, keyword: 'str', *, schema: 'str | None' = None, project: 'str | None' = None) -> 'list[dict[str, Any]]':
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
        all_tables, _ = self.list_tables(schema=schema, project=project)
        for table in all_tables:
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

    def list_partitions(
        self,
        table_name: 'str',
        *,
        limit: 'int' = 100,
        project: 'str | None' = None,
    ) -> 'tuple[dict[str, Any], list[str]]':
        """List partition specs for a table along with latest-partition info.

        Args:
            table_name: Table name.
            limit: Maximum partitions to return (default 100).
            project: Optional project override.

        Returns:
            ``(payload, warnings)`` where payload includes ``table_name``,
            ``partitions`` (list of spec strings), ``visible_count``,
            ``has_more``, ``limit``, and ``latest_partition`` (the most recent
            partition spec, or ``None`` if unavailable).
        """
        table = self._get_table(table_name, project=project)
        definition = self._table_definition_from_table(table)
        warnings: 'list[str]' = []

        if not definition.partition_columns:
            payload = {
                "table_name": definition.name,
                "partitions": [],
                "visible_count": 0,
                "has_more": False,
                "limit": limit,
                "latest_partition": None,
                "is_partitioned": False,
            }
            warnings.append(f"Table `{definition.name}` is not partitioned.")
            return payload, warnings

        try:
            window = list(islice(table.iterate_partitions(), limit + 1))
        except Exception as exc:
            raise translate_odps_error(exc) from exc
        has_more = len(window) > limit
        partitions = [str(part.partition_spec) for part in window[:limit]]

        latest_partition = self._max_partition_spec(table)
        if latest_partition is None and partitions:
            latest_partition = partitions[-1]

        payload = {
            "table_name": definition.name,
            "partitions": partitions,
            "visible_count": len(partitions),
            "has_more": has_more,
            "limit": limit,
            "latest_partition": latest_partition,
            "is_partitioned": True,
        }
        if has_more:
            warnings.append(
                f"Only the first {limit} partitions are shown. "
                f"Pass --limit <N> to widen the window."
            )
        if latest_partition and partitions and latest_partition != partitions[-1]:
            warnings.append(
                f"`latest_partition` ({latest_partition}) is newer than the "
                f"last partition shown in the listing window."
            )
        return payload, warnings

    def latest_partition_info(self, table_name: 'str', *, project: 'str | None' = None) -> 'tuple[dict[str, Any], list[str]]':
        """Get the latest partition info for a partitioned table.

        Uses ``table.partitions`` to find the most recent partition by
        creation time.

        Args:
            table_name: Table name.

        Returns:
            Tuple of (payload dict, warnings list). Payload contains
            partition key, value, creation time, and size info.
        """
        table = self._get_table(table_name, project=project)
        definition = self._table_definition_from_table(table)
        return self._latest_partition_info_from_table(table, definition)

    def freshness_info(self, table_name: 'str', *, project: 'str | None' = None) -> 'tuple[dict[str, Any], list[str]]':
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
        table = self._get_table(table_name, project=project)
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
        """List all projects accessible to the current user.

        Tries the unfiltered ``client.list_projects()`` call first (returns
        every project the principal has visibility into); falls back to an
        owner-filtered call when the unfiltered request is denied.

        Note: Only basic info (name) is returned to avoid triggering
        ``project.reload()`` which requires Read permission on each project.
        Use ``get_project_info`` for full details.
        """
        projects: 'list[dict[str, Any]]' = []
        try:
            for project in self.client.list_projects():
                projects.append({"name": project.name})
        except Exception:
            try:
                owner = self._get_owner_display_name()
                for project in self.client.list_projects(owner=owner):
                    projects.append({"name": project.name})
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
            msg = str(exc)
            if "not 3-tier model project" in msg or "is not 3-tier" in msg:
                raise ValidationError(
                    f"Project '{target_project}' does not use the 3-tier "
                    f"namespace model, so it has no schemas.",
                    suggestion=(
                        "Use `maxc meta list-tables` instead. Schemas only "
                        "exist on projects with 3-tier mode enabled."
                    ),
                ) from exc
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
        """Get first N rows from a table.

        Sample rows are best-effort enrichment for describe_table — an ODPS
        read failure (no permission, partition required, etc.) returns [].
        Unrelated Python bugs propagate so they don't silently degrade
        describe output.
        """
        try:
            reader = table.head(limit)
            rows = list(islice(reader, limit))
        except ODPSError:
            return []
        columns = [column.name for column in table.table_schema.columns]
        return [record_to_dict(columns, record.values) for record in rows]

    def _list_partitions(self, table, *, limit: 'int') -> 'list[str]':
        """List partition specs for a table.

        Best-effort: an ODPS metadata failure returns []. Unrelated Python
        bugs propagate.
        """
        try:
            partitions = list(islice(table.iterate_partitions(), limit))
        except ODPSError:
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
        """Get max partition spec from table using get_max_partition if available.

        TypeError indicates the kwarg isn't supported on this PyODPS version —
        fall through to the next signature. ODPSError means the server
        couldn't compute it — best-effort returns None. Unrelated Python
        bugs propagate.
        """
        getter = getattr(table, "get_max_partition", None)
        if callable(getter):
            for kwargs in ({"skip_empty": True}, {}):
                try:
                    partition = getter(**kwargs)
                except TypeError:
                    continue
                except ODPSError:
                    partition = None
                text = partition_spec_text(partition)
                if text:
                    return text
        return None
