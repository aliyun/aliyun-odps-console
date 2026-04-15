"""Catalog API mixin for OdpsBackend — server-side search via pyodps_catalog."""

from typing import Any

from ..config import TableDefinition


class CatalogMixin:
    """Mixin providing Catalog API methods.

    Requires ``self._catalog_client`` (lazy-initialized via
    ``self.resolved_auth.create_catalog_client()``) and
    ``self.config`` to be present on the host class.
    """

    @property
    def catalog_client(self):
        """Lazy-initialize and cache the Catalog API client.

        Returns None if pyodps_catalog is not installed or catalog endpoint
        is not configured.
        """
        if not hasattr(self, "_catalog_client_cached"):
            try:
                odps_client = getattr(self, "client", None)
                self._catalog_client_cached = self.resolved_auth.create_catalog_client(
                    odps_client=odps_client,
                )
            except Exception:
                self._catalog_client_cached = None
        return self._catalog_client_cached

    def catalog_search_tables(
        self,
        keyword: str,
        *,
        schema: 'str | None' = None,
        page_size: int = 50,
    ) -> 'list[dict[str, Any]] | None':
        """Search tables via Catalog API server-side full-text search.

        Args:
            keyword: Search term — matched against table name and description.
            schema: Optional schema to scope the search.
            page_size: Results per page (max 100).

        Returns:
            List of dicts with keys: name, schema, comment, owner.
            Returns None if Catalog API is unavailable (caller should fallback).

        Raises:
            Nothing — exceptions are caught and None is returned to signal
            fallback to the caller.
        """
        client = self.catalog_client
        if client is None:
            return None

        try:
            # Build the query string per Catalog API spec
            # Required: type=TABLE
            # Optional: project=<proj>, name:<keyword>, description:<keyword>
            parts = [f"type=TABLE"]
            project = self.config.default_project
            if project:
                parts.append(f"project={project}")
            if keyword:
                parts.append(f"name:{keyword}")

            query = ",".join(parts)

            # namespace_id is the main account ID; for AK/SK auth it can be
            # derived from the endpoint or left as "*" for cross-account search.
            # Using "*" as default — the server enforces project-level ACL.
            namespace_id = "*"

            response = client.search(
                namespace_id=namespace_id,
                query=query,
                page_size=min(page_size, 100),
                page_token="",
                order_by="default",
            )

            if response is None:
                return None

            entries = response.entries or []
            matches: 'list[dict[str, Any]]' = []
            for entry in entries:
                if entry is None:
                    continue
                # entry.name: "projects/{id}/schemas/{schema}/tables/{table}"
                # entry.display_name: table name
                table_name = entry.display_name or ""
                entry_schema = ""
                if entry.name:
                    parts = entry.name.split("/")
                    # Extract schema from path if present
                    for i, p in enumerate(parts):
                        if p == "schemas" and i + 1 < len(parts):
                            entry_schema = parts[i + 1]
                            break

                # Filter by schema if requested
                if schema and entry_schema and entry_schema.lower() != schema.lower():
                    continue

                matches.append({
                    "name": table_name,
                    "schema": entry_schema,
                    "comment": entry.description or "",
                    "owner": "",
                })

            return matches

        except Exception:
            # Catalog API unavailable — signal fallback
            return None
