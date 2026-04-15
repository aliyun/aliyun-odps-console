"""Catalog API mixin for OdpsBackend — server-side search via pyodps RestClient.

Uses ``ODPS.catalog_rest`` (RestClient with auth already wired) to call
the Catalog search endpoint directly, **without** depending on
``pyodps_catalog`` SDK (which pulls in ``aiohttp``).

Endpoint pattern:
    POST {catalog_endpoint}/api/catalog/v1alpha/namespaces/{tenant_id}:search

Auto-routing:
    ``ODPS.catalog_endpoint`` resolves the catalog host via
    ``GET {odps_endpoint}/catalogapi`` → region-based default → cached.
    No manual ``catalog_endpoint`` config needed.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class CatalogMixin:
    """Mixin providing Catalog API methods.

    Requires ``self.client`` (the ODPS instance, set by OdpsBackend) and
    ``self.config`` to be present on the host class.
    """

    @property
    def _catalog_rest(self):
        """Lazy-init the catalog RestClient from the ODPS instance.

        Returns None if:
        - pyodps is not installed
        - ODPS.catalog_endpoint cannot be resolved (e.g. no network)
        """
        if not hasattr(self, "_catalog_rest_cached"):
            try:
                odps = self.client
                if odps is not None and odps.catalog_rest is not None:
                    self._catalog_rest_cached = odps.catalog_rest
                else:
                    self._catalog_rest_cached = None
            except Exception:
                self._catalog_rest_cached = None
        return self._catalog_rest_cached

    @property
    def _tenant_id(self):
        """Lazy-resolve tenant_id from the ODPS project.

        Returns None if project info cannot be fetched.
        """
        if not hasattr(self, "_tenant_id_cached"):
            try:
                odps = self.client
                if odps is not None:
                    proj = odps.get_project(self.config.default_project)
                    self._tenant_id_cached = proj.tenant_id
                else:
                    self._tenant_id_cached = None
            except Exception:
                self._tenant_id_cached = None
        return self._tenant_id_cached

    @property
    def catalog_available(self):
        """Whether Catalog API is reachable."""
        return self._catalog_rest is not None and self._tenant_id is not None

    def catalog_search_tables(
        self,
        keyword: str,
        *,
        schema: 'str | None' = None,
        page_size: int = 50,
    ) -> 'list[dict[str, Any]] | None':
        """Search tables via Catalog API server-side full-text search.

        Args:
            keyword: Search term — matched against table name (substring).
            schema: Optional schema to scope the search.
            page_size: Results per page (max 100).

        Returns:
            List of dicts with keys: name, schema, comment, owner.
            Returns None if Catalog API is unavailable (caller should fallback).

        Raises:
            Nothing — exceptions are caught and None is returned to signal
            fallback to the caller.
        """
        catalog_rest = self._catalog_rest
        tenant_id = self._tenant_id
        if catalog_rest is None or tenant_id is None:
            return None

        try:
            base = catalog_rest.endpoint.rstrip("/")
            url = f"{base}/api/catalog/v1alpha/namespaces/{tenant_id}:search"

            # Build query string per Catalog API spec:
            #   name:{keyword}  — substring match on entity name
            #   type=TABLE      — required filter
            #   project={proj}  — scope to project
            parts = ["type=TABLE"]
            project = self.config.default_project
            if project:
                parts.append(f"project={project}")
            if keyword:
                parts.append(f"name:{keyword}")
            query = ",".join(parts)

            params = {
                "query": query,
                "pageSize": str(min(page_size, 100)),
                "orderBy": "default",
            }

            resp = catalog_rest.request(url, "post", params=params, curr_project=project)
            body = resp.text if hasattr(resp, "text") else resp.content.decode("utf-8")
            data = json.loads(body)

            entries = data.get("entries") or []
            matches: 'list[dict[str, Any]]' = []
            for entry in entries:
                if entry is None:
                    continue

                display_name = entry.get("displayName", "")
                full_name = entry.get("name", "")  # projects/X/schemas/Y/tables/Z
                entry_schema = ""
                if full_name:
                    path_parts = full_name.split("/")
                    for i, p in enumerate(path_parts):
                        if p == "schemas" and i + 1 < len(path_parts):
                            entry_schema = path_parts[i + 1]
                            break

                # Filter by schema if requested
                if schema and entry_schema and entry_schema.lower() != schema.lower():
                    continue

                matches.append({
                    "name": display_name,
                    "schema": entry_schema,
                    "comment": entry.get("description", ""),
                    "owner": "",
                })

            return matches

        except Exception:
            logger.debug("Catalog API search failed, will fallback to client-side", exc_info=True)
            return None
