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

Caching:
    tenant_id and catalog_endpoint are cached in the LocalCache kv_store
    table with a 24-hour TTL, avoiding repeated network round-trips for
    values that rarely change.  On subsequent process invocations the
    cached endpoint is used to construct a RestClient directly, bypassing
    the GET /catalogapi discovery call entirely.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# KV store keys — scoped by project to handle multi-tenant setups
_TENANT_ID_KEY = "tenant_id:{project}"
_CATALOG_EP_KEY = "catalog_endpoint:{project}"
# Cache TTL — these values rarely change; 24h is conservative
_CACHE_TTL_HOURS = 24


class CatalogMixin:
    """Mixin providing Catalog API methods.

    Requires ``self.client`` (the ODPS instance, set by OdpsBackend),
    ``self.config``, and ``self._cache`` (optional LocalCache) to be
    present on the host class.
    """

    @property
    def _catalog_cache(self):
        """Access the cache, returning None if unavailable."""
        return getattr(self, "_cache", None)

    def _cache_key(self, template: str) -> str:
        project = getattr(self.config, "default_project", "") or ""
        return template.format(project=project)

    # ------------------------------------------------------------------
    # tenant_id
    # ------------------------------------------------------------------

    @property
    def _tenant_id(self):
        """Resolve tenant_id with caching.

        Resolution order:
        1. Process-level cache (``_tenant_id_cached``)
        2. LocalCache kv_store (24h TTL)
        3. Network call: ``project.tenant_id``
        """
        if hasattr(self, "_tenant_id_cached"):
            return self._tenant_id_cached

        # Try LocalCache
        cache = self._catalog_cache
        if cache is not None:
            cached = cache.get_kv(
                self._cache_key(_TENANT_ID_KEY),
                max_age_hours=_CACHE_TTL_HOURS,
            )
            if cached is not None:
                self._tenant_id_cached = cached
                return cached

        # Network call
        try:
            odps = self.client
            if odps is not None:
                proj = odps.get_project(self.config.default_project)
                tid = proj.tenant_id
                if tid is not None and cache is not None:
                    cache.set_kv(self._cache_key(_TENANT_ID_KEY), str(tid))
                self._tenant_id_cached = str(tid) if tid else None
                return self._tenant_id_cached
        except Exception:
            pass

        self._tenant_id_cached = None
        return None

    # ------------------------------------------------------------------
    # catalog_endpoint
    # ------------------------------------------------------------------

    @property
    def _resolved_catalog_endpoint(self):
        """Resolve catalog endpoint with caching.

        Resolution order:
        1. Process-level cache (``_catalog_ep_cached``)
        2. LocalCache kv_store (24h TTL)
        3. Network call: ``ODPS.catalog_endpoint`` (auto-routing)
        """
        if hasattr(self, "_catalog_ep_cached"):
            return self._catalog_ep_cached

        # Try LocalCache
        cache = self._catalog_cache
        if cache is not None:
            cached = cache.get_kv(
                self._cache_key(_CATALOG_EP_KEY),
                max_age_hours=_CACHE_TTL_HOURS,
            )
            if cached is not None:
                self._catalog_ep_cached = cached
                return cached

        # Network call via pyodps auto-routing
        try:
            odps = self.client
            if odps is not None:
                ep = odps.catalog_endpoint
                if ep is not None and cache is not None:
                    cache.set_kv(self._cache_key(_CATALOG_EP_KEY), ep)
                self._catalog_ep_cached = ep
                return ep
        except Exception:
            pass

        self._catalog_ep_cached = None
        return None

    # ------------------------------------------------------------------
    # catalog_rest (RestClient with auth)
    # ------------------------------------------------------------------

    @property
    def _catalog_rest(self):
        """Lazy-init the catalog RestClient.

        Priority:
        1. If we already have a cached catalog_endpoint in kv_store,
           construct a RestClient directly (avoids GET /catalogapi).
        2. Fall back to ``ODPS.catalog_rest`` (triggers auto-routing).

        Returns None if catalog is unavailable.
        """
        if not hasattr(self, "_catalog_rest_cached"):
            try:
                odps = self.client
                if odps is None:
                    self._catalog_rest_cached = None
                else:
                    # Check kv_store first for cached endpoint
                    cached_ep = self._resolved_catalog_endpoint
                    if cached_ep is not None:
                        # Construct RestClient directly with cached endpoint
                        self._catalog_rest_cached = odps._rest_client_cls(
                            odps.account,
                            cached_ep.rstrip("/"),
                            odps.project,
                            odps.schema,
                            app_account=odps.app_account,
                            region_name=odps.region_name,
                            namespace=odps.namespace,
                            tag="Catalog",
                            **odps._rest_client_kwargs,
                        )
                    else:
                        # Fall back to pyodps auto-routing
                        self._catalog_rest_cached = odps.catalog_rest
            except Exception:
                self._catalog_rest_cached = None
        return self._catalog_rest_cached

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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
            base = (catalog_rest.endpoint or "").rstrip("/")
            if not base:
                return None
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
            matches: list[dict[str, Any]] = []
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
