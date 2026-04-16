"""Tests for CatalogMixin — tenant_id, catalog_endpoint, catalog_available,
catalog_search_tables — with kv_store caching behaviour."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.backend.catalog import CatalogMixin, _TENANT_ID_KEY, _CATALOG_EP_KEY
from maxc_cli.cache import LocalCache


# ============================================================
# Helpers
# ============================================================

def _make_cache(tmp_path: Path) -> LocalCache:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return LocalCache(cache_dir)


def _make_backend(*, cache: LocalCache | None = None, project: str = "test_proj") -> CatalogMixin:
    """Create a minimal CatalogMixin instance with mocked ODPS client."""
    mixin = CatalogMixin()

    # config mock
    mixin.config = MagicMock()
    mixin.config.default_project = project

    # cache
    mixin._cache = cache

    # client mock — return None by default (no network)
    mixin.client = None

    return mixin


# ============================================================
# _tenant_id — caching behaviour
# ============================================================

class TestTenantId:

    def test_no_client_returns_none(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))
        assert backend._tenant_id is None

    def test_network_call_caches_to_kv_store(self, tmp_path):
        """When client returns tenant_id, it's persisted to kv_store."""
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        # Mock ODPS client
        mock_proj = MagicMock()
        mock_proj.tenant_id = "tenant_abc123"
        mock_odps = MagicMock()
        mock_odps.get_project.return_value = mock_proj
        backend.client = mock_odps

        result = backend._tenant_id
        assert result == "tenant_abc123"

        # Verify kv_store
        key = _TENANT_ID_KEY.format(project="test_proj")
        assert cache.get_kv(key) == "tenant_abc123"

    def test_kv_store_hit_skips_network(self, tmp_path):
        """Cached tenant_id in kv_store avoids network call."""
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        # Pre-populate kv_store
        key = _TENANT_ID_KEY.format(project="test_proj")
        cache.set_kv(key, "tenant_from_cache")

        # Client is None (no network), but should still return cached value
        result = backend._tenant_id
        assert result == "tenant_from_cache"

    def test_l1_process_cache_skips_kv_store(self, tmp_path):
        """After first resolution, L1 cache is used on subsequent calls."""
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        # Pre-populate kv_store
        key = _TENANT_ID_KEY.format(project="test_proj")
        cache.set_kv(key, "tenant_l1_test")

        # First call populates L1
        result1 = backend._tenant_id
        assert result1 == "tenant_l1_test"

        # Delete from kv_store — L1 should still serve
        cache.set_kv(key, "tenant_modified")
        result2 = backend._tenant_id
        assert result2 == "tenant_l1_test"

    def test_kv_store_expired_falls_to_network(self, tmp_path):
        """Expired kv_store entry triggers network call."""
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        # Pre-populate kv_store with old entry (manually inject)
        key = _TENANT_ID_KEY.format(project="test_proj")
        # Write directly, then update the timestamp to be 25h old
        cache.set_kv(key, "tenant_stale")
        with cache._connect() as conn:
            old_time = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
            conn.execute("UPDATE kv_store SET updated_at = ? WHERE key = ?", (old_time, key))

        # Mock client
        mock_proj = MagicMock()
        mock_proj.tenant_id = "tenant_fresh"
        mock_odps = MagicMock()
        mock_odps.get_project.return_value = mock_proj
        backend.client = mock_odps

        result = backend._tenant_id
        assert result == "tenant_fresh"

    def test_network_failure_returns_none(self, tmp_path):
        """Network exception is caught, returns None."""
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        mock_odps = MagicMock()
        mock_odps.get_project.side_effect = Exception("network error")
        backend.client = mock_odps

        result = backend._tenant_id
        assert result is None

    def test_no_cache_still_works(self, tmp_path):
        """Without cache, tenant_id is fetched from network (no persistence)."""
        backend = _make_backend(cache=None)

        mock_proj = MagicMock()
        mock_proj.tenant_id = "tenant_no_cache"
        mock_odps = MagicMock()
        mock_odps.get_project.return_value = mock_proj
        backend.client = mock_odps

        result = backend._tenant_id
        assert result == "tenant_no_cache"


# ============================================================
# _resolved_catalog_endpoint — caching behaviour
# ============================================================

class TestCatalogEndpoint:

    def test_no_client_returns_none(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))
        assert backend._resolved_catalog_endpoint is None

    def test_network_call_caches_to_kv_store(self, tmp_path):
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        mock_odps = MagicMock()
        type(mock_odps).catalog_endpoint = PropertyMock(return_value="https://catalog.cn-shanghai.aliyuncs.com")
        backend.client = mock_odps

        result = backend._resolved_catalog_endpoint
        assert result == "https://catalog.cn-shanghai.aliyuncs.com"

        key = _CATALOG_EP_KEY.format(project="test_proj")
        assert cache.get_kv(key) == "https://catalog.cn-shanghai.aliyuncs.com"

    def test_kv_store_hit_skips_network(self, tmp_path):
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        key = _CATALOG_EP_KEY.format(project="test_proj")
        cache.set_kv(key, "https://cached-catalog.aliyuncs.com")

        result = backend._resolved_catalog_endpoint
        assert result == "https://cached-catalog.aliyuncs.com"

    def test_network_failure_returns_none(self, tmp_path):
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        mock_odps = MagicMock()
        type(mock_odps).catalog_endpoint = PropertyMock(side_effect=Exception("fail"))
        backend.client = mock_odps

        result = backend._resolved_catalog_endpoint
        assert result is None


# ============================================================
# catalog_available
# ============================================================

class TestCatalogAvailable:

    def test_available_when_both_rest_and_tenant(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))

        # Mock _catalog_rest
        backend.__dict__["_catalog_rest_cached"] = MagicMock()
        # Mock _tenant_id via L1 cache
        backend._tenant_id_cached = "tenant_123"

        assert backend.catalog_available is True

    def test_unavailable_without_rest(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))
        backend.__dict__["_catalog_rest_cached"] = None
        backend._tenant_id_cached = "tenant_123"
        assert backend.catalog_available is False

    def test_unavailable_without_tenant(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))
        backend.__dict__["_catalog_rest_cached"] = MagicMock()
        backend._tenant_id_cached = None
        assert backend.catalog_available is False

    def test_unavailable_when_both_none(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))
        backend.__dict__["_catalog_rest_cached"] = None
        backend._tenant_id_cached = None
        assert backend.catalog_available is False


# ============================================================
# catalog_search_tables
# ============================================================

class TestCatalogSearchTables:

    def _make_searchable_backend(self, tmp_path, *, response_body: dict) -> CatalogMixin:
        """Create a backend with mocked catalog_rest that returns the given body."""
        cache = _make_cache(tmp_path)
        backend = _make_backend(cache=cache)

        # Mock tenant_id
        backend._tenant_id_cached = "tenant_search_test"

        # Mock catalog_rest
        mock_resp = MagicMock()
        mock_resp.text = json.dumps(response_body)
        mock_rest = MagicMock()
        mock_rest.endpoint = "https://catalog.example.com"
        mock_rest.request.return_value = mock_resp
        backend.__dict__["_catalog_rest_cached"] = mock_rest

        return backend

    def test_search_returns_matches(self, tmp_path):
        body = {
            "entries": [
                {
                    "displayName": "user_table",
                    "name": "projects/test_proj/schemas/default/tables/user_table",
                    "description": "user data",
                },
                {
                    "displayName": "order_table",
                    "name": "projects/test_proj/schemas/default/tables/order_table",
                    "description": "order data",
                },
            ]
        }
        backend = self._make_searchable_backend(tmp_path, response_body=body)

        results = backend.catalog_search_tables("table")
        assert results is not None
        assert len(results) == 2
        assert results[0]["name"] == "user_table"
        assert results[0]["schema"] == "default"
        assert results[1]["name"] == "order_table"

    def test_search_filters_by_schema(self, tmp_path):
        body = {
            "entries": [
                {
                    "displayName": "t1",
                    "name": "projects/test_proj/schemas/prod/tables/t1",
                    "description": "",
                },
                {
                    "displayName": "t2",
                    "name": "projects/test_proj/schemas/dev/tables/t2",
                    "description": "",
                },
            ]
        }
        backend = self._make_searchable_backend(tmp_path, response_body=body)

        results = backend.catalog_search_tables("t", schema="prod")
        assert results is not None
        assert len(results) == 1
        assert results[0]["name"] == "t1"
        assert results[0]["schema"] == "prod"

    def test_search_empty_entries(self, tmp_path):
        backend = self._make_searchable_backend(tmp_path, response_body={"entries": []})
        results = backend.catalog_search_tables("nothing")
        assert results == []

    def test_search_no_entries_key(self, tmp_path):
        backend = self._make_searchable_backend(tmp_path, response_body={})
        results = backend.catalog_search_tables("nothing")
        assert results == []

    def test_search_null_entries_skipped(self, tmp_path):
        body = {
            "entries": [
                None,
                {"displayName": "valid", "name": "projects/p/schemas/s/tables/valid", "description": ""},
                None,
            ]
        }
        backend = self._make_searchable_backend(tmp_path, response_body=body)
        results = backend.catalog_search_tables("valid")
        assert results is not None
        assert len(results) == 1
        assert results[0]["name"] == "valid"

    def test_search_returns_none_when_no_rest(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))
        backend._tenant_id_cached = "tenant_123"
        backend.__dict__["_catalog_rest_cached"] = None

        results = backend.catalog_search_tables("test")
        assert results is None

    def test_search_returns_none_when_no_tenant(self, tmp_path):
        backend = _make_backend(cache=_make_cache(tmp_path))
        backend._tenant_id_cached = None
        backend.__dict__["_catalog_rest_cached"] = MagicMock()

        results = backend.catalog_search_tables("test")
        assert results is None

    def test_search_exception_returns_none(self, tmp_path):
        """Network/parse error → returns None (caller should fallback)."""
        backend = _make_backend(cache=_make_cache(tmp_path))
        backend._tenant_id_cached = "tenant_123"

        mock_rest = MagicMock()
        mock_rest.endpoint = "https://catalog.example.com"
        mock_rest.request.side_effect = Exception("connection reset")
        backend.__dict__["_catalog_rest_cached"] = mock_rest

        results = backend.catalog_search_tables("test")
        assert results is None

    def test_search_request_params(self, tmp_path):
        """Verify the request is called with correct URL and params."""
        body = {"entries": []}
        backend = self._make_searchable_backend(tmp_path, response_body=body)

        backend.catalog_search_tables("mytable")

        mock_rest = backend._catalog_rest_cached
        call_args = mock_rest.request.call_args
        url = call_args[0][0]
        method = call_args[0][1]
        params = call_args[1].get("params", {})

        assert method == "post"
        assert "tenant_search_test" in url
        assert "namespaces" in url
        assert ":search" in url
        assert "name:mytable" in params["query"]
        assert "type=TABLE" in params["query"]
        assert "project=test_proj" in params["query"]

    def test_search_empty_keyword_no_name_in_query(self, tmp_path):
        """Empty keyword → no name: filter in query (only type + project)."""
        body = {"entries": []}
        backend = self._make_searchable_backend(tmp_path, response_body=body)

        backend.catalog_search_tables("")

        mock_rest = backend._catalog_rest_cached
        params = mock_rest.request.call_args[1].get("params", {})
        query = params["query"]
        assert "name:" not in query
        assert "type=TABLE" in query
