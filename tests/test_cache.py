"""Tests for cache.py — _safe_json_loads and LocalCache resilience to bad data."""
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.cache import LocalCache, _safe_json_loads

# ============================================================
# _safe_json_loads unit tests
# ============================================================

class TestSafeJsonLoads:
    def test_valid_json_list(self):
        assert _safe_json_loads('[1, 2, 3]') == [1, 2, 3]

    def test_valid_json_dict(self):
        assert _safe_json_loads('{"a": 1}') == {"a": 1}

    def test_valid_json_string(self):
        assert _safe_json_loads('"hello"') == "hello"

    def test_none_returns_default_list(self):
        assert _safe_json_loads(None) == []

    def test_empty_string_returns_default_list(self):
        assert _safe_json_loads("") == []

    def test_none_with_explicit_default_none(self):
        assert _safe_json_loads(None, default=None) is None

    def test_empty_with_explicit_default_dict(self):
        assert _safe_json_loads("", default={"x": 1}) == {"x": 1}

    def test_corrupted_json_returns_default_list(self):
        assert _safe_json_loads("{not valid json}") == []

    def test_corrupted_json_with_explicit_default_none(self):
        assert _safe_json_loads("{broken", default=None) is None

    def test_truncated_json(self):
        assert _safe_json_loads('["a", "b"') == []

    def test_non_string_input(self):
        # int input is not valid for json.loads → TypeError → returns default
        assert _safe_json_loads(12345) == []


# ============================================================
# LocalCache resilience tests
# ============================================================

class TestLocalCacheResilience:
    """Verify LocalCache functions don't crash when DB contains corrupted JSON."""

    @pytest.fixture()
    def cache(self, tmp_path: Path) -> LocalCache:
        return LocalCache(tmp_path / "cache")

    def _insert_corrupted_table(self, cache: LocalCache, table_name: str = "bad_table"):
        """Insert a row with corrupted JSON in columns_json and partitions_json."""
        with cache._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO table_metadata
                   (project, schema_name, table_name, description, columns_json, partitions_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                ("test_project", "default", table_name, "test table", "{CORRUPTED", "NOT_JSON"),
            )

    def _insert_corrupted_semantic(self, cache: LocalCache, table_name: str = "bad_table"):
        """Insert a row with corrupted JSON in semantic metadata."""
        with cache._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO table_semantic
                   (project, schema_name, table_name, semantic_desc, use_cases,
                    sample_questions, column_semantics_json, relations_json, stats_json, generated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
                ("test_project", "default", table_name, "desc",
                 "NOT[JSON", "{BAD}", "TRUNCATED[", "{{", "NOPE"),
            )

    def test_get_cached_table_with_corrupted_json(self, cache: LocalCache):
        self._insert_corrupted_table(cache)
        result = cache.get_cached_table("test_project", "bad_table")
        assert result is not None
        assert result["columns"] == []
        assert result["partitions"] == []

    def test_get_all_cached_tables_with_corrupted_json(self, cache: LocalCache):
        self._insert_corrupted_table(cache)
        result = cache.get_all_cached_tables("test_project", "default")
        assert len(result) == 1
        assert result[0]["table_name"] == "bad_table"
        assert result[0]["columns"] == []
        assert result[0]["partitions"] == []

    def test_get_tables_by_name_with_corrupted_json(self, cache: LocalCache):
        self._insert_corrupted_table(cache, "lookup_table")
        results = cache.get_tables_by_name("test_project", "lookup_table")
        assert len(results) == 1
        assert results[0]["columns"] == []
        assert results[0]["partitions"] == []

    def test_get_semantic_with_corrupted_json(self, cache: LocalCache):
        self._insert_corrupted_semantic(cache)
        result = cache.get_semantic("test_project", "bad_table")
        assert result is not None
        assert result["use_cases"] == []
        assert result["sample_questions"] == []
        assert result["column_semantics"] == []
        assert result["relations"] == []
        assert result["stats"] is None

    def test_get_all_semantics_with_corrupted_json(self, cache: LocalCache):
        self._insert_corrupted_semantic(cache, "sem_table")
        results = cache.get_all_semantics("test_project", "default")
        assert len(results) >= 1
        match = next(r for r in results if r["table_name"] == "sem_table")
        assert match["use_cases"] == []
        assert match["stats"] is None
