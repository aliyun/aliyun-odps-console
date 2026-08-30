"""Tests for cache.py — _safe_json_loads and LocalCache resilience to bad data."""
import json
import os
import sqlite3
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

import maxc_cli.cache as cache_module
from maxc_cli.app import MaxCApp
from maxc_cli.cache import LocalCache, _safe_json_loads
from maxc_cli.cli import run
from maxc_cli.exceptions import ValidationError


def _write_read_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "default_project: demo_project",
                "default_schema: default",
                f"cache_dir: {tmp_path / 'cache'}",
                f"state_dir: {tmp_path / 'state'}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _filesystem_snapshot(root: Path) -> dict[str, tuple[str, int, int, bytes]]:
    """Capture user-visible filesystem mutations while ignoring read atime."""
    snapshot: dict[str, tuple[str, int, int, bytes]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        path_stat = path.lstat()
        if path.is_dir():
            snapshot[relative] = ("directory", path_stat.st_mode, path_stat.st_mtime_ns, b"")
        elif path.is_symlink():
            snapshot[relative] = (
                "symlink",
                path_stat.st_mode,
                path_stat.st_mtime_ns,
                os.readlink(path).encode(),
            )
        else:
            snapshot[relative] = (
                "file",
                path_stat.st_mode,
                path_stat.st_mtime_ns,
                path.read_bytes(),
            )
    return snapshot

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


class TestReadOnlyCacheCommands:
    @pytest.mark.parametrize(
        "argv",
        [
            ["cache", "status", "--project", "demo_project"],
            ["cache", "build-status", "--project", "demo_project"],
            ["cache", "clear", "--project", "demo_project"],
            ["cache", "clear", "--project", "demo_project", "--dry-run"],
            [
                "meta",
                "semantic",
                "get",
                "orders",
                "--project",
                "demo_project",
                "--schema",
                "sales",
            ],
            [
                "meta",
                "semantic",
                "list-missing",
                "--project",
                "demo_project",
                "--schema",
                "sales",
            ],
        ],
    )
    def test_fresh_read_command_performs_zero_filesystem_writes(
        self,
        tmp_path: Path,
        argv: list[str],
    ) -> None:
        config_path = _write_read_config(tmp_path)
        before = _filesystem_snapshot(tmp_path)
        stdout = StringIO()
        stderr = StringIO()

        exit_code = run(
            ["--config", str(config_path), *argv, "--json"],
            cwd=tmp_path,
            stdout=stdout,
            stderr=stderr,
        )
        payload = json.loads(stdout.getvalue())

        assert exit_code == 0
        assert stderr.getvalue() == ""
        assert payload["status"] == "success"
        assert _filesystem_snapshot(tmp_path) == before
        assert not (tmp_path / "cache").exists()
        assert not (tmp_path / "state").exists()

    def test_read_commands_use_existing_database_without_mutating_it(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = _write_read_config(tmp_path)
        writable = LocalCache(tmp_path / "cache")
        writable.cache_table(
            "demo_project",
            "orders",
            "Sales orders",
            [{"name": "id", "type": "bigint"}],
            schema_name="sales",
        )
        writable.cache_table(
            "demo_project",
            "customers",
            "Customers",
            [{"name": "id", "type": "bigint"}],
            schema_name="sales",
        )
        writable.save_semantic(
            "demo_project",
            "orders",
            "Sales facts",
            [],
            [],
            [],
            schema_name="sales",
        )
        writable.start_build("demo_project", "build-1", 2)
        writable.update_build_progress("demo_project", "build-1", 1, 0)
        app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
        before = _filesystem_snapshot(tmp_path)

        status = app.cache_status(project="demo_project", schema_name="sales")
        build = app.cache_build_status(
            project="demo_project",
            build_id="build-1",
        )
        semantic = app.semantic_get(
            "orders",
            project="demo_project",
            schema="sales",
        )
        missing = app.semantic_list_missing(
            project="demo_project",
            schema="sales",
        )

        assert status.data["table_count"] == 2
        assert status.data["semantic_count"] == 1
        assert build.data["progress_percent"] == 50
        assert semantic.data["semantic"]["semantic_desc"] == "Sales facts"
        assert [table["table_name"] for table in missing.data["tables"]] == [
            "customers"
        ]
        assert _filesystem_snapshot(tmp_path) == before
        assert not (tmp_path / "state").exists()

    def test_read_only_view_tolerates_an_unmigrated_database(
        self,
        tmp_path: Path,
    ) -> None:
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(mode=0o700)
        database = cache_dir / "cache.db"
        with sqlite3.connect(database) as conn:
            conn.execute(
                """
                CREATE TABLE table_metadata (
                    project TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    description TEXT,
                    columns_json TEXT NOT NULL,
                    partitions_json TEXT,
                    row_count INTEGER,
                    size_bytes INTEGER,
                    owner TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO table_metadata VALUES (
                    'demo_project', 'sales', 'orders', 'Sales orders', '[]',
                    NULL, NULL, NULL, NULL, '2026-01-01T00:00:00+00:00'
                )
                """
            )
        if os.name == "posix":
            # A read command must not silently "repair" a legacy/shared-mode
            # cache file; owner-owned 0644 remains readable and unchanged.
            database.chmod(0o644)
        before = _filesystem_snapshot(tmp_path)

        cache = LocalCache(cache_dir, read_only=True)

        assert cache.get_cache_stats("demo_project", "sales")["table_count"] == 1
        assert cache.get_semantic("demo_project", "orders", "sales") is None
        assert cache.get_all_semantics("demo_project", "sales") == []
        assert cache.get_build_status("demo_project") is None
        assert cache.fts_available is False
        assert _filesystem_snapshot(tmp_path) == before

    def test_read_only_status_does_not_migrate_a_legacy_fts_table(
        self,
        tmp_path: Path,
    ) -> None:
        writable = LocalCache(tmp_path / "cache")
        if not writable.fts_available:
            pytest.skip("SQLite runtime does not provide FTS5")
        writable.cache_table(
            "demo_project",
            "orders",
            "Sales orders",
            [],
            schema_name="sales",
        )
        with writable._connect() as conn:
            conn.execute("DROP TABLE table_fts")
            conn.execute(
                """
                CREATE VIRTUAL TABLE table_fts USING fts5(
                    project, table_name, schema_name, description, column_names,
                    column_comments, semantic_desc, use_cases, content='',
                    tokenize='unicode61'
                )
                """
            )
        before = _filesystem_snapshot(tmp_path)

        cache = LocalCache(tmp_path / "cache", read_only=True)

        assert cache.get_cache_stats("demo_project", "sales")["table_count"] == 1
        assert cache.fts_available is True
        assert cache.get_fts_count("demo_project", "sales") == 0
        with cache._connect() as conn:
            sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'table_fts'"
            ).fetchone()["sql"]
        assert "content=''" in sql
        assert _filesystem_snapshot(tmp_path) == before

    @pytest.mark.parametrize(
        "argv",
        [
            ["cache", "status", "--project", "demo_project"],
            ["cache", "build-status", "--project", "demo_project"],
            [
                "meta",
                "semantic",
                "get",
                "orders",
                "--project",
                "demo_project",
                "--schema",
                "sales",
            ],
            [
                "meta",
                "semantic",
                "list-missing",
                "--project",
                "demo_project",
                "--schema",
                "sales",
            ],
        ],
    )
    def test_live_wal_fails_clearly_without_returning_stale_or_writing(
        self,
        tmp_path: Path,
        argv: list[str],
    ) -> None:
        config_path = _write_read_config(tmp_path)
        writable = LocalCache(tmp_path / "cache")
        writer = sqlite3.connect(writable.db_path)
        try:
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute(
                """
                INSERT INTO table_metadata(
                    project, schema_name, table_name, description, columns_json,
                    partitions_json, updated_at
                ) VALUES ('demo_project', 'sales', 'orders', 'in WAL', '[]', NULL, 'now')
                """
            )
            writer.commit()
            assert writable.db_path.with_name("cache.db-wal").exists()
            before = _filesystem_snapshot(tmp_path)
            stdout = StringIO()
            stderr = StringIO()

            exit_code = run(
                ["--config", str(config_path), *argv, "--json"],
                cwd=tmp_path,
                stdout=stdout,
                stderr=stderr,
            )
            payload = json.loads(stdout.getvalue())

            assert exit_code == 1
            assert stderr.getvalue() == ""
            assert payload["status"] == "failure"
            assert payload["error"]["code"] == "CACHE_SNAPSHOT_BUSY"
            assert payload["error"]["recoverable"] is True
            assert "cache build --json" in payload["error"]["suggestion"]
            assert payload["error"]["recovery_steps"]
            assert _filesystem_snapshot(tmp_path) == before
            assert not (tmp_path / "state").exists()
        finally:
            writer.close()

    def test_checkpointed_zero_byte_wal_is_a_readable_snapshot(
        self,
        tmp_path: Path,
    ) -> None:
        cache_dir = tmp_path / "cache"
        writable = LocalCache(cache_dir)
        writable.cache_table(
            "demo_project",
            "orders",
            "checkpointed",
            [],
            schema_name="sales",
        )
        # Some supported SQLite builds retain these files after close. Model
        # that portable state even on runtimes that remove them automatically.
        wal_path = writable.db_path.with_name("cache.db-wal")
        shm_path = writable.db_path.with_name("cache.db-shm")
        wal_path.touch(mode=0o600, exist_ok=True)
        shm_path.touch(mode=0o600, exist_ok=True)
        before = _filesystem_snapshot(tmp_path)

        reader = LocalCache(cache_dir, read_only=True)
        result = reader.get_cache_stats("demo_project", "sales")

        assert result["table_count"] == 1
        assert _filesystem_snapshot(tmp_path) == before

    def test_completed_writer_between_snapshot_checks_cannot_return_success(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        config_path = _write_read_config(tmp_path)
        writable = LocalCache(tmp_path / "cache")
        writable.cache_table(
            "demo_project",
            "orders",
            "before",
            [],
            schema_name="sales",
        )
        reader = LocalCache(tmp_path / "cache", read_only=True)
        original_verify = reader._verify_pinned_database
        verify_calls = 0

        def verify_then_complete_write(directory, database_descriptor):
            nonlocal verify_calls
            verify_calls += 1
            original_verify(directory, database_descriptor)
            if verify_calls != 2:
                return
            with sqlite3.connect(writable.db_path) as writer:
                writer.execute("PRAGMA wal_autocheckpoint=0")
                writer.execute(
                    """
                    UPDATE table_metadata
                    SET description = 'committed between checks'
                    WHERE project = 'demo_project'
                      AND schema_name = 'sales'
                      AND table_name = 'orders'
                    """
                )
                writer.commit()
                checkpoint = writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                assert checkpoint[0] == 0
            wal_path = writable.db_path.with_name("cache.db-wal")
            if wal_path.exists():
                assert wal_path.stat().st_size == 0
                wal_path.unlink()
            assert not wal_path.exists()

        monkeypatch.setattr(reader, "_verify_pinned_database", verify_then_complete_write)
        app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
        monkeypatch.setattr(app, "_read_only_cache", lambda: reader)

        result = app.cache_status(project="demo_project", schema_name="sales")

        assert result.status == "failure"
        assert result.data is None
        assert result.error.code == "CACHE_SNAPSHOT_BUSY"
        assert result.error.recoverable is True
        assert verify_calls == 2

    def test_database_created_between_fresh_snapshot_reads_fails_closed(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        config_path = _write_read_config(tmp_path)
        reader = LocalCache(tmp_path / "cache", read_only=True)
        original_stats = reader.get_cache_stats

        def observe_missing_then_create(project, schema_name=None):
            result = original_stats(project, schema_name)
            writable = LocalCache(tmp_path / "cache")
            writable.cache_table(
                "demo_project",
                "orders",
                "created between reads",
                [],
                schema_name="sales",
            )
            writable.save_semantic(
                "demo_project",
                "orders",
                "new semantic state",
                [],
                [],
                [],
                schema_name="sales",
            )
            return result

        monkeypatch.setattr(reader, "get_cache_stats", observe_missing_then_create)
        app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
        monkeypatch.setattr(app, "_read_only_cache", lambda: reader)

        result = app.cache_status(project="demo_project", schema_name="sales")

        assert result.status == "failure"
        assert result.data is None
        assert result.error.code == "CACHE_SNAPSHOT_BUSY"
        assert result.error.recoverable is True

    def test_read_only_view_rejects_a_symlink_without_touching_target(
        self,
        tmp_path: Path,
    ) -> None:
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        victim = tmp_path / "victim.db"
        with sqlite3.connect(victim) as conn:
            conn.execute("CREATE TABLE table_metadata(value TEXT)")
        if os.name == "posix":
            victim.chmod(0o600)
        (cache_dir / "cache.db").symlink_to(victim)
        before = _filesystem_snapshot(tmp_path)

        cache = LocalCache(cache_dir, read_only=True)
        with pytest.raises(ValidationError, match="database path is unsafe"):
            cache.get_cache_stats("demo_project")

        assert _filesystem_snapshot(tmp_path) == before

    def test_read_only_view_detects_path_swap_before_returning_data(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        cache_dir = tmp_path / "cache"
        writable = LocalCache(cache_dir)
        writable.cache_table("demo_project", "orders", "orders", [])
        reader = LocalCache(cache_dir, read_only=True)
        displaced = tmp_path / "cache-original"
        victim = tmp_path / "victim.db"
        original_connect = cache_module.sqlite3.connect
        with original_connect(victim) as conn:
            conn.execute("CREATE TABLE table_metadata(value TEXT)")
        if os.name == "posix":
            victim.chmod(0o600)
        victim_before = victim.read_bytes()
        swapped = False

        def swapping_connect(path, *args, **kwargs):
            nonlocal swapped
            if not swapped:
                swapped = True
                cache_dir.rename(displaced)
                cache_dir.mkdir()
                (cache_dir / "cache.db").symlink_to(victim)
            return original_connect(path, *args, **kwargs)

        monkeypatch.setattr(cache_module.sqlite3, "connect", swapping_connect)

        with pytest.raises(ValidationError, match="path changed while it was in use"):
            reader.get_cache_stats("demo_project")

        assert victim.read_bytes() == victim_before

    @pytest.mark.skipif(os.name != "posix", reason="directory swap reproducer is POSIX-specific")
    def test_read_only_snapshot_is_descriptor_bound_during_swap_and_restore(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        cache_dir = tmp_path / "cache"
        original = LocalCache(cache_dir)
        original.cache_table("demo_project", "expected", "original", [])

        attacker_dir = tmp_path / "attacker-cache"
        attacker = LocalCache(attacker_dir)
        for index in range(7):
            attacker.cache_table(
                "demo_project",
                f"attacker_{index}",
                "untrusted",
                [],
            )

        reader = LocalCache(cache_dir, read_only=True)
        displaced = tmp_path / "cache-original"
        original_connect = cache_module.sqlite3.connect
        swapped = False

        def swap_only_while_sqlite_opens(path, *args, **kwargs):
            nonlocal swapped
            if swapped:
                return original_connect(path, *args, **kwargs)
            swapped = True
            cache_dir.rename(displaced)
            attacker_dir.rename(cache_dir)
            try:
                return original_connect(path, *args, **kwargs)
            finally:
                cache_dir.rename(attacker_dir)
                displaced.rename(cache_dir)

        monkeypatch.setattr(
            cache_module.sqlite3,
            "connect",
            swap_only_while_sqlite_opens,
        )

        result = reader.get_cache_stats("demo_project")

        assert swapped is True
        assert result["table_count"] == 1
        assert cache_dir.exists()
        assert attacker_dir.exists()


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


class TestFullTextCache:
    @pytest.fixture()
    def cache(self, tmp_path: Path) -> LocalCache:
        cache = LocalCache(tmp_path / "cache")
        if not cache.fts_available:
            pytest.skip("SQLite runtime does not provide FTS5")
        return cache

    @staticmethod
    def _cache_orders(cache: LocalCache, *, project: str = "project_a") -> None:
        cache.cache_table(
            project,
            "orders",
            "Sales order facts",
            [
                {"name": "customer_id", "type": "bigint", "comment": "Buyer key"},
                {"name": "amount", "type": "double", "comment": "Revenue"},
            ],
            schema_name="sales",
        )

    def test_metadata_is_searchable_with_identifiers_and_project_scope(
        self, cache: LocalCache
    ) -> None:
        self._cache_orders(cache)
        self._cache_orders(cache, project="project_b")

        results = cache.fts_search("buyer", project="project_a")

        assert len(results) == 1
        assert results[0]["table_name"] == "orders"
        assert results[0]["schema_name"] == "sales"
        assert "<b>Buyer</b> key" in results[0]["snippet"]

    def test_semantic_upsert_replaces_fts_row_instead_of_duplicating(
        self, cache: LocalCache
    ) -> None:
        self._cache_orders(cache)
        cache.save_semantic(
            "project_a", "orders", "Commerce facts", ["historical cohort"], [], [],
            schema_name="sales",
        )
        cache.save_semantic(
            "project_a", "orders", "Retention facts", ["cohort retention"], [], [],
            schema_name="sales",
        )

        assert cache.fts_search("historical", project="project_a") == []
        assert len(cache.fts_search("retention", project="project_a")) == 1
        assert cache.get_fts_count("project_a", "sales") == 1

    def test_clear_metadata_removes_fts_but_preserves_semantic(
        self, cache: LocalCache
    ) -> None:
        self._cache_orders(cache)
        cache.save_semantic(
            "project_a", "orders", "Commerce facts", ["revenue"], [], [],
            schema_name="sales",
        )

        assert cache.clear_table_cache("project_a", "sales") == 1
        assert cache.fts_search("revenue", project="project_a") == []
        assert cache.get_semantic_count("project_a", "sales") == 1

    def test_clear_semantic_keeps_metadata_searchable(self, cache: LocalCache) -> None:
        self._cache_orders(cache)
        cache.save_semantic(
            "project_a", "orders", "Commerce facts", ["retention"], [], [],
            schema_name="sales",
        )

        assert cache.clear_semantic("project_a", "orders", "sales") == 1
        assert cache.fts_search("retention", project="project_a") == []
        assert len(cache.fts_search("orders", project="project_a")) == 1

    def test_contentless_legacy_index_is_migrated_and_rebuilt(
        self, cache: LocalCache
    ) -> None:
        self._cache_orders(cache)
        with cache._connect() as conn:
            conn.execute("DROP TABLE table_fts")
            conn.execute(
                """
                CREATE VIRTUAL TABLE table_fts USING fts5(
                    project, table_name, schema_name, description, column_names,
                    column_comments, semantic_desc, use_cases, content='',
                    tokenize='unicode61'
                )
                """
            )
        migrated = LocalCache(cache.db_path.parent)

        results = migrated.fts_search("orders", project="project_a")

        assert len(results) == 1
        assert results[0]["table_name"] == "orders"
        assert results[0]["schema_name"] == "sales"


class TestKvPrefixDeletion:
    def test_deletes_only_exact_literal_prefix(self, tmp_path: Path) -> None:
        cache = LocalCache(tmp_path / "cache")
        cache.set_kv(r"ext_creds:%_\\account_a", "a")
        cache.set_kv(r"ext_creds:%_\\account_b", "b")
        cache.set_kv("ext_creds:other", "other")
        cache.set_kv("tenant_id:project", "tenant")

        deleted = cache.delete_kv_prefix(r"ext_creds:%_\\")

        assert deleted == 2
        assert cache.get_kv(r"ext_creds:%_\\account_a") is None
        assert cache.get_kv(r"ext_creds:%_\\account_b") is None
        assert cache.get_kv("ext_creds:other") == "other"
        assert cache.get_kv("tenant_id:project") == "tenant"

    def test_rejects_empty_prefix(self, tmp_path: Path) -> None:
        cache = LocalCache(tmp_path / "cache")
        cache.set_kv("keep", "value")

        with pytest.raises(ValidationError, match="must not be empty"):
            cache.delete_kv_prefix("")

        assert cache.get_kv("keep") == "value"
