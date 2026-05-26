"""SQLite-based local cache for query sessions and metadata."""


import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from .exceptions import ValidationError
from .utils import now_utc_iso

_UNSET = object()


def _safe_json_loads(text, default=_UNSET):
    """Parse JSON text, returning *default* on failure or empty input.

    When *default* is not provided, falls back to ``[]``.
    """
    if not text:
        return [] if default is _UNSET else default
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return [] if default is _UNSET else default


class LocalCache:
    """Lightweight SQLite cache for query sessions and metadata."""

    _INIT_RETRIES = 5

    def __init__(self, cache_dir: 'Path'):
        self.db_path = cache_dir / "cache.db"
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ValidationError(
                f"Local cache directory is unavailable: {self.db_path.parent}",
                suggestion="Set `HOME` or `cache_dir` to a writable location before using cache-backed commands.",
            ) from exc
        self._init_db()

    def _init_db(self) -> 'None':
        for attempt in range(self._INIT_RETRIES):
            try:
                with self._connect() as conn:
                    # Prefer WAL mode, but fall back to the default journal if another
                    # process is currently initializing the database.
                    try:
                        conn.execute("PRAGMA journal_mode=WAL")
                    except ValidationError as exc:
                        if not self._is_lock_error(exc.message):
                            raise
                    conn.executescript("""
                CREATE TABLE IF NOT EXISTS query_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    project TEXT NOT NULL,
                    sql TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sessions_job_id ON query_sessions(job_id);
                CREATE INDEX IF NOT EXISTS idx_sessions_created ON query_sessions(created_at);

                CREATE TABLE IF NOT EXISTS table_metadata (
                    project TEXT NOT NULL,
                    schema_name TEXT NOT NULL DEFAULT 'default',
                    table_name TEXT NOT NULL,
                    description TEXT,
                    columns_json TEXT NOT NULL,
                    partitions_json TEXT,
                    row_count INTEGER,
                    size_bytes INTEGER,
                    owner TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (project, schema_name, table_name)
                );
                CREATE INDEX IF NOT EXISTS idx_table_meta_project ON table_metadata(project);
                CREATE INDEX IF NOT EXISTS idx_table_meta_project_schema ON table_metadata(project, schema_name);
                CREATE INDEX IF NOT EXISTS idx_table_meta_table_name ON table_metadata(table_name);
                CREATE INDEX IF NOT EXISTS idx_table_meta_updated ON table_metadata(updated_at DESC);

                -- AI-generated semantic metadata for NL2SQL
                CREATE TABLE IF NOT EXISTS table_semantic (
                    project TEXT NOT NULL,
                    schema_name TEXT NOT NULL DEFAULT 'default',
                    table_name TEXT NOT NULL,
                    semantic_desc TEXT,
                    use_cases TEXT,
                    sample_questions TEXT,
                    column_semantics_json TEXT,
                    
                    -- Relations and statistics
                    relations_json TEXT,
                    stats_json TEXT,
                    
                    -- Metadata
                    embedding BLOB,
                    generated_at TEXT NOT NULL,
                    generated_by TEXT DEFAULT 'agent',
                    version INTEGER DEFAULT 1,
                    
                    PRIMARY KEY (project, schema_name, table_name)
                );
                CREATE INDEX IF NOT EXISTS idx_semantic_project ON table_semantic(project);
                CREATE INDEX IF NOT EXISTS idx_semantic_project_schema ON table_semantic(project, schema_name);

                -- FTS5 full-text index for keyword search
                CREATE VIRTUAL TABLE IF NOT EXISTS table_fts USING fts5(
                    project,
                    table_name,
                    schema_name,
                    description,
                    column_names,
                    column_comments,
                    semantic_desc,
                    use_cases,
                    content='',
                    tokenize='unicode61'
                );

                -- Cache build status tracking
                CREATE TABLE IF NOT EXISTS cache_build_status (
                    project TEXT NOT NULL,
                    build_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_tables INTEGER DEFAULT 0,
                    processed_tables INTEGER DEFAULT 0,
                    failed_tables INTEGER DEFAULT 0,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    error_message TEXT,
                    PRIMARY KEY (project, build_id)
                );
                CREATE INDEX IF NOT EXISTS idx_build_status_project ON cache_build_status(project);
                CREATE INDEX IF NOT EXISTS idx_build_status_started ON cache_build_status(started_at DESC);

                -- Generic key-value store for low-churn metadata
                -- (tenant_id, catalog_endpoint, etc.)
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
                return
            except ValidationError as exc:
                if self._is_lock_error(exc.message) and attempt < self._INIT_RETRIES - 1:
                    time.sleep(0.05 * (attempt + 1))
                    continue
                raise

    @contextmanager
    def _connect(self) -> 'Generator[sqlite3.Connection, None, None]':
        # Increased timeout to 30 seconds to prevent lock contention in concurrent scenarios
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        except sqlite3.Error as exc:
            raise self._translate_sqlite_error(exc) from exc
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except sqlite3.Error as exc:
            conn.rollback()
            raise self._translate_sqlite_error(exc) from exc
        finally:
            conn.close()

    def _translate_sqlite_error(self, exc: 'sqlite3.Error') -> 'ValidationError':
        message = str(exc)
        if self._is_lock_error(message):
            return ValidationError(
                f"Local cache is busy: {message}",
                suggestion="Retry the command in a moment, or avoid starting multiple maxc processes against the same cache at once.",
            )
        if "unable to open database file" in message.lower():
            return ValidationError(
                f"Local cache database is unavailable: {self.db_path}",
                suggestion="Set `HOME` or `cache_dir` to a writable location before using cache-backed commands.",
            )
        return ValidationError(
            f"Local cache error: {message}",
            suggestion="Check the cache path and local SQLite state before retrying.",
        )

    @staticmethod
    def _is_lock_error(message: 'str') -> 'bool':
        lowered = message.lower()
        return "database is locked" in lowered or "database table is locked" in lowered

    def create_session(
        self,
        job_id: 'str',
        project: 'str',
        sql: 'str | None' = None,
    ) -> 'int':
        """Create a new query session, return session_id."""
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO query_sessions (job_id, project, sql, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, project, sql, now_utc_iso()),
            )
            return cursor.lastrowid  # type: ignore

    def get_session(self, session_id: 'int') -> 'dict[str, Any] | None':
        """Get session by id."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, job_id, project, sql, created_at FROM query_sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if row:
                return dict(row)
            return None

    def find_session_by_job_id(self, job_id: 'str') -> 'dict[str, Any] | None':
        """Find existing session by job_id (for deduplication)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, job_id, project, sql, created_at FROM query_sessions WHERE job_id = ? ORDER BY id DESC LIMIT 1",
                (job_id,),
            ).fetchone()
            if row:
                return dict(row)
            return None

    def cleanup_old_sessions(self, keep_hours: 'int' = 24) -> 'int':
        """Remove sessions older than keep_hours. Returns count deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                """
                DELETE FROM query_sessions
                WHERE datetime(created_at) < datetime('now', ?)
                """,
                (f"-{keep_hours} hours",),
            )
            return cursor.rowcount

    # ========== Table Metadata Cache ==========

    def cache_table(
        self,
        project: 'str',
        table_name: 'str',
        description: 'str | None',
        columns: 'list[dict[str, Any]]',
        partitions: 'list[str] | None' = None,
        row_count: 'int | None' = None,
        size_bytes: 'int | None' = None,
        owner: 'str | None' = None,
        schema_name: 'str' = "default",
    ) -> 'None':
        """Cache table metadata."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_metadata
                (project, schema_name, table_name, description, columns_json, partitions_json, row_count, size_bytes, owner, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project,
                    schema_name,
                    table_name,
                    description,
                    json.dumps(columns, ensure_ascii=False),
                    json.dumps(partitions, ensure_ascii=False) if partitions else None,
                    row_count,
                    size_bytes,
                    owner,
                    now_utc_iso(),
                ),
            )

    def get_cached_table(self, project: 'str', table_name: 'str', schema_name: 'str' = "default") -> 'dict[str, Any] | None':
        """Get cached table metadata."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT table_name, schema_name, description, columns_json, partitions_json, row_count, size_bytes, owner, updated_at
                FROM table_metadata WHERE project = ? AND schema_name = ? AND table_name = ?
                """,
                (project, schema_name, table_name),
            ).fetchone()
            if row:
                return {
                    "table_name": row["table_name"],
                    "schema_name": row["schema_name"],
                    "description": row["description"],
                    "columns": _safe_json_loads(row["columns_json"]),
                    "partitions": _safe_json_loads(row["partitions_json"]),
                    "row_count": row["row_count"],
                    "size_bytes": row["size_bytes"],
                    "owner": row["owner"],
                    "updated_at": row["updated_at"],
                }
            return None

    def get_all_cached_tables(
        self, project: 'str', schema_name: 'str | None' = None
    ) -> 'list[dict[str, Any]]':
        """Get all cached tables for a project, optionally filtered by schema."""
        with self._connect() as conn:
            if schema_name:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, description, columns_json, partitions_json, row_count, size_bytes, owner, updated_at
                    FROM table_metadata WHERE project = ? AND schema_name = ?
                    ORDER BY schema_name, table_name
                    """,
                    (project, schema_name),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, description, columns_json, partitions_json, row_count, size_bytes, owner, updated_at
                    FROM table_metadata WHERE project = ?
                    ORDER BY schema_name, table_name
                    """,
                    (project,),
                ).fetchall()
            return [
                {
                    "table_name": row["table_name"],
                    "schema_name": row["schema_name"],
                    "description": row["description"],
                    "columns": _safe_json_loads(row["columns_json"]),
                    "partitions": _safe_json_loads(row["partitions_json"]),
                    "row_count": row["row_count"],
                    "size_bytes": row["size_bytes"],
                    "owner": row["owner"],
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

    def get_cache_stats(self, project: 'str', schema_name: 'str | None' = None) -> 'dict[str, Any]':
        """Get cache statistics."""
        with self._connect() as conn:
            if schema_name:
                row = conn.execute(
                    """
                    SELECT COUNT(*) as count, MIN(updated_at) as oldest, MAX(updated_at) as newest
                    FROM table_metadata WHERE project = ? AND schema_name = ?
                    """,
                    (project, schema_name),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT COUNT(*) as count, MIN(updated_at) as oldest, MAX(updated_at) as newest
                    FROM table_metadata WHERE project = ?
                    """,
                    (project,),
                ).fetchone()
            return {
                "table_count": row["count"] if row else 0,
                "oldest": row["oldest"] if row else None,
                "newest": row["newest"] if row else None,
            }

    def get_schemas(self, project: 'str') -> 'list[str]':
        """Get all schemas for a project."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT schema_name FROM table_metadata WHERE project = ? ORDER BY schema_name
                """,
                (project,),
            ).fetchall()
            return [row["schema_name"] for row in rows]

    def get_tables_by_name(self, project: 'str', table_name: 'str') -> 'list[dict[str, Any]]':
        """Get all tables with the given name across different schemas."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT schema_name, description, columns_json, partitions_json, row_count, updated_at
                FROM table_metadata WHERE project = ? AND table_name = ?
                """,
                (project, table_name),
            ).fetchall()
            return [
                {
                    "schema_name": row["schema_name"],
                    "table_name": table_name,
                    "description": row["description"],
                    "columns": _safe_json_loads(row["columns_json"]),
                    "partitions": _safe_json_loads(row["partitions_json"]),
                    "row_count": row["row_count"],
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

    def clear_table_cache(self, project: 'str | None' = None, schema_name: 'str | None' = None) -> 'int':
        """Clear table metadata cache. If project is None, clear all."""
        with self._connect() as conn:
            if project and schema_name:
                cursor = conn.execute(
                    "DELETE FROM table_metadata WHERE project = ? AND schema_name = ?",
                    (project, schema_name),
                )
            elif project:
                cursor = conn.execute(
                    "DELETE FROM table_metadata WHERE project = ?",
                    (project,),
                )
            else:
                cursor = conn.execute("DELETE FROM table_metadata")
            return cursor.rowcount

    # ========== Semantic Metadata (for NL2SQL) ==========

    def save_semantic(
        self,
        project: 'str',
        table_name: 'str',
        semantic_desc: 'str',
        use_cases: 'list[str]',
        sample_questions: 'list[str]',
        column_semantics: 'list[dict[str, Any]]',
        schema_name: 'str' = "default",
        relations: 'list[dict[str, Any]] | None' = None,
        stats: 'dict[str, Any] | None' = None,
        embedding: 'bytes | None' = None,
        generated_by: 'str' = "agent",
    ) -> 'None':
        """Save AI-generated semantic metadata for NL2SQL."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_semantic
                (project, schema_name, table_name, semantic_desc, use_cases, sample_questions,
                 column_semantics_json, relations_json, stats_json, embedding, generated_at, generated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project,
                    schema_name,
                    table_name,
                    semantic_desc,
                    json.dumps(use_cases, ensure_ascii=False),
                    json.dumps(sample_questions, ensure_ascii=False),
                    json.dumps(column_semantics, ensure_ascii=False),
                    json.dumps(relations, ensure_ascii=False) if relations else None,
                    json.dumps(stats, ensure_ascii=False) if stats else None,
                    embedding,
                    now_utc_iso(),
                    generated_by,
                ),
            )
            # Update FTS index
            cached = self.get_cached_table(project, table_name, schema_name)
            if cached:
                col_names = " ".join(c["name"] for c in cached.get("columns", []))
                col_comments = " ".join(c.get("comment", "") for c in cached.get("columns", []))
                conn.execute(
                    "INSERT OR REPLACE INTO table_fts(project, table_name, schema_name, description, column_names, column_comments, semantic_desc, use_cases) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (project, table_name, schema_name, cached.get("description", ""), col_names, col_comments, semantic_desc, " ".join(use_cases)),
                )

    def get_semantic(self, project: 'str', table_name: 'str', schema_name: 'str' = "default") -> 'dict[str, Any] | None':
        """Get semantic metadata for a table."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT semantic_desc, use_cases, sample_questions, column_semantics_json, 
                       relations_json, stats_json, generated_at, generated_by
                FROM table_semantic WHERE project = ? AND schema_name = ? AND table_name = ?
                """,
                (project, schema_name, table_name),
            ).fetchone()
            if row:
                return {
                    "schema_name": schema_name,
                    "semantic_desc": row["semantic_desc"],
                    "use_cases": _safe_json_loads(row["use_cases"]),
                    "sample_questions": _safe_json_loads(row["sample_questions"]),
                    "column_semantics": _safe_json_loads(row["column_semantics_json"]),
                    "relations": _safe_json_loads(row["relations_json"]),
                    "stats": _safe_json_loads(row["stats_json"], default=None),
                    "generated_at": row["generated_at"],
                    "generated_by": row["generated_by"],
                }
            return None

    def fts_search(self, query: 'str', limit: 'int' = 20, project: 'str | None' = None) -> 'list[dict[str, Any]]':
        """Full-text search across all indexed tables."""
        with self._connect() as conn:
            if project:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, snippet(table_fts, 0, '<b>', '</b>', '...', 32) as match_snippet,
                           bm25(table_fts) as score
                    FROM table_fts WHERE table_fts MATCH ? AND project = ?
                    ORDER BY score LIMIT ?
                    """,
                    (query, project, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, snippet(table_fts, 0, '<b>', '</b>', '...', 32) as match_snippet,
                           bm25(table_fts) as score
                    FROM table_fts WHERE table_fts MATCH ?
                    ORDER BY score LIMIT ?
                    """,
                    (query, limit),
                ).fetchall()
            return [
                {"table_name": row["table_name"], "schema_name": row["schema_name"], "snippet": row["match_snippet"], "score": row["score"]}
                for row in rows
            ]

    def get_all_semantics(
        self, project: 'str', schema_name: 'str | None' = None
    ) -> 'list[dict[str, Any]]':
        """Get all semantic metadata for a project."""
        with self._connect() as conn:
            if schema_name:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, semantic_desc, use_cases, sample_questions, 
                           column_semantics_json, relations_json, stats_json, generated_at, generated_by
                    FROM table_semantic WHERE project = ? AND schema_name = ?
                    """,
                    (project, schema_name),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, semantic_desc, use_cases, sample_questions, 
                           column_semantics_json, relations_json, stats_json, generated_at, generated_by
                    FROM table_semantic WHERE project = ?
                    """,
                    (project,),
                ).fetchall()
            return [
                {
                    "table_name": row["table_name"],
                    "schema_name": row["schema_name"],
                    "semantic_desc": row["semantic_desc"],
                    "use_cases": _safe_json_loads(row["use_cases"]),
                    "sample_questions": _safe_json_loads(row["sample_questions"]),
                    "column_semantics": _safe_json_loads(row["column_semantics_json"]),
                    "relations": _safe_json_loads(row["relations_json"]),
                    "stats": _safe_json_loads(row["stats_json"], default=None),
                    "generated_at": row["generated_at"],
                    "generated_by": row["generated_by"],
                }
                for row in rows
            ]

    # ========== Cache Build Status Tracking ==========

    def start_build(self, project: 'str', build_id: 'str', total_tables: 'int') -> 'None':
        """Start a cache build process."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cache_build_status
                (project, build_id, status, total_tables, processed_tables, failed_tables, started_at)
                VALUES (?, ?, 'running', ?, 0, 0, ?)
                """,
                (project, build_id, total_tables, now_utc_iso()),
            )

    def update_build_progress(
        self, project: 'str', build_id: 'str', processed: 'int', failed: 'int'
    ) -> 'None':
        """Update cache build progress."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE cache_build_status
                SET processed_tables = ?, failed_tables = ?
                WHERE project = ? AND build_id = ? AND status = 'running'
                """,
                (processed, failed, project, build_id),
            )

    def complete_build(self, project: 'str', build_id: 'str', error_message: 'str | None' = None) -> 'None':
        """Mark cache build as completed."""
        with self._connect() as conn:
            if error_message:
                conn.execute(
                    """
                    UPDATE cache_build_status
                    SET status = 'failed', completed_at = ?, error_message = ?
                    WHERE project = ? AND build_id = ?
                    """,
                    (now_utc_iso(), error_message, project, build_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE cache_build_status
                    SET status = 'completed', completed_at = ?
                    WHERE project = ? AND build_id = ?
                    """,
                    (now_utc_iso(), project, build_id),
                )

    def get_build_status(self, project: 'str', build_id: 'str | None' = None) -> 'dict[str, Any] | None':
        """Get cache build status. If build_id is None, get the latest build."""
        with self._connect() as conn:
            if build_id:
                row = conn.execute(
                    """
                    SELECT project, build_id, status, total_tables, processed_tables, failed_tables,
                           started_at, completed_at, error_message
                    FROM cache_build_status WHERE project = ? AND build_id = ?
                    """,
                    (project, build_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT project, build_id, status, total_tables, processed_tables, failed_tables,
                           started_at, completed_at, error_message
                    FROM cache_build_status WHERE project = ?
                    ORDER BY started_at DESC LIMIT 1
                    """,
                    (project,),
                ).fetchone()

            if row:
                result = dict(row)
                # Calculate progress percentage
                if result["total_tables"] > 0:
                    result["progress_percent"] = int(
                        (result["processed_tables"] / result["total_tables"]) * 100
                    )
                else:
                    result["progress_percent"] = 0
                return result
            return None

    def get_recent_builds(self, project: 'str', limit: 'int' = 10) -> 'list[dict[str, Any]]':
        """Get recent build history for a project."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT project, build_id, status, total_tables, processed_tables, failed_tables,
                       started_at, completed_at, error_message
                FROM cache_build_status WHERE project = ?
                ORDER BY started_at DESC LIMIT ?
                """,
                (project, limit),
            ).fetchall()
            results = []
            for row in rows:
                result = dict(row)
                if result["total_tables"] > 0:
                    result["progress_percent"] = int(
                        (result["processed_tables"] / result["total_tables"]) * 100
                    )
                else:
                    result["progress_percent"] = 0
                results.append(result)
            return results

    # ------------------------------------------------------------------
    # Generic KV store for low-churn metadata
    # ------------------------------------------------------------------

    def get_kv(self, key: str, *, max_age_hours: 'int | None' = None) -> 'str | None':
        """Read a value from the kv_store table.

        Args:
            key: Lookup key (e.g. ``"tenant_id:my_project"``).
            max_age_hours: If set, return None when the entry is older
                than this many hours.

        Returns:
            Stored value string, or None if absent / expired.
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value, updated_at FROM kv_store WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                return None
            if max_age_hours is not None:
                from datetime import datetime, timedelta, timezone
                updated = datetime.fromisoformat(row["updated_at"])
                if datetime.now(timezone.utc) - updated > timedelta(hours=max_age_hours):
                    return None
            return row["value"]

    def set_kv(self, key: str, value: str) -> 'None':
        """Write a value to the kv_store table (upsert).

        Args:
            key: Lookup key.
            value: Value string to store.
        """
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, value, now),
            )
