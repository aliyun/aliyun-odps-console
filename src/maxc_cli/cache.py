"""SQLite-based local cache for query sessions and metadata."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from .utils import now_utc_iso


class LocalCache:
    """Lightweight SQLite cache for query sessions and metadata."""

    def __init__(self, cache_dir: Path):
        self.db_path = cache_dir / "cache.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
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
                    table_name TEXT NOT NULL,
                    description TEXT,
                    columns_json TEXT NOT NULL,
                    partitions_json TEXT,
                    row_count INTEGER,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (project, table_name)
                );
                CREATE INDEX IF NOT EXISTS idx_table_meta_project ON table_metadata(project);

                -- AI 生成的语义元数据，用于 NL2SQL
                CREATE TABLE IF NOT EXISTS table_semantic (
                    project TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    semantic_desc TEXT,
                    use_cases TEXT,
                    sample_questions TEXT,
                    column_semantics_json TEXT,
                    embedding BLOB,
                    generated_at TEXT NOT NULL,
                    PRIMARY KEY (project, table_name)
                );
                CREATE INDEX IF NOT EXISTS idx_semantic_project ON table_semantic(project);

                -- FTS5 全文索引，用于关键词搜索
                CREATE VIRTUAL TABLE IF NOT EXISTS table_fts USING fts5(
                    table_name,
                    description,
                    column_names,
                    column_comments,
                    semantic_desc,
                    use_cases,
                    content='',
                    tokenize='unicode61'
                );
            """)

    @contextmanager
    def _connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self.db_path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def create_session(
        self,
        job_id: str,
        project: str,
        sql: str | None = None,
    ) -> int:
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

    def get_session(self, session_id: int) -> dict[str, Any] | None:
        """Get session by id."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, job_id, project, sql, created_at FROM query_sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if row:
                return dict(row)
            return None

    def find_session_by_job_id(self, job_id: str) -> dict[str, Any] | None:
        """Find existing session by job_id (for deduplication)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, job_id, project, sql, created_at FROM query_sessions WHERE job_id = ? ORDER BY id DESC LIMIT 1",
                (job_id,),
            ).fetchone()
            if row:
                return dict(row)
            return None

    def cleanup_old_sessions(self, keep_hours: int = 24) -> int:
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
        project: str,
        table_name: str,
        description: str | None,
        columns: list[dict[str, Any]],
        partitions: list[str] | None = None,
        row_count: int | None = None,
    ) -> None:
        """Cache table metadata."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_metadata
                (project, table_name, description, columns_json, partitions_json, row_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project,
                    table_name,
                    description,
                    json.dumps(columns, ensure_ascii=False),
                    json.dumps(partitions, ensure_ascii=False) if partitions else None,
                    row_count,
                    now_utc_iso(),
                ),
            )

    def get_cached_table(self, project: str, table_name: str) -> dict[str, Any] | None:
        """Get cached table metadata."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT table_name, description, columns_json, partitions_json, row_count, updated_at
                FROM table_metadata WHERE project = ? AND table_name = ?
                """,
                (project, table_name),
            ).fetchone()
            if row:
                return {
                    "table_name": row["table_name"],
                    "description": row["description"],
                    "columns": json.loads(row["columns_json"]),
                    "partitions": json.loads(row["partitions_json"]) if row["partitions_json"] else [],
                    "row_count": row["row_count"],
                    "updated_at": row["updated_at"],
                }
            return None

    def get_all_cached_tables(self, project: str) -> list[dict[str, Any]]:
        """Get all cached tables for a project."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT table_name, description, columns_json, partitions_json, row_count, updated_at
                FROM table_metadata WHERE project = ?
                """,
                (project,),
            ).fetchall()
            return [
                {
                    "table_name": row["table_name"],
                    "description": row["description"],
                    "columns": json.loads(row["columns_json"]),
                    "partitions": json.loads(row["partitions_json"]) if row["partitions_json"] else [],
                    "row_count": row["row_count"],
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

    def get_cache_stats(self, project: str) -> dict[str, Any]:
        """Get cache statistics."""
        with self._connect() as conn:
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

    def clear_table_cache(self, project: str | None = None) -> int:
        """Clear table metadata cache. If project is None, clear all."""
        with self._connect() as conn:
            if project:
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
        project: str,
        table_name: str,
        semantic_desc: str,
        use_cases: list[str],
        sample_questions: list[str],
        column_semantics: list[dict[str, Any]],
        embedding: bytes | None = None,
    ) -> None:
        """Save AI-generated semantic metadata for NL2SQL."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_semantic
                (project, table_name, semantic_desc, use_cases, sample_questions, 
                 column_semantics_json, embedding, generated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project,
                    table_name,
                    semantic_desc,
                    json.dumps(use_cases, ensure_ascii=False),
                    json.dumps(sample_questions, ensure_ascii=False),
                    json.dumps(column_semantics, ensure_ascii=False),
                    embedding,
                    now_utc_iso(),
                ),
            )
            # 更新 FTS 索引
            cached = self.get_cached_table(project, table_name)
            if cached:
                col_names = " ".join(c["name"] for c in cached.get("columns", []))
                col_comments = " ".join(c.get("comment", "") for c in cached.get("columns", []))
                conn.execute(
                    "INSERT INTO table_fts(table_name, description, column_names, column_comments, semantic_desc, use_cases) VALUES (?, ?, ?, ?, ?, ?)",
                    (table_name, cached.get("description", ""), col_names, col_comments, semantic_desc, " ".join(use_cases)),
                )

    def get_semantic(self, project: str, table_name: str) -> dict[str, Any] | None:
        """Get semantic metadata for a table."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT semantic_desc, use_cases, sample_questions, column_semantics_json, generated_at
                FROM table_semantic WHERE project = ? AND table_name = ?
                """,
                (project, table_name),
            ).fetchone()
            if row:
                return {
                    "semantic_desc": row["semantic_desc"],
                    "use_cases": json.loads(row["use_cases"]) if row["use_cases"] else [],
                    "sample_questions": json.loads(row["sample_questions"]) if row["sample_questions"] else [],
                    "column_semantics": json.loads(row["column_semantics_json"]) if row["column_semantics_json"] else [],
                    "generated_at": row["generated_at"],
                }
            return None

    def fts_search(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Full-text search across all indexed tables."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT table_name, snippet(table_fts, 0, '<b>', '</b>', '...', 32) as match_snippet,
                       bm25(table_fts) as score
                FROM table_fts WHERE table_fts MATCH ?
                ORDER BY score LIMIT ?
                """,
                (query, limit),
            ).fetchall()
            return [
                {"table_name": row["table_name"], "snippet": row["match_snippet"], "score": row["score"]}
                for row in rows
            ]

    def get_all_semantics(self, project: str) -> list[dict[str, Any]]:
        """Get all semantic metadata for a project."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT table_name, semantic_desc, use_cases, sample_questions, column_semantics_json, generated_at
                FROM table_semantic WHERE project = ?
                """,
                (project,),
            ).fetchall()
            return [
                {
                    "table_name": row["table_name"],
                    "semantic_desc": row["semantic_desc"],
                    "use_cases": json.loads(row["use_cases"]) if row["use_cases"] else [],
                    "sample_questions": json.loads(row["sample_questions"]) if row["sample_questions"] else [],
                    "column_semantics": json.loads(row["column_semantics_json"]) if row["column_semantics_json"] else [],
                    "generated_at": row["generated_at"],
                }
                for row in rows
            ]
