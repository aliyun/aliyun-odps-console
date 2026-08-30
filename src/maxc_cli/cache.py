"""SQLite-based local cache for query sessions and metadata."""


import json
import os
import re
import sqlite3
import stat
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .exceptions import ValidationError
from .state_permissions import (
    PrivateDirectoryHandle,
    close_private_directory,
    descriptor_matches_path,
    ensure_private_directory,
    open_private_directory,
    open_private_file_at,
)
from .utils import now_utc_iso

_UNSET = object()


class CacheSnapshotBusyError(ValidationError):
    """A zero-write read cannot safely include an active SQLite transaction."""

    error_code = "CACHE_SNAPSHOT_BUSY"
    recoverable = True

    def _default_recovery_steps(self) -> 'list[str]':
        from .utils import current_cli_entry_point

        cli = current_cli_entry_point()
        return [
            f"Wait for any active `{cli} cache build` command to finish.",
            f"If no cache writer is active, refresh the cache: {cli} cache build --json",
            "Retry the original cache or semantic read command.",
        ]


def _acquire_windows_snapshot_guards(
    database_path: Path,
    database_descriptor: int,
) -> list[int]:
    """Pin a Windows database path against rename/delete while SQLite opens it."""
    if os.name != "nt":
        return []

    import ctypes
    import msvcrt
    from ctypes import wintypes

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = [
            ("dwFileAttributes", wintypes.DWORD),
            ("ftCreationTime", wintypes.FILETIME),
            ("ftLastAccessTime", wintypes.FILETIME),
            ("ftLastWriteTime", wintypes.FILETIME),
            ("dwVolumeSerialNumber", wintypes.DWORD),
            ("nFileSizeHigh", wintypes.DWORD),
            ("nFileSizeLow", wintypes.DWORD),
            ("nNumberOfLinks", wintypes.DWORD),
            ("nFileIndexHigh", wintypes.DWORD),
            ("nFileIndexLow", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    get_information.restype = wintypes.BOOL

    invalid_handle = wintypes.HANDLE(-1).value
    open_existing = 3
    generic_read = 0x80000000
    file_read_attributes = 0x00000080
    share_read = 0x00000001
    share_write = 0x00000002
    normal_attributes = 0x00000080
    backup_semantics = 0x02000000

    def open_guard(
        path: Path,
        *,
        access: int,
        sharing: int,
        flags: int,
    ) -> int:
        handle = create_file(
            str(path),
            access,
            sharing,
            None,
            open_existing,
            flags,
            None,
        )
        handle_value = (
            int(handle) if isinstance(handle, int) else int(handle.value)
        )
        if handle_value == invalid_handle:
            raise ctypes.WinError(ctypes.get_last_error())
        return handle_value

    def identity(handle: int) -> tuple[int, int]:
        information = _ByHandleFileInformation()
        if not get_information(wintypes.HANDLE(handle), ctypes.byref(information)):
            raise ctypes.WinError(ctypes.get_last_error())
        return (
            int(information.dwVolumeSerialNumber),
            (int(information.nFileIndexHigh) << 32)
            | int(information.nFileIndexLow),
        )

    handles: list[int] = []
    try:
        file_guard = open_guard(
            database_path,
            access=generic_read,
            sharing=share_read,
            flags=normal_attributes,
        )
        handles.append(file_guard)
        pinned_handle = int(msvcrt.get_osfhandle(database_descriptor))
        if identity(file_guard) != identity(pinned_handle):
            raise OSError(
                f"Local cache file changed while acquiring its Windows guard: {database_path}"
            )
        # With the legitimate file now deny-delete pinned, acquire a parent
        # guard that prevents directory swap during SQLite's pathname open.
        handles.append(
            open_guard(
                database_path.parent,
                access=file_read_attributes,
                sharing=share_read | share_write,
                flags=backup_semantics,
            )
        )
        return handles
    except BaseException:
        for handle in reversed(handles):
            close_handle(wintypes.HANDLE(handle))
        raise


def _release_windows_snapshot_guards(handles: list[int]) -> None:
    if not handles:
        return
    import ctypes
    from ctypes import wintypes

    close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL
    for handle in reversed(handles):
        close_handle(wintypes.HANDLE(handle))


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

    def __init__(self, cache_dir: 'Path', *, read_only: 'bool' = False):
        self.db_path = cache_dir / "cache.db"
        self._read_only = read_only
        self._read_only_tables: set[str] | None = None
        self._read_only_database_missing = False
        self._read_only_fingerprint: tuple[int, ...] | None = None
        self._fts_available = False
        if read_only:
            # Read commands must remain observational on a fresh machine. Do
            # not create the directory/database, change permissions, select a
            # journal mode, or run schema/FTS migrations here.
            return
        try:
            ensure_private_directory(self.db_path.parent)
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
                    self._init_fts(conn)
                return
            except ValidationError as exc:
                if self._is_lock_error(exc.message) and attempt < self._INIT_RETRIES - 1:
                    time.sleep(0.05 * (attempt + 1))
                    continue
                raise

    def _init_fts(self, conn: 'sqlite3.Connection') -> 'None':
        """Initialize the optional FTS5 index and migrate legacy contentless data.

        The original index used ``content=''``. SQLite intentionally returns
        NULL for every stored column in a contentless FTS table, so callers
        could neither identify results nor filter them by project. A regular
        FTS table keeps those identifiers while remaining a local derivative
        that can be rebuilt from the canonical metadata tables.

        Some Python/SQLite builds omit FTS5. Metadata caching must continue to
        work in that environment, so FTS initialization is best-effort.
        """
        try:
            row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'table_fts'"
            ).fetchone()
            existing_sql = str(row["sql"] or "") if row else ""
            compact_sql = "".join(existing_sql.lower().split())
            needs_rebuild = row is None
            if "content=''" in compact_sql or 'content=""' in compact_sql:
                conn.execute("DROP TABLE table_fts")
                needs_rebuild = True

            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS table_fts USING fts5(
                    project UNINDEXED,
                    table_name,
                    schema_name UNINDEXED,
                    description,
                    column_names,
                    column_comments,
                    semantic_desc,
                    use_cases,
                    tokenize='unicode61'
                )
                """
            )
            self._fts_available = True
            if needs_rebuild:
                self._refresh_fts_scope(conn)
        except sqlite3.OperationalError as exc:
            if "no such module: fts5" not in str(exc).lower():
                raise
            self._fts_available = False

    @property
    def fts_available(self) -> 'bool':
        """Whether this SQLite runtime supports the optional FTS5 index."""
        if self._read_only:
            self._load_read_only_tables()
        return self._fts_available

    @property
    def database_available(self) -> 'bool':
        """Whether a cache database path currently exists without creating it."""
        return os.path.lexists(self.db_path)

    def _load_read_only_tables(self) -> 'None':
        """Inspect an existing schema without applying any migrations."""
        if not self._read_only:
            return
        if self._read_only_tables is not None:
            self._assert_missing_snapshot_still_missing()
            return
        if not self.database_available:
            # Absence is itself the first observed snapshot state. Freeze it
            # for this LocalCache view so a database created between the
            # command's individual reads cannot produce a mixed response.
            self._read_only_database_missing = True
            self._read_only_tables = set()
            return
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            ).fetchall()
            self._read_only_tables = {str(row["name"]) for row in rows}
            if "table_fts" in self._read_only_tables:
                try:
                    conn.execute("SELECT 1 FROM table_fts LIMIT 0")
                except sqlite3.OperationalError as exc:
                    if "no such module" not in str(exc).lower():
                        raise
                else:
                    self._fts_available = True

    def _assert_missing_snapshot_still_missing(self) -> 'None':
        if not self._read_only_database_missing or not self.database_available:
            return
        raise CacheSnapshotBusyError(
            "Local cache was created while a zero-write snapshot was being read.",
            suggestion=(
                "Wait for the cache writer to finish, then retry the original "
                "command; the CLI will not combine missing and newly created "
                "cache state in one response."
            ),
        )

    def _table_available(self, table_name: 'str') -> 'bool':
        if not self._read_only:
            return True
        self._load_read_only_tables()
        return bool(
            self._read_only_tables is not None
            and table_name in self._read_only_tables
        )

    def _assert_no_transaction_sidecar(
        self,
        directory: 'PrivateDirectoryHandle',
    ) -> 'None':
        """Fail closed when a zero-write snapshot cannot include committed state.

        SQLite's immutable mode intentionally ignores WAL content, while a
        normal read-only connection can create or update shared-memory state.
        A rollback journal likewise means the main file alone is not a safe
        snapshot. Check entries relative to the pinned directory so a reader
        never silently reports stale or in-flight state.
        """
        for suffix in ("-wal", "-journal"):
            name = f"{self.db_path.name}{suffix}"
            try:
                if directory.descriptor is not None:
                    sidecar_stat = os.stat(
                        name,
                        dir_fd=directory.descriptor,
                        follow_symlinks=False,
                    )
                else:
                    sidecar_stat = os.lstat(directory.path / name)
            except FileNotFoundError:
                continue
            # CPython 3.9 / SQLite on macOS can retain a regular, zero-byte
            # WAL (plus SHM) after a fully checkpointed writer closes. The
            # main database is authoritative in that state. If a writer later
            # appends or checkpoints during this read, the repeated sidecar
            # and main-file fingerprint checks below still fail closed.
            if (
                suffix == "-wal"
                and stat.S_ISREG(sidecar_stat.st_mode)
                and sidecar_stat.st_size == 0
            ):
                continue
            raise CacheSnapshotBusyError(
                f"Local cache cannot be read as a zero-write snapshot while `{name}` exists.",
                suggestion=(
                    "Wait for any active cache build to finish. If no writer is active, "
                    "run `maxc cache build --json` to refresh and checkpoint the cache, "
                    "then retry; the CLI will not return a potentially stale snapshot."
                ),
            )

    @staticmethod
    def _database_fingerprint(database_descriptor: 'int') -> 'tuple[int, ...]':
        database_stat = os.fstat(database_descriptor)
        return (
            database_stat.st_dev,
            database_stat.st_ino,
            database_stat.st_size,
            database_stat.st_mtime_ns,
            database_stat.st_ctime_ns,
        )

    def _assert_database_unchanged(
        self,
        database_descriptor: 'int',
        expected_fingerprint: 'tuple[int, ...]',
    ) -> 'None':
        """Reject a snapshot if a writer changed the main DB during the read."""
        if self._database_fingerprint(database_descriptor) == expected_fingerprint:
            return
        raise CacheSnapshotBusyError(
            "Local cache changed while the zero-write snapshot was being read.",
            suggestion=(
                "Wait for any active cache build to finish. If no writer is active, "
                "run `maxc cache build --json` to refresh and checkpoint the cache, "
                "then retry; the CLI will not return a potentially stale snapshot."
            ),
        )

    @staticmethod
    def _fts_scope(
        *,
        project: 'str | None' = None,
        schema_name: 'str | None' = None,
        table_name: 'str | None' = None,
        prefix: 'str' = "",
    ) -> 'tuple[str, list[str]]':
        clauses: list[str] = []
        params: list[str] = []
        if project is not None:
            clauses.append(f"{prefix}project = ?")
            params.append(project)
        if schema_name is not None:
            clauses.append(f"{prefix}schema_name = ?")
            params.append(schema_name)
        if table_name is not None:
            clauses.append(f"{prefix}table_name = ?")
            params.append(table_name)
        return (" AND ".join(clauses), params)

    def _delete_fts_scope(
        self,
        conn: 'sqlite3.Connection',
        *,
        project: 'str | None' = None,
        schema_name: 'str | None' = None,
        table_name: 'str | None' = None,
    ) -> 'int':
        if not self._fts_available:
            return 0
        where, params = self._fts_scope(
            project=project,
            schema_name=schema_name,
            table_name=table_name,
        )
        sql = "DELETE FROM table_fts"
        if where:
            sql += f" WHERE {where}"
        return conn.execute(sql, params).rowcount

    @staticmethod
    def _fts_values(row: 'sqlite3.Row') -> 'tuple[str, ...]':
        columns = _safe_json_loads(row["columns_json"])
        if not isinstance(columns, list):
            columns = []
        column_names = " ".join(
            str(column.get("name") or "")
            for column in columns
            if isinstance(column, dict)
        )
        column_comments = " ".join(
            str(column.get("comment") or "")
            for column in columns
            if isinstance(column, dict)
        )
        use_cases = _safe_json_loads(row["use_cases"])
        if isinstance(use_cases, list):
            use_case_text = " ".join(str(item) for item in use_cases)
        else:
            use_case_text = str(use_cases or "")
        return (
            str(row["project"]),
            str(row["table_name"]),
            str(row["schema_name"]),
            str(row["description"] or ""),
            column_names,
            column_comments,
            str(row["semantic_desc"] or ""),
            use_case_text,
        )

    def _refresh_fts_scope(
        self,
        conn: 'sqlite3.Connection',
        *,
        project: 'str | None' = None,
        schema_name: 'str | None' = None,
        table_name: 'str | None' = None,
    ) -> 'None':
        """Rebuild the derived FTS rows for one scope from canonical tables."""
        if not self._fts_available:
            return
        self._delete_fts_scope(
            conn,
            project=project,
            schema_name=schema_name,
            table_name=table_name,
        )
        where, params = self._fts_scope(
            project=project,
            schema_name=schema_name,
            table_name=table_name,
            prefix="m.",
        )
        sql = """
            SELECT m.project, m.schema_name, m.table_name, m.description,
                   m.columns_json, s.semantic_desc, s.use_cases
            FROM table_metadata AS m
            LEFT JOIN table_semantic AS s
              ON s.project = m.project
             AND s.schema_name = m.schema_name
             AND s.table_name = m.table_name
        """
        if where:
            sql += f" WHERE {where}"
        rows = conn.execute(sql, params).fetchall()
        conn.executemany(
            """
            INSERT INTO table_fts(
                project, table_name, schema_name, description, column_names,
                column_comments, semantic_desc, use_cases
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [self._fts_values(row) for row in rows],
        )

    @contextmanager
    def _connect(self) -> 'Generator[sqlite3.Connection, None, None]':
        if self._read_only:
            with self._connect_read_only() as conn:
                yield conn
            return

        # Increased timeout to 30 seconds to prevent lock contention in concurrent scenarios
        directory = None
        database_descriptor = None
        conn = None
        setup_complete = False
        try:
            # SQLite's Python API accepts paths rather than already-open file
            # descriptors. Pin both the parent and database, verify their
            # identities immediately before and after sqlite opens the path,
            # and keep the descriptors alive for the whole transaction. This
            # rejects parent/leaf substitutions before any schema or data
            # writes occur.
            directory = open_private_directory(
                self.db_path.parent,
                create=False,
            )
            database_descriptor = open_private_file_at(
                directory,
                self.db_path.name,
                os.O_RDWR,
                create=True,
                display_path=self.db_path,
            )
            self._verify_pinned_database(directory, database_descriptor)
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            self._verify_pinned_database(directory, database_descriptor)
            setup_complete = True
        except OSError as exc:
            raise ValidationError(
                f"Local cache database path is unsafe: {self.db_path}: {exc}",
                suggestion="Use a regular cache file owned by the current user.",
            ) from exc
        except sqlite3.Error as exc:
            raise self._translate_sqlite_error(exc) from exc
        finally:
            if not setup_complete:
                if conn is not None:
                    conn.close()
                if database_descriptor is not None:
                    os.close(database_descriptor)
                if directory is not None:
                    close_private_directory(directory)

        assert conn is not None
        assert database_descriptor is not None
        assert directory is not None
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            self._verify_pinned_database(directory, database_descriptor)
            conn.commit()
            self._verify_pinned_database(directory, database_descriptor)
        except sqlite3.Error as exc:
            conn.rollback()
            raise self._translate_sqlite_error(exc) from exc
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
            os.close(database_descriptor)
            close_private_directory(directory)

    @contextmanager
    def _connect_read_only(self) -> 'Generator[sqlite3.Connection, None, None]':
        """Open an existing cache without creating or mutating filesystem state.

        ``immutable=1`` is intentional: a plain SQLite read-only connection may
        create ``-shm`` for a WAL database. Each command gets a short-lived
        snapshot connection, while pinned directory/file descriptors and
        identity checks retain the writable connection's path protections.
        """
        if not self.database_available:
            raise ValidationError(
                f"Local cache database does not exist: {self.db_path}",
                suggestion="Run `maxc cache build` to create the metadata cache.",
            )

        directory = None
        database_descriptor = None
        conn = None
        windows_guards: list[int] = []
        setup_complete = False
        try:
            directory = open_private_directory(self.db_path.parent, create=False)
            flags = os.O_RDONLY
            for flag_name in ("O_CLOEXEC", "O_NOFOLLOW", "O_NONBLOCK"):
                flags |= getattr(os, flag_name, 0)
            if directory.descriptor is not None:
                database_descriptor = os.open(
                    self.db_path.name,
                    flags,
                    dir_fd=directory.descriptor,
                )
            else:
                database_descriptor = os.open(directory.path / self.db_path.name, flags)

            database_stat = os.fstat(database_descriptor)
            if not stat.S_ISREG(database_stat.st_mode):
                raise OSError(
                    f"CLI state path is not a regular file: {self.db_path}"
                )
            if os.name == "posix":
                effective_uid = os.geteuid()
                if database_stat.st_uid != effective_uid:
                    raise OSError(
                        f"CLI state file is not owned by uid {effective_uid}: {self.db_path}"
                    )
                if stat.S_IMODE(database_stat.st_mode) & 0o022:
                    raise OSError(
                        f"CLI state file is writable by another user: {self.db_path}"
                    )

            database_fingerprint = self._database_fingerprint(database_descriptor)
            if self._read_only_fingerprint is not None:
                self._assert_database_unchanged(
                    database_descriptor,
                    self._read_only_fingerprint,
                )
                database_fingerprint = self._read_only_fingerprint
            self._assert_no_transaction_sidecar(directory)
            self._verify_pinned_database(directory, database_descriptor)
            windows_guards = _acquire_windows_snapshot_guards(
                self.db_path,
                database_descriptor,
            )
            conn = self._connect_descriptor_snapshot(
                database_descriptor,
                self.db_path,
            )
            self._assert_no_transaction_sidecar(directory)
            self._assert_database_unchanged(
                database_descriptor,
                database_fingerprint,
            )
            self._verify_pinned_database(directory, database_descriptor)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            if self._read_only_fingerprint is None:
                self._read_only_fingerprint = database_fingerprint
            setup_complete = True
        except OSError as exc:
            raise ValidationError(
                f"Local cache database path is unsafe: {self.db_path}: {exc}",
                suggestion="Use an owner-private regular cache file owned by the current user.",
            ) from exc
        except sqlite3.Error as exc:
            raise self._translate_sqlite_error(exc) from exc
        finally:
            if not setup_complete:
                if conn is not None:
                    conn.close()
                _release_windows_snapshot_guards(windows_guards)
                if database_descriptor is not None:
                    os.close(database_descriptor)
                if directory is not None:
                    close_private_directory(directory)

        assert conn is not None
        assert database_descriptor is not None
        assert directory is not None
        try:
            yield conn
            self._assert_no_transaction_sidecar(directory)
            self._assert_database_unchanged(
                database_descriptor,
                database_fingerprint,
            )
            self._verify_pinned_database(directory, database_descriptor)
        except sqlite3.Error as exc:
            raise self._translate_sqlite_error(exc) from exc
        finally:
            conn.close()
            _release_windows_snapshot_guards(windows_guards)
            os.close(database_descriptor)
            close_private_directory(directory)

    @staticmethod
    def _connect_descriptor_snapshot(
        database_descriptor: int,
        database_path: Path,
    ) -> sqlite3.Connection:
        """Open SQLite from the already-validated descriptor, never its path.

        Reopening ``cache.db`` by pathname leaves a swap-and-restore race even
        when identity checks bracket ``sqlite3.connect``. POSIX ``/dev/fd``
        duplicates the pinned descriptor into SQLite. Runtimes that expose
        ``Connection.deserialize`` can instead load the pinned bytes into an
        in-memory database without touching the filesystem.
        """
        descriptor_path = Path("/dev/fd") / str(database_descriptor)
        if os.name == "posix" and os.path.exists(descriptor_path):
            descriptor_uri = (
                f"{descriptor_path.as_uri()}?mode=ro&immutable=1"
            )
            return sqlite3.connect(descriptor_uri, timeout=30.0, uri=True)

        if os.name == "nt":
            # _acquire_windows_snapshot_guards has identity-bound the file and
            # deny-delete pinned both it and its parent for this pathname open.
            database_uri = f"{database_path.as_uri()}?mode=ro&immutable=1"
            return sqlite3.connect(database_uri, timeout=30.0, uri=True)

        if hasattr(sqlite3.Connection, "deserialize"):
            duplicate = os.dup(database_descriptor)
            try:
                with os.fdopen(duplicate, "rb", closefd=True) as source:
                    database_bytes = source.read()
            except BaseException:
                # fdopen owns the duplicate after successful construction; if
                # construction itself failed, close the duplicate explicitly.
                try:
                    os.close(duplicate)
                except OSError:
                    pass
                raise
            connection = sqlite3.connect(":memory:")
            try:
                connection.deserialize(database_bytes)
            except BaseException:
                connection.close()
                raise
            return connection

        raise OSError(
            "This Python/OS combination cannot bind SQLite to a validated "
            "read-only file descriptor. Use a supported POSIX or Windows runtime."
        )

    def _verify_pinned_database(
        self,
        directory: 'PrivateDirectoryHandle',
        database_descriptor: 'int',
    ) -> 'None':
        if not descriptor_matches_path(
            self.db_path.parent,
            directory,
            directory=True,
        ) or not descriptor_matches_path(self.db_path, database_descriptor):
            raise ValidationError(
                f"Local cache path changed while it was in use: {self.db_path}",
                suggestion="Use a stable cache directory owned by the current user.",
            )

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
            self._refresh_fts_scope(
                conn,
                project=project,
                schema_name=schema_name,
                table_name=table_name,
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
        if not self._table_available("table_metadata"):
            return []
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
        if not self._table_available("table_metadata"):
            return {"table_count": 0, "oldest": None, "newest": None}
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

    def get_semantic_count(
        self, project: 'str', schema_name: 'str | None' = None
    ) -> 'int':
        """Count semantic annotations in a project scope."""
        if not self._table_available("table_semantic"):
            return 0
        with self._connect() as conn:
            if schema_name is not None:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM table_semantic
                    WHERE project = ? AND schema_name = ?
                    """,
                    (project, schema_name),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) AS count FROM table_semantic WHERE project = ?",
                    (project,),
                ).fetchone()
            return int(row["count"] if row else 0)

    def get_fts_count(
        self, project: 'str', schema_name: 'str | None' = None
    ) -> 'int | None':
        """Count derived FTS rows, or return None when FTS5 is unavailable."""
        if not self.fts_available:
            return None
        with self._connect() as conn:
            if schema_name is not None:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM table_fts
                    WHERE project = ? AND schema_name = ?
                    """,
                    (project, schema_name),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) AS count FROM table_fts WHERE project = ?",
                    (project,),
                ).fetchone()
            return int(row["count"] if row else 0)

    def get_schemas(self, project: 'str') -> 'list[str]':
        """Get all schemas for a project."""
        if not self._table_available("table_metadata"):
            return []
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
        """Clear metadata and its derived FTS rows, preserving semantics."""
        with self._connect() as conn:
            self._delete_fts_scope(
                conn,
                project=project,
                schema_name=schema_name,
            )
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
            self._refresh_fts_scope(
                conn,
                project=project,
                schema_name=schema_name,
                table_name=table_name,
            )

    def clear_semantic(
        self,
        project: 'str | None' = None,
        table_name: 'str | None' = None,
        schema_name: 'str | None' = None,
    ) -> 'int':
        """Clear semantic annotations and rebuild affected metadata-only FTS rows."""
        where, params = self._fts_scope(
            project=project,
            schema_name=schema_name,
            table_name=table_name,
        )
        sql = "DELETE FROM table_semantic"
        if where:
            sql += f" WHERE {where}"
        with self._connect() as conn:
            cursor = conn.execute(sql, params)
            self._refresh_fts_scope(
                conn,
                project=project,
                schema_name=schema_name,
                table_name=table_name,
            )
            return cursor.rowcount

    def get_semantic(self, project: 'str', table_name: 'str', schema_name: 'str' = "default") -> 'dict[str, Any] | None':
        """Get semantic metadata for a table."""
        if not self._table_available("table_semantic"):
            return None
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
        if not self._fts_available or limit <= 0:
            return []
        terms = re.findall(r"\w+", query, flags=re.UNICODE)
        if not terms:
            return []
        # Treat user input as keywords, not raw FTS syntax. Quoting each token
        # prevents malformed MATCH expressions from surfacing SQLite errors.
        match_query = " AND ".join(f'"{term}"' for term in terms)
        with self._connect() as conn:
            if project:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, snippet(table_fts, -1, '<b>', '</b>', '...', 32) as match_snippet,
                           bm25(table_fts) as score
                    FROM table_fts WHERE table_fts MATCH ? AND project = ?
                    ORDER BY score LIMIT ?
                    """,
                    (match_query, project, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT table_name, schema_name, snippet(table_fts, -1, '<b>', '</b>', '...', 32) as match_snippet,
                           bm25(table_fts) as score
                    FROM table_fts WHERE table_fts MATCH ?
                    ORDER BY score LIMIT ?
                    """,
                    (match_query, limit),
                ).fetchall()
            return [
                {"table_name": row["table_name"], "schema_name": row["schema_name"], "snippet": row["match_snippet"], "score": row["score"]}
                for row in rows
            ]

    def get_all_semantics(
        self, project: 'str', schema_name: 'str | None' = None
    ) -> 'list[dict[str, Any]]':
        """Get all semantic metadata for a project."""
        if not self._table_available("table_semantic"):
            return []
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

    def complete_build(
        self,
        project: 'str',
        build_id: 'str',
        error_message: 'str | None' = None,
        *,
        status: 'str | None' = None,
    ) -> 'None':
        """Mark cache build as completed."""
        with self._connect() as conn:
            if error_message:
                terminal_status = status or "failed"
                conn.execute(
                    """
                    UPDATE cache_build_status
                    SET status = ?, completed_at = ?, error_message = ?
                    WHERE project = ? AND build_id = ?
                    """,
                    (
                        terminal_status,
                        now_utc_iso(),
                        error_message,
                        project,
                        build_id,
                    ),
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
        if not self._table_available("cache_build_status"):
            return None
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
                    result["progress_percent"] = (
                        0 if result["status"] == "running" else 100
                    )
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
                    result["progress_percent"] = (
                        0 if result["status"] == "running" else 100
                    )
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

    def delete_kv_prefix(self, prefix: 'str') -> 'int':
        """Delete KV entries whose keys start with an exact literal prefix.

        ``substr`` deliberately avoids LIKE wildcard semantics, so credential
        provider names containing ``%``, ``_``, or ``\\`` cannot broaden the
        deletion scope. An empty prefix is rejected instead of deleting the
        entire store accidentally.
        """
        if not prefix:
            raise ValidationError(
                "KV prefix must not be empty.",
                suggestion="Provide the exact key namespace to clear, for example `ext_creds:`.",
            )
        with self._connect() as conn:
            cursor = conn.execute(
                """
                DELETE FROM kv_store
                WHERE substr(key, 1, length(?)) = ?
                """,
                (prefix, prefix),
            )
            return cursor.rowcount
