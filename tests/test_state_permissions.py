"""Regression coverage for private local-state permissions."""

import json
import os
import stat
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import pytest

import maxc_cli.audit as audit_module
import maxc_cli.cache as cache_module
from maxc_cli.app import MaxCApp
from maxc_cli.audit import AuditLogger
from maxc_cli.cache import LocalCache
from maxc_cli.config import load_config
from maxc_cli.exceptions import ValidationError
from maxc_cli.store import JobStore

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(os.name != "posix", reason="POSIX mode bits only"),
]


@contextmanager
def _umask_022():
    previous = os.umask(0o022)
    try:
        yield
    finally:
        os.umask(previous)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_local_cache_preserves_existing_shared_directory_and_repairs_database(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(mode=0o755)
    cache_dir.chmod(0o755)
    database = cache_dir / "cache.db"
    database.touch(mode=0o644)
    database.chmod(0o644)

    with _umask_022():
        LocalCache(cache_dir)

    assert _mode(cache_dir) == 0o755
    assert _mode(database) == 0o600


def test_job_store_preserves_existing_shared_directory_and_repairs_files(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o755)
    state_dir.chmod(0o755)
    jobs_file = state_dir / "jobs.json"
    jobs_file.write_text("{}", encoding="utf-8")
    jobs_file.chmod(0o644)

    with _umask_022():
        store = JobStore(state_dir)
        store.save_remote_job_context("job-1", {"project": "p"})

    assert _mode(state_dir) == 0o755
    assert _mode(jobs_file) == 0o600
    lock_file = state_dir / "jobs.lock"
    if lock_file.exists():
        assert _mode(lock_file) == 0o600


def test_audit_logger_repairs_default_state_directory_and_file(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o755)
    state_dir.chmod(0o755)
    audit_file = state_dir / "audit.log"
    audit_file.write_text('{"old": true}\n', encoding="utf-8")
    audit_file.chmod(0o644)

    with _umask_022():
        logger = AuditLogger(audit_file, secure_parent=True)
        logger.log({"command": "query", "status": "success"})

    assert _mode(state_dir) == 0o700
    assert _mode(audit_file) == 0o600


def test_app_log_does_not_chmod_existing_custom_state_directory(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o755)
    state_dir.chmod(0o755)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "default_project: test_project",
                f"state_dir: {state_dir}",
                f"cache_dir: {tmp_path / 'cache'}",
            ]
        ),
        encoding="utf-8",
    )

    with _umask_022():
        app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
        app.log("agent.context", "success")

    assert _mode(state_dir) == 0o755
    assert _mode(state_dir / "audit.log") == 0o600


def test_app_audit_records_share_one_invocation_id(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "default_project: test_project",
                f"state_dir: {state_dir}",
                f"cache_dir: {tmp_path / 'cache'}",
            ]
        ),
        encoding="utf-8",
    )

    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    app.log("query", "success", {"remote_execution_succeeded": True})
    app.log(
        "query",
        "failure",
        {"remote_execution_succeeded": True, "output_written": False},
        error={"code": "OUTPUT_WRITE_FAILED", "recoverable": True},
    )

    records = [
        json.loads(line)
        for line in (state_dir / "audit.log").read_text(encoding="utf-8").splitlines()
    ]
    assert [record["status"] for record in records] == ["success", "failure"]
    invocation_ids = {record["invocation_id"] for record in records}
    assert len(invocation_ids) == 1
    invocation_id = invocation_ids.pop()
    assert len(invocation_id) == 32
    int(invocation_id, 16)


def test_new_cache_and_state_directories_are_private(tmp_path: Path) -> None:
    cache_dir = tmp_path / "new-cache"
    state_dir = tmp_path / "new-state"

    with _umask_022():
        LocalCache(cache_dir)
        store = JobStore(state_dir)
        store.save_remote_job_context("job-1", {"project": "p"})

    assert _mode(cache_dir) == 0o700
    assert _mode(cache_dir / "cache.db") == 0o600
    assert _mode(state_dir) == 0o700
    assert _mode(state_dir / "jobs.json") == 0o600


def test_custom_audit_path_does_not_chmod_existing_shared_parent(tmp_path: Path) -> None:
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir(mode=0o755)
    shared_dir.chmod(0o755)
    audit_file = shared_dir / "maxc-audit.log"

    with _umask_022():
        AuditLogger(audit_file).log({"status": "success"})

    assert _mode(shared_dir) == 0o755
    assert _mode(audit_file) == 0o600


def test_audit_logger_refuses_symlink_without_touching_target(tmp_path: Path) -> None:
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    victim = tmp_path / "victim.txt"
    victim.write_text("keep\n", encoding="utf-8")
    victim.chmod(0o644)
    (shared_dir / "audit.log").symlink_to(victim)

    with pytest.raises(OSError):
        AuditLogger(shared_dir / "audit.log")

    assert victim.read_text(encoding="utf-8") == "keep\n"
    assert _mode(victim) == 0o644


def test_app_audit_redacts_sql_credentials_and_serializes_unknown_values(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: project-a\n"
        "state_dir: ./state\n"
        "cache_dir: ./cache\n",
        encoding="utf-8",
    )
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    sql_sentinel = "SELECT * FROM t WHERE secret = 'AUDIT-SQL-SENTINEL'"
    credential_sentinel = "AUDIT-CREDENTIAL-SENTINEL"

    app.log(
        "query",
        "success",
        {
            "project": "project-a",
            "sql_executed": sql_sentinel,
            "access_token": credential_sentinel,
            "non_json": {Decimal("1.25"), Decimal("2.50")},
        },
        error={
            "code": "SQL_ERROR",
            "message": f"failed around {sql_sentinel}",
            "recoverable": False,
        },
    )

    raw = app._audit_path.read_text(encoding="utf-8")
    record = json.loads(raw)
    assert "AUDIT-SQL-SENTINEL" not in raw
    assert credential_sentinel not in raw
    assert record["metadata"]["sql_executed"]["redacted"] is True
    assert len(record["metadata"]["sql_executed"]["sha256"]) == 64
    assert record["metadata"]["access_token"] == "<redacted>"
    assert record["error"] == {"code": "SQL_ERROR", "recoverable": False}


def test_app_audit_failure_never_changes_command_outcome(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("state_dir: ./state\ncache_dir: ./cache\n", encoding="utf-8")
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)

    class _BrokenAudit:
        def log(self, _payload):
            raise TypeError("not serializable")

    app._audit = _BrokenAudit()
    app.log("query", "success", {"value": object()})


def test_job_store_lock_refuses_symlink_without_touching_target(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    store = JobStore(state_dir)
    victim = tmp_path / "victim.lock"
    victim.write_text("keep\n", encoding="utf-8")
    victim.chmod(0o644)
    store.lock_path.symlink_to(victim)

    with pytest.raises(OSError):
        store.list_jobs()

    assert victim.read_text(encoding="utf-8") == "keep\n"
    assert _mode(victim) == 0o644


def test_job_store_data_refuses_symlink_without_touching_target(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    victim = tmp_path / "victim.json"
    victim.write_text('{"keep": true}\n', encoding="utf-8")
    victim.chmod(0o644)
    (state_dir / "jobs.json").symlink_to(victim)

    with pytest.raises(OSError):
        JobStore(state_dir)

    assert victim.read_text(encoding="utf-8") == '{"keep": true}\n'
    assert _mode(victim) == 0o644


def test_local_cache_refuses_symlink_without_touching_target(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    victim = tmp_path / "victim.db"
    victim.write_text("not a maxc database\n", encoding="utf-8")
    victim.chmod(0o644)
    (cache_dir / "cache.db").symlink_to(victim)

    with pytest.raises(ValidationError, match="database path is unsafe"):
        LocalCache(cache_dir)

    assert victim.read_text(encoding="utf-8") == "not a maxc database\n"
    assert _mode(victim) == 0o644


def test_loading_legacy_default_config_repairs_owner_only_permissions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "home"
    work = tmp_path / "work"
    maxc_dir = home / ".maxc"
    maxc_dir.mkdir(parents=True)
    work.mkdir()
    config_path = maxc_dir / "config.yaml"
    config_path.write_text(
        "auth:\n  access_id: legacy-ak\n  secret_access_key: legacy-secret\n",
        encoding="utf-8",
    )
    config_path.chmod(0o644)
    monkeypatch.setenv("HOME", str(home))

    load_config(work)

    assert _mode(config_path) == 0o600


def test_default_config_symlink_is_rejected_without_reading_target(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "home"
    work = tmp_path / "work"
    maxc_dir = home / ".maxc"
    maxc_dir.mkdir(parents=True)
    work.mkdir()
    victim = tmp_path / "victim.yaml"
    victim.write_text(
        "auth:\n  access_id: victim-ak\n  secret_access_key: victim-secret\n",
        encoding="utf-8",
    )
    victim.chmod(0o644)
    (maxc_dir / "config.yaml").symlink_to(victim)
    monkeypatch.setenv("HOME", str(home))

    with pytest.raises(ValidationError, match="unsafe or unreadable"):
        load_config(work)

    assert _mode(victim) == 0o644
    assert victim.read_text(encoding="utf-8").endswith("victim-secret\n")


@pytest.mark.parametrize("kind", ["cache", "job", "audit"])
def test_state_directory_leaf_symlink_is_rejected(
    tmp_path: Path,
    kind: str,
) -> None:
    real_directory = tmp_path / "real-state"
    real_directory.mkdir()
    linked_directory = tmp_path / "linked-state"
    linked_directory.symlink_to(real_directory, target_is_directory=True)

    if kind == "cache":
        with pytest.raises(ValidationError, match="cache directory is unavailable"):
            LocalCache(linked_directory)
    elif kind == "job":
        with pytest.raises(OSError):
            JobStore(linked_directory)
    else:
        with pytest.raises(OSError):
            AuditLogger(linked_directory / "audit.log")

    assert list(real_directory.iterdir()) == []


def test_cache_detects_parent_swap_before_sqlite_initialization_writes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cache_directory = tmp_path / "cache"
    cache = LocalCache(cache_directory)
    displaced = tmp_path / "cache-original"
    victim = tmp_path / "victim.db"
    original_connect = cache_module.sqlite3.connect
    with original_connect(victim) as connection:
        connection.execute("CREATE TABLE keep(value TEXT)")
        connection.execute("INSERT INTO keep VALUES ('unchanged')")
    original_bytes = victim.read_bytes()
    swapped = False

    def swapping_connect(path, *args, **kwargs):
        nonlocal swapped
        if not swapped:
            swapped = True
            cache_directory.rename(displaced)
            cache_directory.mkdir()
            (cache_directory / "cache.db").symlink_to(victim)
        return original_connect(path, *args, **kwargs)

    monkeypatch.setattr(cache_module.sqlite3, "connect", swapping_connect)

    with pytest.raises(ValidationError, match="path changed while it was in use"):
        with cache._connect():
            pass

    assert victim.read_bytes() == original_bytes


def test_audit_append_stays_in_the_directory_pinned_for_the_operation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_directory = tmp_path / "state"
    logger = AuditLogger(state_directory / "audit.log")
    original_open = audit_module.open_private_file_at
    displaced = tmp_path / "state-original"
    swapped = False

    def swapping_open(directory_descriptor, name, flags, **kwargs):
        nonlocal swapped
        if not swapped:
            swapped = True
            state_directory.rename(displaced)
            state_directory.mkdir()
            (state_directory / "audit.log").write_text("decoy\n", encoding="utf-8")
        return original_open(directory_descriptor, name, flags, **kwargs)

    monkeypatch.setattr(audit_module, "open_private_file_at", swapping_open)

    logger.log({"command": "query", "status": "success"})

    assert (state_directory / "audit.log").read_text(encoding="utf-8") == "decoy\n"
    assert '"command": "query"' in (displaced / "audit.log").read_text(
        encoding="utf-8"
    )


def test_job_store_lock_and_data_use_the_same_pinned_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_directory = tmp_path / "state"
    store = JobStore(state_directory)
    created = store.create_job(
        sql="SELECT 1",
        project="project-a",
        result={"data": {"rows": []}},
    )
    displaced = tmp_path / "state-original"
    original_load = store._load
    swapped = False

    def swapping_load(directory_descriptor):
        nonlocal swapped
        if not swapped:
            swapped = True
            state_directory.rename(displaced)
            state_directory.mkdir()
            (state_directory / "jobs.json").write_text(
                '{"jobs": {}, "idempotency": {}, "remote_job_contexts": {}}',
                encoding="utf-8",
            )
        return original_load(directory_descriptor)

    monkeypatch.setattr(store, "_load", swapping_load)

    jobs = store.list_jobs()

    assert [job["job_id"] for job in jobs] == [created["job_id"]]
    assert json.loads((state_directory / "jobs.json").read_text(encoding="utf-8"))[
        "jobs"
    ] == {}
