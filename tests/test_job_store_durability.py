"""Durability and cross-process safety contracts for the local job store."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import maxc_cli.state_permissions as state_permissions
import maxc_cli.store as store_module
from maxc_cli.exceptions import ValidationError
from maxc_cli.store import JobStore

pytestmark = pytest.mark.unit


def _create_job(store: JobStore, value: int = 1) -> dict:
    return store.create_job(
        sql=f"SELECT {value}",
        project="test_project",
        result={"data": {"rows": [{"value": value}]}},
    )


def test_corrupted_json_fails_closed_without_overwriting_the_file(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    jobs_file = state_dir / "jobs.json"
    damaged = b'{"jobs": {'
    jobs_file.write_bytes(damaged)
    store = JobStore(state_dir)

    with pytest.raises(ValidationError, match="corrupted and was not modified"):
        _create_job(store)

    assert jobs_file.read_bytes() == damaged
    assert list(state_dir.glob(".jobs.json.*.tmp")) == []


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"jobs": [], "idempotency": {}, "remote_job_contexts": {}},
        {"jobs": {}, "idempotency": [], "remote_job_contexts": {}},
        {"jobs": {}, "idempotency": {}, "remote_job_contexts": []},
    ],
)
def test_invalid_store_shape_is_not_treated_as_an_empty_store(
    tmp_path: Path,
    payload: object,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    jobs_file = state_dir / "jobs.json"
    original = json.dumps(payload).encode()
    jobs_file.write_bytes(original)
    store = JobStore(state_dir)

    with pytest.raises(ValidationError, match="corrupted and was not modified"):
        store.list_jobs()

    assert jobs_file.read_bytes() == original


def test_failed_atomic_replace_preserves_previous_store(tmp_path: Path, monkeypatch) -> None:
    store = JobStore(tmp_path / "state")
    job = _create_job(store)
    original = store.path.read_bytes()

    def fail_replace(source, destination, **_kwargs):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(store_module.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated replace failure"):
        store.update_job(job["job_id"], status="success")

    assert store.path.read_bytes() == original
    assert list(store.state_dir.glob(".jobs.json.*.tmp")) == []


def test_save_fsyncs_file_and_atomically_replaces_target(tmp_path: Path, monkeypatch) -> None:
    store = JobStore(tmp_path / "state")
    calls: list[int] = []
    original_fsync = store_module.os.fsync

    def recording_fsync(descriptor: int) -> None:
        calls.append(descriptor)
        original_fsync(descriptor)

    monkeypatch.setattr(store_module.os, "fsync", recording_fsync)

    _create_job(store)

    assert store.path.exists()
    assert calls
    assert list(store.state_dir.glob(".jobs.json.*.tmp")) == []


@pytest.mark.skipif(os.name != "posix", reason="exercises POSIX cross-process locking")
def test_concurrent_processes_do_not_lose_read_modify_write_updates(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    start_file = tmp_path / "start"
    worker_count = 4
    jobs_per_worker = 20
    script = """
import sys
import time
from pathlib import Path
from maxc_cli.store import JobStore

state_dir = Path(sys.argv[1])
start_file = Path(sys.argv[2])
worker = int(sys.argv[3])
count = int(sys.argv[4])
while not start_file.exists():
    time.sleep(0.002)
store = JobStore(state_dir)
for index in range(count):
    store.create_job(
        sql=f"SELECT {worker * count + index}",
        project="test_project",
        result={"worker": worker, "index": index},
    )
"""
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                os.fspath(state_dir),
                os.fspath(start_file),
                str(worker),
                str(jobs_per_worker),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for worker in range(worker_count)
    ]
    start_file.touch()

    failures = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=30)
        if process.returncode != 0:
            failures.append((process.returncode, stdout, stderr))
    assert failures == []

    jobs = JobStore(state_dir).list_jobs()
    assert len(jobs) == worker_count * jobs_per_worker
    assert {job["sql"] for job in jobs} == {
        f"SELECT {value}" for value in range(worker_count * jobs_per_worker)
    }


def test_windows_lock_backend_serializes_with_msvcrt_byte_lock(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FakeMsvcrt:
        LK_LOCK = 1
        LK_UNLCK = 2

        def __init__(self) -> None:
            self.calls: list[tuple[int, int]] = []

        def locking(self, descriptor: int, operation: int, length: int) -> None:
            self.calls.append((operation, length))

    fake = FakeMsvcrt()
    monkeypatch.setattr(state_permissions, "_fcntl", None)
    monkeypatch.setattr(state_permissions, "_msvcrt", fake)
    store = JobStore(tmp_path / "state")

    with store._locked():
        pass

    assert fake.calls == [(fake.LK_LOCK, 1), (fake.LK_UNLCK, 1)]
    assert store.lock_path.read_bytes() == b"\0"


def test_unknown_locking_platform_fails_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(state_permissions, "_fcntl", None)
    monkeypatch.setattr(state_permissions, "_msvcrt", None)
    store = JobStore(tmp_path / "state")

    with pytest.raises(RuntimeError, match="requires fcntl or msvcrt"):
        store.list_jobs()
