"""Tests for job command improvements."""

import json
import os
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.app import MaxCApp
from maxc_cli.cli import run
from maxc_cli.exceptions import BackendConnectionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_app(tmp_path: Path) -> MaxCApp:
    """Return a MaxCApp backed only by the local job store (no ODPS backend).

    Uses load_backend=False so no credentials are needed.
    Overrides state_dir to tmp_path/state to isolate each test's job store.
    """
    config_path = tmp_path / "config.yaml"
    config_path.write_text("default_project: test_project\n", encoding="utf-8")
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    app.remote_jobs = False
    app.config.state_dir = tmp_path / "state"  # isolate from other tests
    return app


def run_cmd(tmp_path: Path, argv: list[str]) -> tuple[int, dict, str]:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("default_project: test_project\n", encoding="utf-8")
    stdout = StringIO()
    stderr = StringIO()
    env_keys = [k for k in os.environ if "ODPS" in k or "MAXCOMPUTE" in k or "ALIBABA" in k]
    with patch.dict(os.environ, {k: "" for k in env_keys}, clear=False):
        code = run(["--config", str(config_path), *argv], cwd=tmp_path, stdout=stdout, stderr=stderr)
    raw = stdout.getvalue()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {}
    return code, data, stderr.getvalue()


# ---------------------------------------------------------------------------
# Task 1: Polling error handling
# ---------------------------------------------------------------------------

class FakeInstance:
    """Fake ODPS instance for testing wait_job polling."""

    def __init__(self, *, reload_errors: int = 0, final_status: str = "TERMINATED") -> None:
        self._reload_errors = reload_errors
        self._reload_calls = 0
        self.status = type("Status", (), {"__str__": lambda s: "Instance.RUNNING"})()
        self.start_time = None
        self.end_time = None
        self.tasks = []
        self._final_status_str = final_status

    def reload(self, blocking: bool = False) -> None:
        self._reload_calls += 1
        if self._reload_calls <= self._reload_errors:
            raise OSError(f"network error #{self._reload_calls}")
        # Set terminal status
        self.status = type("Status", (), {"__str__": lambda s: self._final_status_str})()

    def is_successful(self) -> bool:
        return True

    def get_task_statuses(self) -> list:
        return []

    def get_logview_address(self, *a, **kw) -> str:
        return ""


def make_job_mixin_with_instance(instance: FakeInstance):
    """Return a JobMixin-like object whose _get_instance returns the given instance."""
    from maxc_cli.backend.job import JobMixin

    class _TestBackend(JobMixin):
        project = "test_project"
        odps = None

        def _get_instance(self, job_id, *, project=None):
            return instance

        def _instance_to_job_info(self, inst, *, project=None):
            from maxc_cli.models import JobInfo
            status_str = str(getattr(inst, "status", "")).split(".")[-1]
            return JobInfo(
                job_id="job_test",
                status="success" if status_str != "RUNNING" else "running",
                progress=100,
                project=project or "test_project",
            )

    return _TestBackend()


def test_polling_continues_after_fewer_than_5_consecutive_errors() -> None:
    """4 consecutive reload failures should not raise — polling continues."""
    instance = FakeInstance(reload_errors=4)
    backend = make_job_mixin_with_instance(instance)
    result = backend.wait_job("job_test", poll_interval=0)
    assert result.status == "success"
    assert instance._reload_calls == 5  # 4 errors + 1 success


def test_polling_raises_after_5_consecutive_errors() -> None:
    """5 consecutive reload failures should raise BackendConnectionError."""
    instance = FakeInstance(reload_errors=10)  # always fails
    backend = make_job_mixin_with_instance(instance)
    with pytest.raises(BackendConnectionError, match="network error"):
        backend.wait_job("job_test", poll_interval=0)
    assert instance._reload_calls == 5


def test_polling_resets_error_count_on_success() -> None:
    """Error counter resets on a successful reload; a later burst of 4 errors is fine."""

    class IntermittentInstance(FakeInstance):
        def reload(self, blocking=False):
            self._reload_calls += 1
            # Fail calls 1–4 (4 errors), succeed call 5 (still RUNNING),
            # fail calls 6–9 (4 errors), succeed call 10 (terminal)
            if self._reload_calls in (1, 2, 3, 4, 6, 7, 8, 9):
                raise OSError("transient error")
            if self._reload_calls == 5:
                return  # reload succeeds but status remains RUNNING; counter resets, loop continues
            # call 10: terminal
            self.status = type("Status", (), {"__str__": lambda s: "TERMINATED"})()

    instance = IntermittentInstance(reload_errors=0)
    backend = make_job_mixin_with_instance(instance)
    result = backend.wait_job("job_test", poll_interval=0)
    assert result.status == "success"


# ---------------------------------------------------------------------------
# Task 2: job wait --timeout
# ---------------------------------------------------------------------------

def test_job_wait_accepts_timeout_flag(tmp_path: Path) -> None:
    """job wait --timeout N should be accepted by the parser without error."""
    code2, _, stderr = run_cmd(tmp_path, ["job", "wait", "--timeout", "600", "nonexistent_job_id"])
    # Will fail with NotFoundError, NOT an argparse error
    assert "unrecognized arguments" not in stderr
    assert code2 != 0  # job not found is expected


def test_job_wait_timeout_default_is_none(tmp_path: Path) -> None:
    """When --timeout is not supplied, args.timeout must be None."""
    from maxc_cli.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["job", "wait", "some_job_id"])
    assert args.timeout is None


# ---------------------------------------------------------------------------
# Task 3: job list --limit
# ---------------------------------------------------------------------------

def _seed_jobs(app: MaxCApp, count: int) -> list[str]:
    """Create `count` local jobs and return their IDs."""
    jobs_store = app._ensure_job_store()
    ids = []
    for i in range(count):
        job = jobs_store.create_job(
            sql=f"SELECT {i}",
            project="test_project",
            result={
                "data": {
                    "rows": [{"v": i}],
                    "schema": [{"name": "v", "type": "bigint"}],
                    "total_rows": 1,
                    "returned_rows": 1,
                    "has_more": False,
                    "next_cursor": None,
                },
                "metadata": {"project": "test_project", "elapsed_ms": 1},
                "agent_hints": {"warnings": []},
            },
        )
        ids.append(job["job_id"])
    return ids


def test_job_list_limit_flag_accepted(tmp_path: Path) -> None:
    """--limit flag must parse without error."""
    from maxc_cli.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["job", "list", "--limit", "5"])
    assert args.limit == 5


def test_job_list_default_limit_is_20(tmp_path: Path) -> None:
    from maxc_cli.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["job", "list"])
    assert args.limit == 20


def test_job_list_returns_at_most_limit_jobs(tmp_path: Path) -> None:
    """job list with limit=3 returns at most 3 jobs even when 5 exist."""
    app = make_app(tmp_path)
    _seed_jobs(app, 5)

    envelope = app.list_jobs(limit=3)
    assert envelope.status == "success"
    jobs = envelope.data["jobs"]
    assert len(jobs) == 3


# ---------------------------------------------------------------------------
# Task 4: job result --max-rows / --cursor
# ---------------------------------------------------------------------------

def _make_job_with_rows(app: MaxCApp, row_count: int) -> str:
    """Create a local job whose stored result has `row_count` rows."""
    jobs_store = app._ensure_job_store()
    rows = [{"n": i} for i in range(row_count)]
    schema = [{"name": "n", "type": "bigint"}]
    job = jobs_store.create_job(
        sql="SELECT n FROM t",
        project="test_project",
        result={
            "data": {
                "rows": rows,
                "schema": schema,
                "total_rows": row_count,
                "returned_rows": row_count,
                "has_more": False,
                "next_cursor": None,
            },
            "metadata": {"project": "test_project", "elapsed_ms": 10, "sql_executed": "SELECT n FROM t"},
            "agent_hints": {"warnings": []},
        },
    )
    jobs_store.update_job(job["job_id"], status="success", progress=100)
    return job["job_id"]


def test_job_result_max_rows_flag_accepted(tmp_path: Path) -> None:
    from maxc_cli.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["job", "result", "some_job", "--max-rows", "10"])
    assert args.max_rows == 10


def test_job_result_cursor_flag_accepted(tmp_path: Path) -> None:
    from maxc_cli.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["job", "result", "some_job", "--cursor", "abc123"])
    assert args.cursor == "abc123"


def test_job_result_default_max_rows_is_100(tmp_path: Path) -> None:
    from maxc_cli.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["job", "result", "some_job"])
    assert args.max_rows == 100
    assert args.cursor is None


def test_job_result_first_page(tmp_path: Path) -> None:
    """job result with max_rows=3 on a 5-row job returns 3 rows + has_more=True."""
    app = make_app(tmp_path)
    job_id = _make_job_with_rows(app, 5)

    envelope = app.job_result(job_id, max_rows=3)
    assert envelope.status == "success"
    data = envelope.data
    assert len(data["rows"]) == 3
    assert data["rows"] == [{"n": 0}, {"n": 1}, {"n": 2}]
    assert data["has_more"] is True
    assert data["next_cursor"] is not None


def test_job_result_second_page(tmp_path: Path) -> None:
    """Using next_cursor from first page yields the remaining rows."""
    app = make_app(tmp_path)
    job_id = _make_job_with_rows(app, 5)

    first = app.job_result(job_id, max_rows=3)
    cursor = first.data["next_cursor"]

    second = app.job_result(job_id, max_rows=3, cursor=cursor)
    assert second.status == "success"
    assert second.data["rows"] == [{"n": 3}, {"n": 4}]
    assert second.data["has_more"] is False
    assert second.data["next_cursor"] is None


def test_job_result_beyond_end_returns_empty(tmp_path: Path) -> None:
    """Cursor past the end of results returns empty rows, has_more=False."""
    from maxc_cli.utils import encode_cursor
    app = make_app(tmp_path)
    job_id = _make_job_with_rows(app, 3)

    cursor = encode_cursor(10)  # offset beyond all rows
    envelope = app.job_result(job_id, max_rows=10, cursor=cursor)
    assert envelope.status == "success"
    assert envelope.data["rows"] == []
    assert envelope.data["has_more"] is False


def test_job_result_session_id_in_cursor_is_ignored_for_local_jobs(tmp_path: Path) -> None:
    """A cursor with session_id should still work via offset-only on local path."""
    from maxc_cli.utils import encode_cursor
    app = make_app(tmp_path)
    job_id = _make_job_with_rows(app, 5)

    # cursor with offset=2 and a fake session_id=999
    cursor = encode_cursor(2, session_id=999)
    envelope = app.job_result(job_id, max_rows=2, cursor=cursor)
    assert envelope.data["rows"] == [{"n": 2}, {"n": 3}]
