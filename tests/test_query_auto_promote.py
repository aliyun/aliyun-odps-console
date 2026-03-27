"""Tests for query auto-promote feature (--wait flag, removal of --async/--timeout)."""
from unittest.mock import MagicMock, patch
from pathlib import Path
import pytest
from maxc_cli.cli import build_parser
from maxc_cli.app import MaxCApp
from maxc_cli.models import JobInfo, QueryResult
from maxc_cli.exceptions import JobTimeoutError, BackendConnectionError


def _make_app(tmp_path: Path) -> MaxCApp:
    """Build a MaxCApp with no real backend, using a temp config dir."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("backend:\n  type: auto\n")
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    app.config.state_dir = tmp_path / "state"
    # Simulate remote_jobs=True
    app.remote_jobs = True  # remote_jobs is a plain instance attribute set in __init__
    return app


def _fake_job_info(job_id="job-1", status="pending") -> JobInfo:
    return JobInfo(
        job_id=job_id,
        status=status,
        project="test_proj",
        progress=0,
        submitted_at="2026-01-01T00:00:00Z",
        logview="http://logview/job-1",
    )


def _fake_query_result() -> QueryResult:
    return QueryResult(
        rows=[{"x": 1}],
        schema=[{"name": "x", "type": "bigint"}],
        total_rows=1,
        returned_rows=1,
        has_more=False,
        next_cursor=None,
        project="test_proj",
        elapsed_ms=100,
        bytes_scanned=None,
        sql_executed="SELECT 1",
        tables_used=[],
    )


def test_query_wait_default_is_10():
    parser = build_parser()
    args = parser.parse_args(["query", "SELECT 1"])
    assert args.wait == 10


def test_query_wait_flag_accepted():
    parser = build_parser()
    args30 = parser.parse_args(["query", "--wait", "30", "SELECT 1"])
    assert args30.wait == 30
    args0 = parser.parse_args(["query", "--wait", "0", "SELECT 1"])
    assert args0.wait == 0


def test_query_no_longer_has_async_flag():
    parser = build_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["query", "--async", "SELECT 1"])
    assert exc_info.value.code == 2


def test_query_no_longer_has_timeout_flag():
    parser = build_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["query", "--timeout", "60", "SELECT 1"])
    assert exc_info.value.code == 2


def test_query_returns_success_when_job_finishes_within_wait(tmp_path):
    app = _make_app(tmp_path)
    job_done = _fake_job_info(status="success")
    result = _fake_query_result()

    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = job_done
    app.backend.fetch_job_result.return_value = result

    envelope = app.query(command="query", sql="SELECT 1", wait=10)

    assert envelope.status == "success"
    assert envelope.data["rows"] == [{"x": 1}]
    app.backend.wait_job.assert_called_once()
    _, kwargs = app.backend.wait_job.call_args
    assert kwargs["timeout"] == 10
    assert kwargs["poll_interval"] == 1


def test_query_auto_promotes_on_timeout(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(job_id="job-99", status="pending")
    app.backend.wait_job.side_effect = JobTimeoutError("timed out")

    envelope = app.query(command="query", sql="SELECT 1", wait=5)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "job-99"
    assert envelope.metadata["wait_seconds"] == 5
    assert envelope.metadata["job_id"] == "job-99"


def test_query_returns_failure_when_job_fails(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = _fake_job_info(status="failure")

    envelope = app.query(command="query", sql="SELECT 1", wait=10)

    assert envelope.status == "failure"
    assert "job.diagnose" in envelope.agent_hints.next_actions


def test_query_wait_0_submits_and_returns_pending(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(job_id="job-42", status="pending")

    envelope = app.query(command="query", sql="SELECT 1", wait=0)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "job-42"
    assert envelope.metadata["wait_seconds"] == 0
    app.backend.wait_job.assert_not_called()


def test_query_backend_connection_error_includes_job_id(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(job_id="job-err", status="pending")
    app.backend.wait_job.side_effect = BackendConnectionError("network lost")

    envelope = app.query(command="query", sql="SELECT 1", wait=10)

    assert envelope.status == "error"
    assert envelope.metadata["job_id"] == "job-err"
    assert "job.status" in envelope.agent_hints.next_actions


def test_query_fetch_failure_after_success_includes_job_id(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = _fake_job_info(job_id="job-fetch-err", status="success")
    app.backend.fetch_job_result.side_effect = RuntimeError("S3 gone")

    envelope = app.query(command="query", sql="SELECT 1", wait=10)

    assert envelope.status == "error"
    assert envelope.metadata["job_id"] == "job-fetch-err"
    assert "job.result" in envelope.agent_hints.next_actions


def test_query_wait_job_called_with_poll_interval_1(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = _fake_job_info(status="success")
    app.backend.fetch_job_result.return_value = _fake_query_result()

    app.query(command="query", sql="SELECT 1", wait=30)

    app.backend.wait_job.assert_called_once()
    _, kwargs = app.backend.wait_job.call_args
    assert kwargs["poll_interval"] == 1


def test_submit_job_local_backend_returns_success(tmp_path):
    """After auto-promote, submit_job on local backend returns success (not pending)."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("backend:\n  type: auto\n")
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    app.config.state_dir = tmp_path / "state"
    # local backend (remote_jobs=False)
    app.remote_jobs = False

    # Patch _execute_query to return a fake result
    fake_result = _fake_query_result()
    with patch.object(app, "_execute_query", return_value=fake_result):
        envelope = app.submit_job(sql="SELECT 1")

    assert envelope.status == "success"


# job_wait timeout/connection-error tests


def test_job_wait_timeout_returns_pending(tmp_path):
    """job_wait with JobTimeoutError → status=pending, not failure."""
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.get_job.return_value = _fake_job_info(job_id="job-abc", status="running")
    app.backend.wait_job.side_effect = JobTimeoutError("Job did not complete within 30 seconds")

    envelope, events = app.job_wait("job-abc", timeout=30)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "job-abc"
    assert envelope.metadata["job_id"] == "job-abc"
    assert envelope.metadata["wait_seconds"] == 30
    assert "job.wait" in envelope.agent_hints.next_actions
    assert events == []


def test_job_wait_connection_error_returns_error_with_job_id(tmp_path):
    """job_wait with BackendConnectionError → status=error, job_id in metadata."""
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.get_job.return_value = _fake_job_info(job_id="job-xyz", status="running")
    app.backend.wait_job.side_effect = BackendConnectionError(
        "Lost contact after 5 errors", suggestion="Check network."
    )

    envelope, events = app.job_wait("job-xyz", timeout=60)

    assert envelope.status == "error"
    assert envelope.metadata["job_id"] == "job-xyz"
    assert "job.status" in envelope.agent_hints.next_actions
    assert events == []
