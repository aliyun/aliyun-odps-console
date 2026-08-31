"""Tests for query auto-promote feature (--wait flag, removal of --async/--timeout)."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit
from maxc_cli.app import MaxCApp
from maxc_cli.cli import build_parser
from maxc_cli.exceptions import BackendConnectionError, JobTimeoutError
from maxc_cli.models import JobInfo, QueryResult


def _make_app(tmp_path: Path) -> MaxCApp:
    """Build a MaxCApp with no real backend, using a temp config dir."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("backend:\n  type: auto\n")
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    app.config.state_dir = tmp_path / "state"
    # Simulate remote_jobs=True
    app.remote_jobs = True  # remote_jobs is a plain instance attribute set in __init__
    return app


def _fake_job_info(job_id="job-1", status="pending", failure_reason=None) -> JobInfo:
    return JobInfo(
        job_id=job_id,
        status=status,
        project="test_proj",
        progress=0,
        submitted_at="2026-01-01T00:00:00Z",
        logview="http://logview/job-1",
        failure_reason=failure_reason,
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


def test_query_returns_successful_statement_envelope_for_ddl(tmp_path):
    app = _make_app(tmp_path)
    job_done = _fake_job_info(status="success")
    result = QueryResult(
        rows=[],
        schema=[],
        total_rows=0,
        returned_rows=0,
        has_more=False,
        next_cursor=None,
        project="test_proj",
        elapsed_ms=100,
        bytes_scanned=None,
        sql_executed=(
            "SET odps.sql.type.system.odps2=true; "
            "CREATE TABLE IF NOT EXISTS t (id BIGINT)"
        ),
        tables_used=["t"],
        extra_metadata={
            "result_kind": "statement",
            "statement_operation": "CREATE",
        },
    )

    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = job_done
    app.backend.fetch_job_result.return_value = result

    envelope = app.query(
        command="query",
        sql="CREATE TABLE IF NOT EXISTS t (id BIGINT)",
        wait=10,
        force=True,
    )

    assert envelope.status == "success"
    assert envelope.data["rows"] == []
    assert envelope.data["schema"] == []
    assert envelope.metadata["result_kind"] == "statement"
    assert envelope.data["safety"]["mode"] == "force"
    assert envelope.data["safety"]["allowed_operations"] == ["CREATE"]
    assert all(action.id != "meta.describe" for action in envelope.agent_hints.actions)
    assert any(
        "completed successfully" in insight
        for insight in envelope.agent_hints.insights
    )
    assert not any(
        "filters" in insight
        for insight in envelope.agent_hints.insights
    )


def test_job_wait_observes_completed_ddl_without_blocked_safety(tmp_path):
    app = _make_app(tmp_path)
    before = _fake_job_info(job_id="ddl-job", status="pending")
    after = _fake_job_info(job_id="ddl-job", status="success")
    result = QueryResult(
        rows=[],
        schema=[],
        total_rows=0,
        returned_rows=0,
        has_more=False,
        next_cursor=None,
        project="test_proj",
        elapsed_ms=100,
        bytes_scanned=None,
        sql_executed="DROP TABLE t",
        tables_used=["t"],
        extra_metadata={
            "result_kind": "statement",
            "statement_operation": "DROP",
        },
    )
    app.backend = MagicMock()
    app.backend.get_job.return_value = before
    app.backend.wait_job.return_value = after
    app.backend.fetch_job_result.return_value = result

    envelope, _ = app.job_wait("ddl-job", timeout=10)

    assert envelope.status == "success"
    assert envelope.data["safety"]["policy_decision"] == "allowed"
    assert envelope.data["safety"]["mode"] == "read_only"
    assert envelope.data["safety"]["allowed_operations"] == ["JOB_WAIT"]
    assert envelope.data["safety"]["scope"] == "result_observation"
    assert all(action.id != "meta.describe" for action in envelope.agent_hints.actions)


def test_query_auto_promotes_on_timeout(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(job_id="job-99", status="pending")
    app.backend.wait_job.side_effect = JobTimeoutError("timed out")

    envelope = app.query(command="query", sql="SELECT 1", wait=5, max_rows=2)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "job-99"
    assert envelope.metadata["wait_seconds"] == 5
    assert envelope.metadata["job_id"] == "job-99"
    assert envelope.metadata["requested_max_rows"] == 2
    actions = {item.id: item.command for item in envelope.agent_hints.actions}
    assert actions["job.result"].endswith(
        "job result job-99 --project test_proj --max-rows 2 --json"
    )


def test_query_returns_failure_when_job_fails(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = _fake_job_info(status="failure")

    envelope = app.query(command="query", sql="SELECT 1", wait=10)

    assert envelope.status == "failure"
    action_ids = [a.id for a in envelope.agent_hints.actions]
    assert "job.diagnose" in action_ids


def test_query_job_failure_classifies_table_not_found(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = _fake_job_info(
        status="failure",
        failure_reason=(
            "ODPS-0130131:[1,15] Table not found - table "
            "meta_dev.missing_table cannot be resolved"
        ),
    )

    envelope = app.query(command="query", sql="SELECT * FROM missing_table", wait=10)

    assert envelope.status == "failure"
    assert envelope.error is not None
    assert envelope.error.code == "TABLE_NOT_FOUND"
    assert envelope.error.suggestion is not None
    assert "maxc meta search" in envelope.error.suggestion


def test_query_wait_0_submits_and_returns_pending(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(job_id="job-42", status="pending")

    envelope = app.query(command="query", sql="SELECT 1", wait=0, max_rows=7)

    assert envelope.status == "pending"
    assert envelope.data["job_id"] == "job-42"
    assert envelope.metadata["wait_seconds"] == 0
    assert envelope.metadata["requested_max_rows"] == 7
    assert any(
        item.id == "job.result" and "--max-rows 7" in item.command
        for item in envelope.agent_hints.actions
    )
    app.backend.wait_job.assert_not_called()


def test_query_backend_connection_error_includes_job_id(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(job_id="job-err", status="pending")
    app.backend.wait_job.side_effect = BackendConnectionError("network lost")

    envelope = app.query(command="query", sql="SELECT 1", wait=10)

    assert envelope.status == "failure"
    assert envelope.metadata["job_id"] == "job-err"
    action_ids = [a.id for a in envelope.agent_hints.actions]
    assert "job.status" in action_ids


def test_query_fetch_failure_after_success_includes_job_id(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = _fake_job_info(job_id="job-fetch-err", status="success")
    app.backend.fetch_job_result.side_effect = RuntimeError("S3 gone")

    envelope = app.query(command="query", sql="SELECT 1", wait=10)

    assert envelope.status == "failure"
    # The externally published submission ID remains stable even if a backend
    # status object reports a different internal identifier.
    assert envelope.metadata["job_id"] == "job-1"
    action_ids = [a.id for a in envelope.agent_hints.actions]
    assert "job.result" in action_ids


def test_query_fetch_preserves_typed_backend_error_after_success(tmp_path):
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.submit_query.return_value = _fake_job_info(status="pending")
    app.backend.wait_job.return_value = _fake_job_info(status="success")
    app.backend.fetch_job_result.side_effect = BackendConnectionError(
        "result service unavailable",
        suggestion="Retry result fetch for the same job.",
    )

    envelope = app.query(command="query", sql="SELECT 1", wait=10)
    payload = envelope.to_dict()

    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "BACKEND_CONNECTION_ERROR"
    assert payload["error"]["suggestion"] == "Retry result fetch for the same job."
    assert payload["metadata"]["job_id"] == "job-1"


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
    action_ids = [a.id for a in envelope.agent_hints.actions]
    assert "job.wait" in action_ids
    assert events == []


def test_job_wait_connection_error_returns_error_with_job_id(tmp_path):
    """job_wait with BackendConnectionError → status=failure, job_id in metadata."""
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    app.backend.get_job.return_value = _fake_job_info(job_id="job-xyz", status="running")
    app.backend.wait_job.side_effect = BackendConnectionError(
        "Lost contact after 5 errors", suggestion="Check network."
    )

    envelope, events = app.job_wait("job-xyz", timeout=60)

    assert envelope.status == "failure"
    assert envelope.metadata["job_id"] == "job-xyz"
    action_ids = [a.id for a in envelope.agent_hints.actions]
    assert "job.status" in action_ids
    assert events == []


@pytest.mark.parametrize("operation", ["status", "wait", "result"])
def test_failed_remote_job_envelopes_include_structured_error(
    tmp_path: Path,
    operation: str,
) -> None:
    app = _make_app(tmp_path)
    app.backend = MagicMock()
    failed = _fake_job_info(
        job_id="job-failed",
        status="failure",
        failure_reason="table missing_table does not exist",
    )
    failed.retryable = False
    app.backend.get_job.return_value = failed
    app.backend.wait_job.return_value = failed

    if operation == "status":
        envelope = app.job_status("job-failed", project="test_proj")
    elif operation == "wait":
        envelope, _events = app.job_wait("job-failed", project="test_proj")
    else:
        envelope = app.job_result("job-failed", project="test_proj")

    payload = envelope.to_dict()
    assert payload["status"] == "failure"
    assert payload["error"] is not None
    assert payload["error"]["message"] == "table missing_table does not exist"
    assert payload["error"]["instance_id"] == "job-failed"
    assert payload["error"]["logview"] == "http://logview/job-1"
    assert payload["error"]["context"] == {
        "job_id": "job-failed",
        "project": "test_proj",
        "job_status": "failure",
    }


def test_error_payload_redacts_signed_logview_url() -> None:
    from maxc_cli.exceptions import ErrorPayload

    payload = ErrorPayload(
        code="EXECUTION_FAILED",
        message="failed",
        suggestion=None,
        recoverable=False,
        logview="https://logview.example.test/job-1?token=secret#signed",
    ).to_dict()

    assert payload["logview"] == "https://logview.example.test/job-1"


def test_error_payload_keeps_only_safe_numeric_logview_selector() -> None:
    from maxc_cli.exceptions import ErrorPayload

    payload = ErrorPayload(
        code="EXECUTION_FAILED",
        message="failed",
        suggestion=None,
        recoverable=False,
        logview=(
            "https://logview.example.test/job-1"
            "?subQuery=9&signature=secret&token=secret"
        ),
    ).to_dict()

    assert payload["logview"] == (
        "https://logview.example.test/job-1?subQuery=9"
    )
