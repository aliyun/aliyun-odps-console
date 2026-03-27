"""Tests for job command improvements."""

import json
import os
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest

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
