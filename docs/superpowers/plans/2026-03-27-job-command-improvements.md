# Job Command Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix four gaps in long-running and large-result job support: polling error handling, `job wait --timeout`, `job list --limit`, and `job result` pagination.

**Architecture:** Changes are layered — backend `wait_job` gets error handling first, then app-layer method signatures expand, then CLI flags are wired in. All four changes are independent of each other and can be done sequentially. Tests run via `pytest` against local job path (no ODPS credentials needed for app/CLI tests).

**Tech Stack:** Python, argparse, pytest. No new dependencies.

---

## Files touched

| File | Changes |
|------|---------|
| `src/maxc_cli/backend/job.py` | Task 1: consecutive-error counter, fix outer except |
| `src/maxc_cli/app.py` | Tasks 2–4: expand `job_wait`, `list_jobs`, `job_result` signatures |
| `src/maxc_cli/cli.py` | Tasks 2–4: add `--timeout`, `--limit`, `--max-rows`, `--cursor` flags |
| `tests/test_job_improvements.py` | New file: all tests for this feature |

---

## Task 1: Fix polling error handling in `backend/job.py`

**Files:**
- Modify: `src/maxc_cli/backend/job.py:47-74`
- Test: `tests/test_job_improvements.py` (new)

The current `wait_job` has two problems:
1. Inner `except Exception: pass` silently swallows all `reload()` errors (line 58–61).
2. Outer `except Exception: pass` (lines 72–74) would swallow any `BackendConnectionError` raised by the new counter logic.

Fix: add a consecutive-errors counter inside the poll loop; replace the outer handler with `except TimeoutError: raise`.

- [ ] **Step 1: Create test file and write failing tests**

Create `tests/test_job_improvements.py`:

```python
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
    return code, json.loads(stdout.getvalue()), stderr.getvalue()


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
        # After all errors, the next reload sets terminal status
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
            # Fail calls 1, 2, 3, 4 (4 errors), succeed call 5,
            # fail calls 6, 7, 8, 9 (4 errors), succeed call 10 (terminal)
            if self._reload_calls in (1, 2, 3, 4, 6, 7, 8, 9):
                raise OSError("transient error")
            if self._reload_calls == 5:
                return  # still RUNNING after success — stays running
            # call 10: terminal
            self.status = type("Status", (), {"__str__": lambda s: "TERMINATED"})()

    instance = IntermittentInstance(reload_errors=0)
    backend = make_job_mixin_with_instance(instance)
    result = backend.wait_job("job_test", poll_interval=0)
    assert result.status == "success"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/dingxin/pythonProject/maxc-cli
pytest tests/test_job_improvements.py::test_polling_continues_after_fewer_than_5_consecutive_errors tests/test_job_improvements.py::test_polling_raises_after_5_consecutive_errors tests/test_job_improvements.py::test_polling_resets_error_count_on_success -v
```

Expected: FAIL (current code swallows errors, never raises).

- [ ] **Step 3: Implement the fix in `backend/job.py`**

Replace lines 47–76 (the entire `try` block inside `wait_job`):

```python
        start_time = monotonic()
        default_timeout = timeout or 300
        consecutive_errors = 0
        last_error: Exception | None = None

        while True:
            elapsed = monotonic() - start_time
            if elapsed > default_timeout:
                raise TimeoutError(
                    f"Job {job_id} did not complete within {default_timeout} seconds"
                )

            try:
                instance.reload(blocking=False)
                consecutive_errors = 0
            except Exception as exc:
                consecutive_errors += 1
                last_error = exc
                if consecutive_errors >= 5:
                    from ..exceptions import BackendConnectionError
                    raise BackendConnectionError(
                        f"Lost contact with backend after 5 consecutive errors: {exc}",
                        suggestion="Check network connectivity and retry.",
                    ) from exc

            status_name = str(getattr(instance, "status", "")).split(".")[-1]
            if status_name != "RUNNING":
                break

            sleep(poll_interval)
```

Also replace the outer `except Exception: pass` block (currently lines 71–74) with:

```python
        except TimeoutError:
            raise
```

So the full method after the change looks like:

```python
    def wait_job(
        self,
        job_id: 'str',
        *,
        project: 'str | None' = None,
        timeout: 'int | None' = None,
        poll_interval: 'int' = 3,
    ) -> 'JobInfo':
        instance = self._get_instance(job_id, project=project)
        start_time = monotonic()
        default_timeout = timeout or 300
        consecutive_errors = 0
        last_error: 'Exception | None' = None

        try:
            while True:
                elapsed = monotonic() - start_time
                if elapsed > default_timeout:
                    raise TimeoutError(
                        f"Job {job_id} did not complete within {default_timeout} seconds"
                    )

                try:
                    instance.reload(blocking=False)
                    consecutive_errors = 0
                except Exception as exc:
                    consecutive_errors += 1
                    last_error = exc
                    if consecutive_errors >= 5:
                        from ..exceptions import BackendConnectionError
                        raise BackendConnectionError(
                            f"Lost contact with backend after 5 consecutive errors: {exc}",
                            suggestion="Check network connectivity and retry.",
                        ) from exc

                status_name = str(getattr(instance, "status", "")).split(".")[-1]
                if status_name != "RUNNING":
                    break

                sleep(poll_interval)

        except TimeoutError:
            raise

        return self._instance_to_job_info(instance, project=project or self.project)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_job_improvements.py::test_polling_continues_after_fewer_than_5_consecutive_errors tests/test_job_improvements.py::test_polling_raises_after_5_consecutive_errors tests/test_job_improvements.py::test_polling_resets_error_count_on_success -v
```

Expected: all 3 PASS.

- [ ] **Step 5: Run full test suite to check no regressions**

```bash
pytest --ignore=tests/test_integration_real.py -x -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/maxc_cli/backend/job.py tests/test_job_improvements.py
git commit -m "fix: raise BackendConnectionError after 5 consecutive polling failures"
```

---

## Task 2: Add `--timeout` to `job wait`

**Files:**
- Modify: `src/maxc_cli/cli.py:91-95` (add `--timeout` arg), `src/maxc_cli/cli.py:489-494` (pass to app)
- Modify: `src/maxc_cli/app.py:387` (expand `job_wait` signature), `src/maxc_cli/app.py:391` (pass to backend)
- Test: `tests/test_job_improvements.py` (append)

- [ ] **Step 1: Write failing test**

Append to `tests/test_job_improvements.py`:

```python
# ---------------------------------------------------------------------------
# Task 2: job wait --timeout
# ---------------------------------------------------------------------------

def test_job_wait_accepts_timeout_flag(tmp_path: Path, monkeypatch) -> None:
    """job wait --timeout N should pass N to backend.wait_job."""
    # Use local job path — timeout only affects remote wait; for local jobs the
    # flag must be accepted without error (it's a no-op on local path).
    code, payload, _ = run_cmd(tmp_path, ["job", "list", "--json"])
    # No job exists yet; just verify the flag parses without error
    code2, _, stderr = run_cmd(tmp_path, ["job", "wait", "--timeout", "600", "nonexistent_job_id"])
    # Will fail with NotFoundError, NOT an argparse error
    assert "unrecognized arguments" not in stderr
    assert code2 != 0  # job not found is expected


def test_job_wait_timeout_default_is_none(tmp_path: Path) -> None:
    """When --timeout is not supplied, args.timeout must be None (not a string)."""
    from maxc_cli.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["job", "wait", "some_job_id"])
    assert args.timeout is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_job_improvements.py::test_job_wait_accepts_timeout_flag tests/test_job_improvements.py::test_job_wait_timeout_default_is_none -v
```

Expected: FAIL (`unrecognized arguments: --timeout` or `AttributeError: timeout`).

- [ ] **Step 3: Add `--timeout` to CLI parser (`cli.py`)**

In `build_parser()`, find the `job_wait` block (currently lines 91–95):

```python
    job_wait = job_subparsers.add_parser("wait", help="Wait for a job to finish")
    job_wait.add_argument("job_id")
    job_wait.add_argument("--json", action="store_true")
    job_wait.add_argument("--stream", action="store_true")
    job_wait.set_defaults(handler=_handle_job_wait)
```

Change to:

```python
    job_wait = job_subparsers.add_parser("wait", help="Wait for a job to finish")
    job_wait.add_argument("job_id")
    job_wait.add_argument("--json", action="store_true")
    job_wait.add_argument("--stream", action="store_true")
    job_wait.add_argument("--timeout", type=int, default=None, help="Timeout in seconds (default: 300)")
    job_wait.set_defaults(handler=_handle_job_wait)
```

- [ ] **Step 4: Pass `timeout` through handler and app**

In `_handle_job_wait` (currently ~line 489–494):

```python
def _handle_job_wait(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope, events = app.job_wait(args.job_id)
    ...
```

Change to:

```python
def _handle_job_wait(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope, events = app.job_wait(args.job_id, timeout=args.timeout)
    ...
```

In `app.py`, change `job_wait` signature (line 387):

```python
    def job_wait(self, job_id: 'str') -> 'tuple[Envelope, list[dict[str, Any]]]':
```

to:

```python
    def job_wait(self, job_id: 'str', *, timeout: 'int | None' = None) -> 'tuple[Envelope, list[dict[str, Any]]]':
```

In the remote path (line 391), change:

```python
            after = self.backend.wait_job(job_id, project=self.config.default_project)
```

to:

```python
            after = self.backend.wait_job(job_id, project=self.config.default_project, timeout=timeout)
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/test_job_improvements.py::test_job_wait_accepts_timeout_flag tests/test_job_improvements.py::test_job_wait_timeout_default_is_none -v
```

Expected: both PASS.

- [ ] **Step 6: Run full suite**

```bash
pytest --ignore=tests/test_integration_real.py -x -q
```

- [ ] **Step 7: Commit**

```bash
git add src/maxc_cli/cli.py src/maxc_cli/app.py tests/test_job_improvements.py
git commit -m "feat: add --timeout flag to job wait"
```

---

## Task 3: Add `--limit` to `job list`

**Files:**
- Modify: `src/maxc_cli/cli.py:112-114` (add `--limit` arg), `src/maxc_cli/cli.py:512-514` (pass to app)
- Modify: `src/maxc_cli/app.py:602` (expand `list_jobs` signature), `src/maxc_cli/app.py:604,636` (use limit)
- Test: `tests/test_job_improvements.py` (append)

- [ ] **Step 1: Write failing tests**

Append to `tests/test_job_improvements.py`:

```python
# ---------------------------------------------------------------------------
# Task 3: job list --limit
# ---------------------------------------------------------------------------

def _seed_jobs(app: MaxCApp, count: int) -> list[str]:
    """Create `count` local jobs and return their IDs."""
    from maxc_cli.utils import encode_cursor
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


def test_job_list_returns_at_most_limit_jobs(tmp_path: Path, monkeypatch) -> None:
    """job list --limit 3 returns at most 3 jobs even when 5 exist."""
    import os
    monkeypatch.setenv("HOME", str(tmp_path))
    app = make_app(tmp_path)
    _seed_jobs(app, 5)

    envelope = app.list_jobs(limit=3)
    assert envelope.status == "success"
    jobs = envelope.data["jobs"]
    assert len(jobs) == 3
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_job_improvements.py::test_job_list_limit_flag_accepted tests/test_job_improvements.py::test_job_list_default_limit_is_20 tests/test_job_improvements.py::test_job_list_returns_at_most_limit_jobs -v
```

Expected: FAIL.

- [ ] **Step 3: Add `--limit` to CLI parser**

In `build_parser()`, find the `job_list` block (lines 112–114):

```python
    job_list = job_subparsers.add_parser("list", help="List jobs")
    job_list.add_argument("--json", action="store_true")
    job_list.set_defaults(handler=_handle_job_list)
```

Change to:

```python
    job_list = job_subparsers.add_parser("list", help="List jobs")
    job_list.add_argument("--json", action="store_true")
    job_list.add_argument("--limit", type=int, default=20, help="Maximum number of jobs to return (default: 20)")
    job_list.set_defaults(handler=_handle_job_list)
```

- [ ] **Step 4: Pass `limit` through handler**

In `_handle_job_list` (line 512–514):

```python
def _handle_job_list(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.list_jobs()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
```

Change to:

```python
def _handle_job_list(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.list_jobs(limit=args.limit)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
```

- [ ] **Step 5: Expand `list_jobs` in `app.py`**

Change signature (line 602):

```python
    def list_jobs(self) -> 'Envelope':
```

to:

```python
    def list_jobs(self, *, limit: 'int' = 20) -> 'Envelope':
```

Remote path (line 604) — change `limit=20` to `limit=limit`:

```python
            jobs = self.backend.list_jobs(project=self.config.default_project, limit=limit)
```

Local path — `stored_jobs = jobs.list_jobs()` returns all jobs sorted by time. Slice after building `rows`:

Find the local path list comprehension and slice the result. The local path currently:

```python
        jobs = self._ensure_job_store()
        stored_jobs = jobs.list_jobs()
        rows = [
            {
                "job_id": item["job_id"],
                ...
            }
            for item in stored_jobs
        ]
```

Change to:

```python
        jobs = self._ensure_job_store()
        stored_jobs = jobs.list_jobs()[:limit]
        rows = [
            {
                "job_id": item["job_id"],
                ...
            }
            for item in stored_jobs
        ]
```

- [ ] **Step 6: Run tests**

```bash
pytest tests/test_job_improvements.py::test_job_list_limit_flag_accepted tests/test_job_improvements.py::test_job_list_default_limit_is_20 tests/test_job_improvements.py::test_job_list_returns_at_most_limit_jobs -v
```

Expected: all PASS.

- [ ] **Step 7: Run full suite**

```bash
pytest --ignore=tests/test_integration_real.py -x -q
```

- [ ] **Step 8: Commit**

```bash
git add src/maxc_cli/cli.py src/maxc_cli/app.py tests/test_job_improvements.py
git commit -m "feat: add --limit flag to job list"
```

---

## Task 4: Add `--max-rows` / `--cursor` to `job result`

**Files:**
- Modify: `src/maxc_cli/cli.py:102-105` (add args), `src/maxc_cli/cli.py:502-504` (pass to app)
- Modify: `src/maxc_cli/app.py:467` (expand signature + implement pagination)
- Test: `tests/test_job_improvements.py` (append)

**Key data layout for local jobs:**
- `job["result"]` is an envelope dict (from `_query_result_payload`)
- `job["result"]["data"]["rows"]` — full row list
- `job["result"]["data"]["total_rows"]` — int
- `job["result"]["data"]["schema"]` — column list
- For pagination: slice `rows[offset:offset+max_rows]`, compute `has_more`, build `next_cursor` with `encode_cursor(next_offset)`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_job_improvements.py`:

```python
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
    from maxc_cli.utils import decode_cursor
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_job_improvements.py::test_job_result_max_rows_flag_accepted tests/test_job_improvements.py::test_job_result_cursor_flag_accepted tests/test_job_improvements.py::test_job_result_default_max_rows_is_100 tests/test_job_improvements.py::test_job_result_first_page tests/test_job_improvements.py::test_job_result_second_page tests/test_job_improvements.py::test_job_result_beyond_end_returns_empty tests/test_job_improvements.py::test_job_result_session_id_in_cursor_is_ignored_for_local_jobs -v
```

Expected: FAIL (no `--max-rows` / `--cursor` flags, and `job_result` ignores pagination).

- [ ] **Step 3: Add flags to CLI parser**

In `build_parser()`, find the `job_result` block (lines 102–105):

```python
    job_result = job_subparsers.add_parser("result", help="Fetch job results")
    job_result.add_argument("job_id")
    job_result.add_argument("--json", action="store_true")
    job_result.set_defaults(handler=_handle_job_result)
```

Change to:

```python
    job_result = job_subparsers.add_parser("result", help="Fetch job results")
    job_result.add_argument("job_id")
    job_result.add_argument("--json", action="store_true")
    job_result.add_argument("--max-rows", type=int, default=100, dest="max_rows", help="Maximum rows to return (default: 100)")
    job_result.add_argument("--cursor", default=None, help="Pagination cursor from previous response")
    job_result.set_defaults(handler=_handle_job_result)
```

- [ ] **Step 4: Pass args through handler**

In `_handle_job_result` (lines 502–504):

```python
def _handle_job_result(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.job_result(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
```

Change to:

```python
def _handle_job_result(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.job_result(args.job_id, max_rows=args.max_rows, cursor=args.cursor)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
```

- [ ] **Step 5: Implement pagination in `app.job_result`**

Current signature at line 467:

```python
    def job_result(self, job_id: 'str') -> 'Envelope':
```

Change to:

```python
    def job_result(self, job_id: 'str', *, max_rows: 'int' = 100, cursor: 'str | None' = None) -> 'Envelope':
```

**Remote path** — currently (lines 474–478):

```python
            result = self.backend.fetch_job_result(
                job_id,
                project=self.config.default_project,
                max_rows=100,
            )
```

Change to:

```python
            offset, _ = decode_cursor(cursor)
            result = self.backend.fetch_job_result(
                job_id,
                project=self.config.default_project,
                max_rows=max_rows,
                offset=offset,
            )
```

(Note: `decode_cursor` is already imported in `app.py` — it's used in `execute_query`.)

**Local path** — currently (lines 503–526) returns `stored["data"]` as-is. Replace with paginated slice:

```python
        stored = job["result"]
        info = self._local_job_info(job)
        all_rows = stored["data"].get("rows", [])
        schema = stored["data"].get("schema", [])
        total_rows = stored["data"].get("total_rows", len(all_rows))

        offset, _ = decode_cursor(cursor)  # session_id ignored for local jobs
        page_rows = all_rows[offset:offset + max_rows]
        returned_rows = len(page_rows)
        has_more = (offset + returned_rows) < total_rows
        next_cursor = encode_cursor(offset + returned_rows) if has_more else None

        envelope = Envelope(
            command="job.result",
            status="success",
            data={
                "rows": page_rows,
                "schema": schema,
                "total_rows": total_rows,
                "returned_rows": returned_rows,
                "has_more": has_more,
                "next_cursor": next_cursor,
            },
            metadata={
                **stored["metadata"],
                "job_id": job_id,
                "submitted_at": job["submitted_at"],
                "completed_at": job.get("completed_at", job["updated_at"]),
                "stage": info.stage,
                "retryable": info.retryable,
                "failure_reason": info.failure_reason,
                "logview": info.logview,
                "task_summary": info.task_summary,
            },
            agent_hints=AgentHints(
                next_actions=["meta.describe"],
                warnings=stored.get("agent_hints", {}).get("warnings", []),
            ),
        )
        self.log("job.result", envelope.status, envelope.metadata)
        return envelope
```

Ensure `encode_cursor` is imported in `app.py` — check by grepping:

```bash
grep "encode_cursor" src/maxc_cli/app.py | head -3
```

If not imported, add `from .utils import encode_cursor` alongside the existing `from .utils import decode_cursor` import.

- [ ] **Step 6: Run tests**

```bash
pytest tests/test_job_improvements.py::test_job_result_max_rows_flag_accepted tests/test_job_improvements.py::test_job_result_cursor_flag_accepted tests/test_job_improvements.py::test_job_result_default_max_rows_is_100 tests/test_job_improvements.py::test_job_result_first_page tests/test_job_improvements.py::test_job_result_second_page tests/test_job_improvements.py::test_job_result_beyond_end_returns_empty tests/test_job_improvements.py::test_job_result_session_id_in_cursor_is_ignored_for_local_jobs -v
```

Expected: all 7 PASS.

- [ ] **Step 7: Run full test suite**

```bash
pytest --ignore=tests/test_integration_real.py -x -q
```

Expected: all pass. Confirm zero regressions.

- [ ] **Step 8: Commit**

```bash
git add src/maxc_cli/cli.py src/maxc_cli/app.py tests/test_job_improvements.py
git commit -m "feat: add --max-rows and --cursor pagination to job result"
```

---

## Final Verification

- [ ] Run the full suite one final time:

```bash
pytest --ignore=tests/test_integration_real.py -v
```

- [ ] Verify help text shows new flags:

```bash
python -m maxc_cli job result --help
python -m maxc_cli job wait --help
python -m maxc_cli job list --help
```
