# Query Auto-Promote Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the binary `--async`/sync choice in `query` with a single adaptive mode that submits a job, polls for `--wait` seconds (default 10), and returns either rows or a `job_id`.

**Architecture:** `query` always submits via `_submit_remote_job()` (non-blocking), then polls with `backend.wait_job()` up to `--wait` seconds. On success it fetches rows; on timeout it promotes to a pending envelope. `BackendConnectionError` and fetch failures both return error envelopes that preserve `job_id` in metadata so the agent can recover. Local backend jobs return `status: success` immediately (no async simulation).

**Tech Stack:** Python, argparse, pytest. No new libraries.

---

## Files Modified

| File | Change |
|------|--------|
| `src/maxc_cli/cli.py` | Remove `--async`/`--timeout` from query subparser, add `--wait`, update `_handle_query` and `_validate_query_analysis_args` |
| `src/maxc_cli/app.py` | Replace `async_mode` with `wait` in `query()`, rewrite remote branch, rewrite `submit_job()` |
| `tests/test_query_auto_promote.py` | New test file — all 12 tests from the spec |

---

## Codebase Orientation

Before starting, note these key locations:

- **`cli.py` lines 62–63**: `--timeout` and `--async` on the query subparser — to be removed.
- **`cli.py` line 452**: `async_mode=args.async_mode` passed to `app.query()` — becomes `wait=args.wait`.
- **`cli.py` lines 1203–1221**: `_validate_query_analysis_args()` — contains `if args.async_mode: unsupported.append("--async")` to remove.
- **`app.py` lines 190–310**: `query()` method — the main change target. Lines 205–210 are the guards, 236–261 is the remote async branch, 275–298 is the local async branch.
- **`app.py` line 61**: `self.remote_jobs = ...` — plain instance attribute (not a property), set in `__init__`. Tests override with `app.remote_jobs = True/False` directly.
- **`app.py` line 28–35**: Exception imports — add `BackendConnectionError, JobTimeoutError`.
- **`backend/job.py` line 28**: `wait_job()` — returns `JobInfo` with `status="success"` or `status="failure"` when job ends; raises `JobTimeoutError` or `BackendConnectionError`.

---

## Task 1: CLI Parser Changes

**Files:**
- Modify: `src/maxc_cli/cli.py:50-70` (query subparser args)
- Modify: `src/maxc_cli/cli.py:1203-1221` (`_validate_query_analysis_args`)
- Modify: `src/maxc_cli/cli.py:444-457` (`_handle_query`)
- Test: `tests/test_query_auto_promote.py` (new file)

- [ ] **Step 1: Write failing tests for CLI parser changes**

Create `tests/test_query_auto_promote.py`:

```python
"""Tests for query auto-promote feature (--wait flag, removal of --async/--timeout)."""
import pytest
from maxc_cli.cli import build_parser


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
    with pytest.raises(SystemExit):
        parser.parse_args(["query", "--async", "SELECT 1"])


def test_query_no_longer_has_timeout_flag():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["query", "--timeout", "60", "SELECT 1"])
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_query_auto_promote.py -v
```

Expected: 4 FAILs — `args` has no `wait`, still has `--async`/`--timeout`.

- [ ] **Step 3: Remove `--async` and `--timeout` from query subparser; add `--wait`**

In `src/maxc_cli/cli.py`, find the query subparser block (lines ~62–63). Replace:

```python
    query_parser.add_argument("--timeout", type=int)
    query_parser.add_argument("--async", dest="async_mode", action="store_true")
```

With:

```python
    query_parser.add_argument("--wait", type=int, default=10,
                              help="Seconds to poll before promoting to async (default: 10). --wait 0 returns job_id immediately.")
```

- [ ] **Step 4: Update `_handle_query` to pass `wait=args.wait`**

In `_handle_query` (line ~452), replace:
```python
            async_mode=args.async_mode,
```
With:
```python
            wait=args.wait,
```

Also remove the `timeout` key from `app.query()` call if it was ever passed (it wasn't — the old `--timeout` was dead at CLI level, never forwarded to `app.query()`).

- [ ] **Step 5: Remove obsolete `async_mode` guard from `_validate_query_analysis_args`**

In `_validate_query_analysis_args` (line ~1203-1221), remove:
```python
    if args.async_mode:
        unsupported.append("--async")
```

This guard is safe to remove: `cache.build --async` uses a separate subparser namespace and `_validate_query_analysis_args` is never called from `_handle_cache_build`.

- [ ] **Step 6: Run tests to verify they pass**

```bash
pytest tests/test_query_auto_promote.py -v
```

Expected: 4 PASSes.

- [ ] **Step 7: Run full test suite to catch regressions**

```bash
pytest tests/ -v --ignore=tests/test_integration_real.py
```

Expected: all pass. If `test_integration.py` or `test_agent_hints_and_cli.py` reference `async_mode` on query args, fix them now.

- [ ] **Step 8: Commit**

```bash
git add src/maxc_cli/cli.py tests/test_query_auto_promote.py
git commit -m "feat: replace query --async/--timeout with --wait flag"
```

---

## Task 2: Rewrite `app.query()` Remote Branch

This is the core logic change. The remote branch currently short-circuits to a pending envelope when `async_mode=True`. After this task, it always submits, then polls up to `wait` seconds.

**Files:**
- Modify: `src/maxc_cli/app.py:28-35` (imports)
- Modify: `src/maxc_cli/app.py:190-310` (`query()` method)
- Test: `tests/test_query_auto_promote.py`

### What the new `query()` remote branch should look like

```python
def query(
    self,
    *,
    command: str,
    sql: str,
    project: str | None = None,
    max_rows: int = 100,
    cursor: str | None = None,
    dry_run: bool = False,
    wait: int = 10,                   # NEW — replaces async_mode
    cost_check: float | None = None,
    idempotency_key: str | None = None,
    retry_on: list[str] | None = None,
    max_retries: int = 0,
) -> Envelope:
    # Guards (drop async_mode guards; cursor+dry_run guard stays)
    if max_rows <= 0:
        raise ValidationError("`--max-rows` and `--page-size` must be greater than 0.")
    if cursor and dry_run:
        raise ValidationError("Do not combine `--cursor` with `--dry-run`.")

    target_project = project or self.config.default_project
    offset, session_id = decode_cursor(cursor)

    # Session cursor path — unchanged
    if session_id is not None and self.remote_jobs:
        ...  # unchanged

    # NEW remote branch — always submit, then poll
    if self.remote_jobs and not dry_run:
        job = self._submit_remote_job(
            sql=sql,
            project=target_project,
            cost_check=cost_check,
            idempotency_key=idempotency_key,
        )
        if wait == 0:
            # Return pending envelope immediately, no polling
            envelope = Envelope(
                command=command,
                status="pending",
                data={"job_id": job.job_id},
                metadata={
                    "job_id": job.job_id,
                    "project": job.project,
                    "submitted_at": job.submitted_at,
                    "logview": job.logview,
                    "wait_seconds": 0,
                    "sql_executed": sql,
                    "idempotency_key": idempotency_key,
                },
                agent_hints=AgentHints(
                    next_actions=["job.wait", "job.status"],
                    warnings=job.warnings,
                    insights=["Use `job wait <job_id>` to wait for completion, then `job result <job_id>` to fetch rows."],
                ),
            )
            self.log(command, envelope.status, envelope.metadata)
            return envelope
        # Poll
        try:
            job_info = self.backend.wait_job(
                job.job_id,
                project=target_project,
                timeout=wait,
                poll_interval=1,
            )
        except JobTimeoutError:
            envelope = Envelope(
                command=command,
                status="pending",
                data={"job_id": job.job_id},
                metadata={
                    "job_id": job.job_id,
                    "project": job.project,
                    "submitted_at": job.submitted_at,
                    "logview": job.logview,
                    "wait_seconds": wait,
                    "sql_executed": sql,
                    "idempotency_key": idempotency_key,
                },
                agent_hints=AgentHints(
                    next_actions=["job.wait", "job.status"],
                    warnings=job.warnings,
                    insights=[
                        f"Query promoted to async after {wait}s. "
                        "Use `job wait <job_id> --timeout <N>` to wait for completion, "
                        "then `job result <job_id>` to fetch rows."
                    ],
                ),
            )
            self.log(command, envelope.status, envelope.metadata)
            return envelope
        except BackendConnectionError as exc:
            envelope = Envelope(
                command=command,
                status="error",
                data=None,
                error=exc.to_payload(),
                metadata={
                    "job_id": job.job_id,
                    "project": target_project,
                },
                agent_hints=AgentHints(
                    next_actions=["job.status"],
                ),
            )
            self.log(command, envelope.status, envelope.metadata)
            return envelope
        # Job ended — check outcome
        if job_info.status == "failure":
            envelope = Envelope(
                command=command,
                status="failure",
                data={"job_id": job_info.job_id},
                metadata={
                    "job_id": job_info.job_id,
                    "project": job_info.project,
                    "submitted_at": job_info.submitted_at,
                    "logview": job_info.logview,
                    "sql_executed": sql,
                },
                agent_hints=AgentHints(
                    next_actions=["job.diagnose", "job.status"],
                    warnings=job_info.warnings,
                ),
            )
            self.log(command, envelope.status, envelope.metadata)
            return envelope
        # status == "success" — fetch rows
        try:
            result = self.backend.fetch_job_result(
                job_info.job_id,
                project=target_project,
                max_rows=max_rows,
                offset=offset,
            )
        except Exception as exc:
            fetch_err = MaxCError(str(exc))
            envelope = Envelope(
                command=command,
                status="error",
                data=None,
                error=fetch_err.to_payload(),
                metadata={
                    "job_id": job_info.job_id,
                    "project": target_project,
                },
                agent_hints=AgentHints(
                    next_actions=["job.result"],
                ),
            )
            self.log(command, envelope.status, envelope.metadata)
            return envelope
        envelope = self._build_query_envelope(
            command=command,
            result=result,
            dry_run=False,
        )
        envelope.metadata.update({
            "job_id": job_info.job_id,
            "submitted_at": job_info.submitted_at,
            "logview": job_info.logview,
        })
        self.log(command, envelope.status, envelope.metadata)
        return envelope

    # Local / dry_run path — unchanged (calls _execute_query, no async_mode branch)
    result = self._execute_query(...)
    envelope = self._build_query_envelope(...)
    ...
```

- [ ] **Step 1: Write failing tests for the remote branch logic**

Add to `tests/test_query_auto_promote.py`:

```python
from unittest.mock import MagicMock, patch, call
from pathlib import Path
import pytest
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
        submitted_at="2026-01-01T00:00:00Z",
        logview="http://logview/job-1",
    )


def _fake_query_result() -> QueryResult:
    return QueryResult(
        rows=[{"x": 1}],
        schema=[{"name": "x", "type": "bigint"}],
        total_rows=1,
        has_more=False,
        project="test_proj",
        elapsed_ms=100,
    )


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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_query_auto_promote.py::test_query_returns_success_when_job_finishes_within_wait tests/test_query_auto_promote.py::test_query_auto_promotes_on_timeout tests/test_query_auto_promote.py::test_query_returns_failure_when_job_fails tests/test_query_auto_promote.py::test_query_wait_0_submits_and_returns_pending tests/test_query_auto_promote.py::test_query_backend_connection_error_includes_job_id tests/test_query_auto_promote.py::test_query_fetch_failure_after_success_includes_job_id tests/test_query_auto_promote.py::test_query_wait_job_called_with_poll_interval_1 -v
```

Expected: all FAIL (TypeError: unexpected keyword argument `wait`).

- [ ] **Step 3: Add `BackendConnectionError, JobTimeoutError` to imports in `app.py`**

In `src/maxc_cli/app.py`, update the imports block (lines ~28–35) to add the two new exception classes:

```python
from .exceptions import (
    BackendConnectionError,
    CostLimitExceededError,
    ErrorPayload,
    FeatureUnavailableError,
    JobTimeoutError,
    MaxCError,
    PermissionDeniedError,
    ValidationError,
)
```

- [ ] **Step 4: Rewrite `query()` in `app.py`**

Replace the entire `query()` method (lines ~190–310) with the new implementation shown above in the task description. Key changes:
- Signature: replace `async_mode: bool = False` with `wait: int = 10`
- Remove validation guards: `dry_run and async_mode`, `cursor and async_mode`
- Keep: `max_rows <= 0` guard, `cursor and dry_run` guard
- Keep: session_id cursor path (lines ~217–234) — unchanged
- Replace the two async branches (lines ~236–298) with the new submit→wait→branch logic
- Keep: the local/dry_run path at the bottom (lines ~263–310) — remove only the `if async_mode:` block inside it, leaving `result = self._execute_query(...)` and `envelope = self._build_query_envelope(...)` intact

The local path (non-remote, non-dry_run) now never enters the async branch — it always calls `_execute_query()` and builds a success envelope directly.

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_query_auto_promote.py -v
```

Expected: all 11 tests pass (4 from Task 1 + 7 new).

- [ ] **Step 6: Run full test suite**

```bash
pytest tests/ -v --ignore=tests/test_integration_real.py
```

Expected: all pass. Fix any failures.

- [ ] **Step 7: Commit**

```bash
git add src/maxc_cli/app.py tests/test_query_auto_promote.py
git commit -m "feat: rewrite query() remote branch with auto-promote logic"
```

---

## Task 3: Rewrite `submit_job()` and Fix Local Backend Test

**Files:**
- Modify: `src/maxc_cli/app.py:354-371` (`submit_job()`)
- Test: `tests/test_query_auto_promote.py`

`submit_job()` currently calls `self.query(..., async_mode=True)`. After `async_mode` is gone, it calls `self.query(..., wait=0)`. For remote backends, `wait=0` returns a pending envelope immediately (same as before). For local backends, `query()` now runs synchronously and returns a success envelope — this is an intentional behavior change.

- [ ] **Step 1: Write failing test for `submit_job()` local behavior**

Add to `tests/test_query_auto_promote.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_query_auto_promote.py::test_submit_job_local_backend_returns_success -v
```

Expected: FAIL (TypeError because `submit_job` still passes `async_mode=True`).

- [ ] **Step 3: Update `submit_job()` in `app.py`**

Replace (lines ~363–371):
```python
        return self.query(
            command="job.submit",
            sql=sql,
            project=project,
            max_rows=max_rows,
            async_mode=True,
            cost_check=cost_check,
            idempotency_key=idempotency_key,
        )
```

With:
```python
        return self.query(
            command="job.submit",
            sql=sql,
            project=project,
            max_rows=max_rows,
            wait=0,
            cost_check=cost_check,
            idempotency_key=idempotency_key,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_query_auto_promote.py -v
```

Expected: all 12 tests pass.

- [ ] **Step 5: Run full test suite**

```bash
pytest tests/ -v --ignore=tests/test_integration_real.py
```

Expected: all pass. If any test that previously asserted `job submit` returns `status: pending` for a local backend now fails, update that test to assert `status: success` — this is the intended behavior change.

- [ ] **Step 6: Commit**

```bash
git add src/maxc_cli/app.py tests/test_query_auto_promote.py
git commit -m "feat: rewrite submit_job() to use wait=0 instead of async_mode=True"
```

---

## Task 4: Final Verification

- [ ] **Step 1: Run the full test suite one final time**

```bash
pytest tests/ -v --ignore=tests/test_integration_real.py
```

Expected: all pass with 12 new tests in `test_query_auto_promote.py`.

- [ ] **Step 2: Verify no stray `async_mode` references remain in the query path**

```bash
grep -n "async_mode" src/maxc_cli/app.py src/maxc_cli/cli.py
```

Expected: no matches in the query-related code. (`cache_build` in `app.py` and `cli.py` uses `async_mode` in its own unrelated path — those matches are OK.)
