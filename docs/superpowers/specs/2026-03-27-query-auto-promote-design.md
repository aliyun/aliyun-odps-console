# Query Auto-Promote Design

**Date:** 2026-03-27
**Status:** Approved

## Problem

`query` currently forces a binary choice between sync and async mode:

- **Sync (default):** Blocks until done or 300s timeout. On timeout the result is permanently lost — no `job_id` to resume with.
- **Async (`--async`):** Returns `job_id` immediately, requires three separate commands (`job wait`, `job result`).

Agents cannot predict whether a query will be short or long, so they have no safe default. Sync risks losing results; async adds unnecessary overhead for fast queries.

## Solution: Auto-Promote

Replace the binary choice with a single adaptive mode. `query` submits the job, waits up to `--wait` seconds, and returns either a result or a `job_id`:

- If the job finishes within `--wait` seconds → success envelope with rows (same as today's sync)
- If the job is still running after `--wait` seconds → pending envelope with `job_id` (same as today's `--async`)
- If the job fails → failure envelope

Agents look at `status` in the response and branch accordingly. No upfront mode selection required.

## CLI Changes

**Remove from the `query` subparser only:**
- `--async` flag (replaced by `--wait 0`)
- `--timeout` flag (subsumed by `--wait`; `job wait --timeout` at line ~95 of `cli.py` is unchanged)

**Add:**
- `--wait` (type: int, default: 10) — seconds to poll before returning `job_id`. `--wait 0` submits without polling (equivalent to removed `--async`).

All other flags (`--dry-run`, `--cost-check`, `--max-rows`, `--cursor`, `--format`, `--idempotency-key`, `--retry-on`, `--max-retries`, `--retry-backoff`) remain unchanged. (Note: `--mode` is not a flag; it is resolved from the `sql_parts` alias in `_parse_query_mode_and_sql()`.)

## Execution Flow (remote backend)

```
app.query(sql, *, wait=10, max_rows, cursor, ...)
  │
  ├─ dry_run=True → unchanged (estimate cost, return immediately)
  │
  └─ dry_run=False, remote_jobs=True:
       1. _submit_remote_job()          # non-blocking, returns JobInfo with job_id
       2. if wait == 0: → pending envelope immediately (no polling at all)
       3. else: backend.wait_job(
                  job_id,
                  project=target_project,
                  timeout=wait,
                  poll_interval=1       # 1s interval for responsiveness in short waits
                )
       4a. job_info.status == "success"
           → backend.fetch_job_result(job_id, project=target_project, max_rows=max_rows, offset=offset)
           → success envelope
           On fetch_job_result exception → error envelope (see Error Envelope Format)
             that includes job_id in metadata so agent can call `job result <job_id>` independently
       4b. JobTimeoutError raised → pending envelope with job_id
       4c. job_info.status == "failure" → failure envelope (see Failure Envelope Format)
           (wait_job returns a JobInfo with status="failure" when the underlying ODPS instance
            failed execution; it does NOT raise an exception in this case)
       4d. BackendConnectionError raised → error envelope (see Error Envelope Format)
             that includes job_id in metadata so agent can call `job status <job_id>` to check and resume
```

**Local/mock backend:** The `if async_mode:` local branch is removed. Local jobs always return
a success envelope immediately (local jobs complete instantly; no async simulation needed).
`submit_job()` with a local backend will therefore return `status: success` rather than `status:
pending`. This is an intentional behavior change: the local backend is for testing only and local
jobs are not truly async. Tests that previously asserted `status: pending` from `job submit` against
a local backend must be updated to assert `status: success`.

## Pending Envelope Format

Identical to the current `--async` response:

```json
{
  "version": "1.0",
  "command": "query",
  "status": "pending",
  "data": {
    "job_id": "<instance_id>"
  },
  "metadata": {
    "job_id": "<instance_id>",
    "project": "...",
    "submitted_at": "...",
    "logview": "...",
    "wait_seconds": 10,
    "sql_executed": "SELECT ..."
  },
  "agent_hints": {
    "next_actions": ["job.wait", "job.status"],
    "insights": [
      "Query promoted to async after 10s. Use `job wait <job_id> --timeout <N>` to wait for completion, then `job result <job_id>` to fetch rows."
    ]
  }
}
```

For the `BackendConnectionError` case (4d), `next_actions` is `["job.status"]` (not `job.wait`, since the connection was lost and status must be checked first before waiting).

`wait_seconds` is added to metadata so agents know how long was already waited. For `--wait 0`, `wait_seconds` is `0` (the configured value; no time was actually spent polling).

## Failure Envelope Format

The failure envelope follows the standard pattern used by the existing job failure path. The envelope-level `status` is `"failure"`:

```json
{
  "version": "1.0",
  "command": "query",
  "status": "failure",
  "data": {
    "job_id": "<instance_id>"
  },
  "metadata": {
    "job_id": "<instance_id>",
    "project": "...",
    "submitted_at": "...",
    "logview": "...",
    "sql_executed": "SELECT ..."
  },
  "agent_hints": {
    "next_actions": ["job.diagnose", "job.status"]
  }
}
```

## Error Envelope Format

Used for cases 4a (fetch failure) and 4d (BackendConnectionError). Standard error envelope with `job_id` added to `metadata`. `next_actions` differs by case:

- 4a (fetch failure): `["job.result"]` — agent can retry `job result <job_id>` independently
- 4d (BackendConnectionError): `["job.status"]` — agent must check status first before retrying

```json
{
  "version": "1.0",
  "command": "query",
  "status": "error",
  "data": null,
  "error": { "code": "...", "message": "..." },
  "metadata": {
    "job_id": "<instance_id>",
    "project": "..."
  },
  "agent_hints": {
    "next_actions": ["job.status"]
  }
}
```

## Implementation Scope

**`src/maxc_cli/cli.py`:**
- Remove `--async` argument from query subparser
- Remove `--timeout` argument from query subparser (both at lines ~62–63)
- Add `--wait` (type=int, default=10)
- Update `_handle_query` to pass `wait=args.wait` (remove `async_mode` arg)
- Update `_validate_query_analysis_args` (line ~1203): remove `if args.async_mode: unsupported.append("--async")` — this guard is obsolete once `--async` is removed from the query subparser. This removal is safe: `cache.build --async` uses the same `dest="async_mode"` name but is a completely separate subparser namespace; `_validate_query_analysis_args` is only ever called from `_handle_query`, never from `_handle_cache_build`.

**`src/maxc_cli/app.py`:**
- Remove `async_mode` parameter from `query()` (line ~190); add `wait: int = 10` in its place. Note: `query()` does not currently have a `timeout` parameter — `--timeout` was a CLI-only dead argument (parsed but never passed to `app.query()`). `_execute_query()` has its own `timeout` parameter which is live code and must NOT be removed.
- Remove the validation guards referencing `async_mode` (`dry_run+async_mode`, `cursor+async_mode`) and replace with equivalent guards using `wait` where needed (e.g. `dry_run` with any `wait` is fine; `cursor` with `wait=0` is not meaningful but not invalid — leave guard removal to implementer judgment)
- Replace the remote branch with: submit → (if wait>0: wait_job) → fetch/promote/error as described in Execution Flow
- The `if async_mode and self.remote_jobs:` branch and `if async_mode:` local branch are deleted
- The `_submit_remote_job()` helper stays and is now always used for remote jobs
- `submit_job()` (line ~354): rewrite to call `self.query(command="job.submit", ..., wait=0)` instead of `self.query(..., async_mode=True)`. This is the existing `query()` public method with `wait=0`; semantics are identical: `wait=0` short-circuits before polling and returns a pending envelope immediately for remote backends; returns success for local backends.

**`src/maxc_cli/backend/query.py`:**
- `execute_query()` is no longer called from app for remote queries. It can be retained for now (may still be used in integration tests or future paths) but is not a required change.
- `submit_query()` is unchanged.

## Non-Goals

- No changes to `job wait`, `job result`, `job list`, `job status` commands.
- No changes to `--dry-run`, `--cost-check`, or retry logic.
- `backend/query.py`'s `execute_query()` is not deleted (may still have callers in tests).
- No server-side streaming or progress reporting during the `--wait` window.

## Testing

- `test_query_wait_flag_accepted`: `--wait 30` parses correctly; `--wait 0` parses correctly
- `test_query_no_longer_has_async_flag`: `--async` raises argparse error
- `test_query_no_longer_has_timeout_flag`: `--timeout` raises argparse error
- `test_query_auto_promotes_on_timeout`: mock `wait_job` raises `JobTimeoutError` → envelope has `status=pending`, `data.job_id` set, `metadata.wait_seconds` set
- `test_query_returns_success_when_job_finishes_within_wait`: mock `wait_job` succeeds → envelope has `status=success`, rows present
- `test_query_returns_failure_when_job_fails`: mock `wait_job` returns `JobInfo(status="failure")` → envelope has `status=failure`
- `test_submit_job_local_backend_returns_success`: `submit_job()` against local backend → `status=success` (behavior change: was `status=pending` before this feature)
- `test_query_wait_job_called_with_poll_interval_1`: mock `wait_job` spy → assert called with `poll_interval=1`
- `test_query_wait_default_is_10`: `build_parser()` → `args.wait == 10`
- `test_query_wait_0_submits_and_returns_pending`: `--wait 0` with remote mock → pending envelope without any `wait_job` call
- `test_query_backend_connection_error_includes_job_id`: mock `wait_job` raises `BackendConnectionError` → error envelope includes `job_id` in metadata
- `test_query_fetch_failure_after_success_includes_job_id`: mock `wait_job` succeeds but `fetch_job_result` raises → error envelope includes `job_id` in metadata
