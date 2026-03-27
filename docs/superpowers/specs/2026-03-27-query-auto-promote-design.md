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

**Remove:**
- `--async` flag (replaced by `--wait 0`)
- `--timeout` flag (subsumed by `--wait`)

**Add:**
- `--wait` (type: int, default: 10) — seconds to poll before returning `job_id`. `--wait 0` submits without polling (equivalent to removed `--async`).

All other flags (`--dry-run`, `--cost-check`, `--max-rows`, `--cursor`, `--format`, `--mode`, `--idempotency-key`, `--retry-on`, `--max-retries`, `--retry-backoff`) remain unchanged.

## Execution Flow (remote backend)

```
app.execute_query(sql, *, wait=10, max_rows, offset, ...)
  │
  ├─ dry_run=True → unchanged (estimate cost, return immediately)
  │
  └─ dry_run=False, remote_jobs=True:
       1. _submit_remote_job()          # non-blocking, returns JobInfo with job_id
       2. backend.wait_job(
            job_id,
            timeout=wait,
            poll_interval=1             # 1s interval for responsiveness in short waits
          )
       3a. success  → fetch_job_result(max_rows, offset) → success envelope
       3b. JobTimeoutError → pending envelope with job_id
       3c. job.status == "failure"  → failure envelope
```

**Local/mock backend:** Unchanged. Local jobs complete instantly, so auto-promote is a no-op.

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

`wait_seconds` is added to metadata so agents know how long was already waited.

## Implementation Scope

**`src/maxc_cli/cli.py`:**
- Remove `--async` argument from query subparser
- Remove `--timeout` argument from query subparser
- Add `--wait` (type=int, default=10)
- Update `_handle_query` to pass `wait=args.wait` (remove `async_mode`, `timeout` args)

**`src/maxc_cli/app.py`:**
- Remove `async_mode` and `timeout` parameters from `execute_query()` and `query()` public method
- Replace the remote sync path (`_execute_query()` call) with: submit → wait_job(timeout=wait, poll_interval=1) → fetch or promote
- The `if async_mode and self.remote_jobs:` branch and `if async_mode:` local branch are deleted
- The `_submit_remote_job()` helper stays and is now always used for remote jobs

**`src/maxc_cli/backend/query.py`:**
- `execute_query()` is no longer called from app for remote queries. It can be retained for now (may still be used in integration tests or future paths) but is not a required change.
- `submit_query()` is unchanged.

## Non-Goals

- No changes to `job wait`, `job result`, `job list`, `job status` commands.
- No changes to `--dry-run`, `--cost-check`, or retry logic.
- No changes to local/mock backend behavior.
- `backend/query.py`'s `execute_query()` is not deleted (may still have callers in tests).
- No server-side streaming or progress reporting during the `--wait` window.

## Testing

- `test_query_wait_flag_accepted`: `--wait 30` parses correctly; `--wait 0` parses correctly
- `test_query_no_longer_has_async_flag`: `--async` raises argparse error
- `test_query_no_longer_has_timeout_flag`: `--timeout` raises argparse error
- `test_query_auto_promotes_on_timeout`: mock `wait_job` raises `JobTimeoutError` → envelope has `status=pending`, `data.job_id` set
- `test_query_returns_success_when_job_finishes_within_wait`: mock `wait_job` succeeds → envelope has `status=success`, rows present
- `test_query_returns_failure_when_job_fails`: mock `wait_job` returns failure status → envelope has `status=failure`
- `test_query_wait_default_is_10`: `build_parser()` → `args.wait == 10`
- `test_query_wait_0_submits_and_returns_pending`: `--wait 0` with mock backend → immediate pending envelope
