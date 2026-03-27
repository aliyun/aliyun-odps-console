# Job Command Improvements Design

**Date:** 2026-03-27
**Status:** Approved

## Problem

Four gaps in long-running / large-result job support:

1. `job result` is locked to 100 rows with no pagination — unusable for large async query results.
2. `job wait` has no `--timeout` flag — jobs longer than 300s always fail.
3. `job list` hardcodes limit=20 — not configurable.
4. Polling silently swallows all `reload()` errors — agents cannot detect network failures.

## Changes

### 1. `job result` Pagination

**CLI** (`cli.py`): Add `--max-rows` (int, default 100) and `--cursor` to the `job result` subparser.

**App** (`app.py`): Change `job_result(job_id)` → `job_result(job_id, *, max_rows=100, cursor=None)`.

- Decode cursor → `(offset, session_id)` using existing `decode_cursor`.
- Remote jobs: pass `max_rows` and `offset` to `backend.fetch_job_result()`.
- Local jobs: slice stored rows from `offset` to `offset + max_rows`; generate `next_cursor` with `encode_cursor(next_offset)` when `has_more`.
- Handler passes `args.max_rows` and `args.cursor` through.

Cursor format is identical to `query` — reuses `encode_cursor` / `decode_cursor`.

### 2. `job wait` Timeout

**CLI** (`cli.py`): Add `--timeout` (int) to the `job wait` subparser.

**App** (`app.py`): Change `job_wait(job_id)` → `job_wait(job_id, *, timeout=None)`.

- Pass `timeout` to `backend.wait_job(timeout=timeout)`. Backend already accepts this parameter and defaults to 300s when `None`.
- Handler passes `args.timeout` (may be `None` if not supplied).

### 3. `job list` Limit

**CLI** (`cli.py`): Add `--limit` (int, default 20) to the `job list` subparser.

**App** (`app.py`): Change `list_jobs()` → `list_jobs(*, limit=20)`.

- Remote path: pass `limit` to `backend.list_jobs(limit=limit)`.
- Local path: slice the jobs list to `limit` entries.
- Handler passes `args.limit`.

### 4. Polling Error Handling

**Backend** (`backend/job.py`, `wait_job`):

- Add `consecutive_errors = 0` counter before the poll loop.
- On successful `reload()`: reset `consecutive_errors = 0`.
- On `reload()` exception: increment counter; if `consecutive_errors >= 5`, raise `BackendConnectionError` with the last exception message.
- `BackendConnectionError` is already defined in `exceptions.py`.

## Interface Summary

```
maxc job result <job_id> [--max-rows N] [--cursor TOKEN] [--json]
maxc job wait   <job_id> [--timeout N] [--stream] [--json]
maxc job list            [--limit N]   [--json]
```

## Non-Goals

- No server-side cursor support (ODPS does not expose one for instance results).
- No streaming row output for `job result` (row-level streaming requires ODPS reader internals change, separate concern).
- No changes to `job submit` or `job status`.

## Testing

- Extend `tests/test_cli_mock.py`:
  - `job result` with `--max-rows` and `--cursor` (first page, second page, beyond end).
  - `job wait` with `--timeout`.
  - `job list` with `--limit`.
  - Polling: mock `reload()` to fail 4× then succeed (should continue); fail 5× (should raise).
