# MCQA Support Design

**Date:** 2026-06-23
**Status:** Approved

## Problem

`maxc-cli` currently executes all SQL through offline MaxCompute APIs. PyODPS 0.12.6 already exposes MCQA entry points (`execute_sql_interactive`, `run_sql_interactive`, `use_mcqa_v2`, `quota_name`), but the CLI has no way to request them, no config model for defaults, and no metadata telling agents whether a query actually ran through MCQA or fell back to offline mode.

## Goals

- Support both MCQA v1 and MCQA v2.
- Allow per-command flags plus config defaults.
- Make `query` fallback-capable by default.
- Keep `job submit` strict and async-safe.
- Expose stable envelope metadata so agents can tell requested mode from actual execution mode.

## Non-Goals

- No new top-level `mcqa` command group.
- No session management UX beyond what PyODPS already provides.
- No automatic quota discovery.

## CLI Surface

Add these flags to `query` and `job submit`:

- `--mcqa`
- `--no-mcqa`
- `--mcqa-version {v1,v2}`
- `--quota <name>`
- `--mcqa-fallback`
- `--no-mcqa-fallback`

Rules:

- Flags override config defaults.
- `--no-mcqa` conflicts with the other MCQA flags.
- `--quota` is only valid with `v2`.
- `query` defaults to fallback-enabled when MCQA is active.
- `job submit` rejects fallback because async submission must return a stable instance id immediately.

## Config Model

Add an `mcqa` block to `MaxCConfig`:

```yaml
mcqa:
  enabled: false
  version: v2
  quota_name: null
  fallback: true
```

`MaxCConfig` should expose a structured dataclass for this block, and CLI flags should resolve against it.

## Execution Model

Introduce a resolved execution settings object in the query path with:

- `enabled`
- `version`
- `fallback`
- `quota_name`
- `requested_mode` (`offline`, `mcqa_v1`, `mcqa_v2`)

### `query`

- Offline: keep `run_sql(...)` + `wait_for_success(...)`.
- MCQA v1: `execute_sql_interactive(..., fallback=<bool>)`
- MCQA v2: `execute_sql_interactive(..., use_mcqa_v2=True, quota_name=<name>, fallback=<bool>)`

`query` waits for the final instance and returns rows. If fallback occurs, the envelope still succeeds, but metadata records both requested and actual mode.

### `job submit`

- Offline: keep `run_sql(...)`
- MCQA v1/v2: use `run_sql_interactive(...)`
- Reject fallback with `ValidationError` because fallbackable interactive submission can stop being a pure async submission.

## Envelope Metadata

Add stable metadata fields on query/job responses:

- `execution_requested`: `offline | mcqa_v1 | mcqa_v2`
- `execution_mode`: `offline | mcqa_v1 | mcqa_v2`
- `mcqa_fallback_enabled`: `bool`
- `mcqa_fallback_used`: `bool`
- `mcqa_quota_name`: `str | null`

Warnings:

- MCQA requested but fallback used.
- MCQA v2 missing quota.
- `job submit` requested with fallback.
- `--quota` used with `v1`.

## File Changes

- `src/maxc_cli/config.py`: add MCQA config dataclass + parsing.
- `src/maxc_cli/cli.py`: add MCQA flags to `query` and `job submit`.
- `src/maxc_cli/app.py`: plumb resolved settings from CLI/config to backend calls.
- `src/maxc_cli/backend/query.py`: add MCQA-aware execution helpers and metadata.
- `tests/test_cli_mock.py`: CLI/config behavior coverage.
- `tests/test_backend_query.py` or `tests/test_cli_mock.py`: backend execution path coverage.

## Testing Strategy

Use TDD for each behavior:

1. Parser and config resolution tests.
2. Query MCQA v1/v2 execution tests.
3. Query fallback metadata tests.
4. Job submit strict-mode tests.
5. Regression run for the full suite.

## Success Criteria

- `query` can execute through MCQA v1 or v2.
- `query` auto-falls back to offline by default and records it.
- `job submit` can submit MCQA jobs but rejects fallback.
- Config defaults work and flags override them.
- All tests pass.
