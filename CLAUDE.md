# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`maxc-cli` is a MaxCompute CLI tool layer. It is NOT an agent itself, but a structured CLI for external AI agents (Claude Code, Codex, Cursor, or custom agents) to call. All commands output structured JSON with consistent envelope format.

## Development Commands

```bash
# Install in development mode
python -m pip install -e .

# Run tests
pytest

# Run a single test
pytest tests/test_cli_mock.py::test_auth_login_can_create_new_explicit_config_without_validation -v

# Run with real MaxCompute backend
pytest tests/test_integration_real.py -v
```

## Key Architecture

### Core Flow
```
cli.py (argparse) → app.py (MaxCApp) → backend/ (OdpsBackend) → models.py (Envelope output)
```

### Backend Module Structure
```
src/maxc_cli/backend/
├── __init__.py      # Exports: OdpsBackend, ODPS_ENV_ALIASES
├── odps.py          # OdpsBackend (combines all mixins)
├── query.py         # QueryMixin: execute_query, estimate_query_cost, explain_query, submit_query
├── job.py           # JobMixin: get_job, wait_job, fetch_job_result, cancel_job, diagnose_job, list_jobs
├── meta.py          # MetaMixin: list_tables, describe_table, search_tables, search_columns, etc.
├── data.py          # DataMixin: sample_table, profile_table
└── auth.py          # AuthMixin: whoami_info, can_i_info
```

### Key Files

| File | Purpose |
|------|---------|
| `src/maxc_cli/cli.py` | CLI argument parsing with argparse, command routing |
| `src/maxc_cli/app.py` | `MaxCApp` class - main application logic, orchestrates backend calls |
| `src/maxc_cli/backend/` | Backend module with mixin-based OdpsBackend |
| `src/maxc_cli/models.py` | Data models: `Envelope`, `AgentHints`, `QueryResult`, `JobInfo` |
| `src/maxc_cli/config.py` | Configuration loading, `MaxCConfig`, `AuthConfig` dataclasses |
| `src/maxc_cli/helpers.py` | Utility functions for ODPS operations |
| `src/maxc_cli/cache.py` | SQLite-based local cache for metadata and semantic data |
| `src/maxc_cli/store.py` | `JobStore` for local job tracking |
| `src/maxc_cli/exceptions.py` | Custom exceptions with error codes and suggestions |

### Output Envelope Format
All commands return this JSON structure:
```json
{
  "version": "1.0",
  "command": "query",
  "status": "success",
  "data": { ... },
  "metadata": { "elapsed_ms": 123, "project": "...", ... },
  "error": null,
  "agent_hints": { "next_actions": [...], "warnings": [...], "insights": [...] }
}
```

### Backend Selection
- `backend.type=auto`: Uses ODPS if connection info is complete, otherwise raises error
- `backend.type=odps|maxcompute`: Forces real MaxCompute backend

### Configuration Hierarchy

Priority from highest to lowest:

1. Environment variables (only `MAXCOMPUTE_PROJECT`/`ODPS_PROJECT` for `default_project`,
   `MAXCOMPUTE_REGION`/`ALIBABA_CLOUD_REGION` for `default_region`; suppressed for project
   when `auth.provider` is explicitly set, to prevent silent re-routing).
2. Config files, with later items in this list overriding earlier ones (deep-merged):
   - `~/.maxc/config.yaml` (global; this is what `maxc auth login` and `maxc session set` write to)
   - `cwd/.maxc/config.yaml` (project-level)
   - `cwd/.maxc.yaml`
   - `cwd/.maxc`
3. `auth.project` / `auth.region_name` (saved in the `auth` section of the config file)
4. Built-in defaults (`"demo_project"`, `"local"`, etc.)

Use `--config <path>` to bypass the config-file discovery chain entirely and load a single
explicit file.

`maxc session set --project X` writes `default_project: X` to `~/.maxc/config.yaml`. If a
project-level config file in the cwd also sets `default_project`, it will continue to
shadow the user-level value — `session set` warns when this is the case.

### ODPS Environment Variables
Primary: `ALIBABA_CLOUD_ACCESS_KEY_ID`, `ALIBABA_CLOUD_ACCESS_KEY_SECRET`, `MAXCOMPUTE_PROJECT`, `MAXCOMPUTE_ENDPOINT`, `MAXCOMPUTE_REGION`

Aliases supported: `ODPS_ACCESS_ID`, `ODPS_ACCESS_KEY`, `ACCESS_KEY_ID`, `ODPS_PROJECT`, etc.

## Testing

- Tests use `FakeODPS` class to mock the ODPS client for auth tests
- Integration tests with real MaxCompute are in `tests/test_integration_real.py`
- Tests that require real backend are skipped by default

## Important Patterns

### Adding a New Backend Method
1. Add abstract method to `backend/base.py` in `BaseBackend`
2. Implement in appropriate mixin file (`query.py`, `job.py`, `meta.py`, `data.py`, `auth.py`)
3. Add method in `app.py:MaxCApp` to call the backend
4. Add CLI handler in `cli.py`
5. Add tests in `tests/test_cli_mock.py`

### Adding a New Command
1. Add subparser in `cli.py:build_parser()`
2. Create handler function `_handle_*` in `cli.py`
3. Add method in `app.py:MaxCApp`
4. Add backend method if needed in appropriate mixin
5. Add tests in `tests/test_cli_mock.py`

### Error Handling
Use custom exceptions from `exceptions.py`:
- `ValidationError`: Input validation errors
- `NotFoundError`: Resource not found
- `PermissionDeniedError`: Authorization failures
- `SqlError`: SQL execution errors
- `BackendConnectionError`: Connection issues

All exceptions include `error_code`, `message`, and optional `suggestion`.

### Backend Protocol
The `BaseBackend` class in `backend/base.py` defines the interface:
- `execute_query()`, `estimate_query_cost()`, `explain_query()`
- `get_job()`, `wait_job()`, `fetch_job_result()`, `cancel_job()`, `diagnose_job()`
- `list_tables()`, `describe_table()`, `search_tables()`, `search_columns()`
- `sample_table()`, `profile_table()`
- `whoami_info()`, `can_i_info()`

## Known Limitations

- `meta.lineage` on real backend returns `supported=false` placeholder
- `--cursor` is CLI-side offset token, not server-side cursor
- `diff.data` is keyed snapshot compare, not exhaustive diff
- `auth.login` stores AccessKey in plaintext YAML (file permissions set to 0600).
  When `--project` is omitted, it pops an interactive project picker via the
  Catalog API (TTY required, China-region projects only). Use `--no-picker`
  for CI, `--reselect` to re-pick after a prior login, or `--catalog-endpoint`
  to override the catalog URL for non-China users.
