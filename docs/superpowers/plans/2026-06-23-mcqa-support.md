# MCQA Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add MCQA v1/v2 execution support to `query` and `job submit`, with config defaults, query fallback metadata, and strict async semantics for job submission.

**Architecture:** Resolve MCQA settings from CLI flags plus `MaxCConfig`, then thread a small execution-settings object into the query backend. `query` uses fallback-capable interactive execution while `job submit` uses strict interactive submission without fallback. Responses expose requested mode, actual mode, and fallback state in metadata.

**Tech Stack:** Python, argparse, PyODPS 0.12.6, pytest.

---

## Files Modified

| File | Change |
|------|--------|
| `src/maxc_cli/config.py` | Add MCQA config dataclass and parsing/serialization |
| `src/maxc_cli/cli.py` | Add MCQA flags to `query` and `job submit` |
| `src/maxc_cli/app.py` | Resolve MCQA settings from args/config and pass them to backend |
| `src/maxc_cli/backend/query.py` | Implement MCQA-aware query and submit execution helpers |
| `tests/test_cli_mock.py` | Add CLI/config behavior tests |

---

### Task 1: Add parser and config support

**Files:**
- Modify: `src/maxc_cli/config.py`
- Modify: `src/maxc_cli/cli.py`
- Test: `tests/test_cli_mock.py`

- [ ] Step 1: Write failing tests for query/job MCQA flags and config defaults.
- [ ] Step 2: Run targeted tests and verify they fail for missing args/config fields.
- [ ] Step 3: Add `McqaConfig` to `config.py` and wire parse/serialize defaults.
- [ ] Step 4: Add MCQA flags to `query` and `job submit` parsers.
- [ ] Step 5: Re-run targeted tests and verify they pass.

### Task 2: Add MCQA execution settings resolution

**Files:**
- Modify: `src/maxc_cli/app.py`
- Test: `tests/test_cli_mock.py`

- [ ] Step 1: Write failing tests for flag-over-config resolution and validation errors.
- [ ] Step 2: Run targeted tests and verify they fail for missing behavior.
- [ ] Step 3: Add a resolver that returns enabled/version/quota/fallback/requested_mode.
- [ ] Step 4: Reject invalid combinations (`--no-mcqa` conflicts, `v1+quota`, submit+fallback).
- [ ] Step 5: Re-run targeted tests and verify they pass.

### Task 3: Add MCQA query execution

**Files:**
- Modify: `src/maxc_cli/backend/query.py`
- Modify: `src/maxc_cli/app.py`
- Test: `tests/test_cli_mock.py`

- [ ] Step 1: Write failing tests for MCQA v1, MCQA v2, and fallback metadata on `query`.
- [ ] Step 2: Run targeted tests and verify they fail for missing backend calls/metadata.
- [ ] Step 3: Add MCQA-aware execution helpers using `execute_sql_interactive`.
- [ ] Step 4: Attach requested mode, actual mode, fallback enabled, fallback used, and quota name to metadata.
- [ ] Step 5: Re-run targeted tests and verify they pass.

### Task 4: Add strict MCQA job submission

**Files:**
- Modify: `src/maxc_cli/backend/query.py`
- Modify: `src/maxc_cli/app.py`
- Test: `tests/test_cli_mock.py`

- [ ] Step 1: Write failing tests for MCQA submit on v1/v2 and fallback rejection.
- [ ] Step 2: Run targeted tests and verify they fail.
- [ ] Step 3: Add strict `run_sql_interactive` submit path.
- [ ] Step 4: Re-run targeted tests and verify they pass.

### Task 5: Regression verification

**Files:**
- Test: `tests/test_cli_mock.py`
- Test: full suite

- [ ] Step 1: Run focused MCQA tests.
- [ ] Step 2: Run full `pytest` suite.
- [ ] Step 3: Investigate and fix regressions until green.
