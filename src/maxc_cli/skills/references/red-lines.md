# Red Lines And Recovery

Single source of truth for "what not to do" and "how to recover when X breaks". Read this when you hit an error, before retrying anything.

## Critical Red Lines

Cross-referenced with SKILL.md §Core Principles (which lists the highest-priority subset). Full set below.

| # | Rule | Why |
|---|------|-----|
| 1 | Always use `--json` for machine-driven work | Plain output is for humans; agents must parse the envelope |
| 2 | Never invent table, schema, project, or endpoint names | Always verify with `meta search` / `meta list-tables` / `auth whoami` |
| 3 | Never install or upgrade Python without explicit user confirmation | System-level change with broad impact |
| 4 | Never re-prompt for credentials when `auth whoami` shows `authenticated=true` | Permission errors are almost always a workspace issue, not a credential issue — see SKILL.md §Dev vs Production Workspaces |
| 5 | Always check partition value via `meta latest-partition` before querying partitioned tables | Hardcoded partitions go stale; format varies per table |
| 6 | Always use schema-qualified (`schema.table`) or `project.table` names in SQL | Bare names fail across schemas/projects |
| 7 | Never log, echo, or include AK/SK in output | Even in error context |
| 8 | Read `error.suggestion` before retrying a failed command | Same input → same error |
| 9 | Never pick the auth method yourself during bootstrap | Always ask the user (AK/SK vs env vars). If `auth_type=external` already exists, leave the auth config alone |
| 10 | Trust runtime help and actual command output over stale snippets | The CLI evolves; cached knowledge can be wrong |
| 11 | Never use `--force` unless write operations are explicitly authorized | The SQL gate is read-only by design. (Note: `data upload` is a separate, intentional write path via Tunnel — it does not require `--force`, but still ask the user before overwriting a partition.) |
| 12 | Always check `agent_hints.warnings` even when `status=success` | Cache staleness, cost alerts, semantic gaps surface there |

## Common Mistakes

| Mistake | Correct approach |
|---------|------------------|
| Using bare table names in SQL (`FROM frpm`) | Use schema-qualified names: `FROM california_schools.frpm` |
| Guessing column filter values (`WHERE type = 'X'`) | Check actual values first: `data sample` or `SELECT DISTINCT` |
| Using `maxc sql ...` | The command is `maxc query ...` |
| `auth login --from-env` without confirming env vars are set | Run `auth whoami --json` first; only use `--from-env` when env vars are confirmed |
| Hand-editing `~/.maxc/config.yaml` | Use `auth login` (or `auth login-external`) |
| Inventing endpoints | Only use endpoints the user provided or that exist in current config |
| `job wait --stream` and expecting a JSON envelope | `--stream` emits NDJSON; use plain `job wait --json` for the envelope |
| Running a query without checking cost first | Use `query cost`, or `--cost-check N` to auto-abort |
| Ignoring `agent_hints.warnings` in the response | They surface backend issues, cache staleness, cost alerts |
| Assuming `meta describe` data is live | Cache may be stale; check `metadata.source` and warnings |
| Using a production project name as default | See SKILL.md §Dev vs Production Workspaces |
| Querying partitioned table without partition filter | Always run `meta latest-partition` first; use the exact returned value in WHERE |

## Agent Anti-Patterns

| Anti-pattern | Why it fails | Do this instead |
|--------------|--------------|-----------------|
| Iterating all schemas/tables to "discover" what's available | Slow, may hit rate limits, wastes tokens | Ask the user which project/schema/table they need |
| Retrying the exact same failed SQL | Same input → same error | Read `error.suggestion`, fix the SQL, then retry |
| `SELECT *` on unknown tables | May scan TB of data, hit cost limits | `meta describe` first, then select specific columns with LIMIT |
| Generating SQL without checking column names | Names are often non-obvious (Chinese, abbreviated) | Always `meta describe` before writing SQL |
| Running multiple queries when one suffices | Wastes compute and time | Combine with JOINs or subqueries |
| Treating `next_actions[]` as a script | Each entry is a hint; quoting may break for SQL with `'` | Reconstruct the command yourself from the entry's intent. There is no `action_ids[]` field — only the three fields `next_actions`/`warnings`/`insights` are emitted. |

## Error Code → Recovery

When `status=failure`, inspect `error.code` and follow the recovery action. Always read `error.suggestion` first — it contains case-specific next steps.

| `error.code` | Meaning | Recovery |
|--------------|---------|----------|
| `VALIDATION_ERROR` | Invalid input or missing required args | Fix the arguments and retry |
| `NOT_FOUND` | Table, job, or resource does not exist | Check the name with `meta search` or `job list` |
| `SCHEMA_NOT_FOUND` | Schema does not exist | Check `error.did_you_mean`; list schemas with `meta list-schemas --json` |
| `TABLE_NOT_FOUND` | Table does not exist in the schema | Check `error.did_you_mean`; search with `meta search <name> --json` |
| `COLUMN_NOT_FOUND` | Column reference does not exist | Check `error.available`; run `meta describe <table> --json` |
| `WRITE_OPERATION_REQUIRES_FORCE` | SQL DDL/DML blocked by read-only mode | Read-only is by design; use `--force` only if authorized. Does NOT apply to `data upload` (Tunnel-based write, no `--force` needed). |
| `CSV_PARSE_ERROR` | A CSV cell could not be parsed against the column type during `data upload` | Read `error.context.line` (1-based row number, including header if present) and `error.context.column` (the column **name** as a string, not a position index) to find the bad cell; fix the source CSV and retry. The Tunnel session is aborted, nothing is committed. |
| `PERMISSION_DENIED` | No access to the resource | Almost always dev vs production workspace — see SKILL.md §Dev vs Production Workspaces |
| `SQL_ERROR` | SQL syntax or execution error | Fix the SQL; use `query explain` to validate first |
| `COST_LIMIT_EXCEEDED` | Cost exceeds `--cost-check` threshold | Add partition filters, reduce columns, or raise the threshold |
| `BACKEND_CONNECTION_ERROR` | Network or service unavailable | Retry after a delay; check endpoint with `auth whoami --json` |
| `JOB_TIMEOUT` | Job did not finish within `--timeout` | `job status <id>` to check; `job wait <id> --timeout <longer>` |
| `QUOTA_EXCEEDED` | Project quota limit reached | Wait and retry, or contact project admin |
| `EXECUTION_FAILED` | General backend failure | `job diagnose <id> --json` if job_id is available |
| `FEATURE_UNAVAILABLE` | Feature not supported in current backend | Check `agent context --json` for supported operations |
| `INTERNAL_ERROR` | Unexpected internal error | Report full error; retry or check CLI version |

## Symptom-Based Troubleshooting

When the symptom doesn't map to a clear `error.code`:

| Symptom | Cause | Fix |
|---------|-------|-----|
| `list-tables` returns empty but tables exist | Wrong schema or no tables in default schema | Use `--schema <name>` or `session set --schema` |
| `search` returns no matches | Keyword not in table/column names or descriptions | Try broader keywords; check `list-tables --schema` |
| `cache build` reports 0 tables | Schema not specified for non-default schemas | Add `--schema <name>` |
| `describe` fails with NOT_FOUND | Table in a different schema | Use `schema.table_name` format or set session schema |
| Commands hang or timeout | Network/endpoint issue | Check `auth whoami --json` for endpoint; verify connectivity |
| `whoami` shows wrong project | Session override or env var shadowing | `session show --json`, `session unset --json`; inspect env vars |
| `whoami` shows `identity_source=mixed` | Env vars are shadowing config | Ask user before unsetting; see bootstrap-flow.md §Common pitfalls |

When all else fails, verify with raw pyodps:

```python
from odps import ODPS
o = ODPS(access_id, secret_key, project, endpoint)
list(o.list_tables(schema='<schema_name>'))
```
