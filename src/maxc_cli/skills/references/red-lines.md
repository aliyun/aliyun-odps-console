> Loaded on demand — full "what not to do" list and error-code → recovery map. Skip unless the agent hit an error or is about to retry a failed command.

# Red Lines And Recovery

Single source of truth for "what not to do" and "how to recover when X breaks". Read this when you hit an error, before retrying anything.

## Critical Red Lines

Cross-referenced with SKILL.md §Core Principles (which lists the highest-priority subset). Full set below.

| # | Rule | Why |
|---|------|-----|
| 1 | Always use `--json` for machine-driven work | Plain output is for humans; agents must parse the envelope |
| 2 | Never invent table, schema, project, or endpoint names | Always verify with `meta search` / `meta list-tables` / `auth whoami` |
| 3 | Never install or upgrade Python without explicit user confirmation | System-level change with broad impact |
| 4 | Never re-prompt for credentials when `auth whoami` shows `authenticated=true` | A permission error can reflect the selected object, project, schema, policy, or identity; inspect it rather than assuming bad credentials |
| 5 | Always check partition value via `meta latest-partition` before querying partitioned tables | Hardcoded partitions go stale; format varies per table |
| 6 | Use the namespace shape established by metadata | Three-tier projects may require `schema.table`; two-tier projects do not have a schema to invent |
| 7 | Never log, echo, or include AK/SK in output | Even in error context |
| 8 | Read `error.suggestion` before retrying a failed command | Same input → same error |
| 9 | Do not replace an existing authentication method without authorization | Keep a working configured provider. For a new public-cloud login, recommend OAuth first; use another approved method only when policy or runtime constraints require it. |
| 10 | Trust runtime help and actual command output over stale snippets | The CLI evolves; cached knowledge can be wrong |
| 11 | Never execute SQL DDL/DML through this Skill | The public Agent SQL contract is `SELECT`-only. Use an approved change workflow outside this Skill. (`data upload` remains a separate, intentional Tunnel write and still needs authorization.) |
| 12 | Always check `agent_hints.warnings` even when `status=success` | Cache staleness, cost alerts, semantic gaps surface there |

## Common Mistakes

| Mistake | Correct approach |
|---------|------------------|
| Guessing a table's qualification | Run `meta describe` / `meta list-schemas` and use the verified two-tier or three-tier name |
| Guessing column filter values (`WHERE type = 'X'`) | Check actual values first: `data sample` or `SELECT DISTINCT` |
| Using `{{cli}} sql ...` | The command is `{{cli}} query ...` |
| `auth login --from-env` without confirming env vars are set | Run `auth whoami --json` first; only use `--from-env` when env vars are confirmed |
| Hand-editing `~/.maxc/config.yaml` | Use `auth login` (or `auth login-external`) |
| Inventing endpoints | Only use endpoints the user provided or that exist in current config |
| `job wait --stream` and expecting live progress | It emits buffered NDJSON events after waiting; use plain `job wait --json` for one envelope |
| Running a query without checking cost first | Use `query cost`, or `--cost-check N` to auto-abort |
| Ignoring `agent_hints.warnings` in the response | They surface backend issues, cache staleness, cost alerts |
| Assuming `meta describe` data is live | Cache may be stale; check `metadata.source` and warnings |
| Inferring dev/prod from a project suffix | Treat project names as opaque and ask when the target is ambiguous |
| Querying partitioned table without partition filter | Always run `meta latest-partition` first; use the exact returned value in WHERE |

## Agent Anti-Patterns

| Anti-pattern | Why it fails | Do this instead |
|--------------|--------------|-----------------|
| Iterating all schemas/tables to "discover" what's available | Slow, may hit rate limits, wastes tokens | Ask the user which project/schema/table they need |
| Retrying the exact same failed SQL | Same input → same error | Read `error.suggestion`, fix the SQL, then retry |
| `SELECT *` on unknown tables | May scan TB of data, hit cost limits | `meta describe` first, then select specific columns with LIMIT |
| Generating SQL without checking column names | Names are often non-obvious (Chinese, abbreviated) | Always `meta describe` before writing SQL |
| Running multiple queries when one suffices | Wastes compute and time | Combine with JOINs or subqueries |
| Treating every action as immediately runnable | Required values may be unresolved | Check `actions[].executable` and resolve `placeholders` from verified context |
| Assuming every structured action appears in `next_actions` | Mutations, confirmation-gated actions, and templates are intentionally omitted | Treat `actions[]` as authoritative and check `effect`, `agent_allowed`, and `confirmation_required` |
| Enabling automatic retry flags for a remote query | Resumable remote execution rejects them to avoid duplicate submissions | Submit once, retain `metadata.job_id`, inspect the job, then decide manually |
| Assuming upload creates a missing partition | Ordinary upload never creates one | Use `--create-partition` only with explicit authorization and account for a possible empty partition after failure |

## Error Code → Recovery

When `status=failure`, inspect `error.code` and follow the recovery action. Always read `error.suggestion` first — it contains case-specific next steps.

| `error.code` | Meaning | Recovery |
|--------------|---------|----------|
| `VALIDATION_ERROR` | Invalid input or missing required args | Fix the arguments and retry |
| `NOT_FOUND` | Table, job, or resource does not exist | Check the name with `meta search` or `job list` |
| `SCHEMA_NOT_FOUND` | Schema does not exist | Check `error.did_you_mean`; list schemas with `meta list-schemas --json` |
| `TABLE_NOT_FOUND` | Table does not exist in the schema | Check `error.did_you_mean`; search with `meta search <name> --json` |
| `COLUMN_NOT_FOUND` | Column reference does not exist | Check `error.available`; run `meta describe <table> --json` |
| `WRITE_OPERATION_REQUIRES_FORCE` | SQL DDL/DML blocked by the read-only gate | Do not bypass it from this Skill. Use an approved mutation workflow outside the Skill. This does not apply to the separately authorized Tunnel `data upload` command. |
| `CSV_PARSE_ERROR` | A CSV cell could not be parsed against the column type during `data upload` | Read `error.context.line` (1-based row number, including header if present) and `error.context.column` (the column **name** as a string, not a position index) to find the bad cell; fix the source CSV and retry. File validation finishes before a Tunnel session is opened, so no rows are written. If `--create-partition` was explicitly requested, treat creation of an empty partition as a separate possible metadata side effect. |
| `UPLOAD_COMMIT_OUTCOME_UNKNOWN` | The process was interrupted or the backend failed after the Tunnel commit request began, so remote visibility cannot be proven | **Do not retry the upload.** Verify the target table or partition first; retrying append can duplicate rows and retrying overwrite can replace a successful result. This error is non-recoverable and exits with code 130. |
| `PERMISSION_DENIED` | The checked operation is not allowed for the effective identity/object | Verify identity, project/schema/object, then use `auth can-i` for that exact target |
| `SQL_ERROR` | SQL syntax or execution error | Fix the SQL; use `query explain` to validate first |
| `COST_LIMIT_EXCEEDED` | Cost exceeds `--cost-check` threshold | Add partition filters, reduce columns, or raise the threshold |
| `BACKEND_CONNECTION_ERROR` | Network or service unavailable | Check the online doctor and endpoint; retry once only when the failure is transient |
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

When recovery remains unclear, inspect `agent manifest`, the relevant `--help`,
and the failure envelope. Do not bypass the CLI with ad-hoc credential-bearing
PyODPS code unless the user explicitly requested SDK debugging.
