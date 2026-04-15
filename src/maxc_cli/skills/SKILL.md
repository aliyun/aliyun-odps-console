---
name: use-maxc-cli
description: Use when the task involves MaxCompute, ODPS, or maxc — including environment setup, auth bootstrap (AK/SK or env vars), session project/schema switching, metadata discovery, read-only SQL execution, or cache and semantic-metadata workflows.
category: database
requires: MaxCompute account with AK/SK or environment variables
entry_point: maxc
---

# Use MaxC CLI

Use the live CLI instead of inventing a separate MaxCompute adapter. Prefer `maxc ...`; fall back to `python3 -m maxc_cli ...` when the console script is not on `PATH`.

## When To Use

- First-time setup or repair of Python or `maxc-cli`
- Auth bootstrap or identity inspection (AK/SK or env vars)
- Session project or schema overrides
- Metadata discovery, schema inspection, cache-backed search
- Read-only query execution or job tracking
- Cache and semantic-metadata workflows

Do **not** use when the task is to implement `maxc-cli` itself, or when the user wants raw pyodps/SDK code.

## Bootstrap Flow

```dot
digraph bootstrap {
    "Run auth whoami --json" [shape=box];
    "authenticated?" [shape=diamond];
    "Show current identity\nAsk: continue or re-configure?" [shape=box];
    "Continue with task" [shape=doublecircle];
    "Ask user: which auth method?" [shape=box];
    "AK/SK" [shape=box];
    "Env vars" [shape=box];

    "Run auth whoami --json" -> "authenticated?";
    "authenticated?" -> "Show current identity\nAsk: continue or re-configure?" [label="yes"];
    "authenticated?" -> "Ask user: which auth method?" [label="no"];
    "Show current identity\nAsk: continue or re-configure?" -> "Continue with task" [label="continue"];
    "Show current identity\nAsk: continue or re-configure?" -> "Ask user: which auth method?" [label="re-configure"];
    "Ask user: which auth method?" -> "AK/SK";
    "Ask user: which auth method?" -> "Env vars";
    "AK/SK" -> "Continue with task";
    "Env vars" -> "Continue with task";
}
```

**When already authenticated, always show the current identity and ask before continuing:**

> "Currently authenticated as `<principal_display>` on project `<project>` via `<auth_type>`.
> Continue with this, or re-configure auth?"

**When auth is not ready, always ask the user before choosing a path:**

> "Which auth method would you like to use?
> (A) Access Key / Secret Key — long-lived key pair
> (B) Environment variables — keys already set in the current shell"

Then follow the corresponding section in [references/bootstrap-auth.md](references/bootstrap-auth.md).

## First Pass

1. Prefer `maxc ...`; use `python3 -m maxc_cli ...` if not on `PATH`. If the machine may not be bootstrapped, read [references/setup-install.md](references/setup-install.md) first.
2. Run `maxc auth whoami --json`. Check `data.identity`:
   - `authenticated=true, validation_status=verified` → ready, continue.
   - `configured=false` → no auth set up → **ask which method** (see Bootstrap Flow above).
   - `configured=true, validation_status=failed` → config exists but remote check failed → inspect warnings, then fix or re-login.
3. Read [references/bootstrap-auth.md](references/bootstrap-auth.md) for auth paths.
4. `meta list-tables --json` falls back to live queries on cache miss. Run `cache build --json` to speed up repeat queries.
5. Read [references/command-patterns.md](references/command-patterns.md) for command syntax and output shapes.

## Working Rules

- Stay read-only unless the user explicitly asks for state changes. Query execution limited to `SELECT`.
- Prefer `--json` for machine-driven work.
- `--json` stdout is one final envelope. Exception: `job wait --stream` emits NDJSON events.
- `cache build --json` emits progress to `stderr`, one final envelope to `stdout`.
- Trust runtime help and actual command output over stale snippets.
- Never install or upgrade Python without explicit user confirmation.
- Prefer `auth login` over hand-editing `~/.maxc/config.yaml`.
- `meta list-tables` is cache-backed; falls back to live backend query on cache miss.
- Most meta commands support `--schema` to override the session default (list-tables, search, search-columns).
- `session set/show/unset` are local-only — no authenticated backend required.
- `agent context` is a fast local config summary (auth status, backend reachability, capabilities, skill path); does not enumerate tables.
- `agent skill` returns the SKILL.md path and metadata.
- `agent install-skill <platform>` registers the skill with an Agent platform (claude-code, cursor, windsurf, codex). Idempotent; re-run after `pip install --upgrade` to update local skill files.
- Use normalized `data` shapes: `auth whoami` → `data.identity`, `query`/`job result` → `data.result`, `meta describe` → `data.table`, `data sample` → `data.sample`.
- Use `agent_hints.action_ids` for stable program logic; `next_actions` are hints only.

## NL2SQL Workflow

Standard flow for answering data questions:

```
1. meta list-tables --schema <s> --json     → get table names + schema_name
2. meta describe <schema.table> --json      → get ALL columns (--json returns full schema)
3. query cost "SELECT ..." --json           → estimate cost (skip for simple queries)
4. query "SELECT ..." --json                → execute query
```

**Critical rules:**
- Always use schema-qualified table names in SQL: `<schema>.<table>` (e.g. `california_schools.frpm`), not bare table names. The `list-tables` output includes `schema_name` for each table.
- `meta describe --json` returns **all columns** automatically. Without `--json`, use `--full` to avoid truncation.
- Column names with spaces or special characters must be backtick-escaped: `` `column name` ``.
- When filtering by column values, first check actual values with `data sample` or a `SELECT DISTINCT` query — don't guess enum values.

## Query Commands

```bash
# Execute a query
maxc query "SELECT * FROM schema.table LIMIT 10" --json

# Estimate cost before running
maxc query cost "SELECT * FROM schema.table" --json

# Explain execution plan
maxc query explain "SELECT * FROM schema.table" --json

# Submit and return immediately (async)
maxc query "SELECT ..." --wait 0 --json

# Auto-abort if cost exceeds threshold
maxc query "SELECT ..." --cost-check 10.0 --json
```

Note: the command is `query`, not `sql`. There is no `maxc sql` command.

## Common Mistakes

| Mistake | Correct approach |
|---------|-----------------|
| Using bare table names in SQL (`FROM frpm`) | Use schema-qualified names: `FROM california_schools.frpm` |
| Guessing column filter values (`WHERE type = 'Continuation'`) | Check actual values first: `data sample` or `SELECT DISTINCT` |
| Using `maxc sql ...` | The command is `maxc query ...` |
| Using `auth login --from-env` without checking env vars exist | Run `auth whoami --json` first; only use `--from-env` when env vars are confirmed set |
| Hand-editing `~/.maxc/config.yaml` | Use `auth login` |
| Calling `meta list-tables` on a cold cache | Tables are fetched live on cache miss; `cache build` improves speed for repeat queries |
| Inventing endpoints | Only use endpoints the user provided or that exist in current config |
| Using `job wait --stream` and expecting a JSON envelope | `--stream` emits NDJSON; use plain `job wait --json` for envelope |
| Running a query without checking cost first | Use `query cost` before large queries; use `--cost-check` to set auto-abort threshold |
| Ignoring `agent_hints.warnings` in the response | Always check warnings — they surface backend issues, cache staleness, and cost alerts |
| Assuming `meta describe` data is live | Cache source may be stale; check `metadata.source` field and `agent_hints.warnings` |

## Error Recovery

When a command returns `status=failure`, inspect the `error.code` field to determine the recovery action:

| `error.code` | Meaning | Recovery |
|--------------|---------|----------|
| `VALIDATION_ERROR` | Invalid input or missing required args | Fix the arguments and retry |
| `NOT_FOUND` | Table, job, or resource does not exist | Check the name with `meta search` or `job list` |
| `PERMISSION_DENIED` | No access to the resource | Run `auth can-i --table <t> --operation SELECT --json` to verify; switch account if needed |
| `SQL_ERROR` | SQL syntax or execution error | Fix the SQL; use `query explain` to validate syntax first |
| `COST_LIMIT_EXCEEDED` | Query cost exceeds `--cost-check` threshold | Lower the scan scope (add partition filters, reduce columns), or raise the threshold |
| `BACKEND_CONNECTION_ERROR` | Network or service unavailable | Retry after a delay; check endpoint with `auth whoami --json` |
| `JOB_TIMEOUT` | Job did not complete within `--timeout` | Use `job status <id> --json` to check progress; `job wait <id> --timeout <longer>` to continue |
| `QUOTA_EXCEEDED` | Project quota limit reached | Wait and retry, or contact project admin |
| `EXECUTION_FAILED` | General backend failure | Run `job diagnose <id> --json` if a job_id is available |
| `FEATURE_UNAVAILABLE` | Feature not supported in current backend | Check `agent context --json` for supported operations |
| `INTERNAL_ERROR` | Unexpected internal error | Report the full error message; retry or check CLI version |

Always check `error.suggestion` — it contains actionable next steps when available.

## Wait and Timeout Behavior

- `query "..." --wait N --json`: polls for up to N seconds. If the job finishes within N seconds, returns the result. If not, auto-promotes to async and returns `status=pending` with a `job_id`.
- `query "..." --wait 0 --json`: submits the job and returns immediately with `status=pending` and a `job_id`.
- `job wait <id> --timeout N --json`: waits up to N seconds for the job to complete. Returns `status=pending` if the timeout is reached.
- Default `--wait` for `query` is 10 seconds. Default `--timeout` for `job wait` is 300 seconds.
- For long-running queries, use `--wait 0` to get the job_id immediately, then poll with `job status`.

## Multi-Project Workflow

```bash
# List accessible projects
maxc meta list-projects --json

# Switch to a different project
maxc session set --project other_project --json

# Optionally set a specific schema
maxc session set --project other_project --schema my_schema --json

# Verify the switch
maxc session show --json

# Build cache for the new project
maxc cache build --json

# Revert to config defaults
maxc session unset --json
```

Session overrides are stored in `~/.maxc/session_override.yaml` and take priority over config files and env vars for project/schema only.

## Schema Operations

For projects with 3-tier namespace (project.schema.table):

```bash
# List available schemas
maxc meta list-schemas --json

# List tables in a specific schema (two approaches)
maxc meta list-tables --schema california_schools --json        # one-shot
maxc session set --schema california_schools --json             # sticky session

# Search within a schema
maxc meta search school --schema california_schools --json
maxc meta search-columns county --schema california_schools --json

# Build cache for a specific schema
maxc cache build --schema california_schools --json

# Describe a table (use schema.table_name format)
maxc meta describe california_schools.frpm --json

# Reset to default schema
maxc session unset --json
```

When `--schema` is given, it overrides `session set --schema`. When neither is set, the project default schema is used.

## Cache Mechanism

The metadata cache accelerates `list-tables`, `search`, `search-columns`, and `describe`.

- **How it works**: `cache build` fetches all table metadata from MaxCompute and stores it in a local SQLite DB (`~/.maxc/cache/cache.db`). Subsequent meta commands read from cache first.
- **Cache key**: `(project, schema_name, table_name)` — schema is part of the key, so different schemas have independent caches.
- **Cache miss behavior**: `list-tables` and `search` fall back to live backend queries on cache miss. No manual cache build is required, but caching speeds up repeated queries.
- **When to rebuild**: After schema changes, new tables, or when cache is stale. Check with `cache status --json`.

```bash
maxc cache build --json                                # build for current project/schema
maxc cache build --schema my_schema --json             # build for specific schema
maxc cache status --json                               # check cache freshness
maxc cache clear --json                                # wipe and rebuild
```

## Known Limitations

| Feature | Status | Detail |
|---------|--------|--------|
| `meta lineage` | Placeholder | Returns `supported=false`; MaxCompute lineage API not yet integrated |
| `list-tables` pagination | Not implemented | CLI-side `--cursor` is offset token, not server-side cursor |
| `diff data` | Snapshot compare | Keyed snapshot compare, not exhaustive diff |
| `auth login` | Plaintext YAML | AccessKey stored in `~/.maxc/config.yaml` (file permissions 0600) |
| Write operations | Read-only | CLI enforces SELECT-only; DDL/DML not supported |

## Cost Control

Before running large queries, always estimate cost first:

```bash
# Check cost before executing
maxc query cost "SELECT * FROM big_table" --json

# Auto-abort if cost exceeds threshold (in CU)
maxc query "SELECT * FROM big_table" --cost-check 10.0 --json

# Use dry-run to see the plan without execution
maxc query "SELECT * FROM big_table" --dry-run --json
```

The `agent context` output includes `cost_threshold_cu` (project-level default) and `allowed_operations` — respect these guardrails.

## Semantic Metadata Workflow

Semantic metadata enriches tables with business context for NL2SQL and agent discovery.

```bash
# Check which tables need semantic metadata
maxc meta semantic list-missing --json

# Add semantic metadata (agent generates this from LLM understanding)
maxc meta semantic set my_table \
  --desc "Daily user login events" \
  --use-cases "login funnel analysis" "DAU calculation" \
  --sample-questions "How many users logged in yesterday?" \
  --column-semantics '[{"name":"user_id","semantic_type":"user_identifier"}]' \
  --json

# Retrieve existing metadata
maxc meta semantic get my_table --json

# Verify in describe output (semantic section appears when metadata exists)
maxc meta describe my_table --json
```

When `meta describe` returns a warning about missing semantic metadata, the agent should generate it using its own LLM understanding of the table schema and save it with `meta semantic set`.

## Diff Workflow

Use diff commands to compare tables across environments or track schema changes:

```bash
# Compare schemas of two tables
maxc diff schema table_a table_b --json

# Compare partition lists
maxc diff partition table_a table_b --json

# Compare data by key columns (read-only snapshot comparison)
maxc diff data table_a table_b --keys id --columns value_col --rows 100 --json

# Compare with different partitions on each side
maxc diff data prod_table staging_table \
  --keys user_id \
  --left-partition ds=2026-04-09 \
  --right-partition ds=2026-04-10 \
  --json
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `list-tables` returns empty but tables exist | Wrong schema or no tables in default schema | Use `--schema <name>` or `session set --schema` |
| `search` returns no matches | Keyword not in table/column names or descriptions | Try broader keywords; check with `list-tables --schema` first |
| `cache build` reports 0 tables | Schema not specified for non-default schemas | Add `--schema <name>` |
| `describe` fails with NOT_FOUND | Table in a different schema | Use `schema.table_name` format or set session schema |
| Commands hang or timeout | Network/endpoint issue | Check `auth whoami --json` for endpoint; verify connectivity |

When all else fails, verify with raw pyodps:
```python
from odps import ODPS
o = ODPS(access_id, secret_key, project, endpoint)
list(o.list_tables(schema='<schema_name>'))
```

## Command Families

- Bootstrap: `python3 --version`, `pip install --upgrade maxc-cli`, `python3 -m maxc_cli --help`
- Auth and session: `auth whoami`, `auth login`, `auth can-i`, `session set/show/unset`
- Metadata and data: `meta list-tables`, `meta describe`, `meta search`, `meta search-columns`, `meta latest-partition`, `meta freshness`, `meta partitions`, `meta list-projects`, `meta list-schemas`, `data sample`, `data profile`
- Query and jobs: `query`, `query cost`, `query explain`, `job submit/status/wait/result/diagnose/cancel/list`
- Cache and semantic metadata: `cache build`, `cache build-status`, `cache status`, `cache clear`, `cache save-semantic`, `cache get-semantic`, `meta semantic set/get/list-missing`
- Diffs and context: `diff schema`, `diff partition`, `diff data`, `agent context`
- Agent and skill: `agent context`, `agent skill`, `agent install-skill`
