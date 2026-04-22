---
name: maxcompute-cli-guidance
description: Use when the task involves MaxCompute, ODPS, or maxc — querying tables, viewing table schema, listing tables, searching metadata, executing SQL, checking partitions, sampling data, or managing jobs.
description_zh: 当用户需要查询 MaxCompute/ODPS 中的表、查看表结构、列出项目中的表、搜索元数据、执行 SQL、查看分区、预览数据、跟踪任务时使用。适用于提到 ODPS、MaxCompute、maxc、数据仓库表查询等场景。
name_zh: MaxCompute数据查询
category: database
keywords: [MaxCompute, ODPS, maxc, 表, 查表, 查数据, SQL, 数据仓库, 元数据, 分区, odps sql, 阿里云]
requires: MaxCompute account with AK/SK or environment variables
entry_point: maxc
min_cli_version: "0.1.5"
---

# Use MaxC CLI

Use the live CLI instead of inventing a separate MaxCompute adapter. Prefer `maxc ...`; fall back to `python3 -m maxc_cli ...` when the console script is not on `PATH`.

## When To Use

- First-time setup or repair of Python or `maxc-cli`
- Auth bootstrap or identity inspection (AK/SK or env vars)
- Migrating from odpscmd (reusing existing ODPS Console credentials)
- Session project or schema overrides
- Metadata discovery, schema inspection, cache-backed search
- Read-only query execution or job tracking
- Cache and semantic-metadata workflows

Do **not** use when the task is to implement `maxc-cli` itself, or when the user wants raw pyodps/SDK code.

## Core Principles

These are non-negotiable. See [references/red-lines.md](references/red-lines.md) for the full list including common mistakes, anti-patterns, and error recovery.

1. **Always use `--json`** for machine work. Use `--format markdown` for user-facing output, `--format brief` in token-tight contexts. `--json` is shorthand for `--format json`.
2. **Never invent names** — table, schema, project, or endpoint. Verify with `meta` commands and `auth whoami`.
3. **Default to `--project` for the user's target workspace.** The configured project (in `~/.maxc/config.yaml`) is the user's **home dev workspace** — the data they actually want to query usually lives in a *different* workspace (often the corresponding production one). When the user mentions a table/project without specifying which workspace, **ask first**, then pass `--project <name>` on every meta/data command and use `project.table` in SQL.
4. **Workspace naming convention is a fixed pair:** `<name>_dev` is the dev workspace; the same `<name>` **without** the `_dev` suffix is its corresponding **production** workspace. They share metadata structure but hold different data and different permissions. See Dev vs Production Workspaces below.
5. **Never re-prompt for credentials** when `auth whoami` shows `authenticated=true`. Permission errors are almost always a workspace/project issue, not a credential issue.
6. **Always discover partitions** via `meta latest-partition` before querying partitioned tables. Format varies per table.
7. **Always read `error.suggestion`** before retrying a failed command. Same input → same error.
8. **Never install or upgrade Python** without explicit user confirmation.
9. **Never log AK/SK** in output, even in error context.

## Bootstrap Flow

When `auth whoami --json` returns `configured=false` (no auth set up), follow [references/bootstrap-flow.md](references/bootstrap-flow.md) step by step. Two principles:

1. **Never pick the auth method yourself** — always ask the user to choose between AK/SK and environment variables.
2. **If the user already has `odpscmd` configured**, proactively offer to migrate those credentials before asking them to enter anything new — see [references/migrate-from-odpscmd.md](references/migrate-from-odpscmd.md).
3. **If `auth whoami` shows `auth_type=external`, the user is on an externally-managed credential provider — do NOT modify the auth config.** Treat the bootstrap as already done. Only `project`/`endpoint`/`schema` are safe to change (via `session set` or by re-running `auth login-external` with the same `--process-command`).

## First Pass

1. Prefer `maxc ...`; use `python3 -m maxc_cli ...` if not on `PATH`. If the machine may not be bootstrapped, read [references/setup-install.md](references/setup-install.md) first.
2. Run `maxc auth whoami --json`. Check `data.identity`:
   - `authenticated=true, validation_status=verified` → ready, continue.
   - `configured=false` → no auth set up → follow Bootstrap Flow above.
   - `configured=true, validation_status=failed` → config exists but remote check failed → inspect warnings, then fix or re-login.
3. For exact command syntax and output shapes, see [references/command-patterns.md](references/command-patterns.md).

## JSON Output Format

All `--json` output follows the envelope format. Always check `status` first — on `failure`, read `error.code` and `error.suggestion`; on `success`/`pending`, check `agent_hints.next_actions` and `agent_hints.warnings`.

Key paths:

- `data.result.rows` (query)
- `data.analysis` (query cost/explain)
- `data.identity` (auth whoami)
- `data.table` (meta describe)
- `data.sample` (data sample)
- `metadata.job_id` (async)

`--json` stdout is one final envelope. Exception: `job wait --stream` emits NDJSON events. `cache build --json` emits progress to `stderr`, one final envelope to `stdout`.

See [references/json-output-format.md](references/json-output-format.md) for full examples and [references/command-patterns.md](references/command-patterns.md) §JSON Contract for all data shapes.

## Dev vs Production Workspaces

MaxCompute workspaces come in **fixed pairs**: a dev workspace and its corresponding production workspace. Confusing the two is the #1 source of permission errors.

### The naming pair (memorize this)

| Workspace type | Name pattern | Example | Who can access | What lives there |
|----------------|--------------|---------|----------------|------------------|
| **Dev**        | `<name>_dev` | `my_project_dev` | Personal accounts (the user themselves) | Test data, scratch tables, the user's own work |
| **Production** | `<name>`     | `my_project`     | Usually only service accounts / DataWorks pipelines | The real business data the user actually wants to query |

The dev workspace and the production workspace **share metadata structure but hold different data**. A table that exists in `my_project_dev` almost always exists in `my_project` too — but the rows, partitions, and freshness will differ.

### Other key facts

- The project configured in `~/.maxc/config.yaml` or env vars is always the **dev** workspace — this is the user's home workspace.
- Personal accounts usually only have *write* access to dev and *read* access to production (varies by org policy). Pointing a session directly at production often results in `PERMISSION_DENIED`.
- `--project` is the canonical way to access **another project's** tables — most often the corresponding production workspace, occasionally a different team's project.
- When the user asks about a table without naming the workspace, **ask whether they mean the dev or production copy** before guessing.

### How to tell which workspace you are in

```bash
maxc auth whoami --json    # check data.identity.project — ends with _dev?
maxc session show --json   # check current session project
```

If the project name does NOT end with `_dev`, you may be pointed at a production workspace by mistake.

### Accessing production tables from dev workspace

Use `--project` to read metadata from the production workspace without switching session:

```bash
maxc meta list-tables --project my_project --json
maxc meta describe my_table --project my_project --json
maxc data sample my_table --project my_project --json
```

When writing SQL, use `project.table` format to reference tables in another workspace:

```sql
-- From dev workspace, query a production table
SELECT * FROM my_project.my_table WHERE ds = '20260418' LIMIT 100
```

Do NOT use bare table names (`FROM my_table`) when the target table lives in a different project — the query will fail with `TABLE_NOT_FOUND`.

### Common permission error scenarios

| Scenario | Symptom | Fix |
|----------|---------|-----|
| Config points to production workspace | `PERMISSION_DENIED` on most operations | `maxc session set --project my_project_dev` |
| Need to read production table metadata | `PERMISSION_DENIED` on `meta describe` | `maxc meta describe my_table --project my_project --json` |
| SQL references a table in another project without project prefix | `TABLE_NOT_FOUND` | Use `project.table` format in SQL |
| Mixed access: dev metadata + production data | Confusing results | Be explicit: use `--project` for metadata commands, `project.table` in SQL |

## NL2SQL Workflow

Standard flow for answering data questions:

```
1. meta list-tables --schema <s> --json     → get table names + schema_name
2. meta describe <schema.table> --json      → get ALL columns (--json returns full schema)
3. query cost "SELECT ..." --json           → estimate cost (skip for simple queries)
4. query "SELECT ..." --json                → execute query
```

Add `--project <p>` to any step when working with a non-default project.

**Critical rules:**

- Always use schema-qualified table names in SQL: `<schema>.<table>` (e.g. `california_schools.frpm`), not bare table names. The `list-tables` output includes `schema_name` for each table.
- `meta describe --json` returns **all columns** automatically. Without `--json`, use `--full` to avoid truncation.
- Column names with spaces or special characters must be backtick-escaped: `` `column name` ``.
- When filtering by column values, first check actual values with `data sample` or a `SELECT DISTINCT` query — don't guess enum values.
- For partitioned tables, always filter by partition column in WHERE (e.g. `WHERE ds = '20260415'`) to avoid full-table scans.
- When accessing tables from another project, use `project.table` format in SQL (see Dev vs Production Workspaces).

For dialect notes (SET options, type coercion, date functions, common error codes), see [references/maxcompute-sql-notes.md](references/maxcompute-sql-notes.md).

## Partition Strategy

Before querying a partitioned table, always determine the correct partition value:

```
1. maxc meta describe <table> --json  → check partition_columns
   - No partitions? → Query directly with LIMIT
   - Has partitions? → Continue to step 2

2. maxc meta latest-partition <table> --json  → get latest value and format
   - Note the exact format (e.g. "20260415" vs "2026-04-15")

3. Construct WHERE clause using exact value from step 2
   - Example: WHERE ds = '20260415' LIMIT 100

4. For "_df" tables: latest partition = latest full snapshot
   For "_di" tables: may need date range for complete picture
```

Prefer `meta latest-partition` over `MAX_PT()` for ad-hoc queries — `MAX_PT` may return incomplete partitions or only consider the first partition level.

See [references/partition-guide.md](references/partition-guide.md) for naming conventions, MAX_PT() guidance, and ambiguity handling.

## "I want to X → Use Y" Decision Table

For full command syntax and options, see [references/command-patterns.md](references/command-patterns.md).

| I want to... | Use |
|--------------|-----|
| Check who I am and where I'm pointed | `maxc auth whoami --json` |
| Set up auth from scratch | See Bootstrap Flow above |
| Switch project/schema for this session | `maxc session set --project P --schema S --json` |
| List tables | `maxc meta list-tables --schema S --json` |
| Get full schema of a table | `maxc meta describe T --json` |
| Find tables by keyword | `maxc meta search KW --json` |
| Find columns by keyword | `maxc meta search-columns KW --json` |
| Get latest partition value | `maxc meta latest-partition T --json` |
| Sample data | `maxc data sample T --rows 10 --json` |
| Run a query | `maxc query "SELECT ..." --json` |
| Estimate cost first | `maxc query cost "SELECT ..." --json` |
| Run a long query async | `maxc query "..." --wait 0 --json`, then `maxc job wait <id> --json` |
| Auto-abort if too costly | `maxc query "..." --cost-check 10.0 --json` |
| Read another project's tables | Add `--project P` to any meta/data command; use `project.table` in SQL |
| Build/refresh metadata cache | `maxc cache build --json` |
| Check permission for an op | `maxc auth can-i --table T --operation SELECT --json` |
| Diagnose a failed job | `maxc job diagnose <id> --json` |
| Add semantic metadata to a table | `maxc meta semantic set T ... --json` (see command-patterns.md §Semantic Metadata) |
| Compare two tables | `maxc diff schema|partition|data ...` (see command-patterns.md §Diffs) |
| Migrate from odpscmd | See [references/migrate-from-odpscmd.md](references/migrate-from-odpscmd.md) |

## Capability Boundaries

| Boundary | Detail | Alternative |
|----------|--------|-------------|
| Read-only enforcement | Client-side SQL keyword detection; write operations blocked before reaching MaxCompute | Use odpscmd, pyodps SDK, or DataWorks |
| No permission management | `auth can-i` checks one table+operation; cannot enumerate accessible tables | MaxCompute console or project admin tools |
| No complete permission inventory | Cannot iterate projects to discover all readable tables | Ask user for target project/table |
| No data upload/import | Read-only tool | Use odpscmd tunnel or DataWorks |
| No lineage API | Returns `supported: false` placeholder | Use DataWorks lineage |
| No resource/UDF management | No upload/registration | Use odpscmd or DataWorks |

## Known Limitations

| Feature | Status | Detail |
|---------|--------|--------|
| `meta search` | Catalog API preferred | Server-side FTS via pyodps RestClient (auto-routed); falls back to cache/live substring match |
| `list-tables` pagination | Not implemented | CLI-side `--cursor` is offset token, not server-side cursor |
| `diff data` | Snapshot compare | Keyed snapshot compare, not exhaustive diff |
| `auth login` | Plaintext YAML | AccessKey stored in `~/.maxc/config.yaml` (file permissions 0600) |
| Write operations | Client-side read-only | Write operations blocked by CLI before submission; `--force` bypasses |

## References Index

| File | When to read |
|------|--------------|
| [bootstrap-flow.md](references/bootstrap-flow.md) | First-time setup or `configured=false` |
| [setup-install.md](references/setup-install.md) | Python / maxc-cli install detail |
| [bootstrap-auth.md](references/bootstrap-auth.md) | Per-method auth setup (AK/SK, env vars) |
| [migrate-from-odpscmd.md](references/migrate-from-odpscmd.md) | User has `odpscmd` configured |
| [command-patterns.md](references/command-patterns.md) | Full command syntax, output shapes, cache/diff/semantic, multi-project, schema, async |
| [json-output-format.md](references/json-output-format.md) | JSON envelope examples |
| [partition-guide.md](references/partition-guide.md) | Partition naming, MAX_PT() guidance, ambiguity |
| [maxcompute-sql-notes.md](references/maxcompute-sql-notes.md) | SQL dialect, SET options, date functions, error codes |
| [red-lines.md](references/red-lines.md) | What not to do; common mistakes; anti-patterns; error code → recovery; symptom troubleshooting |
