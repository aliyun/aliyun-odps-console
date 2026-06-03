---
name: maxc-cli
description: Use when the task involves MaxCompute, ODPS, or {{cli}} — querying tables, viewing table schema, listing tables, searching metadata, executing SQL, checking partitions, sampling data, uploading or downloading CSV data, managing jobs, or generating MaxCompute SQL.
---

# Use MaxC CLI

Use the live CLI instead of inventing a separate MaxCompute adapter. Prefer `{{cli}} ...`<!-- @if cli_module_differs -->; fall back to `{{cli_module}} ...` when the console script is not on `PATH`<!-- @endif -->.

## When To Use

- First-time setup or repair of Python or `maxc-cli`
- Auth bootstrap or identity inspection (AK/SK or env vars)
- Session project or schema overrides
- Metadata discovery, schema inspection, fast table/column search
- Read-only query execution or job tracking
- Cache and semantic-metadata workflows
- Bulk-loading a local CSV/TSV into an existing table (`data upload`) or pulling a partition out to a local CSV/TSV (`data download`)

Do **not** use when the task is to implement `maxc-cli` itself, or when the user wants raw pyodps/SDK code.

## Intent → Command Quick Map

Load the matching reference only when the intent below applies. Otherwise stay
in SKILL.md alone.

| Agent intent | First command to try | Load this reference if needed |
|---|---|---|
| Run a SELECT | `{{cli}} query "<sql>" --json` | [command-patterns.md](references/command-patterns.md) |
| Schema/columns of a table | `{{cli}} meta describe <table> --json` | [command-patterns.md](references/command-patterns.md) |
| Find tables by keyword | `{{cli}} meta search <keyword> --json` | [command-patterns.md](references/command-patterns.md) |
| Latest partition / freshness | `{{cli}} meta latest-partition <table> --json` | [partition-guide.md](references/partition-guide.md) |
| Sample rows | `{{cli}} data sample <table> --limit 10 --json` | [command-patterns.md](references/command-patterns.md) |
| SQL kept erroring | (see error envelope's `error.suggestion`) | [sql-common-errors.md](references/sql-common-errors.md) |
| Generate SQL from NL | (see NL2SQL Workflow below) | [text2sql-principles.md](references/text2sql-principles.md) |
| Install SKILL for a new platform | `{{cli}} agent skill install <platform> --json` | [setup-install.md](references/setup-install.md) |
| Inspect what's installed | `{{cli}} agent skill list --json` | n/a |

## Core Principles

These are non-negotiable. See [references/red-lines.md](references/red-lines.md) for the full list including common mistakes, anti-patterns, and error recovery.

1. **Always use `--json`** for machine work. Use `--format markdown` for user-facing output, `--format brief` in token-tight contexts. `--json` is shorthand for `--format json`. **`--format` is a top-level flag — it must come before the subcommand**: `{{cli}} --format markdown query "SELECT 1"` (✓), not `{{cli}} query "SELECT 1" --format markdown` (✗). `--json` may appear anywhere because each subcommand also accepts it.
2. **Never invent names** — table, schema, project, or endpoint. Verify with `meta` commands and `auth whoami`.
3. **Default to `--project` for the user's target project.** The configured project (in `~/.maxc/config.yaml`) is the user's **dev project** — the data they actually want to query usually lives in a *different* project (often the corresponding production one). When the user mentions a table/project without specifying which environment, **ask first**, then pass `--project <name>` on every meta/data command and use `project.table` in SQL.
4. **Project naming convention is a fixed pair:** `<name>_dev` is the dev project; the same `<name>` **without** the `_dev` suffix is its corresponding **production** project. Together they form one DataWorks workspace. They share metadata structure but hold different data and different permissions. See Dev vs Production Projects below.
5. **Never re-prompt for credentials** when `auth whoami` shows `authenticated=true`. Permission errors are almost always a project environment issue (dev vs prod), not a credential issue.
6. **Always discover partitions** via `meta latest-partition` before querying partitioned tables. Format varies per table.
7. **Always read `error.suggestion`** before retrying a failed command. Same input → same error.
8. **Never install or upgrade Python** without explicit user confirmation.
9. **Never log AK/SK** in output, even in error context.

## Bootstrap Flow

When `auth whoami --json` returns `configured=false` (no auth set up), follow [references/bootstrap-flow.md](references/bootstrap-flow.md) step by step. Three principles:

1. **Never pick the auth method yourself** — always ask the user to choose between AK/SK and environment variables.
2. **If `auth whoami` shows `auth_type=external`, the user is on an externally-managed credential provider — do NOT modify the auth config.** Treat the bootstrap as already done. Only `project`/`endpoint`/`schema` are safe to change (via `session set` or by re-running `auth login-external` with the same `--process-command`).

## First Pass

1. Prefer `{{cli}} ...`<!-- @if cli_module_differs -->; use `{{cli_module}} ...` if not on `PATH`<!-- @endif -->. If the machine may not be bootstrapped, read [references/setup-install.md](references/setup-install.md) first.
2. Run `{{cli}} auth whoami --json`. Check `data.identity`:
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
- `data.rows_written` / `data.applied_partition` / `data.blocks` (data upload)
- `data.rows_written` / `data.output_path` / `data.truncated` (data download)
- `metadata.job_id` (async)

`--json` stdout is one final envelope. Exception: `job wait --stream` emits NDJSON events.

See [references/json-output-format.md](references/json-output-format.md) for full examples and [references/command-patterns.md](references/command-patterns.md) §JSON Contract for all data shapes.

## Dev vs Production Projects

A single **DataWorks workspace** corresponds to **two MaxCompute projects**: a dev project and a production project. Confusing the two is the #1 source of permission errors.

### The naming pair (memorize this)

| Project type   | Name pattern | Example          | Who can access | What lives there |
|----------------|--------------|------------------|----------------|------------------|
| **Dev**        | `<name>_dev` | `my_project_dev` | Personal accounts (the user themselves) | Test data, scratch tables, the user's own work |
| **Production** | `<name>`     | `my_project`     | Usually only service accounts / DataWorks pipelines | The real business data the user actually wants to query |

Both projects belong to the same DataWorks workspace (`my_project`). They **share metadata structure but hold different data**. A table that exists in `my_project_dev` almost always exists in `my_project` too — but the rows, partitions, and freshness will differ.

### Other key facts

- The project configured in `~/.maxc/config.yaml` or env vars is always the **dev project** — this is the user's home project.
- Personal accounts usually only have *write* access to dev and *read* access to production (varies by org policy). Pointing a session directly at the production project often results in `PERMISSION_DENIED`.
- `--project` is the canonical way to access **another project's** tables — most often the corresponding production project, occasionally a different team's project.
- When the user asks about a table without naming the project, **ask whether they mean the dev or production copy** before guessing.

### How to tell which project you are in

```bash
{{cli}} auth whoami --json    # check data.identity.project — ends with _dev?
{{cli}} session show --json   # check current session project
```

If the project name does NOT end with `_dev`, you may be pointed at the production project by mistake.

### Accessing production tables from dev project

Use `--project` to read metadata from the production project without switching session:

```bash
{{cli}} meta list-tables --project my_project --json
{{cli}} meta describe my_table --project my_project --json
{{cli}} data sample my_table --project my_project --json
```

When writing SQL, use `project.table` format to reference tables in another project:

```sql
-- From dev project, query a production table
SELECT * FROM my_project.my_table WHERE ds = '20260418' LIMIT 100
```

Do NOT use bare table names (`FROM my_table`) when the target table lives in a different project — the query will fail with `TABLE_NOT_FOUND`.

### Common permission error scenarios

| Scenario | Symptom | Fix |
|----------|---------|-----|
| Config points to production project | `PERMISSION_DENIED` on most operations | `{{cli}} session set --project my_project_dev` |
| Need to read production table metadata | `PERMISSION_DENIED` on `meta describe` | `{{cli}} meta describe my_table --project my_project --json` |
| SQL references a table in another project without project prefix | `TABLE_NOT_FOUND` | Use `project.table` format in SQL |
| Mixed access: dev metadata + production data | Confusing results | Be explicit: use `--project` for metadata commands, `project.table` in SQL |

## NL2SQL Workflow

Standard flow for answering data questions:

```
0. Plan the query                          → read text2sql-principles.md (intent, granularity, JOIN, output)
1. meta list-tables --schema <s> --json    → get table names + schema_name
2. meta describe <schema.table> --json     → get ALL columns (--json returns full schema)
3. query cost "SELECT ..." --json          → estimate cost (skip for simple queries)
4. query "SELECT ..." --json               → execute query
5. On failure                              → look up error.code in sql-common-errors.md
```

Add `--project <p>` to any step when working with a non-default project.

**Critical rules:**

- Always use schema-qualified table names in SQL: `<schema>.<table>` (e.g. `california_schools.frpm`), not bare table names. The `list-tables` output includes `schema_name` for each table.
- `meta describe --json` returns **all columns** automatically. Without `--json`, use `--full` to avoid truncation.
- Column names with spaces or special characters must be backtick-escaped: `` `column name` ``.
- When filtering by column values, first check actual values with `data sample` or a `SELECT DISTINCT` query — don't guess enum values.
- For partitioned tables, always filter by partition column in WHERE (e.g. `WHERE ds = '20260415'`) to avoid full-table scans.
- When accessing tables from another project, use `project.table` format in SQL (see Dev vs Production Workspaces).

## SQL Generation

For writing or migrating MaxCompute SQL, route to the right reference:

| I want to... | Read |
|---|---|
| Plan an NL→SQL conversion (intent, granularity, JOIN, output contract) | [references/text2sql-principles.md](references/text2sql-principles.md) |
| Write/migrate MaxCompute DQL (dialect rules, function mapping, type traps, SET parameters) | [references/maxcompute-select-guide.md](references/maxcompute-select-guide.md) |
| Find a query template (Top-N per group, PIVOT, retention, YoY/MoM, EXISTS rewrite, …) | [references/sql-query-patterns.md](references/sql-query-patterns.md) |
| Recover from a failed `{{cli}} query` (ODPS-0xxx error codes) | [references/sql-common-errors.md](references/sql-common-errors.md) |

## Partition Strategy

Before querying a partitioned table, always determine the correct partition value:

```
1. {{cli}} meta describe <table> --json  → check partition_columns
   - No partitions? → Query directly with LIMIT
   - Has partitions? → Continue to step 2

2. {{cli}} meta latest-partition <table> --json  → get latest value and format
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
| Check who I am and where I'm pointed | `{{cli}} auth whoami --json` |
| Set up auth from scratch | See Bootstrap Flow above |
| Switch project/schema for this session | `{{cli}} session set --project P --schema S --json` |
| List tables | `{{cli}} meta list-tables --schema S --json` |
| Get full schema of a table | `{{cli}} meta describe T --json` |
| Find tables by keyword | `{{cli}} meta search KW --json` |
| Find columns by keyword | `{{cli}} meta search-columns KW --json` |
| Get latest partition value | `{{cli}} meta latest-partition T --json` |
| Sample data | `{{cli}} data sample T --rows 10 --json` |
| Upload a CSV into an existing table | `{{cli}} data upload T --file path.csv [--partition ds=...] [--overwrite] --json` |
| Download a table/partition to CSV | `{{cli}} data download T --output path.csv [--partition ds=...] [--columns a,b] [--limit N] --json` |
| Run a query | `{{cli}} query "SELECT ..." --json` |
| Estimate cost first | `{{cli}} query cost "SELECT ..." --json` |
| Run a long query async | `{{cli}} query "..." --wait 0 --json`, then `{{cli}} job wait <id> --json` |
| Auto-abort if too costly | `{{cli}} query "..." --cost-check 10.0 --json` |
| Read another project's tables | Add `--project P` to any meta/data command; use `project.table` in SQL |
| Check permission for an op | `{{cli}} auth can-i --table T --operation SELECT --json` |
| Diagnose a failed job | `{{cli}} job diagnose <id> --json` |
| Add semantic metadata to a table | `{{cli}} meta semantic set T ... --json` (see command-patterns.md §Semantic Metadata) |
| Plan NL→SQL before writing | See [references/text2sql-principles.md](references/text2sql-principles.md) |
| Look up MaxCompute SQL dialect rule | See [references/maxcompute-select-guide.md](references/maxcompute-select-guide.md) |
| Pick a query template (Top-N, PIVOT, retention, …) | See [references/sql-query-patterns.md](references/sql-query-patterns.md) |
| Look up an `ODPS-0xxx` error code | See [references/sql-common-errors.md](references/sql-common-errors.md) |

## Capability Boundaries

| Boundary | Detail | Alternative |
|----------|--------|-------------|
| Read-only enforcement (SQL only) | Client-side SQL keyword detection blocks DDL/DML before reaching MaxCompute. **Does not gate `data upload`** — that uses the Tunnel API, which is a write path by design. | For SQL-side writes use odpscmd, pyodps SDK, or DataWorks |
| No permission management | `auth can-i` checks one table+operation; cannot enumerate accessible tables | MaxCompute console or project admin tools |
| No complete permission inventory | Cannot iterate projects to discover all readable tables | Ask user for target project/table |
| CSV upload/download is single-thread Tunnel | `data upload` / `data download` round-trip CSV/TSV via PyODPS Tunnel; primitive types only (no array/map/struct); fail-fast on bad rows; target table must exist | For very large or parallel transfers, use `odpscmd tunnel` with multiple threads |
| No lineage queries | Lineage is not exposed by this CLI; any lineage-shaped output returns a `supported: false` placeholder | Use DataWorks lineage |
| No resource/UDF management | No upload/registration | Use odpscmd or DataWorks |

## Known Limitations

| Feature | Status | Detail |
|---------|--------|--------|
| `meta search` | Catalog API preferred | Server-side FTS via pyodps RestClient (auto-routed); falls back to substring match |
| `list-tables` pagination | Not implemented | CLI-side `--cursor` is offset token, not server-side cursor |
| `auth login` | Plaintext YAML | AccessKey stored in `~/.maxc/config.yaml` (file permissions 0600) |
| Write operations | Client-side read-only | Write operations blocked by CLI before submission; `--force` bypasses |

## References Index

| File | When to read |
|------|--------------|
| [bootstrap-flow.md](references/bootstrap-flow.md) | First-time setup or `configured=false` |
| [setup-install.md](references/setup-install.md) | Python / maxc-cli install detail |
| [bootstrap-auth.md](references/bootstrap-auth.md) | Per-method auth setup (AK/SK, env vars) |
| [command-patterns.md](references/command-patterns.md) | Full command syntax, output shapes, semantic, multi-project, schema, async |
| [json-output-format.md](references/json-output-format.md) | JSON envelope examples |
| [partition-guide.md](references/partition-guide.md) | Partition naming, MAX_PT() guidance, ambiguity |
| [maxcompute-sql-notes.md](references/maxcompute-sql-notes.md) | CLI-side SQL knobs (SET injection, `--force` write-block, `--max-rows`, upload semantics) |
| [text2sql-principles.md](references/text2sql-principles.md) | Engine-agnostic NL→SQL planning (intent, granularity, JOIN, output contract) |
| [maxcompute-select-guide.md](references/maxcompute-select-guide.md) | Authoritative MaxCompute DQL dialect rules — patterns that error, function mapping, type traps, DQL extensions, SET parameters |
| [sql-query-patterns.md](references/sql-query-patterns.md) | Ready-made SELECT templates — Top-N per group, dedup latest, cumulative, YoY/MoM, retention, PIVOT/UNPIVOT, JSON, GROUPING SETS, pagination |
| [sql-common-errors.md](references/sql-common-errors.md) | ODPS-0xxx error code recovery handbook — wrapper codes, OOM diagnosis, MAPJOIN exceptions, MCQA limits |
| [red-lines.md](references/red-lines.md) | What not to do; common mistakes; anti-patterns; error code → recovery; symptom troubleshooting |
