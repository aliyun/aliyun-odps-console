---
name: alibabacloud-maxcompute-cli
description: Inspect, query, troubleshoot, and transfer MaxCompute or ODPS data. Use when a user wants to discover metadata, generate or run SQL, inspect authentication, sessions, partitions, jobs, or permissions, diagnose access or query issues, or move table data with the Alibaba Cloud CLI.
---

# Alibaba Cloud MaxCompute CLI

Use Alibaba Cloud CLI to work with MaxCompute. In the public-cloud distribution,
`aliyun maxc` provides MaxCompute data-plane operations for metadata, SQL, jobs,
permissions, and data transfer. Run commands through the configured invocation:
`{{cli}} ...`<!-- @if cli_module_differs -->, or `{{cli_module}} ...` when the
standalone console script is unavailable<!-- @endif -->.

## When To Use

Use this Skill when the user wants to:

- find a project, schema, table, column, or partition;
- inspect or profile table data;
- compose, cost, explain, run, or troubleshoot MaxCompute SQL, including an exact DDL/DML statement explicitly authorized by the user;
- submit, wait for, inspect, diagnose, or cancel a MaxCompute job;
- check a MaxCompute permission;
- upload CSV or TSV data to an existing table, or download table data;
- configure, diagnose, or explicitly remove saved MaxCompute authentication.

Do not use this Skill to implement `maxc-cli` itself or when the user explicitly
asks for PyODPS or SDK code.

## CLI Version And Upgrade

The preferred public-cloud entry is `aliyun maxc`.

1. Run `aliyun version` before using that entry.
2. Alibaba Cloud CLI must be version **3.3.19 or later** because that release
   introduced `aliyun maxc`.
3. If it is older, obtain confirmation before changing the environment. A
   non-Homebrew Alibaba Cloud CLI 3.3.5+ can use `aliyun upgrade`; update an
   earlier, Homebrew, or missing installation through its official installation
   method. Then verify the version again.

For the standalone Python entry, Python 3.9 or later is required. Install or
upgrade only when the user authorizes changing the Python environment:

```bash
python3 -m pip install --upgrade maxc-cli
```

Read [setup-install.md](references/setup-install.md) only when installation or
PATH repair is actually needed.

## Observability

Generate one `session-id` at the start of the Agent session and reuse it for
every MaxCompute cloud-API call in that session. It must be exactly 32
lowercase hex characters. Do not create a new value for every command.

Append this User-Agent to every `{{cli}}` invocation that calls a cloud API:

```text
--user-agent AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/<session-id>
```

Example setup:

```bash
SESSION_ID="$(openssl rand -hex 16)"
UA="AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/${SESSION_ID}"
{{cli}} agent context --json
```

Never put credentials, project data, SQL text, or user identifiers in the
session ID or User-Agent.

Local commands such as help, `agent context`, `agent manifest`, `session show`,
and `cache status` do not require the option.

## Preflight

Do not guess the command surface. The live CLI is authoritative.

```bash
{{cli}} agent context --json
{{cli}} agent manifest --json
{{cli}} agent doctor --online --user-agent "$UA" --json
```

- `agent context` is local-only. Check `version`, `min_cli_version`,
  `auth_status`, project/schema context, and `network_checked=false`.
- `agent manifest` is generated from the live parser and lists commands,
  arguments, auth/network requirements, and side effects.
- `agent doctor --online` performs the live identity check. Continue with data
  operations only when `data.ready=true`.
- If the manifest is unavailable on an older CLI, use the relevant `--help`
  output and upgrade before relying on a missing command.

## Authentication

Prefer OAuth for public-cloud interactive login. If Alibaba Cloud CLI already
has a usable profile, verify it without reconfiguring credentials:

```bash
{{cli}} auth whoami --user-agent "$UA" --json
```

If authentication is not configured:

```bash
{{cli}} auth login --oauth --user-agent "$UA" --json
```

This starts Authorization Code + PKCE on the CLI host and listens for the
callback on `127.0.0.1`. `--no-browser` only suppresses automatic browser
opening; it still uses the same loopback callback. When the CLI runs over SSH,
use port forwarding so the callback reaches that host, or open the URL in a
browser running on that same host. It is not a device-code or headless flow.

Follow the returned `agent_hints.actions` when project selection is pending.
Use AK/SK, STS, environment variables, or `auth login-external` only when the
user or runtime specifically requires that method. Never ask the user to paste
a secret into chat, and never echo credentials in commands, logs, or errors.

Read [bootstrap-auth.md](references/bootstrap-auth.md) only for non-OAuth setup
or authentication troubleshooting.

## Response Contract

Use `--json` for machine-driven work. `job wait --stream` is the only standard
exception; it emits buffered NDJSON lifecycle events after the wait completes,
not a live server stream.

For each JSON envelope:

1. Check the envelope's top-level `status`: `success`, `pending`, or `failure`.
   Job and cache lifecycle states belong in their documented nested `data`
   fields or stream events; do not confuse them with the envelope status, and
   stop on an unknown top-level value.
2. On `failure`, read `error.code`, `error.suggestion`, and
   `error.recovery_steps` before changing the request.
3. On every status, inspect `agent_hints.warnings`.
4. Treat structured `agent_hints.actions` as authoritative. `action_ids`
   identifies every structured action. Legacy `next_actions` contains only
   actions that are executable, agent-allowed, and do not require confirmation.
5. Before running an action, check `executable`, `agent_allowed`,
   `confirmation_required`, and `effect`. Resolve placeholders from verified
   user or command output, and obtain authorization appropriate to the effect.

Important data paths:

| Command | Path |
|---|---|
| query / successful job wait / job result | `data.result.rows`, `data.pagination` |
| query cost / explain | `data.analysis` |
| auth whoami | `data.identity` |
| auth can-i | `data.authorization` |
| meta describe | `data.table` |
| data sample / profile | `data.sample` / `data.profile` |
| async submission | `metadata.job_id` |

Read [json-output-format.md](references/json-output-format.md) for worked
examples only when these paths are insufficient.

## Safe Operating Rules

1. Treat project, schema, table, column, partition, quota, and endpoint names as
   opaque. Never infer an environment or related project from a suffix.
2. Verify the current identity and project before remote operations. If the
   target is ambiguous, ask the user rather than choosing a project.
3. Detect the namespace model with `meta list-schemas`. Use `schema.table` only
   when a three-tier project or the returned metadata requires it; do not force
   a schema onto a two-tier project.
4. Run `meta describe` before generating SQL. Never invent columns or enum
   values; sample or query distinct values when needed.
5. For partitioned tables, inspect `meta partitions` or
   `meta latest-partition`, then include an explicit partition filter.
6. Cost-check broad or unfamiliar queries before execution.
7. For DDL/DML, require an explicit user request and verify the exact statement,
   project, schema, target, and effect. Submit one statement at a time with
   `--force`; never infer a write from a read request, combine it with another
   statement, or replay a suggested write action automatically. The CLI
   positive allowlist accepts recognized data-plane mutations; permission,
   account, project, system, resource, package, and unknown administrative SQL
   remain blocked and require a dedicated approved workflow. A leading `SET`
   is part of the authorized execution context, not a second authorization
   channel: project-security and masking controls are always blocked, and a
   forced mutation accepts only audited statement-local execution hints.
   `data upload`,
   `data download --overwrite`, and `job cancel` are separate mutations and
   require authorization appropriate to their effect.
8. A failed command is not permission to retry indefinitely. Apply the
   suggested recovery once, then stop or ask when the target or authority is
   still unclear.
9. Treat signed LogView URLs and their tokens as credentials. Do not copy them
   into artifacts, logs, or final answers; retain only sanitized request or job
   identifiers needed for diagnosis.

## Common Workflows

### Change The Default Project Or Schema

When the current identity is already authenticated, keep its authentication
provider unchanged. For a persistent project or schema preference, verify the
target and update only the session defaults:

```bash
{{cli}} meta list-projects --user-agent "$UA" --json
{{cli}} session set --project <verified-project> --json
{{cli}} session show --json
```

Add `--schema <verified-schema>` only when the project uses the three-tier
namespace and metadata confirms that schema. `session set` does not change the
credential provider or endpoint. For a one-off operation, prefer that
command's `--project` flag instead of changing the persisted default.

### Discover And Query

```bash
{{cli}} meta search <keyword> --user-agent "$UA" --json
{{cli}} meta describe <table> --user-agent "$UA" --json
{{cli}} meta latest-partition <table> --user-agent "$UA" --json
{{cli}} query cost "SELECT ... WHERE <partition_filter>" --user-agent "$UA" --json
{{cli}} query "SELECT ... WHERE <partition_filter>" --user-agent "$UA" --json
```

Use `meta list-projects`, `meta list-schemas`, or `meta list-tables` when a
search result does not establish the target. Add `--project` and `--schema`
only with values verified from the user, context, or prior command output.

### Sample Or Profile

```bash
{{cli}} data sample <table> --rows 10 --user-agent "$UA" --json
{{cli}} data profile <table> --partition <spec> --user-agent "$UA" --json
```

### Async Query

```bash
{{cli}} query "SELECT ..." --wait 0 --user-agent "$UA" --json
{{cli}} job wait <job-id> --user-agent "$UA" --json
{{cli}} job diagnose <job-id> --user-agent "$UA" --json
```

Treat `metadata.job_id` as opaque, including MCQA composite IDs.
When successful `job wait` already returns `data.result`, consume it directly.
Use `job result` only when the completed wait lacks the requested result,
another page is needed, or output must be written separately.

### Permissions

```bash
{{cli}} auth can-i --table <table> --operation Select --project <project> --user-agent "$UA" --json
{{cli}} auth can-i --object <schema> --type Schema --operation Describe --project <project> --user-agent "$UA" --json
```

`allowed=false` is a successful permission check, not a CLI execution failure.

### Data Transfer

```bash
# Validate an upload before creating a Tunnel write session.
{{cli}} data upload <table> --file <path.csv> --dry-run --user-agent "$UA" --json

# Write only after the user authorizes it. --overwrite replaces table/partition data.
{{cli}} data upload <table> --file <path.csv> --partition <spec> --user-agent "$UA" --json

# A missing partition is created only with this explicit metadata mutation.
{{cli}} data upload <table> --file <path.csv> --partition <spec> --create-partition --user-agent "$UA" --json

# Existing local files are protected unless --overwrite is explicitly supplied.
{{cli}} data download <table> --output <path.csv> --partition <spec> --user-agent "$UA" --json
```

An explicitly requested dry-run is validation, not authorization for the write
action it may return. Never replay an upload action unless it is executable,
agent-allowed, confirmation-free, and independently authorized for the exact
target and effect.

Upload/download supports primitive columns through Tunnel. The target table
must already exist. Ordinary upload never creates a missing partition.
`--create-partition` is a separate, explicit metadata side effect; a later
upload failure can leave the newly created partition empty. Read
[command-patterns.md](references/command-patterns.md) for delimiter, header,
NULL, column, block, partition-creation, and overwrite details.

## SQL And Partition Guidance

For substantial SQL generation, load only the reference matching the task:

- [text2sql-principles.md](references/text2sql-principles.md): intent,
  granularity, joins, and output contract.
- [maxcompute-select-guide.md](references/maxcompute-select-guide.md):
  MaxCompute DQL dialect and type/function differences.
- [sql-query-patterns.md](references/sql-query-patterns.md): reusable query
  patterns.
- [partition-guide.md](references/partition-guide.md): multi-level partitions
  and freshness.
- [sql-common-errors.md](references/sql-common-errors.md): SQL error recovery.

## Capability Boundaries

- The CLI does not grant permissions or enumerate a complete permission graph.
- It does not provide lineage, resource artifact upload, dedicated UDF
  lifecycle commands, or an active mock data backend. A supported, exact SQL
  function DDL remains subject to the one-statement `--force` boundary.
- Tunnel upload/download is single-process and primitive-type oriented; use a
  dedicated bulk-transfer tool for very large parallel transfers.
- `agent context` does not prove network reachability; use
  `agent doctor --online` or `auth whoami`.

## References

- [command-patterns.md](references/command-patterns.md): exact advanced flags
  and workflows.
- [red-lines.md](references/red-lines.md): mutation boundaries and recovery.
- [setup-install.md](references/setup-install.md): installation and PATH repair.
- [bootstrap-flow.md](references/bootstrap-flow.md): first-time setup routing.
- [bootstrap-auth.md](references/bootstrap-auth.md): non-OAuth auth methods and
  troubleshooting.
