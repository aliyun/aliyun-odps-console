# MaxC Command Patterns

This is the comprehensive command reference. For principles and what-not-to-do, see [red-lines.md](red-lines.md). For high-level workflow guidance, see [../SKILL.md](../SKILL.md).

## Preflight

Use these first when the environment or command surface is unclear:

```bash
python3 --version
python3 -m maxc_cli --help
maxc --help
maxc query --help
maxc auth whoami --json
```

If Python or `maxc` are missing, read [setup-install.md](setup-install.md) before proceeding.
If `maxc` is not on `PATH` but the package is installed, replace `maxc` with `python3 -m maxc_cli`.

## Auth And Session

```bash
maxc auth whoami --json
maxc auth login --access-id "<id>" --secret-access-key "<secret>" --project "<project>" --endpoint "<endpoint>" --json
maxc auth login --access-id "<id>" --secret-access-key "<secret>" --project "<project>" --endpoint "<endpoint>" --no-validate --json
maxc auth login --from-env --json
maxc auth login-external --process-command "<cmd>" --project "<project>" --endpoint "<endpoint>" --json
maxc auth can-i --table your_table --operation SELECT --json
maxc session show --json
maxc session set --project your_project --schema your_schema --json
maxc session unset --json
```

`session set/show/unset` are local-only — no authenticated backend required. Session overrides live in `~/.maxc/session_override.yaml` and take priority over config files and env vars for project/schema only.

Use `auth login` instead of hand-editing `~/.maxc/config.yaml`.

## Metadata And Data Discovery

```bash
maxc cache build --json
maxc meta list-tables --json
maxc meta list-tables --schema my_schema --json
maxc meta list-tables --project other_project --json
maxc meta describe your_table --json
maxc meta describe your_table --full --json
maxc meta search "keyword" --json
maxc meta search-columns "user_id" --json
maxc meta partitions your_table --json
maxc meta latest-partition your_table --json
maxc meta freshness your_table --json
maxc meta list-projects --json
maxc meta list-schemas --project your_project --json
maxc data sample your_table --rows 5 --partition ds=2026-03-20 --columns id,ds --json
maxc data profile your_table --partition ds=2026-03-20 --json
```

- All meta and data commands accept `--project` for one-off cross-project access without switching session.
- Most meta commands support `--schema` to override the session default.
- `cache build` before `meta list-tables` on a cold environment. `meta list-tables` is cache-backed; status is `cache_miss` until metadata has been cached.
- `meta search` uses Catalog API (server-side FTS via pyodps RestClient) when auto-routed; falls back to cache substring match, then live scan.

## Query And Jobs

Preferred query syntax:

```bash
maxc query "SELECT 1 AS one" --json
maxc query cost "SELECT 1 AS one" --json
maxc query explain "SELECT 1 AS one" --json
```

With SET options (parsed and passed as hints to MaxCompute):

```bash
maxc query "SET odps.sql.type.system.odps2=true; SELECT CAST(id AS INT) FROM schema.table LIMIT 10" --json
```

Legacy-compatible syntax still works:

```bash
maxc query "SELECT 1 AS one" --mode cost --json
```

The command is `query`, not `sql`. There is no `maxc sql` command.

### Wait And Timeout

```bash
# Default: wait up to 10 seconds, auto-promote to async if not done
maxc query "SELECT * FROM big_table" --json

# Submit and return immediately (get job_id without waiting)
maxc query "SELECT * FROM big_table" --wait 0 --json

# Wait up to 60 seconds before promoting
maxc query "SELECT * FROM big_table" --wait 60 --json
```

- `query --wait N`: polls for up to N seconds. If the job finishes within N seconds, returns the result. Otherwise auto-promotes to async and returns `status=pending` with a `job_id`.
- `query --wait 0`: submits and returns immediately with `status=pending` and `job_id`.
- `job wait <id> --timeout N`: waits up to N seconds for completion. Returns `status=pending` if timeout reached.
- Default `--wait` for `query` is 10 seconds. Default `--timeout` for `job wait` is 300 seconds.

Async pattern for long queries:

```bash
# Step 1: submit
maxc query "SELECT * FROM my_schema.big_table WHERE ds = '20260418'" --wait 0 --json
# Returns: { "status": "pending", "metadata": { "job_id": "<job_id>" } }

# Step 2: extract metadata.job_id (e.g. 2026042011_abc123) and wait
maxc job wait <job_id> --json
# If still pending, retry with longer timeout:
maxc job wait <job_id> --timeout 600 --json
```

### Cost Control And Pagination

```bash
# Estimate cost before running
maxc query cost "SELECT * FROM big_table" --json

# Auto-abort if estimated cost exceeds threshold (in CU)
maxc query "SELECT * FROM big_table" --cost-check 10.0 --json

# Dry-run: see plan without execution
maxc query "SELECT * FROM big_table" --dry-run --json

# Pagination
maxc query "SELECT * FROM your_table LIMIT 20" --page-size 20 --json
maxc query "SELECT * FROM your_table LIMIT 20" --page-size 20 --cursor "<cursor>" --json
maxc query "SELECT * FROM your_table" --output /tmp/results.json --json
```

`agent context --json` includes `cost_threshold_cu` (project-level default) and `allowed_operations` — respect these guardrails.

### Async Jobs

```bash
maxc job submit "SELECT * FROM your_table" --json
maxc job status <job_id> --json
maxc job wait <job_id> --json
maxc job wait <job_id> --timeout 600 --json
maxc job wait <job_id> --stream
maxc job result <job_id> --json
maxc job result <job_id> --max-rows 50 --cursor "<cursor>" --json
maxc job diagnose <job_id> --json
maxc job cancel <job_id> --json
maxc job list --json
maxc job list --limit 50 --json
```

Use `job wait --stream` only when you want NDJSON events instead of the normal JSON envelope.

## Multi-Project Access

All meta and data commands accept `--project` for one-off cross-project access without switching session:

```bash
maxc meta list-tables --project other_project --json
maxc meta describe default.my_table --project other_project --json
maxc data sample my_table --project other_project --json
```

Use `session set --project` when you need to stay in that project for multiple commands:

```bash
maxc session set --project other_project --json
maxc session set --project other_project --schema my_schema --json
maxc session show --json
maxc cache build --json
maxc session unset --json
```

When writing SQL that references tables in another project, use `project.table` format — see SKILL.md §Dev vs Production Workspaces.

## Schema Operations (3-Tier vs 2-Tier)

Some MaxCompute projects use **3-tier namespace** (`project.schema.table`); others use **2-tier** (`project.table` only). Detect at runtime: run `maxc meta list-schemas --json` — if it returns an error or empty result, the project is 2-tier and you should skip the schema layer entirely.

For 3-tier projects:

```bash
maxc meta list-schemas --json
maxc meta list-schemas --project other_project --json

# List tables in a specific schema (one-shot vs sticky)
maxc meta list-tables --schema california_schools --json
maxc session set --schema california_schools --json

# Search within a schema
maxc meta search school --schema california_schools --json
maxc meta search-columns county --schema california_schools --json

# Build cache for a specific schema
maxc cache build --schema california_schools --json

# Describe (use schema.table format)
maxc meta describe california_schools.frpm --json

# Reset
maxc session unset --json
```

When `--schema` is given, it overrides `session set --schema`. When neither is set, the project default schema is used.

## Cache

`cache build` stores table metadata in a local SQLite DB (`~/.maxc/cache/cache.db`) to accelerate `list-tables`, `search`, `search-columns`, and `describe`. Subsequent meta commands read from cache first; on cache miss, falls back to live backend queries.

- **Cache key**: `(project, schema_name, table_name)` — schema is part of the key, so different schemas have independent caches.
- **When to rebuild**: after schema changes, new tables, or when `cache status` shows it stale.

```bash
maxc cache status --json                               # check freshness
maxc cache build --json                                # build for current project/schema
maxc cache build --schema my_schema --json             # build for specific schema
maxc cache build --async --json                        # async build, returns build_id
maxc cache build-status --build-id <build_id> --json   # poll async build
maxc cache clear --json                                # wipe (forces full rebuild)
```

`cache build --json` prints progress to `stderr` and writes one final JSON envelope to `stdout`.

## Semantic Metadata

Semantic metadata enriches tables with business context for NL2SQL and agent discovery. When `meta describe` warns about missing semantic metadata, the agent should generate it from its own LLM understanding of the table schema and save it with `meta semantic set`.

```bash
maxc meta semantic list-missing --json

maxc meta semantic set my_table \
  --desc "Daily user login events" \
  --use-cases "login funnel analysis" "DAU calculation" \
  --sample-questions "How many users logged in yesterday?" \
  --column-semantics '[{"name":"user_id","semantic_type":"user_identifier"}]' \
  --json

maxc meta semantic get my_table --json

# Verify in describe output (semantic section appears when metadata exists)
maxc meta describe my_table --json
```

## Diffs

Compare tables across environments or track schema changes:

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

`diff data` is keyed snapshot compare, not exhaustive diff.

## Agent Commands And Skill Registration

```bash
# Show environment context (auth, backend, capabilities)
maxc agent context --json

# Show SKILL.md path and metadata
maxc agent skill --json

# Register skill to an Agent platform
maxc agent install-skill --json              # Claude Code (default)
maxc agent install-skill cursor --json       # Cursor
maxc agent install-skill windsurf --json     # Windsurf
maxc agent install-skill codex --json        # OpenAI Codex
maxc agent install-skill qwen --json         # Qwen
maxc agent install-skill qoder --json        # Qoder
maxc agent install-skill qoderwork --json    # QoderWork
```

`agent context` is a fast configuration summary. It does not enumerate tables and does not require authenticated backend startup. Use as a preflight before any data operation.

`agent install-skill` copies SKILL.md and references from the installed package into the platform's skill directory. Idempotent: re-running at the same version skips the copy. After `pip install --upgrade maxc-cli`, re-run to update the local skill files.

## JSON Contract

Most `--json` commands return an envelope shaped like:

- `version`
- `command`
- `command_id`
- `status`
- normalized `data`
- `metadata`
- `error`
- `agent_hints`

Important normalized `data` shapes:

| Command | Path |
|---------|------|
| `query` / `job wait` / `job result` | `data.result` and `data.pagination` |
| `query cost` / `query explain` | `data.analysis` |
| `auth whoami` | `data.identity` and optional `data.auth_options` |
| `auth can-i` | `data.authorization` |
| `meta describe` | `data.table` |
| `meta search` / `meta search-columns` | `data.search.matches` |
| `data sample` | `data.sample` |
| `data profile` | `data.profile` |
| `job status` / `job cancel` | `data.job` |
| `job diagnose` | `data.diagnosis` |
| `agent context` | `data.context` |

`cache status`, `cache build`, `cache build-status`, `cache clear`, and `session *` currently return their native top-level `data` payloads without an extra wrapper.

`agent_hints` includes:

- `actions`: structured action objects with `id`, `title`, `command`, `executable`, `placeholders`
- `action_ids`: list of `actions[].id` values (stable identifiers for program logic)
- `next_actions`: list of `actions[].command` values (hint only — may have shell quoting issues with special characters in SQL)
- `warnings`: actionable alerts
- `insights`: contextual notes about the result

Use `action_ids` when you want stable program logic. Use `next_actions` as hints only — if a command contains single quotes or special characters, construct the command yourself from `actions[].id` and context.

See [json-output-format.md](json-output-format.md) for end-to-end envelope examples.

## Error Handling Patterns

For the full error code → recovery table, see [red-lines.md](red-lines.md) §Error Code → Recovery.

### Checking error responses

```bash
result=$(maxc query "SELECT * FROM missing_table" --json 2>/dev/null)
status=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))")

if [ "$status" = "failure" ]; then
  error_code=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('error',{}).get('code',''))")
  # Handle specific error codes
fi
```

### Common error → recovery flows

```bash
# NOT_FOUND → search for the correct name
maxc meta search "partial_name" --json

# PERMISSION_DENIED → check permissions (often a workspace issue)
maxc auth can-i --table your_table --operation SELECT --json

# JOB_TIMEOUT → check status and continue waiting
maxc job status <job_id> --json
maxc job wait <job_id> --timeout 600 --json

# EXECUTION_FAILED → diagnose the job
maxc job diagnose <job_id> --json

# BACKEND_CONNECTION_ERROR → verify auth is still valid
maxc auth whoami --json

# cache_miss status → build the cache
maxc cache build --json
```

## Gotchas

- There is no active runtime mock backend path. Missing auth does not produce fake table data.
- `auth whoami` performs a remote security `whoami` probe when config exists.
- `cache build --json` prints progress to `stderr` and writes a single final JSON envelope to `stdout`.
- `cache build --async --json` returns a `build_id`; poll with `cache build-status --build-id <build_id> --json`.
- `query cost` and `query explain` cannot be combined with `--async`, `--dry-run`, `--cursor`, `--output`, or `--output-format`. They only support `table` or `json` output.
- `meta list-projects` should lead into `session set --project ... --json` and `meta list-schemas --project ... --json`.
- If `~/.maxc/cache/cache.db` is unwritable or shared across concurrent startup processes, you may still see SQLite failures such as `unable to open database file` or `database is locked`, but current CLI code translates them into structured validation failures.
