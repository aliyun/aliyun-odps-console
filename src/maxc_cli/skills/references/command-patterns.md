# MaxC Command Patterns

This is the comprehensive command reference. For principles and what-not-to-do, see [red-lines.md](red-lines.md). For high-level workflow guidance, see [../SKILL.md](../SKILL.md).

## Preflight

Use these first when the environment or command surface is unclear:

```bash
python3 --version
{{cli}} --help
{{cli}} query --help
{{cli}} auth whoami --json
```

If Python or `{{cli}}` are missing, read [setup-install.md](setup-install.md) before proceeding.
<!-- @if cli_module_differs -->
If `{{cli}}` is not on `PATH` but the package is installed, replace `{{cli}}` with `{{cli_module}}`.
<!-- @endif -->

## Auth And Session

```bash
{{cli}} auth whoami --json
{{cli}} auth login --access-id "<id>" --secret-access-key "<secret>" --project "<project>" --endpoint "<endpoint>" --json
{{cli}} auth login --access-id "<id>" --secret-access-key "<secret>" --project "<project>" --endpoint "<endpoint>" --no-validate --json
{{cli}} auth login --from-env --json
{{cli}} auth login-external --process-command "<cmd>" --project "<project>" --endpoint "<endpoint>" --json
{{cli}} auth can-i --table your_table --operation SELECT --json
{{cli}} session show --json
{{cli}} session set --project your_project --schema your_schema --json
{{cli}} session unset --json
```

`session set/show/unset` are local-only — no authenticated backend required. They edit `default_project` / `default_schema` in `~/.maxc/config.yaml` directly; project-local cwd configs can still shadow these (and `session set` warns when they do).

Use `auth login` instead of hand-editing `~/.maxc/config.yaml`.

## Metadata And Data Discovery

```bash
{{cli}} meta list-tables --json
{{cli}} meta list-tables --schema my_schema --json
{{cli}} meta list-tables --project other_project --json
{{cli}} meta describe your_table --json
{{cli}} meta describe your_table --full --json
{{cli}} meta search "keyword" --json
{{cli}} meta search-columns "user_id" --json
{{cli}} meta partitions your_table --json
{{cli}} meta latest-partition your_table --json
{{cli}} meta freshness your_table --json
{{cli}} meta list-projects --json
{{cli}} meta list-schemas --project your_project --json
{{cli}} data sample your_table --rows 5 --partition ds=2026-03-20 --columns id,ds --json
{{cli}} data profile your_table --partition ds=2026-03-20 --json
{{cli}} data upload your_table --file ./rows.csv --partition ds=2026-03-20 --overwrite --json
{{cli}} data download your_table --output ./out.csv --partition ds=2026-03-20 --columns id,name --limit 1000 --json
```

- All meta and data commands accept `--project` for one-off cross-project access without switching session.
- Most meta commands support `--schema` to override the session default.

### Bulk CSV Upload / Download

`data upload` and `data download` move CSV/TSV between local files and an existing table or partition via the PyODPS Tunnel API (no SQL CU consumed).

Rules:

- **Target table must already exist.** No auto-create. If the table is missing, `NOT_FOUND` — create it via `{{cli}} query "CREATE TABLE ..." --force --json` first.
- **Partitioned table requires `--partition`.** Spec must list every partition key with no extras (e.g. `ds=20260509,hh=12` — not `ds=20260509` alone, not `wrong=1`). Wrong keys → `VALIDATION_ERROR` up front, no Tunnel session opened.
- **Default semantics is append.** Pass `--overwrite` for INSERT-OVERWRITE-style replacement of the partition (or whole non-partitioned table). Without `--overwrite`, rows are added.
- **Fail-fast on bad rows.** Any row that fails to parse against the column type aborts the Tunnel session; nothing is committed. The error envelope's `error.context` gives `line` and `column`.
- **Primitive types only** (bigint/int/double/decimal/boolean/string/varchar/char/date/datetime/timestamp). `array`/`map`/`struct` columns → `VALIDATION_ERROR` before opening the session. Use `INSERT ... SELECT` for those.
- **CSV defaults**: `,` delimiter, header row required (use `--no-header` for ordinal mapping), `\N` as NULL on upload / empty cell as NULL on download (override with `--null-marker`). UTF-8 encoding only.
- **Download requires `--partition` for partitioned tables**, same as upload. Use `--limit N` to cap rows; `data.truncated=true` plus a warning surface in the envelope when the limit was hit.
- **`data sample` is still preferred for inline JSON inspection** of a few rows. Use `data download` only when you need a CSV file on disk.

Examples:

```bash
# Upload (append) a CSV into a non-partitioned table
{{cli}} data upload my_table --file ./rows.csv --json

# Overwrite a partition
{{cli}} data upload my_part_table --file ./rows.csv --partition ds=20260509 --overwrite --json

# TSV upload
{{cli}} data upload my_table --file ./rows.tsv --delimiter $'\t' --json

# Download a column subset, capped at 10000 rows
{{cli}} data download my_part_table --output ./out.csv --partition ds=20260509 --columns id,name --limit 10000 --json
```
- `meta search` uses Catalog API (server-side FTS via pyodps RestClient) when auto-routed; falls back to substring match, then live scan.

## Query And Jobs

Preferred query syntax:

```bash
{{cli}} query "SELECT 1 AS one" --json
{{cli}} query cost "SELECT 1 AS one" --json
{{cli}} query explain "SELECT 1 AS one" --json
```

With SET options (parsed and passed as hints to MaxCompute):

```bash
{{cli}} query "SET odps.sql.type.system.odps2=true; SELECT CAST(id AS INT) FROM schema.table LIMIT 10" --json
```

Legacy-compatible syntax still works:

```bash
{{cli}} query "SELECT 1 AS one" --mode cost --json
```

The command is `query`, not `sql`. There is no `{{cli}} sql` command.

### Wait And Timeout

```bash
# Default: wait up to 10 seconds, auto-promote to async if not done
{{cli}} query "SELECT * FROM big_table" --json

# Submit and return immediately (get job_id without waiting)
{{cli}} query "SELECT * FROM big_table" --wait 0 --json

# Wait up to 60 seconds before promoting
{{cli}} query "SELECT * FROM big_table" --wait 60 --json
```

- `query --wait N`: polls for up to N seconds. If the job finishes within N seconds, returns the result. Otherwise auto-promotes to async and returns `status=pending` with a `job_id`.
- `query --wait 0`: submits and returns immediately with `status=pending` and `job_id`.
- `job wait <id> --timeout N`: waits up to N seconds for completion. Returns `status=pending` if timeout reached.
- Default `--wait` for `query` is 10 seconds. Default `--timeout` for `job wait` is 300 seconds.

Async pattern for long queries:

```bash
# Step 1: submit
{{cli}} query "SELECT * FROM my_schema.big_table WHERE ds = '20260418'" --wait 0 --json
# Returns: { "status": "pending", "metadata": { "job_id": "<job_id>" } }

# Step 2: extract metadata.job_id (e.g. 2026042011_abc123) and wait
{{cli}} job wait <job_id> --json
# If still pending, retry with longer timeout:
{{cli}} job wait <job_id> --timeout 600 --json
```

### Cost Control And Pagination

```bash
# Estimate cost before running
{{cli}} query cost "SELECT * FROM big_table" --json

# Auto-abort if estimated cost exceeds threshold (in CU)
{{cli}} query "SELECT * FROM big_table" --cost-check 10.0 --json

# Dry-run: see plan without execution
{{cli}} query "SELECT * FROM big_table" --dry-run --json

# Pagination
{{cli}} query "SELECT * FROM your_table LIMIT 20" --page-size 20 --json
{{cli}} query "SELECT * FROM your_table LIMIT 20" --page-size 20 --cursor "<cursor>" --json
{{cli}} query "SELECT * FROM your_table" --output /tmp/results.json --json
```

`agent context --json` includes `cost_threshold_cu` (project-level default) and `allowed_operations` — respect these guardrails.

### Async Jobs

```bash
{{cli}} job submit "SELECT * FROM your_table" --json
{{cli}} job status <job_id> --json
{{cli}} job wait <job_id> --json
{{cli}} job wait <job_id> --timeout 600 --json
{{cli}} job wait <job_id> --stream
{{cli}} job result <job_id> --json
{{cli}} job result <job_id> --max-rows 50 --cursor "<cursor>" --json
{{cli}} job diagnose <job_id> --json
{{cli}} job cancel <job_id> --json
{{cli}} job list --json
{{cli}} job list --limit 50 --json
```

Use `job wait --stream` only when you want NDJSON events instead of the normal JSON envelope.

## Multi-Project Access

All meta and data commands accept `--project` for one-off cross-project access without switching session:

```bash
{{cli}} meta list-tables --project other_project --json
{{cli}} meta describe default.my_table --project other_project --json
{{cli}} data sample my_table --project other_project --json
```

Use `session set --project` when you need to stay in that project for multiple commands:

```bash
{{cli}} session set --project other_project --json
{{cli}} session set --project other_project --schema my_schema --json
{{cli}} session show --json
{{cli}} session unset --json
```

When writing SQL that references tables in another project, use `project.table` format — see SKILL.md §Dev vs Production Workspaces.

## Schema Operations (3-Tier vs 2-Tier)

Some MaxCompute projects use **3-tier namespace** (`project.schema.table`); others use **2-tier** (`project.table` only). Detect at runtime: run `{{cli}} meta list-schemas --json` — if it returns an error or empty result, the project is 2-tier and you should skip the schema layer entirely.

For 3-tier projects:

```bash
{{cli}} meta list-schemas --json
{{cli}} meta list-schemas --project other_project --json

# List tables in a specific schema (one-shot vs sticky)
{{cli}} meta list-tables --schema california_schools --json
{{cli}} session set --schema california_schools --json

# Search within a schema
{{cli}} meta search school --schema california_schools --json
{{cli}} meta search-columns county --schema california_schools --json

# Describe (use schema.table format)
{{cli}} meta describe california_schools.frpm --json

# Reset
{{cli}} session unset --json
```

When `--schema` is given, it overrides `session set --schema`. When neither is set, the project default schema is used.

## Semantic Metadata

Semantic metadata enriches tables with business context for NL2SQL and agent discovery. When `meta describe` warns about missing semantic metadata, the agent should generate it from its own LLM understanding of the table schema and save it with `meta semantic set`.

```bash
{{cli}} meta semantic list-missing --json

{{cli}} meta semantic set my_table \
  --desc "Daily user login events" \
  --use-cases "login funnel analysis" "DAU calculation" \
  --sample-questions "How many users logged in yesterday?" \
  --column-semantics '[{"name":"user_id","semantic_type":"user_identifier"}]' \
  --json

{{cli}} meta semantic get my_table --json

# Verify in describe output (semantic section appears when metadata exists)
{{cli}} meta describe my_table --json
```

## Agent Commands And Skill Registration

```bash
# Show environment context (auth, backend, capabilities)
{{cli}} agent context --json

# Show SKILL.md path and metadata
{{cli}} agent skill --json

# Register skill to an Agent platform
{{cli}} agent install-skill --json              # Claude Code (default)
{{cli}} agent install-skill cursor --json       # Cursor
{{cli}} agent install-skill windsurf --json     # Windsurf
{{cli}} agent install-skill codex --json        # OpenAI Codex
{{cli}} agent install-skill qwen --json         # Qwen
{{cli}} agent install-skill qoder --json        # Qoder
{{cli}} agent install-skill qoderwork --json    # QoderWork
```

`agent context` is a fast, **local** configuration summary — it reads `~/.maxc/config.yaml` and env vars, and does not make remote calls. Use it to inspect project/schema/cost guardrails or to check whether the CLI is even reachable. **It is not a substitute for `auth whoami`** — use `auth whoami --json` for an authenticated identity check (it does hit the backend), and `agent context --json` for the local-only configuration view.

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
| `data upload` | top-level `data` (rows_written, applied_partition, blocks, overwrite, ...) |
| `data download` | top-level `data` (rows_written, output_path, columns, truncated, ...) |
| `job status` / `job cancel` | `data.job` |
| `job diagnose` | `data.diagnosis` |
| `agent context` | `data.context` |

`session *` currently returns its native top-level `data` payload without an extra wrapper.

`agent_hints` includes (any field is omitted when empty):

- `next_actions`: list of suggested follow-up commands as plain strings (e.g. `"{{cli}} data sample foo --partition ds=20260509"`). Treat as hints, not as a script — quoting may break for SQL containing single quotes or other shell metacharacters; reconstruct the command yourself when needed.
- `warnings`: list of strings — actionable alerts (partition auto-selection, `--limit` truncation, etc.). Always check, even when `status=success`.
- `insights`: list of strings — contextual notes about the result.

There is **no** `actions[]` array, no `action_ids[]`, no per-action `id`/`title`/`placeholders` exposed in the rendered envelope. Build program logic on the three fields above.

See [json-output-format.md](json-output-format.md) for end-to-end envelope examples.

## Error Handling Patterns

For the full error code → recovery table, see [red-lines.md](red-lines.md) §Error Code → Recovery.

### Checking error responses

```bash
result=$({{cli}} query "SELECT * FROM missing_table" --json 2>/dev/null)
status=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))")

if [ "$status" = "failure" ]; then
  error_code=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('error',{}).get('code',''))")
  # Handle specific error codes
fi
```

### Common error → recovery flows

```bash
# NOT_FOUND → search for the correct name
{{cli}} meta search "partial_name" --json

# PERMISSION_DENIED → check permissions (often a workspace issue)
{{cli}} auth can-i --table your_table --operation SELECT --json

# JOB_TIMEOUT → check status and continue waiting
{{cli}} job status <job_id> --json
{{cli}} job wait <job_id> --timeout 600 --json

# EXECUTION_FAILED → diagnose the job
{{cli}} job diagnose <job_id> --json

# BACKEND_CONNECTION_ERROR → verify auth is still valid
{{cli}} auth whoami --json
```

## Gotchas

- There is no active runtime mock backend path. Missing auth does not produce fake table data.
- `auth whoami` performs a remote security `whoami` probe when config exists.
- `query cost` and `query explain` cannot be combined with `--async`, `--dry-run`, `--cursor`, `--output`, or `--output-format`. They only support `table` or `json` output.
- `meta list-projects` should lead into `session set --project ... --json` and `meta list-schemas --project ... --json`.
