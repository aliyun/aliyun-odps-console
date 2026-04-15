# MaxC Command Patterns

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
maxc auth can-i --table your_table --operation SELECT --json
maxc session show --json
maxc session set --project your_project --schema your_schema --json
maxc session unset --json
```

Use `session set` instead of inventing project-switch wrappers. Session overrides live in `~/.maxc/session_override.yaml`.
Use `auth login` instead of hand-editing `~/.maxc/config.yaml`.

## Metadata And Data Discovery

```bash
maxc cache build --json
maxc meta list-tables --json
maxc meta describe your_table --json
maxc meta describe your_table --full --json
maxc meta search "keyword" --json
maxc meta search-columns "user_id" --json
maxc meta partitions your_table --json
maxc meta latest-partition your_table --json
maxc meta freshness your_table --json
maxc meta lineage your_table --json
maxc meta list-projects --json
maxc meta list-schemas --project your_project --json
maxc data sample your_table --rows 5 --partition ds=2026-03-20 --columns id,ds --json
maxc data profile your_table --partition ds=2026-03-20 --json
```

Use `cache build` before `meta list-tables` on a cold environment. `meta list-tables` is cache-backed and returns `status=cache_miss` until metadata has been cached.

## Query And Jobs

Preferred query syntax:

```bash
maxc query "SELECT 1 AS one" --json
maxc query cost "SELECT 1 AS one" --json
maxc query explain "SELECT 1 AS one" --json
```

Legacy-compatible syntax still works:

```bash
maxc query "SELECT 1 AS one" --mode cost --json
```

Wait and timeout control:

```bash
# Default: wait up to 10 seconds, auto-promote to async if not done
maxc query "SELECT * FROM big_table" --json

# Submit and return immediately (get job_id without waiting)
maxc query "SELECT * FROM big_table" --wait 0 --json

# Wait up to 60 seconds before promoting
maxc query "SELECT * FROM big_table" --wait 60 --json

# Cost-guard: abort if estimated cost exceeds threshold
maxc query "SELECT * FROM big_table" --cost-check 10.0 --json

# Dry-run: see query plan without executing
maxc query "SELECT * FROM big_table" --dry-run --json
```

Pagination and output:

```bash
maxc query "SELECT * FROM your_table LIMIT 20" --page-size 20 --json
maxc query "SELECT * FROM your_table LIMIT 20" --page-size 20 --cursor "<cursor>" --json
maxc query "SELECT * FROM your_table" --output /tmp/results.json --json
```

Async jobs:

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
Use `job wait --timeout N` to extend the default 300s wait. If timeout is reached, status will be `pending` — use `job status` to keep checking.

## Cache And Semantic Metadata

Cache lifecycle:

```bash
maxc cache status --json
maxc cache build --json
maxc cache build --async --json
maxc cache build-status --build-id <build_id> --json
maxc cache clear --json
```

Semantic metadata:

```bash
maxc cache save-semantic \
  --table your_table \
  --schema default \
  --semantic-desc "One-sentence business meaning" \
  --use-cases '["analysis A","analysis B"]' \
  --sample-questions '["question A","question B"]' \
  --column-semantics '[{"name":"id","semantic_type":"id"}]' \
  --json

maxc cache get-semantic --table your_table --schema default --json

maxc meta semantic set your_table \
  --desc "One-sentence business meaning" \
  --use-cases analysis_a analysis_b \
  --sample-questions question_a question_b \
  --column-semantics '[{"name":"id","semantic_type":"id"}]' \
  --json

maxc meta semantic get your_table --json
maxc meta semantic list-missing --json
```

Use `meta semantic` when you want session-scoped semantics on the current project/schema. Use `cache save-semantic` / `cache get-semantic` when you need explicit `--project` or `--schema`.

## Agent Commands And Skill Registration

```bash
# Show environment context (auth, backend, capabilities)
maxc agent context --json

# Show SKILL.md path and metadata
maxc agent skill --json

# List all available commands
maxc agent commands --json

# Register skill to an Agent platform
maxc agent install-skill --json              # Claude Code (default)
maxc agent install-skill cursor --json       # Cursor
maxc agent install-skill windsurf --json     # Windsurf
maxc agent install-skill codex --json        # OpenAI Codex
```

`agent context` returns auth status, backend reachability, python version, capabilities, and skill path — use it as a preflight before any data operation.
`agent install-skill` copies SKILL.md and references from the installed package into the platform's skill directory. It is idempotent: re-running at the same version skips the copy; after `pip install --upgrade maxc-cli`, re-run to update the local skill files.

## Diffs And Agent Context

```bash
maxc diff schema left_table right_table --json
maxc diff partition left_table right_table --json
maxc diff data left_table right_table --keys id --columns value_col --rows 100 --json
maxc diff data left_table right_table --keys id --left-partition ds=2026-04-09 --right-partition ds=2026-04-10 --json
maxc agent context --json
```

`agent context` is a fast configuration summary. It does not enumerate tables and does not require authenticated backend startup.

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

- `query` / `job wait` / `job result` -> `data.result` and `data.pagination`
- `query cost` / `query explain` -> `data.analysis`
- `auth whoami` -> `data.identity` and optional `data.auth_options`
- `auth can-i` -> `data.authorization`
- `meta describe` -> `data.table`
- `meta search` / `meta search-columns` -> `data.search.matches`
- `data sample` -> `data.sample`
- `data profile` -> `data.profile`
- `job status` / `job cancel` -> `data.job`
- `job diagnose` -> `data.diagnosis`
- `agent context` -> `data.context`

`cache status`, `cache build`, `cache build-status`, `cache clear`, and `session *` currently return their native top-level `data` payloads without an extra wrapper.

`agent_hints` includes:

- `action_ids`: stable identifiers such as `meta.describe`
- `next_actions`: rendered shell commands derived from those ids

Use `action_ids` when you want stable program logic. Use `next_actions` as hints only.

## Error Handling Patterns

All errors return an `error` object with `code`, `message`, `suggestion`, and `recoverable` fields.

### Checking error responses

```bash
# Run a command and check the result
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

# PERMISSION_DENIED → check permissions
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

### Using agent_hints for navigation

Every successful response includes `agent_hints` with:
- `action_ids`: stable identifiers for program logic (e.g., `"meta.describe"`)
- `next_actions`: rendered shell commands you can run directly
- `warnings`: actionable alerts (e.g., cache staleness, missing semantic metadata)
- `insights`: contextual information about the result

Always check `agent_hints.warnings` — they surface issues that are not errors but require attention.

## Gotchas

- There is no active runtime mock backend path. Missing auth does not produce fake table data.
- `auth whoami` now performs a remote security `whoami` probe when config exists.
- `cache build --json` prints progress to `stderr` and writes a single final JSON envelope to `stdout`.
- `cache build --async --json` returns a `build_id`; poll with `cache build-status --build-id <build_id> --json`.
- `query cost` and `query explain` cannot be combined with `--async`, `--dry-run`, `--cursor`, `--output`, or `--output-format`. They only support `table` or `json` output.
- `meta list-projects` should lead into `session set --project ... --json` and `meta list-schemas --project ... --json`.
- If `~/.maxc/cache/cache.db` is unwritable or shared across concurrent startup processes, you may still see SQLite failures such as `unable to open database file` or `database is locked`, but current CLI code translates them into structured validation failures.
