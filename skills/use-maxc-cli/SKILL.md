---
name: use-maxc-cli
description: Bootstrap Python, maxc-cli, and ncs, then operate the current maxc CLI as the execution layer for internal MaxCompute / ODPS workflows with ncs as the primary auth path. Use when Codex needs to set up the environment, bootstrap or validate ncs auth, switch session project/schema, inspect metadata, run read-only SQL, or manage cache and semantic metadata through maxc.
---

# Use MaxC CLI

Use the live CLI instead of inventing a separate MaxCompute adapter. On user machines, prefer `maxc ...`. If the console script is not yet on `PATH` but the package is installed, use `python3 -m maxc_cli ...`.

## When To Use

Use this skill when the task involves MaxCompute, ODPS, or `maxc`, especially for:

- first-time setup or repair of Python, `maxc-cli`, or `ncs`
- ncs-backed auth setup, account discovery, or config inspection
- ncs-first auth bootstrap or identity inspection
- session project or schema overrides
- metadata discovery, schema inspection, or cache-backed search
- read-only query execution or job tracking
- cache and semantic-metadata workflows
- checking the current CLI surface or JSON envelope format

Do not use this skill when:

- the task is to implement `maxc-cli` itself rather than operate it
- the user explicitly wants raw pyodps / SDK code instead of the CLI

## First Pass

1. Determine the runtime target:
   - prefer installed `maxc ...`
   - fall back to `python3 -m maxc_cli ...` if the console script is not on `PATH` yet
2. If the machine may not be bootstrapped yet, read [references/setup-install.md](references/setup-install.md) first and complete the Python, `maxc-cli`, and `ncs` checks before trying to answer ODPS questions.
3. Confirm the active command surface:
   - use `maxc --help`
   - if needed, use `python3 -m maxc_cli --help`
4. Normalize runtime paths before trusting results:
   - maxc writes config, session overrides, state, and cache under `~/.maxc` by default
   - in sandboxes or CI, make sure `HOME`, `state_dir`, and `cache_dir` point to writable locations
5. Run `auth whoami --json`.
6. Inspect `data.identity`:
   - `authenticated=false` and `configured=false` means required auth settings are missing
   - `authenticated=false`, `configured=true`, `validation_status=failed` means config exists but remote validation failed
   - `authenticated=true`, `validation_status=verified` means the security `whoami` probe succeeded
7. If auth is not ready, use the `ncs` path only. Ignore access-key or STS suggestions that may still appear in `data.auth_options`.
8. Read [references/setup-install.md](references/setup-install.md) when Python, `maxc`, or `ncs` are missing, outdated, or misconfigured.
9. Read [references/ncs-auth.md](references/ncs-auth.md) for account-type mapping, `login-ncs` patterns, endpoint/project guidance, and config-persistence rules.
10. Read [references/bootstrap-auth.md](references/bootstrap-auth.md) when auth or config behavior matters.
11. If you need cached metadata and `meta list-tables --json` returns `status=cache_miss`, run `cache build --json` or `cache build --async --json` first.
12. Read [references/command-patterns.md](references/command-patterns.md) when command syntax, output shape, or cache/job workflow details matter.

## Working Rules

- Stay read-only unless the user explicitly asks for state changes. Query execution is still limited to `SELECT`.
- Prefer `--json` for machine-driven work.
- `--json` stdout is normally a single final envelope. The main exception is `job wait --stream`, which emits NDJSON events.
- `cache build --json` keeps stdout as one final envelope and emits human progress lines to `stderr`.
- Trust runtime help and actual command output over stale snippets or second-hand instructions.
- Treat environment bootstrap as part of the job. If Python, `maxc`, or `ncs` are missing, install or repair them before attempting ODPS work.
- `maxc-cli` currently supports Python `3.6` through `3.12`.
- Never install or upgrade Python without explicit user confirmation. It is a system-level change that may affect the user's shell, PATH, and other tools.
- For this internal-release skill, treat `auth login-ncs` as the only supported login path.
- Do not instruct users to use `auth login`, `--from-env`, access keys, secret access keys, or STS tokens through this skill.
- If `data.auth_options` includes non-`ncs` choices, ignore them unless the task is explicitly about inspecting CLI implementation details instead of following the release workflow.
- Prefer `auth login-ncs` over hand-editing `~/.maxc/config.yaml` or persisting temporary access keys returned by raw `ncs create credential` output.
- The CLI itself does not auto-install `ncs`. When `ncs` is missing, use the bundled installer script first, then continue with `auth login-ncs`.
- Use normalized `data` shapes from the JSON envelope:
  - `auth whoami` -> `data.identity`
  - `query` / `job wait` / `job result` -> `data.result` and `data.pagination`
  - `query cost` / `query explain` -> `data.analysis`
  - `meta describe` -> `data.table`
  - `data sample` -> `data.sample`
  - `agent context` -> `data.context`
- Use `agent_hints.action_ids` as the stable action registry. `agent_hints.next_actions` are rendered shell commands and may include placeholders.
- `auth can-i` is currently a table-level `SELECT` precheck only.
- `session set/show/unset` are local override commands. They do not require an authenticated backend session.
- `agent context` is a fast local config summary. It does not enumerate tables.
- `meta list-tables` is cache-backed. On a cold cache it returns `cache_miss` guidance instead of live table enumeration.
- `meta semantic *` and `cache *-semantic` both write local semantic metadata. Use the family that matches the task:
  - `meta semantic` for current session project/schema workflows
  - `cache save-semantic` / `cache get-semantic` when explicit `--project` or `--schema` control matters

## Command Families

- Bootstrap and setup: `python3 --version`, `python3 -m pip install ...`, `python3 -m maxc_cli --help`, `scripts/install_ncs.sh`
- Auth and session for this skill: `auth whoami`, `auth login-ncs`, `auth can-i`, `session set/show/unset`
- Metadata and data: `meta list-tables`, `meta describe`, `meta search`, `meta search-columns`, `meta latest-partition`, `meta freshness`, `meta partitions`, `meta list-projects`, `meta list-schemas`, `data sample`, `data profile`
- Query and jobs: `query`, `query cost`, `query explain`, `job submit/status/wait/result/diagnose/cancel/list`
- Cache and semantic metadata: `cache build`, `cache build-status`, `cache status`, `cache clear`, `cache save-semantic`, `cache get-semantic`, `meta semantic set/get/list-missing`
- Diffs and context: `diff schema`, `diff partition`, `diff data`, `agent context`
