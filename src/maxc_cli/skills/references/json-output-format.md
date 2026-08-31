> Loaded on demand — full envelope spec with worked `jq` examples. Skip unless the agent is parsing a response shape not covered by SKILL.md's key-path list.

# JSON Output Format

Examples omit the already-established session User-Agent for readability;
append `--user-agent "$UA"` before executing them.

Ordinary `--json` command responses use Envelope v2.0. Specialized CSV/NDJSON
row output and `job wait --stream` lifecycle records are streams, not an
Envelope around every row or event. Use `jq` or Python to extract fields such
as `command`, `status`, `data`, `error`, and `agent_hints` from an envelope.

```bash
# Extract query result rows
{{cli}} query "SELECT ..." --json | jq '.data.result.rows'

# Export as TSV
{{cli}} query "SELECT ..." --json | jq -r '.data.result.rows[] | [.col1, .col2] | @tsv'
```

Always check the envelope's top-level `status` first: `success`, `pending`, or
`failure`. Job and cache lifecycle states stay in their documented nested
`data` fields or stream events; do not confuse them with the envelope status,
and stop on an unknown top-level value. On `failure`, read `error.suggestion`
before retrying. On any status, inspect `agent_hints.warnings`. Treat
`agent_hints.actions` as authoritative. Legacy
`next_actions`, when present, contains only actions that are executable,
agent-allowed, and require no confirmation.

## Query success

`data` is normalized into `result`, `pagination`, and `safety`:

```json
{
  "data": {
    "result": {
      "rows": [{"id": 1, "name": "Alice"}],
      "schema": [{"name": "id", "type": "BIGINT", "comment": ""}],
      "row_count": 1,
      "returned_rows": 1
    },
    "pagination": {
      "has_more": false,
      "next_cursor": null
    },
    "safety": {
      "mode": "read_only",
      "force": false,
      "allowed_operations": ["SELECT"],
      "effective_hints": {},
      "policy_decision": "allowed"
    }
  }
}
```

Key paths: `data.result.rows`, `data.result.returned_rows`, `data.result.row_count`, `data.pagination.has_more`, `data.pagination.next_cursor`.

## Query cost / explain

```json
{
  "data": {
    "analysis": {
      "estimated_input_size_bytes": 456789,
      "sql_complexity": "low",
      "tables_used": ["schema.table"]
    },
    "safety": { "mode": "read_only", "policy_decision": "allowed" }
  }
}
```

Key path: `data.analysis` (not `data.result`).

## Query timeout (auto-promoted to async)

When `--wait N` is exceeded, `status` is `pending` with a `job_id` in metadata:

```json
{
  "status": "pending",
  "metadata": {
    "job_id": "2026...",
    "project": "my_project",
    "wait_seconds": 10,
    "sql_executed": "SELECT ..."
  },
  "agent_hints": {
    "actions": [
      {
        "id": "job.wait",
        "command": "{{cli}} --user-agent <user_agent> job wait 2026... --project my_project --json",
        "executable": false,
        "placeholders": { "user_agent": "<user_agent>" },
        "effect": "read",
        "confirmation_required": false,
        "agent_allowed": true
      },
      {
        "id": "job.status",
        "command": "{{cli}} --user-agent <user_agent> job status 2026... --project my_project --json",
        "executable": false,
        "placeholders": { "user_agent": "<user_agent>" },
        "effect": "read",
        "confirmation_required": false,
        "agent_allowed": true
      },
      {
        "id": "job.result",
        "command": "{{cli}} --user-agent <user_agent> job result 2026... --project my_project --max-rows 100 --json",
        "executable": false,
        "placeholders": { "user_agent": "<user_agent>" },
        "effect": "read",
        "confirmation_required": false,
        "agent_allowed": true
      }
    ],
    "action_ids": ["job.wait", "job.status", "job.result"],
    "insights": ["Query promoted to async after 10s."]
  }
}
```

Only the action fields needed to illustrate continuation safety are shown above;
the runtime also returns each action's title and argument schema. Resolve the
session User-Agent placeholder before following up. Because these templates are
not yet executable, this envelope does not include legacy `next_actions`.

Successful `job wait` and `job result` responses describe the current,
non-mutating follow-up in `data.safety`: `scope` is `result_observation` and
`allowed_operations` contains `JOB_WAIT` or `JOB_RESULT`. The block does not
reclassify the already-submitted SQL or claim that it was executed again.
When `job wait` already contains `data.result`, use it directly instead of
issuing a redundant `job result` call.

## Authorized SQL mutations

DDL/DML is allowed only when the user explicitly requests the exact mutation.
Verify the statement, project, schema, target, and effect, then submit one
statement at a time with `--force`. The positive allowlist accepts recognized
data-plane mutations; unknown, procedural, permission, session-control, and
administrative SQL remain blocked even with `--force`.
`WRITE_OPERATION_REQUIRES_FORCE` is a safety signal, not authorization; never
infer the write or automatically replay a suggested mutation action.
Leading `SET` options are part of the same authorized execution context.
Project-security and masking controls remain blocked, and forced mutations
accept only audited statement-local execution hints.
On successful query or submission responses, `data.safety.effective_hints`
reports the hints actually sent to MaxCompute. Audited hint values are shown;
an audited key whose value is outside its documented boolean, numeric, or enum
domain is still rendered as `<redacted>`. Values of unknown hints retained for
read-only compatibility are also never echoed.

## data upload

Tunnel-based bulk load. `data` is flat (no inner wrapper):

```json
{
  "command": "data upload",
  "status": "success",
  "data": {
    "table": "proj.sch.tbl",
    "applied_partition": "ds=20260509",
    "rows_written": 12345,
    "bytes_read": 2345678,
    "blocks": 2,
    "overwrite": false,
    "create_partition": false,
    "warnings": []
  },
  "metadata": { "elapsed_ms": 4567, "project": "my_project" },
  "agent_hints": {
    "actions": [
      {
        "id": "data.sample",
        "command": "{{cli}} --user-agent <user_agent> data sample proj.sch.tbl --partition ds=20260509 --project my_project --json",
        "executable": false,
        "placeholders": { "user_agent": "<user_agent>" },
        "effect": "read",
        "confirmation_required": false,
        "agent_allowed": true
      }
    ],
    "action_ids": ["data.sample"]
  }
}
```

As above, the unresolved User-Agent keeps the cloud follow-up out of
`next_actions`. Fill the placeholder with the existing session value before
deciding whether the sample is appropriate.

A missing partition is created only when the caller explicitly supplies
`--create-partition` together with `--partition`. That metadata mutation can
remain as an empty partition if a later Tunnel operation fails. Without the
flag, upload does not create partitions.

On failure, `error.context` carries `line` (1-based) and `column` (column NAME):

```json
{
  "status": "failure",
  "error": {
    "code": "CSV_PARSE_ERROR",
    "message": "could not parse 'abc' as bigint: invalid literal",
    "context": { "line": 3, "column": "user_id" }
  }
}
```

## data download

```json
{
  "command": "data download",
  "status": "success",
  "data": {
    "table": "proj.sch.tbl",
    "applied_partition": "ds=20260509",
    "output_path": "/abs/path/out.csv",
    "rows_written": 10000,
    "bytes_written": 4567890,
    "columns": ["col1", "col2"],
    "truncated": true,
    "warnings": ["--limit reached; output may be partial (session has 53210 rows)."]
  }
}
```
