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

Always check `status` first; it is `success`, `pending`, or `failure`. On
`failure`, read `error.suggestion` before retrying. On any status, inspect
`agent_hints.warnings`. Treat `agent_hints.actions` as authoritative. Legacy
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
    "next_actions": [
      "{{cli}} job wait 2026... --json",
      "{{cli}} job status 2026... --json"
    ],
    "insights": ["Query promoted to async after 10s."]
  }
}
```

Follow up with `{{cli}} job wait` or `{{cli}} job status` using the `job_id`.

Successful `job wait` and `job result` responses describe the current,
non-mutating follow-up in `data.safety`: `scope` is `result_observation` and
`allowed_operations` contains `JOB_WAIT` or `JOB_RESULT`. The block does not
reclassify the already-submitted SQL or claim that it was executed again.

## Blocked SQL mutations

This Skill only executes `SELECT`. If `WRITE_OPERATION_REQUIRES_FORCE` is
returned for DDL/DML, do not bypass the gate; route the requested mutation to
an approved change workflow outside this Skill.

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
  "metadata": { "elapsed_ms": 4567, "project": "..." },
  "agent_hints": { "next_actions": ["{{cli}} data sample proj.sch.tbl --partition ds=20260509"] }
}
```

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
