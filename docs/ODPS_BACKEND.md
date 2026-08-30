# ODPS Backend Analysis

> This document maps maxc-cli commands to their underlying pyodps API calls,
> documenting the status, limitations, and fallback behavior of each integration point.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  maxc-cli (CLI Layer)                                       │
│  cli.py (argparse) → app.py (MaxCApp) → models.py (Envelope)│
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│  backend/ (ODPS Adapter Layer)                               │
│  OdpsBackend = QueryMixin + JobMixin                         │
│              + MetaMixin + DataMixin + AuthMixin              │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│  pyodps SDK (Official ODPS Python SDK)                       │
│  Odps / ODPSEntry / Instance / Table / SQLCost               │
└─────────────────────────────────────────────────────────────┘
```

## API Mapping Table

### Query Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `query run` | `client.execute_sql()` + `instance.wait_for_success(timeout=300)` | ✅ Full | Default 300s timeout; configurable via `--wait` |
| `query cost` | `client.execute_sql(hints={'skynet_network': 'true'})` (dry-run) | ✅ Full | Returns `SQLCost` with `input_size`, `complexity`, `udf_num` |
| `query explain` | `instance.get_sql_cost()` after dry-run | ✅ Full | |

### Job Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `job submit` | `client.execute_sql()` (async, returns instance ID) | ✅ Full | Returns job_id for tracking |
| `job status` | `instance.status` | ✅ Full | |
| `job wait` | Polling loop: `instance.reload()` every 3s | ✅ Full | Default 300s timeout; configurable via `--timeout` |
| `job result` | `instance.get_result()` + cursor-based pagination | ✅ Full | `--max-rows` (default 100) + `--cursor`; can atomically publish a completed page with `--output` |
| `job cancel` | `instance.stop()` | ✅ Full | |
| `job list` | `project.instances` | ✅ Full | Default limit=20, configurable |
| `job diagnose` | Composite: `instance.status` + `task_summary` + `logview` | ⚠️ Assembled | No dedicated diagnose API; inferences from status and logs |

### Metadata Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `meta list-tables` | `project.tables` iterator | ✅ Full | Large projects (>10k tables) may be slow on first call |
| `meta describe` | `table.schema` | ✅ Full | |
| `meta search` | Catalog API → SQLite cache → live table scan | ✅ Layered | Falls back safely when Catalog search is unavailable |
| `meta search-columns` | `table.schema.columns` + client-side filter | ⚠️ Client-side | No server-side column search |
| `meta partitions` | `table.partitions` | ✅ Full | |
| `meta latest-partition` | `table.partitions[-1]` | ✅ Full | |
| `meta freshness` | Inferred from partition modification times | ⚠️ Approximate | Not a native ODPS API; derived value |
| `meta list-projects` | `odps.list_projects()` | ✅ Full | |
| `meta list-schemas` | `project.schemas` | ✅ Full | |
| `meta semantic set` | Local SQLite only | ✅ Local | Not synced to ODPS server |
| `meta semantic get` | Local SQLite only | ✅ Local | |
| `meta semantic list-missing` | Local SQLite + `project.tables` cross-reference | ✅ Local | |

### Data Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `data sample` | `instance.get_result(max_rows=N)` via generated SELECT | ✅ Full | Supports `--partition`, `--columns`, `--rows` |
| `data profile` | Generated aggregation SQL (COUNT, MIN, MAX, etc.) | ⚠️ Assembled | Not a native ODPS profile; uses SQL queries to compute stats |
| `data upload` | Table Tunnel upload session | ✅ Full | Existing table only; validates the complete file before the session; missing partitions are created only with explicit `--create-partition`; PyODPS has no upload-session abort API |
| `data download` | Table Tunnel download session | ✅ Full | Existing local files are protected unless `--overwrite` is explicit |

### Auth Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `auth login --oauth` | Authorization Code + PKCE → OAuth refresh → STS exchange | ✅ Full | Public-cloud interactive default; persists sensitive token state locally |
| `auth login-external` | External credential process | ✅ Full | Supports NCS and other approved helpers |
| `auth whoami` | MaxCompute security `whoami` | ✅ Full | Returns desensitized identity |
| `auth can-i` | Schema-aware MaxCompute `checkPermission` API | ✅ Full | Object/action combinations are listed by live `--help` |

### Cache Commands

| maxc Command | Storage | Status | Limitations |
|---|---|---|---|
| `cache build` | SQLite (`~/.maxc/cache/cache.db`) | ✅ Full | Multi-threaded table schema crawling |
| `cache build-status` | SQLite | ✅ Full | |
| `cache status` | SQLite | ✅ Full | |
| `cache clear` | SQLite | ✅ Full | |

## Known Limitations

### Safety And Execution Contracts

- Resumable remote queries reject automatic retry flags. The caller keeps the
  first `metadata.job_id`, inspects that job, and decides manually whether a
  new submission is safe.
- Ordinary upload does not create a missing partition. `--create-partition`
  explicitly authorizes that metadata mutation; if a later Tunnel operation
  fails, the newly created partition can remain empty.
- Before `commit()` is attempted, failed Tunnel blocks are not visible and the
  uncommitted upload session expires server-side (PyODPS exposes no abort
  method). If the `commit()` request itself fails, the remote outcome is
  unknown: inspect the target before retrying because a blind retry can
  duplicate or replace data.
- External credential helpers come only from trusted user-level configuration
  or a user-selected `--config`. Automatically discovered workspace config
  cannot define `auth`. Helpers execute as one executable plus argv with no
  shell, so pipelines and redirections are unsupported.

### Critical (Affecting Core Workflows)

1. **meta lineage**: The backend has an internal unsupported placeholder, but
   the current public parser does not expose a `meta lineage` command.

2. **meta search fallback**: Catalog API is preferred. When it is unavailable,
   the CLI falls back to SQLite cache or a live client-side table scan; the
   final path can be slow on very large projects.

### Moderate (Affecting Edge Cases)

3. **data profile**: Implemented by generating SQL aggregation queries, not an ODPS-native profile feature. May be slow on very large tables or tables without partition pruning.

4. **meta freshness**: Derived from partition modification times, not a native ODPS API. Approximate and may not reflect data pipeline freshness accurately.

5. **job diagnose**: Assembled from `instance.status`, `task_summary`, and `logview` — no dedicated ODPS diagnose API. Some failure patterns may not be correctly classified.

### Minor (Quality of Life)

7. **meta list-tables**: On first call without cache, iterates all tables client-side. Large projects (>10k tables) may take 30+ seconds.

8. **query cursor pagination**: Cursor-based pagination uses base64-encoded offset + session_id. Cursors expire when the underlying ODPS instance is garbage-collected (typically 24-72 hours).

## Error Mapping

| Source condition | MaxCError subclass | Error code | Recoverable |
|---|---|---|---|
| `odps.errors.NoPermission` (including access-denied variants) | `PermissionDeniedError` | `PERMISSION_DENIED` | No |
| `odps.errors.NoSuchObject` | `SchemaNotFoundError`, `TableNotFoundError`, or `NotFoundError`, based on service context | `SCHEMA_NOT_FOUND`, `TABLE_NOT_FOUND`, or `NOT_FOUND` | No |
| Generic `ODPSError` classified as SQL/read-only | `SqlError` or `ReadOnlyError` | `SQL_ERROR` or `READ_ONLY_VIOLATION` | No |
| Connection/resolution failure | `BackendConnectionError` | `BACKEND_CONNECTION_ERROR` | Yes |
| Job polling exceeds the requested timeout | `JobTimeoutError` | `JOB_TIMEOUT` | Yes |
| Client estimate exceeds `--cost-check` | `CostLimitExceededError` | `COST_LIMIT_EXCEEDED` | No |
| Tunnel commit request began but its outcome cannot be proven | `UploadCommitOutcomeUnknownError` | `UPLOAD_COMMIT_OUTCOME_UNKNOWN` | No |

The translator also preserves a sanitized request ID in the suggestion when
PyODPS provides one. Unknown service failures fall back to a typed SQL or
connection error; the CLI does not emit the obsolete `BACKEND_UNREACHABLE` or
`ODPS_INTERNAL_ERROR` codes.

## Backend Mixin Structure

```python
class OdpsBackend(
    JobMixin,    # extends QueryMixin — job submit/status/wait/result/cancel/list/diagnose
    MetaMixin,   # list_tables/describe/search/search_columns/partitions/latest_partition/freshness/lineage
    DataMixin,   # sample_table/profile_table
    AuthMixin,   # whoami/can_i
):
    supports_remote_jobs = True
    supports_cost_check = False  # ODPS cost check uses dry-run, not a dedicated API
```
