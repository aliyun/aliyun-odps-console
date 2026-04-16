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
| `job result` | `instance.get_result()` + cursor-based pagination | ✅ Full | `--max-rows` (default 100) + `--cursor` for pagination |
| `job cancel` | `instance.stop()` | ✅ Full | |
| `job list` | `project.instances` | ✅ Full | Default limit=20, configurable |
| `job diagnose` | Composite: `instance.status` + `task_summary` + `logview` | ⚠️ Assembled | No dedicated diagnose API; inferences from status and logs |

### Metadata Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `meta list-tables` | `project.tables` iterator | ✅ Full | Large projects (>10k tables) may be slow on first call |
| `meta describe` | `table.schema` | ✅ Full | |
| `meta search` | `project.tables` + client-side substring match | ⚠️ Client-side | No server-side FTS; relies on SQLite cache when available |
| `meta search-columns` | `table.schema.columns` + client-side filter | ⚠️ Client-side | No server-side column search |
| `meta partitions` | `table.partitions` | ✅ Full | |
| `meta latest-partition` | `table.partitions[-1]` | ✅ Full | |
| `meta freshness` | Inferred from partition modification times | ⚠️ Approximate | Not a native ODPS API; derived value |
| `meta lineage` | — | ❌ Unsupported | Returns `supported=false` placeholder; no ODPS lineage API accessible via pyodps |
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

### Auth Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `auth login` | `Odps()` with AK/SK → `project.name` validation | ✅ Full | Writes config to `~/.maxc/config.yaml` |
| `auth login-external` | NCS token-based auth | ✅ Full | Internal Alibaba auth |
| `auth whoami` | `project.name` + `project.owner` verification | ✅ Full | Returns desensitized identity |
| `auth can-i` | Attempt `SELECT 1 FROM table LIMIT 0` | ⚠️ Limited | Only checks SELECT permission; other operations not pre-checkable |

### Diff Commands

| maxc Command | pyodps API | Status | Limitations |
|---|---|---|---|
| `diff schema` | `table.schema` comparison | ✅ Full | |
| `diff partition` | `table.partitions` comparison | ✅ Full | |
| `diff data` | Keyed snapshot comparison via SQL | ✅ Full | Requires `--key-columns` for data diff |

### Cache Commands

| maxc Command | Storage | Status | Limitations |
|---|---|---|---|
| `cache build` | SQLite (`~/.maxc/cache/cache.db`) | ✅ Full | Multi-threaded table schema crawling |
| `cache build-status` | SQLite | ✅ Full | |
| `cache status` | SQLite | ✅ Full | |
| `cache clear` | SQLite | ✅ Full | |
| `cache save-semantic` | SQLite `table_semantic` table | ✅ Full | |
| `cache get-semantic` | SQLite `table_semantic` table | ✅ Full | |

## Known Limitations

### Critical (Affecting Core Workflows)

1. **meta lineage**: ODPS lineage API is not exposed via pyodps. The command returns a `supported=false` placeholder with a clear contract. If the API becomes available, the backend can be updated without changing the CLI interface.

2. **meta search**: No server-side full-text search. Current implementation uses:
   - Client-side substring match over `project.tables` (slow on large projects)
   - SQLite cache (fast, but requires `cache build` first)
   - Future: evaluate FTS5 or sqlite-vec for better semantic matching

3. **auth can-i**: Only covers `SELECT` permission check. INSERT, CREATE, DROP, etc. cannot be pre-checked via the current pyodps API.

### Moderate (Affecting Edge Cases)

4. **data profile**: Implemented by generating SQL aggregation queries, not an ODPS-native profile feature. May be slow on very large tables or tables without partition pruning.

5. **meta freshness**: Derived from partition modification times, not a native ODPS API. Approximate and may not reflect data pipeline freshness accurately.

6. **job diagnose**: Assembled from `instance.status`, `task_summary`, and `logview` — no dedicated ODPS diagnose API. Some failure patterns may not be correctly classified.

### Minor (Quality of Life)

7. **meta list-tables**: On first call without cache, iterates all tables client-side. Large projects (>10k tables) may take 30+ seconds.

8. **query cursor pagination**: Cursor-based pagination uses base64-encoded offset + session_id. Cursors expire when the underlying ODPS instance is garbage-collected (typically 24-72 hours).

## Error Mapping

| pyodps Exception | MaxCError Subclass | Error Code | Recoverable |
|---|---|---|---|
| `odps.errors.ODPSError` | `BackendConnectionError` | `BACKEND_UNREACHABLE` | Yes |
| `odps.errors.NoSuchObject` | `ValidationError` | `TABLE_NOT_FOUND` | No |
| `odps.errors.AccessDenied` | `PermissionDeniedError` | `PERMISSION_DENIED` | No |
| `odps.errors.InternalError` | `MaxCError` | `ODPS_INTERNAL_ERROR` | Yes |
| Timeout (polling > limit) | `JobTimeoutError` | `JOB_TIMEOUT` | Yes |
| `odps.errors.CostLimitExceeded` | `CostLimitExceededError` | `COST_LIMIT_EXCEEDED` | No |

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
