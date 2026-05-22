# MaxCompute SQL Error Recovery Handbook

Decision reference after a SQL execution fails. When `{{cli}} query --json` returns `status=failure`, this file maps the `error.code` (e.g. `ODPS-0130071`) to recovery actions. Self-explanatory errors that need no agent action are not expanded here.

## Auto-retry Policy

| Category | Retry action | Max attempts |
|---|---|---|
| Compile-time error (rewrite-SQL class) | Modify SQL, retry | 3 |
| Runtime error (SET-tunable class) | Modify SQL or set parameter | 3 |
| Same error 3× in a row | Stop, surface raw error text | — |

> **DML write idempotency reminder**: auto-retry is only safe by default for `SELECT`, compile-time errors, and idempotent writes such as `INSERT OVERWRITE`. `INSERT INTO` / `MERGE` / `UPDATE` / `DELETE` **must not be blindly retried** — the same job may have already partially committed; retrying duplicates rows or conflicts with the previous result. Before retrying these, confirm the instance state (success / failure / not committed).

## How to Match Entries

The error-code prefix indicates the broad category: `0130xxx` compile / `0123xxx` runtime / `0140xxx` Sandbox / `1850xxx` MCQA.

`ODPS-0130071` and `ODPS-0010000` are **wrappers** (one code covers many subscenarios) — split them by message keyword, see below.

## Dedicated Semantic Error Codes

| Code | Meaning | Fix |
|---|---|---|
| `ODPS-0130131` | Table not found | Check spelling, project/schema prefix, querier ACL |
| `ODPS-0130121` | Invalid argument type | Compare to function signature, `CAST` input to the correct type |
| `ODPS-0130141` | Illegal implicit type cast | Explicit `CAST(col AS ...)` (**note**: `Partition not found` is **not** this code, it's `0130071`) |
| `ODPS-0130241` | Illegal union operation | Make UNION column count and types match; explicit `CAST` each branch to a unified type (implicit promotion often fails: BIGINT vs DECIMAL, STRING vs BIGINT) |
| `ODPS-0130252` | Cartesian product is not allowed | Add an `ON` condition; or `/*+ MAPJOIN(small_table) */`; fallback `SET odps.sql.allow.cartesian=true;` |
| `ODPS-0140081` | Unsupported join type | MAPJOIN does not support that OUTER configuration; switch JOIN type or drop the MAPJOIN hint |
| `ODPS-0130013` | Authorization exception / Access Denied | Insufficient table/column ACL; ask the owner to grant |
| `ODPS-0130161` | Syntax error, **or** SQL too large (message contains `DFA count`) | Fix syntax; for size, see "SQL size limit" below |

### `ODPS-0130071` is a generic semantic-exception wrapper (covers many subscenarios)

**Do not try to enumerate `0130071` subscenarios** — it is MaxCompute's catch-all semantic error code; any semantic exception without a dedicated code lands here.

Correct flow:
1. First compare against the "Dedicated semantic error codes" table above → match → fix per that row.
2. If the code is genuinely `0130071`, look up the message keyword in the "High-frequency subscenarios" section below.

---

## Compile-time Errors

### SQL size limit (three same-root error codes)

Different stages, same fix direction:

| Code | Message signature | Trigger stage |
|---|---|---|
| `ODPS-0130161` | `Parse fail ... DFA count` | Parse |
| `ODPS-0130071` | `compile fail ... AST node count` | Semantic |
| `ODPS-0010000` | `The Size of Plan is too large` | Plan generation (>1MB rejected) |

**Fix** (only by reducing SQL complexity — no SET switch):
- Split CTEs into multiple `INSERT OVERWRITE` chained via intermediate tables.
- Reduce JOIN levels; use partition pruning to shrink leaf scan size.
- Aim for ≤ 5 window functions per single SELECT (soft limit); UNION ALL ≤ 256 tables.

### `ODPS-0010000` wrapper: plan-too-large vs. worker OOM (split by message)

Code `0010000` covers two opposite scenarios; you must classify by message text:

| Message keyword | Subscenario | Fix direction |
|---|---|---|
| `The Size of Plan is too large` | **Compile-time** plan rejected (>1MB) | Split SQL, see "SQL size limit" above |
| `worker out of memory` / `sigkill(oom)` (no `sqltask` keyword) | **Runtime** worker process OOM | First identify the failing task type (mapper / reducer / joiner) and tune the corresponding memory: `odps.sql.mapper.memory` / `odps.sql.reducer.memory` / `odps.sql.joiner.memory` (unit MB, common 4096–8192). Also inspect operator hot spots and data skew. |
| `sqltask` + OOM | Compile / planning SQL-task process OOM | Reduce partition scan, split SQL, reduce metadata queries. **Not an operator memory issue** — tuning stage memory has no effect. |

**Recommended fix order (worker OOM)**:
1. Read logview / instance summary, identify the failed task type and the specific operator (HashJoin / SortMerge / WindowAgg / UDF).
2. Check skew: `DistinctValueCounts`, JOIN-key distribution, whether single-task input bytes are far above average.
3. If skewed, address the skew first (`SKEWJOIN` hint / random-suffix the key / split hot keys), then consider raising memory.
4. If none of the above work **and the data volume itself is genuinely large** → bump the corresponding stage memory or split SQL.
5. **Easy misdiagnosis**: when you see `Size of Plan` in the message, do **not** tune stage memory (root cause is compile-time plan size). HashJoin / window-operator blowup sometimes is not solved purely by adding memory — fix skew first.

### `ODPS-0130071` high-frequency subscenarios

| Message keyword | Scenario | Fix |
|---|---|---|
| `compile fail ... AST node count` | SQL size limit (semantic stage) | Split CTE / chain multiple `INSERT OVERWRITE` |
| `recursive-cte ... exceed max iterate number %d` | Recursive CTE iteration limit | `SET odps.sql.rcte.max.iterate.num=100;` (default 10, hard limit 100, larger values are clamped); otherwise rewrite as multi-JOIN |
| `partition not found:<spec>` | Partition does not exist | Check the partition value format (`'YYYYMMDD'` vs `'YYYY-MM-DD'`); confirm with `SHOW PARTITIONS <table>` or `{{cli}} meta latest-partition` |
| `column %s cannot be resolved` | Column name resolution failed | **Case-sensitive**; confirm with `DESC <table>` or `{{cli}} meta describe`. When the edit distance is small the compiler suggests "Did you mean %s?" |
| `expect equality expression for join condition` | Non-equi JOIN without a mapjoin hint | Rewrite as equi-JOIN, or `/*+ MAPJOIN(small_table) */` |
| `function sum cannot match any overloaded functions with (BOOLEAN)` | `SUM(boolean expression)` | Rewrite as `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` or `COUNT_IF(...)` |
| `expression is not in GROUP BY` | Non-aggregate column missing from `GROUP BY` | **Preferred**: add the column to `GROUP BY`, or switch to a deterministic aggregate (`MAX` / `MIN` / `SUM`); use `ANY_VALUE(col)` only when "any representative value is acceptable" (**non-deterministic** — different runs may return different values) |
| `INSERT INTO HASH CLUSTERED table` | Hash-clustered table does not support `INSERT INTO` | Use `INSERT OVERWRITE` |
| `invalid partition value` | Dynamic partition value contains illegal characters | Allowed: letters / digits / spaces + `_@$#.!:-`; ≤255 bytes; no Chinese; runtime does not allow NULL. ("First character must be a letter" is **not** universal — varies by version, cross-check the official docs.) |
| `function date_format is not supported in current mode` | `DATE_FORMAT` type / mode restriction | TIMESTAMP input → `SET odps.sql.type.system.odps2=true;`. Other types → `SET odps.sql.hive.compatible=true;`. Recommended: switch to `TO_CHAR`. |
| `function or view '<name>' cannot be resolved` | Function / view name wrong (e.g. `IFNULL`) | Check spelling. `IFNULL` does not exist — use `NVL` or `COALESCE`. |
| `Result of a union cannot be a map table` | UNION + MAPJOIN combination restriction | Rewrite SQL to avoid MAPJOIN inside UNION |
| `DDL does not support explain` | `EXPLAIN` followed by a DDL statement | Drop `EXPLAIN` |
| Other `Semantic analysis exception` | Hundreds of fine-grained scenarios | Decide based on message text + context |

### `ODPS-0130252` Cartesian product not allowed

A few well-known rewrite patterns:

| Scenario | Rewrite strategy |
|---|---|
| `CROSS JOIN + AVG/SUM` subquery | Replace with window function `AVG(x) OVER()` |
| `FROM a, b` with no `ON` | Explicit `JOIN ... ON` |
| Non-equi JOIN (the business genuinely needs CROSS-like semantics) | Add `/*+ MAPJOIN(small_table) */` |
| Full cross-tagging (**use with care**) | Dummy key: each side `SELECT *, 1 AS jk`, JOIN `ON jk=jk` |

> Dummy-key full cross is an **N×M Cartesian product** — result size explodes. **Use only when all of these hold**: (1) the business genuinely needs a full cross, (2) both sides have small bounded cardinality, (3) the user accepts the storage / compute cost, (4) combined with `/*+ MAPJOIN(small) */` to broadcast the small side. Otherwise use a window function / grouped aggregate / semi-Cartesian (JOIN with a filter condition) instead.

Fallback (use sparingly): `SET odps.sql.allow.cartesian=true;`

### `ODPS-0123091` Dirty value CAST failure

Adding a `CAST()` does **not** help — the `CAST` itself is the failure point. The data contains dirty values.

**Fix (in priority order)**:
1. Pre-filter: `WHERE col RLIKE '^-?[0-9]+$'` then `CAST` — explicitly control how dirty values are handled (drop / mark / route to a separate table).
2. Upstream audit: write dirty values to an audit table / log them; do not silently swallow them.
3. Fallback: `SET odps.function.strictmode=false;` or `SET odps.sql.udf.strict.mode=false;` (the public-cloud parameter name is more often the former; the name varies across environments). **This switch does NOT "drop the row"** — it makes invalid `CAST` produce `NULL` and pass through, while **the rest of the row's columns are output normally**. Downstream code must therefore distinguish "business NULL" from "CAST-failure NULL", or it will silently corrupt data quality.

---

## Runtime Errors

### `ODPS-0123065` Join exception

Two trigger paths; the error text does not directly distinguish them:

| Path | How to identify | Fix |
|---|---|---|
| User-explicit `/*+ MAPJOIN(...) */` exceeds threshold | Hint visible in the SQL | Drop the hint, let the CBO decide |
| CBO auto-MAPJOIN (no hint) | Error text contains `small table exceeds when auto map join applied` | `SET odps.optimizer.auto.mapjoin.threshold=<smaller bytes>;` (default 128 MB = 134217728), or `0` to disable auto-MAPJOIN |

Increase MAPJOIN memory: `SET odps.sql.mapjoin.memory.max=<N>;` (unit MB, common 1024–4096). Also check whether the JOIN key is severely skewed.

### `ODPS-0123131` User-defined function exception

The UDF threw an exception, or input data caused the UDF to fail.

**Fix**:
- Compare against the UDF signature, check input column types.
- Add try/catch inside the UDF as a guard so dirty values do not fail the whole job.
- Add `WHERE` pre-filtering upstream.
- Optimize the UDF code (may require the UDF author).

### `ODPS-0123144` UDF timeout

Error text contains `kInstanceMonitorTimeout` + `usually caused by bad udf performance`. Root cause: UDF operation is too slow (infinite loop / complex algorithm / external call).

**Fix (in recommended order)**:
1. **Localize the slow point first**: read logview UDF profiling. Identify whether it is an infinite loop, a complex algorithm, an external call, or one bad row stuck.
2. Add upstream `WHERE` pre-filtering of abnormal inputs (bad data is a common root cause; faster to catch than tuning timeout).
3. `SET odps.sql.executionengine.batch.rowcount=32;` (smaller per-batch row count, mitigates batch-internal bad-data chain reactions).
4. Optimize UDF code (cure: remove slow external calls, add caching, change the algorithm).
5. **Temporarily** widen the timeout: `SET odps.sql.udf.timeout=3600;` (range 0–3600s, default 600; `odps.function.timeout` is a legacy alias) — only after the above measures all fail and you need urgent output.

### `ODPS-1850001` MCQA query-acceleration mode restriction

| Scenario | Action |
|---|---|
| UDF triggers fallback | `SET odps.mcqa.disable=true;` |
| DML (`INSERT` / `UPDATE` / `DELETE`) | MCQA only supports DDL/DQL; you must turn MCQA off |
| Other restrictions | Disable MCQA and retry |

### `ODPS-0140171` Sandbox violation / archive load failure

Three different errors share this code; **do not handle uniformly**:

| Message keyword | Nature | Fix |
|---|---|---|
| `permission denied to read archive resource` / `not allow symlink in archive files` | Hive Bridge sandbox **Java archive load restriction** (protects third-party Java code loading — not data ACL) | Only when the scenario is external table + TextFile + LazySimpleSerDe, try `SET odps.ext.hive.lazy.simple.serde.native=true;` to switch to the native reader and bypass the Hive Bridge. Other formats not applicable. |
| `PanguPermission` / `permission denied for volume` | **Volume data access permission** (data ACL) | Must go through authorization (`GRANT Read ON VOLUME ...`) — **cannot be bypassed via SET** |
| Non-external-table `Access Denied` | Table / column ACL | Ask the owner to grant access |

**Important**: `odps.ext.hive.lazy.simple.serde.native=true` is **not** an ACL bypass flag — it only switches the read code path. The sandbox protects Java-code-load safety; it has no effect on "data-level permission denied".

### UDF registration / invocation issues

Split by root cause:

**(A) Registration-side issues (jar / annotation / class signature / environment) — fix the UDF itself, do not change the calling SQL**

| Error keyword | Fix direction |
|---|---|
| `cannot be loaded from any resources` | Check whether `CREATE FUNCTION ... USING '<jar>'` resource is uploaded |
| `does not match annotation` | Make the UDF class's `@Resolve` annotation match the actual signature |
| `UnsatisfiedLinkError` | Java version / native library mismatch |
| `Invalid function class ... static evaluate method` | UDF class signature wrong (missing `evaluate` method or wrong parameter types) |

**(B) Invocation-side issues (parameter type mismatch) — change the input type in SQL**

| Error keyword | Fix direction |
|---|---|
| `Wrong arguments UDTF ... initialize returned failed` | UDTF input parameter type mismatch; `CAST(col AS <expected>)` explicitly in the calling SQL |
| `cannot match any overloaded functions with (...)` | Invocation type does not match any UDF overload; `CAST` inputs per the signature |
