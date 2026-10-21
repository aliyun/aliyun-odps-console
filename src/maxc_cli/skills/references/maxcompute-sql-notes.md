> Loaded on demand — CLI-side SQL behaviors: SET injection, write gate, result fetching, upload semantics. Skip unless the agent needs to know how `{{cli}}` wraps SQL execution.

# MaxCompute SQL via maxc-cli — CLI-side knobs

Read this file for `{{cli}}`-specific SQL behaviors: how `{{cli}} query` injects SET options, how the client-side write gate works, how result fetching behaves, and how `{{cli}} data upload` maps to `INSERT INTO` / `INSERT OVERWRITE` semantics.

For SQL dialect rules (NULL handling, date functions, types, JOIN semantics, window frames, SET parameter semantics), see [maxcompute-select-guide.md](maxcompute-select-guide.md). For ODPS error code recovery, see [sql-common-errors.md](sql-common-errors.md). For NL→SQL planning, see [text2sql-principles.md](text2sql-principles.md). For partition discovery, see [partition-guide.md](partition-guide.md).

## SET Options via maxc-cli

`maxc-cli` supports inline `SET` statements before SQL. The SET values are passed to MaxCompute as execution hints:

```bash
{{cli}} query "SET odps.sql.type.system.odps2=true; SELECT CAST(id AS INT) FROM schema.table LIMIT 10" --json
```

Multiple SET statements can be chained:

```bash
{{cli}} query "SET odps.sql.type.system.odps2=true; SET odps.sql.hive.compatible=true; SELECT ..." --json
```

For the meaning of each SET option (which switches enable which types / dialect features), see [maxcompute-select-guide.md](maxcompute-select-guide.md) §12.

A leading `SET` belongs to the same authorized execution context as the SQL.
Its syntax is parsed locally and its values are passed as hints to MaxCompute.
There is no client-side SET allowlist. Security, access-control and masking
parameters require explicit approval and `--force`, even alongside SELECT.
MaxCompute validates the accepted parameter keys, values and caller permissions.

## SQL Write Gate

The client recognizes SELECT, SHOW, DESC, DESCRIBE, EXPLAIN and proven read-only
WITH queries. Every other operation requires explicit user authorization and
`--force`. New or administrative SQL is treated as a write; its operation name
is not a client-side reason to reject an authorized statement.

Verify the exact SQL, SET context, project, schema, target and effect, then
submit one executable statement at a time with `--force`. MaxCompute owns
syntax validation and permission checks. The `odps.sql.read.only` hint is not
injected. A gate error is not permission to infer a write or automatically
replay a suggested action.

The gate applies to SQL only. `{{cli}} data upload` is a separate Tunnel write
path and retains its authorization and overwrite boundaries.

## Result Fetch: `--max-rows` and the 10k Tunnel Threshold

- Default `--max-rows` is 100. Use `--max-rows N` to retrieve up to N rows.
- Results larger than 10,000 rows are fetched via Instance Tunnel automatically — no extra configuration needed.
- For larger results, use output redirection or pagination via `--cursor`.
- `LIMIT` without `ORDER BY` returns **non-deterministic** rows — the same query may return different rows each run. See [maxcompute-select-guide.md](maxcompute-select-guide.md) §1 for the dialect rule on `ORDER BY`+`LIMIT` pairing.

## INSERT Semantics ↔ `{{cli}} data upload`

Understanding write semantics helps interpret data patterns and pick the right upload mode:

| SQL statement | Effect | `{{cli}} data upload` equivalent |
|---|---|---|
| `INSERT INTO` | Append rows to the table/partition | `{{cli}} data upload <table> --file path.csv [--partition ...]` (default append) |
| `INSERT OVERWRITE` | Replace all data in the target table/partition | `{{cli}} data upload <table> --file path.csv [--partition ...] --overwrite` |

Data interpretation hints (when reading existing data, not writing):
- **Duplicate rows** may indicate multiple `INSERT INTO` runs.
- A failed CLI upload does not authorize an automatic retry. Inspect the
  target and original job/session outcome before deciding whether a new append
  or overwrite is safe. An explicitly created partition may remain empty.
- **Missing recent partitions** may indicate the ETL pipeline is delayed.

Note: `{{cli}} data upload` goes through Tunnel (no SQL CU consumed), supports primitive types only (no array/map/struct), is fail-fast on bad rows, and requires the target table to already exist. For very large or parallel transfers, use `odpscmd tunnel` with multiple threads.
