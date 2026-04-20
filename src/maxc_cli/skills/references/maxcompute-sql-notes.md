# MaxCompute SQL Notes

## When To Read This File

Read this file when:
- SQL behaves differently from MySQL, Hive, or standard ANSI SQL
- You need to understand MaxCompute-specific SET options
- A query fails with type conversion or syntax errors
- You need to interpret data patterns (duplicates, partial data)

## SET Options via maxc-cli

maxc-cli supports inline SET statements before SQL. The SET values are passed as execution hints to MaxCompute:

```bash
maxc query "SET odps.sql.type.system.odps2=true; SELECT CAST(id AS INT) FROM schema.table LIMIT 10" --json
```

Multiple SET statements can be chained:

```bash
maxc query "SET odps.sql.type.system.odps2=true; SET odps.sql.hive.compatible=true; SELECT ..." --json
```

**Note**: maxc-cli blocks write operations (INSERT, CREATE, DROP, ALTER, etc.) client-side before submission. The `odps.sql.read.only` hint is not injected. Use `--force` to bypass for authorized write operations.

## Type System

MaxCompute has two type system modes. Many projects default to ODPS 1.0 (legacy), which limits implicit type conversions.

### Enabling ODPS 2.0 types

```sql
SET odps.sql.type.system.odps2=true;
```

This enables:
- `TINYINT`, `SMALLINT`, `INT`, `FLOAT` types
- Implicit type coercion between compatible types
- `CHAR(n)`, `VARCHAR(n)` types

### When to enable

- When you get type conversion errors like `ODPS-0130071`
- When using `CAST()` to specific types that aren't recognized
- When comparing columns of different numeric types

### Common fix pattern

If a query fails with a type error:

```bash
maxc query "SET odps.sql.type.system.odps2=true; SELECT CAST(col AS INT) FROM schema.table LIMIT 10" --json
```

## Common SET Options

| Option | Default | Effect |
|--------|---------|--------|
| `odps.sql.type.system.odps2` | `false` | Enable ODPS 2.0 type system |
| `odps.sql.decimal.odps2` | `false` | Enable DECIMAL(p,s) precision |
| `odps.sql.hive.compatible` | `false` | Enable Hive-compatible syntax |
| `odps.sql.allow.fullscan` | varies | Allow full table scan without partition filter |

## String and Comparison Behavior

- **Case-sensitive by default**: `'abc' = 'ABC'` evaluates to `false`
- Use `TOLOWER()` or `TOUPPER()` for case-insensitive comparison:
  ```sql
  WHERE TOLOWER(name) = 'john'
  ```
- `LIKE` is case-sensitive: `'ABC' LIKE '%abc%'` is `false`
- `RLIKE` uses Java regular expression syntax

## NULL Handling

- `NULL = NULL` evaluates to `NULL`, not `TRUE`
- Use `IS NULL` / `IS NOT NULL` for NULL checks:
  ```sql
  WHERE col IS NOT NULL
  ```
- `COALESCE(a, b, c)` returns the first non-NULL value
- `NVL(a, b)` returns `b` if `a` is NULL
- Aggregate functions (`COUNT`, `SUM`, `AVG`) skip NULL values
- `COUNT(*)` counts all rows including NULLs; `COUNT(col)` skips NULLs

## LIMIT and ORDER BY

- `LIMIT` without `ORDER BY` returns **non-deterministic** rows — the same query may return different rows each time
- Always add `ORDER BY` when the result order matters
- Default `--max-rows` is 100. Use `--max-rows N` to retrieve up to N rows.
- Results >10,000 rows are fetched via instance tunnel automatically — no extra configuration needed.
- For larger results, use output redirection or pagination via `--cursor`

## Date and Time Functions

Common patterns for partition date arithmetic:

```sql
-- Add/subtract days
DATEADD(TO_DATE('20260415', 'yyyyMMdd'), -7, 'dd')

-- Date difference in days
DATEDIFF(TO_DATE('20260415', 'yyyyMMdd'), TO_DATE('20260401', 'yyyyMMdd'), 'dd')

-- Format date
TO_CHAR(GETDATE(), 'yyyyMMdd')

-- Parse date string
TO_DATE('2026-04-15', 'yyyy-MM-dd')
```

## INSERT Semantics (Reference Only)

Understanding write semantics helps interpret data patterns:

| Statement | Effect |
|-----------|--------|
| `INSERT INTO` | Appends rows to the table/partition |
| `INSERT OVERWRITE` | Replaces all data in the target table/partition |

Data interpretation hints:
- **Duplicate rows** may indicate multiple `INSERT INTO` runs
- **Partial data** in a partition may indicate a failed `INSERT OVERWRITE`
- **Missing recent partitions** may indicate the ETL pipeline is delayed

Note: maxc-cli cannot execute INSERT/OVERWRITE — these are reference notes for understanding data you query.

## Common SQL Error Patterns

| Error code | Cause | Fix |
|-----------|-------|-----|
| `ODPS-0130161` | Column not found | Check column names with `meta describe` |
| `ODPS-0130071` | Type conversion error | Add `SET odps.sql.type.system.odps2=true;` |
| `ODPS-0420061` | Full scan not allowed | Add partition filter in WHERE clause |
| `ODPS-0010000` | Syntax error | Check SQL syntax; MaxCompute SQL differs from MySQL |
| `ODPS-0130013` | Table not found | Verify table name with `meta search`; check schema qualification |
| `ODPS-0130252` | Cartesian product rejected | Use `MAPJOIN` hint or add a JOIN condition |

## SQL Restrictions

### Cartesian Product and MAPJOIN

MaxCompute rejects all Cartesian products (even 2x2) unless MAPJOIN is used. This applies to comma-joins, CROSS JOIN, and JOINs with `ON 1=1`.

```sql
-- This FAILS with ODPS-0130252:
SELECT a.*, b.* FROM table_a a, table_b b;

-- Fix: use MAPJOIN hint on the smaller table
SELECT /*+ MAPJOIN(b) */ a.*, b.*
FROM large_table a
JOIN small_table b
ON 1=1;
```

MAPJOIN loads the hinted table into memory on each mapper. The hinted table must fit in memory (~64MB default).

### Generating Test Data Without Cartesian Products

Use `EXPLODE` instead of Cartesian products for test data generation:

```sql
-- Generate N rows without Cartesian product
SELECT EXPLODE(SPLIT(REPEAT('x,', 12000), ',')) AS val;
```

### Full Table Scan

The full scan restriction (`ODPS-0420061`) is **project-dependent** — some projects allow full scans by default, others require a partition filter.

```sql
-- If full scan is blocked, either add a partition filter:
SELECT * FROM partitioned_table WHERE ds = '20260415';

-- Or set the hint (if your project allows it):
SET odps.sql.allow.fullscan=true;
SELECT * FROM partitioned_table;
