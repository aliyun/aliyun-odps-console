# Partition Query Strategy

## When To Read This File

Read this file when working with partitioned MaxCompute tables — before constructing SQL with partition filters, when `meta describe` shows `partition_columns`, or when the user asks about "latest data" or "recent records".

## Partition Column Conventions

Common partition column names:

| Column | Typical meaning |
|--------|----------------|
| `ds` | Date string, e.g. `20260415` or `2026-04-15` |
| `dt` | Same as `ds` (alias used in some projects) |
| `pt` | Partition, often date-based |
| `hh` | Hour (`00`–`23`), used in multi-level partitions |
| `region` | Geographic or business region |

### Table Name Suffix Conventions

| Suffix | Meaning | Partition semantics |
|--------|---------|-------------------|
| `_df` | Daily Full snapshot | Each partition is a complete copy of the entire dataset |
| `_di` | Daily Incremental | Each partition contains only that day's changes |
| `_hf` | Hourly Full snapshot | Each partition is a complete hourly snapshot |
| `_hi` | Hourly Incremental | Each partition contains only that hour's changes |

For `_df` tables, the latest partition alone gives you the full current dataset.
For `_di` tables, you typically need a date range to reconstruct the complete picture.

## Discovering Partitions with maxc

### Step 1: Check if the table is partitioned

```bash
{{cli}} meta describe <table> --json
```

Inspect `data.table.partition_columns` in the response. If empty or absent, the table is not partitioned — query directly with `LIMIT`.

### Step 2: List available partition values

```bash
{{cli}} meta partitions <table> --json
```

Returns all partition values with their creation times. Useful for understanding the partition range and format.

### Step 3: Get the latest partition

```bash
{{cli}} meta latest-partition <table> --json
```

Returns the most recent partition value and its format. **Always use the exact format returned** — do not assume `YYYYMMDD` vs `YYYY-MM-DD`.

### Step 4: Check data freshness

```bash
{{cli}} meta freshness <table> --json
```

Returns staleness analysis: hours since the latest partition, expected refresh frequency, and whether the table appears stale.

## Query Patterns

### Latest snapshot (for `_df` tables)

```bash
# 1. Get the latest partition value
{{cli}} meta latest-partition my_table_df --json
# Response: {"partition_value": "20260415", "partition_column": "ds", ...}

# 2. Query using the exact value
{{cli}} query "SELECT col1, col2 FROM schema.my_table_df WHERE ds = '20260415' LIMIT 100" --json
```

### Date range query (for `_di` tables)

```bash
{{cli}} query "SELECT * FROM schema.my_table_di WHERE ds >= '20260410' AND ds <= '20260415' LIMIT 100" --json
```

### Cross-partition aggregation

```bash
{{cli}} query "SELECT ds, COUNT(1) AS cnt FROM schema.my_table GROUP BY ds ORDER BY ds DESC LIMIT 20" --json
```

### Multi-level partition pruning

For tables with partitions like `(ds, hh)`:

```bash
{{cli}} query "SELECT * FROM schema.my_table WHERE ds = '20260415' AND hh = '12' LIMIT 100" --json
```

## MAX_PT() Guidance

`MAX_PT('table_name')` is a MaxCompute built-in macro that returns the value of the largest partition.

### When MAX_PT() works well

- Scheduled SQL jobs that always need the latest partition
- Single-level partitions where the latest partition is always complete

### When MAX_PT() is unreliable

- **Ad-hoc queries**: The latest partition may be incomplete (still loading)
- **Multi-level partitions**: MAX_PT only considers the first partition level
- **Specific historical dates**: You need a fixed date, not "whatever is latest"
- **Incremental tables**: The latest partition alone may not contain the data you need

### Recommended alternative

Use `{{cli}} meta latest-partition <table> --json` to get the latest value, then use that literal value in your WHERE clause. This is more explicit and avoids the pitfalls of MAX_PT.

## Partition Ambiguity Handling

When the user says "latest data" or "most recent records" and the partition semantics are unclear:

1. **Check `partition_columns[*].comment`** — the column comment may describe the semantics
2. **Check the table comment** via `meta describe` — it may describe the refresh pattern
3. **Sample partition values** with `meta partitions` — check if values look like dates, and whether they cover recent days
4. **Ask the user** if the above steps don't clarify whether the table is a full snapshot or incremental

Do not silently assume snapshot semantics. If ambiguous, state the assumption before executing.

## Cost Impact

Querying a partitioned table **without** a partition filter can scan the entire table — potentially terabytes of data.

- Always use `{{cli}} query cost "..." --json` on unfamiliar partitioned tables before executing
- Adding a partition filter can reduce scanned data by 100x–1000x
- For tables with years of daily partitions, a single partition is typically 1/365th of the total data
