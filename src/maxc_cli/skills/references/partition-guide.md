# Select the intended partition

`meta describe --metadata-only` gives partition **keys**, not values. Do not list partitions or sample data when the user only requested the schema. For a request about data, establish whether it needs one snapshot, one increment or a date range. Names like `ds` or `_df` alone do not establish date format, completeness or snapshot semantics.

```bash
{{cli}} meta latest-partition "$TABLE" --project "$PROJECT" --user-agent "$UA" --json
{{cli}} meta partitions "$TABLE" --project "$PROJECT" --user-agent "$UA" --json
```

- For “latest visible”, inspect `data.partition.latest_partition`, `latest_partition_values` and warnings. Use the actual keys and values, including secondary keys such as an hour. A null spec, a cache warning or a partial listing is not proof that today is available. `meta freshness` reports observed freshness, not an upstream completion guarantee.
- For a named day or interval, inspect matching visible specs and preserve the requested range. Do not substitute the latest partition for a missing day, or use a single increment as a full snapshot. If the business day or completeness criterion is unclear, ask.
- Put the verified predicate on the partition columns in SQL. `LIMIT` does not prune partitions. `MAX_PT` can be useful in a scheduled query whose contract really is the largest partition; it is not proof of completeness and a single value cannot specify every key of a multi-level partition.
- Tunnel upload/download takes a single full key/value spec in `--partition`, **not** a SQL WHERE clause. See [transfers](transfers.md) for the separate write and partition-creation gates.
