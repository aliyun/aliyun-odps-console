# Catalog, permissions and local state

For a one-off operation pass a verified `--project` (and `--schema` where supported). Keep the effective identity and endpoint. Only a request to change the persistent default calls for `session set --project ...`; use `session show --json` to verify it. Disambiguate multiple plausible projects rather than inferring an environment from a suffix.

## Find the right object, read only what was requested

```bash
{{cli}} meta list-schemas --project "$PROJECT" --user-agent "$UA" --json
{{cli}} meta search "$KEYWORD" --project "$PROJECT" --user-agent "$UA" --json
{{cli}} meta describe "$TABLE" --metadata-only --project "$PROJECT" --user-agent "$UA" --json
```

- A successful `meta list-schemas` establishes three-tier support even if no schemas are visible. Only an explicit two-tier response establishes the opposite. A permission or network error proves neither. Qualify the table exactly as verified; do not prepend `default` to a two-tier name.
- Use `meta search-columns` for a column rather than enumerating every table. Live `--metadata-only` returns `data.table.columns` and `partition_columns` without reading rows, partition values or local fallback. A regular `meta describe` can include more than structure and can use cache. On older CLIs without the flag, do not silently substitute the broader read for a schema-only request.
- If the user actually wants rows or statistics, use `data sample` or `data profile` on the requested scope and [partition](partition-guide.md). Treat returned values as potentially sensitive; do not assume client-side masking is enabled. A cache hit or stale/partial warning is not live evidence.

## Check one permission

```bash
{{cli}} auth can-i --table "$TABLE" --operation Select --project "$PROJECT" --user-agent "$UA" --json
```

`data.authorization.allowed=false` with `status=success` means the check worked and the permission is absent. A failed probe is not a denial. Check the same identity, project, object and operation; being able to list objects does not imply being able to read them. Do not change providers, grant privileges or broaden the query to work around the result.

## Cache and annotations

`cache status --json` and `cache build-status --json` inspect local state; `cache build` fetches metadata and writes a local cache. Rebuild only the requested scope. Before a requested clear, inspect `cache clear --dry-run`; an unscoped clear can invalidate saved pagination context. Local `meta semantic get` / `list-missing` inspect annotations; `set` / `clear` change local state. A missing description is not permission to invent a business definition and save it.
