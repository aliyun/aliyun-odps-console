# Tunnel transfers

Use `data sample` for an inline preview; `data download` creates a local file. Tunnel transfer requires an existing table, supports primitive columns and needs the full partition spec for a partitioned table. Verify the table, column order/types and partition keys; never turn a failed upload into table DDL or SQL `INSERT`.

## Validate versus write

```bash
{{cli}} data upload "$TABLE" --file "$INPUT" --partition "$PARTITION" --project "$PROJECT" --dry-run --user-agent "$UA" --json
```

Omit `--partition` for non-partitioned tables. Dry-run validates the file and target without committing rows; for “check this file” stop and report validation. A write action in the response is not authorization. CSV uses a header by default; `--no-header` maps by position. Verify delimiter and NULL representation when importing TSV or round-tripping an export (upload and download defaults differ).

For an independently authorized write, use the reviewed input, mode and target. Append is the default; `--overwrite` replaces the table or named partition. `--create-partition` is a separate metadata write and can leave an empty partition after later failure. On success check `data.rows_written`, `applied_partition` and `overwrite`. For a bad row report the line/column, not its sensitive value. If `error.context.remote_commit_state` is `unknown` or the code is `UPLOAD_COMMIT_OUTCOME_UNKNOWN`, **do not replay**: inspect the target before considering another append or overwrite.

## Download to a new path

```bash
{{cli}} data download "$TABLE" --partition "$PARTITION" --columns "$COLUMNS" --limit 1000 --output "$NEW_FILE" --project "$PROJECT" --user-agent "$UA" --json
```

An existing file is protected; do not delete it or add `--overwrite` to make a failed command pass. Obtain approval for that exact replacement or use an agreed new path. Return `data.output_path`, `rows_written`, and whether `data.truncated` or warnings indicate a partial export, not the contents of the file.
