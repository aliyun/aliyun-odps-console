# Queries, results and jobs

## Before submission

For a cost-only or explain-only request return `data.analysis` and stop. `query cost` estimates the SQL; it doesn't run it or grant approval. For subsequent authorized execution use the same SQL file, leading SET hints, project and namespace. `LIMIT` bounds returned rows, not scanned partitions. Use `--cost-check` for an authorized CU ceiling rather than raising it after rejection.

```bash
{{cli}} query cost --file "$SQL_FILE" --project "$PROJECT" --user-agent "$UA" --json
```

For execution use `query` (not `sql`); `--wait 0` submits without waiting. Use `--mcqa` for MCQA v1, `--maxqa` with a verified quota/`mcqa.quota_name` for v2, or `--no-mcqa` to override an interactive default. Do not guess a quota. For non-read SQL the exact statement including SET, scope and effect must already be authorized before `query` or `job submit` with `--force`.

```bash
{{cli}} query --file "$SQL_FILE" --project "$PROJECT" --wait 0 --user-agent "$UA" --json
```

## With a job ID, resume instead of submitting

| Observation | Next action |
|---|---|
| Submission or wait is `pending` | Keep `metadata.job_id` **verbatim** and its owning project. An MCQA v1 ID may contain `@`; the parent instance alone is not the subquery. `job status` inspects; `job wait --timeout` waits on that ID. A wait timeout is not cancellation. |
| Successful `job wait` already has `data.result` | Use the rows (including a valid empty result). No second result fetch or second SQL submission is required. |
| Need the result or another page | Use `job result` for the same ID and project. Reuse the returned `data.pagination.next_cursor` with its original scope. If `has_more` is true but cursor state is missing, read from that job again with a bounded `--max-rows`; distinguish re-read rows from a new page. |
| Job completed but result download failed | Report execution and retrieval separately. Retry the **read** from the same job/page if appropriate. `--wait` / `--timeout` govern job polling; `result_read_timeout` governs result-reader waits, not a whole-command deadline. |
| The user only asks for progress, diagnosis or logs | Inspect the existing job. Cancel only when the user authorizes cancellation of that exact job; never cancel as a timeout remedy. |

```bash
{{cli}} job wait "$JOB_ID" --project "$PROJECT" --timeout 60 --user-agent "$UA" --json
{{cli}} job result "$JOB_ID" --project "$PROJECT" --cursor "$NEXT_CURSOR" --max-rows 50 --user-agent "$UA" --json
```

`job wait --stream` emits buffered NDJSON lifecycle records after the wait, not live server events; read the terminal record. Row streams and result files have other shapes. Before replacing any requested result file, confirm the exact local path and `--overwrite`.

## Diagnostic evidence

Start with `job diagnose` and the returned task name. `job task-detail` exposes operator detail; `job task-summary` may report `available=false`. A session-wide SQLRT summary does not describe an individual subquery. For one worker, use `job workers` then its returned `log_id` for `job worker-log`. A limited log is not evidence of completeness. Redact signed URLs and sensitive log contents.

```bash
{{cli}} job diagnose "$JOB_ID" --project "$PROJECT" --user-agent "$UA" --json
{{cli}} job workers "$JOB_ID" --task-name "$TASK" --project "$PROJECT" --user-agent "$UA" --json
{{cli}} job worker-log "$JOB_ID" "$LOG_ID" --size 4096 --project "$PROJECT" --user-agent "$UA" --json
```

Use `job diagnose --stream` when the user wants deeper Gateway diagnosis. It uses
the current MaxCompute identity and model analysis may consume MaxAgent Credits.
Pass the original job ID and the owning project when the CLI cannot resolve it.
Read the [last JSONL record](json-output-format.md#live-job-diagnosis) for the diagnosis
result and any missing evidence.

```bash
{{cli}} job diagnose "$JOB_ID" --project "$PROJECT" --user-agent "$UA" --stream
```
