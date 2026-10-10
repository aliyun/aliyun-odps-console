# Reference already registered functions

Use this workflow for an existing MaxCompute function. Registration and resource
publication belong to a separately authorized task. Commands require MaxC 0.9.0
or later. Reuse the session User-Agent from SKILL.md.

## Resolve the function and its contract

1. Establish the SQL execution project separately from the function's project.
   Use verified project/schema values. `meta list-schemas` can establish the
   namespace model; a denied schema lookup is not evidence of a two-tier project.
2. If the registered alias is known, describe it directly. Otherwise, list a
   bounded page in the relevant scope with an optional alias prefix:

   ```bash
   {{cli}} meta list-functions --project <function-project> --prefix <prefix> --limit 20 --user-agent "$UA" --json
   {{cli}} meta describe-function <function-alias> --project <function-project> --user-agent "$UA" --json
   ```

   Add `--schema <verified-schema>` for a schema-scoped function. Describe takes
   a bare alias, not a SQL-qualified reference. List returns `data.functions`
   and `data.pagination`; reuse `next_cursor` with the same project, schema,
   and prefix. Limit is 1–1000 (default 50). Pages are live catalog windows,
   not a snapshot; offset pagination can rescan earlier entries. A list reads
   collection metadata only, without per-function Read requests or resource
   downloads. Missing fields remain null. An empty page establishes no visible
   matching functions in that scope, not global absence.
3. Describe returns `data.function`: alias, implementation class, exact resource
   references, owner, creation time, and additional registration flags if the
   service supplies them. It excludes implementation code and resource contents.
   `signature` and `runtime_version` remain null: class names, filenames, and
   creation times do not establish a signature, Python version, function kind,
   business definition, or immutable implementation version.
4. Obtain UDF/UDAF/UDTF kind, overload/input/output types, output column names
   for UDTF, NULL behavior when relevant, and required settings from the owner's
   documentation, a trusted semantic definition, or verified user SQL. Ask only
   for missing facts that affect the call. Disambiguate same-name candidates.
   A user with known SQL and Function Execute can proceed even if Function Read
   or project List is denied. Preserve the permission error; do not relabel it
   as an absent function or switch credentials.

## Generate and execute the SQL

The function's namespace creation mode must match the execution mode.
Two-tier cross-project calls use `project:function(...)`. Three-tier objects
use `project.schema.function(...)`, or `schema.function(...)` within one
project. Verify the syntax in the actual project; do not mechanically rewrite
colon references into dotted references. Never enable schema mode merely
because the default schema is named `default`.

Preserve author-required leading SET statements in the SQL file for both cost
and execution. For example, a Python 3.11 function can require
`SET odps.sql.python.version=cp311;`. Do not apply that version to every Python
function. Conflicting Python or namespace requirements need resolution before
submission. See the official [schema operations](https://help.aliyun.com/zh/maxcompute/schema-related-operations)
and [Python 3 UDF documentation](https://help.aliyun.com/zh/maxcompute/python-3-udfs).

Use a scalar UDF as an expression, UDAF in the correct aggregation grain, and
UDTF with the correct output shape (often `LATERAL VIEW`). Use explicit casts
only when the contract supports them. For a newly integrated function, prefer
constant inputs or a known small partition for a first check; `LIMIT` alone
does not bound scan work or UDF invocations.

```bash
{{cli}} query cost --file <verified-query.sql> --project <execution-project> --user-agent "$UA" --json
{{cli}} query --file <verified-query.sql> --project <execution-project> --wait 0 --user-agent "$UA" --json
{{cli}} job wait <job-id> --project <execution-project> --user-agent "$UA" --json
```

Apply the existing cost/authorization gates. Cost or EXPLAIN success is a
planning check, not proof that the UDF runs correctly. Wait for the job's
terminal state and verify result values, types, and cardinality. If another
result page is needed, use the returned cursor without resubmitting the SQL.
Retain the submitted SQL, settings, function scope, contract source, instance
ID, and actual result in the requested delivery. Function Execute and dependent
resource access are decided by MaxCompute under the configured caller identity.
Report permission, signature, missing dependency, namespace, and runtime errors
as distinct failures; do not repair them by registering a replacement function.
