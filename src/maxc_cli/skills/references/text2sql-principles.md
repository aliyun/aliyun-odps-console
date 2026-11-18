# Draft SQL against the right data

Start with the requested deliverable. For “write the SQL, don't run it,” use the supplied schema and return a draft; do not run cost, explain or query. If a needed column, partition value or business definition is missing and lookup is outside the request, ask or mark a placeholder rather than fabricating a value. Use the user's requested format, not a fixed JSON report.

For authorized discovery, use `meta describe --metadata-only` for live column names, types and partition keys. It deliberately returns **no** enum values, rows or partition values. Request a small, scoped data read only when the task actually calls for it. Verify two- versus three-tier names in the execution project: table names in a proposal or a cache are not proof that the caller can read them.

For a metric, establish the result grain and business definition. A column named `amount` doesn't tell you which statuses count as revenue; a plausible join can double-count the measure. Use verified semantics or ask about material ambiguity. Follow [partition discovery](partition-guide.md) for the intended snapshot or time range; a sample day cannot establish a multi-day total.

Use [the SELECT dialect guide](maxcompute-select-guide.md) for MaxCompute-specific syntax, [query patterns](sql-query-patterns.md) when one fits, and [registered functions](registered-functions.md) for user UDFs. For an execution request, take the verified SQL to [query and job handling](query-jobs.md). Distinguish an unexecuted draft, an estimate, and an answer backed by returned rows.
