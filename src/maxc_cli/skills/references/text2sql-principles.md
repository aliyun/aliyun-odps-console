> Loaded on demand — intent parsing, schema mapping, JOIN/aggregation/filter rules for NL→SQL. Skip unless the agent is generating SQL from a natural-language question.

# Text2SQL General Generation Principles

Read this file when generating a SELECT query from a natural-language question plus a table schema. This file only covers intent parsing, schema mapping, result granularity, JOINs, aggregation, filtering, and the output contract. MaxCompute syntax, function names, partition format, and `SET` parameters are covered by [maxcompute-select-guide.md](maxcompute-select-guide.md). For partition discovery via the CLI, see [partition-guide.md](partition-guide.md).

---

## 1. Generation Order

1. Confirm available tables and columns: only use the schema given in context — do not invent table names, column names, enum values, date context, or join keys.
2. Decide result granularity: each output row represents an entity row, a time bucket, a group, or a ranking row.
3. Choose the minimal query: SELECT only the columns required, JOIN only the tables required, do not add `DISTINCT` by default, do not add unrelated business filters.
4. Write MaxCompute-executable SQL in one pass: this file only handles logical planning; the final syntax follows [maxcompute-select-guide.md](maxcompute-select-guide.md). Do not produce an intermediate ANSI-SQL draft.

If no usable table is available, or the schema cannot answer the question, return empty SQL and explain what is missing.

---

## 2. Schema Mapping

- Prefer table descriptions, column comments, and value-domain hints. When a comment is closer to the user's intent than the column name, pick by the comment and record the choice in `assumptions`.
- When the user mentions an entity attribute (name, status, age, category, etc.), pick the table that carries that attribute, even if the user did not name a table directly.
- When no out-of-the-box metric exists, derive one — e.g. revenue can be `price * quantity`. Put the derivation in `explanation` or `assumptions`.
- If two tables have no reliable join path, do not force a JOIN; check for a bridge / relationship table first. If still uncertain, return empty SQL or document the assumption.

---

## 3. Aggregation and Filtering

- "How many records / orders / events" → `COUNT(*)`. "How many users / customers / products" → `COUNT(DISTINCT entity_id)`, unless the schema already guarantees one entity per row.
- In aggregate queries, every non-aggregate column in `SELECT` must appear in `GROUP BY`. Filter aggregate results with `HAVING`, not `WHERE`.
- For ratios, classifications, conditional counts: use `CASE WHEN` (or the dialect-equivalent), and make the numerator and denominator explicit.
- When value-domain hints are available, resolve filter values from them — never invent enum values.
- Relative time references must use the date context that was provided. With no date context, use a placeholder and record it in `assumptions`.
- When the schema marks a column as a partition column, push time-range filters down to that partition column. If the dialect requires a partition filter but the user gave no time range, follow the dialect rules (latest partition, placeholder, or full-scan switch) and record it in `assumptions`.

---

## 4. JOIN and Ranking

- Pick the driving table first: usually the entity at the heart of the question, or the fact table.
- When you need to keep all rows of the driving table, use `LEFT JOIN` — e.g. "all users and their order count".
- When you only need matched rows, use `INNER JOIN` — e.g. "users who placed an order".
- Every JOIN must have an explicit `ON` condition. Do not use comma-implicit JOINs.
- Watch for one-to-many JOINs that inflate metrics. Pre-aggregate the many-side to the target granularity before the JOIN.
- For global Top/Bottom: `ORDER BY ... LIMIT N`. If the user did not specify N, decide based on context whether to add a `LIMIT` and record it in `assumptions`.
- For "Top N per group" / "latest row per entity", use window functions `ROW_NUMBER()` / `RANK()`. Do not substitute a global `LIMIT`.

---

## 5. Output Contract

If the user or caller specified an output format, follow it. Otherwise, when generating a SELECT from natural language, return raw JSON without a markdown code fence:

```json
{
  "sql": "<generated SELECT query>",
  "explanation": "<brief explanation>",
  "tables": ["table1"],
  "assumptions": ["assumption if any"]
}
```

When you cannot answer:

```json
{
  "sql": "",
  "explanation": "Cannot generate SQL: <reason>",
  "tables": [],
  "assumptions": []
}
```

SQL formatting: major clauses on separate lines; `JOIN ... ON` conditions indented; `WHERE` conditions one per line; SQL keywords UPPERCASE; string literals in single quotes.

---

## 6. Anti-Patterns

- `SELECT *`, unless a template or the user explicitly asks for all columns.
- Inventing table names, columns, enum values, date context, or join keys.
- JOIN without an `ON` condition; substituting a global `LIMIT` for a per-group Top-N.
- Aggregate query missing `GROUP BY`; using `WHERE` to filter aggregate results.
- Unnecessary `DISTINCT`.
- Adding business filters that the question did not ask for.
