# MaxCompute SQL Common Pattern Templates

For text2sql scenarios, this file provides MaxCompute **DQL (SELECT query)** templates. When generating SQL, prefer matching one of these patterns over building from scratch. For partition discovery (latest partition value, partition format), see [partition-guide.md](partition-guide.md). For dialect rules, see [maxcompute-select-guide.md](maxcompute-select-guide.md).

**Template conventions**:

- `${bizdate}` is MaxCompute's scheduling-system date variable (current business date). When generating SQL, replace it with a concrete date literal (e.g. `'2024-01-15'`) or `TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyy-mm-dd')` (yesterday) per the user's intent.
- Column names in templates (`order_id`, `user_id`, …) are placeholders — replace with the real columns of the target table; prefer explicit columns over `SELECT *`. When the outer template must preserve all source columns (PIVOT output, pure paging passthrough, etc.) `*` is acceptable.

---

## 1. Top-N per Group

NL example: "Top 3 highest-paid people per department".

```sql
SELECT employee_id, name, department, salary
FROM (
    SELECT employee_id, name, department, salary,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
    FROM employees
    WHERE ds = '${bizdate}'
) tmp
WHERE rn <= 3;
```

**Note**: Do NOT use `GROUP BY + LIMIT` — MaxCompute's `LIMIT` is global. You must use a window function. In the inner subquery, only `SELECT` the business columns you will actually use plus `rn`; do not return `rn` in the final result.

---

## 2. Dedup, Keep Latest Row

NL example: "Keep each user's most recent order".

```sql
SELECT order_id, user_id, amount, create_time
FROM (
    SELECT order_id, user_id, amount, create_time,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY create_time DESC) AS rn
    FROM orders
    WHERE ds = '${bizdate}'
) tmp
WHERE rn = 1;
```

**Variant — dedup by multiple columns**:
```sql
SELECT user_id, product_id, qty, update_time
FROM (
    SELECT user_id, product_id, qty, update_time,
           ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY update_time DESC) AS rn
    FROM user_products
    WHERE ds = '${bizdate}'
) tmp
WHERE rn = 1;
```

---

## 3. Cumulative Sum / Running Total

NL example: "Cumulative sales by date".

```sql
SELECT
    ds,
    daily_amount,
    SUM(daily_amount) OVER (ORDER BY ds ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
FROM daily_sales
WHERE ds >= '2024-01-01' AND ds <= '2024-01-31';
```

**Note**: You must explicitly specify `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`; do not rely on the default frame.

---

## 4. Year-over-Year / Month-over-Month

NL example: "Monthly sales and MoM growth rate".

```sql
SELECT
    month_id,
    amount,
    LAG(amount, 1) OVER (ORDER BY month_id) AS prev_month_amount,
    ROUND((amount - LAG(amount, 1) OVER (ORDER BY month_id))
          / LAG(amount, 1) OVER (ORDER BY month_id) * 100, 2) AS mom_growth_pct
FROM monthly_sales
ORDER BY month_id
LIMIT 10000;
```

**Year-over-year (same month last year)**:
```sql
SELECT
    month_id,
    amount,
    LAG(amount, 12) OVER (ORDER BY month_id) AS same_month_last_year,
    ROUND((amount - LAG(amount, 12) OVER (ORDER BY month_id))
          / LAG(amount, 12) OVER (ORDER BY month_id) * 100, 2) AS yoy_growth_pct
FROM monthly_sales
ORDER BY month_id
LIMIT 10000;
```

---

## 5. Consecutive N Days Active

NL example: "Users who logged in for 3+ consecutive days".

```sql
SELECT user_id, MIN(ds) AS start_date, MAX(ds) AS end_date, COUNT(*) AS consecutive_days
FROM (
    SELECT user_id, ds,
           DATEADD(TO_DATE(ds, 'yyyy-mm-dd'),
                   -ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ds), 'dd') AS grp
    FROM (
        SELECT DISTINCT user_id, ds
        FROM user_login
        WHERE ds >= '2024-01-01' AND ds <= '2024-01-31'
    ) t1
) t2
GROUP BY user_id, grp
HAVING COUNT(*) >= 3;
```

**Core idea**: subtract the row number from the date — consecutive dates collapse to the same `grp` value.

> ⚠️ **Precondition**: this template assumes `ds` is ISO format (`'2024-01-15'`). If the actual `ds` is the compact format (`'20240115'`), change `TO_DATE(ds, 'yyyy-mm-dd')` to `TO_DATE(ds, 'yyyymmdd')` and adjust the WHERE-range literals to `'20240101'/'20240131'` accordingly.

---

## 6. Row-to-Column (PIVOT)

NL example: "Total amount per order type, per user".

> ⚠️ Some projects do not support PIVOT — **if execution returns `unsupported syntax`, fall back to method two (CASE WHEN)**. UNPIVOT is generally available.

**Method 1: PIVOT**
```sql
SELECT *
FROM (
    SELECT user_id, order_type, amount
    FROM orders
    WHERE ds = '${bizdate}'
) src
PIVOT (
    SUM(amount)
    FOR order_type IN ('food' AS food, 'drink' AS drink, 'other' AS other_type)
) pvt;
-- PIVOT restrictions:
--   * The top level of PIVOT (...) must be an aggregate function — you cannot wrap another regular function (e.g. ROUND(SUM(amount),2) errors)
--   * Scalar expressions inside the aggregate are allowed (e.g. SUM(amount * rate))
--   * Cannot mix in window functions / table functions; values in the IN list must be constant literals
```

**Method 2: CASE WHEN (recommended when type values are uncertain or PIVOT is unavailable)**
```sql
SELECT
    user_id,
    SUM(CASE WHEN order_type = 'food' THEN amount ELSE 0 END) AS food_amount,
    SUM(CASE WHEN order_type = 'drink' THEN amount ELSE 0 END) AS drink_amount
FROM orders
WHERE ds = '${bizdate}'
GROUP BY user_id;
```

---

## 7. Column-to-Row (UNPIVOT)

NL example: "Turn monthly metric columns into rows".

**Method 1: UNPIVOT (recommended)**
```sql
SELECT user_id, metric_name, metric_value
FROM monthly_metrics
UNPIVOT (
    metric_value FOR metric_name IN (revenue, cost, profit)
) unpvt
WHERE ds = '${bizdate}';
```

**Method 2: UNION ALL (alternative)**
```sql
SELECT user_id, 'revenue' AS metric_name, revenue AS metric_value FROM monthly_metrics WHERE ds = '${bizdate}'
UNION ALL
SELECT user_id, 'cost', cost FROM monthly_metrics WHERE ds = '${bizdate}'
UNION ALL
SELECT user_id, 'profit', profit FROM monthly_metrics WHERE ds = '${bizdate}';
```

---

## 8. Array Explode (LATERAL VIEW)

NL example: "Explode the user-tags array".

```sql
SELECT t.user_id, tag.tag_value
FROM user_tags t
LATERAL VIEW EXPLODE(t.tags) tag AS tag_value
WHERE t.ds = '${bizdate}';
```

**Explode with index**:
```sql
SELECT t.user_id, tag.pos AS tag_index, tag.val AS tag_value
FROM user_tags t
LATERAL VIEW POSEXPLODE(t.tags) tag AS pos, val
WHERE t.ds = '${bizdate}';
```

**OUTER (preserve users with no tags)**:
```sql
SELECT t.user_id, tag.tag_value
FROM user_tags t
LATERAL VIEW OUTER EXPLODE(t.tags) tag AS tag_value
WHERE t.ds = '${bizdate}';
```

---

## 9. JSON Field Extraction

NL example: "Extract user behavior from a JSON log".

```sql
-- Extract a single-level field
SELECT
    GET_JSON_OBJECT(log_content, '$.user_id') AS user_id,
    GET_JSON_OBJECT(log_content, '$.action') AS action,
    GET_JSON_OBJECT(log_content, '$.timestamp') AS event_time
FROM raw_logs
WHERE ds = '${bizdate}';

-- Extract a nested field
SELECT
    GET_JSON_OBJECT(log_content, '$.user.name') AS user_name,
    GET_JSON_OBJECT(log_content, '$.user.age') AS user_age
FROM raw_logs
WHERE ds = '${bizdate}';

-- Extract an element of a JSON array
SELECT
    GET_JSON_OBJECT(log_content, '$.items[0].name') AS first_item_name
FROM raw_logs
WHERE ds = '${bizdate}';
```

---

## 10. MAP Operations

NL example: "Extract a specific key from a property MAP".

```sql
-- Extract a MAP value
SELECT user_id, properties['city'] AS city
FROM user_profiles
WHERE ds = '${bizdate}';

-- Explode a MAP into rows
SELECT t.user_id, kv.key AS prop_key, kv.value AS prop_value
FROM user_profiles t
LATERAL VIEW EXPLODE(t.properties) kv AS key, value
WHERE t.ds = '${bizdate}';

-- Parse a string into a MAP and extract
SELECT
    STR_TO_MAP(params, '&', '=')['source'] AS traffic_source
FROM page_views
WHERE ds = '${bizdate}';
```

---

## 11. Latest Partition

For dynamically resolving the latest partition value, see [partition-guide.md](partition-guide.md). The CLI command `maxc meta latest-partition <table> --json` is preferred; the SQL-side equivalent is `MAX_PT('project.table')`.

---

## 12. EXISTS / NOT EXISTS Rewrite

NL example: "Find users who have not placed any orders".

**Method 1: LEFT ANTI JOIN (recommended)**
```sql
SELECT u.*
FROM users u
LEFT ANTI JOIN orders o ON u.user_id = o.user_id AND o.ds = '${bizdate}'
WHERE u.ds = '${bizdate}';
```

**Method 2: NOT EXISTS**
```sql
SELECT u.*
FROM users u
WHERE u.ds = '${bizdate}'
  AND NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.user_id AND o.ds = '${bizdate}'
  );
```

**"Users who placed at least one order" — LEFT SEMI JOIN**
```sql
SELECT u.*
FROM users u
LEFT SEMI JOIN orders o ON u.user_id = o.user_id AND o.ds = '${bizdate}'
WHERE u.ds = '${bizdate}';
```

---

## 13. N-Day Retention / Cohort Retention

NL example: "Daily new-user cohorts and their next-day / 7-day / 30-day retention".

```sql
-- Key: cohort_ds must be derived from "all-history MIN(ds)", otherwise existing users'
-- first-active day is truncated to the analysis-window start, miscounting them as "new users".
-- Compute cohort first, then keep only those whose cohort_ds falls inside the analysis window.
WITH cohort_full AS (
    -- All-history first-active day per user (no window restriction)
    SELECT user_id, MIN(ds) AS cohort_ds
    FROM events
    GROUP BY user_id
),
cohort AS (
    -- Keep only users whose first-active day falls inside the analysis window (i.e. real "new users")
    SELECT user_id, cohort_ds
    FROM cohort_full
    WHERE cohort_ds >= '20240101' AND cohort_ds <= '20240131'
),
active AS (
    -- Active records inside the analysis window + 30-day observation period (deduped to day grain)
    SELECT DISTINCT user_id, ds
    FROM events
    WHERE ds >= '20240101' AND ds <= '20240301'
)
SELECT
    c.cohort_ds,
    COUNT(DISTINCT c.user_id) AS cohort_size,
    COUNT(DISTINCT CASE WHEN DATEDIFF(TO_DATE(a.ds, 'yyyymmdd'), TO_DATE(c.cohort_ds, 'yyyymmdd'), 'dd') = 1
                        THEN a.user_id END) AS d1_retained,
    COUNT(DISTINCT CASE WHEN DATEDIFF(TO_DATE(a.ds, 'yyyymmdd'), TO_DATE(c.cohort_ds, 'yyyymmdd'), 'dd') = 7
                        THEN a.user_id END) AS d7_retained,
    COUNT(DISTINCT CASE WHEN DATEDIFF(TO_DATE(a.ds, 'yyyymmdd'), TO_DATE(c.cohort_ds, 'yyyymmdd'), 'dd') = 30
                        THEN a.user_id END) AS d30_retained
FROM cohort c
LEFT JOIN active a ON c.user_id = a.user_id
GROUP BY c.cohort_ds
ORDER BY c.cohort_ds;
```

**Key points**:
- `cohort_ds` uses **all-history** `MIN`, then filter to the window — otherwise existing users' first-active day gets truncated to the window start and they get miscounted as "new users".
- `DATEDIFF(active_day, cohort_day, 'dd')` gives the N-day offset; bucket by N with `COUNT(DISTINCT)`.
- If `ds` is `'yyyymmdd'`, use `TO_DATE(..., 'yyyymmdd')`; if ISO `'YYYY-MM-DD'`, use `'yyyy-mm-dd'`.

---

## 14. Range Lookup / Range Join

NL example: "Find which time period each event belongs to".

```sql
-- The table name in the hint must be the actual alias p (the broadcast side), NOT the original table name time_periods
SELECT /*+ RANGEJOIN(p, 86400) */
    e.event_id, e.event_time, p.period_name
FROM events e
JOIN time_periods p
    ON e.event_time >= p.start_time AND e.event_time < p.end_time
WHERE e.ds = '${bizdate}';
```

**Note**: Range Join needs a hint for performance. In `RANGEJOIN(table, N)`, N is the estimated match range — **same unit as the join column**. If the column is a BIGINT seconds-timestamp, N is in seconds (86400 = 1 day); if milliseconds, N is also in milliseconds.

---

## 15. Multi-dimensional Aggregation (GROUPING SETS / CUBE / ROLLUP)

NL example: "Statistics by region and product, plus a grand total".

```sql
SELECT
    region,
    product,
    SUM(amount) AS total,
    GROUPING(region) AS g_region,
    GROUPING(product) AS g_product
FROM sales
WHERE ds = '${bizdate}'
GROUP BY GROUPING SETS ((region, product), (region), ())
ORDER BY region, product
LIMIT 10000;
```

- `GROUPING(col) = 0` means the column is in the current group (participating in grouping); `= 1` means it is being aggregated (NULL placeholder).
- `GROUPING_ID(a, b, ...)` packs multiple `GROUPING()` results into a single bitmap integer.
- `CUBE(a, b)` = all combinations `(a,b), (a), (b), ()`.
- `ROLLUP(a, b)` = hierarchical combinations `(a,b), (a), ()`.

---

## 16. Pagination

NL example: "Page 3, 10 per page".

MaxCompute supports `LIMIT ... OFFSET ...`; you can also use `ROW_NUMBER` for more flexible paging.

```sql
WITH ranked AS (
    SELECT order_id, user_id, amount, create_time,
           ROW_NUMBER() OVER (ORDER BY order_id) AS rn
    FROM orders
    WHERE ds = '${bizdate}'
)
SELECT order_id, user_id, amount, create_time
FROM ranked
WHERE rn BETWEEN 21 AND 30
ORDER BY rn
LIMIT 10;
```

**Core idea**: `ROW_NUMBER` indexes the rows; `BETWEEN` selects the page range. For page P with N rows per page: `WHERE rn BETWEEN (P-1)*N+1 AND P*N`.

---

## 17. Multi-level CTE for Complex Queries

NL example: "Order statistics for high-value users".

When a query has many nested levels or the same logic is referenced multiple times, prefer `WITH ... AS` for readability.

```sql
WITH high_value_users AS (
    SELECT user_id, SUM(amount) AS total
    FROM orders
    WHERE ds >= '2024-01-01' AND ds <= '2024-01-31'
    GROUP BY user_id
    HAVING SUM(amount) > 10000
),
user_orders AS (
    SELECT o.user_id, o.order_id, o.amount, o.ds
    FROM orders o
    JOIN high_value_users h ON o.user_id = h.user_id
    WHERE o.ds >= '2024-01-01' AND o.ds <= '2024-01-31'
)
SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM user_orders
GROUP BY user_id
ORDER BY total_amount DESC
LIMIT 100;
```

**Recursive CTE**: `WITH RECURSIVE cte AS (base_query UNION ALL recursive_query) SELECT * FROM cte;`

Recursive CTE runtime constraints (without a termination condition you generate non-runnable SQL):
- Available by default only in **offline mode**; **MCQA / MCQA2 reject it** (compile-time error `rcte.session.mode`). To use under MCQA, first `SET odps.mcqa.disable=true;`.
- Default max iterations 10, hard limit 100; larger values are clamped. Raise via `SET odps.sql.rcte.max.iterate.num=100;`.
- The recursive branch must have a **converging termination condition** (e.g. `WHERE level < 10`); otherwise even hitting the iteration cap raises an error.
