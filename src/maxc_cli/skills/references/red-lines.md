# Recover without changing intent

Read `status`, `error.code`, `error.context`, `error.suggestion`, `error.recovery_steps` and warnings. If a suggested action needs placeholders, confirmation, or is not agent-allowed, it is not a runnable follow-up. Even a runnable action must serve the user's actual request. If one targeted correction does not resolve an uncertain target or authority, stop and explain the blocker.

| Signal | What can be done safely |
|---|---|
| `PERMISSION_DENIED` | Verify the effective identity and exact project/schema/object/operation using [access checks](command-patterns.md). Do not replace a working provider, infer a missing object or grant permissions. |
| `NOT_FOUND`, empty search or unresolved namespace | Recheck the verified scope. An empty visible list does not prove global absence. |
| `WRITE_OPERATION_REQUIRES_FORCE`, or a suggested overwrite | Return the proposed effect for review. A dry-run, earlier read-only request or error is never consent to mutate. This applies to leading SET context as well as the statement. |
| `COST_LIMIT_EXCEEDED` | Narrow the scan or ask about the budget. Do not silently raise the threshold. |
| `SQL_ERROR` | Use the reported phase/code and [SQL recovery](sql-common-errors.md); do not resubmit an unchanged statement. |
| `pending`, wait timeout or result-reader failure | Inspect [the original job](query-jobs.md). Retry only a suitable read, not the submission. Lost local cursor state need not mean lost remote results. |
| `CSV_PARSE_ERROR` or interrupted Tunnel commit | Locate the invalid row/column without disclosing raw input. For an unknown commit outcome, reconcile [the original transfer](transfers.md); a second append can duplicate data. |
| `SEMANTIC_WRITE_UNCERTAIN` or revision conflict | Read the named package and revisions. Reconcile and review [a fresh plan](semantic-packages.md) rather than replaying publish/apply. |
| Missing authentication or connection | Follow [bootstrap](bootstrap-flow.md). `configured` in local context and a completed login attempt are not proof of readiness. |

Give the user the sanitized job/request ID, observed outcome and next decision. Keep signed LogView URLs, tokens, credential-helper output and sensitive rows/logs out of the report.
