# MCMCP Access: Verified Auth and Tool Contract

Research findings for implementing `aliyun maxc kb ask|search` (request R2).
Verified 2026-09-20 against the live endpoint and the official reference
[`aliyun/alibabacloud-maxcompute-mcp-server`](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server).

Everything here is observed, not inferred. Where a probe produced an error, the error is quoted.

---

## 1. The endpoint exists and is publicly reachable

```
POST https://mcp.cn-shanghai.maxcompute.aliyun.com/mcp
→ HTTP/1.1 401 Unauthorized
  Www-Authenticate: Bearer scope="maxcompute:read maxcompute:sql"
  Server: APISIX/3.14.1
```

Endpoint forms from the product docs:

| Network | China site | International site |
|---------|-----------|--------------------|
| Public | `https://mcp.maxcompute.aliyun.com/mcp` | `https://mcp-intl.maxcompute.aliyun.com/mcp` |
| Fixed region | `https://mcp.<regionId>.maxcompute.aliyun.com/mcp` | `https://mcp-intl.<regionId>.maxcompute.aliyun.com/mcp` |
| VPC | `https://mcp.<regionId>-vpc.maxcompute.aliyun-inc.com/mcp` | intl equivalent |

Transport is **Streamable HTTP** (not SSE-only).

## 2. How the bearer token is obtained — the important part

There are two plausible mechanisms and only one is used by the official client.

### ❌ What we do NOT use: the OAuth browser flow

The host does publish standard OAuth AS metadata at
`/.well-known/oauth-authorization-server`:

```json
{
  "authorization_endpoint": ".../oauth/authorize",
  "token_endpoint": ".../oauth/token",
  "registration_endpoint": ".../oauth/register",
  "grant_types_supported": ["authorization_code", "refresh_token"],
  "code_challenge_methods_supported": ["S256"],
  "scopes_supported": ["maxcompute:read", "maxcompute:sql"],
  "token_endpoint_auth_methods_supported": ["none"]
}
```

Dynamic registration works (`POST /oauth/register` → `201`, public client, no secret), and
`POST /oauth/token` with `grant_type=client_credentials` returns
`{"error":"unsupported_grant_type"}`.

An early read of this metadata suggested that headless use is impossible because only an interactive
browser login could obtain a token. **That conclusion was wrong.** This flow is intended for MCP
clients that cannot reach CatalogAPI — Claude Code, Codex, and similar. It is not what the Alibaba
Cloud SDK-based clients do.

### ✅ What the official client uses: mint via authenticated CatalogAPI

`remote_auth.py` issues the bearer through a **normal signed CatalogAPI call**, reusing whatever
credentials the client already resolved:

```
POST {catalog_endpoint}/api/catalog/v1alpha/mcpAccessToken
action = MccpAccessToken, version = v1alpha, auth_type = "AK"
```

Response shape, all four fields required:

```json
{
  "accessToken": "mcpc_...",
  "tokenType": "Bearer",
  "expiresIn": 300,
  "scope": ["maxcompute:read", "maxcompute:sql"]
}
```

Then: `Authorization: Bearer mcpc_...` on every Streamable-HTTP request to `/mcp`.

Consequences:

- **Headless works.** AK/SK, STS, credentials URI, ECS RAM role, OIDC, `~/.aliyun/config.json` —
  anything the default credential chain resolves. No browser, no token store, no refresh handling.
- Tokens are **short-lived: exactly 300 seconds**. The reference implementation asserts
  `expiresIn == 300` and rejects anything else.
- **There is deliberately no refresh token.** The parser raises if `refreshToken` or
  `refresh_token` appears in the response. Renewal means calling the mint endpoint again.
- Renewal is cached with a 60-second expiry skew and single-flighted under a lock, so concurrent
  requests do not stampede the mint endpoint.
- The token prefix `mcpc_` is validated; length capped at 16 KiB.

### A signing subtlety worth copying verbatim

The request has no body, but Tea sends an empty stream as `application/octet-stream`, and
CatalogAPI signs the received Content-Type into its canonical string. So the client must declare
that header explicitly *before* the SDK builds the signature, or the server rejects it:

```python
request = OpenApiRequest(headers={"content-type": "application/octet-stream"})
```

Getting this wrong produces a signature error that looks like an authentication failure.

## 3. Why maxc can reuse existing plumbing

`src/maxc_cli/backend/catalog.py` already implements everything the mint call needs:

| Need | Existing capability |
|------|---------------------|
| Catalog endpoint | `_resolved_catalog_endpoint()` → kv_store cache → `ODPS.catalog_endpoint` auto-routing via `GET {odps_endpoint}/catalogapi` |
| Signed transport | `_catalog_rest` — a PyODPS `RestClient` built with `account`, resolved endpoint, region, namespace, `tag="Catalog"` |
| Availability probe | `catalog_available` property |
| Error mapping | `backend/semantic.py` already maps HTTP statuses to `MaxCError` on this same path |

So R2 needs **no new credential dependency and no new HTTP stack**: mint through the existing catalog
rest client, then talk JSON-RPC to `/mcp`.

This also means `kb` inherits the project's auth state. If `auth whoami` works, `kb` should work —
which is the property we want, rather than a second login the user has to discover and complete.

## 4. Tools exposed by Remote MCP (45 total)

Prefix `maxcompute_`. Names below come from the product documentation page listing remote tools.

**Knowledge base — the R2 target**

| Tool | Behavior |
|------|----------|
| `maxcompute_kb_search` | keyword search over MaxCompute documentation fragments |
| `maxcompute_kb_ask` | retrieves documents and answers grounded in them; **returns cited answers** |

The docs state the remote server ships a built-in MaxCompute documentation knowledge base supporting
both keyword search and natural-language Q&A with citation-bearing responses. That satisfies the
package requirement that every product claim name its source.

**Also present, relevant to later requests (out of scope for this branch)**

- Semanticspec: `semanticspec_create/get/list/update/delete/publish`,
  `semanticspec_list_published_revisions`, `semanticspec_get_published_revision`,
  `semanticspec_refresh_suggestions`, `semanticspec_apply_suggestions`,
  `datascan_get_latest_job_status`
- SQL: `sql_validate`, `sql_estimate_cost`, `sql_execute`, `sql_get_status`, `sql_fetch_result`,
  `sql_cancel`, `sql_get_logview`, `sql_list_instances`, `sql_list_queueing`, `generate_sql`
- Diagnostics/analysis: `diagnose_job`, `analyze_quota_usage`, `analyze_table`, `access_check`
- Schema: `schema_list_projects/get_project/list_schemas/get_schema/search_metadata/list_tables/
  describe_table/list_partitions/get_table_ddl/create_table/update_table`
- Quota: `quota_list`, `quota_get`
- Connection: `health_ping`, `gateway_capabilities`
- Distribution: `skill_list`, `skill_read`

Two observations for later scoping, recorded now so they are not rediscovered:

1. `semanticspec_refresh_suggestions` / `apply_suggestions` provide platform-generated semantic
   suggestions — a capability the skill package currently treats as manual set/get only.
2. Several tools overlap ground claimed by request **R1** (`aliyun maxc dw`). Before building that
   wrapper, re-check which DataWorks-side needs are actually served here. Note these are MaxCompute
   tools; DataWorks nodes/DI/quality/scheduling are **not** in this list, so R1 likely still stands,
   but the overlap should be mapped explicitly rather than assumed.

## 5. Implementation plan for R2

Derived from the above, in dependency order.

1. **Token provider** — `CatalogMcpTokenProvider`
   - mint via existing catalog RestClient; assert `tokenType == "Bearer"`, `expiresIn == 300`,
     exact expected scope, `mcpc_` prefix
   - reject any response carrying a refresh token
   - monotonic-clock expiry with 60 s skew; single-flight renewal
   - surface failures as `MaxCError` with a sanitized request id when available
2. **JSON-RPC client** — `backend/mcp.py`
   - `initialize` → capture `mcp-session-id` → `tools/call`
   - `Accept: application/json, text/event-stream`; handle both response media types, since
     Streamable HTTP may answer either
   - bind the session id to the token that created it; a renewed token does not automatically
     transplant a session
   - fixed timeouts on connect and read; no redirect following
   - never log or echo the bearer
3. **Command surface** — `kb ask "<question>"`, `kb search "<keyword>"`
   - flags: `--top-k`, `--category odps|dataworks`, `--page-size`, `--json`
   - output envelope carries **citations** (title, URL, updated marker where present) as first-class
     data, not prose. An uncited answer must be marked low-confidence
   - `effect: read` in `agent manifest` so preflight may call it freely
4. **Config** — `[mcp]` section: `enabled`, `endpoint` (or region-derived), `timeout_seconds`
   - default disabled initially, mirroring the R1 visibility rule: absent config key → actionable
     error naming the key, not `invalid choice`
5. **Failure modes to test explicitly**
   - catalog unavailable → say KB access needs working credentials, do not fall back to model memory
   - 401 after mint → token rejected; distinguish from missing credentials
   - tool returns no citations → emit an explicit warning
   - endpoint unreachable vs 4xx vs JSON-RPC error → three distinct messages

## 6. Live-verified protocol and result contract (2026-09-20)

All three open questions were resolved by direct calls rather than inference.

### 6.1 Token expiry is enforced strictly

A token held past 300 seconds returns `401` with body `invalid token`. Renewal is therefore not
optional for anything longer than a single request; mint lazily and re-mint on expiry.

### 6.2 The server is stateless — no session id

`initialize` returned `200` with **no** `mcp-session-id` response header, and a bare
`tools/list` without any session header succeeded. So:

- Do not require or cache a session id. Send `initialize` once per process if at all, then call
  tools directly.
- The earlier concern about binding a session to the token that created it does not apply here.
  Re-check if a future server version starts returning session ids.

Protocol header accepted: `MCP-Protocol-Version: 2025-06-18`.
Requests must send `Accept: application/json, text/event-stream`; responses in practice came back as
plain JSON, so handle both shapes.

### 6.3 Tool inventory: 44, not 45

Live `tools/list` returned **44** tools. The documentation page lists one more than exists. Two names
differ from the docs too: `maxcompute_schema_get_ddls` (plural) is present alongside
`get_table_ddl`, and there is an undocumented-by-me tool **`maxcompute_web_search`**.

Relevant to later scoping: `semanticspec_refresh_suggestions` and
`semanticspec_apply_suggestions` exist, and `maxcompute_diagnose_job`, `analyze_quota_usage`,
`analyze_table`, `generate_sql`, `access_check`, `sql_validate`, `sql_estimate_cost` all cover ground
the skill package currently implements by hand-assembled OpenAPI or inference.

### 6.4 kb tool input schemas (verbatim, `additionalProperties: false`)

```json
"maxcompute_kb_ask":    {"required": ["question"], "properties": {
                           "question": {"type": "string"},
                           "region":   {"type": "string", "description": "Optional MaxCompute region for retrieval embedding, rerank, and gate, judge, and answer model calls. Omit to use the configured default region."},
                           "max_docs": {"type": "integer"}}}

"maxcompute_kb_search": {"required": ["query"], "properties": {
                           "query":         {"type": "string"},
                           "region":        {"type": "string", "description": "Optional MaxCompute region for query embedding and rerank."},
                           "limit":         {"type": "integer"},
                           "before_lines":  {"type": ["null","integer"]},
                           "after_lines":   {"type": ["null","integer"]}}}
```

Note the argument name differs between them: `question` vs `query`.

Both descriptions warn that **one tool call may trigger multiple model calls**, routed to MaxAgent
first with a controlled fallback path. So latency is materially higher than a metadata lookup — size
command timeouts accordingly, and do not treat slowness as failure.

### 6.5 Result envelope (observed)

`tools/call` returns both `content[0].text` and `structuredContent`. Prefer structured. Shape:

```
structuredContent
├── ok            bool
├── data
│   ├── package   e.g. "maxcompute_kb"
│   ├── query     echo of the request
│   └── results[] each item:
│       ├── title            document title
│       ├── score            float relevance, observed 0.94
│       ├── snippet          markdown excerpt, truncated server-side with "...[truncated]"
│       ├── section_headings list[str]
│       └── source.uri       canonical help.aliyun.com URL   <-- the citation
├── has_more      bool
├── next_cursor   pagination token
├── metadata      {}
├── request_id    string
└── warnings      list
```

Citations are therefore `results[].source.uri` plus `title` and `section_headings` — exactly what the
skill package requires to attribute a product claim. `warnings` and `ok` should be surfaced rather
than swallowed, since retrieval can degrade while still returning HTTP 200.

Observed example citation: `https://help.aliyun.com/zh/maxcompute/user-guide/split-size-hint`

### 6.6 Consequences for the command surface

Revise §5 accordingly:

- `kb ask` → `maxcompute_kb_ask(question, max_docs?, region?)`
- `kb search` → `maxcompute_kb_search(query, limit?, before_lines?, after_lines?, region?)`
- Flag naming should follow the tool, not invent a third vocabulary: `--max-docs` for ask,
  `--limit` for search, plus `--context-lines N` mapping onto before/after lines
- `region` is meaningful and should default from session config rather than being required
- Emit `citations[]` as first-class output; propagate `warnings`, `ok=false`, and `next_cursor`
- Timeouts generous (tens of seconds), because model-backed retrieval is on the path
- No session handling needed; keep the client stateless

## Sources

- Live probes against `mcp.cn-shanghai.maxcompute.aliyun.com` (401 challenge, OAuth metadata,
  dynamic registration, `unsupported_grant_type`)
- `aliyun/alibabacloud-maxcompute-mcp-server` @ HEAD cloned 2026-09-20:
  `remote_auth.py` (mint + validation + caching), `remote_proxy.py` (`DynamicBearerAuth`),
  `credentials.py` (default chain), `server.py` (`build_remote_token_provider`)
- MaxCompute MCP service documentation (endpoint table, transport, tool list, KB behavior)
