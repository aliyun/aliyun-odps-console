# Product documentation versus an MCP bridge

For a product rule, hint or error code, `kb search` retrieves documentation passages and `kb ask` returns an answer with citations. Neither reads a user's table. Use a generic retrieval prompt, not credentials, private SQL or row values. Both need an existing authorized identity and `mcp.enabled: true`; do not enable the integration or replace credentials just to get an answer.

```bash
{{cli}} kb search "partition pruning" --limit 3 --user-agent "$UA" --json
{{cli}} kb ask "When is partition pruning applied?" --user-agent "$UA" --json
```

Ground the answer in `data.search.matches[].uri` (search) or `data.citations[].uri` (ask); qualify claims without supporting passages. Empty hits, warnings or `ok=false` describe this retrieval, not a platform limitation.

`mcp serve` instead exposes the hosted tool set over stdio using the current identity. Start it only on an explicit request to connect another MCP client. Report the startup banner's scope (project, endpoint and tool count) without revealing credentials. Tools can mutate data or cancel jobs and some incur credits; configure approval in the consuming client. Standard output is JSON-RPC, not a CLI result envelope. A documentation lookup does not need a bridge.
