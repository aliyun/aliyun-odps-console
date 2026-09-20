"""Tests for the MCP transport in ``maxc_cli.backend.mcp``.

Network-free by construction: token minting is injected and HTTP goes through a
stub opener. These pin the behaviours that are expensive to diagnose from a CLI
once shipped — expiry math, single-flight minting, strict response validation,
the one-retry 401 path, and both response encodings.
"""

from __future__ import annotations

import io
import json
import threading
import urllib.error

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.backend.mcp import (
    DEFAULT_PROTOCOL_VERSION,
    CatalogMcpTokenProvider,
    McpError,
    McpHttpClient,
    McpRequestError,
    TokenUnavailableError,
    build_catalog_mint,
    default_endpoint,
)


def _token_payload(**overrides):
    payload = {
        "accessToken": "mcpc_" + "a" * 40,
        "tokenType": "Bearer",
        "expiresIn": 300,
        "scope": ["maxcompute:read", "maxcompute:sql"],
    }
    payload.update(overrides)
    return payload


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ── CatalogMcpTokenProvider ────────────────────────────────────────────────


def test_provider_mints_once_then_serves_from_cache() -> None:
    calls = []

    def mint():
        calls.append(1)
        return _token_payload()

    clock = FakeClock()
    provider = CatalogMcpTokenProvider(mint, clock=clock)

    first = provider.get()
    second = provider.get()

    assert first == second
    assert len(calls) == 1


def test_provider_renews_within_expiry_skew_window() -> None:
    """A token with less than the skew left must not be used for a new call."""
    calls = []

    def mint():
        calls.append(1)
        return _token_payload()

    clock = FakeClock()
    provider = CatalogMcpTokenProvider(mint, expiry_skew=60.0, clock=clock)
    provider.get()

    # 239s elapsed leaves 61s > skew: still fresh.
    clock.advance(239)
    provider.get()
    assert len(calls) == 1

    # Crossing into the skew window renews before the call is sent.
    clock.advance(1)
    provider.get()
    assert len(calls) == 2


def test_provider_concurrent_getters_mint_only_once() -> None:
    release = threading.Event()
    calls = []

    def mint():
        calls.append(1)
        release.wait(timeout=5)
        return _token_payload()

    provider = CatalogMcpTokenProvider(mint, clock=FakeClock())
    results = []

    def worker() -> None:
        results.append(provider.get())

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    release.set()
    for thread in threads:
        thread.join(timeout=5)

    assert len(results) == 5
    assert len(calls) == 1, "single-flight failed: concurrent callers each minted"


def test_invalidate_forces_renewal() -> None:
    counter = {"n": 0}

    def mint():
        counter["n"] += 1
        return _token_payload(accessToken=f"mcpc_{'a' * 30}{counter['n']}")

    provider = CatalogMcpTokenProvider(mint, clock=FakeClock())
    first = provider.get()
    provider.invalidate()

    assert provider.get() != first
    assert counter["n"] == 2


# Each row breaks exactly one invariant the provider must not paper over.
@pytest.mark.parametrize(
    "payload",
    [
        {"tokenType": "Bearer", "expiresIn": 300, "scope": ["maxcompute:read", "maxcompute:sql"]},
        _token_payload(tokenType="MAC"),
        _token_payload(expiresIn=3600),
        _token_payload(scope=["maxcompute:read"]),
        _token_payload(accessToken="eyJunsigned"),
        _token_payload(accessToken="mcpc_"),
        {**_token_payload(), "refreshToken": "rt"},
        "not-a-dict",
    ],
)
def test_provider_rejects_unexpected_token_responses(payload) -> None:
    provider = CatalogMcpTokenProvider(lambda: payload, clock=FakeClock())
    with pytest.raises(TokenUnavailableError):
        provider.get()


def test_provider_rejects_absurd_expiry_skew() -> None:
    with pytest.raises(ValueError):
        CatalogMcpTokenProvider(lambda: _token_payload(), expiry_skew=300)


# ── build_catalog_mint ─────────────────────────────────────────────────────


class StubCatalogRest:
    def __init__(self, endpoint="https://catalog.cn-shanghai.maxcompute.aliyun.com/api", error=None):
        self.endpoint = endpoint
        self.error = error
        self.calls = []

    def request(self, url, method, **kwargs):
        self.calls.append((url, method, kwargs))
        if self.error:
            raise self.error
        return {"content": json.dumps(_token_payload()).encode()}


def test_mint_declares_octet_stream_content_type() -> None:
    """CatalogAPI signs the received Content-Type; anything else fails as a signature error."""
    rest = StubCatalogRest()
    build_catalog_mint(rest, rest.endpoint)()

    url, method, kwargs = rest.calls[0]
    assert method == "post"
    assert kwargs["headers"] == {"content-type": "application/octet-stream"}
    assert kwargs["data"] == b""
    assert url.endswith("/api/catalog/v1alpha/mcpAccessToken")


def test_mint_strips_trailing_api_suffix_without_doubling() -> None:
    rest = StubCatalogRest(endpoint="https://catalog.example.com/api")
    build_catalog_mint(rest, rest.endpoint)()
    assert rest.calls[0][0] == "https://catalog.example.com/api/catalog/v1alpha/mcpAccessToken"


def test_mint_wraps_transport_failure_as_token_unavailable() -> None:
    rest = StubCatalogRest(error=OSError("connection reset"))
    with pytest.raises(TokenUnavailableError) as excinfo:
        build_catalog_mint(rest, rest.endpoint)()
    assert "auth whoami" in str(excinfo.value)


def test_mint_rejects_non_http_endpoint() -> None:
    with pytest.raises(TokenUnavailableError):
        build_catalog_mint(StubCatalogRest(), "file:///tmp/catalog")


def test_mint_reads_stream_body_when_content_absent() -> None:
    class StreamResponse:
        def read(self):
            return json.dumps(_token_payload()).encode()

    class Rest:
        endpoint = "https://catalog.example.com"

        def request(self, url, method, **kwargs):
            return StreamResponse()

    assert build_catalog_mint(Rest(), Rest.endpoint)()["accessToken"].startswith("mcpc_")


# ── McpHttpClient ──────────────────────────────────────────────────────────


class StubOpener:
    """Stand-in for urllib's opener; records requests and replays queued answers."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append(request)
        outcome = self.responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if not isinstance(outcome, bytes):
            outcome = json.dumps(outcome).encode()
        return _Response(outcome)


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _client(responses, provider=None):
    provider = provider or CatalogMcpTokenProvider(lambda: _token_payload(), clock=FakeClock())
    return McpHttpClient(
        "https://mcp.cn-shanghai.maxcompute.aliyun.com/mcp",
        provider,
        opener=StubOpener(responses),
    )


def _tool_call_result(**overrides):
    result = {
        "content": [{"type": "text", "text": "ok"}],
        "structuredContent": {"ok": True, "data": {"results": []}},
    }
    result.update(overrides)
    return {"jsonrpc": "2.0", "id": 1, "result": result}


def test_client_refuses_plaintext_endpoint() -> None:
    provider = CatalogMcpTokenProvider(lambda: _token_payload(), clock=FakeClock())
    with pytest.raises(ValueError, match="https"):
        McpHttpClient("http://mcp.example.com/mcp", provider)


def test_client_sends_bearer_and_protocol_headers() -> None:
    client = _client([_tool_call_result()])
    client.call_tool("maxcompute_kb_search", {"query": "x"})

    request = client._opener.requests[0]
    assert request.get_header("Authorization") == "Bearer " + _token_payload()["accessToken"]
    assert request.get_header("Mcp-protocol-version") == DEFAULT_PROTOCOL_VERSION
    assert "text/event-stream" in request.get_header("Accept")
    assert request.get_method() == "POST"


def test_client_sends_json_rpc_envelope() -> None:
    client = _client([_tool_call_result()])
    client.call_tool("maxcompute_kb_ask", {"question": "why"})

    body = json.loads(client._opener.requests[0].data)
    assert body["method"] == "tools/call"
    assert body["params"] == {"name": "maxcompute_kb_ask", "arguments": {"question": "why"}}
    assert body["jsonrpc"] == "2.0"


def test_client_decodes_plain_json_response() -> None:
    client = _client([_tool_call_result()])
    assert client.call_tool("t", {})["structuredContent"]["ok"] is True


def test_client_decodes_sse_response() -> None:
    sse = (
        b"event: message\r\n"
        + b"data: " + json.dumps(_tool_call_result()).encode() + b"\r\n\r\n"
        + b"event: message\r\ndata: [DONE]\r\n\r\n"
    )
    client = _client([sse])
    assert client.call_tool("t", {})["structuredContent"]["ok"] is True


def test_client_retries_401_once_with_a_fresh_token() -> None:
    mints = {"n": 0}

    def mint():
        mints["n"] += 1
        return _token_payload(accessToken=f"mcpc_{'a' * 30}{mints['n']}")

    provider = CatalogMcpTokenProvider(mint, clock=FakeClock())
    unauthorized = urllib.error.HTTPError(
        "https://mcp.example.com/mcp", 401, "Unauthorized", {}, io.BytesIO(b"invalid token")
    )
    client = _client([unauthorized, _tool_call_result()], provider=provider)

    result = client.call_tool("t", {})

    assert result["structuredContent"]["ok"] is True
    assert mints["n"] == 2, "expired bearer should trigger exactly one re-mint"
    assert len(client._opener.requests) == 2


def test_client_does_not_retry_non_401() -> None:
    forbidden = urllib.error.HTTPError(
        "https://mcp.example.com/mcp", 403, "Forbidden", {}, io.BytesIO(b"nope")
    )
    client = _client([forbidden])
    with pytest.raises(McpRequestError) as excinfo:
        client.call_tool("t", {})
    assert excinfo.value.status == 403
    assert len(client._opener.requests) == 1


def test_client_gives_up_after_a_second_401() -> None:
    def http_error():
        return urllib.error.HTTPError(
            "https://mcp.example.com/mcp", 401, "Unauthorized", {}, io.BytesIO(b"invalid token")
        )

    client = _client([http_error(), http_error()])
    with pytest.raises(McpRequestError) as excinfo:
        client.call_tool("t", {})
    assert excinfo.value.status == 401
    assert len(client._opener.requests) == 2, "retry must be bounded at one"


def test_client_surfaces_json_rpc_error() -> None:
    client = _client([{"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "unknown tool"}}])
    with pytest.raises(McpRequestError, match="unknown tool"):
        client.call_tool("nope", {})


def test_client_rejects_missing_result_object() -> None:
    client = _client([{"jsonrpc": "2.0", "id": 1}])
    with pytest.raises(McpRequestError, match="no result"):
        client.call_tool("t", {})


def test_client_reports_empty_body() -> None:
    client = _client([b""])
    with pytest.raises(McpRequestError, match="empty response"):
        client.call_tool("t", {})


def test_client_reports_unreachable_endpoint() -> None:
    client = _client([urllib.error.URLError("name not resolved")])
    with pytest.raises(McpRequestError, match="Could not reach"):
        client.call_tool("t", {})


def test_list_tools_returns_empty_list_when_absent() -> None:
    client = _client([{"jsonrpc": "2.0", "id": 1, "result": {}}])
    assert client.list_tools() == []


def test_list_tools_reads_tool_array() -> None:
    client = _client([{"jsonrpc": "2.0", "id": 1, "result": {"tools": [{"name": "a"}, {"name": "b"}]}}])
    assert [tool["name"] for tool in client.list_tools()] == ["a", "b"]


def test_all_errors_are_mcp_error_subclasses() -> None:
    """The CLI catches McpError once; a stray exception type would escape as a traceback."""
    assert issubclass(TokenUnavailableError, McpError)
    assert issubclass(McpRequestError, McpError)


# ── default_endpoint ───────────────────────────────────────────────────────


def test_endpoint_without_region_uses_shared_host() -> None:
    assert default_endpoint(None) == "https://mcp.maxcompute.aliyun.com/mcp"
    assert default_endpoint("") == "https://mcp.maxcompute.aliyun.com/mcp"


def test_endpoint_regionalises() -> None:
    assert default_endpoint("cn-shanghai") == "https://mcp.cn-shanghai.maxcompute.aliyun.com/mcp"
    assert default_endpoint("CN-Shanghai") == "https://mcp.cn-shanghai.maxcompute.aliyun.com/mcp"


def test_intl_endpoint_uses_separate_host() -> None:
    assert default_endpoint("ap-southeast-1", site="INTL") == (
        "https://mcp-intl.ap-southeast-1.maxcompute.aliyun.com/mcp"
    )


def test_endpoint_rejects_injection_shaped_region() -> None:
    """Region reaches a URL; a path or host could otherwise be smuggled in."""
    for bad in ("cn/../evil", "x?y", "a b", "host:443", "#frag"):
        with pytest.raises(ValueError):
            default_endpoint(bad)
