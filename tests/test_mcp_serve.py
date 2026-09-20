"""Tests for the stdio MCP bridge in ``maxc_cli.mcp_serve``.

These exercise the protocol layer with a stubbed remote client, so they cover the
parts that a live run cannot easily assert: exact framing, what must never appear on
stdout, version negotiation, and that annotations survive the bridge. The live
round-trip is covered separately by ``test_serve_matches_live_tool_annotations``,
which is skipped without credentials.
"""

from __future__ import annotations

import argparse
import io
import json

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.backend.mcp import McpRequestError
from maxc_cli.mcp_serve import (
    LATEST_PROTOCOL_VERSION,
    StdioServer,
    negotiate_version,
    warn_exposure,
)


class StubClient:
    url = "https://mcp.example.com/mcp"

    def __init__(self, tools=None, call_result=None, error=None):
        self._tools = tools if tools is not None else []
        self._call_result = call_result or {"content": [], "structuredContent": {"ok": True}}
        self.error = error
        self.calls = []
        self.list_calls = 0

    def list_tools(self):
        self.list_calls += 1
        return self._tools

    def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.error:
            raise self.error
        return self._call_result


def _run(client, messages):
    """Feed framed requests through the server and return decoded replies in order."""
    stdin = io.BytesIO("".join(json.dumps(m) + "\n" for m in messages).encode())
    stdout = io.BytesIO()
    logs = []
    server = StdioServer(
        client, read=stdin, write=stdout, log=logs.append, server_version="9.9.9"
    )
    assert server.run() == 0
    frames = [json.loads(line) for line in stdout.getvalue().decode().splitlines() if line.strip()]
    return frames, logs


_INIT = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
    "protocolVersion": LATEST_PROTOCOL_VERSION, "capabilities": {},
    "clientInfo": {"name": "t", "version": "0"}}}
_READY = {"jsonrpc": "2.0", "method": "notifications/initialized"}


def _handshake(extra=()):
    return [_INIT, _READY, *extra]


# ── framing ────────────────────────────────────────────────────────────────


def test_each_response_is_one_newline_delimited_frame() -> None:
    frames, _ = _run(StubClient(), _handshake())
    assert len(frames) == 1
    assert frames[0]["jsonrpc"] == "2.0"
    assert frames[0]["id"] == 1


def test_notifications_get_no_reply() -> None:
    frames, _ = _run(StubClient(), _handshake([{"jsonrpc": "2.0", "method": "notifications/cancelled"}]))
    assert len(frames) == 1, "a notification must not produce a response frame"


def test_responses_to_client_requests_are_ignored_not_errored() -> None:
    """The bridge never sends requests, so an inbound response is just noise."""
    frames, _ = _run(StubClient(), _handshake([{"jsonrpc": "2.0", "id": 77, "result": {}}]))
    assert [f["id"] for f in frames] == [1]


def test_malformed_json_replies_with_null_id() -> None:
    stdin = io.BytesIO(b"{not json\n")
    stdout = io.BytesIO()
    StdioServer(StubClient(), read=stdin, write=stdout, log=lambda m: None).run()
    reply = json.loads(stdout.getvalue().decode())
    assert reply["id"] is None
    assert reply["error"]["code"] == -32600


def test_batch_requests_are_refused_wholesale() -> None:
    """Batching was dropped in the 2025 revision; partial execution would be worse."""
    stdin = io.BytesIO((json.dumps([{"jsonrpc": "2.0", "id": 2, "method": "ping"},
                                    {"jsonrpc": "2.0", "id": 3, "method": "ping"}]) + "\n").encode())
    stdout = io.BytesIO()
    StdioServer(StubClient(), read=stdin, write=stdout, log=lambda m: None).run()
    reply = json.loads(stdout.getvalue().decode())
    assert reply["id"] is None
    assert reply["error"]["code"] == -32600
    assert "batch" in reply["error"]["message"]


def test_object_without_method_is_an_invalid_request() -> None:
    frames, _ = _run(StubClient(), _handshake([{"jsonrpc": "2.0", "id": 8}]))
    assert frames[-1]["error"]["code"] == -32600
    assert "missing method" in frames[-1]["error"]["message"]


def test_blank_lines_are_skipped() -> None:
    stdin = io.BytesIO(("\n\n" + json.dumps(_INIT) + "\n\n").encode())
    stdout = io.BytesIO()
    StdioServer(StubClient(), read=stdin, write=stdout, log=lambda m: None).run()
    assert len(stdout.getvalue().decode().strip().splitlines()) == 1


def test_eof_ends_the_loop_cleanly() -> None:
    frames, logs = _run(StubClient(), [])
    assert frames == []
    assert logs == []


# ── initialize ─────────────────────────────────────────────────────────────


def test_initialize_reports_tools_capability_and_server_info() -> None:
    frames, _ = _run(StubClient(), [_INIT])
    result = frames[0]["result"]
    assert "tools" in result["capabilities"]
    assert result["serverInfo"]["name"] == "maxc-mcp-serve"
    assert result["serverInfo"]["version"] == "9.9.9"
    assert "destructiveHint" in result["instructions"]


@pytest.mark.parametrize(
    "requested, expected",
    [
        ("2024-11-05", "2024-11-05"),
        (LATEST_PROTOCOL_VERSION, LATEST_PROTOCOL_VERSION),
        ("9999-99-99", LATEST_PROTOCOL_VERSION),
        (None, LATEST_PROTOCOL_VERSION),
        ("garbage", LATEST_PROTOCOL_VERSION),
    ],
)
def test_version_negotiation_never_fails_a_newer_client(requested, expected) -> None:
    assert negotiate_version(requested) == expected


def test_requests_before_initialize_are_rejected() -> None:
    frames, _ = _run(StubClient(), [{"jsonrpc": "2.0", "id": 5, "method": "tools/list", "params": {}}])
    assert frames[0]["error"]["code"] == -32600
    assert "initialize" in frames[0]["error"]["message"]


# ── tools/list ─────────────────────────────────────────────────────────────


def test_tool_annotations_pass_through_unchanged() -> None:
    """The client's only enforcement hook is these annotations; dropping them is unsafe."""
    tools = [
        {"name": "maxcompute_kb_search", "inputSchema": {"type": "object"},
         "annotations": {"readOnlyHint": True, "destructiveHint": False}},
        {"name": "maxcompute_sql_execute", "inputSchema": {"type": "object"},
         "annotations": {"readOnlyHint": False, "destructiveHint": True}},
    ]
    frames, _ = _run(StubClient(tools=tools), _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}]))
    served = {item["name"]: item["annotations"] for item in frames[1]["result"]["tools"]}
    assert served["maxcompute_sql_execute"]["destructiveHint"] is True
    assert served["maxcompute_kb_search"]["readOnlyHint"] is True


def test_tools_list_is_cached_across_requests() -> None:
    client = StubClient(tools=[{"name": "a"}])
    _run(client, _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}}]))
    assert client.list_calls == 1


def test_pagination_cursor_is_rejected_not_ignored() -> None:
    """Returning a full list while claiming continuation would mislead the client."""
    frames, _ = _run(StubClient(tools=[{"name": "a"}]), _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {"cursor": "x"}}]))
    assert frames[1]["error"]["code"] == -32602


# ── tools/call ─────────────────────────────────────────────────────────────


def test_tools_call_forwards_name_and_arguments() -> None:
    client = StubClient()
    _run(client, _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
         "params": {"name": "maxcompute_kb_ask", "arguments": {"question": "q"}}}]))
    assert client.calls == [("maxcompute_kb_ask", {"question": "q"})]


def test_structured_content_is_returned_verbatim() -> None:
    payload = {"content": [{"type": "text", "text": "x"}],
               "structuredContent": {"ok": True, "data": {"results": [{"newField": 1}]}}}
    frames, _ = _run(StubClient(call_result=payload), _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "t", "arguments": {}}}]))
    assert frames[1]["result"] == payload


def test_missing_tool_name_is_invalid_params() -> None:
    frames, _ = _run(StubClient(), _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {}}]))
    assert frames[1]["error"]["code"] == -32602


def test_non_object_arguments_is_invalid_params() -> None:
    frames, _ = _run(StubClient(), _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
         "params": {"name": "t", "arguments": ["nope"]}}]))
    assert frames[1]["error"]["code"] == -32602


def test_remote_failure_becomes_an_is_error_result_not_a_protocol_error() -> None:
    """A tool fault belongs in the conversation; a dropped connection does not."""
    client = StubClient(error=McpRequestError("endpoint returned HTTP 503", status=503))
    frames, _ = _run(client, _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "t", "arguments": {}}}]))
    reply = frames[1]
    assert "error" not in reply
    assert reply["result"]["isError"] is True
    assert "503" in reply["result"]["content"][0]["text"]


def test_unexpected_exception_does_not_kill_the_server() -> None:
    class Exploding(StubClient):
        def call_tool(self, name, arguments):
            raise RuntimeError("boom")

    frames, logs = _run(Exploding(), _handshake([
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "t", "arguments": {}}},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}}]))
    assert frames[1]["error"]["code"] == -32603
    assert "boom" in frames[1]["error"]["data"]
    assert frames[2]["id"] == 3, "the loop must continue after one failed call"
    assert logs


# ── unimplemented methods ──────────────────────────────────────────────────


@pytest.mark.parametrize("method", [
    "ping", "prompts/list", "resources/read", "completion/complete", "logging/setLevel",
])
def test_every_advertised_method_set_is_enforced(method) -> None:
    """Pin the surface: anything outside the four implemented methods must error.

    A future method that starts returning success without a contract note here would
    silently widen what callers can rely on.
    """
    frames, _ = _run(StubClient(), _handshake([
        {"jsonrpc": "2.0", "id": 9, "method": method, "params": {}}]))
    reply = frames[-1]
    if method == "ping":
        assert reply["result"] == {}
    else:
        assert reply["error"]["code"] == -32601


# ── startup banner ─────────────────────────────────────────────────────────


def test_banner_names_count_endpoint_and_side_effects() -> None:
    stderr = io.StringIO()
    warn_exposure(stderr, project="bird", region="cn-shanghai",
                  tool_count=44, endpoint="https://mcp.cn-shanghai.maxcompute.aliyun.com/mcp")
    text = stderr.getvalue()
    assert "44 MaxCompute MCP tools" in text
    assert "project=bird" in text
    assert "destructive" in text
    assert "credits" in text


def test_banner_never_mentions_secrets() -> None:
    """The banner is copied into bug reports; it must stay safe to share."""
    stderr = io.StringIO()
    warn_exposure(stderr, project="", region="", tool_count=1, endpoint="https://mcp.example.com/mcp")
    text = stderr.getvalue().lower()
    for token in ("access_key", "secret", "bearer", "mcpc_", "authorization"):
        assert token not in text
    assert "project=unset" in text


# ── CLI wiring ─────────────────────────────────────────────────────────────


def test_serve_is_exempt_from_the_auth_auto_redirect() -> None:
    """The redirect prompts on stdout, which is this command's protocol channel."""
    from maxc_cli.cli import _AUTO_LOGIN_EXEMPT_COMMANDS, _LOCAL_ONLY_COMMANDS

    assert "mcp.serve" in _AUTO_LOGIN_EXEMPT_COMMANDS
    # ...but unlike other exempt commands it still needs a backend to mint a bearer.
    assert "mcp.serve" not in _LOCAL_ONLY_COMMANDS


def test_serve_manifest_declares_write_capability() -> None:
    from maxc_cli.cli import _command_manifest, build_parser

    entry = {c["command"]: c for c in _command_manifest(build_parser())["commands"]}["mcp.serve"]
    assert entry["effect"] == "remote_write"
    kinds = {(e["kind"], e["target"]) for e in entry["effects"]}
    assert ("data_mutation", "maxcompute_via_mcp_tool") in kinds
    assert entry["requirements"]["credentials"]["mode"] == "required"


def test_serve_takes_no_routing_or_scoping_arguments() -> None:
    """The tool set comes from the service; scoping comes from the caller's identity.

    A --project or --tools filter here would create a CLI-side view of the surface
    that disagrees with what `tools/list` reports to the connected client.
    """
    from maxc_cli.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["mcp", "serve"])
    assert (args.command_group, args.mcp_command) == ("mcp", "serve")
    for rejected in ("--project", "--region", "--schema"):
        with pytest.raises(SystemExit):
            parser.parse_args(["mcp", "serve", rejected, "x"])


def test_placeholder_flags_stay_out_of_help() -> None:
    """These exist for forward compatibility only; advertising them invites use."""
    from maxc_cli.cli import build_parser

    current = build_parser()
    for token in ("mcp", "serve"):
        subparsers = next(
            action for action in current._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        current = subparsers.choices[token]
    rendered = current.format_help()
    assert "--tools" not in rendered
    assert "--protocol-version" not in rendered
