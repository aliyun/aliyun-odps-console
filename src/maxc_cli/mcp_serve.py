"""Serve MaxCompute MCP tools over stdio, authorized by maxc's own credentials.

Why this exists instead of pointing an agent at the upstream MCP server: the hosted
endpoint authenticates with a bearer minted from MaxCompute credentials, and the
upstream server only reads ``ALIBABA_CLOUD_ACCESS_KEY_*`` or its own config file. A
user who already ran ``maxc auth login --oauth`` would still have to configure that
identity a second time. This bridges the profile maxc already resolved onto stdio.

The protocol is implemented directly rather than with the official ``mcp`` SDK on
purpose: stdio framing is newline-delimited JSON-RPC 2.0 with no session state, while
the SDK pulls pydantic v2 (compiled core), anyio, uvicorn, starlette and OpenTelemetry
into a CLI whose bundle currently carries only PyYAML and pyodps.

All remote tools are exposed, including destructive ones, because filtering them out
would make maxc a worse MCP server for no security gain — the endpoint stays reachable
to any client. What this module guarantees instead is that the server's own
``readOnlyHint`` / ``destructiveHint`` annotations survive the bridge intact, so the
connecting client can enforce them, and that a banner naming the exposure is written to
stderr where it cannot corrupt the stdout message stream.
"""

from __future__ import annotations

import json
import threading
from typing import Any, BinaryIO, Callable, TextIO

from .backend.mcp import McpError, McpHttpClient

# Newest protocol revision this server understands. Clients may ask for a newer
# one; we answer with this value and they either continue or disconnect.
LATEST_PROTOCOL_VERSION = "2025-06-18"
SUPPORTED_PROTOCOL_VERSIONS = ("2025-03-26", "2024-11-05")

_SERVER_INFO = {"name": "maxc-mcp-serve", "version": "0"}

_INSTRUCTIONS = (
    "Bridged by `maxc mcp serve` using the caller's existing MaxCompute credentials. "
    "Tool results are the hosted service's structuredContent passed through unchanged, "
    "so fields added server-side appear without a CLI change. Several tools are billed "
    "per model call and several mutate data; honour each tool's readOnlyHint and "
    "destructiveHint annotations before invoking one."
)

# Notification methods carry no id and expect no reply.
_NOTIFICATIONS = frozenset({"notifications/initialized", "notifications/cancelled"})

_METHOD_NOT_FOUND = -32601
_INVALID_REQUEST = -32600
_INVALID_PARAMS = -32602
_INTERNAL_ERROR = -32603


class _ProtocolError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def negotiate_version(requested: Any) -> str:
    """Return a mutually usable protocol version, newest first.

    The spec requires answering an unknown request version with the server's latest
    supported version rather than failing, so a newer client can decide to disconnect.
    """
    if isinstance(requested, str) and requested in (
        LATEST_PROTOCOL_VERSION,
        *SUPPORTED_PROTOCOL_VERSIONS,
    ):
        return requested
    return LATEST_PROTOCOL_VERSION


def _envelope(msg_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _error(msg_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": msg_id, "error": error}


class StdioServer:
    """Line-oriented JSON-RPC responder in front of one :class:`McpHttpClient`.

    Requests are handled sequentially. That is deliberate: MCP clients serialise
    requests per connection, and a single writer keeps stdout free of interleaved
    frames without needing a lock around partial writes.
    """

    def __init__(
        self,
        client: McpHttpClient,
        *,
        read: BinaryIO,
        write: BinaryIO,
        log: Callable[[str], None],
        server_version: str = "0",
    ) -> None:
        self._client = client
        self._read = read
        self._write = write
        self._log = log
        self._server_info = dict(_SERVER_INFO)
        self._server_info["version"] = server_version
        self._tools_cache: list[dict[str, Any]] | None = None
        self._initialised = False
        self._write_lock = threading.Lock()

    # --- transport -------------------------------------------------------

    def _send(self, payload: dict[str, Any]) -> None:
        frame = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        blob = (frame + "\n").encode("utf-8")
        with self._write_lock:
            self._write.write(blob)
            self._write.flush()

    def run(self) -> int:
        """Read stdin until EOF. Returns a process exit code."""
        for raw in self._read:
            line = raw.decode("utf-8", "replace").strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except ValueError:
                # A malformed frame has no recoverable id, so the spec says reply
                # with id null rather than guessing which request it belonged to.
                self._send(_error(None, _INVALID_REQUEST, "invalid JSON on stdin"))
                continue
            if isinstance(message, list):
                # Batch requests were dropped from the 2025 revision. Refusing the
                # whole frame beats executing some of it and silently skipping rest.
                self._send(_error(None, _INVALID_REQUEST, "batch requests are not supported"))
                continue
            if not isinstance(message, dict):
                self._send(_error(None, _INVALID_REQUEST, "expected a JSON-RPC object"))
                continue
            response = self._dispatch(message)
            if response is not None:
                self._send(response)
        return 0

    # --- dispatch --------------------------------------------------------

    def _dispatch(self, message: dict[str, Any]) -> dict[str, Any] | None:
        method = message.get("method")
        msg_id = message.get("id")
        if not isinstance(method, str):
            # An object with no method is a response to a request this server never
            # makes; ignore it instead of echoing an error back at the client.
            if "result" in message or "error" in message:
                return None
            return _error(msg_id, _INVALID_REQUEST, "missing method")

        if method in _NOTIFICATIONS:
            if method == "notifications/initialized":
                self._initialised = True
            return None

        params = message.get("params") or {}
        if not isinstance(params, dict):
            return _error(msg_id, _INVALID_PARAMS, "params must be an object")

        handler = {
            "initialize": self._on_initialize,
            "ping": self._on_ping,
            "tools/list": self._on_tools_list,
            "tools/call": self._on_tools_call,
        }.get(method)
        if handler is None:
            # Deliberately unimplemented: completion/, prompts/, resources/, and
            # logging/setLevel. The hosted service exposes none of them, so there is
            # nothing to bridge; returning METHOD_NOT_FOUND is spec-correct and lets
            # capable clients degrade rather than misbehave.
            return _error(msg_id, _METHOD_NOT_FOUND, f"method not supported: {method}")
        if method != "initialize" and not self._initialised:
            return _error(msg_id, _INVALID_REQUEST, "send initialize before other requests")
        try:
            return _envelope(msg_id, handler(params))
        except _ProtocolError as exc:
            return _error(msg_id, exc.code, exc.message)
        except McpError as exc:
            # Transport and credential faults are tool-execution failures, not
            # protocol faults: they belong in an isError result so the agent sees
            # them in the conversation rather than as a dropped connection.
            return _envelope(msg_id, {"content": [{"type": "text", "text": str(exc)}], "isError": True})
        except Exception as exc:  # noqa: BLE001 -- one bad call must not kill the loop
            detail = f"{type(exc).__name__}: {exc}"
            self._log(f"unhandled error serving {method}: {detail}")
            return _error(msg_id, _INTERNAL_ERROR, "internal error", detail[:400])

    # --- handlers --------------------------------------------------------

    def _on_initialize(self, params: dict[str, Any]) -> dict[str, Any]:
        self._initialised = False
        return {
            "protocolVersion": negotiate_version(params.get("protocolVersion")),
            "capabilities": {
                "tools": {"listChanged": False},
                "experimental": {},
            },
            "serverInfo": self._server_info,
            "instructions": _INSTRUCTIONS,
        }

    @staticmethod
    def _on_ping(_params: dict[str, Any]) -> dict[str, Any]:
        return {}

    def _on_tools_list(self, params: dict[str, Any]) -> dict[str, Any]:
        if params.get("cursor") not in (None, ""):
            raise _ProtocolError(_INVALID_PARAMS, "this server does not paginate tools")
        if self._tools_cache is None:
            # Fetched once and kept: the catalogue is static for a session, and a
            # long-lived client polling tools/list should not re-hit CatalogAPI.
            self._tools_cache = self._client.list_tools()
        return {"tools": self._tools_cache}

    def _on_tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        if not isinstance(name, str) or not name:
            raise _ProtocolError(_INVALID_PARAMS, "tools/call requires a tool name")
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise _ProtocolError(_INVALID_PARAMS, "arguments must be an object")
        return self._client.call_tool(name, arguments)


def warn_exposure(
    stderr: TextIO,
    *,
    project: str,
    region: str,
    tool_count: int,
    endpoint: str,
) -> None:
    """Tell the human what this process just made callable.

    Written to stderr because stdout is the protocol channel. The warning is
    disclosure, not enforcement: the gate is the annotations the client receives.
    """
    lines = [
        f"maxc mcp serve: bridging {tool_count} MaxCompute MCP tools over stdio.",
        f"  identity   : credentials resolved by maxc (project={project or 'unset'},"
        f" region={region or 'unset'})",
        f"  endpoint   : {endpoint}",
        "  side effects: this set includes destructive tools (SQL execution, table",
        "                create/update, row inserts, job cancel) which the hosted",
        "                service authorises per-credential, with no per-call prompt.",
        "                Each tool's destructiveHint/readOnlyHint annotation is passed",
        "                through unchanged — configure your client to require approval",
        "                for anything not marked readOnly.",
        "  cost        : several tools bill MaxAgent credits per call. A looping agent",
        "                can spend quota without an obvious signal.",
        "  stdin       : stop this server by closing stdin (Ctrl-D); it exits at EOF.",
    ]
    for line in lines:
        print(line, file=stderr)
    stderr.flush()
