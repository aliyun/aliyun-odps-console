"""MaxCompute Remote MCP access: token minting and a stateless JSON-RPC client.

Two responsibilities live here, deliberately separated because they fail differently:

``CatalogMcpTokenProvider``
    Trades the caller's *already resolved* MaxCompute credentials for a short-lived
    ``mcpc_`` bearer by issuing an authenticated CatalogAPI call. No browser, no OAuth
    redirect, no refresh token: renewal means calling the mint endpoint again. Reference
    implementation: ``remote_auth.py`` in aliyun/alibabacloud-maxcompute-mcp-server.

``McpHttpClient``
    Minimal Streamable-HTTP JSON-RPC client for ``tools/list`` and ``tools/call``. The
    deployed server is stateless today (it returns no ``mcp-session-id``), so nothing here
    caches a session; see ``docs/mcp-kb-access-research.md`` §6.2 before assuming that holds.

Only stdlib HTTP is used, so this module adds no runtime dependency.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable

# --- CatalogAPI token issuance ------------------------------------------------
# Verified against the live service on 2026-09-20.

_MCP_ACCESS_TOKEN_PATH = "/api/catalog/v1alpha/mcpAccessToken"
_EXPECTED_SCOPES = ["maxcompute:read", "maxcompute:sql"]
_TOKEN_LIFETIME_SECONDS = 300
# Renew this long before real expiry so a request never starts on a doomed token.
_EXPIRY_SKEW_SECONDS = 60.0
_MAX_TOKEN_CHARS = 16 * 1024
_TOKEN_PREFIX = "mcpc_"
# Tea sends a bodyless stream as application/octet-stream and CatalogAPI signs the
# received Content-Type into its canonical string. Declaring anything else here
# produces a signature error that reads like an authentication failure.
_EMPTY_BODY_CONTENT_TYPE = "application/octet-stream"
_MINT_TIMEOUT_SECONDS = 10.0

DEFAULT_PROTOCOL_VERSION = "2025-06-18"


class McpError(RuntimeError):
    """Base class for MCP access failures safe to show to a user."""


class TokenUnavailableError(McpError):
    """The bearer could not be minted, usually missing or unusable credentials."""


class McpRequestError(McpError):
    """The MCP endpoint rejected or failed a JSON-RPC call."""

    def __init__(self, message: str, *, status: int | None = None,
                 request_id: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.request_id = request_id


def _strip_api_suffix(endpoint: str) -> str:
    text = (endpoint or "").rstrip("/")
    return text[: -len("/api")] if text.endswith("/api") else text


class CatalogMcpTokenProvider:
    """Mint and cache CatalogAPI MCP bearers, single-flighting renewals.

    ``mint`` is injected rather than hard-wired so tests never touch the network and
    so the signing path stays whatever pyodps already does correctly.
    """

    def __init__(
        self,
        mint: Callable[[], dict[str, Any]],
        *,
        expiry_skew: float = _EXPIRY_SKEW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0 <= expiry_skew < _TOKEN_LIFETIME_SECONDS:
            raise ValueError("expiry skew must be within the token lifetime")
        self._mint = mint
        self._expiry_skew = expiry_skew
        self._clock = clock
        self._token = ""
        self._expires_at = 0.0
        self._lock = threading.Lock()

    def get(self) -> str:
        """Return a usable bearer, minting at most once for concurrent callers."""
        with self._lock:
            if self._token and self._clock() + self._expiry_skew < self._expires_at:
                return self._token
            value = self._parse(self._mint())
            self._token = value
            self._expires_at = self._clock() + _TOKEN_LIFETIME_SECONDS
            return value

    def invalidate(self) -> None:
        """Drop the cached bearer so the next ``get`` re-mints."""
        with self._lock:
            self._token = ""
            self._expires_at = 0.0

    @staticmethod
    def _parse(payload: Any) -> str:
        """Validate every field the reference implementation asserts, then return the token.

        Strictness is intentional: a silently different lifetime or scope would turn a
        working call into an intermittent 401 that is expensive to diagnose from a CLI.
        """
        if not isinstance(payload, dict):
            raise TokenUnavailableError("MCP token response was not a JSON object.")
        missing = {"accessToken", "tokenType", "expiresIn", "scope"} - set(payload)
        if missing:
            raise TokenUnavailableError(
                "MCP token response missing field(s): " + ", ".join(sorted(missing)) + "."
            )
        # A refresh token here means the service changed contract; caching one would
        # imply a renewal path this provider deliberately does not have.
        if any("refresh" in key.lower() for key in payload):
            raise TokenUnavailableError(
                "MCP token response carried a refresh token, which this client does not support."
            )
        value = payload.get("accessToken")
        if (
            not isinstance(value, str)
            or not value.startswith(_TOKEN_PREFIX)
            or len(value) <= len(_TOKEN_PREFIX)
            or len(value) > _MAX_TOKEN_CHARS
        ):
            raise TokenUnavailableError("MCP token response carried an unexpected access token.")
        if payload.get("tokenType") != "Bearer":
            raise TokenUnavailableError("MCP token response carried an unexpected tokenType.")
        expires_in = payload.get("expiresIn")
        if isinstance(expires_in, bool) or expires_in != _TOKEN_LIFETIME_SECONDS:
            raise TokenUnavailableError(
                f"MCP token lifetime {expires_in!r} differs from the expected "
                f"{_TOKEN_LIFETIME_SECONDS}s; caching assumptions would be invalid."
            )
        if payload.get("scope") != _EXPECTED_SCOPES:
            raise TokenUnavailableError("MCP token response carried an unexpected scope.")
        return value


def build_catalog_mint(catalog_rest: Any, catalog_endpoint: str) -> Callable[[], dict[str, Any]]:
    """Return a callable that mints a bearer through an authenticated Catalog rest client.

    Reuses the same signed transport as catalog search, so whatever credential chain
    ``auth whoami`` resolved is what authorizes the MCP call.
    """
    base = _strip_api_suffix(catalog_endpoint)
    if not base.startswith(("https://", "http://")):
        raise TokenUnavailableError("Catalog endpoint is not an HTTP(S) URL.")
    url = base + _MCP_ACCESS_TOKEN_PATH

    def mint() -> dict[str, Any]:
        try:
            response = catalog_rest.request(
                url,
                "post",
                data=b"",
                headers={"content-type": _EMPTY_BODY_CONTENT_TYPE},
                timeout=_MINT_TIMEOUT_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 -- credential/transport SDK errors vary
            raise TokenUnavailableError(
                "Could not request an MCP access token from CatalogAPI. "
                "Check that MaxCompute credentials resolve (try `maxc auth whoami --json`)."
            ) from exc
        body = getattr(response, "content", None)
        if body is None:
            body = response.read()
        if isinstance(body, bytes):
            body = body.decode("utf-8", "replace")
        try:
            return json.loads(body)
        except (TypeError, ValueError) as exc:
            raise TokenUnavailableError("CatalogAPI returned a non-JSON MCP token response.") from exc

    return mint


# --- Stateless JSON-RPC over Streamable HTTP ---------------------------------

class McpHttpClient:
    """Call MCP tools over Streamable HTTP without assuming a server session."""

    def __init__(
        self,
        url: str,
        token_provider: CatalogMcpTokenProvider,
        *,
        timeout: float = 120.0,
        client_name: str = "maxc-cli",
        client_version: str = "0",
        opener: Any = None,
    ) -> None:
        if not url.startswith("https://"):
            # The bearer is a credential; never send it over plaintext.
            raise ValueError("MCP endpoint must be an https:// URL.")
        self._url = url
        self._tokens = token_provider
        self._timeout = timeout
        self._client = (client_name, client_version)
        self._opener = opener or urllib.request.build_opener()
        self._next_id = 0

    def list_tools(self) -> list[dict[str, Any]]:
        result = self._request("tools/list", {})
        tools = result.get("tools") if isinstance(result, dict) else None
        return list(tools or [])

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Invoke one tool, retrying exactly once after a token expiry."""
        payload = {"name": name, "arguments": arguments}
        try:
            return self._request("tools/call", payload)
        except McpRequestError as exc:
            if exc.status != 401:
                raise
            # Observed behaviour: an expired bearer answers 401 "invalid token".
            self._tokens.invalidate()
            return self._request("tools/call", payload)

    def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._next_id += 1
        envelope = {"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params}
        raw = self._post(json.dumps(envelope).encode("utf-8"))
        decoded = self._decode(raw)
        if "error" in decoded:
            err = decoded["error"] or {}
            raise McpRequestError(
                f"MCP {method} failed: {err.get('message', 'unknown error')}",
                request_id=str(err.get("code")) if err.get("code") is not None else None,
            )
        result = decoded.get("result")
        if not isinstance(result, dict):
            raise McpRequestError(f"MCP {method} returned no result object.")
        return result

    def _post(self, body: bytes, *, retry_on_expiry: bool = True) -> bytes:
        token = self._tokens.get()
        request = urllib.request.Request(self._url, data=body, method="POST")
        request.add_header("Content-Type", "application/json")
        # Streamable HTTP servers may answer either shape; accepting both keeps the
        # client working if the gateway switches to event-stream responses.
        request.add_header("Accept", "application/json, text/event-stream")
        request.add_header("MCP-Protocol-Version", DEFAULT_PROTOCOL_VERSION)
        request.add_header("Authorization", f"Bearer {token}")
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            detail = b""
            try:
                detail = exc.read()
            except Exception:  # noqa: BLE001 -- body is best-effort context only
                pass
            text = detail.decode("utf-8", "replace").strip()
            if exc.code == 401:
                raise McpRequestError(
                    "MCP endpoint rejected the access token.", status=401
                ) from exc
            raise McpRequestError(
                f"MCP endpoint returned HTTP {exc.code}" + (f": {text[:200]}" if text else ""),
                status=exc.code,
            ) from exc
        except urllib.error.URLError as exc:
            raise McpRequestError(f"Could not reach the MCP endpoint: {exc.reason}") from exc

    @staticmethod
    def _decode(raw: bytes) -> dict[str, Any]:
        """Accept plain JSON or an SSE body carrying a single JSON data frame."""
        text = (raw or b"").decode("utf-8", "replace").strip()
        if not text:
            raise McpRequestError("MCP endpoint returned an empty response body.")
        if not text.lstrip().startswith("{"):
            frames = [
                line[len("data:"):].strip()
                for line in text.splitlines()
                if line.startswith("data:")
            ]
            for frame in reversed(frames):
                if frame and frame != "[DONE]":
                    text = frame
                    break
        try:
            decoded = json.loads(text)
        except ValueError as exc:
            raise McpRequestError("MCP endpoint returned a non-JSON response.") from exc
        if not isinstance(decoded, dict):
            raise McpRequestError("MCP endpoint returned an unexpected response shape.")
        return decoded


def default_endpoint(region: str | None, *, site: str = "CN") -> str:
    """Build the public MCP host for a region, falling back to dynamic routing."""
    if not region:
        host = "mcp.maxcompute.aliyun.com" if site.upper() != "INTL" else "mcp-intl.maxcompute.aliyun.com"
        return f"https://{host}/mcp"
    cleaned = str(region).strip().lower()
    if not cleaned.replace("-", "").isalnum():
        raise ValueError("region must be an alphanumeric region id")
    prefix = "mcp" if site.upper() != "INTL" else "mcp-intl"
    return f"https://{prefix}.{cleaned}.maxcompute.aliyun.com/mcp"
