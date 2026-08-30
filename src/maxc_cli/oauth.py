"""Browser-based OAuth login for MaxCompute, ported from aliyun CLI.

This mirrors the official Alibaba Cloud CLI ``--mode OAuth`` implementation
(aliyun/aliyun-cli, config/configure.go):

- Authorization Code + PKCE against signin.aliyun.com / signin.alibabacloud.com
- Local loopback callback server on 127.0.0.1, ports 12345-12349
- Token exchange / refresh at oauth.aliyun.com / oauth.alibabacloud.com
- OAuth access token -> temporary STS via POST /v1/exchange

Prerequisite (same as the official CLI): a RAM admin must have installed the
``official-cli`` OAuth application and assigned identities to the RAM
users/roles that are allowed to log in this way.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import socket
import string
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable

from .auth_continuation import (
    delete_auth_continuation,
    load_auth_continuation,
    save_auth_continuation,
)
from .exceptions import ValidationError

# --- Endpoints and client IDs, verbatim from aliyun-cli config/configure.go --

OAUTH_CLIENT_MAP = {
    "CN": "4038181954557748008",
    "INTL": "4103531455503354461",
}

OAUTH_BASE_URL_MAP = {
    "CN": "https://oauth.aliyun.com",
    "INTL": "https://oauth.alibabacloud.com",
}

SIGN_IN_MAP = {
    "CN": "https://signin.aliyun.com",
    "INTL": "https://signin.alibabacloud.com",
}

CALLBACK_PATH = "/cli/callback"
CALLBACK_PORT_RANGE = (12345, 12349)

# Refresh the cached STS this long before it actually expires.
STS_REFRESH_BUFFER_SECONDS = 300

_DEFAULT_TIMEOUT_SECONDS = 300


@dataclass
class OAuthTokens:
    access_token: str
    refresh_token: str
    expires_at: int  # unix seconds


@dataclass
class StsCredential:
    access_key_id: str
    access_key_secret: str
    security_token: str
    expiration_iso: str  # RFC3339, e.g. "2026-08-19T08:00:00Z"


class OAuthError(ValidationError):
    """OAuth flow failed (browser flow, token exchange, or refresh)."""

    error_code = "OAUTH_ERROR"


def save_oauth_continuation(
    state_dir: Path,
    *,
    target_config_path: Path,
    site_type: str,
    tokens: OAuthTokens,
    sts: StsCredential,
    now: float | None = None,
) -> tuple[str, int]:
    """Save a short-lived OAuth project-selection continuation."""
    return save_auth_continuation(
        state_dir,
        kind="oauth",
        target_config_path=target_config_path,
        secret_payload={
            "site_type": _validate_site_type(site_type),
            "oauth": {
                "access_token": tokens.access_token,
                "refresh_token": tokens.refresh_token,
                "access_token_expire": tokens.expires_at,
            },
            "sts": {
                "access_key_id": sts.access_key_id,
                "access_key_secret": sts.access_key_secret,
                "security_token": sts.security_token,
                "expiration_iso": sts.expiration_iso,
            },
        },
        now=now,
    )


def load_oauth_continuation(
    state_dir: Path,
    continuation_id: str,
    *,
    target_config_path: Path,
    now: float | None = None,
) -> tuple[str, OAuthTokens, StsCredential]:
    """Load, validate, and bind a continuation to its original config path."""
    try:
        payload = load_auth_continuation(
            state_dir,
            continuation_id,
            kind="oauth",
            target_config_path=target_config_path,
            now=now,
        )
    except ValidationError as exc:
        raise OAuthError(
            str(exc),
            suggestion="Restart `maxc auth login --oauth`.",
        ) from exc
    try:
        site_type = _validate_site_type(str(payload["site_type"]))
        oauth_payload = payload["oauth"]
        sts_payload = payload["sts"]
        tokens = OAuthTokens(
            access_token=str(oauth_payload["access_token"]),
            refresh_token=str(oauth_payload["refresh_token"]),
            expires_at=int(oauth_payload["access_token_expire"]),
        )
        sts = StsCredential(
            access_key_id=str(sts_payload["access_key_id"]),
            access_key_secret=str(sts_payload["access_key_secret"]),
            security_token=str(sts_payload["security_token"]),
            expiration_iso=str(sts_payload["expiration_iso"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise OAuthError(
            "The OAuth continuation state is incomplete.",
            suggestion="Restart `maxc auth login --oauth`.",
        ) from exc

    if not all(
        (
            tokens.access_token,
            tokens.refresh_token,
            sts.access_key_id,
            sts.access_key_secret,
            sts.security_token,
            sts.expiration_iso,
        )
    ):
        raise OAuthError(
            "The OAuth continuation state is incomplete.",
            suggestion="Restart `maxc auth login --oauth`.",
        )
    return site_type, tokens, sts


def delete_oauth_continuation(state_dir: Path, continuation_id: str) -> None:
    """Remove a successfully consumed continuation without following links."""
    delete_auth_continuation(state_dir, continuation_id)


def _validate_site_type(site_type: str) -> str:
    site = (site_type or "").upper()
    if site not in ("CN", "INTL"):
        raise ValidationError(
            f"Invalid OAuth site type: {site_type!r}, only CN or INTL are supported."
        )
    return site


# --- PKCE helpers (ported from aliyun-cli generateCodeVerifier/Challenge) ---

_VERIFIER_ALPHABET = string.ascii_letters + string.digits


def generate_code_verifier(length: int = 128) -> str:
    """Random 43-128 char string; aliyun-cli uses 128 alphanumerics."""
    return "".join(secrets.choice(_VERIFIER_ALPHABET) for _ in range(length))


def generate_code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def detect_port(start: int = CALLBACK_PORT_RANGE[0], end: int = CALLBACK_PORT_RANGE[1]) -> int:
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise OAuthError(f"No available port found in range {start}-{end} for the OAuth callback server.")


def build_auth_url(
    *,
    site_type: str,
    redirect_uri: str,
    state: str,
    code_challenge: str,
    sign_in_url: str | None = None,
    client_id: str | None = None,
) -> str:
    site = _validate_site_type(site_type)
    base = sign_in_url or SIGN_IN_MAP[site]
    cid = client_id or OAUTH_CLIENT_MAP[site]
    query = urllib.parse.urlencode(
        {
            "response_type": "code",
            "client_id": cid,
            "redirect_uri": redirect_uri,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
    )
    return f"{base}/oauth2/v1/auth?{query}"


# --- HTTP plumbing -----------------------------------------------------------

def _post_form(url: str, data: dict[str, str], *, timeout: float = 30.0) -> dict[str, Any]:
    from .odps_runtime import outbound_http_user_agent

    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": outbound_http_user_agent(),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        # OAuth error bodies are untrusted and may contain tokens or echoed
        # request material. Consume but never copy them into CLI output.
        exc.read()
        request_id = exc.headers.get("x-acs-request-id") if exc.headers else None
        suffix = f" (request id: {request_id})" if request_id else ""
        raise OAuthError(f"Token endpoint returned HTTP {exc.code}{suffix}.") from exc
    except urllib.error.URLError as exc:
        raise OAuthError(
            f"Cannot reach OAuth endpoint {url}: {exc.reason}.",
            suggestion="Check network connectivity and proxy settings, then retry.",
        ) from exc
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise OAuthError("Token endpoint returned an invalid JSON response.") from exc
    if not isinstance(payload, dict):
        raise OAuthError("Token endpoint returned an unexpected response shape.")
    return payload


def _parse_token_response(payload: dict[str, Any]) -> OAuthTokens:
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise OAuthError("access_token not found in OAuth token response.")
    try:
        expires_in = int(payload.get("expires_in", 0))
    except (TypeError, ValueError) as exc:
        raise OAuthError("expires_in is invalid in the OAuth token response.") from exc
    if expires_in <= 0:
        raise OAuthError("expires_in must be positive in the OAuth token response.")
    refresh_token = payload.get("refresh_token") or ""
    if not isinstance(refresh_token, str):
        raise OAuthError("refresh_token is invalid in the OAuth token response.")
    return OAuthTokens(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=int(time.time()) + expires_in,
    )


# --- Browser flow ------------------------------------------------------------

def start_oauth_flow(
    site_type: str = "CN",
    *,
    open_browser: bool = True,
    on_url: Callable[[str], None] | None = None,
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    oauth_base_url: str | None = None,
    sign_in_url: str | None = None,
    client_id: str | None = None,
) -> OAuthTokens:
    """Run the Authorization Code + PKCE flow like ``aliyun configure --mode OAuth``.

    Starts a loopback callback server, presents the sign-in URL (opening the
    browser unless disabled), waits for the callback, and exchanges the code
    for tokens. ``on_url`` receives the sign-in URL for custom presentation
    (e.g. CLI stderr in JSON mode).
    """
    site = _validate_site_type(site_type)
    cid = client_id or OAUTH_CLIENT_MAP[site]
    base_url = oauth_base_url or OAUTH_BASE_URL_MAP[site]

    state = "".join(secrets.choice(_VERIFIER_ALPHABET) for _ in range(16))
    code_verifier = generate_code_verifier()
    code_challenge = generate_code_challenge(code_verifier)

    result: dict[str, str] = {}
    error: dict[str, str] = {}
    done = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib naming
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != CALLBACK_PATH:
                self.send_error(404)
                return
            form = urllib.parse.parse_qs(parsed.query)
            if form.get("state", [""])[0] != state:
                error["message"] = "invalid state"
                self.send_error(400, "Invalid state")
                done.set()
                return
            code = form.get("code", [""])[0]
            if not code:
                error["message"] = "code not found in callback"
                self.send_error(400, "Code not found")
                done.set()
                return
            body = b"Authorization successful. You can close this window."
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            result["code"] = code
            done.set()

        def log_message(self, format, *args):  # noqa: A002 - stdlib naming
            pass

    # Bind the real callback server directly instead of probing with
    # ``detect_port`` and releasing the socket before constructing HTTPServer.
    # Besides avoiding that TOCTOU window, starting the server before publishing
    # the URL lets an ``on_url`` callback complete the redirect synchronously.
    server = None
    for port in range(CALLBACK_PORT_RANGE[0], CALLBACK_PORT_RANGE[1] + 1):
        try:
            server = HTTPServer(("127.0.0.1", port), CallbackHandler)
            break
        except OSError:
            continue
    if server is None:
        start, end = CALLBACK_PORT_RANGE
        raise OAuthError(
            f"No available port found in range {start}-{end} for the OAuth callback server."
        )

    port = int(server.server_address[1])
    redirect_uri = f"http://127.0.0.1:{port}{CALLBACK_PATH}"
    auth_url = build_auth_url(
        site_type=site,
        redirect_uri=redirect_uri,
        state=state,
        code_challenge=code_challenge,
        sign_in_url=sign_in_url,
        client_id=client_id,
    )
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    try:
        server_thread.start()
    except BaseException:
        server.server_close()
        raise
    try:
        if on_url is not None:
            on_url(auth_url)
        if open_browser:
            # Best effort: on headless machines this silently does nothing.
            try:
                webbrowser.open(auth_url)
            except Exception:
                pass

        if not done.wait(timeout=timeout_seconds):
            raise OAuthError(
                f"Timed out waiting for the OAuth callback ({int(timeout_seconds)}s).",
                suggestion="Retry `maxc auth login --oauth` and complete the browser sign-in promptly.",
            )
        if error:
            raise OAuthError(f"OAuth callback failed: {error['message']}")
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    payload = _post_form(
        f"{base_url}/v1/token",
        {
            "grant_type": "authorization_code",
            "code": result["code"],
            "client_id": cid,
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
        },
    )
    return _parse_token_response(payload)


def refresh_oauth_token(
    site_type: str,
    refresh_token: str,
    *,
    oauth_base_url: str | None = None,
    client_id: str | None = None,
) -> OAuthTokens:
    """Exchange a refresh token for a new token pair (ported from tryRefreshOauthToken)."""
    site = _validate_site_type(site_type)
    if not refresh_token:
        raise OAuthError(
            "OAuth refresh token is empty; please re-authenticate.",
            suggestion="Run `maxc auth login --oauth` to sign in again.",
        )
    payload = _post_form(
        f"{oauth_base_url or OAUTH_BASE_URL_MAP[site]}/v1/token",
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id or OAUTH_CLIENT_MAP[site],
        },
    )
    tokens = _parse_token_response(payload)
    # OAuth servers are allowed to rotate refresh tokens, but some successful
    # refresh responses omit the field and expect the client to keep using the
    # previous token. Never replace a usable token with an empty string.
    if not tokens.refresh_token:
        tokens.refresh_token = refresh_token
    return tokens


def exchange_sts(
    site_type: str,
    access_token: str,
    *,
    oauth_base_url: str | None = None,
) -> StsCredential:
    """Trade an OAuth access token for temporary STS credentials (POST /v1/exchange)."""
    from .odps_runtime import outbound_http_user_agent

    site = _validate_site_type(site_type)
    if not access_token:
        raise OAuthError(
            "OAuth access token is empty; please re-authenticate.",
            suggestion="Run `maxc auth login --oauth` to sign in again.",
        )
    url = f"{oauth_base_url or OAUTH_BASE_URL_MAP[site]}/v1/exchange"
    req = urllib.request.Request(
        url,
        data=b"",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "User-Agent": outbound_http_user_agent(),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30.0) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        exc.read()
        request_id = exc.headers.get("x-acs-request-id") if exc.headers else None
        suffix = f" (request id: {request_id})" if request_id else ""
        raise OAuthError(f"STS exchange failed with HTTP {exc.code}{suffix}.") from exc
    except urllib.error.URLError as exc:
        raise OAuthError(f"Cannot reach OAuth endpoint {url}: {exc.reason}.") from exc

    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise OAuthError("STS exchange returned an invalid JSON response.") from exc
    if not isinstance(payload, dict):
        raise OAuthError("STS exchange returned an unexpected response shape.")
    required_fields = (
        "accessKeyId",
        "accessKeySecret",
        "securityToken",
        "expiration",
    )
    missing = [
        field
        for field in required_fields
        if not isinstance(payload.get(field), str) or not payload.get(field)
    ]
    if missing:
        raise OAuthError(
            "STS exchange response is missing required credential fields: "
            + ", ".join(missing)
            + "."
        )
    return StsCredential(
        access_key_id=payload["accessKeyId"],
        access_key_secret=payload["accessKeySecret"],
        security_token=payload["securityToken"],
        expiration_iso=payload["expiration"],
    )


# --- Cached-credential lifecycle ----------------------------------------------

def _parse_rfc3339(value: str) -> float:
    from datetime import datetime

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).timestamp()


def sts_cache_valid(auth, *, now: float | None = None) -> bool:
    """True when the cached STS triple in AuthConfig is still usable."""
    if not (auth.access_id and auth.secret_access_key and auth.security_token):
        return False
    if not auth.token_expires_at:
        return False
    try:
        expiry = _parse_rfc3339(auth.token_expires_at)
    except (ValueError, TypeError):
        return False
    current = now if now is not None else time.time()
    return expiry - STS_REFRESH_BUFFER_SECONDS > current


def _cached_sts(auth) -> StsCredential:
    return StsCredential(
        access_key_id=auth.access_id,
        access_key_secret=auth.secret_access_key,
        security_token=auth.security_token,
        expiration_iso=auth.token_expires_at,
    )


def _refresh_oauth_sts(auth) -> StsCredential:
    """Refresh *auth* in memory and return its new STS credential."""
    if sts_cache_valid(auth):
        return _cached_sts(auth)

    oauth = auth.oauth
    if not oauth.is_configured() or not oauth.site_type:
        raise OAuthError(
            "OAuth login is not configured.",
            suggestion="Run `maxc auth login --oauth` to sign in.",
        )
    site = _validate_site_type(oauth.site_type)

    access_token = oauth.access_token or ""
    if not oauth.access_token_expire or oauth.access_token_expire <= int(time.time()):
        tokens = refresh_oauth_token(site, oauth.refresh_token or "")
        oauth.access_token = tokens.access_token
        oauth.refresh_token = tokens.refresh_token
        oauth.access_token_expire = tokens.expires_at
        access_token = tokens.access_token

    sts = exchange_sts(site, access_token)

    # Write exchanged credentials + rotated tokens back, like aliyun-cli does.
    auth.access_id = sts.access_key_id
    auth.secret_access_key = sts.access_key_secret
    auth.security_token = sts.security_token
    auth.token_expires_at = sts.expiration_iso

    return sts


def ensure_oauth_sts(auth, *, config_path: Path | None = None) -> StsCredential:
    """Return usable OAuth STS without racing login/logout config updates.

    A refresh that commits after logout must never resurrect deleted tokens.
    When persistence is enabled, the config lock is therefore held from the
    authoritative re-read through token exchange and atomic replacement. A
    changed provider/project/endpoint fails closed instead of writing stale
    state over a newer login.
    """
    if sts_cache_valid(auth):
        return _cached_sts(auth)
    if config_path is None:
        return _refresh_oauth_sts(auth)

    from .config import (
        AuthConfig,
        _save_config_mapping_unlocked,
        config_file_lock,
        load_config_mapping,
    )

    with config_file_lock(config_path):
        payload = load_config_mapping(config_path) if config_path.exists() else {}
        raw_auth = payload.get("auth")
        if not isinstance(raw_auth, dict):
            raise OAuthError(
                "OAuth configuration changed or was removed before refresh completed.",
                suggestion="Run `maxc auth whoami --json` before signing in again.",
            )
        current_auth = AuthConfig.from_mapping(raw_auth)
        current_provider = str(current_auth.provider or "").lower()
        expected_binding = (
            auth.project,
            auth.endpoint,
            str(auth.oauth.site_type or "").upper(),
        )
        current_binding = (
            current_auth.project,
            current_auth.endpoint,
            str(current_auth.oauth.site_type or "").upper(),
        )
        if current_provider != "oauth" or current_binding != expected_binding:
            raise OAuthError(
                "OAuth configuration changed or was removed before refresh completed.",
                suggestion="Use the current saved login; do not retry with stale OAuth state.",
            )

        sts = _refresh_oauth_sts(current_auth)
        payload["auth"] = current_auth.to_mapping()
        _save_config_mapping_unlocked(config_path, payload)
        return sts
