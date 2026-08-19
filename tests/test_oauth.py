"""Tests for the OAuth login flow (ported from aliyun CLI --mode OAuth)."""

import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from maxc_cli import oauth
from maxc_cli.config import AuthConfig, OAuthAuthConfig
from maxc_cli.oauth import (
    OAuthError,
    build_auth_url,
    detect_port,
    ensure_oauth_sts,
    exchange_sts,
    generate_code_challenge,
    generate_code_verifier,
    refresh_oauth_token,
    start_oauth_flow,
    sts_cache_valid,
)


# --- PKCE --------------------------------------------------------------------

def test_code_verifier_is_128_alnum() -> None:
    verifier = generate_code_verifier()
    assert len(verifier) == 128
    assert verifier.isalnum()


def test_code_challenge_matches_rfc7636_vector() -> None:
    # RFC 7636 appendix B known answer.
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    assert generate_code_challenge(verifier) == "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"


# --- URL / port ----------------------------------------------------------------

def test_build_auth_url_contains_pkce_parameters() -> None:
    url = build_auth_url(
        site_type="CN",
        redirect_uri="http://127.0.0.1:12345/cli/callback",
        state="abc123",
        code_challenge="challenge",
    )
    assert url.startswith("https://signin.aliyun.com/oauth2/v1/auth?")
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert query["response_type"] == ["code"]
    assert query["client_id"] == [oauth.OAUTH_CLIENT_MAP["CN"]]
    assert query["redirect_uri"] == ["http://127.0.0.1:12345/cli/callback"]
    assert query["state"] == ["abc123"]
    assert query["code_challenge"] == ["challenge"]
    assert query["code_challenge_method"] == ["S256"]


def test_build_auth_url_intl_uses_intl_endpoints() -> None:
    url = build_auth_url(
        site_type="INTL",
        redirect_uri="http://127.0.0.1:12345/cli/callback",
        state="s",
        code_challenge="c",
    )
    assert url.startswith("https://signin.alibabacloud.com/oauth2/v1/auth?")
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert query["client_id"] == [oauth.OAUTH_CLIENT_MAP["INTL"]]


def test_build_auth_url_rejects_bad_site_type() -> None:
    with pytest.raises(Exception):
        build_auth_url(site_type="XX", redirect_uri="u", state="s", code_challenge="c")


def test_detect_port_returns_port_in_range() -> None:
    port = detect_port()
    assert 12345 <= port <= 12349


# --- Fake OAuth server -----------------------------------------------------------

class _FakeOAuthServer:
    """Serves /v1/token and /v1/exchange on a loopback port."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.handler = HTTPServer(("127.0.0.1", 0), self._make_handler())
        self.port = self.handler.server_address[1]
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.handler.serve_forever, daemon=True)
        self.thread.start()

    def _make_handler(self):
        requests = self.requests

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode("utf-8")
                requests.append(
                    {
                        "path": self.path,
                        "body": body,
                        "authorization": self.headers.get("Authorization"),
                    }
                )
                if self.path == "/v1/token":
                    form = urllib.parse.parse_qs(body)
                    grant = form.get("grant_type", [""])[0]
                    if grant == "authorization_code":
                        payload = {
                            "access_token": "at-1",
                            "refresh_token": "rt-1",
                            "expires_in": 3600,
                            "token_type": "Bearer",
                        }
                    elif grant == "refresh_token":
                        payload = {
                            "access_token": "at-2",
                            "refresh_token": "rt-2",
                            "expires_in": 3600,
                            "token_type": "Bearer",
                        }
                    else:
                        self.send_error(400, "bad grant")
                        return
                elif self.path == "/v1/exchange":
                    payload = {
                        "requestId": "req-1",
                        "accessKeyId": "STS.AKID",
                        "accessKeySecret": "STSSECRET",
                        "securityToken": "STSTOKEN",
                        "expiration": "2099-01-01T00:00:00Z",
                    }
                else:
                    self.send_error(404)
                    return
                data = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format, *args):  # noqa: A002
                pass

        return Handler

    def stop(self) -> None:
        self.handler.shutdown()
        self.handler.server_close()
        self.thread.join(timeout=5)


@pytest.fixture
def fake_oauth():
    server = _FakeOAuthServer()
    yield server
    server.stop()


# --- Token endpoints -----------------------------------------------------------

def test_refresh_oauth_token_posts_refresh_grant(fake_oauth: _FakeOAuthServer) -> None:
    tokens = refresh_oauth_token("CN", "rt-old", oauth_base_url=fake_oauth.base_url)
    assert tokens.access_token == "at-2"
    assert tokens.refresh_token == "rt-2"
    assert tokens.expires_at > 0

    form = urllib.parse.parse_qs(fake_oauth.requests[0]["body"])
    assert form["grant_type"] == ["refresh_token"]
    assert form["refresh_token"] == ["rt-old"]
    assert form["client_id"] == [oauth.OAUTH_CLIENT_MAP["CN"]]


def test_refresh_oauth_token_empty_refresh_token_raises() -> None:
    with pytest.raises(OAuthError):
        refresh_oauth_token("CN", "")


def test_exchange_sts_sends_bearer_and_parses_response(fake_oauth: _FakeOAuthServer) -> None:
    sts = exchange_sts("CN", "at-1", oauth_base_url=fake_oauth.base_url)
    assert sts.access_key_id == "STS.AKID"
    assert sts.access_key_secret == "STSSECRET"
    assert sts.security_token == "STSTOKEN"
    assert sts.expiration_iso == "2099-01-01T00:00:00Z"
    assert fake_oauth.requests[0]["authorization"] == "Bearer at-1"


# --- Full browser flow -----------------------------------------------------------

def test_start_oauth_flow_end_to_end(fake_oauth: _FakeOAuthServer) -> None:
    captured: dict[str, str] = {}

    def simulate_browser(url: str) -> None:
        captured["url"] = url
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        redirect_uri = query["redirect_uri"][0]
        state = query["state"][0]
        callback = f"{redirect_uri}?code=authcode-1&state={urllib.parse.quote(state)}"

        def hit_callback() -> None:
            import time
            import urllib.error
            import urllib.request

            # The callback server starts right after on_url returns; retry
            # until it is listening instead of racing its startup.
            for _ in range(50):
                try:
                    urllib.request.urlopen(callback, timeout=5).read()
                    return
                except urllib.error.URLError:
                    time.sleep(0.1)

        threading.Thread(target=hit_callback, daemon=True).start()

    tokens = start_oauth_flow(
        "CN",
        open_browser=False,
        on_url=simulate_browser,
        oauth_base_url=fake_oauth.base_url,
        sign_in_url=fake_oauth.base_url,  # only the URL shape matters here
        timeout_seconds=15,
    )

    assert tokens.access_token == "at-1"
    assert tokens.refresh_token == "rt-1"

    # The code exchange must carry PKCE material.
    token_posts = [r for r in fake_oauth.requests if r["path"] == "/v1/token"]
    assert len(token_posts) == 1
    form = urllib.parse.parse_qs(token_posts[0]["body"])
    assert form["grant_type"] == ["authorization_code"]
    assert form["code"] == ["authcode-1"]
    assert form["code_verifier"][0] != ""
    assert form["client_id"] == [oauth.OAUTH_CLIENT_MAP["CN"]]

    # The auth URL itself carried the S256 challenge derived from the verifier.
    auth_query = urllib.parse.parse_qs(urllib.parse.urlparse(captured["url"]).query)
    assert auth_query["code_challenge"][0] == generate_code_challenge(form["code_verifier"][0])
    assert auth_query["code_challenge_method"] == ["S256"]


def test_start_oauth_flow_times_out_without_callback(fake_oauth: _FakeOAuthServer) -> None:
    with pytest.raises(OAuthError, match="Timed out"):
        start_oauth_flow(
            "CN",
            open_browser=False,
            oauth_base_url=fake_oauth.base_url,
            sign_in_url=fake_oauth.base_url,
            timeout_seconds=0.5,
        )


# --- STS cache lifecycle -----------------------------------------------------------

def _oauth_auth(**overrides) -> AuthConfig:
    auth = AuthConfig(
        provider="oauth",
        access_id="CACHED.AK",
        secret_access_key="CACHEDSECRET",
        security_token="CACHEDTOKEN",
        token_expires_at="2099-01-01T00:00:00Z",
        project="p",
        endpoint="e",
        oauth=OAuthAuthConfig(
            site_type="CN",
            access_token="at-cached",
            refresh_token="rt-cached",
            access_token_expire=9_999_999_999,
        ),
    )
    for key, value in overrides.items():
        setattr(auth, key, value)
    return auth


def test_sts_cache_valid_accepts_future_expiry() -> None:
    assert sts_cache_valid(_oauth_auth()) is True


def test_sts_cache_valid_rejects_expired_or_missing() -> None:
    assert sts_cache_valid(_oauth_auth(token_expires_at="2000-01-01T00:00:00Z")) is False
    assert sts_cache_valid(_oauth_auth(token_expires_at=None)) is False
    assert sts_cache_valid(_oauth_auth(security_token=None)) is False
    assert sts_cache_valid(_oauth_auth(token_expires_at="not-a-date")) is False


def test_ensure_oauth_sts_uses_cache_without_network(fake_oauth: _FakeOAuthServer) -> None:
    sts = ensure_oauth_sts(_oauth_auth(), config_path=None)
    assert sts.access_key_id == "CACHED.AK"
    assert fake_oauth.requests == []


def test_ensure_oauth_sts_refreshes_and_persists(
    fake_oauth: _FakeOAuthServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(oauth.OAUTH_BASE_URL_MAP, "CN", fake_oauth.base_url)
    auth = _oauth_auth(
        access_id="OLD.AK",
        secret_access_key="OLDSECRET",
        security_token="OLDTOKEN",
        token_expires_at="2000-01-01T00:00:00Z",  # STS expired
        oauth=OAuthAuthConfig(
            site_type="CN",
            access_token="at-old",
            refresh_token="rt-old",
            access_token_expire=1,  # access token expired -> refresh first
        ),
    )
    config_file = tmp_path / "config.yaml"
    config_file.write_text("auth:\n  provider: oauth\n", encoding="utf-8")

    sts = ensure_oauth_sts(auth, config_path=config_file)

    assert sts.access_key_id == "STS.AKID"
    # refresh then exchange
    assert [r["path"] for r in fake_oauth.requests] == ["/v1/token", "/v1/exchange"]
    # exchanged credentials + rotated tokens persisted
    saved = config_file.read_text(encoding="utf-8")
    assert "STS.AKID" in saved
    assert "rt-2" in saved
    assert "2099-01-01T00:00:00Z" in saved


def test_ensure_oauth_sts_without_oauth_config_raises() -> None:
    auth = AuthConfig(provider="oauth")
    with pytest.raises(OAuthError):
        ensure_oauth_sts(auth, config_path=None)


# --- Config round-trip -----------------------------------------------------------

def test_auth_config_oauth_round_trip() -> None:
    auth = _oauth_auth()
    mapping = auth.to_mapping()
    assert mapping["oauth"]["site_type"] == "CN"
    assert mapping["oauth"]["access_token"] == "at-cached"
    assert mapping["oauth"]["access_token_expire"] == 9_999_999_999

    restored = AuthConfig.from_mapping(mapping)
    assert restored.provider == "oauth"
    assert restored.oauth.is_configured()
    assert restored.oauth.refresh_token == "rt-cached"


# --- Provider inference & CLI parsing -----------------------------------------------------------

def test_infer_auth_provider_prefers_oauth_over_cached_sts(tmp_path: Path) -> None:
    from maxc_cli.auth_providers import infer_auth_provider, resolve_odps_settings

    from tests.test_job_improvements import make_app

    app = make_app(tmp_path)
    app.config.auth = _oauth_auth()
    settings, _, _ = resolve_odps_settings(app.config)
    # Cached STS triple must not shadow the OAuth provider.
    assert infer_auth_provider(app.config, settings) == "oauth"


def test_auth_login_oauth_flags_parse(tmp_path: Path) -> None:
    from maxc_cli.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["auth", "login", "--oauth", "--site-type", "INTL", "--no-browser"])
    assert args.oauth is True
    assert args.site_type == "INTL"
    assert args.no_browser is True

    default_args = parser.parse_args(["auth", "login"])
    assert default_args.oauth is False
    assert default_args.site_type == "CN"
