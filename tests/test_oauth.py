"""Tests for the OAuth login flow (ported from aliyun CLI --mode OAuth)."""

import io
import json
import shlex
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from maxc_cli import oauth
from maxc_cli.config import AuthConfig, OAuthAuthConfig
from maxc_cli.exceptions import ValidationError
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
                        "user_agent": self.headers.get("User-Agent"),
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
    assert fake_oauth.requests[0]["user_agent"].startswith("maxc-cli/")


def test_refresh_oauth_token_empty_refresh_token_raises() -> None:
    with pytest.raises(OAuthError):
        refresh_oauth_token("CN", "")


def test_refresh_oauth_token_preserves_old_token_when_response_omits_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oauth,
        "_post_form",
        lambda *args, **kwargs: {
            "access_token": "at-rotated",
            "expires_in": 3600,
            "token_type": "Bearer",
        },
    )

    tokens = refresh_oauth_token("CN", "rt-still-valid")

    assert tokens.access_token == "at-rotated"
    assert tokens.refresh_token == "rt-still-valid"


def test_exchange_sts_sends_bearer_and_parses_response(fake_oauth: _FakeOAuthServer) -> None:
    sts = exchange_sts("CN", "at-1", oauth_base_url=fake_oauth.base_url)
    assert sts.access_key_id == "STS.AKID"
    assert sts.access_key_secret == "STSSECRET"
    assert sts.security_token == "STSTOKEN"
    assert sts.expiration_iso == "2099-01-01T00:00:00Z"
    assert fake_oauth.requests[0]["authorization"] == "Bearer at-1"
    assert fake_oauth.requests[0]["user_agent"].startswith("maxc-cli/")


def test_oauth_http_requests_include_agent_observability_user_agent(
    fake_oauth: _FakeOAuthServer,
) -> None:
    from maxc_cli.odps_runtime import set_agent_user_agent

    skill_ua = (
        "AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/"
        "0123456789abcdef0123456789abcdef"
    )
    set_agent_user_agent(skill_ua)
    try:
        refresh_oauth_token("CN", "rt-old", oauth_base_url=fake_oauth.base_url)
        exchange_sts("CN", "at-1", oauth_base_url=fake_oauth.base_url)
    finally:
        set_agent_user_agent(None)

    assert len(fake_oauth.requests) == 2
    assert all(skill_ua in request["user_agent"] for request in fake_oauth.requests)


def test_exchange_sts_http_error_never_exposes_response_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = urllib.error.HTTPError(
        "https://oauth.aliyun.com/v1/exchange",
        403,
        "Forbidden",
        {"x-acs-request-id": "safe-request-id"},
        io.BytesIO(b'{"accessKeySecret":"TOP_SECRET_SENTINEL"}'),
    )
    monkeypatch.setattr(oauth.urllib.request, "urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(error))

    with pytest.raises(OAuthError) as excinfo:
        exchange_sts("CN", "access-token")

    payload = excinfo.value.to_payload().to_dict()
    assert "safe-request-id" in payload["message"]
    assert "TOP_SECRET_SENTINEL" not in str(payload)
    assert "accessKeySecret" not in str(payload)


def test_exchange_sts_missing_fields_never_exposes_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def read():
            return json.dumps(
                {
                    "accessKeySecret": "TOP_SECRET_SENTINEL",
                    "requestId": "request-1",
                }
            ).encode()

    monkeypatch.setattr(oauth.urllib.request, "urlopen", lambda *args, **kwargs: _Response())

    with pytest.raises(OAuthError) as excinfo:
        exchange_sts("CN", "access-token")

    payload = excinfo.value.to_payload().to_dict()
    assert "TOP_SECRET_SENTINEL" not in str(payload)
    assert "accessKeyId" in payload["message"]
    assert "securityToken" in payload["message"]


# --- Full browser flow -----------------------------------------------------------

def test_start_oauth_flow_end_to_end(
    fake_oauth: _FakeOAuthServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def fail_if_probed(*args, **kwargs):
        pytest.fail("start_oauth_flow must bind HTTPServer directly instead of probing a port")

    monkeypatch.setattr(oauth, "detect_port", fail_if_probed)

    def simulate_browser(url: str) -> None:
        captured["url"] = url
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        redirect_uri = query["redirect_uri"][0]
        state = query["state"][0]
        callback = f"{redirect_uri}?code=authcode-1&state={urllib.parse.quote(state)}"

        # The callback server must already be listening before on_url is
        # invoked, so a synchronous redirect works without retrying.
        urllib.request.urlopen(callback, timeout=5).read()

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
    auth.project = "keep_project"
    config_file.write_text(
        json.dumps({"auth": auth.to_mapping()}),
        encoding="utf-8",
    )

    sts = ensure_oauth_sts(auth, config_path=config_file)

    assert sts.access_key_id == "STS.AKID"
    # refresh then exchange
    assert [r["path"] for r in fake_oauth.requests] == ["/v1/token", "/v1/exchange"]
    # exchanged credentials + rotated tokens persisted
    saved = config_file.read_text(encoding="utf-8")
    assert "STS.AKID" in saved
    assert "rt-2" in saved
    assert "2099-01-01T00:00:00Z" in saved
    assert "keep_project" in saved


def test_oauth_refresh_and_logout_are_serialized_without_credential_resurrection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from maxc_cli.config import load_config_mapping, update_config_mapping

    auth = _oauth_auth(
        token_expires_at="2000-01-01T00:00:00Z",
        oauth=OAuthAuthConfig(
            site_type="CN",
            access_token="expired-at",
            refresh_token="refresh-before-logout",
            access_token_expire=1,
        ),
    )
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        json.dumps({"auth": auth.to_mapping()}),
        encoding="utf-8",
    )
    refresh_started = threading.Event()
    release_refresh = threading.Event()
    logout_attempted = threading.Event()
    errors: list[BaseException] = []

    def blocked_refresh(_site: str, _token: str):
        refresh_started.set()
        assert release_refresh.wait(timeout=5)
        return oauth.OAuthTokens("new-at", "new-rt", 9_999_999_999)

    monkeypatch.setattr(oauth, "refresh_oauth_token", blocked_refresh)
    monkeypatch.setattr(
        oauth,
        "exchange_sts",
        lambda *_args, **_kwargs: oauth.StsCredential(
            "NEW.AK", "NEW.SECRET", "NEW.TOKEN", "2099-01-01T00:00:00Z"
        ),
    )

    def refresh_worker() -> None:
        try:
            ensure_oauth_sts(auth, config_path=config_file)
        except BaseException as exc:  # pragma: no cover - assertion reports below
            errors.append(exc)

    def logout_worker() -> None:
        logout_attempted.set()

        def clear(payload):
            payload.pop("auth", None)

        update_config_mapping(config_file, clear)

    refresh_thread = threading.Thread(target=refresh_worker)
    refresh_thread.start()
    assert refresh_started.wait(timeout=5)
    logout_thread = threading.Thread(target=logout_worker)
    logout_thread.start()
    assert logout_attempted.wait(timeout=5)
    release_refresh.set()
    refresh_thread.join(timeout=5)
    logout_thread.join(timeout=5)

    assert not refresh_thread.is_alive()
    assert not logout_thread.is_alive()
    assert errors == []
    assert "auth" not in load_config_mapping(config_file)


def test_oauth_persist_path_uses_highest_precedence_token_owner(tmp_path: Path) -> None:
    from maxc_cli.auth_providers import _oauth_persist_path
    from maxc_cli.config import load_config

    global_config = tmp_path / "global.yaml"
    project_config = tmp_path / "project.yaml"
    global_config.write_text(
        "auth:\n  provider: oauth\n  oauth:\n    site_type: CN\n    refresh_token: global-token\n",
        encoding="utf-8",
    )
    project_config.write_text(
        "auth:\n  oauth:\n    site_type: CN\n    refresh_token: project-token\n",
        encoding="utf-8",
    )
    config = load_config(tmp_path, global_config)
    config.sources = [global_config, project_config]

    assert _oauth_persist_path(config) == project_config


def test_oauth_persist_path_does_not_write_project_override_instead_of_token_owner(
    tmp_path: Path,
) -> None:
    from maxc_cli.auth_providers import _oauth_persist_path
    from maxc_cli.config import load_config

    global_config = tmp_path / "global.yaml"
    project_config = tmp_path / "project.yaml"
    global_config.write_text(
        "auth:\n  provider: oauth\n  oauth:\n    site_type: CN\n    refresh_token: global-token\n",
        encoding="utf-8",
    )
    project_config.write_text(
        "auth:\n  project: project_override\n",
        encoding="utf-8",
    )
    config = load_config(tmp_path, global_config)
    config.sources = [global_config, project_config]

    assert _oauth_persist_path(config) == global_config


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
    assert default_args.site_type is None


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["auth", "login", "--no-browser"], "only valid with `--oauth`"),
        (["auth", "login", "--site-type", "INTL"], "only valid with `--oauth`"),
        (["auth", "login", "--oauth", "--from-env"], "cannot be combined"),
        (["auth", "login", "--oauth", "--access-id", "AK"], "cannot be combined"),
        (
            ["auth", "login", "--oauth", "--secret-access-key", "SECRET"],
            "cannot be combined",
        ),
        (
            ["auth", "login", "--oauth", "--security-token", "TOKEN"],
            "cannot be combined",
        ),
    ],
)
def test_auth_login_rejects_flags_that_would_be_silently_ignored(
    argv: list[str],
    message: str,
) -> None:
    from maxc_cli.cli import build_parser

    args = build_parser().parse_args(argv)
    args.stderr = io.StringIO()
    args.requested_config_path = None

    class _NoCallApp:
        def auth_login(self, **_kwargs):
            raise AssertionError("access-key login must not run")

        def auth_login_oauth(self, **_kwargs):
            raise AssertionError("OAuth login must not run")

    with pytest.raises(ValidationError, match=message):
        args.handler(_NoCallApp(), args, io.StringIO())


def test_auth_login_oauth_persists_refreshable_identity_in_one_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful OAuth login must not leave a transient STS-only config."""
    import yaml

    from maxc_cli.app import MaxCApp

    config_path = tmp_path / "config.yaml"
    config_path.write_text("default_format: json\n", encoding="utf-8")
    monkeypatch.setattr(
        oauth,
        "start_oauth_flow",
        lambda *args, **kwargs: oauth.OAuthTokens("oauth-at", "oauth-rt", 9_999_999_999),
    )
    monkeypatch.setattr(
        oauth,
        "exchange_sts",
        lambda *args, **kwargs: oauth.StsCredential(
            "STS.AKID", "STS.SECRET", "STS.TOKEN", "2099-01-01T00:00:00Z"
        ),
    )
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    monkeypatch.setattr(
        app,
        "_validate_auth_config",
        lambda auth: (
            {
                "authenticated": True,
                "configured": True,
                "validation_status": "verified",
                "backend": "odps",
                "auth_type": auth.provider,
                "identity_source": "config_file",
                "principal_display": "STS.****AKID",
                "principal_masked": "STS.****AKID",
                "project": auth.project,
                "region": auth.region_name,
                "endpoint": auth.endpoint,
                "project_owner": None,
                "allowed_operations": [],
            },
            [],
        ),
    )

    envelope = app.auth_login_oauth(
        project="oauth_project",
        endpoint="http://service.cn-test.maxcompute.aliyun.com/api",
        no_picker=True,
        target_config_path=config_path,
    )

    assert envelope.status == "success"
    assert envelope.data["auth_type"] == "oauth"
    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))["auth"]
    assert saved["provider"] == "oauth"
    assert saved["oauth"]["refresh_token"] == "oauth-rt"
    assert saved["security_token"] == "STS.TOKEN"


def test_oauth_project_selection_resumes_without_reauthorizing_and_preserves_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import yaml

    from maxc_cli import catalog_bootstrap as cb
    from maxc_cli.app import MaxCApp
    from maxc_cli.odps_runtime import set_agent_user_agent

    config_path = tmp_path / "custom config.yaml"
    config_path.write_text("default_format: json\n", encoding="utf-8")
    calls: list[str] = []

    def fake_oauth(*args, **kwargs):
        calls.append("oauth")
        return oauth.OAuthTokens("oauth-at", "oauth-rt", 9_999_999_999)

    monkeypatch.setattr(oauth, "start_oauth_flow", fake_oauth)
    monkeypatch.setattr(
        oauth,
        "exchange_sts",
        lambda *args, **kwargs: oauth.StsCredential(
            "STS.AKID", "STS.SECRET", "STS.TOKEN", "2099-01-01T00:00:00Z"
        ),
    )
    monkeypatch.setattr(cb, "build_bootstrap_odps", lambda **kwargs: object())
    monkeypatch.setattr(
        cb,
        "list_all_projects",
        lambda _client: [
            cb.ProjectInfo("intl_project", "ap-southeast-1", "owner", True, None),
        ],
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    set_agent_user_agent("AlibabaCloud-Agent-Skills/test/session123")
    try:
        pending = app.auth_login_oauth(
            site_type="INTL",
            no_browser=True,
            endpoint="https://private.service.example.test/api",
            region_name="custom-intl-region",
            tunnel_endpoint="https://private.tunnel.example.test",
            catalog_endpoint="https://catalog.example.test",
            no_validate=True,
            target_config_path=config_path,
        )
    finally:
        set_agent_user_agent(None)

    assert pending.status == "pending"
    assert calls == ["oauth"]
    assert len(pending.agent_hints.actions) == 1
    suggested = pending.agent_hints.actions[0]
    assert suggested.executable is False
    assert suggested.confirmation_required is True
    assert suggested.agent_allowed is False
    argv = shlex.split(suggested.command)
    assert argv[:3] == ["maxc", "--config", str(config_path)]
    assert argv[argv.index("--site-type") + 1] == "INTL"
    assert argv[argv.index("--project") + 1] == "intl_project"
    assert argv[argv.index("--endpoint") + 1] == (
        "https://private.service.example.test/api"
    )
    assert argv[argv.index("--region") + 1] == "custom-intl-region"
    assert argv[argv.index("--tunnel-endpoint") + 1] == (
        "https://private.tunnel.example.test"
    )
    assert argv[argv.index("--catalog-endpoint") + 1] == "https://catalog.example.test"
    assert argv[argv.index("--user-agent") + 1] == (
        "AlibabaCloud-Agent-Skills/test/session123"
    )
    assert "--no-browser" in argv
    assert "--no-validate" in argv
    assert "oauth-at" not in suggested.command
    assert "STS.SECRET" not in suggested.command

    continuation_id = argv[argv.index("--oauth-continuation") + 1]
    monkeypatch.setattr(
        oauth,
        "start_oauth_flow",
        lambda *args, **kwargs: pytest.fail("OAuth browser flow must not repeat"),
    )
    completed = app.auth_login_oauth(
        site_type="INTL",
        no_browser=True,
        project="intl_project",
        endpoint="https://private.service.example.test/api",
        region_name="custom-intl-region",
        tunnel_endpoint="https://private.tunnel.example.test",
        catalog_endpoint="https://catalog.example.test",
        no_validate=True,
        target_config_path=config_path,
        continuation_id=continuation_id,
    )

    assert completed.status == "success"
    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))["auth"]
    assert saved["provider"] == "oauth"
    assert saved["oauth"]["site_type"] == "INTL"
    assert saved["project"] == "intl_project"
    with pytest.raises(oauth.OAuthError, match="not found|already been used"):
        oauth.load_oauth_continuation(
            app.config.state_dir,
            continuation_id,
            target_config_path=config_path,
        )


def test_cli_oauth_no_browser_always_prints_sign_in_url_to_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from maxc_cli.app import MaxCApp
    from maxc_cli.cli import run
    from maxc_cli.models import Envelope

    def fake_login(self, **kwargs):
        assert kwargs["no_browser"] is True
        kwargs["on_url"]("https://signin.example/authorize")
        return Envelope(command="auth.login", status="success", data={"auth_type": "oauth"})

    monkeypatch.setattr(MaxCApp, "auth_login_oauth", fake_login)
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = run(
        ["auth", "login", "--oauth", "--no-browser"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code == 0
    assert "Sign-in URL: https://signin.example/authorize" in stderr.getvalue()
    assert "signin.example" not in stdout.getvalue()


def test_auto_oauth_pending_is_terminal_and_does_not_run_original_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from maxc_cli.app import MaxCApp
    from maxc_cli.cli import run
    from maxc_cli.models import AgentHints, Envelope, action

    calls = {"login": 0, "original": 0}

    def fake_login(self, **_kwargs):
        calls["login"] += 1
        return Envelope(
            command="auth.login",
            status="pending",
            data={"reason": "project_selection_required"},
            agent_hints=AgentHints(actions=[action("auth.login")]),
        )

    def should_not_run(self):
        calls["original"] += 1
        return Envelope(command="meta.list-projects", status="success", data={})

    class _TTY(io.StringIO):
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(MaxCApp, "auth_login_oauth", fake_login)
    monkeypatch.setattr(MaxCApp, "meta_list_projects", should_not_run)
    monkeypatch.setattr("sys.stdin", _TTY())
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    from maxc_cli.helpers import ODPS_ENV_ALIASES
    for aliases in ODPS_ENV_ALIASES.values():
        for alias in aliases:
            monkeypatch.delenv(alias, raising=False)
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = run(
        ["meta", "list-projects", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code == 0
    assert json.loads(stdout.getvalue())["status"] == "pending"
    assert calls == {"login": 1, "original": 0}
