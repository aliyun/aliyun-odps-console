"""Credential-free egress auth: real PyODPS requests and CLI persistence."""

import json
from io import StringIO
from unittest.mock import Mock

import pytest
import requests
import yaml

from maxc_cli.auth_providers import auth_settings_available, resolve_auth_connection
from maxc_cli.cli import _command_manifest, build_parser, run
from maxc_cli.config import load_config
from maxc_cli.exceptions import ValidationError
from maxc_cli.proxy_auth import ProxyAccount

pytestmark = pytest.mark.unit


@pytest.fixture
def config(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"auth": {"provider": "proxy", "project": "test_project",
        "endpoint": "https://odps.example/api", "access_id": "stale-id",
        "secret_access_key": "stale-secret", "security_token": "stale-token",
        "external": {"process_command": "must-not-execute"}},
        "state_dir": str(tmp_path / "state"), "cache_dir": str(tmp_path / "cache")}))
    return load_config(tmp_path, path)


def test_proxy_does_not_load_or_sign_with_ambient_credentials(config, monkeypatch):
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "ambient-id")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ambient-secret")
    monkeypatch.setenv("ODPS_BEARER_TOKEN", "ambient-bearer")
    monkeypatch.setattr("subprocess.run", Mock(side_effect=AssertionError("helper executed")))
    resolved = resolve_auth_connection(config)
    assert auth_settings_available(config)
    assert resolved.provider == "proxy"
    assert resolved.access_id is resolved.secret_access_key is resolved.security_token is None
    assert resolved.settings["access_id"] is None
    client = resolved.create_client()
    captured = []

    def send(session, request, **kwargs):
        captured.append(request)
        response = requests.Response()
        response.status_code = 200
        response._content = b"ok"
        response.request = request
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    client.rest.get("https://odps.example/api/projects/test_project")
    client.rest.post("https://odps.example/api/projects/test_project/instances", data=b"test")
    for request in captured:
        assert "Date" in request.headers
        assert not any(name.lower() in {"authorization", "x-odps-bearer-token",
            "x-odps-security-token", "x-odps-app-authentication"} for name in request.headers)
    assert len(captured) == 2


def test_proxy_removes_stale_auth_headers():
    request = requests.Request("GET", "https://odps.example/api", headers={
        "authorization": "old", "x-odps-bearer-token": "old",
        "x-odps-security-token": "old", "x-odps-app-authentication": "old",
        "User-Agent": "test-agent"}).prepare()
    ProxyAccount().sign_request(request, "https://odps.example/api")
    assert set(request.headers) == {"User-Agent", "Date"}


@pytest.mark.parametrize("field", ["project", "endpoint"])
def test_proxy_requires_routing(config, field):
    setattr(config.auth, field, None)
    with pytest.raises(ValidationError, match=field):
        resolve_auth_connection(config)


def invoke(args):
    out = StringIO()
    code = run(args, stdout=out, stderr=StringIO())
    return code, json.loads(out.getvalue())


def test_proxy_login_replaces_credentials_without_claiming_online_success(config):
    path = config.sources[-1]
    code, result = invoke(["--config", str(path), "auth", "login-proxy", "--project",
        "test_project", "--endpoint", "https://odps.example/api", "--no-validate", "--json"])
    assert code == 0
    assert result["data"]["identity"]["authenticated"] is None
    assert result["data"]["persistence"] == {"saved": True, "validated": False}
    assert yaml.safe_load(path.read_text())["auth"] == {
        "provider": "proxy", "project": "test_project", "endpoint": "https://odps.example/api"}
    code, context = invoke(["--config", str(path), "agent", "context", "--json"])
    assert code == 0
    assert context["data"]["context"]["auth_status"] == "configured"
    assert context["data"]["context"]["network_checked"] is False


def test_failed_proxy_validation_preserves_previous_config(config, monkeypatch):
    path = config.sources[-1]
    before = path.read_bytes()
    monkeypatch.setattr("maxc_cli.app.MaxCApp._validate_auth_config",
                        Mock(side_effect=ValidationError("identity rejected")))
    code, result = invoke(["--config", str(path), "auth", "login-proxy", "--project",
        "test_project", "--endpoint", "https://odps.example/api", "--json"])
    assert code != 0
    assert result["status"] == "failure"
    assert path.read_bytes() == before


def test_proxy_manifest_matches_runtime():
    commands = {c["command"]: c for c in _command_manifest(build_parser())["commands"]}
    entry = commands["auth.login-proxy"]
    assert entry["requirements"]["credentials"]["mode"] == "none"
    assert entry["requirements"]["network"]["mode"] == "conditional"


def test_proxy_login_flags_survive_aliyun_wrapper_names():
    args = build_parser().parse_args(["auth", "login-proxy", "--project", "test_project",
        "--odps-endpoint", "https://odps.example/api", "--odps-region", "cn-shanghai"])
    assert args.endpoint == "https://odps.example/api"
    assert args.region_name == "cn-shanghai"
