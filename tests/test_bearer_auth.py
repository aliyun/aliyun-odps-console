import concurrent.futures
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from maxc_cli.bearer_auth import BearerAuthConfig, create_bearer_account, oauth_sts
from maxc_cli.config import AuthConfig
from maxc_cli.exceptions import ValidationError


def config():
    return BearerAuthConfig(
        profile="test",
        issuer_region="cn-beijing",
        issuer_project="issuer",
        policy={
            "Version": "1",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["odps:Read"],
                    "Resource": "acs:odps:*:projects/query/instances/*",
                }
            ],
        },
    )


def credential():
    return dict(
        Version=1,
        Type="sts",
        AccessKeyId="ak",
        AccessKeySecret="private-secret",
        SecurityToken="private-sts",
        Expiration=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    )


def test_roundtrip_and_legacy():
    c = config()
    assert AuthConfig.from_mapping({"provider": "bearer", "bearer": c.to_mapping()}).bearer == c
    assert (
        "bearer"
        not in AuthConfig.from_mapping(
            {"provider": "sts_token", "security_token": "old"}
        ).to_mapping()
    )
    for update in (
        {"issuer_region": "bad"},
        {"lifetime_hours": True},
        {"policy": {}},
        {"config_path": "relative"},
        {"profile": ""},
    ):
        with pytest.raises(ValidationError):
            BearerAuthConfig.from_mapping({**c.to_mapping(), **update})


def test_process_contract_and_environment(monkeypatch):
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "must-not-inherit")
    run = Mock(return_value=SimpleNamespace(returncode=0, stdout=json.dumps(credential())))
    monkeypatch.setattr("maxc_cli.bearer_auth.subprocess.run", run)
    assert oauth_sts(config())["SecurityToken"] == "private-sts"
    kwargs = run.call_args.kwargs
    assert "ALIBABA_CLOUD_ACCESS_KEY_ID" not in kwargs["env"]
    assert "shell" not in kwargs
    assert run.call_args.args[0] == ["aliyun", "configure", "oauth-credential", "--profile", "test"]


@pytest.mark.parametrize(
    "data",
    [
        {"Version": 2},
        {"Type": "access_key"},
        {"Expiration": "bad"},
        {"Expiration": "2000-01-01T00:00:00Z"},
        {"SecurityToken": None},
    ],
)
def test_bad_credentials_are_redacted(monkeypatch, data):
    monkeypatch.setattr(
        "maxc_cli.bearer_auth.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout=json.dumps({**credential(), **data})),
    )
    with pytest.raises(ValidationError) as err:
        oauth_sts(config())
    assert "private" not in str(err.value)


def test_refresh_lock_failure_recovery_and_logging(monkeypatch, caplog):
    monkeypatch.setattr("maxc_cli.bearer_auth.oauth_sts", lambda c: credential())
    signer = Mock(return_value="private-bearer")
    client = Mock()
    client.get_project.return_value.generate_auth_token = signer
    odps = Mock(return_value=client)
    monkeypatch.setattr("odps.ODPS", odps)
    with caplog.at_level(logging.DEBUG, logger="odps.accounts"):
        account = create_bearer_account(config())
        assert odps.call_args.kwargs["region_name"] == "cn-beijing"
        account._last_refresh_time = time.time() - 4000
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: account.reload(), range(8)))
        assert signer.call_count == 2
        account._last_refresh_time = time.time() - 4000
        before = account._last_refresh_time
        signer.side_effect = RuntimeError("private-server-body")
        for _ in range(2):
            with pytest.raises(ValidationError, match="issuance failed"):
                account.reload()
            assert account._last_refresh_time == before
        signer.side_effect = None
        account.reload()
        assert signer.call_count == 5
    assert "private-bearer" not in caplog.text
    assert "private-server-body" not in caplog.text


def test_old_cli_failure_never_falls_back_or_leaks_output(monkeypatch):
    run = Mock(
        return_value=SimpleNamespace(returncode=1, stdout="private-output", stderr="private-stderr")
    )
    monkeypatch.setattr("maxc_cli.bearer_auth.subprocess.run", run)
    with pytest.raises(ValidationError, match="oauth-credential") as err:
        oauth_sts(config())
    assert "private" not in str(err.value)
    run.assert_called_once()


def test_bearer_backend_does_not_mint_logview():
    from maxc_cli.backend.job import JobMixin

    backend = JobMixin()
    backend.resolved_auth = SimpleNamespace(provider="bearer")
    instance = Mock()
    assert backend._safe_logview(instance) is None
    instance.get_logview_address.assert_not_called()


def test_rest_debug_body_and_signed_headers_are_not_logged(monkeypatch, caplog):
    from maxc_cli.bearer_auth import _SECRET_FILTER

    logger = logging.getLogger("odps.rest")
    logger.addFilter(_SECRET_FILTER)
    with caplog.at_level(logging.DEBUG, logger="odps.rest"):
        logger.debug("response.content: %s", "private-new-bearer")
        logger.debug("%s: %s", "authorization-sts-token", "private-sts")
        logger.warning("ordinary transport warning")
    assert "private-" not in caplog.text
    assert "ordinary transport warning" in caplog.text


def test_signer_forced_reload_never_uses_environment_or_mutated_config(monkeypatch):
    monkeypatch.setenv("ODPS_STS_ACCESS_KEY_ID", "unrelated-key")
    monkeypatch.setenv("ODPS_STS_ACCESS_KEY_SECRET", "unrelated-secret")
    monkeypatch.setenv("ODPS_STS_TOKEN", "unrelated-token")
    monkeypatch.setattr("maxc_cli.bearer_auth.oauth_sts", lambda c: credential())
    seen = []

    def client(**kwargs):
        snapshot = kwargs["account"]
        snapshot.reload(force=True)
        assert snapshot.sts_token == "private-sts"
        seen.append(kwargs["region_name"])
        return SimpleNamespace(
            get_project=lambda: SimpleNamespace(generate_auth_token=lambda *args: "bearer")
        )

    monkeypatch.setattr("odps.ODPS", client)
    settings = config()
    account = create_bearer_account(settings)
    settings.issuer_region = "us-east-1"
    account.reload(force=True)
    assert seen == ["cn-beijing", "cn-beijing"]


def test_retry_signing_debug_does_not_expose_existing_bearer(caplog):
    import requests
    from odps.accounts import BearerTokenAccount

    from maxc_cli.bearer_auth import _SECRET_FILTER

    logger = logging.getLogger("odps.accounts")
    logger.addFilter(_SECRET_FILTER)
    endpoint = "https://service.cn-beijing.maxcompute.aliyun.com/api"
    account = BearerTokenAccount("private-retry-token", expired_hours=None)
    request = requests.Request("GET", endpoint + "/projects/p").prepare()
    with caplog.at_level(logging.DEBUG, logger="odps.accounts"):
        account.sign_request(request, endpoint)
        # SDK retry reuses headers from the first signature.
        account.sign_request(request, endpoint)
    assert request.headers["x-odps-bearer-token"] == "private-retry-token"
    assert "private-retry-token" not in caplog.text


@pytest.mark.parametrize("field", ["endpoint", "tunnel_endpoint", "catalog_endpoint"])
@pytest.mark.parametrize(
    "endpoint",
    [
        "http://example.test/api",
        "https://user:private@example.test/api",
        "https://example.test:bad/api",
        "https://[bad",
    ],
)
def test_insecure_endpoint_rejected_before_credential_process(monkeypatch, field, endpoint):
    from maxc_cli.auth_providers import resolve_auth_connection

    settings = dict(provider="bearer", project="p", endpoint="https://example.test/api")
    settings[field] = endpoint
    monkeypatch.setattr(
        "maxc_cli.auth_providers.resolve_odps_settings", lambda *a, **k: (settings, {}, [])
    )
    factory = Mock()
    monkeypatch.setattr("maxc_cli.bearer_auth.create_bearer_account", factory)
    c = SimpleNamespace(auth=AuthConfig(provider="bearer", bearer=config()))
    with pytest.raises(ValidationError, match="HTTPS") as error:
        resolve_auth_connection(c)
    assert "private" not in str(error.value)
    factory.assert_not_called()


def test_sdk_endpoint_override_cannot_send_bearer_over_http(monkeypatch):
    import requests

    monkeypatch.setattr("maxc_cli.bearer_auth.oauth_sts", lambda c: credential())
    monkeypatch.setattr(
        "odps.ODPS",
        lambda **kw: SimpleNamespace(
            get_project=lambda: SimpleNamespace(generate_auth_token=lambda *a: "private-token")
        ),
    )
    account = create_bearer_account(config())
    request = requests.Request("GET", "http://override.test/api?action=read").prepare()
    with pytest.raises(ValidationError, match="HTTPS"):
        account.sign_request(request, "http://override.test/api")
    assert "x-odps-bearer-token" not in request.headers
    good = requests.Request("GET", "https://override.test/api?action=read").prepare()
    account.sign_request(good, "https://override.test/api")
    assert good.headers["x-odps-bearer-token"] == "private-token"
