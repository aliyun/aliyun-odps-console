from types import SimpleNamespace

import pytest

from maxc_cli.cli import build_parser
from maxc_cli.config import AuthConfig
from maxc_cli.exceptions import ValidationError
from maxc_cli.request_routing import RoutedJobStore, resolve_request
from maxc_cli.store import JobStore
from tests.test_bearer_auth import config as bearer_config


def configuration(tmp_path, provider="bearer"):
    return SimpleNamespace(
        auth=AuthConfig(
            provider=provider,
            bearer=bearer_config(),
            project="default",
            region_name="cn-beijing",
            endpoint="https://service.cn-beijing.maxcompute.aliyun.com/api",
            tunnel_endpoint="https://old-tunnel",
        ),
        default_project="default",
        default_region="cn-beijing",
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )


def test_request_does_not_change_issuer_or_reuse_old_endpoint(tmp_path):
    c = configuration(tmp_path)
    route = resolve_request(c, region="us-east-1", project="target")
    assert c.auth.bearer.issuer_region == "cn-beijing"
    assert c.auth.endpoint == "https://service.us-east-1.maxcompute.aliyun.com/api"
    assert c.auth.tunnel_endpoint is None
    assert route["project"] == "target"
    other = configuration(tmp_path)
    resolve_request(other, region="ap-southeast-1", project="target")
    assert c.cache_dir != other.cache_dir


def test_restart_uses_submission_route_and_rotating_identity(tmp_path):
    c = configuration(tmp_path)
    route = resolve_request(c, region="us-east-1", project="target")
    RoutedJobStore(c.state_dir, route).save_remote_job_context(
        "old-job", {"project": "target", "instance_id": "old-job"}
    )
    fresh = configuration(tmp_path)
    recovered = resolve_request(fresh, job_id="old-job")
    assert recovered == route
    assert fresh.default_project == "target"
    assert fresh.auth.region_name == "us-east-1"
    for override in (
        {"region": "cn-beijing"},
        {"project": "other"},
        {"endpoint": "https://other/api"},
    ):
        with pytest.raises(ValidationError, match="conflicts"):
            resolve_request(configuration(tmp_path), job_id="old-job", **override)
    switched = configuration(tmp_path)
    switched.auth.bearer.profile = "other"
    with pytest.raises(ValidationError, match="credential source"):
        resolve_request(switched, job_id="old-job")


def test_legacy_jobs_and_auth_are_preserved(tmp_path):
    c = configuration(tmp_path)
    JobStore(c.state_dir).save_remote_job_context(
        "legacy", {"project": "target", "session_subquery_id": 2}
    )
    with pytest.raises(ValidationError, match="original --region"):
        resolve_request(c, job_id="legacy")
    route = resolve_request(c, job_id="legacy", region="us-east-1", project="target")
    assert (
        RoutedJobStore(c.state_dir, route).get_remote_job_context("legacy")["session_subquery_id"]
        == 2
    )
    for mode in ["access_key", "sts_token", "oauth", "external", "proxy"]:
        legacy = configuration(tmp_path, mode)
        assert resolve_request(legacy, job_id="legacy") is None
        assert legacy.cache_dir == tmp_path / "cache"


def test_request_flags_and_legacy_auth_flags():
    p = build_parser()
    q = p.parse_args(["query", "SELECT 1", "--region", "us-east-1", "--project", "p"])
    assert q.request_region == "us-east-1"
    a = p.parse_args(["auth", "login", "--region", "cn-beijing"])
    assert a.region_name == "cn-beijing"


def test_saved_context_cannot_be_overwritten_by_other_region_or_legacy_writer(tmp_path):
    c = configuration(tmp_path)
    route = resolve_request(c, region="us-east-1", project="target")
    RoutedJobStore(c.state_dir, route).save_remote_job_context("job", {"project": "target"})
    with pytest.raises(ValidationError):
        JobStore(c.state_dir).save_remote_job_context("job", {"project": "target"})
    other = {**route, "region": "cn-beijing"}
    with pytest.raises(ValidationError):
        RoutedJobStore(c.state_dir, other).save_remote_job_context("job", {"project": "target"})
    assert JobStore(c.state_dir).get_remote_job_context("job")["connection"] == route


def test_timeout_restart_recovers_original_job_without_resubmission(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    import yaml

    from maxc_cli.app import MaxCApp
    from maxc_cli.exceptions import JobTimeoutError
    from tests.test_query_auto_promote import _fake_job_info, _fake_query_result

    config_path = tmp_path / "maxc.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "auth": {"provider": "bearer", "bearer": bearer_config().to_mapping()},
                "state_dir": str(tmp_path / "state"),
                "cache_dir": str(tmp_path / "cache"),
            }
        )
    )
    first = MagicMock()
    first.supports_remote_jobs = True
    first.submit_query.return_value = _fake_job_info("regional-job")
    first.wait_job.side_effect = JobTimeoutError("deadline")
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **k: first)
    app = MaxCApp(
        cwd=tmp_path,
        config_path=config_path,
        region_override="us-east-1",
        project_override="test_proj",
    )
    value = app.query(command="query", sql="SELECT 1", wait=1)
    assert value.status == "pending"
    assert value.data["job_id"] == "regional-job"
    first.submit_query.assert_called_once()
    second = MagicMock()
    second.supports_remote_jobs = True
    second.get_job.return_value = _fake_job_info("regional-job", "success")
    second.fetch_job_result.return_value = _fake_query_result()
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *a, **k: second)
    resumed = MaxCApp(cwd=tmp_path, config_path=config_path, job_id_override="regional-job")
    assert resumed.config.auth.region_name == "us-east-1"
    assert resumed.config.auth.bearer.issuer_region == "cn-beijing"
    value = resumed.job_result("regional-job", max_rows=1)
    assert value.status == "success"
    assert value.data["rows"] == [{"x": 1}]
    second.submit_query.assert_not_called()


def test_executable_upgrade_does_not_invalidate_job_identity(tmp_path):
    from maxc_cli.request_routing import credential_source

    c = configuration(tmp_path)
    before = credential_source(c)
    c.auth.bearer.cli = "/new/version/aliyun"
    assert credential_source(c) == before


@pytest.mark.parametrize("provider", ["Bearer", " BEARER "])
def test_provider_case_preserves_policy_and_regional_recovery(tmp_path, provider):
    c = configuration(tmp_path, provider)
    roundtrip = AuthConfig.from_mapping(c.auth.to_mapping())
    assert roundtrip.bearer == c.auth.bearer
    route = resolve_request(c, region="us-east-1", project="target")
    assert route["credential_source"]
    RoutedJobStore(c.state_dir, route).save_remote_job_context("job", {"project": "target"})
    resumed = configuration(tmp_path, provider)
    assert resolve_request(resumed, job_id="job") == route
    assert resumed.auth.region_name == "us-east-1"


def test_explicit_request_region_wins_over_legacy_environment(tmp_path, monkeypatch):
    from maxc_cli.helpers import resolve_odps_settings
    monkeypatch.setenv("MAXCOMPUTE_REGION", "cn-beijing")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "https://old-env/api")
    monkeypatch.setenv("MAXCOMPUTE_TUNNEL_ENDPOINT", "https://old-env-tunnel")
    c = configuration(tmp_path, provider=None)
    settings, _, _ = resolve_odps_settings(c)
    assert settings["endpoint"] == "https://old-env/api"
    resolve_request(c, region="us-east-1", project="target")
    settings, _, _ = resolve_odps_settings(c)
    assert settings["region_name"] == "us-east-1"
    assert settings["endpoint"] == "https://service.us-east-1.maxcompute.aliyun.com/api"
    assert settings["tunnel_endpoint"] is None
    # A legacy invocation without new routing flags retains environment behavior.
    untouched = configuration(tmp_path, provider=None)
    resolve_request(untouched)
    assert resolve_odps_settings(untouched)[0]["endpoint"] == "https://old-env/api"
