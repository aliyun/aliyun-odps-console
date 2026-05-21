import json
from unittest.mock import MagicMock, patch

import pytest

from maxc_cli.catalog_bootstrap import (
    NoProjectsError,
    ProjectInfo,
    build_bootstrap_odps,
    list_all_projects,
    list_one_page,
    pick_project,
    region_to_endpoint,
    region_to_tunnel_endpoint,
)

pytestmark = pytest.mark.unit


def test_region_to_endpoint_known_china_regions():
    assert region_to_endpoint("cn-hangzhou") == "https://service.cn-hangzhou.maxcompute.aliyun.com/api"
    assert region_to_endpoint("cn-shanghai") == "https://service.cn-shanghai.maxcompute.aliyun.com/api"
    assert region_to_endpoint("cn-beijing")  == "https://service.cn-beijing.maxcompute.aliyun.com/api"


def test_region_to_endpoint_unknown_region_returns_none():
    assert region_to_endpoint("foo-bar-1") is None
    assert region_to_endpoint("") is None
    assert region_to_endpoint(None) is None


def test_region_to_tunnel_endpoint_known():
    assert region_to_tunnel_endpoint("cn-hangzhou") == "https://dt.cn-hangzhou.maxcompute.aliyun.com"


class _FakeResp:
    def __init__(self, body: dict, status: int = 200):
        self.status_code = status
        self.text = json.dumps(body)


class _FakeRestClient:
    def __init__(self, body, endpoint="https://catalogapi.cn-shanghai.maxcompute.aliyun.com"):
        self.endpoint = endpoint
        self._body = body
        self.calls = []

    def request(self, url, method, params=None, **kw):
        self.calls.append({"url": url, "method": method, "params": params})
        return _FakeResp(self._body)


def _fake_odps_with_rest(rest):
    odps = MagicMock()
    odps.catalog_endpoint = "catalogapi.cn-shanghai.maxcompute.aliyun.com"  # no scheme!
    odps.account = MagicMock()
    odps.app_account = None
    odps.region_name = None
    odps.namespace = None
    odps._rest_client_kwargs = {}
    odps._rest_client_cls = MagicMock(return_value=rest)
    return odps


def test_list_one_page_parses_response_and_prepends_https():
    rest = _FakeRestClient({
        "projects": [
            {"projectId": "proj_a", "region": "cn-hangzhou", "owner": "ALIYUN$x",
             "schemaEnabled": True, "name": "/projects/proj_a"},
        ],
        "nextPageToken": None,
    })
    odps = _fake_odps_with_rest(rest)

    page = list_one_page(odps, page_token=None)

    assert len(page.projects) == 1
    assert page.projects[0].project_id == "proj_a"
    assert page.projects[0].region == "cn-hangzhou"
    assert page.projects[0].owner == "ALIYUN$x"
    assert page.projects[0].schema_enabled is True
    assert page.next_page_token is None
    # The URL passed to the RestClient must have a scheme.
    assert rest.calls[0]["url"].startswith("https://")
    # Guard against pyodps's private RestClient signature changing silently:
    # we MUST be passing tag="Catalog" so requests route to the catalog API.
    _, rest_kwargs = odps._rest_client_cls.call_args
    assert rest_kwargs.get("tag") == "Catalog"


def test_list_all_projects_strips_linebreaks_from_page_token_and_paginates():
    """Server returns nextPageToken containing \\r\\n; must be stripped before retry."""
    pages = [
        {"projects": [{"projectId": "p1", "region": "cn-hangzhou", "schemaEnabled": False}],
         "nextPageToken": "tok\r\nWITH\r\nLINEBREAKS\r\n"},
        {"projects": [{"projectId": "p2", "region": "cn-shanghai", "schemaEnabled": True}],
         "nextPageToken": None},
    ]
    bodies = iter(pages)

    class _MultiPageRest(_FakeRestClient):
        def request(self, url, method, params=None, **kw):
            self.calls.append({"url": url, "method": method, "params": params})
            return _FakeResp(next(bodies))

    rest = _MultiPageRest({})
    odps = _fake_odps_with_rest(rest)

    result = list_all_projects(odps)

    assert [p.project_id for p in result] == ["p1", "p2"]
    # Second call must have received a token with NO \r\n
    second_params = rest.calls[1]["params"]
    assert "\r" not in second_params["pageToken"]
    assert "\n" not in second_params["pageToken"]
    assert second_params["pageToken"] == "tokWITHLINEBREAKS"


def test_build_bootstrap_odps_passes_no_project(monkeypatch):
    captured = {}

    class _FakeODPS:
        def __init__(self, *a, **kw):
            captured.update(kw)
            self.catalog_endpoint = "catalogapi.example.com"

    monkeypatch.setattr("maxc_cli.catalog_bootstrap.ODPS", _FakeODPS)

    odps = build_bootstrap_odps(
        access_id="AK", secret_access_key="SK",
        endpoint="https://service.cn-shanghai.maxcompute.aliyun.com/api",
        security_token=None,
    )
    assert captured["access_id"] == "AK"
    assert captured["project"] is None
    assert captured["endpoint"] == "https://service.cn-shanghai.maxcompute.aliyun.com/api"


def test_pick_project_returns_selection():
    projects = [
        ProjectInfo("proj_a", "cn-hangzhou", "ALIYUN$alice", True, ""),
        ProjectInfo("proj_b", "cn-shanghai", "ALIYUN$bob",   False, ""),
    ]
    inputs = iter(["2"])
    outputs = []

    picked = pick_project(projects,
                          input_fn=lambda prompt: next(inputs),
                          output_fn=outputs.append)
    assert picked.project_id == "proj_b"
    rendered = "\n".join(outputs)
    assert "proj_a" in rendered
    assert "proj_b" in rendered
    assert "cn-hangzhou" in rendered


def test_pick_project_auto_selects_when_only_one():
    projects = [ProjectInfo("only_one", "cn-hangzhou", "x", True, "")]
    picked = pick_project(projects, input_fn=lambda _: "", output_fn=lambda _: None)
    assert picked.project_id == "only_one"


def test_pick_project_rejects_invalid_then_accepts_valid():
    projects = [
        ProjectInfo("a", "cn-hangzhou", "x", True, ""),
        ProjectInfo("b", "cn-shanghai", "y", False, ""),
    ]
    inputs = iter(["abc", "99", "1"])
    picked = pick_project(projects, input_fn=lambda _: next(inputs), output_fn=lambda _: None)
    assert picked.project_id == "a"


def test_pick_project_raises_on_empty():
    with pytest.raises(NoProjectsError):
        pick_project([], input_fn=lambda _: "", output_fn=lambda _: None)


def test_pick_project_filter_narrows_then_picks():
    """With >30 projects, typing text narrows the list; then a number selects."""
    projects = [
        ProjectInfo(f"prod_{i:03d}", "cn-hangzhou", "x", True, "") for i in range(25)
    ] + [
        ProjectInfo(f"dev_{i:03d}", "cn-shanghai", "x", False, "") for i in range(10)
    ]
    # Sequence: type "dev" to filter → list of 10 → pick "2"
    inputs = iter(["dev", "2"])
    outputs = []

    picked = pick_project(projects,
                          input_fn=lambda _: next(inputs),
                          output_fn=outputs.append,
                          page_size=30)
    assert picked.project_id == "dev_001"


def test_pick_project_empty_input_widens_back_to_full_list():
    projects = [
        ProjectInfo(f"a_{i}", "cn-hangzhou", "x", True, "") for i in range(35)
    ]
    # Filter "zzz" → 0 matches → empty input widens → pick "1" (first of all)
    inputs = iter(["zzz", "", "1"])
    picked = pick_project(projects,
                          input_fn=lambda _: next(inputs),
                          output_fn=lambda _: None,
                          page_size=30)
    assert picked.project_id == "a_0"
