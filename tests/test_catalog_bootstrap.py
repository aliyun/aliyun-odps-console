from maxc_cli.catalog_bootstrap import region_to_endpoint, region_to_tunnel_endpoint


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


import json
from unittest.mock import MagicMock, patch

from maxc_cli.catalog_bootstrap import list_one_page, ProjectInfo


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

    page = list_one_page(odps, page_token=None, page_size=100)

    assert len(page.projects) == 1
    assert page.projects[0].project_id == "proj_a"
    assert page.projects[0].region == "cn-hangzhou"
    assert page.projects[0].owner == "ALIYUN$x"
    assert page.projects[0].schema_enabled is True
    assert page.next_page_token is None
    # The URL passed to the RestClient must have a scheme.
    assert rest.calls[0]["url"].startswith("https://")


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

    from maxc_cli.catalog_bootstrap import list_all_projects
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

    from maxc_cli.catalog_bootstrap import build_bootstrap_odps
    odps = build_bootstrap_odps(
        access_id="AK", secret_access_key="SK",
        endpoint="https://service.cn-shanghai.maxcompute.aliyun.com/api",
        security_token=None,
    )
    assert captured["access_id"] == "AK"
    assert captured["project"] is None
    assert captured["endpoint"] == "https://service.cn-shanghai.maxcompute.aliyun.com/api"
