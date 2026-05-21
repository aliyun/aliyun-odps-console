"""Project-less Catalog API client used by auth login bootstrap.

This module exists OUTSIDE backend/ because OdpsBackend assumes a project
is configured. During `auth login` we have AK but no project yet — we
need a thin path that can construct a project-less ODPS instance and hit
the Catalog API to list everything the AK can see.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from odps import ODPS

# Discovery endpoint used when the user hasn't provided one. Any China
# MaxCompute service endpoint works — pyodps's GET /catalogapi auto-
# routing resolves the catalog host regardless. cn-shanghai is the
# default because it has the broadest cross-region visibility in POC.
DEFAULT_DISCOVERY_ENDPOINT = "https://service.cn-shanghai.maxcompute.aliyun.com/api"

# Known MaxCompute regions. Mapping is hard-coded (small, stable, well-known
# set). Unknown regions return None — the caller falls back to asking the
# user for an endpoint.
_KNOWN_REGIONS = (
    "cn-hangzhou", "cn-shanghai", "cn-beijing", "cn-shenzhen",
    "cn-chengdu",  "cn-zhangjiakou", "cn-wulanchabu", "cn-hongkong",
    "ap-southeast-1", "ap-southeast-3", "ap-southeast-5",
    "ap-northeast-1", "eu-central-1", "us-west-1", "us-east-1",
    "me-east-1",
)


def region_to_endpoint(region: 'str | None') -> 'str | None':
    if not region or region not in _KNOWN_REGIONS:
        return None
    return f"https://service.{region}.maxcompute.aliyun.com/api"


def region_to_tunnel_endpoint(region: 'str | None') -> 'str | None':
    if not region or region not in _KNOWN_REGIONS:
        return None
    return f"https://dt.{region}.maxcompute.aliyun.com"


@dataclass(frozen=True)
class ProjectInfo:
    project_id: str
    region: 'str | None'
    owner: 'str | None'
    schema_enabled: bool
    description: 'str | None'


@dataclass(frozen=True)
class Page:
    projects: 'list[ProjectInfo]'
    next_page_token: 'str | None'


def _ensure_scheme(endpoint: 'str | None') -> 'str | None':
    if not endpoint:
        return None
    if "://" in endpoint:
        return endpoint
    return "https://" + endpoint


def list_one_page(odps, *, page_token: 'str | None', page_size: int = 100) -> Page:
    """Single-page call to GET /api/catalog/v1alpha/projects.

    `odps` must be a pyodps ODPS instance (no project required). Auth and
    region routing reuse pyodps' RestClient machinery.

    Gotchas (verified in POC):
      - `odps.catalog_endpoint` lacks a scheme — we prepend ``https://``.
      - The server's ``nextPageToken`` contains literal ``\\r\\n`` from
        base64 line-wrapping. Caller must strip them before passing back.
    """
    base = _ensure_scheme((odps.catalog_endpoint or "").rstrip("/"))
    if not base:
        raise RuntimeError("Could not resolve catalog_endpoint from ODPS")

    rest = odps._rest_client_cls(
        odps.account, base, None, None,
        app_account=odps.app_account,
        region_name=odps.region_name,
        namespace=odps.namespace,
        tag="Catalog",
        **odps._rest_client_kwargs,
    )

    url = f"{base}/api/catalog/v1alpha/projects"
    params = {"pageSize": str(min(page_size, 100))}
    if page_token:
        params["pageToken"] = page_token

    resp = rest.request(url, "get", params=params)
    body = json.loads(resp.text if hasattr(resp, "text") else resp.content.decode("utf-8"))

    projects = []
    for p in (body.get("projects") or []):
        projects.append(ProjectInfo(
            project_id=p.get("projectId") or "",
            region=p.get("region"),
            owner=p.get("owner"),
            schema_enabled=bool(p.get("schemaEnabled")),
            description=p.get("description") or None,
        ))
    return Page(projects=projects, next_page_token=body.get("nextPageToken"))


def list_all_projects(odps, *, max_pages: int = 50) -> 'list[ProjectInfo]':
    """Paginate through all projects visible to the AK.

    `max_pages` is a safety cap so we never loop forever if the server
    returns a non-None token unexpectedly.
    """
    out: 'list[ProjectInfo]' = []
    token: 'str | None' = None
    for _ in range(max_pages):
        page = list_one_page(odps, page_token=token, page_size=100)
        out.extend(page.projects)
        token = page.next_page_token
        if token:
            token = token.replace("\r", "").replace("\n", "").strip()
        if not token:
            break
    return out


def build_bootstrap_odps(
    *,
    access_id: str,
    secret_access_key: str,
    endpoint: 'str | None' = None,
    security_token: 'str | None' = None,
):
    """Build a project-less ODPS instance for Catalog API bootstrapping."""
    return ODPS(
        access_id=access_id,
        secret_access_key=secret_access_key,
        endpoint=endpoint or DEFAULT_DISCOVERY_ENDPOINT,
        project=None,
        sts_token=security_token,
    )
