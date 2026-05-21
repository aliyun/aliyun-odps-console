"""Project-less Catalog API client used by auth login bootstrap.

This module exists OUTSIDE backend/ because OdpsBackend assumes a project
is configured. During `auth login` we have AK but no project yet — we
need a thin path that can construct a project-less ODPS instance and hit
the Catalog API to list everything the AK can see.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

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
