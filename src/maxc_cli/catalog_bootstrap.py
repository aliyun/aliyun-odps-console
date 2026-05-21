"""Project-less Catalog API client used by auth login bootstrap.

This module exists OUTSIDE backend/ because OdpsBackend assumes a project
is configured. During `auth login` we have AK but no project yet — we
need a thin path that can construct a project-less ODPS instance and hit
the Catalog API to list everything the AK can see.
"""
from __future__ import annotations

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
