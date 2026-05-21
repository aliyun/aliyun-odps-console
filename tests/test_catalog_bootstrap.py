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
