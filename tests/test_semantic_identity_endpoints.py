from types import SimpleNamespace

import pytest

from maxc_cli.backend.auth import semantic_account_identity
from maxc_cli.backend.semantic import SemanticMixin
from maxc_cli.exceptions import ValidationError

pytestmark = pytest.mark.unit

@pytest.mark.parametrize(
    "raw,account",
    [
        ({"DisplayName": "ALIYUN$main", "ID": "123456"}, "123456"),
        ({"DisplayName": "RAM$main:user", "ID": "999"}, None),
        ({"DisplayName": "RAM$main:user", "ID": "123456:999"}, "123456"),
        ({"DisplayName": "RAM$123456:role/test", "ID": "123456:role/test"}, "123456"),
        ({"DisplayName": "ALIYUN$parent:user", "ID": "999"}, None),
        ({"DisplayName": "role", "ID": "999", "AccountId": "123456"}, "123456"),
        ({"DisplayName": "ALIYUN$main", "ID": "not-numeric"}, None),
        ({"ID": "123456"}, None),
    ],
)
def test_namespace_never_confuses_principal_or_tenant(raw, account):
    assert semantic_account_identity(raw)["semantic_namespace"] == account


def test_conflicting_identity_fails_closed():
    with pytest.raises(ValidationError, match="Conflicting"):
        semantic_account_identity({"DisplayName": "ALIYUN$main", "ID": "123", "AccountId": "456"})


@pytest.mark.parametrize(
    "endpoint,expected",
    [
        (
            "catalogapi.cn-shanghai.maxcompute.aliyun.com",
            "https://catalogapi.cn-shanghai.maxcompute.aliyun.com",
        ),
        ("https://catalog.example.test/api/", "https://catalog.example.test/api"),
        ("http://127.0.0.1:1234/api", "http://127.0.0.1:1234/api"),
    ],
)
def test_catalog_endpoint_compatibility(endpoint, expected):
    backend = SemanticMixin()
    backend.client = SimpleNamespace(catalog_rest=SimpleNamespace(endpoint=endpoint))
    assert backend.semantic_endpoint() == expected


@pytest.mark.parametrize(
    "endpoint",
    [
        "file:///tmp/a",
        "user@host.test",
        "https://u:p@host.test",
        "host.test/path",
        "https://host.test/?secret=1",
        "https://host.test/#frag",
        "https://bad host.test",
        "https://host.test\\path",
    ],
)
def test_invalid_catalog_target_rejected(endpoint):
    backend = SemanticMixin()
    backend.client = SimpleNamespace(catalog_rest=SimpleNamespace(endpoint=endpoint))
    with pytest.raises(ValidationError):
        backend.semantic_endpoint()



def test_metadata_only_describe_reads_live_schema_without_cache_or_samples(tmp_path):
    from maxc_cli.app import MaxCApp
    from maxc_cli.config import TableColumn, TableDefinition
    app = MaxCApp(cwd=tmp_path, load_backend=False)
    calls = []
    def describe(name, **scope):
        calls.append((name, scope))
        return TableDefinition(name=name, description="", columns=[TableColumn("id", "BIGINT")],
                               partition_columns=[TableColumn("ds", "STRING")])
    app.backend = SimpleNamespace(describe_table_metadata=describe)
    app.log = lambda *args: None
    envelope = app.meta_describe("orders", project="p", schema="s", metadata_only=True)
    assert envelope.data["partition_columns"] == [{"name": "ds", "type": "STRING"}]
    assert envelope.data["has_more_columns"] is False
    assert "sample_preview" not in envelope.data
    assert calls == [("orders", {"project": "p", "schema": "s"})]
    assert app._cache is None


def test_dbt_source_command_is_not_in_generic_cli():
    from maxc_cli.cli import _command_manifest, build_parser
    commands = {c["command"] for c in _command_manifest(build_parser())["commands"]}
    assert "semantic.import-dbt" not in commands
