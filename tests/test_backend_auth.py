"""Unit coverage for schema-aware permission checks."""

import json

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.backend.auth import AuthMixin
from maxc_cli.exceptions import ValidationError


class _Response:
    content = (
        b"<Auth><Result>Allow</Result>"
        b"<Message>{&quot;message&quot;:&quot;&quot;}</Message></Auth>"
    )


class _Rest:
    endpoint = "https://service.example.com/api"

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def post(self, url, *, data, params, headers):
        self.calls.append(
            {"url": url, "data": data, "params": params, "headers": headers}
        )
        return _Response()


class _StubAuth(AuthMixin):
    def __init__(self, project: str = "bird") -> None:
        self.project = project
        self.client = type("Client", (), {"rest": _Rest()})()


def test_check_permission_uses_schema_aware_post_contract() -> None:
    backend = _StubAuth()

    allowed, message = backend._check_permission(
        object_name="orders",
        object_type="Table",
        action="Select",
        project="bird",
        schema="default",
    )

    assert allowed is True
    assert message == ""
    call = backend.client.rest.calls[0]
    assert call["url"] == "https://service.example.com/api/projects/bird/auth/"
    assert call["params"] == {"curr_schema": "default"}
    assert call["headers"] == {"Content-Type": "application/json"}
    assert json.loads(call["data"]) == [
        {
            "Action": "Select",
            "Resource": "projects/bird/tables/orders",
        }
    ]


def test_resolve_schema_qualified_table() -> None:
    backend = _StubAuth()

    resolved = backend._resolve_permission_target(
        object_name="default.orders",
        object_type="Table",
        project="bird",
        schema=None,
    )

    assert resolved == ("bird", "orders", "default", "default.orders")


def test_resolve_two_tier_project_qualified_table() -> None:
    backend = _StubAuth()

    resolved = backend._resolve_permission_target(
        object_name="bird.orders",
        object_type="Table",
        project="bird",
        schema=None,
    )

    assert resolved == ("bird", "orders", None, "bird.orders")


def test_resolve_three_tier_table_and_project_object() -> None:
    backend = _StubAuth()

    assert backend._resolve_permission_target(
        object_name="bird.sales.orders",
        object_type="Table",
        project=None,
        schema=None,
    ) == ("bird", "orders", "sales", "sales.orders")
    assert backend._resolve_permission_target(
        object_name="analytics",
        object_type="Project",
        project=None,
        schema=None,
    ) == ("analytics", "analytics", None, "analytics")


def test_resolve_rejects_conflicting_qualifiers() -> None:
    backend = _StubAuth()

    with pytest.raises(ValidationError, match="Conflicting project names"):
        backend._resolve_permission_target(
            object_name="other.sales.orders",
            object_type="Table",
            project="bird",
            schema=None,
        )
    with pytest.raises(ValidationError, match="Conflicting schema names"):
        backend._resolve_permission_target(
            object_name="sales.orders",
            object_type="Table",
            project="bird",
            schema="finance",
        )


def test_can_i_info_preserves_requested_and_resolved_names() -> None:
    backend = _StubAuth()

    payload, warnings = backend.can_i_info(
        object_name="default.orders",
        object_type="Table",
        operation="Select",
        project="bird",
    )

    assert warnings == []
    assert payload == {
        "object_type": "Table",
        "object_name": "default.orders",
        "resolved_object_name": "orders",
        "qualified_name": "default.orders",
        "project": "bird",
        "schema": "default",
        "operation": "Select",
        "allowed": True,
        "check_mode": "odps_check_permission_api_v2",
        "reason": "Allowed.",
        "check_error_code": None,
    }
