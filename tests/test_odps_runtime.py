"""Request User-Agent observability contract."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from maxc_cli.cli import build_parser
from maxc_cli.models import action
from maxc_cli.odps_runtime import configure_user_agent, set_agent_user_agent

pytestmark = pytest.mark.unit


def test_agent_user_agent_is_appended_to_pyodps_pattern(monkeypatch) -> None:
    fake_odps = ModuleType("odps")
    fake_odps.options = SimpleNamespace(user_agent_pattern=None)
    monkeypatch.setitem(sys.modules, "odps", fake_odps)

    set_agent_user_agent(
        "AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/"
        "0123456789abcdef0123456789abcdef"
    )
    try:
        configure_user_agent()
    finally:
        set_agent_user_agent(None)

    pattern = fake_odps.options.user_agent_pattern
    assert pattern.startswith("maxc-cli/")
    assert (
        "AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/"
        "0123456789abcdef0123456789abcdef"
    ) in pattern


def test_suggested_actions_inherit_agent_user_agent() -> None:
    skill_ua = (
        "AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/"
        "0123456789abcdef0123456789abcdef"
    )
    set_agent_user_agent(skill_ua)
    try:
        suggested = action(
            "job.wait",
            data={"job_id": "job-1"},
            metadata={"project": "project-1"},
        )
    finally:
        set_agent_user_agent(None)

    assert f"--user-agent {skill_ua}" in suggested.command
    assert "job wait job-1 --project project-1 --json" in suggested.command


@pytest.mark.parametrize(
    "value",
    ["", "bad\nheader", "非 ASCII"],
)
def test_user_agent_rejects_unsafe_header_values(value: str) -> None:
    parser = build_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--user-agent", value, "agent", "context"])
    assert exc_info.value.code == 2
