"""Safety contracts for actions rendered in human-readable output."""

from __future__ import annotations

import pytest

from maxc_cli.cli import _render_human
from maxc_cli.models import AgentHints, Envelope, SuggestedAction
from maxc_cli.output import render_brief, render_markdown

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "action_overrides",
    [
        {"executable": False},
        {"agent_allowed": False},
        {"confirmation_required": True},
    ],
)
def test_gated_actions_are_not_rendered_as_next_actions(
    action_overrides: dict[str, bool],
) -> None:
    action = SuggestedAction(
        id="unsafe.action",
        title="Unsafe action",
        command="maxc unsafe-action --json",
        **action_overrides,
    )
    envelope = Envelope(
        command="meta.describe",
        status="success",
        data={"table_name": "example", "columns": []},
        agent_hints=AgentHints(actions=[action]),
    )

    markdown = render_markdown(envelope)
    brief = render_brief(envelope)

    assert "Next Actions" not in markdown
    assert action.command not in markdown
    assert "next:" not in brief
    assert action.command not in brief


def test_renderers_skip_gated_action_and_use_first_safe_action() -> None:
    gated = SuggestedAction(
        id="unsafe.action",
        title="Unsafe action",
        command="maxc unsafe-action --json",
        confirmation_required=True,
    )
    safe = SuggestedAction(
        id="meta.describe",
        title="Describe table",
        command="maxc meta describe example --json",
    )
    envelope = Envelope(
        command="meta.search",
        status="success",
        data={"keyword": "example", "matches": [], "total": 0},
        agent_hints=AgentHints(actions=[gated, safe]),
    )

    markdown = render_markdown(envelope)
    brief = render_brief(envelope)

    assert "Next Actions (fill required placeholders)" in markdown
    assert "maxc --user-agent <user_agent> meta describe example --json" in markdown
    assert gated.command not in markdown
    assert (
        "next template: maxc --user-agent <user_agent> "
        "meta describe example --json"
    ) in brief
    assert gated.command not in brief


def test_pending_output_falls_back_when_all_hint_actions_are_gated() -> None:
    gated = SuggestedAction(
        id="job.cancel",
        title="Cancel job",
        command="maxc job cancel job-42 --json",
        executable=False,
        agent_allowed=False,
        confirmation_required=True,
    )
    envelope = Envelope(
        command="query",
        status="pending",
        data={"job_id": "job-42"},
        metadata={"job_id": "job-42"},
        agent_hints=AgentHints(actions=[gated]),
    )

    markdown = render_markdown(envelope)
    brief = render_brief(envelope)

    assert gated.command not in markdown
    assert gated.command not in brief
    assert "job wait job-42 --json" in markdown
    assert "next template:" in brief
    assert "--user-agent <user_agent>" in markdown
    assert "--user-agent <user_agent>" in brief
    assert "job wait job-42 --json" in brief


def test_auth_project_selection_pending_is_not_misrendered_as_async_query() -> None:
    gated = SuggestedAction(
        id="auth.login",
        title="Complete login with selected project",
        command="maxc auth login --project <project_id> --json",
        executable=False,
        placeholders={"project_id": "<project_id>"},
        effect="local_write",
        confirmation_required=True,
        agent_allowed=False,
    )
    envelope = Envelope(
        command="auth.login",
        status="pending",
        data={
            "reason": "project_selection_required",
            "projects": [
                {"project_id": "project_a", "region": "cn-hangzhou"},
                {"project_id": "project_b", "region": "cn-shanghai"},
            ],
            "count": 2,
        },
        agent_hints=AgentHints(actions=[gated]),
    )

    markdown = render_markdown(envelope)
    brief = render_brief(envelope)

    assert "project_selection_required" in markdown
    assert "project_a" in markdown
    assert "project_b" in markdown
    assert "User Action Required" in markdown
    assert "query has not finished" not in markdown
    assert "job wait" not in markdown
    assert gated.command not in markdown
    assert brief == (
        "auth login | pending | project_selection_required | 2 options"
    )
    assert "job ?" not in brief
    assert gated.command not in brief


def test_human_renderers_use_public_distribution_entry_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    action = SuggestedAction(
        id="job.wait",
        title="Wait for job",
        command="maxc job wait job-42 --json",
    )
    envelope = Envelope(
        command="query",
        status="pending",
        data={"job_id": "job-42"},
        agent_hints=AgentHints(actions=[action]),
    )

    markdown = render_markdown(envelope)
    brief = render_brief(envelope)

    expected = "aliyun maxc --user-agent <user_agent> job wait job-42 --json"
    assert expected in markdown
    assert expected in brief
    assert "`maxc job wait" not in markdown


@pytest.mark.parametrize(
    "action_overrides",
    [
        {"executable": False},
        {"agent_allowed": False},
        {"confirmation_required": True},
    ],
)
def test_default_human_failure_does_not_render_gated_next_actions(
    action_overrides: dict[str, bool],
) -> None:
    from maxc_cli.exceptions import ErrorPayload

    action = SuggestedAction(
        id="cache.clear",
        title="Clear cache",
        command="maxc cache clear --force --json",
        **action_overrides,
    )
    envelope = Envelope(
        command="cache.clear",
        status="failure",
        error=ErrorPayload(
            code="VALIDATION_ERROR",
            message="simulated failure",
            suggestion="Inspect the request.",
            recoverable=False,
        ),
        agent_hints=AgentHints(actions=[action]),
    )

    rendered = _render_human(envelope)

    assert "Next actions" not in rendered
    assert action.command not in rendered
