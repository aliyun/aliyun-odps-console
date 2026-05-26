"""Envelope-shape stability and agent_hints serialization tests.

Pins the contract that:

1. `agent_hints` with all-empty arrays serializes as ``null`` (not as
   ``{"next_actions": [], "warnings": [], "insights": []}`` and not as ``{}``).
   Empty hints carry no information, and downstream agents shouldn't have to
   special-case both empty-list and missing-key forms.

2. `job.list` pagination reflects reality. Previously hard-coded to
   ``has_more: False`` even when the backend window was clipped, which made
   agents trust an end-of-stream signal that wasn't true.

3. Envelope shape per command stays stable: the keys at envelope top-level and
   the keys directly under ``data`` are pinned per (command, status) pair, so
   accidental shape drift (e.g. a new top-level key, a rename) is caught by
   tests rather than discovered by an agent in production.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.models import AgentHints, Envelope, SuggestedAction

# ── agent_hints empty serialization ────────────────────────────────────────


def test_agent_hints_all_empty_renders_as_null() -> None:
    """An AgentHints with no actions, warnings, insights → ``agent_hints: null``.

    Empty hints are not informative. Downstream agents should be able to dispatch
    on `payload["agent_hints"] is None` instead of also checking
    `payload["agent_hints"] != {}` and ``not payload["agent_hints"]["warnings"]``.
    """
    envelope = Envelope(
        command="meta.list-tables",
        status="success",
        data={"tables": [], "total": 0},
        metadata={"project": "demo"},
        agent_hints=AgentHints(),
    )
    payload = envelope.to_dict()
    assert payload["agent_hints"] is None


def test_agent_hints_partial_keeps_non_empty_keys() -> None:
    """If at least one of actions/warnings/insights is non-empty, the dict is kept."""
    envelope = Envelope(
        command="meta.list-tables",
        status="success",
        data={"tables": [], "total": 0},
        metadata={"project": "demo"},
        agent_hints=AgentHints(warnings=["heads up"]),
    )
    payload = envelope.to_dict()
    assert payload["agent_hints"] == {"warnings": ["heads up"]}


def test_agent_hints_none_attribute_renders_as_null() -> None:
    """`Envelope(agent_hints=None)` already rendered as null; this pins the
    contract so the empty-AgentHints path matches it."""
    envelope = Envelope(
        command="meta.list-tables",
        status="success",
        data={"tables": [], "total": 0},
        metadata={"project": "demo"},
        agent_hints=None,
    )
    payload = envelope.to_dict()
    assert payload["agent_hints"] is None


# ── job.list pagination reflects reality ───────────────────────────────────


def test_job_list_has_more_propagates_from_data() -> None:
    """`models.py:_normalize_data` for ``job.list`` must reflect ``data['has_more']``
    rather than hard-coding ``False``. Agents that paginate jobs need a truthful
    end-of-stream signal."""
    envelope = Envelope(
        command="job.list",
        status="success",
        data={"jobs": [{"job_id": "j1"}], "total": 1, "has_more": True},
        metadata={"backend": "odps"},
    )
    payload = envelope.to_dict()
    assert payload["data"]["pagination"]["has_more"] is True


def test_job_list_has_more_defaults_false_when_absent() -> None:
    """When ``data`` does not carry ``has_more`` (older callers), the legacy
    default of ``False`` is preserved so we don't break existing emit sites in
    the same PR."""
    envelope = Envelope(
        command="job.list",
        status="success",
        data={"jobs": [], "total": 0},
        metadata={"backend": "odps"},
    )
    payload = envelope.to_dict()
    assert payload["data"]["pagination"]["has_more"] is False


# ── Envelope top-level shape is stable ─────────────────────────────────────


def test_envelope_top_level_keys_are_stable() -> None:
    """Every envelope, regardless of command, exposes the same six top-level
    keys in the same order. Downstream tooling indexes into these by name, so
    accidental additions/removals are breaking changes."""
    envelope = Envelope(
        command="meta.list-tables",
        status="success",
        data={"tables": [], "total": 0},
        metadata={"project": "demo"},
        agent_hints=AgentHints(actions=[
            SuggestedAction(
                id="meta.describe",
                title="Describe table",
                command="maxc meta describe <table_name> --json",
                executable=False,
                placeholders={"table_name": "<table_name>"},
            ),
        ]),
    )
    payload = envelope.to_dict()
    assert list(payload.keys()) == [
        "version",
        "command",
        "status",
        "data",
        "metadata",
        "error",
        "agent_hints",
    ]


# ── data-block shape pinned per command (the snapshot battery) ────────────


@pytest.mark.parametrize(
    "command,data_in,expected_data_keys",
    [
        (
            "job.list",
            {"jobs": [{"job_id": "j1"}], "total": 1, "has_more": False},
            {"jobs", "pagination"},
        ),
        (
            "job.status",
            {"job_id": "j1", "status": "success"},
            {"job"},
        ),
        (
            "meta.list-tables",
            {"tables": [], "total": 0, "has_more": False},
            {"tables", "pagination"},
        ),
        (
            "meta.list-projects",
            {"projects": [{"name": "p1"}], "total": 1},
            {"projects", "pagination"},
        ),
        (
            "meta.list-schemas",
            {"schemas": [{"name": "default"}], "total": 1},
            {"schemas", "pagination"},
        ),
        (
            "meta.describe",
            {"name": "t1", "columns": []},
            {"table"},
        ),
        (
            "meta.partitions",
            {"table_name": "t1", "partitions": []},
            {"table", "partitions"},
        ),
        (
            "data.sample",
            {"table_name": "t1", "rows": [], "schema": []},
            {"sample"},
        ),
        (
            "data.profile",
            {"table_name": "t1", "stats": {}},
            {"profile"},
        ),
        (
            "auth.can-i",
            {"allowed": True, "operation": "SELECT"},
            {"authorization"},
        ),
    ],
)
def test_data_block_shape_per_command(
    command: str,
    data_in: dict,
    expected_data_keys: set[str],
) -> None:
    """Each command's normalized ``data`` block has a fixed set of top-level
    keys. The snapshot here is the contract: changing it ripples through every
    downstream consumer."""
    envelope = Envelope(command=command, status="success", data=data_in)
    payload = envelope.to_dict()
    assert set(payload["data"].keys()) == expected_data_keys, (
        f"command={command!r}: expected {expected_data_keys}, got {set(payload['data'].keys())}"
    )
