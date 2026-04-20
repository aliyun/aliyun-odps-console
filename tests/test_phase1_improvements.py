import pytest
from maxc_cli.models import SuggestedAction, AgentHints, Envelope, action

pytestmark = pytest.mark.unit


class TestSuggestedAction:
    def test_to_dict(self):
        sa = SuggestedAction(
            id="meta.describe",
            title="Describe table",
            command="maxc meta describe my_table --json",
        )
        d = sa.to_dict()
        assert d["id"] == "meta.describe"
        assert d["title"] == "Describe table"
        assert d["command"] == "maxc meta describe my_table --json"
        assert d["executable"] is True
        assert d["placeholders"] == {}

    def test_not_executable_with_placeholders(self):
        sa = SuggestedAction(
            id="meta.describe",
            title="Describe table",
            command="maxc meta describe <table_name> --json",
            executable=False,
            placeholders={"table_name": "<table_name>"},
        )
        assert sa.executable is False
        assert sa.placeholders == {"table_name": "<table_name>"}


class TestActionFactory:
    def test_action_with_table_context(self):
        sa = action(
            "meta.describe",
            data={"table_name": "my_schema.my_table"},
            metadata={},
        )
        assert sa.id == "meta.describe"
        assert sa.command == "maxc meta describe my_schema.my_table --json"
        assert sa.executable is True
        assert sa.title != ""

    def test_action_without_context_has_placeholder(self):
        sa = action("meta.describe")
        assert "<table_name>" in sa.command
        assert sa.executable is False
        assert "table_name" in sa.placeholders

    def test_action_query_cost_with_sql(self):
        sa = action(
            "query.cost",
            metadata={"sql_executed": "SELECT 1"},
        )
        assert "SELECT 1" in sa.command or "'SELECT 1'" in sa.command
        assert sa.executable is True


class TestAgentHintsWithActions:
    def test_serialization_derives_next_actions_and_action_ids(self):
        hints = AgentHints(actions=[
            SuggestedAction(
                id="meta.describe",
                title="Describe table",
                command="maxc meta describe schools --json",
            ),
            SuggestedAction(
                id="data.sample",
                title="Sample data",
                command="maxc data sample schools --json",
            ),
        ])
        d = hints.to_dict()
        assert d["action_ids"] == ["meta.describe", "data.sample"]
        assert d["next_actions"] == [
            "maxc meta describe schools --json",
            "maxc data sample schools --json",
        ]
        assert len(d["actions"]) == 2
        assert d["actions"][0]["id"] == "meta.describe"

    def test_envelope_renders_actions_through_agent_hints(self):
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
        hints = payload["agent_hints"]
        assert "actions" in hints
        assert hints["action_ids"] == ["meta.describe"]
        assert hints["next_actions"] == ["maxc meta describe <table_name> --json"]
