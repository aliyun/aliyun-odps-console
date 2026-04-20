import pytest
from maxc_cli.models import SuggestedAction, AgentHints, Envelope, action, build_safety_block

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


class TestBuildSafetyBlock:
    def test_read_only_select(self):
        safety = build_safety_block(force=False, sql="SELECT * FROM t")
        assert safety["mode"] == "read_only"
        assert safety["force"] is False
        assert safety["policy_decision"] == "allowed"
        assert "SELECT" in safety["allowed_operations"]
        assert safety["effective_hints"] == {"odps.sql.read.only": "true"}

    def test_force_mode(self):
        safety = build_safety_block(force=True, sql="INSERT INTO t VALUES (1)")
        assert safety["mode"] == "force"
        assert safety["force"] is True
        assert safety["policy_decision"] == "allowed"

    def test_write_blocked(self):
        safety = build_safety_block(force=False, sql="INSERT INTO t VALUES (1)")
        assert safety["policy_decision"] == "blocked"
        assert safety["reason"] == "WRITE_OPERATION_REQUIRES_FORCE"

    def test_no_sql(self):
        safety = build_safety_block(force=False)
        assert safety["mode"] == "read_only"
        assert safety["policy_decision"] == "allowed"


from maxc_cli.exceptions import (
    SchemaNotFoundError,
    TableNotFoundError,
    ColumnNotFoundError,
    WriteOperationRequiresForceError,
    NotFoundError,
    MaxCError,
)


class TestNewErrorSubclasses:
    def test_schema_not_found(self):
        err = SchemaNotFoundError("Schema 'foo' not found")
        assert err.error_code == "SCHEMA_NOT_FOUND"
        assert isinstance(err, NotFoundError)
        payload = err.to_payload()
        assert payload.code == "SCHEMA_NOT_FOUND"

    def test_table_not_found(self):
        err = TableNotFoundError("Table 'bar' not found")
        assert err.error_code == "TABLE_NOT_FOUND"
        assert isinstance(err, NotFoundError)

    def test_column_not_found(self):
        err = ColumnNotFoundError("Column 'baz' not found")
        assert err.error_code == "COLUMN_NOT_FOUND"
        assert isinstance(err, NotFoundError)

    def test_write_requires_force(self):
        err = WriteOperationRequiresForceError("Write operation blocked")
        assert err.error_code == "WRITE_OPERATION_REQUIRES_FORCE"
        assert err.recoverable is True
        assert isinstance(err, MaxCError)

    def test_recovery_steps_for_new_codes(self):
        err = SchemaNotFoundError("test")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0

        err = TableNotFoundError("test")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0

        err = ColumnNotFoundError("test")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0

        err = WriteOperationRequiresForceError("test")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0


from maxc_cli.output import render_markdown, render_brief


class TestRenderMarkdown:
    def test_query_result(self):
        envelope = Envelope(
            command="query",
            status="success",
            data={
                "rows": [{"id": 1, "name": "Alice"}],
                "schema": [{"name": "id", "type": "INT"}, {"name": "name", "type": "STRING"}],
                "total_rows": 1,
                "returned_rows": 1,
                "has_more": False,
            },
            metadata={"project": "demo", "elapsed_ms": 42},
        )
        md = render_markdown(envelope)
        assert "Alice" in md
        assert "42" in md

    def test_meta_describe(self):
        envelope = Envelope(
            command="meta.describe",
            status="success",
            data={
                "table_name": "users",
                "columns": [{"name": "id", "type": "INT", "comment": "Primary key"}],
                "description": "User table",
            },
            metadata={"project": "demo"},
        )
        md = render_markdown(envelope)
        assert "users" in md
        assert "id" in md

    def test_error_envelope(self):
        from maxc_cli.exceptions import ErrorPayload
        envelope = Envelope(
            command="query",
            status="failure",
            data={},
            error=ErrorPayload(
                code="TABLE_NOT_FOUND",
                message="Table foo not found",
                suggestion="Check table name",
                recoverable=False,
            ),
        )
        md = render_markdown(envelope)
        assert "TABLE_NOT_FOUND" in md
        assert "foo" in md

    def test_job_status(self):
        envelope = Envelope(
            command="job.status",
            status="success",
            data={"job_id": "abc123", "status": "running", "progress": 50},
            metadata={},
        )
        md = render_markdown(envelope)
        assert "abc123" in md
        assert "running" in md

    def test_meta_search(self):
        envelope = Envelope(
            command="meta.search",
            status="success",
            data={
                "keyword": "school",
                "matches": [{"table_name": "schools", "description": "School data"}],
                "total": 1,
            },
            metadata={},
        )
        md = render_markdown(envelope)
        assert "schools" in md

    def test_with_agent_hints(self):
        envelope = Envelope(
            command="query",
            status="success",
            data={"rows": [], "total_rows": 0},
            metadata={},
            agent_hints=AgentHints(actions=[
                SuggestedAction(id="meta.describe", title="Describe table", command="maxc meta describe t --json"),
            ]),
        )
        md = render_markdown(envelope)
        assert "maxc meta describe" in md


class TestRenderBrief:
    def test_query_result(self):
        envelope = Envelope(
            command="query",
            status="success",
            data={"total_rows": 42, "has_more": True},
            metadata={"elapsed_ms": 100},
            agent_hints=AgentHints(actions=[
                SuggestedAction(id="query.paginate", title="Next page", command="maxc query ... --cursor x --json"),
            ]),
        )
        brief = render_brief(envelope)
        assert "42" in brief
        assert len(brief) < 300

    def test_meta_describe_brief(self):
        envelope = Envelope(
            command="meta.describe",
            status="success",
            data={
                "table_name": "users",
                "columns": [{"name": "id"}, {"name": "name"}],
            },
            metadata={},
        )
        brief = render_brief(envelope)
        assert "users" in brief
        assert "2" in brief  # column count

    def test_error_brief(self):
        from maxc_cli.exceptions import ErrorPayload
        envelope = Envelope(
            command="query",
            status="failure",
            data={},
            error=ErrorPayload(
                code="SQL_ERROR",
                message="Syntax error near SELECT",
                suggestion="Check SQL syntax",
                recoverable=False,
            ),
        )
        brief = render_brief(envelope)
        assert "SQL_ERROR" in brief

    def test_job_brief(self):
        envelope = Envelope(
            command="job.status",
            status="success",
            data={"job_id": "abc123", "status": "running"},
            metadata={},
        )
        brief = render_brief(envelope)
        assert "abc123" in brief
        assert "running" in brief
