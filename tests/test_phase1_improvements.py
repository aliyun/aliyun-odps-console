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

    def test_action_meta_semantic_get(self):
        sa = action("meta.semantic.get", data={"table_name": "my_schema.users"})
        assert "meta semantic get" in sa.command
        assert "my_schema.users" in sa.command
        assert sa.executable is True

    def test_action_meta_semantic_set(self):
        sa = action("meta.semantic.set", data={"table_name": "users"})
        assert "meta semantic set" in sa.command
        assert "users" in sa.command
        assert sa.executable is True

    def test_action_meta_semantic_list_missing(self):
        sa = action("meta.semantic.list-missing")
        assert "meta semantic list-missing" in sa.command
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
        assert safety["effective_hints"] == {}

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


from maxc_cli.cli import build_parser


class TestFormatFlag:
    def test_global_format_json(self):
        parser = build_parser()
        args = parser.parse_args(["--format", "json", "agent", "context"])
        assert args.format == "json"

    def test_global_format_markdown(self):
        parser = build_parser()
        args = parser.parse_args(["--format", "markdown", "agent", "context"])
        assert args.format == "markdown"

    def test_global_format_brief(self):
        parser = build_parser()
        args = parser.parse_args(["--format", "brief", "agent", "context"])
        assert args.format == "brief"

    def test_json_flag_still_works(self):
        parser = build_parser()
        args = parser.parse_args(["agent", "context", "--json"])
        assert args.json is True

    def test_format_table(self):
        parser = build_parser()
        args = parser.parse_args(["--format", "table", "meta", "list-tables"])
        assert args.format == "table"

    def test_format_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args(["agent", "context", "--json"])
        assert args.format is None  # default when not specified


from maxc_cli.helpers import classify_sql_error


class TestEnhancedClassifySqlError:
    def test_schema_not_found(self):
        msg = "Schema 'nonexistent' does not exist"
        result = classify_sql_error(msg)
        assert result["error_type"] == "schema_not_found"

    def test_schema_not_found_namespace(self):
        msg = "ODPS-0420111: Authorization exception - namespace not found: bad_schema"
        result = classify_sql_error(msg)
        assert result["error_type"] == "schema_not_found"
        assert result.get("schema_name") == "bad_schema"

    def test_schema_not_found_no_such_schema(self):
        msg = "No such schema: test_schema"
        result = classify_sql_error(msg)
        assert result["error_type"] == "schema_not_found"

    def test_existing_column_not_found_still_works(self):
        msg = "ODPS-0130071: Semantic analysis exception - column price cannot be resolved"
        result = classify_sql_error(msg)
        assert result["error_type"] == "column_not_found"
        assert result["column_name"] == "price"

    def test_existing_table_not_found_still_works(self):
        msg = "Table not found - table meta_dev.ordes cannot be resolved"
        result = classify_sql_error(msg)
        assert result["error_type"] == "table_not_found"


class TestConsistency:
    """Verify action_ids, actions[].id, command_id, and next_actions are consistent."""

    def test_action_ids_match_actions(self):
        hints = AgentHints(actions=[
            action("meta.describe", data={"table_name": "t"}),
            action("data.sample", data={"table_name": "t"}),
        ])
        d = hints.to_dict()
        assert d["action_ids"] == [a["id"] for a in d["actions"]]

    def test_next_actions_match_commands(self):
        hints = AgentHints(actions=[
            action("meta.describe", data={"table_name": "t"}),
        ])
        d = hints.to_dict()
        assert d["next_actions"] == [a["command"] for a in d["actions"]]

    def test_command_id_uses_dot_notation(self):
        envelope = Envelope(
            command="meta.describe",
            status="success",
            data={"table_name": "t"},
            metadata={},
        )
        payload = envelope.to_dict()
        assert payload["command_id"] == "meta.describe"
        assert payload["command"] == "meta describe"

    def test_command_id_consistent_with_action_vocabulary(self):
        """command_id and action IDs should use same vocabulary."""
        envelope = Envelope(
            command="query.cost",
            status="success",
            data={},
            metadata={"project": "p"},
            agent_hints=AgentHints(actions=[
                action("query.explain"),
                action("query"),
            ]),
        )
        payload = envelope.to_dict()
        # command_id is dot notation
        assert payload["command_id"] == "query.cost"
        # action_ids are also dot notation
        for aid in payload["agent_hints"]["action_ids"]:
            assert "." in aid or aid in {"query"}  # single-word commands are ok

    def test_empty_actions_produces_no_hints_keys(self):
        hints = AgentHints(actions=[], warnings=["test"])
        d = hints.to_dict()
        assert "actions" not in d
        assert "action_ids" not in d
        assert "next_actions" not in d
        assert d["warnings"] == ["test"]


class TestSafetyBlockIntegration:
    def test_query_envelope_preserves_safety(self):
        """Safety block should survive _normalize_data for query commands."""
        envelope = Envelope(
            command="query",
            status="success",
            data={
                "rows": [],
                "schema": [],
                "total_rows": 0,
                "returned_rows": 0,
                "has_more": False,
                "safety": build_safety_block(force=False, sql="SELECT 1"),
            },
            metadata={},
        )
        payload = envelope.to_dict()
        assert "safety" in payload["data"]
        assert payload["data"]["safety"]["policy_decision"] == "allowed"
        assert payload["data"]["safety"]["mode"] == "read_only"

    def test_query_cost_envelope_preserves_safety(self):
        """Safety block should survive _normalize_data for query.cost."""
        envelope = Envelope(
            command="query.cost",
            status="success",
            data={
                "estimated_input_size_bytes": 100,
                "safety": build_safety_block(force=False, sql="SELECT 1"),
            },
            metadata={},
        )
        payload = envelope.to_dict()
        assert "safety" in payload["data"]
        assert payload["data"]["safety"]["policy_decision"] == "allowed"

    def test_safety_blocked_write(self):
        """Write operation without --force should be blocked."""
        safety = build_safety_block(force=False, sql="INSERT INTO t VALUES (1)")
        assert safety["policy_decision"] == "blocked"
        assert safety["reason"] == "WRITE_OPERATION_REQUIRES_FORCE"

    def test_safety_allowed_with_force(self):
        """Write operation with --force should be allowed."""
        safety = build_safety_block(force=True, sql="INSERT INTO t VALUES (1)")
        assert safety["policy_decision"] == "allowed"
        assert safety["mode"] == "force"


class TestFormatRouting:
    """Test that _emit_envelope routes correctly for different formats."""

    def test_json_format_output(self):
        from io import StringIO
        import json
        from maxc_cli.cli import run

        stdout = StringIO()
        # agent context doesn't need backend
        exit_code = run(
            ["--format", "json", "agent", "context"],
            stdout=stdout,
        )
        output = stdout.getvalue()
        # Should be valid JSON
        parsed = json.loads(output)
        assert parsed["command"] == "agent context"
        assert parsed["status"] == "success"

    def test_markdown_format_output(self):
        from io import StringIO
        from maxc_cli.cli import run

        stdout = StringIO()
        exit_code = run(
            ["--format", "markdown", "agent", "context"],
            stdout=stdout,
        )
        output = stdout.getvalue()
        assert len(output) > 0
        # Should NOT be JSON
        assert not output.strip().startswith("{")

    def test_brief_format_output(self):
        from io import StringIO
        from maxc_cli.cli import run

        stdout = StringIO()
        exit_code = run(
            ["--format", "brief", "agent", "context"],
            stdout=stdout,
        )
        output = stdout.getvalue()
        assert len(output) > 0
        assert len(output) < 500  # brief should be short
