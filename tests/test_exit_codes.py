"""Verify exit-code propagation from exception → ErrorPayload → CLI exit."""
import pytest

from maxc_cli.exceptions import (
    CostLimitExceededError,
    MaxCError,
    NotFoundError,
    PermissionDeniedError,
    QuotaExceededError,
    SqlError,
    TableNotFoundError,
    ValidationError,
)


@pytest.mark.parametrize(
    "exc_cls, expected_exit",
    [
        (MaxCError, 1),
        (PermissionDeniedError, 2),
        (QuotaExceededError, 3),
        (SqlError, 4),
        (CostLimitExceededError, 5),
        (NotFoundError, 1),
        (ValidationError, 1),
        (TableNotFoundError, 1),
    ],
)
def test_to_payload_carries_exit_code(exc_cls, expected_exit):
    payload = exc_cls("boom").to_payload()
    assert payload.exit_code == expected_exit


def test_to_payload_exit_code_not_serialized():
    """exit_code is internal-only; envelope JSON must not leak it."""
    payload = SqlError("oops").to_payload()
    serialized = payload.to_dict()
    assert "exit_code" not in serialized


def test_max_cerror_accepts_context():
    err = TableNotFoundError("nope", context={"table": "x", "project": "p"})
    payload = err.to_payload()
    assert payload.context == {"table": "x", "project": "p"}
    assert payload.to_dict()["context"] == {"table": "x", "project": "p"}


def test_csv_parse_error_context_still_works():
    """Regression: CsvParseError uses a custom to_payload() — must keep working."""
    from maxc_cli.exceptions import CsvParseError
    err = CsvParseError("bad line", line=5, column="email")
    payload = err.to_payload()
    assert payload.context == {"line": 5, "column": "email"}
    # exit_code defaults to 1 (CsvParseError inherits from ValidationError)
    assert payload.exit_code == 1


def test_cli_envelope_failure_uses_class_exit_code():
    """The envelope-failure path used to hardcode exit 1. Now it must read
    the originating exception's exit_code."""
    import argparse
    import io

    from maxc_cli.cli import _emit_envelope
    from maxc_cli.exceptions import SqlError
    from maxc_cli.models import Envelope

    args = argparse.Namespace(json=True, format=None)
    payload = SqlError("syntax error near FOO").to_payload()
    env = Envelope(command="query", status="failure", error=payload)
    _emit_envelope(env, args=args, stdout=io.StringIO(), default_format="json")
    assert getattr(args, "_envelope_exit_code", None) == 4


def test_cli_envelope_failure_default_when_no_payload_exit_code():
    """Hand-built payloads (no exit_code override) still surface as exit 1."""
    import argparse
    import io

    from maxc_cli.cli import _emit_envelope
    from maxc_cli.exceptions import ErrorPayload
    from maxc_cli.models import Envelope

    args = argparse.Namespace(json=True, format=None)
    payload = ErrorPayload(code="X", message="x", suggestion=None, recoverable=False)
    env = Envelope(command="query", status="failure", error=payload)
    _emit_envelope(env, args=args, stdout=io.StringIO(), default_format="json")
    assert getattr(args, "_envelope_exit_code", None) == 1


def test_table_not_found_context_lands_on_error_context():
    """Promoted schema_context (previously envelope.data.schema_context) is now
    consumed via MaxCError(context=...) → ErrorPayload.context → envelope.error.context.
    """
    from maxc_cli.exceptions import TableNotFoundError

    exc = TableNotFoundError(
        "Table 'foo' does not exist",
        context={"available_tables": ["foo_v2", "foos"], "project": "demo"},
    )
    envelope_dict = {
        "error": exc.to_payload().to_dict(),
    }
    assert envelope_dict["error"]["context"] == {
        "available_tables": ["foo_v2", "foos"],
        "project": "demo",
    }


def test_typed_errors_expose_distribution_aware_recovery_steps(monkeypatch):
    from maxc_cli.exceptions import PermissionDeniedError
    from maxc_cli.odps_runtime import set_agent_user_agent

    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    user_agent = (
        "AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/"
        "0123456789abcdef0123456789abcdef"
    )
    set_agent_user_agent(user_agent)
    try:
        payload = PermissionDeniedError("denied").to_payload().to_dict()
    finally:
        set_agent_user_agent(None)

    assert payload["recovery_steps"]
    assert any(
        f"aliyun maxc --user-agent {user_agent} auth can-i" in step
        for step in payload["recovery_steps"]
    )
    assert all(
        "maxc auth" not in step.replace("aliyun maxc --user-agent", "")
        for step in payload["recovery_steps"]
    )


def test_handbuilt_error_suggestion_uses_distribution_entry_point(monkeypatch):
    from maxc_cli.exceptions import ErrorPayload

    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    payload = ErrorPayload(
        code="EXAMPLE",
        message="Run maxc only after checking the context.",
        suggestion="Run `maxc agent context --json`.",
        recoverable=True,
    ).to_dict()

    assert payload["message"] == "Run maxc only after checking the context."
    assert payload["suggestion"] == "Run `aliyun maxc agent context --json`."


def test_distribution_rewrite_does_not_mutate_sql_literals(monkeypatch):
    from maxc_cli.exceptions import ErrorPayload
    from maxc_cli.models import AgentHints

    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    diagnostic = "MaxCompute rejected SQL: SELECT 'maxc query payload' AS literal"

    error = ErrorPayload(
        code="SQL_ERROR",
        message=diagnostic,
        suggestion="Inspect with `maxc query explain <sql> --json`.",
        recoverable=False,
    ).to_dict()
    hints = AgentHints(warnings=[diagnostic], insights=[diagnostic]).to_dict()

    assert error["message"] == diagnostic
    assert "`aliyun maxc query explain" in error["suggestion"]
    assert hints["warnings"] == [diagnostic]
    assert hints["insights"] == [diagnostic]
