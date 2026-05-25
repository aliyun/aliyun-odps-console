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
