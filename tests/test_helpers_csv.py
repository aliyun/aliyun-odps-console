"""Tests for CSV value helpers in helpers.py."""

import pytest


@pytest.mark.parametrize("odps_type", [
    "bigint", "int", "smallint", "tinyint",
    "double", "float", "decimal", "decimal(10,2)",
    "boolean", "string", "varchar(255)", "char(10)",
    "date", "datetime", "timestamp",
    "BIGINT", "STRING",
])
def test_csv_supported_type_returns_true_for_primitives(odps_type: str):
    from maxc_cli.helpers import csv_supported_type
    assert csv_supported_type(odps_type) is True


@pytest.mark.parametrize("odps_type", [
    "array<bigint>",
    "map<string,bigint>",
    "struct<a:bigint,b:string>",
    "ARRAY<STRING>",
])
def test_csv_supported_type_returns_false_for_complex_types(odps_type: str):
    from maxc_cli.helpers import csv_supported_type
    assert csv_supported_type(odps_type) is False


from datetime import date, datetime
from decimal import Decimal


@pytest.mark.parametrize("text,odps_type,expected", [
    ("123", "bigint", 123),
    ("-7", "int", -7),
    ("3.14", "double", 3.14),
    ("1.5", "decimal(10,2)", Decimal("1.5")),
    ("true", "boolean", True),
    ("False", "boolean", False),
    ("hello", "string", "hello"),
    ("", "string", ""),
    ("2026-05-08", "date", date(2026, 5, 8)),
    ("2026-05-08 12:34:56", "datetime", datetime(2026, 5, 8, 12, 34, 56)),
])
def test_csv_parse_value_happy_path(text, odps_type, expected):
    from maxc_cli.helpers import csv_parse_value
    assert csv_parse_value(text, odps_type, null_marker=r"\N") == expected


def test_csv_parse_value_null_marker_returns_none():
    from maxc_cli.helpers import csv_parse_value
    assert csv_parse_value(r"\N", "bigint", null_marker=r"\N") is None
    assert csv_parse_value(r"\N", "string", null_marker=r"\N") is None


def test_csv_parse_value_empty_null_marker_treats_empty_as_null_for_non_string():
    from maxc_cli.helpers import csv_parse_value
    assert csv_parse_value("", "bigint", null_marker="") is None
    assert csv_parse_value("", "string", null_marker="") == ""


def test_csv_parse_value_raises_csv_parse_error_on_bad_int():
    from maxc_cli.helpers import csv_parse_value
    from maxc_cli.exceptions import CsvParseError
    with pytest.raises(CsvParseError) as exc:
        csv_parse_value("abc", "bigint", null_marker=r"\N")
    assert "bigint" in str(exc.value)
