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
