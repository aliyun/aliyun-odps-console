"""Tests for error self-correction (classify_sql_error + schema context)."""

import pytest

from maxc_cli.helpers import classify_sql_error
from maxc_cli.utils import sql_has_limit

pytestmark = pytest.mark.unit


class TestClassifySqlError:
    def test_column_not_found_odps(self):
        msg = "ODPS-0130071: Semantic analysis exception - column price cannot be resolved"
        result = classify_sql_error(msg)
        assert result["error_type"] == "column_not_found"
        assert result["column_name"] == "price"

    def test_column_not_found_invalid_ref(self):
        msg = "Invalid column reference 'user_name'"
        result = classify_sql_error(msg)
        assert result["error_type"] == "column_not_found"
        assert result["column_name"] == "user_name"

    def test_column_not_found_unknown_column(self):
        msg = "Unknown column 'amt' in 'field list'"
        result = classify_sql_error(msg)
        assert result["error_type"] == "column_not_found"
        assert result["column_name"] == "amt"

    def test_table_not_found(self):
        msg = "Table not found - table meta_dev.ordes cannot be resolved"
        result = classify_sql_error(msg)
        assert result["error_type"] == "table_not_found"
        assert result["table_name"] == "meta_dev.ordes"

    def test_table_does_not_exist(self):
        msg = "table test.ordes does not exist"
        result = classify_sql_error(msg)
        assert result["error_type"] == "table_not_found"
        assert result["table_name"] == "test.ordes"

    def test_generic_sql_error_semantic(self):
        msg = "ODPS-0130071: Semantic analysis exception - some other error"
        result = classify_sql_error(msg)
        assert result["error_type"] == "generic_sql_error"

    def test_generic_sql_error_parse(self):
        msg = "ODPS-0130161: Parse exception - unexpected token"
        result = classify_sql_error(msg)
        assert result["error_type"] == "generic_sql_error"

    def test_unknown_error(self):
        msg = "Some random connection error"
        result = classify_sql_error(msg)
        assert result["error_type"] == "unknown"


class TestSqlHasLimit:
    def test_has_limit(self):
        assert sql_has_limit("SELECT * FROM t LIMIT 10") is True

    def test_no_limit(self):
        assert sql_has_limit("SELECT * FROM t") is False

    def test_case_insensitive(self):
        assert sql_has_limit("select * from t limit 10") is True

    def test_limit_in_subquery(self):
        assert sql_has_limit("SELECT * FROM (SELECT * FROM t LIMIT 5) sub") is True

    def test_limit_with_comment(self):
        assert sql_has_limit("SELECT * FROM t -- no limit here") is False

    def test_limit_after_comment(self):
        assert sql_has_limit("SELECT * FROM t /* comment */ LIMIT 10") is True
