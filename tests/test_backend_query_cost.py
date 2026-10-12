from types import SimpleNamespace

import pytest

from maxc_cli.backend.query import QueryMixin


@pytest.mark.parametrize("input_size, expected", [(None, None), (0, 0), (1024, 1024)])
def test_native_cost_preserves_missing_input_estimate(input_size, expected):
    cost = SimpleNamespace(input_size=input_size, complexity=1, udf_num=1)
    calls = []

    def execute_sql_cost(sql, **kwargs):
        calls.append((sql, kwargs))
        return cost

    backend = QueryMixin()
    backend.client = SimpleNamespace(execute_sql_cost=execute_sql_cost)

    result = backend.estimate_query_cost("SELECT foo('x');", project="test_project")

    assert result["estimated_input_size_bytes"] == expected
    assert result["sql_complexity"] == 1
    assert result["sql_udf_num"] == 1
    assert calls == [("SELECT foo('x');", {"project": "test_project", "hints": {}})]
