"""Regression tests for the query/job effective-hints contract."""

from __future__ import annotations

import json

import pytest

from maxc_cli.app import MaxCApp
from maxc_cli.backend.query import QueryMixin
from maxc_cli.models import QueryResult
from maxc_cli.utils import effective_sql_hints_for_output

pytestmark = pytest.mark.unit


class _Instance:
    id = "instance-1"
    subquery_id = None
    _session_task_name = None
    _is_select = True
    project = type("Project", (), {"name": "proj"})()

    def wait_for_success(self, timeout=None):
        return None


class _SqlCost:
    input_size = 0
    complexity = None
    udf_num = 0


class _RecordingClient:
    def __init__(self):
        self.run_sql_calls: list[dict[str, object]] = []
        self.execute_sql_cost_calls: list[dict[str, object]] = []

    def run_sql(self, sql, **kwargs):
        self.run_sql_calls.append({"sql": sql, **kwargs})
        return _Instance()

    def execute_sql_cost(self, sql, **kwargs):
        self.execute_sql_cost_calls.append({"sql": sql, **kwargs})
        return _SqlCost()


class _Backend(QueryMixin):
    supports_remote_jobs = False
    supports_cost_check = True

    def __init__(self, client):
        self.client = client

    def _safe_logview(self, instance):
        return None

    def _instance_to_query_result(
        self,
        instance,
        *,
        project,
        max_rows,
        sql,
        elapsed_ms,
        offset=0,
    ):
        return QueryResult(
            rows=[{"value": 1}],
            schema=[{"name": "value", "type": "bigint", "comment": ""}],
            total_rows=1,
            returned_rows=1,
            has_more=False,
            next_cursor=None,
            elapsed_ms=elapsed_ms,
            bytes_scanned=0,
            project=project,
            sql_executed=sql,
            tables_used=[],
            job_id=instance.id,
        )


def _app(tmp_path, backend, *, remote_jobs=False):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: proj\n"
        f"state_dir: {tmp_path / 'state'}\n"
        f"cache_dir: {tmp_path / 'cache'}\n",
        encoding="utf-8",
    )
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)
    app.backend = backend
    app.remote_jobs = remote_jobs
    return app


def test_successful_query_reports_actual_audited_hints_and_redacts_unknown_values(
    tmp_path,
):
    secret_value = "arbitrary-value-that-must-not-be-echoed"
    sql = (
        "SET odps.sql.type.system.odps2=true; "
        f"SET future.vendor.execution.control={secret_value}; "
        "SET odps.instance.priority=3; "
        "SELECT 1"
    )
    client = _RecordingClient()
    app = _app(tmp_path, _Backend(client))

    envelope = app.query(command="query", sql=sql, project="proj")

    call = client.run_sql_calls[-1]
    assert call["sql"] == "SELECT 1"
    assert call["hints"] == {
        "odps.sql.type.system.odps2": "true",
        "future.vendor.execution.control": secret_value,
    }
    assert call["priority"] == 3
    effective = envelope.data["safety"]["effective_hints"]
    assert effective == {
        "odps.sql.type.system.odps2": "true",
        "future.vendor.execution.control": "<redacted>",
        "odps.instance.priority": "3",
    }
    payload = envelope.to_dict()
    assert payload["data"]["safety"]["effective_hints"] == effective
    assert secret_value not in json.dumps(payload["data"]["safety"])


def test_allowlisted_boolean_key_with_arbitrary_secret_like_value_is_redacted(
    tmp_path,
):
    secret_value = "sk_live_allowlisted_key_must_not_make_value_safe"
    client = _RecordingClient()
    app = _app(tmp_path, _Backend(client))

    envelope = app.query(
        command="query",
        sql=f"SET odps.sql.type.system.odps2={secret_value}; SELECT 1",
        project="proj",
    )

    assert client.run_sql_calls[-1]["hints"] == {
        "odps.sql.type.system.odps2": secret_value,
    }
    effective = envelope.to_dict()["data"]["safety"]["effective_hints"]
    assert effective == {"odps.sql.type.system.odps2": "<redacted>"}
    assert secret_value not in json.dumps(effective)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("odps.ext.hive.lazy.simple.serde.native", "true"),
        ("odps.function.strictmode", "false"),
        ("odps.mcqa.disable", "TRUE"),
        ("odps.namespace.schema", "false"),
        ("odps.optimizer.auto.mapjoin.threshold", "134217728"),
        ("odps.sql.allow.cartesian", "true"),
        ("odps.sql.allow.namespace.schema", "false"),
        ("odps.sql.decimal.odps2", "true"),
        ("odps.sql.default.zorder.type", "global"),
        ("odps.sql.executionengine.batch.rowcount", "1024"),
        ("odps.sql.hive.compatible", "true"),
        ("odps.sql.job.max.time.hours", "72"),
        ("odps.sql.mapjoin.memory.max", "8192"),
        ("odps.sql.rcte.max.iterate.num", "100"),
        ("odps.sql.submit.mode", "script"),
        ("odps.sql.timestamp.function.ntz", "true"),
        ("odps.sql.type.json.enable", "false"),
        ("odps.sql.type.system.odps2", "true"),
        ("odps.sql.udf.strict.mode", "false"),
        ("odps.sql.udf.timeout", "3600"),
        ("odps.sql.validate.orderby.limit", "true"),
    ],
)
def test_expected_audited_hint_values_remain_visible(key, value):
    assert effective_sql_hints_for_output({key: value}) == {key: value}


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("odps.sql.type.system.odps2", "yes"),
        ("odps.sql.hive.compatible", "'true'"),
        ("odps.sql.default.zorder.type", "secret-global-token"),
        ("odps.sql.submit.mode", "non_script"),
        ("odps.sql.executionengine.batch.rowcount", "1025"),
        ("odps.sql.job.max.time.hours", "73"),
        ("odps.sql.mapjoin.memory.max", "8193"),
        ("odps.sql.rcte.max.iterate.num", "101"),
        ("odps.sql.udf.timeout", "3601"),
        ("odps.optimizer.auto.mapjoin.threshold", "1e8"),
        ("odps.optimizer.auto.mapjoin.threshold", "9" * 20),
    ],
)
def test_allowlisted_hint_values_outside_expected_domains_are_redacted(key, value):
    assert effective_sql_hints_for_output({key: value}) == {
        key: "<redacted>",
    }


def test_priority_disclosure_is_limited_to_service_range():
    assert effective_sql_hints_for_output({}, priority=0) == {
        "odps.instance.priority": "0",
    }
    assert effective_sql_hints_for_output({}, priority=9) == {
        "odps.instance.priority": "9",
    }
    assert effective_sql_hints_for_output({}, priority=10) == {
        "odps.instance.priority": "<redacted>",
    }


def test_every_force_allowlisted_hint_has_an_explicit_disclosure_domain():
    from maxc_cli.utils import (
        _BOOLEAN_SQL_HINT_KEYS,
        _ENUM_SQL_HINT_VALUES,
        _FORCE_ALLOWED_SQL_SETTING_KEYS,
        _INTEGER_SQL_HINT_RANGES,
    )

    disclosure_keys = (
        set(_BOOLEAN_SQL_HINT_KEYS)
        | set(_INTEGER_SQL_HINT_RANGES)
        | set(_ENUM_SQL_HINT_VALUES)
    )
    assert disclosure_keys == set(_FORCE_ALLOWED_SQL_SETTING_KEYS)


def test_query_dry_run_reports_only_hints_actually_sent_to_cost_api(tmp_path):
    client = _RecordingClient()
    app = _app(tmp_path, _Backend(client))

    envelope = app.query(
        command="query",
        sql=(
            "SET odps.sql.type.system.odps2=true; "
            "SET odps.instance.priority=5; SELECT 1"
        ),
        project="proj",
        dry_run=True,
    )

    call = client.execute_sql_cost_calls[-1]
    assert call["hints"] == {"odps.sql.type.system.odps2": "true"}
    assert "priority" not in call
    assert envelope.data["safety"]["effective_hints"] == {
        "odps.sql.type.system.odps2": "true",
    }


def test_script_mode_auto_hint_is_visible_in_success_safety(tmp_path):
    client = _RecordingClient()
    app = _app(tmp_path, _Backend(client))

    envelope = app.query(
        command="query",
        sql="SELECT 1; SELECT 2",
        project="proj",
    )

    assert client.run_sql_calls[-1]["hints"] == {
        "odps.sql.submit.mode": "script",
    }
    assert envelope.data["safety"]["effective_hints"] == {
        "odps.sql.submit.mode": "script",
    }


def test_remote_job_submit_reports_the_hints_attached_to_submitted_job(tmp_path):
    client = _RecordingClient()
    backend = _Backend(client)
    backend.supports_remote_jobs = True
    app = _app(tmp_path, backend, remote_jobs=True)

    envelope = app.submit_job(
        sql="SET odps.sql.hive.compatible=true; SELECT 1",
        project="proj",
    )

    assert envelope.status == "pending"
    assert envelope.data["safety"]["effective_hints"] == {
        "odps.sql.hive.compatible": "true",
    }


def test_query_cost_moves_internal_hint_summary_into_safety_only(tmp_path):
    client = _RecordingClient()
    app = _app(tmp_path, _Backend(client))

    envelope = app.query_cost(
        sql="SET odps.sql.type.system.odps2=true; SELECT 1",
        project="proj",
    )

    assert "_effective_hints" not in envelope.data
    assert envelope.data["safety"]["effective_hints"] == {
        "odps.sql.type.system.odps2": "true",
    }
