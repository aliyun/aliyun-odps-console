"""Capture real PyODPS tasks at the service boundary without contacting MaxCompute."""

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import pytest
from odps import ODPS, errors, options
from odps.config import option_context
from odps.models.instances import BaseInstances
from odps.models.session.v1 import PUBLIC_SESSION_NAME, McqaV1Methods, SessionInstance
from odps.models.tasks import SQLCostTask, SQLTask
from odps.rest import RestClient

from maxc_cli.backend.query import QueryMixin
from maxc_cli.exceptions import BackendConnectionError

pytestmark = pytest.mark.unit


@pytest.fixture
def captured_backend(monkeypatch):
    def reject_network(*args, **kwargs):
        raise AssertionError("Namespace tests must not contact MaxCompute")

    monkeypatch.setattr(RestClient, "_request", reject_network)
    tasks = []

    def create(collection, *, task, **kwargs):
        assert isinstance(task, (SQLTask, SQLCostTask))
        tasks.append((task.query, json.loads(task.properties["settings"])))
        return SimpleNamespace(
            id="offline-instance",
            project=SimpleNamespace(name="test_project"),
            subquery_id=None,
            _session_task_name=None,
            _is_select=True,
            wait_for_success=lambda **kwargs: None,
            get_sql_task_cost=lambda: SimpleNamespace(input_size=1, complexity=1, udf_num=0),
            get_task_results=lambda: {"AnonymousSQLTask": "offline explain plan"},
        )

    monkeypatch.setattr(BaseInstances, "create", create)
    with option_context() as options:
        options.sql.settings = {}
        options.enable_schema = False
        client = ODPS(
            "test-id", "test-key", "test_project",
            endpoint="https://example.invalid/api", overwrite_global=False,
        )
        client.default_tenant.parameters = {"odps.namespace.schema": "true"}
        client.default_tenant._loaded = True
        backend = QueryMixin()
        backend.client = client
        backend._safe_logview = lambda instance: None
        backend._instance_to_query_result = lambda *args, **kwargs: SimpleNamespace(
            extra_metadata={},
        )
        yield backend, tasks


def test_explicit_two_tier_mode_is_used_for_both_cost_and_submission(captured_backend):
    backend, tasks = captured_backend
    sql = "SET odps.namespace.schema=false; SET odps.sql.python.version=cp311; SELECT 1;"

    backend.estimate_query_cost(sql, project="test_project")
    result = backend.submit_query(sql, project="test_project", idempotency_key="owned-key")

    assert [settings["odps.namespace.schema"] for _, settings in tasks] == ["false", "false"]
    assert [settings["odps.sql.python.version"] for _, settings in tasks] == ["cp311", "cp311"]
    assert [query.strip() for query, _ in tasks] == ["SELECT 1;", "SELECT 1;"]
    assert result.sql == sql
    assert result.effective_hints["odps.namespace.schema"] == "false"


def test_absent_mode_keeps_the_tenant_default(captured_backend):
    backend, tasks = captured_backend

    backend.submit_query("SELECT 1;", project="test_project")

    assert tasks[0][1]["odps.namespace.schema"] == "true"
    assert tasks[0][1]["odps.default.schema"] == "default"


@pytest.mark.parametrize("tenant_mode", ["false", "true"])
def test_explicit_three_tier_schema_remains_selected(captured_backend, tenant_mode):
    backend, tasks = captured_backend
    backend.client.default_tenant.parameters["odps.namespace.schema"] = tenant_mode
    sql = "SET odps.namespace.schema=true; SET odps.default.schema=analytics; SELECT 1;"

    backend.estimate_query_cost(sql, project="test_project")
    backend.submit_query(sql, project="test_project")

    assert [settings["odps.namespace.schema"] for _, settings in tasks] == ["true", "true"]
    assert [settings["odps.default.schema"] for _, settings in tasks] == ["analytics", "analytics"]


@pytest.mark.parametrize("operation", ["execute", "dry_run", "explain"])
def test_two_tier_mode_applies_to_other_query_paths(captured_backend, operation):
    backend, tasks = captured_backend
    sql = "SET odps.namespace.schema=false; SELECT 1;"

    if operation == "explain":
        backend.explain_query(sql, project="test_project")
    else:
        backend.execute_query(
            sql, project="test_project", max_rows=1, dry_run=operation == "dry_run",
        )

    assert all(settings["odps.namespace.schema"] == "false" for _, settings in tasks)
    assert len(tasks) == (2 if operation == "explain" else 1)


@pytest.mark.parametrize("namespace", ["false", "true"])
@pytest.mark.parametrize("operation", ["submit", "execute"])
def test_mcqa_v2_uses_real_sdk_tasks_with_the_statement_mode(captured_backend, operation, namespace):
    backend, tasks = captured_backend
    # A service-established connection is reused; the real MCQA dispatcher and
    # run_sql still assemble the task, with the same mocked service boundary.
    backend.client._quota_to_mcqa_odps = {"test_quota": backend.client}
    execution = SimpleNamespace(enabled=True, version="v2", quota_name="test_quota", fallback=True)
    sql = f"SET odps.namespace.schema={namespace}; SET odps.default.schema=analytics; SELECT 1;"

    if operation == "submit":
        backend.submit_query(sql, project="test_project", execution_settings=execution)
    else:
        backend.execute_query(
            sql, project="test_project", max_rows=1, dry_run=False, execution_settings=execution,
        )

    assert tasks[0][1]["odps.namespace.schema"] == namespace
    assert tasks[0][1]["odps.default.schema"] == "analytics"
    assert tasks[0][0].strip() == "SELECT 1;"


@pytest.mark.parametrize("namespace", ["false", "true"])
@pytest.mark.parametrize("operation", ["submit", "execute"])
def test_mcqa_v1_preserves_the_statement_mode(captured_backend, monkeypatch, operation, namespace):
    backend, tasks = captured_backend
    client = backend.client
    project = client.get_project()
    session = SessionInstance(
        name="offline-session", parent=project.instances, client=client.rest,
        session_task_name="SQLRTTask", session_project=project, session_name=PUBLIC_SESSION_NAME,
    )
    client._default_session = session
    client._default_session_name = PUBLIC_SESSION_NAME
    monkeypatch.setattr(McqaV1Methods, "_get_mcqa_session_file", lambda *args: None)
    monkeypatch.setattr(SessionInstance, "is_running", lambda *args: True)

    def create_internal(session, *, task):
        result = BaseInstances.create(session, task=task)
        result.subquery_id = 7
        result._session_task_name = "SQLRTTask"
        result.open_reader = lambda **kwargs: True
        return result

    # Only the session service's internal-instance submission is replaced;
    # SessionInstance.run_sql assembles the actual SQLTask and settings.
    monkeypatch.setattr(SessionInstance, "_create_internal_instance", create_internal)
    execution = SimpleNamespace(enabled=True, version="v1", fallback=False)
    sql = f"SET odps.namespace.schema={namespace}; SET odps.default.schema=analytics; SELECT 1;"

    if operation == "submit":
        result = backend.submit_query(sql, project="test_project", execution_settings=execution)
        assert result.job_id == "offline-instance@7"
    else:
        backend.execute_query(
            sql, project="test_project", max_rows=1, dry_run=False, execution_settings=execution,
        )

    assert tasks[0][1]["odps.namespace.schema"] == namespace
    assert tasks[0][1]["odps.default.schema"] == "analytics"
    assert tasks[0][0].strip() == "SELECT 1;"


@pytest.fixture
def mcqa_v1_fallback(captured_backend, monkeypatch):
    backend, tasks = captured_backend
    client = backend.client
    project = client.get_project()
    client._default_session = SessionInstance(
        name="offline-session", parent=project.instances, client=client.rest,
        session_task_name="SQLRTTask", session_project=project, session_name=PUBLIC_SESSION_NAME,
    )
    client._default_session_name = PUBLIC_SESSION_NAME
    monkeypatch.setattr(McqaV1Methods, "_get_mcqa_session_file", lambda *args: None)
    monkeypatch.setattr(SessionInstance, "is_running", lambda *args: True)

    def create_internal(session, *, task):
        result = BaseInstances.create(session, task=task)
        result.subquery_id = 7
        result._session_task_name = "SQLRTTask"

        def unsupported(**kwargs):
            raise errors.SQAUnsupportedFeature("offline fixture: unsupported MCQA statement")

        result.wait_for_success = unsupported
        return result

    monkeypatch.setattr(SessionInstance, "_create_internal_instance", create_internal)
    return backend, tasks


@pytest.mark.parametrize("namespace", ["false", "true"])
def test_mcqa_v1_offline_fallback_keeps_the_same_statement_settings(mcqa_v1_fallback, namespace):
    backend, tasks = mcqa_v1_fallback
    before = {"odps.namespace.schema": "true", "odps.sql.python.version": "cp310"}
    options.sql.settings = before
    execution = SimpleNamespace(enabled=True, version="v1", fallback=True)
    sql = (
        f"SET odps.namespace.schema={namespace}; SET odps.default.schema=analytics; "
        "SET odps.sql.python.version=cp311; SELECT 1;"
    )

    backend.estimate_query_cost(sql, project="test_project")
    backend.execute_query(
        sql, project="test_project", max_rows=1, dry_run=False, execution_settings=execution,
    )

    assert len(tasks) == 3  # SQLCostTask, session SQLTask, offline fallback SQLTask.
    assert [settings["odps.default.schema"] for _, settings in tasks] == ["analytics"] * 3
    assert [settings["odps.namespace.schema"] for _, settings in tasks] == [namespace] * 3
    assert [settings["odps.sql.python.version"] for _, settings in tasks] == ["cp311"] * 3
    assert [query.strip() for query, _ in tasks] == ["SELECT 1;"] * 3
    assert options.sql.settings is before
    assert backend.client._schema is None
    assert vars(execution) == {"enabled": True, "version": "v1", "fallback": True}


def test_mcqa_v1_fallback_failure_restores_options(mcqa_v1_fallback, monkeypatch):
    backend, tasks = mcqa_v1_fallback
    before = {"odps.namespace.schema": "true", "odps.sql.python.version": "cp310"}
    options.sql.settings = before
    original_create = BaseInstances.create

    def fail_offline(collection, *, task, **kwargs):
        result = original_create(collection, task=task, **kwargs)
        if isinstance(task, SQLTask) and not isinstance(collection, SessionInstance):
            raise RuntimeError("injected offline fallback failure")
        return result

    monkeypatch.setattr(BaseInstances, "create", fail_offline)
    sql = (
        "SET odps.namespace.schema=true; SET odps.default.schema=analytics; "
        "SET odps.sql.python.version=cp311; SELECT 1;"
    )
    backend.estimate_query_cost(sql, project="test_project")
    with pytest.raises(BackendConnectionError, match="injected offline fallback failure"):
        backend.execute_query(
            sql, project="test_project", max_rows=1, dry_run=False,
            execution_settings=SimpleNamespace(enabled=True, version="v1", fallback=True),
        )

    assert [settings["odps.default.schema"] for _, settings in tasks] == ["analytics"] * 3
    assert [settings["odps.namespace.schema"] for _, settings in tasks] == ["true"] * 3
    assert options.sql.settings is before
    assert before == {"odps.namespace.schema": "true", "odps.sql.python.version": "cp310"}
    assert backend.client._schema is None


def test_mcqa_v1_fallback_settings_do_not_leak_to_a_concurrent_request(mcqa_v1_fallback, monkeypatch):
    backend, tasks = mcqa_v1_fallback
    entered, release = Event(), Event()
    original_create = BaseInstances.create
    worker_restored = []

    def create(collection, *, task, **kwargs):
        result = original_create(collection, task=task, **kwargs)
        selected_schema = json.loads(task.properties["settings"]).get("odps.default.schema")
        if isinstance(task, SQLTask) and not isinstance(collection, SessionInstance) and selected_schema == "analytics":
            entered.set()
            assert release.wait(timeout=5)
        return result

    def execute_selected_schema():
        before = options.sql.settings
        sql = (
            "SET odps.namespace.schema=true; SET odps.default.schema=analytics; "
            "SET odps.sql.python.version=cp311; SELECT 1;"
        )
        backend.estimate_query_cost(sql, project="test_project")
        backend.execute_query(
            sql, project="test_project", max_rows=1, dry_run=False,
            execution_settings=SimpleNamespace(enabled=True, version="v1", fallback=True),
        )
        worker_restored.append(options.sql.settings is before)

    monkeypatch.setattr(BaseInstances, "create", create)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(execute_selected_schema)
        try:
            assert entered.wait(timeout=5)
            backend.submit_query("SELECT 1;", project="test_project")
        finally:
            release.set()
        pending.result(timeout=5)

    assert [settings["odps.default.schema"] for _, settings in tasks] == ["analytics"] * 3 + ["default"]
    assert [settings["odps.namespace.schema"] for _, settings in tasks] == ["true"] * 4
    assert worker_restored == [True]
    assert backend.client._schema is None


@pytest.mark.parametrize("namespace", ["false", "true"])
def test_scoped_options_restore_after_success_and_next_request(captured_backend, namespace):
    backend, tasks = captured_backend
    before = {"odps.namespace.schema": "true", "odps.sql.python.version": "cp310"}
    options.sql.settings = before
    sql = (
        f"SET odps.namespace.schema={namespace}; SET odps.default.schema=analytics; "
        "SET odps.sql.python.version=cp311; SELECT 1;"
    )

    backend.submit_query(sql, project="test_project")
    assert options.sql.settings is before
    backend.submit_query("SELECT 1;", project="test_project")

    assert tasks[0][1]["odps.namespace.schema"] == namespace
    assert tasks[0][1]["odps.sql.python.version"] == "cp311"
    assert tasks[1][1]["odps.namespace.schema"] == "true"
    assert tasks[1][1]["odps.sql.python.version"] == "cp310"
    assert before == {"odps.namespace.schema": "true", "odps.sql.python.version": "cp310"}


@pytest.mark.parametrize("namespace", ["false", "true"])
def test_scoped_options_restore_after_service_failure(captured_backend, monkeypatch, namespace):
    backend, _ = captured_backend
    before = {"odps.namespace.schema": "true"}
    options.sql.settings = before

    def fail_create(*args, **kwargs):
        raise RuntimeError("injected service failure")

    monkeypatch.setattr(BaseInstances, "create", fail_create)
    with pytest.raises(BackendConnectionError, match="injected service failure"):
        backend.submit_query(
            f"SET odps.namespace.schema={namespace}; SET odps.default.schema=analytics; SELECT 1;",
            project="test_project",
        )

    assert options.sql.settings is before
    assert before == {"odps.namespace.schema": "true"}


@pytest.mark.parametrize("namespace", ["false", "true"])
def test_statement_mode_does_not_leak_to_a_concurrent_request(captured_backend, monkeypatch, namespace):
    backend, tasks = captured_backend
    entered, release = Event(), Event()
    original_create = BaseInstances.create
    worker_modes = []

    def create(collection, *, task, **kwargs):
        result = original_create(collection, task=task, **kwargs)
        if json.loads(task.properties["settings"]).get("odps.default.schema") == "analytics":
            entered.set()
            assert release.wait(timeout=5)
        return result

    def submit_selected_mode():
        before = options.sql.settings
        backend.submit_query(
            f"SET odps.namespace.schema={namespace}; SET odps.default.schema=analytics; SELECT 1;",
            project="test_project",
        )
        worker_modes.append(options.sql.settings is before)

    monkeypatch.setattr(BaseInstances, "create", create)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(submit_selected_mode)
        try:
            assert entered.wait(timeout=5)
            backend.submit_query("SELECT 1;", project="test_project")
        finally:
            release.set()
        pending.result(timeout=5)

    assert [settings["odps.namespace.schema"] for _, settings in tasks] == [namespace, "true"]
    assert [settings["odps.default.schema"] for _, settings in tasks] == ["analytics", "default"]
    assert worker_modes == [True]
