"""Task diagnostics preserve raw metrics and use existing remote job routing."""
import json
from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from odps.errors import NoPermission
from odps.models.instance import Instance
from odps.models.worker import WorkerDetail2

from maxc_cli.app import MaxCApp
from maxc_cli.backend.job import JobMixin
from maxc_cli.cli import _manifest_effects, _manifest_requirements, build_parser, run
from maxc_cli.exceptions import FeatureUnavailableError, PermissionDeniedError, ValidationError

pytestmark = pytest.mark.unit


def make_app(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("default_project: test_project\n")
    app = MaxCApp(cwd=tmp_path, config_path=config, load_backend=False)
    app.remote_jobs = False
    app.config.state_dir = tmp_path / "state"
    return app


def backend(instance):
    obj = JobMixin()
    obj._get_instance = Mock(return_value=instance)
    return obj


def test_detail_preserves_raw_metrics_and_task_routing():
    detail = {"mapReduce": {"jsonSummary": {"AnnIndexScan": {"micros": 123}}}}
    inst = Mock()
    inst.get_task_detail2.return_value = detail
    b = backend(inst)
    result = b.inspect_job("id", section="task-detail", task_name="task", project="p", session_context={"x": 1})
    assert result["detail"] is detail
    inst.get_task_detail2.assert_called_once_with("task")
    b._get_instance.assert_called_once_with("id", project="p", session_context={"x": 1})


@pytest.mark.parametrize("detail", ["pending", {}, []])
def test_raw_detail_fallback(detail):
    inst = Mock()
    inst.get_task_detail2.return_value = detail
    assert backend(inst).inspect_job("id", section="task-detail")["detail"] == detail


def test_summary_serializes_sdk_attributes_and_absence():
    inst = Mock()
    summary = Instance.TaskSummary({"cpu": 4})
    summary.summary_text = "CPU summary"
    inst.get_task_summary.return_value = summary
    result = backend(inst).inspect_job("id", section="task-summary")
    assert result == {"task_name": None, "available": True, "summary": {"cpu": 4}, "summary_text": "CPU summary"}
    json.dumps(result)
    inst.get_task_summary.return_value = None
    assert backend(inst).inspect_job("id", section="task-summary")["available"] is False


def test_workers_include_all_jobs_and_stages_without_second_fetch():
    detail = {"mapReduce": {"jobs": [
        {"tasks": [{"name": "AnnIndexScan", "instances": [{"id": "w1", "logId": "log1"}]}]},
        {"tasks": [{"name": "Merge", "instances": [{"id": "w2", "logId": "log2"}]}]},
    ]}}
    inst = Mock()
    inst.get_task_detail2.return_value = detail
    inst.get_task_workers.side_effect = lambda task, json_obj: WorkerDetail2.extract_from_json(json_obj)
    result = backend(inst).inspect_job("id", section="workers", task_name="task")
    assert [(w["type"], w["log_id"]) for w in result["workers"]] == [("AnnIndexScan", "log1"), ("Merge", "log2")]
    inst.get_task_workers.assert_called_once_with("task", json_obj=detail)
    inst.get_task_detail2.assert_called_once_with("task")


def test_unstructured_workers_not_reported_as_empty_success():
    inst = Mock()
    inst.get_task_detail2.return_value = "not ready"
    with pytest.raises(FeatureUnavailableError):
        backend(inst).inspect_job("id", section="workers")


def test_worker_log_bounded_and_unicode_preserved():
    inst = Mock()
    inst.get_worker_log.return_value = "算子\nmetrics\x1b[31m"
    result = backend(inst).inspect_job("id", section="worker-log", log_id="log")
    inst.get_worker_log.assert_called_once_with("log", "stdout", size=1048576)
    assert result["content"] == "算子\nmetrics\x1b[31m"


@pytest.mark.parametrize("kw", [{"size": 0}, {"size": -1}, {"log_type": "bad"}, {"log_id": " "}])
def test_invalid_log_arguments_fail_before_remote_call(kw):
    b = backend(Mock())
    with pytest.raises(ValidationError):
        b.inspect_job("id", section="worker-log", **{"log_id": "log", **kw})
    b._get_instance.assert_not_called()


@pytest.mark.parametrize("section,method", [("task-detail", "get_task_detail2"), ("task-summary", "get_task_summary"), ("workers", "get_task_detail2"), ("worker-log", "get_worker_log")])
def test_permission_errors_not_swallowed(section, method):
    inst = Mock()
    getattr(inst, method).side_effect = NoPermission("Access denied")
    with pytest.raises(PermissionDeniedError):
        backend(inst).inspect_job("id", section=section, log_id="log")


@pytest.mark.parametrize("section", ["task-detail", "task-summary", "workers", "worker-log"])
def test_cli_envelope_and_saved_routing(tmp_path, section):
    app = make_app(tmp_path)
    app.remote_jobs = True
    app.backend = Mock()
    app.backend.inspect_job.return_value = {"content": "text"} if section == "worker-log" else {"detail": {}}
    app._resolve_remote_job_id = Mock(return_value=SimpleNamespace(instance_id="instance", project="saved-project", session_context={"session_subquery_id": 2}, external_job_id="external"))
    out = StringIO()
    argv = ["job", section, "external", "--json"] + (["log"] if section == "worker-log" else ["--task-name", "task"])
    with patch("maxc_cli.cli.MaxCApp", return_value=app):
        code = run(argv, cwd=tmp_path, stdout=out, stderr=StringIO())
    payload = json.loads(out.getvalue())
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == f"job {section}"
    assert payload["data"]["job_id"] == "external"
    assert payload["metadata"]["project"] == "saved-project"
    assert app.backend.inspect_job.call_args.kwargs["session_context"] == {"session_subquery_id": 2}
    assert _manifest_requirements(f"job.{section}")["network"]["mode"] == "required"
    effects = _manifest_effects(f"job.{section}")
    assert any(e.get("target") == "job_followup_context" for e in effects)


def test_local_diagnostics_fail_explicitly(tmp_path):
    with pytest.raises(FeatureUnavailableError):
        make_app(tmp_path).job_inspect("id", section="task-detail")


@pytest.mark.parametrize("argv", [["job", "worker-log", "id", "log", "--size", "0"], ["job", "worker-log", "id", "log", "--log-type", "invalid"], ["job", "workers", "id", "--task", "task"]])
def test_parser_rejects_invalid_and_abbreviated_flags(argv):
    with pytest.raises(SystemExit):
        build_parser().parse_args(argv)


def test_sqlrt_summary_does_not_return_session_aggregate():
    inst = Mock()
    inst._subquery_id = 3
    with pytest.raises(FeatureUnavailableError, match="SQLRT"):
        backend(inst).inspect_job("id", section="task-summary")
    inst.get_task_summary.assert_not_called()


def test_multi_task_error_remains_failure():
    from odps.errors import ODPSError

    from maxc_cli.exceptions import MaxCError

    inst = Mock()
    inst.get_task_detail2.side_effect = ODPSError("Multiple tasks in instance.")
    with pytest.raises(MaxCError, match="Multiple tasks"):
        backend(inst).inspect_job("id", section="task-detail")
