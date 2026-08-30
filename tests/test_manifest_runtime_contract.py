"""Runtime-backed checks for the Agent command manifest."""

import argparse
import csv
import json
from io import StringIO
from pathlib import Path

import pytest

from maxc_cli._samples import SAMPLES
from maxc_cli.cli import (
    _AUTO_LOGIN_EXEMPT_COMMANDS,
    _command_manifest,
    _emit_envelope,
    _handle_job_wait,
    build_parser,
    run,
)
from maxc_cli.exceptions import ErrorPayload
from maxc_cli.models import Envelope

pytestmark = pytest.mark.unit


def _commands() -> tuple[dict, dict[str, dict]]:
    manifest = _command_manifest(build_parser())
    return manifest, {command["command"]: command for command in manifest["commands"]}


def _effect(command: dict, target: str) -> dict:
    matches = [effect for effect in command["effects"] if effect["target"] == target]
    assert len(matches) == 1, (command["command"], target, matches)
    return matches[0]


def _shape(manifest: dict, command: dict, rule_id: str) -> str:
    contract_name = command["output"]["shape_contract"]
    rules: list[dict] = []
    while contract_name:
        contract = manifest["output_shape_contracts"][contract_name]
        rules.extend(contract["shape_rules"])
        contract_name = contract.get("extends")
    matches = [rule for rule in rules if rule.get("id") == rule_id]
    assert len(matches) == 1, (command["command"], rule_id, matches)
    return matches[0]["shape"]


def _condition_matches(condition: dict, args: argparse.Namespace) -> bool:
    if "all" in condition:
        return all(_condition_matches(item, args) for item in condition["all"])
    if "arg" not in condition:
        raise AssertionError(f"The test evaluator only accepts argument conditions: {condition}")
    value = getattr(args, condition["arg"])
    if "present" in condition:
        return (value is not None) is condition["present"]
    return value == condition["equals"]


def test_manifest_is_deterministic_and_has_one_sorted_entry_per_live_command():
    first = _command_manifest(build_parser())
    second = _command_manifest(build_parser())

    assert first == second
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)
    names = [command["command"] for command in first["commands"]]
    assert names == sorted(names)
    assert len(names) == len(set(names)) == first["command_count"]
    for command in first["commands"]:
        argument_names = [argument["name"] for argument in command["arguments"]]
        assert len(argument_names) == len(set(argument_names)), command["command"]
    assert first["status_values"] == ["success", "pending", "failure"]
    assert set(first["output_shapes"]) >= {
        "envelope",
        "records",
        "control_record",
        "lifecycle_event",
    }


def test_every_live_command_has_a_safe_discovery_sample():
    manifest, _commands_by_name = _commands()
    command_names = {command["command"] for command in manifest["commands"]}

    assert command_names <= set(SAMPLES)
    assert "--secret-access-key" not in SAMPLES["auth.login"]
    assert "--dry-run" in SAMPLES["data.upload"]


@pytest.mark.parametrize(
    "argv",
    [
        ["agent", "context", "--json"],
        ["agent", "manifest", "--json"],
        ["agent", "doctor", "--json"],
    ],
    ids=["context", "manifest", "offline-doctor"],
)
def test_read_only_discovery_leaves_a_fresh_home_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
):
    """Prove the no-write claim against the actual CLI, not its manifest text."""
    from maxc_cli.helpers import ODPS_ENV_ALIASES

    home = tmp_path / "home"
    work = home / "work"
    work.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("CODEX_HOME", str(home / ".codex"))
    for aliases in ODPS_ENV_ALIASES.values():
        for name in aliases:
            monkeypatch.delenv(name, raising=False)

    before = sorted(path.relative_to(home) for path in home.rglob("*"))
    stdout = StringIO()
    stderr = StringIO()
    code = run(argv, cwd=work, stdout=stdout, stderr=stderr)
    after = sorted(path.relative_to(home) for path in home.rglob("*"))

    assert code == 0, stderr.getvalue()
    assert json.loads(stdout.getvalue())["status"] == "success"
    assert after == before
    assert not (home / ".maxc").exists()


@pytest.mark.parametrize(
    "argv",
    [
        ["agent", "context", "--json"],
        ["agent", "manifest", "--json"],
        ["agent", "doctor", "--json"],
    ],
)
def test_zero_write_discovery_output_failure_does_not_create_audit_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
) -> None:
    class _BrokenOutput(StringIO):
        def write(self, _text: str) -> int:
            raise OSError("simulated output failure")

    home = tmp_path / "home"
    work = home / "work"
    work.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))

    with pytest.raises(OSError, match="simulated output failure"):
        run(
            argv,
            cwd=work,
            stdout=_BrokenOutput(),
            stderr=StringIO(),
        )

    assert not (home / ".maxc").exists()


def test_manifest_effect_conditions_match_parser_flags_and_runtime_branches():
    manifest, commands = _commands()

    query_arguments = {argument["name"]: argument for argument in commands["query"]["arguments"]}
    for retry_argument in ("retry_on", "max_retries", "retry_backoff"):
        assert query_arguments[retry_argument]["visibility"] == "deprecated_or_internal"
        assert query_arguments[retry_argument]["agent_allowed"] is False
    fallback = query_arguments["mcqa_fallback"]
    assert fallback["flags"] == ["--mcqa-fallback", "--no-mcqa-fallback"]
    assert "default" not in fallback
    assert [variant["sets"] for variant in fallback["variants"]] == [True, False]
    submit_arguments = {
        argument["name"]: argument
        for argument in commands["job.submit"]["arguments"]
    }
    submit_fallback = submit_arguments["mcqa_fallback"]
    assert submit_fallback["visibility"] == "deprecated_or_internal"
    assert submit_fallback["agent_allowed"] is False
    assert [variant["sets"] for variant in submit_fallback["variants"]] == [
        True,
        False,
    ]

    assert commands["agent.context"]["effects"] == [
        {
            "scope": "local",
            "kind": "read",
            "target": "config_and_runtime_readiness",
            "agent_allowed": True,
        }
    ]
    assert commands["agent.manifest"]["effect"] == "read_only"
    doctor_remote = _effect(commands["agent.doctor"], "maxcompute_identity")
    assert doctor_remote["when"] == {
        "all": [
            {"arg": "online", "equals": True},
            {"runtime": "complete_auth_candidate_exists"},
        ]
    }
    doctor_audit = _effect(commands["agent.doctor"], "audit_log")
    assert doctor_audit["when"] == {"arg": "online", "equals": True}
    session_audit = _effect(commands["session.show"], "audit_log")
    assert session_audit["best_effort"] is True
    assert commands["session.show"]["effect"] == "local_write"
    assert "session.show" in manifest["observability"]["audit_log"]["commands"]
    assert "agent.context" not in manifest["observability"]["audit_log"]["commands"]
    assert manifest["observability"]["audit_log"]["authoritative"] is True
    assert manifest["observability"]["audit_log"]["correlation_field"] == "invocation_id"
    assert "supersedes" in manifest["observability"]["audit_log"]["ordering"]

    upload_args = build_parser().parse_args(
        ["data", "upload", "schema.table", "--file", "rows.csv", "--create-partition"]
    )
    upload_dry_run_args = build_parser().parse_args(
        [
            "data",
            "upload",
            "schema.table",
            "--file",
            "rows.csv",
            "--create-partition",
            "--dry-run",
        ]
    )
    partition_create = _effect(commands["data.upload"], "maxcompute_partition")
    assert partition_create["kind"] == "create"
    assert partition_create["confirmation"] == "--create-partition"
    assert _condition_matches(partition_create["when"], upload_args) is True
    assert _condition_matches(partition_create["when"], upload_dry_run_args) is False
    upload_snapshot_effects = [
        effect
        for effect in commands["data.upload"]["effects"]
        if effect["target"] == "owner_private_upload_snapshot"
    ]
    assert {effect["kind"] for effect in upload_snapshot_effects} == {
        "create",
        "delete",
    }
    upload_snapshot = next(
        effect for effect in upload_snapshot_effects if effect["kind"] == "create"
    )
    assert upload_snapshot["kind"] == "create"
    assert _condition_matches(upload_snapshot["when"], upload_args) is True
    assert _condition_matches(upload_snapshot["when"], upload_dry_run_args) is False
    upload_session = _effect(
        commands["data.upload"],
        "maxcompute_tunnel_upload_session",
    )
    assert upload_session["kind"] == "create"
    assert _condition_matches(upload_session["when"], upload_args) is True
    assert _condition_matches(upload_session["when"], upload_dry_run_args) is False

    login_effects = commands["auth.login"]["effects"]
    assert {effect["target"] for effect in login_effects if effect["kind"] == "create"} == {
        "owner_only_access_key_continuation",
        "owner_only_oauth_continuation",
    }
    assert _effect(commands["auth.login"], "auth_config")["when"] == {"runtime": "login_succeeds"}
    assert _effect(commands["auth.login"], "owner_only_auth_continuation")["kind"] == "delete"

    external_process = _effect(
        commands["auth.login-external"],
        "credential_helper",
    )
    assert external_process["when"] == {"arg": "no_validate", "equals": False}

    logout_targets = {effect["target"] for effect in commands["auth.logout"]["effects"]}
    assert {
        "auth_config",
        "external_credential_cache_entries",
        "auth_continuations_for_target_and_expired_entries",
    } <= logout_targets

    submit_context = _effect(commands["job.submit"], "job_followup_context")
    assert submit_context["when"] == {"arg": "dry_run", "equals": False}
    assert submit_context["best_effort"] is True
    for name in ("job.status", "job.wait", "job.result", "job.cancel", "job.diagnose"):
        targets = {effect["target"] for effect in commands[name]["effects"]}
        assert {"job_state_lock", "job_followup_context"} <= targets

    query_cursor_read = _effect(commands["query"], "pagination_context")
    assert query_cursor_read["when"] == {"arg": "cursor", "present": True}
    query_submit = _effect(commands["query"], "maxcompute_select_job")
    assert {"arg": "cursor", "present": False} in query_submit["when"]["all"]
    result_cursor_effects = [
        effect
        for effect in commands["job.result"]["effects"]
        if effect["target"] == "pagination_context"
    ]
    assert {effect["kind"] for effect in result_cursor_effects} == {
        "read",
        "create_or_replace",
    }

    bootstrap = next(
        flow for flow in manifest["implicit_flows"] if flow["id"] == "interactive_oauth_bootstrap"
    )
    assert bootstrap["excluded_commands"] == sorted(_AUTO_LOGIN_EXEMPT_COMMANDS)
    assert {"auth.can-i", "cache.build"} <= set(bootstrap["excluded_commands"])
    implicit_ids = {flow["id"] for flow in manifest["implicit_flows"]}
    assert {
        "default_config_permission_repair",
        "external_or_ncs_credential_resolution",
        "legacy_session_migration",
        "oauth_sts_refresh",
        "stale_legacy_session_override_cleanup",
    } <= implicit_ids

    stale_cleanup = next(
        flow
        for flow in manifest["implicit_flows"]
        if flow["id"] == "stale_legacy_session_override_cleanup"
    )
    assert stale_cleanup["when"] == {
        "all": [
            {"runtime": "command_writes_default_global_config"},
            {"runtime": "legacy_session_migration_marker_exists"},
            {"runtime": "stale_legacy_session_override_exists"},
        ]
    }
    assert stale_cleanup["effects"] == [
        {
            "scope": "local",
            "kind": "delete",
            "target": "legacy_session_override",
            "agent_allowed": True,
        }
    ]


def test_manifest_declares_output_preflight_side_effects() -> None:
    _, commands = _commands()

    for command_name in ("query", "job.result"):
        command = commands[command_name]
        parent_effect = _effect(command, "output_parent_directories")
        assert parent_effect["kind"] == "create"
        assert parent_effect["when"] == {"arg": "output", "present": True}

        probe_effects = [
            effect
            for effect in command["effects"]
            if effect["target"] == "output_preflight_probe"
        ]
        assert {effect["kind"] for effect in probe_effects} == {"create", "delete"}
        assert all(
            effect["when"] == {"arg": "output", "present": True}
            for effect in probe_effects
        )


def test_manifest_query_result_file_defers_every_pending_format() -> None:
    manifest, commands = _commands()
    file_contract = manifest["output_shape_contracts"]["query_result_file"]
    pending = next(
        rule
        for rule in file_contract["shape_rules"]
        if rule.get("id") == "file_pending_deferred"
    )

    assert pending["when"] == {"status": "pending"}
    assert pending["shape"] == "no_file"
    assert pending["metadata"] == {
        "output_written": False,
        "output_deferred": True,
    }
    assert commands["query"]["output"]["rules"][1]["file_shape_contract"] == (
        "query_result_file"
    )
    assert commands["job.result"]["output"]["rules"][0][
        "file_shape_contract"
    ] == "query_result_file"


def test_manifest_record_shapes_match_actual_emitters():
    manifest, commands = _commands()

    query_success = Envelope(
        command="query",
        status="success",
        data={"rows": [{"value": 7}]},
    )
    ndjson = StringIO()
    _emit_envelope(
        query_success,
        args=argparse.Namespace(format="ndjson", json=False),
        stdout=ndjson,
        default_format="json",
    )
    assert json.loads(ndjson.getvalue()) == {"value": 7}
    assert _shape(manifest, commands["query"], "ndjson_records") == "records"

    json_output = StringIO()
    _emit_envelope(
        query_success,
        args=argparse.Namespace(format="json", json=False),
        stdout=json_output,
        default_format="table",
    )
    json_payload = json.loads(json_output.getvalue())
    assert (json_payload["version"], json_payload["status"]) == ("2.0", "success")
    assert _shape(manifest, commands["query"], "json") == "envelope"

    query_failure = Envelope(
        command="query",
        status="failure",
        error=ErrorPayload(
            code="SQL_ERROR",
            message="invalid SQL",
            suggestion="Fix the statement.",
            recoverable=True,
        ),
    )
    csv_output = StringIO()
    _emit_envelope(
        query_failure,
        args=argparse.Namespace(format="csv", json=False),
        stdout=csv_output,
        default_format="json",
    )
    rows = list(csv.DictReader(StringIO(csv_output.getvalue())))
    assert len(rows) == 1
    assert rows[0]["status"] == "failure"
    assert rows[0]["error_code"] == "SQL_ERROR"
    assert _shape(manifest, commands["query"], "csv_control") == "control_record"

    ndjson_control = StringIO()
    _emit_envelope(
        query_failure,
        args=argparse.Namespace(format="ndjson", json=False),
        stdout=ndjson_control,
        default_format="json",
    )
    control_payload = json.loads(ndjson_control.getvalue())
    assert control_payload["version"] == "2.0"
    assert control_payload["status"] == "failure"
    assert _shape(manifest, commands["query"], "ndjson_control") == "envelope"


def test_manifest_job_wait_stream_shape_matches_terminal_lifecycle_event():
    manifest, commands = _commands()

    class FakeApp:
        def job_wait(self, job_id, *, timeout, project):
            assert (job_id, timeout, project) == ("job-1", 3, "project-1")
            return (
                Envelope(
                    command="job.wait",
                    status="pending",
                    data={"job_id": job_id},
                    metadata={"job_id": job_id, "project": project},
                ),
                [{"type": "started", "job_id": job_id}],
            )

    stdout = StringIO()
    _handle_job_wait(
        FakeApp(),
        argparse.Namespace(
            job_id="job-1",
            timeout=3,
            project="project-1",
            stream=True,
        ),
        stdout,
    )
    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["type"] for event in events] == ["started", "pending"]
    assert events[-1]["status"] == "pending"
    assert events[-1]["metadata"]["project"] == "project-1"
    assert _shape(manifest, commands["job.wait"], "job_wait_stream") == "lifecycle_event"
