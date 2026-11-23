from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from time import time

import pytest

from maxc_cli import telemetry_runtime
from maxc_cli.telemetry import HOST_DISABLE_ENV, RETENTION_DAYS, decode_batch, report_day


@pytest.fixture
def usage_state(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.delenv(HOST_DISABLE_ENV, raising=False)
    return tmp_path / ".maxc" / "state" / "telemetry"


def _invoke(home: Path, entry: str, *args: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.pop(HOST_DISABLE_ENV, None)
    env.update(
        HOME=str(home),
        USERPROFILE=str(home),
        MAXC_CLI_NAME=entry,
        PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
    )
    return subprocess.run(
        [sys.executable, "-m", "maxc_cli", *args, "--json"],
        cwd=home, env=env, capture_output=True, text=True, timeout=8,
    )


def _make_due(state: Path) -> None:
    old_time = time() - telemetry_runtime.DEFAULT_UPLOAD_INTERVAL - 1
    os.utime(state / "upload.lock", (old_time, old_time))


def test_normal_entry_points_append_and_keep_output_with_broken_state(tmp_path):
    state = tmp_path / ".maxc" / "state" / "telemetry"
    standalone = _invoke(tmp_path, "maxc", "agent", "context")
    host = _invoke(tmp_path, "aliyun maxc", "agent", "context")
    for run in (standalone, host):
        assert run.returncode == 0
        assert json.loads(run.stdout)["command"] == "agent context"
        assert not run.stderr

    for scope in ("standalone", "aliyun"):
        log = state / f"{scope}.{report_day().isoformat()}.usage"
        assert log.read_bytes() == b"agent.context\n"
        if os.name == "posix":
            assert log.stat().st_mode & 0o777 == 0o600
    assert not (state / "receiver").exists()
    assert not (state / "usage.sqlite3").exists()

    state.rename(state.with_name("saved-telemetry"))
    state.write_bytes(b"unsafe state path")
    reopened = _invoke(tmp_path, "maxc", "agent", "context")
    assert reopened.returncode == 0
    assert json.loads(reopened.stdout)["status"] == "success"
    assert reopened.stderr == standalone.stderr


def test_upload_attempts_are_rate_limited_even_when_launch_fails(usage_state, monkeypatch):
    launches = []

    def fail_to_start():
        launches.append(True)
        raise OSError("process limit")

    monkeypatch.setattr(telemetry_runtime, "_launch_upload", fail_to_start)
    telemetry_runtime.emit_automatic_invocation("standalone", "query")
    assert not launches
    assert (usage_state / "upload.lock").stat().st_size == 1

    _make_due(usage_state)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(
            telemetry_runtime.emit_automatic_invocation,
            ["standalone"] * 24,
            ["query"] * 24,
        ))
    assert launches == [True]
    log = next(usage_state.glob("*.usage"))
    if os.name == "posix":
        assert log.read_bytes().splitlines() == [b"query"] * 25

    _make_due(usage_state)
    telemetry_runtime.emit_automatic_invocation("standalone", "query")
    assert launches == [True, True]


def test_failed_batches_keep_identity_and_later_calls_remain_separate(usage_state, monkeypatch):
    today = report_day()
    earlier = today - timedelta(days=1)
    expired = today - timedelta(days=RETENTION_DAYS + 1)
    sent = []
    outcome = "retry"

    def send(scope, payload, _timeout):
        sent.append((scope, payload))
        return outcome

    monkeypatch.setattr(telemetry_runtime, "_send_batch", send)
    monkeypatch.setattr(telemetry_runtime, "_launch_upload", lambda: pytest.fail("Upload is not due"))
    monkeypatch.setattr(telemetry_runtime, "report_day", lambda: expired)
    telemetry_runtime.emit_automatic_invocation("standalone", "query")
    monkeypatch.setattr(telemetry_runtime, "report_day", lambda: earlier)
    telemetry_runtime.emit_automatic_invocation("standalone", "query")
    telemetry_runtime.emit_automatic_invocation("standalone", "query")
    telemetry_runtime.emit_automatic_invocation("standalone", "agent.context")
    monkeypatch.setattr(telemetry_runtime, "report_day", lambda: today)
    telemetry_runtime.upload_usage(frozenset({"query", "agent.context"}))
    scope, original = sent[0]
    assert scope == "standalone"
    assert decode_batch(original)[1:] == (earlier.isoformat(), [("agent.context", 1), ("query", 2)])
    assert not list(usage_state.glob(f"*.{expired.isoformat()}.usage"))

    telemetry_runtime.emit_automatic_invocation("standalone", "query")
    outcome = "accepted"
    telemetry_runtime.upload_usage(frozenset({"query", "agent.context"}))
    assert sent[0] == sent[1]
    assert decode_batch(sent[2][1])[1:] == (today.isoformat(), [("query", 1)])
    assert not list(usage_state.glob("*.usage"))


def test_switches_discard_unsent_files_by_scope(usage_state, monkeypatch):
    profile_home = usage_state.parents[2]
    host = profile_home / ".aliyun" / "telemetry.json"
    config = profile_home / ".maxc" / "config.yaml"
    telemetry_runtime.emit_automatic_invocation("standalone", "query")
    telemetry_runtime.emit_automatic_invocation("aliyun", "query")
    monkeypatch.setenv(HOST_DISABLE_ENV, "true")
    telemetry_runtime.emit_automatic_invocation("aliyun", "query")
    assert next(usage_state.glob("aliyun.*.usage")).read_text().splitlines() == ["query"]
    monkeypatch.delenv(HOST_DISABLE_ENV)

    monkeypatch.setattr(telemetry_runtime, "_send_batch", lambda *_args: "retry")
    telemetry_runtime.upload_usage(frozenset({"query"}))
    assert len(list(usage_state.glob("aliyun.*.usage"))) == 1
    assert len(list(usage_state.glob("standalone.*.usage"))) == 1

    host.parent.mkdir()
    host.write_text('{"enabled":false}', encoding="utf-8")
    telemetry_runtime.emit_automatic_invocation("aliyun", "query")
    assert not list(usage_state.glob("aliyun.*.usage"))
    assert list(usage_state.glob("standalone.*.usage"))
    _make_due(usage_state)
    config.write_text("telemetry: [", encoding="utf-8")
    assert telemetry_runtime.read_switches() == {"standalone": None, "aliyun": False}
    telemetry_runtime.upload_usage(frozenset({"query"}))
    assert list(usage_state.glob("standalone.*.usage"))
    config.write_text("telemetry:\n  enabled: false\n", encoding="utf-8")
    monkeypatch.setattr(telemetry_runtime, "_launch_upload", lambda: pytest.fail("Upload is disabled"))
    for scope in ("standalone", "aliyun"):
        telemetry_runtime.emit_automatic_invocation(scope, "query")
    assert not list(usage_state.glob("*.usage"))


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO fixture needs POSIX")
def test_nonregular_host_settings_do_not_block_first_command(tmp_path):
    host = tmp_path / ".aliyun"
    host.mkdir()
    os.mkfifo(host / "telemetry.json")
    completed = _invoke(tmp_path, "aliyun maxc", "agent", "context")
    assert completed.returncode == 0
    assert json.loads(completed.stdout)["status"] == "success"
    assert not (tmp_path / ".maxc/state/telemetry").exists()
