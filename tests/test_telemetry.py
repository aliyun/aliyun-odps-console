from __future__ import annotations

import json
import socket
import sqlite3
import threading
from collections import Counter
from datetime import timedelta
from io import StringIO
from pathlib import Path
from uuid import uuid4

import pytest

from maxc_cli import cli, telemetry_worker
from maxc_cli.telemetry import (
    HOST_DISABLE_ENV,
    RECEIVER_ENV,
    decode_batch,
    decode_invocation,
    emit_invocation,
    encode_batch,
    report_day,
)
from maxc_cli.telemetry_report import daily_counts
from maxc_cli.telemetry_worker import Delivery, TelemetryWorker, UsageQueue, _open_state


@pytest.fixture
def receiver(monkeypatch):
    token = uuid4().hex
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.settimeout(0.12)
        addr = f"{listener.getsockname()[1]}:{token}"
        monkeypatch.setenv(RECEIVER_ENV, addr)
        monkeypatch.delenv(HOST_DISABLE_ENV, raising=False)
        yield addr, token, listener


def test_cli_records_normalized_invocations_without_input(monkeypatch, receiver, tmp_path):
    _, token, listener = receiver
    calls = []

    def query(_app, args, _stdout):
        calls.append(cli._resolve_query_mode(args)[0])

    monkeypatch.setattr(cli, "MaxCApp", lambda **_kwargs: object())
    monkeypatch.setattr(cli, "_auth_seems_configured", lambda _app: True)
    monkeypatch.setattr(cli, "_handle_query", query)
    cases = [
        ("maxc", ["query", "SELECT 'synthetic-marker'"], "standalone", "query"),
        ("aliyun maxc", ["query", "run", "SELECT 1"], "aliyun", "query"),
        ("maxc", ["query", "cost", "SELECT 2"], "standalone", "query.cost"),
        ("maxc", ["query", "SELECT 3", "--mode", "explain"], "standalone", "query.explain"),
    ]
    for entry, argv, scope, command in cases:
        monkeypatch.setenv("MAXC_CLI_NAME", entry)
        if "--mode" in argv:
            with pytest.warns(DeprecationWarning, match="--mode.*deprecated") as warnings:
                assert cli.run(argv, cwd=tmp_path) == 0
            assert len(warnings) == 1
        else:
            assert cli.run(argv, cwd=tmp_path) == 0
        packet, _ = listener.recvfrom(256)
        assert packet == f"{token} {scope} {report_day().isoformat()} {command}".encode("ascii")
    assert calls == ["run", "run", "cost", "explain"]


def test_empty_invalid_help_and_internal_reentry_are_not_counted(monkeypatch, receiver, tmp_path):
    _, _, listener = receiver
    monkeypatch.setattr(cli, "MaxCApp", lambda **_kwargs: object())
    monkeypatch.setattr(cli, "_auth_seems_configured", lambda _app: True)
    monkeypatch.setattr(cli, "_handle_query", lambda *_args: None)
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False)
    assert cli.run([], stdout=StringIO(), cwd=tmp_path) == 0
    assert cli.run(["query", "cost"], stdout=StringIO(), cwd=tmp_path) == 0
    assert cli.run(["--json", "bad-command"], stdout=StringIO(), cwd=tmp_path) == 2
    with pytest.raises(SystemExit) as help_exit:
        cli.run(["query", "--help"], cwd=tmp_path)
    assert help_exit.value.code == 0
    assert cli.run(["query", "SELECT 1"], cwd=tmp_path, _auto_login_done=True) == 0
    with pytest.raises(socket.timeout):
        listener.recvfrom(256)


def test_host_env_opt_out_and_missing_worker_preserve_cli_result(monkeypatch, receiver, tmp_path):
    addr, _, listener = receiver
    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    monkeypatch.setenv(HOST_DISABLE_ENV, "true")
    monkeypatch.setattr(cli, "MaxCApp", lambda **_kwargs: object())
    monkeypatch.setattr(cli, "_handle_agent_context", lambda _app, _args, out: out.write("result\n"))

    def invoke():
        stdout, stderr = StringIO(), StringIO()
        exit_code = cli.run(["agent", "context"], cwd=tmp_path, stdout=stdout, stderr=stderr)
        return exit_code, stdout.getvalue(), stderr.getvalue()

    assert invoke() == (0, "result\n", "")
    with pytest.raises(socket.timeout):
        listener.recvfrom(256)
    monkeypatch.delenv(HOST_DISABLE_ENV)
    listener.close()
    assert invoke() == (0, "result\n", "")
    monkeypatch.setenv(RECEIVER_ENV, addr.replace(":", "::", 1))
    assert invoke() == (0, "result\n", "")


def test_opt_out_removes_pending_counts_and_preserves_other_scope(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    day = report_day().isoformat()
    config = tmp_path / ".maxc" / "config.yaml"
    config.parent.mkdir()
    host = tmp_path / ".aliyun" / "telemetry.json"
    host.parent.mkdir()
    today_cutoff = (report_day() - timedelta(days=7)).isoformat()
    assert telemetry_worker.read_switches() == {"standalone": True, "aliyun": True}
    with _open_state(tmp_path / "state") as db:
        queue = UsageQueue(db)
        queue.persist(Counter({("standalone", day, "query"): 2, ("aliyun", day, "query"): 3}))
        queue.next_batch({"aliyun"})
        host.write_text('{"enabled": false}', encoding="utf-8")
        queue.discard(telemetry_worker.read_switches(), today_cutoff)
        assert queue.next_batch({"aliyun"}) is None
        standalone = queue.next_batch({"standalone"})
        assert standalone is not None
        assert decode_batch(standalone.payload)[-1] == [("query", 2)]

        config.write_text("telemetry: [", encoding="utf-8")
        assert telemetry_worker.read_switches() == {"standalone": None, "aliyun": False}
        queue.discard(telemetry_worker.read_switches(), today_cutoff)
        assert queue.next_batch({"standalone"}) == standalone

        config.write_text("telemetry:\n  enabled: false\n", encoding="utf-8")
        queue.discard(telemetry_worker.read_switches(), today_cutoff)
        assert queue.next_batch({"standalone"}) is None


def test_worker_reloads_opt_out_and_rejects_later_datagrams(monkeypatch, tmp_path, receiver):
    addr, token, listener = receiver
    listener.settimeout(0.03)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    config = tmp_path / ".maxc" / "config.yaml"
    config.parent.mkdir()
    monkeypatch.setattr(telemetry_worker, "_POLICY_SECONDS", 0.03)
    monkeypatch.setattr(telemetry_worker, "_PERSIST_EVENTS", 1)
    ready, stopped = threading.Event(), threading.Event()
    errors, stats = [], {}

    def work():
        try:
            with _open_state(tmp_path / "state") as db:
                worker = TelemetryWorker(UsageQueue(db), frozenset({"query"}), None, 1800)
                ready.set()
                stats.update(worker.run(listener, token, stopped))
        except Exception as exc:
            errors.append(exc)

    def wait_for_count(db, n):
        for _ in range(100):
            if db.execute("SELECT COALESCE(SUM(n), 0) FROM counts").fetchone() == (n,):
                return
            stopped.wait(0.02)
        pytest.fail(f"Expected {n} queued invocations")

    thread = threading.Thread(target=work)
    thread.start()
    try:
        assert ready.wait(3)
        emit_invocation(addr, "standalone", "query")
        with sqlite3.connect(tmp_path / "state" / "usage.sqlite3") as db:
            wait_for_count(db, 1)
            config.write_text("telemetry:\n  enabled: false\n")
            wait_for_count(db, 0)
            emit_invocation(addr, "standalone", "query")
            emit_invocation(addr, "aliyun", "query")
            stopped.wait(0.1)
            wait_for_count(db, 0)
    finally:
        stopped.set()
        thread.join(timeout=4)
    assert not thread.is_alive()
    assert not errors
    assert stats["collected"] == 1


def test_frozen_batch_survives_restart_and_new_calls(tmp_path):
    day = report_day().isoformat()
    earlier = (report_day() - timedelta(days=1)).isoformat()
    state = tmp_path / "state"
    with _open_state(state) as db:
        queue = UsageQueue(db)
        queue.persist(Counter({("standalone", day, "query"): 3, ("standalone", earlier, "query"): 2}))
        first = queue.next_batch({"standalone"})
        assert first is not None
        assert decode_batch(first.payload) == (first.batch_id, earlier, [("query", 2)])
        queue.persist(Counter({("standalone", day, "query"): 4}))

    with _open_state(state) as db:
        queue = UsageQueue(db)
        assert first == queue.next_batch({"standalone"})
        queue.finish(first)
        second = queue.next_batch({"standalone"})
        assert second is not None
        assert second.batch_id != first.batch_id
        assert decode_batch(second.payload)[1:] == (day, [("query", 7)])


def test_worker_keeps_receiving_during_an_upload(monkeypatch, tmp_path, receiver):
    addr, token, listener = receiver
    started, release, stop = threading.Event(), threading.Event(), threading.Event()
    attempted = []
    errors = []
    listener.settimeout(0.03)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(telemetry_worker, "_PERSIST_EVENTS", 1)
    monkeypatch.setattr(TelemetryWorker, "_next_interval", lambda _worker: 0.15)

    def send(batch, _commands, _config, _timeout):
        attempted.append(batch)
        started.set()
        assert release.wait(3)
        return Delivery("retry", 429)

    monkeypatch.setattr(telemetry_worker, "send_batch", send)

    def work():
        try:
            with _open_state(tmp_path / "state") as db:
                queue = UsageQueue(db)
                worker = TelemetryWorker(queue, frozenset({"query"}), None, 1)
                worker.run(listener, token, stop)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=work)
    thread.start()
    try:
        emit_invocation(addr, "standalone", "query")
        assert started.wait(3)
        for _ in range(3):
            emit_invocation(addr, "standalone", "query")
        with sqlite3.connect(tmp_path / "state" / "usage.sqlite3") as reader:
            for _ in range(50):
                result = reader.execute("SELECT SUM(n) FROM counts").fetchone()
                if result == (3,):
                    break
                stop.wait(0.02)
            assert result == (3,)
        assert len(attempted) == 1
    finally:
        stop.set()
        release.set()
        thread.join(timeout=4)
    assert not thread.is_alive()
    assert not errors
    with _open_state(tmp_path / "state") as db:
        assert UsageQueue(db).next_batch({"standalone"}) == attempted[0]


def test_invalid_packets_and_expired_days_are_dropped():
    today = report_day()
    token = uuid4().hex
    day = (today - timedelta(days=8)).isoformat()
    commands = frozenset({"query"})
    assert decode_invocation(f"{token} aliyun {day} query".encode(), token, commands, today) is None
    assert decode_invocation(f"{token} aliyun {today} query.unknown".encode(), token, commands, today) is None
    assert decode_invocation(f"{'0' * 32} aliyun {today} query".encode(), token, commands, today) is None


def test_daily_rollup_deduplicates_and_omits_conflicting_batch_ids():
    day = "2026-11-18"
    first_id, conflict_id = str(uuid4()), str(uuid4())

    def sdk_log(batch_id, counts):
        return json.dumps({"SDK LOG": encode_batch(batch_id, day, counts).decode("ascii")})

    batches = [
        sdk_log(first_id, [("query", 3), ("job.wait", 1)]),
        sdk_log(first_id, [("job.wait", 1), ("query", 3)]),
        sdk_log(conflict_id, [("query", 5)]),
        sdk_log(conflict_id, [("query", 9)]),
        sdk_log(str(uuid4()), [("query", 2)]),
        sdk_log(str(uuid4()), [("unregistered", 100)]),
        "{'some other SDK log'}",
    ]
    rows, quality = daily_counts(batches, frozenset({"query", "job.wait"}))
    assert rows == [(day, "job.wait", 1), (day, "query", 5)]
    assert quality == {"accepted": 2, "duplicates": 1, "conflicts": 1, "rejected": 1}
