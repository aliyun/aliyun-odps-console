from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from uuid import uuid4

import pytest

from maxc_cli import cli
from maxc_cli.telemetry import HOST_DISABLE_ENV, encode_batch, report_day
from maxc_cli.telemetry_report import daily_counts


@pytest.fixture
def usage_state(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.delenv(HOST_DISABLE_ENV, raising=False)
    return tmp_path / ".maxc" / "state" / "telemetry"


def test_cli_records_normalized_invocations_without_input(monkeypatch, usage_state, tmp_path):
    calls = []

    def query(_app, args, _stdout):
        calls.append(cli._resolve_query_mode(args)[0])

    monkeypatch.setattr(cli, "MaxCApp", lambda **_kwargs: object())
    monkeypatch.setattr(cli, "_auth_seems_configured", lambda _app: True)
    monkeypatch.setattr(cli, "_handle_query", query)
    monkeypatch.setenv("MAXC_TELEMETRY_RECEIVER", "old-setting")
    cases = [
        ("maxc", ["query", "SELECT 'synthetic-marker'"]),
        ("aliyun maxc", ["query", "run", "SELECT 1"]),
        ("maxc", ["query", "cost", "SELECT 2"]),
        ("maxc", ["query", "SELECT 3", "--mode", "explain"]),
    ]
    for entry, argv in cases:
        monkeypatch.setenv("MAXC_CLI_NAME", entry)
        if "--mode" in argv:
            with pytest.warns(DeprecationWarning, match="--mode.*deprecated") as warnings:
                assert cli.run(argv, cwd=tmp_path, _automatic_telemetry=True) == 0
            assert len(warnings) == 1
        else:
            assert cli.run(argv, cwd=tmp_path, _automatic_telemetry=True) == 0
    assert calls == ["run", "run", "cost", "explain"]
    day = report_day().isoformat()
    assert (usage_state / f"standalone.{day}.usage").read_bytes().splitlines() == [
        b"query", b"query.cost", b"query.explain",
    ]
    assert (usage_state / f"aliyun.{day}.usage").read_bytes().splitlines() == [b"query"]


def test_empty_invalid_help_and_internal_reentry_are_not_counted(monkeypatch, usage_state, tmp_path):
    monkeypatch.setattr(cli, "MaxCApp", lambda **_kwargs: object())
    monkeypatch.setattr(cli, "_auth_seems_configured", lambda _app: True)
    monkeypatch.setattr(cli, "_handle_query", lambda *_args: None)
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False)

    def run(argv, **kwargs):
        return cli.run(argv, stdout=StringIO(), cwd=tmp_path, _automatic_telemetry=True, **kwargs)

    assert run([]) == 0
    assert run(["query", "cost"]) == 0
    assert run(["--json", "bad-command"]) == 2
    with pytest.raises(SystemExit) as help_exit:
        run(["query", "--help"])
    assert help_exit.value.code == 0
    assert run(["query", "SELECT 1"], _auto_login_done=True) == 0
    assert not usage_state.exists()


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
