#!/usr/bin/env python3
"""Accept every command from an installed or bundled MaxC executable."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

from release_e2e.services import PROJECT, emulator, fixtures
from release_e2e.upgrade import isolated_env, require


class Commands:
    def __init__(self, executable, home, config, certificate):
        self.executable = executable
        self.home = home
        self.config = config
        self.env = isolated_env(home)
        self.env.update({"MAXC_CONFIG": str(config), "SSL_CERT_FILE": str(certificate), "CODEX_HOME": str(home / ".codex")})
        self.attempted = set()
        self.failures = {}

    def process(self, command, argv, *, input_text=None, config=None):
        self.attempted.add(command)
        env = self.env | ({"MAXC_CONFIG": str(config)} if config is not None else {})
        return subprocess.run(
            [str(self.executable), *argv], cwd=self.home, env=env,
            input=input_text, capture_output=True, text=True, encoding="utf-8", timeout=35,
        )

    def fail(self, name, detail):
        self.failures.setdefault(name, []).append(detail)
        print(f"FAIL {name}: {detail}", file=sys.stderr, flush=True)

    def expect(self, name, condition, detail):
        if not condition:
            self.fail(name, detail)

    def call(self, name, *argv, config=None, statuses=("success",), keys=()):
        try:
            result = self.process(name, [*name.split("."), *map(str, argv), "--json"], config=config)
            payload = json.loads(result.stdout)
        except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
            self.fail(name, type(exc).__name__)
            return {}
        data = payload.get("data") or {}
        if result.returncode != 0 or payload.get("status") not in statuses:
            error = payload.get("error") or {}
            self.fail(name, f"exit={result.returncode} status={payload.get('status')} {error.get('code', '')} {str(error.get('message', ''))[:300]}")
        else:
            self.expect(name, all(key in data for key in keys), f"Output missing one of {keys}")
        return data


def local_commands(cli, endpoint):
    cli.call("agent.context", keys=("context",))
    skill_info = cli.call("agent.skill", keys=("skill_path",))
    cli.expect("agent.skill", skill_info.get("skill_exists") is True, "The bundled skill is missing")
    skill = cli.home / "skill"
    cli.call("agent.skill.install", "codex", "--dir", skill)
    cli.expect("agent.skill.install", (skill / "SKILL.md").is_file(), "Skill install did not write SKILL.md")
    cli.call("agent.skill.path", "codex", "--dir", skill)
    cli.call("agent.skill.list")
    cli.call("agent.skill.diff", "codex", "--dir", skill)
    cli.call("agent.skill.update", "codex", "--dir", skill)
    cli.call("agent.skill.uninstall", "codex", "--dir", skill)
    cli.expect("agent.skill.uninstall", not (skill / "SKILL.md").exists(), "Skill uninstall left the installed SKILL.md")

    login_config = cli.home / "login.yaml"
    cli.call("auth.login", "--access-id", "fixture", "--secret-access-key", "fixture",
             "--project", PROJECT, "--endpoint", endpoint, "--no-validate", config=login_config)
    cli.expect("auth.login", login_config.is_file(), "Login did not save the selected configuration")
    cli.call("auth.login-external", "--process-command", "fixture-credential",
             "--project", PROJECT, "--endpoint", endpoint, "--no-validate", config=login_config)
    cli.call("auth.login-proxy", "--project", PROJECT, "--endpoint", endpoint,
             "--no-validate", config=login_config)
    cli.call("auth.logout", config=login_config)

    session_config = cli.home / "session.yaml"
    session_config.write_bytes(cli.config.read_bytes())
    cli.call("session.set", "--project", "fixture_session", "--schema", "default", config=session_config)
    shown = cli.call("session.show", config=session_config, keys=("project",))
    cli.expect("session.show", shown.get("project", {}).get("value") == "fixture_session", "Session override was not persisted")
    cli.call("session.unset", config=session_config)
    shown = cli.call("session.show", config=session_config)
    cli.expect("session.unset", shown.get("project", {}).get("value") == PROJECT, "Session unset did not restore the authenticated project")


def data_commands(cli):
    identity = cli.call("auth.whoami", keys=("identity",)).get("identity", {})
    cli.expect("auth.whoami", identity.get("authenticated") is True and identity.get("validation_status") == "verified", "Fixture identity did not pass online verification")
    permission = cli.call("auth.can-i", "--table", "demo", "--operation", "Select", keys=("authorization",))
    cli.expect("auth.can-i", permission.get("authorization", {}).get("allowed") is True, "Fixture table permission was not returned")
    doctor = cli.call("agent.doctor", "--online", keys=("ready",))
    cli.expect("agent.doctor", doctor.get("ready") is True, "The fixture was not reported ready")

    for command, argv, key in (
        ("meta.list-projects", [], "projects"),
        ("meta.list-schemas", [], "schemas"),
        ("meta.list-tables", [], "tables"),
        ("meta.list-functions", [], "functions"),
        ("meta.describe-function", ["fixture_fn"], "function"),
        ("meta.describe", ["demo"], "table"),
        ("meta.partitions", ["events"], "partitions"),
        ("meta.latest-partition", ["events"], "partition"),
        ("meta.freshness", ["events"], "freshness"),
    ):
        cli.call(command, *argv, keys=(key,))

    cli.call("cache.build", keys=("cached_tables",))
    cli.call("cache.build-status")
    cli.call("cache.status", keys=("table_count",))
    cli.call("meta.search", "demo", keys=("search",))
    cli.call("meta.search-columns", "id", keys=("search",))
    cli.call("meta.semantic.set", "demo", "--desc", "fixture semantic description")
    cli.call("meta.semantic.get", "demo")
    cli.call("meta.semantic.list-missing")
    cli.call("meta.semantic.clear", "demo")
    cli.call("cache.clear", "--force")

    sample = cli.call("data.sample", "events", "--rows", "1", "--partition", "ds=20260915", keys=("sample",))
    rows = sample.get("sample", {}).get("rows") or [{}]
    cli.expect("data.sample", rows[0].get("id") == 3, "The sampled partition returned the wrong row")
    cli.call("data.profile", "events", "--partition", "ds=20260915", keys=("profile",))
    source = cli.home / "upload.csv"
    source.write_text("id,name,amount,active\n99,fixture,12.30,true\n", encoding="utf-8")
    upload = cli.call("data.upload", "demo", "--file", source)
    cli.expect("data.upload", upload.get("rows_written") == 1, "Upload did not commit the fixture row")
    target = cli.home / "download.csv"
    cli.call("data.download", "demo", "--output", target)
    if target.is_file():
        with target.open(encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        cli.expect("data.download", any(row.get("id") == "99" for row in rows), "Download did not return the uploaded row")
    else:
        cli.fail("data.download", "Download did not create a CSV file")


def job_commands(cli):
    result = cli.call("query", "SELECT 7 AS e2e_value", keys=("result",)).get("result", {})
    cli.expect("query", result.get("rows") == [{"e2e_value": 7}], "SELECT 7 did not return the expected row")
    cli.call("query", "cost", "SELECT 7 AS e2e_value", keys=("analysis",))
    cli.call("query", "explain", "SELECT 7 AS e2e_value", keys=("analysis",))

    job = cli.call("job.submit", "SELECT 13 AS job_value", statuses=("pending", "success"), keys=("job_id",))
    job_id = job.get("job_id", "fixture_missing_job")
    for cmd, key in (
        ("job.status", "job"),
        ("job.wait", "result"),
        ("job.result", "result"),
        ("job.diagnose", "diagnosis"),
        ("job.task-detail", "detail"),
        ("job.task-summary", "summary"),
        ("job.workers", "workers"),
    ):
        payload = cli.call(cmd, job_id, keys=(key,))
        if cmd == "job.result":
            cli.expect(cmd, payload.get("result", {}).get("rows") == [{"job_value": 13}], "Job results changed")
    worker = cli.call("job.worker-log", job_id, "fixture_worker_log", keys=("content",))
    cli.expect("job.worker-log", worker.get("content") == "fixture worker log", "Incorrect worker log")
    cli.call("job.list", keys=("jobs",))
    cancel = cli.call("job.submit", "SELECT 14 AS cancel_value", statuses=("pending", "success"))
    cancel_id = cancel.get("job_id", "fixture_missing_job")
    cli.call("job.cancel", cancel_id, statuses=("success", "pending"))
    try:
        readback = cli.process("job.status", ["job", "status", cancel_id, "--json"])
        terminal = json.loads(readback.stdout)
        error = terminal.get("error") or {}
        cli.expect("job.cancel", readback.returncode != 0 and error.get("code") == "EXECUTION_FAILED"
                   and "Cancelled by fixture request" in error.get("message", ""), "Cancellation was not visible in job status")
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        cli.fail("job.cancel", type(exc).__name__)


def remote_commands(cli):
    ask = cli.call("kb.ask", "How do I list tables?", keys=("answer",))
    cli.expect("kb.ask", ask.get("answer", {}).get("text") == "A fixture answer", "MCP answer was not preserved")
    search = cli.call("kb.search", "show tables", keys=("search",))
    matches = search.get("search", {}).get("matches") or [{}]
    cli.expect("kb.search", matches[0].get("uri") == "https://example.invalid/tables", "MCP search result was not preserved")
    try:
        initialize = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                      "params": {"protocolVersion": "2024-11-05", "capabilities": {}}}
        list_tools = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        initialized = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        input_text = "\n".join(map(json.dumps, (initialize, initialized, list_tools))) + "\n"
        result = cli.process("mcp.serve", ["mcp", "serve"], input_text=input_text)
        frames = {frame["id"]: frame for frame in map(json.loads, result.stdout.splitlines())}
        tools = frames.get(2, {}).get("result", {}).get("tools", [])
        cli.expect("mcp.serve", result.returncode == 0 and bool(frames.get(1, {}).get("result")) and bool(tools),
                   f"MCP stdio served no tools (exit {result.returncode}): {result.stderr[:450]}")
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        cli.fail("mcp.serve", type(exc).__name__)

    definition = cli.home / "definition.json"
    definition.write_text(json.dumps({"description": "fixture",
                                      "dataScope": {"dataReferences": [{"project": PROJECT, "schema": "default", "table": "demo"}]}}))
    namespace = ("--namespace", "123456")
    name = "fixture_spec"
    cli.call("semantic.create", name, "--file", definition, "--yes", *namespace)
    cli.call("semantic.list", *namespace)
    cli.call("semantic.get", name, *namespace)
    exported = cli.home / "semantic.json"
    cli.call("semantic.export", name, "--output", exported, *namespace)
    if exported.is_file():
        document = json.loads(exported.read_text(encoding="utf-8"))
        document["content"]["glossary"] = [{"term": "fixture", "definition": "release check"}]
        exported.write_text(json.dumps(document), encoding="utf-8")
    else:
        exported.write_text("{}", encoding="utf-8")
    plan = cli.call("semantic.diff", name, "--file", exported, *namespace)
    applied = cli.call("semantic.apply", name, "--file", exported, "--plan-digest", plan.get("planDigest", "0" * 64),
                       "--yes", *namespace)
    cli.call("semantic.publish", name, "--expected-spec-id", "fixture_spec",
             "--expected-revision", applied.get("revisionId") or "fixture_missing_revision", "--yes", *namespace)
    cli.call("semantic.revisions", name, *namespace)
    cli.call("semantic.delete", name, "--expected-spec-id", "fixture_spec", "--yes", *namespace)


def acceptance(executable, root, upstream, expected_version):
    home = root / "home"
    home.mkdir()
    (home / ".maxc").mkdir()
    (home / ".maxc" / "config.yaml").write_text("telemetry:\n  enabled: false\n", encoding="utf-8")
    with fixtures(root, upstream) as (endpoint, mcp_endpoint, cert, traffic):
        # The public client uses dummy signing credentials and loopback endpoints.
        config = root / "fixture.yaml"
        config.write_text(json.dumps({
            "telemetry": {"enabled": False},
            "default_schema": "default",
            "auth": {
                "provider": "access_key", "access_id": "fixture", "secret_access_key": "fixture",
                "project": PROJECT, "endpoint": endpoint, "catalog_endpoint": endpoint,
                "tunnel_endpoint": upstream.geturl(),
            },
            "mcp": {"enabled": True, "endpoint": mcp_endpoint, "timeout_seconds": 4},
        }), encoding="utf-8")
        cli = Commands(executable, home, config, cert)
        manifest = cli.call("agent.manifest", keys=("cli_version", "commands", "command_count"))
        advertised = {item["command"] for item in manifest.get("commands", [])}
        require(advertised and len(advertised) == manifest["command_count"], "Invalid command inventory")
        if expected_version is not None:
            cli.expect("agent.manifest", manifest["cli_version"] == expected_version,
                       f"Expected candidate {expected_version}, got {manifest['cli_version']}")
        local_commands(cli, endpoint)
        data_commands(cli)
        job_commands(cli)
        remote_commands(cli)
        missing = advertised - cli.attempted
        unexpected = cli.attempted - advertised
        summary = {
            "version": manifest["cli_version"],
            "advertised": len(advertised),
            "attempted": len(cli.attempted),
            "passed": len(advertised & cli.attempted - set(cli.failures)),
            "commands": sorted(cli.attempted),
            "failed": sorted(cli.failures),
            "missing": sorted(missing),
            "unexpected": sorted(unexpected),
        }
        print(json.dumps(summary), flush=True)
        if cli.failures or missing or unexpected:
            (root / "fixture-requests.json").write_text(json.dumps(traffic), encoding="utf-8")
            return 1
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maxc", type=Path, required=True, help="Installed MaxC console script or built executable")
    parser.add_argument("--expected-version", help="Version that the release candidate must report")
    parser.add_argument("--emulator", help="Use an isolated, seeded emulator on 127.0.0.1 instead of starting Docker")
    parser.add_argument("--work-dir", type=Path, help="Retain this run's synthetic state in a new directory")
    args = parser.parse_args()
    executable = args.maxc.resolve()
    require(executable.is_file(), "--maxc must point to the candidate executable")
    if args.emulator:
        endpoint = urlsplit(args.emulator)
        require(endpoint.scheme == "http" and endpoint.hostname == "127.0.0.1" and endpoint.port, "--emulator must be a loopback HTTP address")
    with tempfile.TemporaryDirectory(prefix="maxc-release-commands-") as temp:
        root = args.work_dir.resolve() if args.work_dir else Path(temp).resolve()
        if args.work_dir:
            root.mkdir(mode=0o700)
        if args.emulator:
            return acceptance(executable, root, endpoint, args.expected_version)
        with emulator() as endpoint:
            return acceptance(executable, root, endpoint, args.expected_version)


if __name__ == "__main__":
    sys.exit(main())
