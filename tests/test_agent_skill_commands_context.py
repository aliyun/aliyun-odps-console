"""Tests for maxc agent skill / agent commands / agent context — blind-spot coverage."""

import json
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.cli import run


def _make_config(tmp_path: Path) -> Path:
    """Create a minimal config file for testing."""
    config = tmp_path / "config.yaml"
    config.write_text(
        "default_project: test_proj\n"
        "default_region: cn-hangzhou\n"
        "default_format: json\n"
        "project_context: testing\n"
        "allowed_operations:\n"
        "  - SELECT\n"
        "cost_threshold_cu: 100\n"
        "sensitive_columns: []\n"
    )
    return config


def _run_cmd(config_path: Path, argv: list) -> tuple:
    stdout, stderr = StringIO(), StringIO()
    code = run(["--config", str(config_path), *argv], cwd=config_path.parent, stdout=stdout, stderr=stderr)
    out = stdout.getvalue()
    err = stderr.getvalue()
    payload = json.loads(out) if out.strip() else {}
    return code, payload, err


# ── agent skill ──────────────────────────────────────────────

class TestAgentSkill:
    def test_agent_skill_returns_success(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "--json"])
        assert code == 0, f"Expected 0, got {code}"

    def test_agent_skill_has_skill_path(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "skill", "--json"])
        assert "skill_path" in payload.get("data", {}), f"Missing skill_path: {payload.get('data', {})}"

    def test_agent_skill_file_exists(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "skill", "--json"])
        skill_path = payload["data"]["skill_path"]
        assert Path(skill_path).exists(), f"SKILL.md not found at {skill_path}"

    def test_agent_skill_has_metadata(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "skill", "--json"])
        data = payload.get("data", {})
        for key in ("name", "version", "category", "entry_point"):
            assert key in data, f"Missing '{key}' in agent skill metadata: {data}"


# ── agent commands ───────────────────────────────────────────

class TestAgentCommands:
    def test_agent_commands_returns_success(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "commands", "--json"])
        assert code == 0, f"Expected 0, got {code}"

    def test_agent_commands_has_groups(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "commands", "--json"])
        data = payload.get("data", {})
        assert "groups" in data, f"Missing 'groups' in agent commands: {data}"

    def test_agent_commands_includes_meta(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "commands", "--json"])
        groups = payload["data"]["groups"]
        group_names = [g["name"] for g in groups]
        assert "meta" in group_names, f"Missing 'meta' in command groups: {group_names}"

    def test_agent_commands_group_structure(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "commands", "--json"])
        groups = payload["data"]["groups"]
        for g in groups:
            assert "name" in g, f"Group missing 'name': {g}"
            assert "description" in g, f"Group missing 'description': {g}"
            assert "subcommands" in g, f"Group missing 'subcommands': {g}"

    def test_agent_commands_subcommand_has_usage(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "commands", "--json"])
        groups = payload["data"]["groups"]
        for g in groups:
            for sc in g["subcommands"]:
                assert "usage" in sc, f"Subcommand missing 'usage': {sc}"
                assert "maxc" in sc["usage"], f"Usage should start with maxc: {sc['usage']}"


# ── agent context enhanced ───────────────────────────────────

class TestAgentContextEnhanced:
    def test_context_has_version(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        assert "version" in ctx, "Missing 'version' in agent context"

    def test_context_has_python_version(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        assert "python_version" in ctx, "Missing 'python_version' in agent context"

    def test_context_has_auth_status(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        assert "auth_status" in ctx, "Missing 'auth_status' in agent context"

    def test_context_has_backend_reachable(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        assert "backend_reachable" in ctx, "Missing 'backend_reachable' in agent context"

    def test_context_has_capabilities(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        assert "capabilities" in ctx, "Missing 'capabilities' in agent context"

    def test_context_no_backend_auth_status(self, tmp_path):
        """Without a real backend, auth_status should indicate not configured or unreachable."""
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        assert ctx["auth_status"] in ("not_configured", "unreachable", "unknown"), \
            f"Unexpected auth_status without backend: {ctx['auth_status']}"

    def test_context_has_entry_point(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        assert ctx["entry_point"] == "maxc", f"entry_point should be 'maxc', got {ctx.get('entry_point')}"

    def test_context_capabilities_dict(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "context", "--json"])
        ctx = payload["data"]["context"]
        caps = ctx["capabilities"]
        assert isinstance(caps, dict), f"capabilities should be dict, got {type(caps)}"
        assert "remote_jobs" in caps
        assert "lineage" in caps


# ── ErrorPayload.recovery_steps ──────────────────────────────

class TestErrorRecoverySteps:
    def test_permission_denied_has_recovery(self):
        from maxc_cli.exceptions import PermissionDeniedError
        err = PermissionDeniedError("Access denied", suggestion="Contact admin")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0, "PERMISSION_DENIED should have recovery_steps"

    def test_backend_connection_error_has_recovery(self):
        from maxc_cli.exceptions import BackendConnectionError
        err = BackendConnectionError("Connection failed")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0, "BACKEND_CONNECTION_ERROR should have recovery_steps"

    def test_job_timeout_has_recovery(self):
        from maxc_cli.exceptions import JobTimeoutError
        err = JobTimeoutError("Timed out")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0, "JOB_TIMEOUT should have recovery_steps"

    def test_cost_limit_exceeded_has_recovery(self):
        from maxc_cli.exceptions import CostLimitExceededError
        err = CostLimitExceededError("Cost too high")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0, "COST_LIMIT_EXCEEDED should have recovery_steps"

    def test_validation_error_has_recovery(self):
        from maxc_cli.exceptions import ValidationError
        err = ValidationError("Bad input")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0, "VALIDATION_ERROR should have recovery_steps"

    def test_not_found_has_recovery(self):
        from maxc_cli.exceptions import NotFoundError
        err = NotFoundError("Table not found")
        payload = err.to_payload()
        assert len(payload.recovery_steps) > 0, "NOT_FOUND should have recovery_steps"

    def test_unknown_error_empty_recovery(self):
        from maxc_cli.exceptions import MaxCError
        err = MaxCError("Something went wrong")
        payload = err.to_payload()
        assert payload.recovery_steps == [], "Unknown error code should have empty recovery_steps"

    def test_recovery_steps_in_to_dict(self):
        from maxc_cli.exceptions import PermissionDeniedError
        err = PermissionDeniedError("Access denied")
        d = err.to_payload().to_dict()
        assert "recovery_steps" in d, "recovery_steps should appear in to_dict()"
        assert isinstance(d["recovery_steps"], list)

    def test_recovery_steps_omitted_when_empty(self):
        from maxc_cli.exceptions import MaxCError
        err = MaxCError("Something went wrong")
        d = err.to_payload().to_dict()
        assert "recovery_steps" not in d, "Empty recovery_steps should be omitted from to_dict()"

    def test_recovery_steps_contain_maxc_commands(self):
        from maxc_cli.exceptions import PermissionDeniedError
        err = PermissionDeniedError("Access denied")
        steps = err.to_payload().recovery_steps
        assert any("maxc" in s for s in steps), \
            f"recovery_steps should contain maxc commands: {steps}"


# ── --mode deprecation ───────────────────────────────────────

class TestQueryModeDeprecation:
    def test_mode_flag_still_accepted(self, tmp_path):
        """--mode run should still work for backward compatibility."""
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["query", "SELECT 1", "--mode", "run", "--json"])
        # argparse errors give exit code 2; any other code means argparse accepted --mode
        assert code != 2, "--mode should not cause argparse error"

    def test_mode_cost_still_works(self, tmp_path):
        """--mode cost should still work and trigger deprecation warning."""
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["query", "SELECT 1", "--mode", "cost", "--json"])
        # Should not be an argparse error
        assert code != 2, "--mode cost should not cause argparse error"

    def test_mode_hidden_from_help(self, tmp_path):
        """--mode should be suppressed from --help output."""
        import argparse
        from maxc_cli.cli import build_parser
        parser = build_parser()
        query_parser = None
        for action in parser._subparsers._actions:
            if hasattr(action, 'choices') and action.choices and 'query' in action.choices:
                query_parser = action.choices['query']
                break
        assert query_parser is not None, "query subparser not found"
        mode_actions = [a for a in query_parser._actions if '--mode' in getattr(a, 'option_strings', [])]
        assert len(mode_actions) > 0, "--mode should still exist as an argument"
        assert mode_actions[0].help == argparse.SUPPRESS, \
            f"--mode help should be SUPPRESS, got: {mode_actions[0].help!r}"


# ── Backend docstring consistency ────────────────────────────

class TestBackendDocstrings:
    """Ensure all backend public methods have structured docstrings."""

    @pytest.mark.parametrize("mixin_path", [
        "maxc_cli.backend.query.QueryMixin",
        "maxc_cli.backend.job.JobMixin",
        "maxc_cli.backend.meta.MetaMixin",
        "maxc_cli.backend.data.DataMixin",
        "maxc_cli.backend.auth.AuthMixin",
    ])
    def test_public_methods_have_docstrings(self, mixin_path):
        import importlib
        module_path, class_name = mixin_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        for name in dir(cls):
            if name.startswith("_"):
                continue
            method = getattr(cls, name)
            if callable(method):
                doc = getattr(method, "__doc__", None)
                assert doc and doc.strip(), f"{mixin_path}.{name} missing docstring"

    @pytest.mark.parametrize("mixin_path", [
        "maxc_cli.backend.query.QueryMixin",
        "maxc_cli.backend.job.JobMixin",
        "maxc_cli.backend.meta.MetaMixin",
        "maxc_cli.backend.data.DataMixin",
        "maxc_cli.backend.auth.AuthMixin",
    ])
    def test_docstrings_have_args_section(self, mixin_path):
        """All public method docstrings that take parameters should have Args: section."""
        import importlib, inspect
        module_path, class_name = mixin_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        for name in dir(cls):
            if name.startswith("_"):
                continue
            method = getattr(cls, name)
            if callable(method):
                doc = getattr(method, "__doc__", None)
                if not doc:
                    continue  # covered by test_public_methods_have_docstrings
                sig = inspect.signature(method)
                # Only check methods that have parameters beyond self
                params_beyond_self = [p for p in sig.parameters if p != "self"]
                if params_beyond_self:
                    assert "Args:" in doc, \
                        f"{mixin_path}.{name} takes params {params_beyond_self} but docstring missing Args: section"
