"""Tests for maxc agent skill / agent context / agent skill install — blind-spot coverage."""

import json
import os
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



# ── --mode deprecation ───────────────────────────────────────

class TestQueryModeDeprecation:
    def test_mode_flag_still_accepted(self, tmp_path):
        """--mode run should still work for backward compatibility."""
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["query", "SELECT 1", "--mode", "run", "--json"])
        # argparse errors produce no JSON envelope; a non-empty payload means argparse accepted --mode
        assert payload, "--mode should not cause argparse error (expected JSON envelope)"

    def test_mode_cost_still_works(self, tmp_path):
        """--mode cost should still work and trigger deprecation warning."""
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["query", "SELECT 1", "--mode", "cost", "--json"])
        # argparse errors produce no JSON envelope; a non-empty payload means argparse accepted --mode
        assert payload, "--mode cost should not cause argparse error (expected JSON envelope)"

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
        import importlib
        import inspect
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


# ── agent skill install ──────────────────────────────────────────────────────

class TestAgentInstallSkill:
    """Tests for maxc agent skill install command."""

    @pytest.fixture(autouse=True)
    def _clean_skill_dirs(self):
        """Remove skill install dirs before each test to avoid stale version files."""
        import shutil
        for d in [
            Path.home() / ".claude" / "skills" / "maxc-cli",
            Path.home() / ".cursor" / "skills" / "maxc-cli",
            Path.home() / ".codeium" / "windsurf" / "skills" / "maxc-cli",
            Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills" / "maxc-cli",
            Path.home() / ".qwen" / "skills" / "maxc-cli",
            Path.home() / ".qoder" / "skills" / "maxc-cli",
            Path.home() / ".qoderwork" / "skills" / "maxc-cli",
            Path.home() / ".openclaw" / "workspace" / "skills" / "maxc-cli",
            Path.home() / ".hermes" / "skills" / "maxc-cli",
        ]:
            if d.exists():
                shutil.rmtree(str(d))
        yield
        for d in [
            Path.home() / ".claude" / "skills" / "maxc-cli",
            Path.home() / ".cursor" / "skills" / "maxc-cli",
            Path.home() / ".codeium" / "windsurf" / "skills" / "maxc-cli",
            Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills" / "maxc-cli",
            Path.home() / ".qwen" / "skills" / "maxc-cli",
            Path.home() / ".qoder" / "skills" / "maxc-cli",
            Path.home() / ".qoderwork" / "skills" / "maxc-cli",
            Path.home() / ".openclaw" / "workspace" / "skills" / "maxc-cli",
            Path.home() / ".hermes" / "skills" / "maxc-cli",
        ]:
            if d.exists():
                shutil.rmtree(str(d))

    def test_install_skill_claude_code(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "claude-code"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert (install_path / "SKILL.md").is_file()
        assert (install_path / "references").is_dir()
        assert not (install_path / ".claude-plugin").exists()

    def test_install_skill_cursor(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "cursor", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "cursor"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert "maxc-cli" in str(install_path)
        assert (install_path / "SKILL.md").is_file()
        assert not (install_path / ".claude-plugin").exists()

    def test_install_skill_codex(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "codex", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "codex"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert ".codex/skills" in str(install_path)
        assert (install_path / "SKILL.md").is_file()

    def test_install_skill_windsurf(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "windsurf", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "windsurf"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert ".codeium/windsurf/skills" in str(install_path)
        assert (install_path / "SKILL.md").is_file()

    def test_install_skill_qwen(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "qwen", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "qwen"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert ".qwen/skills" in str(install_path)
        assert (install_path / "SKILL.md").is_file()

    def test_install_skill_qoder(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "qoder", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "qoder"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert ".qoder/skills" in str(install_path)
        assert (install_path / "SKILL.md").is_file()

    def test_install_skill_qoderwork(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "qoderwork", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "qoderwork"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert ".qoderwork/skills" in str(install_path)
        assert (install_path / "SKILL.md").is_file()

    def test_install_skill_openclaw(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "openclaw", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "openclaw"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert ".openclaw/workspace/skills" in str(install_path)
        assert (install_path / "SKILL.md").is_file()

    def test_install_skill_hermes(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "hermes", "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "hermes"
        assert data["upgraded"] is True
        install_path = Path(data["install_path"])
        assert ".hermes/skills" in str(install_path)
        assert (install_path / "SKILL.md").is_file()

    def test_install_skill_others_requires_dir(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "others", "--json"])
        assert code == 1
        assert payload["status"] == "failure"
        assert "--dir" in payload["error"]["message"]

    def test_install_skill_others_with_dir(self, tmp_path):
        config = _make_config(tmp_path)
        target = tmp_path / "custom-agent-skill"
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "others", "--dir", str(target), "--json"])
        assert code == 0
        data = payload["data"]
        assert data["platform"] == "others"
        assert data["upgraded"] is True
        assert (target / "SKILL.md").is_file()

    def test_install_skill_default_platform_is_claude_code(self, tmp_path):
        config = _make_config(tmp_path)
        code, payload, _ = _run_cmd(config, ["agent", "skill", "install", "--json"])
        assert code == 0
        assert payload["data"]["platform"] == "claude-code"

    def test_install_skill_next_step_hint(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        assert "auto-discovered" in payload["data"]["next_step"]

        _, payload, _ = _run_cmd(config, ["agent", "skill", "install", "cursor", "--json"])
        assert "Restart" in payload["data"]["next_step"]

    def test_install_skill_skips_when_same_version(self, tmp_path):
        """Second run at same version should return upgraded=False."""
        config = _make_config(tmp_path)
        _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        _, payload, _ = _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        assert payload["data"]["upgraded"] is False
        assert payload["data"]["files_copied"] == []
        assert "up to date" in payload["data"]["next_step"]

    def test_install_skill_upgrades_on_version_change(self, tmp_path):
        """If version marker differs, files should be overwritten."""
        config = _make_config(tmp_path)
        _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        install_path = Path.home() / ".claude" / "skills" / "maxc-cli"
        (install_path / ".maxc-skill-version").write_text("0.0.0")
        _, payload, _ = _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        assert payload["data"]["upgraded"] is True
        assert "SKILL.md" in payload["data"]["files_copied"]

    def test_install_skill_version_file_created(self, tmp_path):
        config = _make_config(tmp_path)
        _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        install_path = Path.home() / ".claude" / "skills" / "maxc-cli"
        version_file = install_path / ".maxc-skill-version"
        assert version_file.is_file()
        from maxc_cli import __version__
        # Marker is `{version}+{invocation}` so a switch re-renders.
        assert version_file.read_text().strip() == f"{__version__}+maxc"

    def test_install_skill_files_copied(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        files = payload["data"]["files_copied"]
        assert "SKILL.md" in files
        assert "references/" in files
        assert "agents/" in files

    def test_install_skill_default_invocation_renders_maxc(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        assert payload["data"]["invocation"] == "maxc"
        install_path = Path(payload["data"]["install_path"])
        skill_text = (install_path / "SKILL.md").read_text()
        # Placeholders must be fully resolved.
        assert "{{cli}}" not in skill_text
        assert "{{cli_module}}" not in skill_text
        # Rendered as `maxc` command, not the aliyun form.
        assert "`maxc auth whoami" in skill_text
        assert "aliyun maxc" not in skill_text

    def test_install_skill_aliyun_invocation_renders_aliyun_maxc(self, tmp_path):
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(
            config,
            ["agent", "skill", "install", "claude-code", "--invocation", "aliyun-maxc", "--json"],
        )
        assert payload["data"]["invocation"] == "aliyun-maxc"
        install_path = Path(payload["data"]["install_path"])
        skill_text = (install_path / "SKILL.md").read_text()
        assert "{{cli}}" not in skill_text
        assert "{{cli_module}}" not in skill_text
        # Command examples now use `aliyun maxc`.
        assert "`aliyun maxc auth whoami" in skill_text
        # Version marker carries the invocation suffix.
        from maxc_cli import __version__
        version_file = install_path / ".maxc-skill-version"
        assert version_file.read_text().strip() == f"{__version__}+aliyun-maxc"

    def test_install_skill_switching_invocation_triggers_reinstall(self, tmp_path):
        """Switching invocation must re-render even if version is unchanged."""
        config = _make_config(tmp_path)
        _run_cmd(config, ["agent", "skill", "install", "claude-code", "--json"])
        # Same invocation again → upgraded=False.
        _, payload, _ = _run_cmd(
            config, ["agent", "skill", "install", "claude-code", "--json"]
        )
        assert payload["data"]["upgraded"] is False
        # Switch invocation → upgraded=True.
        _, payload, _ = _run_cmd(
            config,
            ["agent", "skill", "install", "claude-code", "--invocation", "aliyun-maxc", "--json"],
        )
        assert payload["data"]["upgraded"] is True
        install_path = Path(payload["data"]["install_path"])
        skill_text = (install_path / "SKILL.md").read_text()
        assert "`aliyun maxc auth whoami" in skill_text

    def test_install_skill_renders_references_and_agents(self, tmp_path):
        """Placeholders inside references/ and agents/ subtrees must render too."""
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(
            config,
            ["agent", "skill", "install", "claude-code", "--invocation", "aliyun-maxc", "--json"],
        )
        install_path = Path(payload["data"]["install_path"])
        for path in (install_path / "references").rglob("*"):
            if path.is_file() and path.suffix == ".md":
                content = path.read_text()
                assert "{{cli}}" not in content, f"leftover placeholder in {path}"
                assert "{{cli_module}}" not in content, f"leftover placeholder in {path}"
        # agents/openai.yaml — ensure the YAML went through the renderer
        # (no leftover {{cli}} / {{cli_module}} placeholders).
        agents_yaml = install_path / "agents" / "openai.yaml"
        if agents_yaml.is_file():
            content = agents_yaml.read_text()
            assert "{{cli}}" not in content
            assert "{{cli_module}}" not in content

    def test_install_skill_aliyun_drops_redundant_fallback_prose(self, tmp_path):
        """For aliyun-maxc, redundant `Prefer X; fall back to X` clauses must
        be eliminated by the conditional-block renderer."""
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(
            config,
            ["agent", "skill", "install", "claude-code", "--invocation", "aliyun-maxc", "--json"],
        )
        install_path = Path(payload["data"]["install_path"])
        skill_md = (install_path / "SKILL.md").read_text()
        # No leftover @if/@endif markers.
        assert "@if" not in skill_md
        assert "@endif" not in skill_md
        # No degenerate "fall back to `aliyun maxc ...`" sentences.
        assert "fall back to `aliyun maxc" not in skill_md
        # Bootstrap flow phase 1 code block must NOT have the redundant
        # `|| aliyun maxc --version` chain.
        bootstrap_flow = (install_path / "references" / "bootstrap-flow.md").read_text()
        assert "|| aliyun maxc --version" not in bootstrap_flow
        # Setup-install verify block must NOT show the same command twice.
        setup_install = (install_path / "references" / "setup-install.md").read_text()
        assert setup_install.count("aliyun maxc --help\n```") <= 1
        # Command-patterns prose about replacing the script with the module
        # form is gone (it's a no-op for aliyun maxc).
        cmd_patterns = (install_path / "references" / "command-patterns.md").read_text()
        assert "replace `aliyun maxc` with `aliyun maxc`" not in cmd_patterns

    def test_install_skill_maxc_keeps_fallback_prose(self, tmp_path):
        """The PyPI invocation keeps the module-form fallback prose since
        `python3 -m maxc_cli` genuinely differs from `maxc`."""
        config = _make_config(tmp_path)
        _, payload, _ = _run_cmd(
            config, ["agent", "skill", "install", "claude-code", "--json"]
        )
        install_path = Path(payload["data"]["install_path"])
        skill_md = (install_path / "SKILL.md").read_text()
        assert "fall back to `python3 -m maxc_cli" in skill_md
        # Marker comments are still stripped, even when the block is kept.
        assert "@if" not in skill_md
        assert "@endif" not in skill_md
