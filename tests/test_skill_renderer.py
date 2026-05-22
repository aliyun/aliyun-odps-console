"""Unit tests for ``render_skill_template`` — placeholder + conditional-block
substitution used when installing the bundled SKILL files.

These tests pin down the renderer in isolation so changes to the source files
or the install path can't silently break the contract."""

import pytest

from maxc_cli.app import render_skill_template


class TestPlaceholderSubstitution:
    def test_substitutes_cli_placeholder(self):
        out = render_skill_template(
            "Run `{{cli}} query --json`", cli="maxc", cli_module="python3 -m maxc_cli"
        )
        assert out == "Run `maxc query --json`"

    def test_substitutes_cli_module_placeholder(self):
        out = render_skill_template(
            "Module form: `{{cli_module}}`", cli="maxc", cli_module="python3 -m maxc_cli"
        )
        assert out == "Module form: `python3 -m maxc_cli`"

    def test_substitutes_both_placeholders_in_one_string(self):
        out = render_skill_template(
            "Use `{{cli}}` or `{{cli_module}}`",
            cli="aliyun maxc",
            cli_module="aliyun maxc",
        )
        assert out == "Use `aliyun maxc` or `aliyun maxc`"

    def test_repeated_placeholders(self):
        out = render_skill_template(
            "{{cli}} a; {{cli}} b; {{cli}} c", cli="maxc", cli_module="python3 -m maxc_cli"
        )
        assert out == "maxc a; maxc b; maxc c"

    def test_no_placeholders_passes_through(self):
        text = "plain text with `backticks` and {non-placeholder}"
        out = render_skill_template(text, cli="maxc", cli_module="python3 -m maxc_cli")
        assert out == text

    def test_empty_input(self):
        out = render_skill_template("", cli="maxc", cli_module="python3 -m maxc_cli")
        assert out == ""

    def test_unicode_content_preserved(self):
        # Source files contain Chinese descriptions; placeholders shouldn't
        # corrupt UTF-8 content around them.
        text = "中文描述：使用 `{{cli}}` 查询数据 — `{{cli_module}}`"
        out = render_skill_template(text, cli="maxc", cli_module="python3 -m maxc_cli")
        assert out == "中文描述：使用 `maxc` 查询数据 — `python3 -m maxc_cli`"


class TestConditionalBlocks:
    def test_kept_when_cli_module_differs(self):
        text = "Prefer `{{cli}}`<!-- @if cli_module_differs -->; fall back to `{{cli_module}}`<!-- @endif -->."
        out = render_skill_template(text, cli="maxc", cli_module="python3 -m maxc_cli")
        assert out == "Prefer `maxc`; fall back to `python3 -m maxc_cli`."

    def test_dropped_when_cli_module_same(self):
        text = "Prefer `{{cli}}`<!-- @if cli_module_differs -->; fall back to `{{cli_module}}`<!-- @endif -->."
        out = render_skill_template(text, cli="aliyun maxc", cli_module="aliyun maxc")
        assert out == "Prefer `aliyun maxc`."

    def test_negated_condition(self):
        # `not cli_module_differs` is the inverse — kept when cli == cli_module.
        text = "<!-- @if not cli_module_differs -->only-when-same<!-- @endif -->|always"
        same = render_skill_template(text, cli="x", cli_module="x")
        diff = render_skill_template(text, cli="x", cli_module="y")
        assert same == "only-when-same|always"
        assert diff == "|always"

    def test_unknown_condition_kept_intact(self):
        # Unknown conditions are kept verbatim rather than silently dropped,
        # so a typo can't quietly delete content.
        text = "<!-- @if unknown_flag -->kept<!-- @endif -->"
        out = render_skill_template(text, cli="x", cli_module="y")
        assert out == "kept"

    def test_multiple_blocks_independent(self):
        text = (
            "<!-- @if cli_module_differs -->A<!-- @endif -->"
            "B"
            "<!-- @if cli_module_differs -->C<!-- @endif -->"
        )
        out_diff = render_skill_template(text, cli="x", cli_module="y")
        out_same = render_skill_template(text, cli="x", cli_module="x")
        assert out_diff == "ABC"
        assert out_same == "B"

    def test_multiline_block_dropped(self):
        text = (
            "before\n"
            "<!-- @if cli_module_differs -->\n"
            "line 1\n"
            "line 2\n"
            "<!-- @endif -->\n"
            "after\n"
        )
        out = render_skill_template(text, cli="x", cli_module="x")
        assert "line 1" not in out
        assert "line 2" not in out
        assert "before" in out and "after" in out

    def test_multiline_block_kept(self):
        text = (
            "before\n"
            "<!-- @if cli_module_differs -->\n"
            "kept content\n"
            "<!-- @endif -->\n"
            "after\n"
        )
        out = render_skill_template(text, cli="x", cli_module="y")
        assert "kept content" in out

    def test_blank_line_runs_collapsed_after_drop(self):
        # When a block elimination leaves a triple+ blank-line run, collapse
        # it to a single blank line so the rendered Markdown stays tidy.
        text = "para 1\n\n<!-- @if cli_module_differs -->\nblock\n<!-- @endif -->\n\npara 2\n"
        out = render_skill_template(text, cli="x", cli_module="x")
        assert "\n\n\n" not in out
        # para 1 and para 2 still present and separated by a blank line.
        assert "para 1\n\npara 2" in out

    def test_placeholder_inside_kept_block_still_substituted(self):
        text = "<!-- @if cli_module_differs -->use `{{cli_module}}`<!-- @endif -->"
        out = render_skill_template(text, cli="maxc", cli_module="python3 -m maxc_cli")
        assert out == "use `python3 -m maxc_cli`"

    def test_placeholder_outside_block_substituted_when_block_dropped(self):
        text = "Run `{{cli}}`. <!-- @if cli_module_differs -->Or `{{cli_module}}`.<!-- @endif -->"
        out = render_skill_template(text, cli="aliyun maxc", cli_module="aliyun maxc")
        assert out == "Run `aliyun maxc`. "

    def test_no_leftover_markers_in_either_invocation(self):
        # Every form of marker should be consumed by the renderer.
        text = (
            "<!-- @if cli_module_differs -->A<!-- @endif -->"
            "<!-- @if not cli_module_differs -->B<!-- @endif -->"
        )
        for cli, cli_mod in [("maxc", "python3 -m maxc_cli"), ("aliyun maxc", "aliyun maxc")]:
            out = render_skill_template(text, cli=cli, cli_module=cli_mod)
            assert "@if" not in out
            assert "@endif" not in out
            assert "{{cli" not in out


class TestBundledSkillFilesRender:
    """Render every bundled .md/.yaml from src/maxc_cli/skills/ through the
    template engine and assert no leftover markers or placeholders. Catches
    drift between source files and the renderer."""

    @pytest.fixture(scope="class")
    def skill_files(self):
        import importlib.resources
        from pathlib import Path
        skills_root = Path(str(importlib.resources.files("maxc_cli") / "skills"))
        files = []
        for ext in ("*.md", "*.yaml", "*.yml"):
            files.extend(skills_root.rglob(ext))
        assert files, "no skill files found"
        return files

    @pytest.mark.parametrize(
        "invocation,cli,cli_module",
        [
            ("maxc", "maxc", "python3 -m maxc_cli"),
            ("aliyun-maxc", "aliyun maxc", "aliyun maxc"),
        ],
    )
    def test_all_files_render_clean(self, skill_files, invocation, cli, cli_module):
        for src in skill_files:
            content = src.read_text(encoding="utf-8")
            out = render_skill_template(content, cli=cli, cli_module=cli_module)
            assert "{{cli}}" not in out, f"leftover {{cli}} in {src} for {invocation}"
            assert "{{cli_module}}" not in out, f"leftover {{cli_module}} in {src}"
            assert "<!-- @if" not in out, f"leftover @if in {src}"
            assert "<!-- @endif" not in out, f"leftover @endif in {src}"

    def test_aliyun_maxc_drops_module_fallback_prose_in_skill_md(self, skill_files):
        skill_md = next(p for p in skill_files if p.name == "SKILL.md")
        content = skill_md.read_text(encoding="utf-8")
        rendered = render_skill_template(
            content, cli="aliyun maxc", cli_module="aliyun maxc"
        )
        # The intro line for aliyun-maxc shouldn't repeat itself in a
        # "Prefer X; fall back to X" form.
        assert "fall back to `aliyun maxc" not in rendered
        assert "; use `aliyun maxc ..." not in rendered

    def test_maxc_keeps_module_fallback_prose_in_skill_md(self, skill_files):
        skill_md = next(p for p in skill_files if p.name == "SKILL.md")
        content = skill_md.read_text(encoding="utf-8")
        rendered = render_skill_template(
            content, cli="maxc", cli_module="python3 -m maxc_cli"
        )
        # For PyPI users we keep the fallback hint pointing at the module form.
        assert "python3 -m maxc_cli" in rendered
        assert "fall back to" in rendered
