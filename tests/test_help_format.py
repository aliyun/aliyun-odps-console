import pytest

pytestmark = pytest.mark.unit

import argparse

from maxc_cli.help_format import AliyunStyleFormatter, cyan, green, strip_ansi


def test_color_helpers_are_noop_when_not_tty(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    assert cyan("hello") == "hello"
    assert green("world") == "world"


def test_color_helpers_wrap_when_tty(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    assert cyan("hello") == "\033[36mhello\033[0m"
    assert green("world") == "\033[32mworld\033[0m"


def test_strip_ansi_removes_color_codes():
    assert strip_ansi("\033[36mhello\033[0m world") == "hello world"


def _help_of(parser):
    return strip_ansi(parser.format_help())


def test_section_headings_remapped():
    p = argparse.ArgumentParser(prog="maxc demo", formatter_class=AliyunStyleFormatter)
    p.add_argument("--foo", help="foo flag")
    p.add_argument("bar", help="bar positional")
    text = _help_of(p)
    assert "Flags:" in text
    assert "options:" not in text
    assert "optional arguments:" not in text
    assert "Commands:" in text
    assert "positional arguments:" not in text


def test_usage_prefix_capitalized_and_on_own_line():
    p = argparse.ArgumentParser(prog="maxc demo", formatter_class=AliyunStyleFormatter)
    text = _help_of(p)
    assert text.startswith("Usage:\n  maxc demo")
    assert "usage:" not in text  # lowercase form is gone


def test_leaf_synopsis_is_compact():
    p = argparse.ArgumentParser(prog="maxc query", formatter_class=AliyunStyleFormatter)
    p.add_argument("--file")
    p.add_argument("--stdin", action="store_true")
    p.add_argument("--max-rows", type=int)
    p.add_argument("sql_parts", nargs="*")
    text = strip_ansi(p.format_help())
    first_two = "\n".join(text.splitlines()[:2])
    assert "[--file" not in first_two
    assert "[--stdin]" not in first_two
    assert "[--flags ...]" in first_two
    assert "sql_parts" in first_two


def test_group_synopsis_uses_command_placeholder():
    p = argparse.ArgumentParser(prog="maxc auth", formatter_class=AliyunStyleFormatter)
    sub = p.add_subparsers(dest="auth_command")
    sub.add_parser("login")
    sub.add_parser("whoami")
    text = strip_ansi(p.format_help())
    first_two = "\n".join(text.splitlines()[:2])
    assert "<command>" in first_two
    assert "[--flags ...]" in first_two


def test_epilog_rendered_as_sample_section():
    p = argparse.ArgumentParser(
        prog="maxc query", formatter_class=AliyunStyleFormatter,
        epilog='Sample:\n  maxc query "SELECT 1"',
    )
    text = strip_ansi(p.format_help())
    assert "Sample:" in text
    assert '  maxc query "SELECT 1"' in text


def test_footer_hint_present_on_subparser(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    p = argparse.ArgumentParser(prog="maxc auth", formatter_class=AliyunStyleFormatter)
    text = p.format_help()
    assert "Use `maxc auth --help` for more information." in text


def test_subcommand_names_colored_when_tty(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    p = argparse.ArgumentParser(prog="maxc auth", formatter_class=AliyunStyleFormatter)
    sub = p.add_subparsers(dest="auth_command")
    sub.add_parser("login", help="Log in")
    text = p.format_help()
    assert "\033[36mlogin\033[0m" in text


def test_footer_hint_absent_on_toplevel(monkeypatch):
    """The root `maxc` parser must not show a redundant 'Use `maxc --help`' footer."""
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    p = argparse.ArgumentParser(prog="maxc", formatter_class=AliyunStyleFormatter)
    text = p.format_help()
    assert "for more information." not in text


def test_single_line_description_wraps_to_width():
    """Single-line descriptions should still get argparse's width wrapping."""
    long_desc = "this is a fairly long single line description " * 4
    p = argparse.ArgumentParser(
        prog="maxc demo",
        formatter_class=AliyunStyleFormatter,
        description=long_desc,
    )
    text = strip_ansi(p.format_help())
    # Width wrap should split it across multiple lines.
    desc_lines = [
        ln for ln in text.splitlines()
        if "fairly long single line description" in ln
    ]
    assert len(desc_lines) >= 2, f"description not wrapped: {desc_lines!r}"


def test_multi_line_description_preserved():
    """Multi-line descriptions (with explicit \\n) must keep their newlines."""
    p = argparse.ArgumentParser(
        prog="maxc demo",
        formatter_class=AliyunStyleFormatter,
        description="line one\nline two\nline three",
    )
    text = strip_ansi(p.format_help())
    # Each of the three input lines must appear as its OWN stripped line —
    # not collapsed onto a shared line. (AliyunStyleFormatter renders the
    # description block between "Usage:" and "Flags:".)
    stripped_lines = [ln.strip() for ln in text.splitlines()]
    for expected in ("line one", "line two", "line three"):
        assert expected in stripped_lines, (
            f"{expected!r} not on its own line in:\n{text}"
        )


def test_top_level_maxc_help_uses_aliyun_style(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    from maxc_cli.cli import build_parser
    text = build_parser().format_help()
    assert text.startswith("Usage:\n  maxc")
    assert "Flags:" in text
    assert "Commands:" in text
    assert "Sample:" in text


def test_auth_login_help_uses_aliyun_style(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    from maxc_cli.cli import build_parser
    p = build_parser()
    sub = next(a for a in p._actions if hasattr(a, "choices") and "auth" in (a.choices or {}))
    auth = sub.choices["auth"]
    login = next(
        a for a in auth._actions if hasattr(a, "choices") and "login" in (a.choices or {})
    ).choices["login"]
    text = login.format_help()
    assert "Usage:" in text
    assert "Flags:" in text
    assert "Sample:" in text
    assert "Use `maxc auth login --help`" in text
