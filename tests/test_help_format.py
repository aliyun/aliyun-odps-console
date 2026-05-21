import pytest

pytestmark = pytest.mark.unit

import argparse

from maxc_cli.help_format import strip_ansi, cyan, green, AliyunStyleFormatter


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
