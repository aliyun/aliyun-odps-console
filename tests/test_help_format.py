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
