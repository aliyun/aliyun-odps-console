import pytest

pytestmark = pytest.mark.unit

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
