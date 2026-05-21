"""Custom argparse HelpFormatter that mimics aliyun CLI's help layout.

Color is gated on ``sys.stdout.isatty()``: aliyun emits ANSI unconditionally,
which pollutes redirected output. Keeping pipes clean matches git/kubectl/gh
and our own --json envelope consumers.
"""
from __future__ import annotations

import argparse
import re
import sys


_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def _isatty() -> bool:
    return bool(getattr(sys.stdout, "isatty", lambda: False)())


def cyan(text: str) -> str:
    return f"\033[36m{text}\033[0m" if _isatty() else text


def green(text: str) -> str:
    return f"\033[32m{text}\033[0m" if _isatty() else text


def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


class AliyunStyleFormatter(argparse.HelpFormatter):
    """Aliyun-style help: ``Usage:`` / ``Flags:`` / ``Commands:`` headings.

    Compact synopsis and Sample-epilog handling are added in Tasks 3-4.
    """

    _SECTION_REMAP = {
        "positional arguments": "Commands",
        "options": "Flags",
    }

    def start_section(self, heading):
        if heading in self._SECTION_REMAP:
            heading = self._SECTION_REMAP[heading]
        super().start_section(heading)

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = "Usage:\n  "
        super().add_usage(usage, actions, groups, prefix)
