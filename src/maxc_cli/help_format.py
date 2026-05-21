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
        "options": "Flags",              # Python 3.10+
        "optional arguments": "Flags",   # Python 3.9
    }

    def start_section(self, heading):
        if heading in self._SECTION_REMAP:
            heading = self._SECTION_REMAP[heading]
        super().start_section(heading)

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = "Usage:\n  "
        super().add_usage(usage, actions, groups, prefix)

    def _format_usage(self, usage, actions, groups, prefix):
        if usage is not None:
            return super()._format_usage(usage, actions, groups, prefix)
        has_subparsers = any(isinstance(a, argparse._SubParsersAction) for a in actions)
        has_flags = any(
            a.option_strings and a.help is not argparse.SUPPRESS
            for a in actions
        )
        positional_parts = [
            a.metavar or a.dest
            for a in actions
            if not a.option_strings
            and not isinstance(a, argparse._SubParsersAction)
            and a.help is not argparse.SUPPRESS
        ]
        parts = [self._prog]
        if has_subparsers:
            parts.append("<command>")
        parts.extend(positional_parts)
        if has_flags:
            parts.append("[--flags ...]")
        line = " ".join(parts)
        return f"{prefix}{line}\n\n"
