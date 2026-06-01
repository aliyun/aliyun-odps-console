"""Custom argparse HelpFormatter that mimics aliyun CLI's help layout.

Color is gated on ``sys.stdout.isatty()``: aliyun emits ANSI unconditionally,
which pollutes redirected output. Keeping pipes clean matches git/kubectl/gh
and our own --json envelope consumers. ``NO_COLOR=1`` (per no-color.org) also
disables color regardless of TTY status.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def _color_enabled() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    return bool(getattr(sys.stdout, "isatty", lambda: False)())


def _isatty() -> bool:
    return _color_enabled()


def cyan(text: str) -> str:
    return f"\033[36m{text}\033[0m" if _color_enabled() else text


def green(text: str) -> str:
    return f"\033[32m{text}\033[0m" if _color_enabled() else text


def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


class AliyunStyleFormatter(argparse.HelpFormatter):
    """Aliyun CLI style: version header, Commands, Flags (long first, no-space comma)."""

    _has_subparsers = False

    _SECTION_REMAP = {
        "options": "Flags",              # Python 3.10+
        "optional arguments": "Flags",   # Python 3.9
    }

    def start_section(self, heading):
        if heading in self._SECTION_REMAP:
            heading = self._SECTION_REMAP[heading]
        # Rename "positional arguments" dynamically: "Commands" when a
        # subparsers action exists, "Arguments" otherwise.
        if heading == "positional arguments":
            heading = "Commands" if self._has_subparsers else "Arguments"
        super().start_section(heading)

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = "Usage:\n  "
        self._has_subparsers = any(
            isinstance(a, argparse._SubParsersAction) for a in (actions or [])
        )
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

    def _format_action_invocation(self, action):
        if not action.option_strings:
            # Positional with choices: show dest name, not {choice1,choice2,...}
            if action.choices and not isinstance(action, argparse._SubParsersAction):
                return action.dest
            return super()._format_action_invocation(action)
        # Long option first, short after, comma with no space
        opts = sorted(action.option_strings, key=lambda s: (not s.startswith('--'), s))
        return ','.join(opts)

    def _format_action(self, action):
        if isinstance(action, argparse._SubParsersAction):
            # Skip the "{choices}" summary line, render only the sub-commands
            parts = []
            for sub_action in action._get_subactions():
                parts.append(super()._format_action(sub_action))
            text = ''.join(parts)
            # Colorize command names
            for choice in action.choices:
                text = re.sub(
                    rf"(?m)^(\s+){re.escape(choice)}(?=\s)",
                    rf"\1{cyan(choice)}",
                    text,
                    count=1,
                )
            return text
        return super()._format_action(action)

    def _fill_text(self, text, width, indent):
        if "\n" in text:
            return "".join(indent + line for line in text.splitlines(keepends=True))
        return super()._fill_text(text, width, indent)

    def format_help(self):
        text = super().format_help()
        if not text.endswith("\n"):
            text += "\n"
        # Inject version header at top for root parser only
        if " " not in self._prog:
            from maxc_cli import __version__
            header = f"MaxCompute CLI {__version__}\n\n"
            text = header + text
        return text


class AliyunRawTextFormatter(AliyunStyleFormatter, argparse.RawTextHelpFormatter):
    """Aliyun style + RawText description handling (for parsers with hand-formatted descriptions)."""
    pass
