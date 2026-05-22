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

    def _fill_text(self, text, width, indent):
        # Preserve sample/epilog formatting (newlines, leading indent) only for
        # pre-formatted multi-line text. Single-line strings fall through to
        # argparse's default ``textwrap.fill`` so long descriptions still wrap.
        if "\n" in text:
            return "".join(indent + line for line in text.splitlines(keepends=True))
        return super()._fill_text(text, width, indent)

    def format_help(self):
        text = super().format_help()
        if not text.endswith("\n"):
            text += "\n"
        # Skip the footer on the top-level parser (matches aliyun behavior).
        # Subparsers always render as ``"<prog> <group>..."`` with spaces; the
        # root prog is bare (no spaces), so this check is durable across
        # entry-point renames.
        if " " not in self._prog:
            return text
        # Skip the footer when argparse calls format_help() internally just to
        # derive a subparser's prog (``add_subparsers`` adds only the usage
        # section then calls format_help().strip()). The root section contains
        # exactly one usage item in that case; for a real --help invocation it
        # contains usage + the action groups (Flags, Commands, ...).
        if len(self._root_section.items) <= 1:
            return text
        text += f"\nUse `{self._prog} --help` for more information.\n"
        return text

    def _format_action(self, action):
        text = super()._format_action(action)
        if isinstance(action, argparse._SubParsersAction):
            for choice in action.choices:
                # Anchor to line start: argparse renders subcommand rows as
                # ``"    <choice>  <help>"`` (leading whitespace varies with
                # nesting). Plain ``str.replace`` would match the first
                # occurrence anywhere in the rendered block (e.g. inside
                # ``{login}`` brace lists or inside a help string).
                text = re.sub(
                    rf"(?m)^(\s+){re.escape(choice)}(?=\s)",
                    rf"\1{cyan(choice)}",
                    text,
                    count=1,
                )
        return text


class AliyunRawTextFormatter(AliyunStyleFormatter, argparse.RawTextHelpFormatter):
    """Aliyun style + RawText description handling (for parsers with hand-formatted descriptions)."""
    pass
