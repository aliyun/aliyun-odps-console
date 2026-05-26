"""Lint: every CLI verb mentioned in skills/**.md must exist in argparse.

If a SKILL doc references `maxc qeury` (typo) or a renamed verb, this fails
loudly — so we catch SKILL/CLI drift at PR time, not in production.
"""
from __future__ import annotations

import re
from pathlib import Path

from maxc_cli.cli import build_parser

SKILL_DIR = Path(__file__).parent.parent / "src" / "maxc_cli" / "skills"
SHELL_FENCE = re.compile(r"```(?:bash|shell|sh|console)\n(.*?)\n```", re.DOTALL)
CLI_CALL = re.compile(
    r"\b(?:\{\{cli\}\}|maxc|aliyun\s+maxc)\s+"
    r"([a-z][a-z0-9-]*(?:\s+[a-z][a-z0-9-]*)?)"
)

# Each whitelist entry must include a comment explaining why it's NOT a real
# subcommand. Empty by default — populate only with proven false positives.
#
# Example shape:
#   "query install",  # appears in references/installation.md as prose describing
#                     # the install flow, not a literal command (re-check before adding).
WHITELIST: set[str] = set()


def _collect_subcommand_pairs(parser) -> set[str]:
    """Walk argparse subparsers and emit {"group", "group sub"} strings.

    Only descends into `_SubParsersAction` — other actions with `choices=`
    (e.g. `--format json|markdown|brief`) hold plain lists and aren't subparsers.
    """
    import argparse
    out: set[str] = set()
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, sub in action.choices.items():
            if name in {"-h", "--help"}:
                continue
            out.add(name)
            sub_pairs = _collect_subcommand_pairs(sub)
            for sp in sub_pairs:
                out.add(f"{name} {sp}")
    return out


def test_skill_references_only_real_subcommands():
    parser = build_parser()
    known = _collect_subcommand_pairs(parser)
    referenced: set[str] = set()
    for md in SKILL_DIR.rglob("*.md"):
        text = md.read_text(encoding="utf-8")
        for block in SHELL_FENCE.findall(text):
            for m in CLI_CALL.finditer(block):
                referenced.add(m.group(1).strip())
    leaked = referenced - known - WHITELIST
    assert not leaked, (
        f"SKILL docs reference unknown CLI verbs: {sorted(leaked)}.\n"
        f"Either fix the doc, or add to WHITELIST with a comment if it's a "
        f"genuine prose example (not a runnable command)."
    )


def test_no_residual_install_skill_in_skill_docs():
    """PR #2 deletion safety — SKILL docs must use `agent skill install`,
    not the deprecated `agent install-skill`."""
    for md in SKILL_DIR.rglob("*.md"):
        text = md.read_text(encoding="utf-8")
        assert "agent install-skill" not in text, (
            f"{md} still references the deprecated `agent install-skill` form. "
            f"Replace with `agent skill install`."
        )
