"""Check what consumers can actually open and parse after skill installation."""
from __future__ import annotations

import re
import shlex
from itertools import takewhile

import pytest
import yaml

from maxc_cli.app import MaxCApp
from maxc_cli.cli import _hoist_global_flags, build_parser

SHELL_FENCE = re.compile(r"```(?:bash|shell|sh|console)\n(.*?)\n```", re.DOTALL)
LOCAL_LINK = re.compile(r"\[[^\]\n]*\]\((?!https?://)([^#\s)]+)(?:#[^)]*)?\)")


@pytest.fixture(params=["maxc", "aliyun-maxc"])
def installed_skill(request, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    target = tmp_path / "alibabacloud-maxcompute-cli"
    result = MaxCApp(cwd=tmp_path, load_backend=False).skill_install(
        platform="cursor", invocation=request.param, dir_override=target,
    )
    assert result.status == "success"
    return target


def test_installed_skill_is_discoverable_and_routes_to_every_reference(installed_skill):
    entry = (installed_skill / "SKILL.md").read_text(encoding="utf-8")
    frontmatter = yaml.safe_load(entry.split("---\n", 2)[1])
    assert frontmatter["name"] == installed_skill.name
    assert 0 < len(frontmatter["description"]) <= 1024
    references = {
        path.relative_to(installed_skill).as_posix()
        for path in (installed_skill / "references").glob("*.md")
    }
    assert references and references <= set(LOCAL_LINK.findall(entry))
    for doc in installed_skill.rglob("*.md"):
        text = doc.read_text(encoding="utf-8")
        assert "agent install-skill" not in text
        for link in LOCAL_LINK.findall(text):
            resolved = (doc.parent / link).resolve()
            assert installed_skill.resolve() in resolved.parents and resolved.is_file(), (doc, link)


def _example_commands(text):
    for block in SHELL_FENCE.findall(text):
        for line in block.replace("\\\n", " ").splitlines():
            if not re.match(r"^\s*(?:maxc\s|aliyun\s+maxc\s|[A-Z_]+=)", line):
                continue
            words = shlex.split(line, comments=True)
            while words and re.match(r"^[A-Z_]+=", words[0]):
                words.pop(0)
            if words[:2] == ["aliyun", "maxc"]:
                words = words[1:]
            if words[:1] == ["maxc"]:
                yield list(takewhile(lambda word: word not in {"|", "||", "&&", ">"}, words[1:]))


def test_rendered_shell_examples_match_the_live_parser(installed_skill):
    parser = build_parser()
    seen = set()
    for doc in installed_skill.rglob("*.md"):
        for argv in _example_commands(doc.read_text(encoding="utf-8")):
            try:
                parser.parse_args(_hoist_global_flags(argv))
            except SystemExit as exc:
                assert exc.code == 0 and {"--help", "--version"}.intersection(argv), (doc.name, argv)
            seen.add(tuple(argv[:2]))
    assert {("agent", "manifest"), ("meta", "describe"), ("job", "result"), ("data", "upload")} <= seen
