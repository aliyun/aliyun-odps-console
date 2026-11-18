"""Opt-in model evals: skill selection and planning, without executing proposed commands.

Requires OPENAI_API_KEY (OpenAI-compatible endpoint). Run with: pytest -m eval -v
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

SKILL_DIR = Path(__file__).parent.parent / "src" / "maxc_cli" / "skills"
SCENARIO_DIR = Path(__file__).parent / "eval" / "scenarios"

pytestmark = pytest.mark.eval


def _render_document(path: Path) -> str:
    from maxc_cli.app import render_skill_template

    raw = path.read_text(encoding="utf-8")
    return render_skill_template(raw, cli="maxc", cli_module="python3 -m maxc_cli")


def _load_scenarios() -> list[dict[str, Any]]:
    scenarios = []
    for yaml_file in sorted(SCENARIO_DIR.glob("*.yaml")):
        data = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        for s in data.get("scenarios", []):
            s["_source"] = yaml_file.name
            scenarios.append(s)
    return scenarios


def _build_agent_prompt(scenario: dict) -> str:
    parts = [f"User intent: {scenario['description'].strip()}"]
    ctx = scenario.get("context", {})
    if ctx:
        parts.append("\nCurrent environment state:")
        for k, v in ctx.items():
            parts.append(f"  - {k}: {v}")
    parts.append(
        "\nReturn the requested deliverable. If an operation is needed, show the exact "
        "CLI commands you propose; do not fabricate output."
    )
    return "\n".join(parts)


def _build_judge_prompt(scenario: dict, agent_response: str) -> str:
    expected = scenario["expected"]
    parts = ["Evaluate the following agent response against these criteria:\n"]

    if expected.get("required_commands"):
        parts.append("REQUIRED COMMANDS (substrings that must appear in the response):")
        for cmd in expected["required_commands"]:
            parts.append(f"  - {cmd}")

    if expected.get("required_behaviors"):
        parts.append("\nREQUIRED BEHAVIORS (the response must demonstrate these):")
        for b in expected["required_behaviors"]:
            parts.append(f"  - {b}")

    if expected.get("forbidden_patterns"):
        parts.append("\nFORBIDDEN PATTERNS (must not be proposed as actions; warnings against them are fine):")
        for p in expected["forbidden_patterns"]:
            parts.append(f"  - {p}")

    if expected.get("key_assertions"):
        parts.append("\nKEY ASSERTIONS (must hold true about the response):")
        for a in expected["key_assertions"]:
            parts.append(f"  - {a}")

    parts.append(f"\n---\nAGENT RESPONSE:\n{agent_response}\n---")
    parts.append(
        '\nScore 0-10. Output ONLY a JSON object: {"score": <int>, "pass": <bool>, "reasons": ["...", ...]}\n'
        "pass=true if score >= 7. Be strict about forbidden patterns and required commands."
    )
    return "\n".join(parts)


def _call_llm(system: str, user: str, *, model: str, max_tokens: int = 2048) -> str:
    from openai import OpenAI

    client = OpenAI()
    response = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    return response.choices[0].message.content


def _parse_model_json(raw: str) -> dict:
    """Best-effort parse of a model's JSON response."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```\w*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        raw = raw.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Find the outermost { ... } allowing nested brackets
    depth = 0
    start = None
    for i, ch in enumerate(raw):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(raw[start : i + 1])
                except json.JSONDecodeError:
                    pass
    # LLM sometimes mangles closing brackets — try common fixes
    if start is not None:
        fragment = raw[start:]
        # Try appending missing closers
        for suffix in ["}", "]}",  "\"]}"]:
            try:
                return json.loads(fragment + suffix)
            except json.JSONDecodeError:
                continue
        # Common LLM mistake: ]] instead of ]} at end
        if fragment.endswith("]]"):
            try:
                return json.loads(fragment[:-1] + "}")
            except json.JSONDecodeError:
                pass
    raise AssertionError(f"Model returned unparseable JSON: {raw}")


ALL_SCENARIOS = _load_scenarios() if SCENARIO_DIR.exists() else []


@pytest.fixture(scope="session")
def skill_content() -> str:
    return _render_document(SKILL_DIR / "SKILL.md")


@pytest.fixture(scope="session")
def eval_model(request) -> str:
    if not os.environ.get("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY not set")
    return request.config.getoption("--eval-model") or os.environ.get("OPENAI_MODEL", "gpt-4o")


@pytest.mark.parametrize(("intent", "expected"), [
    ("看一下 ODPS 的 orders 表有哪些分区列，但不要读表里的数据。", "USE"),
    ("My MaxCompute job finished. Can you read its second page without running it again?", "USE"),
    ("用 PyODPS SDK 写一段 Python 示例，列出 orders 的字段，不要 CLI 命令。", "SKIP"),
    ("Explain what SQL window functions are using an in-memory SQLite example.", "SKIP"),
    ("Modify the maxc-cli parser source to accept a new argument.", "SKIP"),
])
def test_implicit_selection(intent: str, expected: str, skill_content: str, eval_model: str) -> None:
    card = yaml.safe_load(skill_content.split("---\n", 2)[1])
    decision = _call_llm(
        system=("Decide whether to load this optional skill for the user's request. "
                f"Reply only USE or SKIP.\n{card['name']}: {card['description']}"),
        user=intent, model=eval_model, max_tokens=16,
    )
    assert decision.strip().upper() == expected


@pytest.mark.parametrize(
    "scenario", ALL_SCENARIOS, ids=[s["name"] for s in ALL_SCENARIOS]
)
def test_skill_scenario(scenario: dict, skill_content: str, eval_model: str) -> None:
    intent = _build_agent_prompt(scenario)
    references = {
        path.relative_to(SKILL_DIR).as_posix(): path
        for path in (SKILL_DIR / "references").glob("*.md")
    }
    selected = _parse_model_json(_call_llm(
        system=skill_content,
        user=(intent + '\nBefore planning, choose only linked files needed for this task. '
              'Reply with JSON {"references": ["references/<name>.md"]}, or an empty array in that field.'),
        model=eval_model, max_tokens=256,
    ))["references"]
    assert isinstance(selected, list) and all(isinstance(name, str) and name in references for name in selected)
    documents = [f"## {name}\n{_render_document(references[name])}" for name in dict.fromkeys(selected)]

    agent_response = _call_llm(
        system="\n\n".join([skill_content, *documents]),
        user=intent,
        model=eval_model,
        max_tokens=2048,
    )

    judge_raw = _call_llm(
        system=(
            "You are an expert evaluator for CLI agent instructions. "
            "Judge whether the agent response correctly follows the SKILL guidelines. "
            "Be strict about forbidden patterns and required commands."
        ),
        user=_build_judge_prompt(scenario, agent_response),
        model=eval_model,
        max_tokens=512,
    )

    verdict = _parse_model_json(judge_raw)

    score = verdict["score"]
    passed = verdict.get("pass", score >= 7)
    reasons = verdict.get("reasons", [])

    assert passed, (
        f"Scenario '{scenario['name']}' FAILED (score={score}/10).\n"
        f"Reasons: {reasons}\n"
        f"Agent response (truncated):\n{agent_response[:800]}"
    )
