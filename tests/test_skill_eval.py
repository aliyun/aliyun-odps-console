"""Scenario-based eval: SKILL.md guides an LLM to produce correct CLI commands.

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


def _load_skill_content() -> str:
    from maxc_cli.app import render_skill_template

    raw = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
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
        "\nBased on the SKILL instructions, what commands would you run "
        "and what would your approach be? Show the exact CLI commands."
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
        parts.append("\nFORBIDDEN PATTERNS (must NOT appear in the response):")
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


def _parse_judge_json(raw: str) -> dict:
    """Best-effort parse of the judge's JSON response."""
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
    raise AssertionError(f"Judge returned unparseable JSON: {raw}")


ALL_SCENARIOS = _load_scenarios() if SCENARIO_DIR.exists() else []


@pytest.fixture(scope="session")
def skill_content() -> str:
    return _load_skill_content()


@pytest.mark.parametrize(
    "scenario", ALL_SCENARIOS, ids=[s["name"] for s in ALL_SCENARIOS]
)
def test_skill_scenario(scenario: dict, skill_content: str, request) -> None:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY not set")

    model = (
        request.config.getoption("--eval-model")
        or os.environ.get("OPENAI_MODEL", "gpt-4o")
    )

    agent_response = _call_llm(
        system=skill_content,
        user=_build_agent_prompt(scenario),
        model=model,
        max_tokens=2048,
    )

    judge_raw = _call_llm(
        system=(
            "You are an expert evaluator for CLI agent instructions. "
            "Judge whether the agent response correctly follows the SKILL guidelines. "
            "Be strict about forbidden patterns and required commands."
        ),
        user=_build_judge_prompt(scenario, agent_response),
        model=model,
        max_tokens=512,
    )

    verdict = _parse_judge_json(judge_raw)

    score = verdict["score"]
    passed = verdict.get("pass", score >= 7)
    reasons = verdict.get("reasons", [])

    assert passed, (
        f"Scenario '{scenario['name']}' FAILED (score={score}/10).\n"
        f"Reasons: {reasons}\n"
        f"Agent response (truncated):\n{agent_response[:800]}"
    )
