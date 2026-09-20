"""Tests for `kb ask` / `kb search`: config section, tool projection, CLI wiring.

The MCP transport itself is covered by ``test_backend_mcp.py``; these pin what only
the command layer can get wrong — that a disabled config fails loudly instead of
returning empty results, that each tool's distinct response shape lands in the
documented envelope keys, and that citations survive to the agent.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.app import MaxCApp
from maxc_cli.config import McpConfig, load_config
from maxc_cli.exceptions import BackendConnectionError, FeatureUnavailableError


class StubMcpClient:
    """Records calls and replays one canned tools/call result."""

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.error:
            raise self.error
        return self.result


def _app(tmp_path, monkeypatch, *, mcp=McpConfig(enabled=False), region="cn-shanghai"):
    """Build an app over a throwaway config so nothing reads the developer's ~/.maxc."""
    monkeypatch.delenv("MAXCOMPUTE_PROJECT", raising=False)
    monkeypatch.delenv("MAXCOMPUTE_ENDPOINT", raising=False)
    monkeypatch.delenv("MAXCOMPUTE_REGION", raising=False)
    monkeypatch.delenv("ALIBABA_CLOUD_REGION", raising=False)
    monkeypatch.delenv("ODPS_PROJECT", raising=False)
    monkeypatch.delenv("ODPS_ENDPOINT", raising=False)
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "default_project: probe_proj\n"
        f"default_region: {region}\n"
        "auth:\n"
        "  access_id: probe-id\n"
        "  secret_access_key: probe-secret\n"
        "  endpoint: http://service.cn-shanghai.maxcompute.aliyun.com/api\n"
        + ("mcp:\n  enabled: true\n" if mcp.enabled else "")
        + (f"mcp:\n  endpoint: {mcp.endpoint}\n" if mcp.endpoint else ""),
        encoding="utf-8",
    )
    app = MaxCApp(cwd=tmp_path, config_path=config_file, load_backend=False)
    app.config.mcp = mcp
    return app


def _ask_structured():
    return {
        "ok": True,
        "data": {"query": "split size hint", "answer": "Set odps.sql.split.size via a hint."},
        "citations": [
            {"title": "Split Size Hint", "uri": "https://help.aliyun.com/zh/maxcompute/user-guide/split-size-hint"},
        ],
        "has_more": False,
        "next_cursor": None,
        "request_id": "req-1",
        "warnings": [],
    }


def _search_structured():
    return {
        "ok": True,
        "data": {
            "package": "maxcompute_kb",
            "query": "split size hint",
            "results": [
                {
                    "title": "Split Size Hint",
                    "score": 0.96,
                    "snippet": "truncated excerpt...[truncated]",
                    "section_headings": ["使用场景"],
                    "source": {"uri": "https://help.aliyun.com/zh/maxcompute/user-guide/split-size-hint"},
                },
            ],
        },
        "has_more": True,
        "next_cursor": "cursor-2",
        "request_id": "req-2",
        "warnings": ["rerank degraded"],
    }


def _wrap(structured):
    return {"content": [{"type": "text", "text": "..."}], "structuredContent": structured}


# ── config ─────────────────────────────────────────────────────────────────


def test_mcp_section_defaults_to_disabled() -> None:
    assert McpConfig.from_mapping(None).enabled is False
    assert McpConfig.from_mapping({}).endpoint is None
    assert McpConfig.from_mapping({}).timeout_seconds == 90


@pytest.mark.parametrize("raw", [{"timeout_seconds": 0}, {"timeout_seconds": -1}, {"timeout_seconds": "abc"}])
def test_mcp_rejects_bad_timeout(raw) -> None:
    from maxc_cli.exceptions import ValidationError

    with pytest.raises(ValidationError):
        McpConfig.from_mapping(raw)


def test_mcp_round_trips_through_yaml(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "default_project: p\n"
        "mcp:\n"
        "  enabled: true\n"
        "  endpoint: https://mcp.example.com/mcp\n"
        "  timeout_seconds: 30\n",
        encoding="utf-8",
    )
    config = load_config(tmp_path, config_file)
    assert config.mcp.enabled is True
    assert config.mcp.endpoint == "https://mcp.example.com/mcp"
    assert config.mcp.timeout_seconds == 30


def test_non_mapping_mcp_section_is_rejected(tmp_path) -> None:
    from maxc_cli.exceptions import ValidationError

    config_file = tmp_path / "config.yaml"
    config_file.write_text("default_project: p\nmcp: yes-please\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="must be a mapping"):
        load_config(tmp_path, config_file)


# ── gating ─────────────────────────────────────────────────────────────────


def test_disabled_mcp_fails_loudly_rather_than_returning_nothing(tmp_path, monkeypatch) -> None:
    """An empty success would read as "the docs have no answer" — a false conclusion."""
    app = _app(tmp_path, monkeypatch)
    with pytest.raises(FeatureUnavailableError) as excinfo:
        app.kb_ask("anything")
    assert "mcp.enabled" in str(excinfo.value.suggestion)
    assert excinfo.value.recoverable is False


def test_missing_backend_reports_feature_unavailable(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    app.backend = None
    with pytest.raises(FeatureUnavailableError):
        app.kb_search("anything")


# ── argument building ──────────────────────────────────────────────────────


def test_kb_ask_forwards_question_and_region_default(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    stub = StubMcpClient(_wrap(_ask_structured()))
    monkeypatch.setattr(app, "_mcp_client", lambda: stub)

    app.kb_ask("does MAX_PT need a partition filter?")

    name, arguments = stub.calls[0]
    assert name == "maxcompute_kb_ask"
    # The tool's own argument name is `question`, not `query`.
    assert arguments["question"] == "does MAX_PT need a partition filter?"
    assert arguments["region"] == "cn-shanghai"
    assert "max_docs" not in arguments, "unset optional must not be sent"


def test_kb_ask_omits_region_when_unknown(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True), region="")
    app.config.default_region = ""
    stub = StubMcpClient(_wrap(_ask_structured()))
    monkeypatch.setattr(app, "_mcp_client", lambda: stub)

    app.kb_ask("x")

    assert "region" not in stub.calls[0][1]


def test_kb_search_maps_context_lines_to_both_sides(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    stub = StubMcpClient(_wrap(_search_structured()))
    monkeypatch.setattr(app, "_mcp_client", lambda: stub)

    app.kb_search("split size", limit=3, context_lines=2)

    _, arguments = stub.calls[0]
    assert arguments == {
        "query": "split size",
        "limit": 3,
        "before_lines": 2,
        "after_lines": 2,
        "region": "cn-shanghai",
    }


def test_kb_search_zero_context_lines_is_still_sent(tmp_path, monkeypatch) -> None:
    """--context-lines 0 is a deliberate request for no context, not an absent flag."""
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    stub = StubMcpClient(_wrap(_search_structured()))
    monkeypatch.setattr(app, "_mcp_client", lambda: stub)

    app.kb_search("x", context_lines=0)

    assert stub.calls[0][1]["before_lines"] == 0


def test_explicit_region_overrides_config(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    stub = StubMcpClient(_wrap(_ask_structured()))
    monkeypatch.setattr(app, "_mcp_client", lambda: stub)

    app.kb_ask("x", region="ap-southeast-1")

    assert stub.calls[0][1]["region"] == "ap-southeast-1"


# ── projection: the two tools differ, so both are pinned ───────────────────


def test_kb_ask_exposes_citations_at_top_level(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(_ask_structured())))

    payload = app.kb_ask("split size hint").to_dict()

    assert payload["status"] == "success"
    assert payload["command"] == "kb ask"
    assert payload["data"]["answer"]["text"].startswith("Set odps.sql")
    assert payload["data"]["citations"][0]["uri"].startswith("https://help.aliyun.com/")
    assert payload["data"]["request_id"] == "req-1"


def test_kb_search_keeps_score_headings_and_nested_uri(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(_search_structured())))

    payload = app.kb_search("split size hint").to_dict()

    search = payload["data"]["search"]
    assert search["package"] == "maxcompute_kb"
    match = search["matches"][0]
    assert match["uri"] == "https://help.aliyun.com/zh/maxcompute/user-guide/split-size-hint"
    assert match["score"] == 0.96
    assert match["section_headings"] == ["使用场景"]
    assert payload["data"]["pagination"] == {"has_more": True, "next_cursor": "cursor-2"}


def test_server_warnings_are_surfaced_not_swallowed(tmp_path, monkeypatch) -> None:
    """Retrieval can degrade while still answering 200; silence would hide it."""
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(_search_structured())))

    hints = app.kb_search("x").to_dict()["agent_hints"]

    assert "rerank degraded" in hints["warnings"]


def test_ok_false_adds_a_warning(tmp_path, monkeypatch) -> None:
    structured = _ask_structured()
    structured["ok"] = False
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(structured)))

    warnings = app.kb_ask("x").to_dict()["agent_hints"]["warnings"]

    assert any("ok=false" in text for text in warnings)


def test_ask_carries_an_anti_hallucination_insight(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(_ask_structured())))

    insights = " ".join(app.kb_ask("x").to_dict()["agent_hints"]["insights"])

    assert "model-generated" in insights
    assert "not evidence" in insights


def test_follow_up_action_is_executable(tmp_path, monkeypatch) -> None:
    """A templated action with placeholders is useless mid-session; the echoed query fills it."""
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(_ask_structured())))

    action = app.kb_ask("split size hint").to_dict()["agent_hints"]["actions"][0]

    assert action["id"] == "kb.search"
    assert action["executable"] is True
    assert "split size hint" in action["command"]


def test_missing_structured_content_is_a_failure(tmp_path, monkeypatch) -> None:
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient({"content": []}))

    with pytest.raises(BackendConnectionError):
        app.kb_ask("x")


def test_tool_level_error_does_not_read_as_success(tmp_path, monkeypatch) -> None:
    """isError arrives inside a 200 JSON-RPC result; ignoring it yields an empty 'success'."""
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    failing = {
        "content": [{"type": "text", "text": "embedding backend unavailable"}],
        "isError": True,
        "structuredContent": {"ok": False},
    }
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(failing))

    with pytest.raises(BackendConnectionError) as excinfo:
        app.kb_ask("x")
    assert "embedding backend unavailable" in str(excinfo.value)


def test_transport_error_advises_against_negative_inference(tmp_path, monkeypatch) -> None:
    from maxc_cli.backend.mcp import McpRequestError

    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(
        app, "_mcp_client", lambda: StubMcpClient(error=McpRequestError("boom", status=503))
    )

    with pytest.raises(BackendConnectionError) as excinfo:
        app.kb_search("x")
    assert "documentation lacks an answer" in str(excinfo.value.suggestion)


def test_empty_results_stay_successful_but_flagged(tmp_path, monkeypatch) -> None:
    structured = _search_structured()
    structured["data"]["results"] = []
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(structured)))

    payload = app.kb_search("obscure thing").to_dict()

    assert payload["status"] == "success"
    assert payload["data"]["search"]["matches"] == []


# ── agent.context capability reporting ─────────────────────────────────────


def test_agent_context_reports_knowledge_base_capability(tmp_path, monkeypatch) -> None:
    """Agents read this to decide whether kb is available; it must not require a probe."""
    app = _app(tmp_path, monkeypatch)
    assert app.agent_context().data["capabilities"]["knowledge_base"] is False

    app.config.mcp = McpConfig(enabled=True)
    assert app.agent_context().data["capabilities"]["knowledge_base"] is True


# ── CLI surface ────────────────────────────────────────────────────────────


def test_kb_commands_appear_in_manifest() -> None:
    from maxc_cli.cli import _command_manifest, build_parser

    manifest = _command_manifest(build_parser())
    by_command = {entry["command"]: entry for entry in manifest["commands"]}

    assert {"kb.ask", "kb.search"} <= set(by_command)
    for name in ("kb.ask", "kb.search"):
        entry = by_command[name]
        assert entry["requirements"]["credentials"]["mode"] == "required"
        assert entry["supports_json"] is True
        targets = {effect["target"] for effect in entry["effects"]}
        assert "maxcompute_knowledge_base" in targets
        assert "catalog_mcp_access_token" in targets, "token mint is a remote effect agents must see"


def test_kb_positional_argument_is_required() -> None:
    from maxc_cli.cli import build_parser

    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["kb", "ask"])
    with pytest.raises(SystemExit):
        parser.parse_args(["kb", "search"])


def test_kb_limit_rejects_non_positive() -> None:
    from maxc_cli.cli import build_parser

    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["kb", "search", "x", "--limit", "0"])


def test_kb_flags_match_tool_vocabulary() -> None:
    """Flag names follow the tool's parameters rather than inventing a third vocabulary."""
    from maxc_cli.cli import build_parser

    parser = build_parser()
    ask = parser.parse_args(["kb", "ask", "q", "--max-docs", "3", "--region", "cn-beijing"])
    assert (ask.question, ask.max_docs, ask.region) == ("q", 3, "cn-beijing")

    search = parser.parse_args(["kb", "search", "q", "--limit", "2", "--context-lines", "1"])
    assert (search.query, search.limit, search.context_lines) == ("q", 2, 1)


# ── rendering: an agent must be able to see the URI in every format ────────


def _envelope(tmp_path, monkeypatch, method, **kwargs):
    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    structured = _ask_structured() if method == "kb_ask" else _search_structured()
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(structured)))
    return getattr(app, method)(**kwargs)


def test_markdown_render_keeps_answer_and_links(tmp_path, monkeypatch) -> None:
    from maxc_cli.output import render_markdown

    rendered = render_markdown(_envelope(tmp_path, monkeypatch, "kb_ask", question="split size hint"))

    assert "## Answer" in rendered
    assert "[Split Size Hint](https://help.aliyun.com/zh/maxcompute/user-guide/split-size-hint)" in rendered


def test_table_render_tabulates_citations_not_json_blobs(tmp_path, monkeypatch) -> None:
    from maxc_cli.cli import _render_human

    rendered = _render_human(_envelope(tmp_path, monkeypatch, "kb_search", query="split size hint"))

    assert "| title" in rendered
    assert "help.aliyun.com" in rendered
    assert '"section_headings"' not in rendered, "nested objects must not leak as JSON cells"


def test_empty_search_says_so_without_claiming_absence(tmp_path, monkeypatch) -> None:
    """Wording matters: 'not found' would be read as 'does not exist'."""
    from maxc_cli.cli import _render_human

    app = _app(tmp_path, monkeypatch, mcp=McpConfig(enabled=True))
    structured = _search_structured()
    structured["data"]["results"] = []
    monkeypatch.setattr(app, "_mcp_client", lambda: StubMcpClient(_wrap(structured)))

    rendered = _render_human(app.kb_search("obscure"))

    assert "not evidence" in rendered
