"""Package edits must preserve content, reject stale plans and verify writes."""
import copy
import json
import os
from io import StringIO
from types import SimpleNamespace

import pytest

from maxc_cli.app import MaxCApp
from maxc_cli.cli import _command_manifest, build_parser, run
from maxc_cli.exceptions import ValidationError
from maxc_cli.semantic import SemanticError, read_json, write_json
from maxc_cli.semantic_management import SemanticManager

pytestmark = pytest.mark.unit


class Catalog:
    def __init__(self):
        self.spec = {"specName": "sales", "specId": "spec_1", "description": "sales", "tags": [], "dataScope": {"dataReferences": [{"project": "p", "schema": "s", "table": "t", "tableId": "server-id"}]}, "draft": {}}
        self.history = {}
        self.calls = []
        self.number = 0
        self.fail_readback = False
        self.clear_by_omission = False

    def semantic_endpoint(self):
        return "https://catalog.example.test/api"

    def semantic_request(self, namespace, method, name=None, *, suffix="", params=None, body=None):
        self.calls.append((method, name, suffix, copy.deepcopy(params), copy.deepcopy(body)))
        if method == "GET":
            if self.fail_readback:
                raise SemanticError("SEMANTIC_CONNECTION_ERROR", "unavailable")
            if suffix.startswith("/publishedRevisions/"):
                return copy.deepcopy(self.history[suffix.rsplit("/", 1)[1]])
            if suffix == "/publishedRevisions":
                return {"publishedRevisions": [{"revisionId": key} for key in self.history]}
            if name is None:
                return {"semanticSpecs": [self.spec], "nextPageToken": "page_2"}
            return copy.deepcopy(self.spec)
        if method == "PATCH":
            replacement = body.get("draft", {}).get("userDraft", {}).get("content")
            if replacement is not None:
                old = self.spec["draft"].get("userDraft", {})
                if params.get("expectedDraftRevisionId") != old.get("revisionId"):
                    raise SemanticError("SEMANTIC_REVISION_CONFLICT", "stale")
                self.number += 1
                content = {**old.get("content", {}), **copy.deepcopy(replacement)}
                if self.clear_by_omission:
                    content = {k: v for k, v in content.items() if v != []}
                self.spec["draft"]["userDraft"] = {"revisionId": f"r{self.number}", "content": content}
            for key in ("description", "tags", "dataScope"):
                if key in body:
                    self.spec[key] = copy.deepcopy(body[key])
            return copy.deepcopy(self.spec)
        if suffix == ":publish":
            assert body["source"] == "USER_DRAFT"
            draft = self.spec["draft"]["userDraft"]
            assert body["expectedRevisionId"] == draft["revisionId"]
            revision = f"published_{len(self.history) + 1}"
            self.spec["published"] = {"revisionId": revision, "sourceRevisionId": draft["revisionId"], "source": "USER_DRAFT", "content": copy.deepcopy(draft["content"])}
            self.history[revision] = copy.deepcopy(self.spec)
            return copy.deepcopy(self.spec)
        if method == "DELETE":
            if params["expectedSemanticSpecId"] != self.spec["specId"]:
                raise SemanticError("SEMANTIC_REVISION_CONFLICT", "recreated")
            return {"deleted": True}
        if method == "POST":
            assert "specName" in body
            return copy.deepcopy(self.spec)
        raise AssertionError((method, suffix))


@pytest.fixture
def package(tmp_path):
    backend = Catalog()
    manager = SemanticManager(backend, "123456")
    path = tmp_path / "sales.json"
    manager.execute("export", name="sales", output=str(path))
    return backend, manager, path


def save(path, document):
    path.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")


def apply(manager, path):
    plan = manager.execute("diff", name="sales", file=str(path))
    result = manager.execute("apply", name="sales", file=str(path), plan_digest=plan["planDigest"], yes=True)
    return plan, result


def test_roundtrip_first_draft_publish_and_immutable_export(package, tmp_path):
    backend, manager, path = package
    doc = read_json(str(path))
    assert doc["revisionId"] is None
    assert "tableId" not in doc["content"]["dataReferences"][0]
    doc["content"].update({"semanticModel": {"logicalTables": [{"name": "orders", "baseTable": "p.s.t", "timeDimensions": [{"name": "time", "expr": "time", "dataType": "TIMESTAMP_NTZ", "futureField": {"keep": True}}]}], "namedFilters": [], "approvedRelationships": []}, "glossary": [{"term": "销售额", "definition": "完整内容"}]})
    save(path, doc)
    plan, result = apply(manager, path)
    assert result["revisionId"] == "r1"
    patch = [call for call in backend.calls if call[0] == "PATCH"][-1]
    assert "expectedDraftRevisionId" not in patch[3]
    assert "draft.userDraft.content.dataReferences" in plan["updateMask"]
    published = manager.execute("publish", name="sales", expected_spec_id="spec_1", expected_revision="r1", yes=True)
    assert published["publishedRevisionId"] == "published_1"
    target = tmp_path / "published.json"
    manager.execute("export", name="sales", source="PUBLISHED", revision="published_1", output=str(target))
    assert read_json(str(target))["content"] == doc["content"]
    assert manager.execute("delete", name="sales", expected_spec_id="spec_1", yes=True)["deleted"]


def test_existing_draft_preserves_unedited_sections_and_nested_unknowns(package):
    backend, manager, path = package
    doc = read_json(str(path))
    doc["content"]["semanticModel"] = {"logicalTables": [], "namedFilters": [], "approvedRelationships": [], "futureExtension": {"a": 1}}
    doc["content"]["glossary"] = [{"term": "old", "definition": "meaning"}]
    save(path, doc)
    apply(manager, path)
    manager.execute("export", name="sales", output=str(path), overwrite=True)
    doc = read_json(str(path))
    doc["content"]["glossary"].append({"term": "new", "definition": "第二条"})
    save(path, doc)
    plan, result = apply(manager, path)
    assert plan["updateMask"] == ["draft.userDraft.content.glossary"]
    assert result["revisionId"] == "r2"
    assert backend.spec["draft"]["userDraft"]["content"]["semanticModel"]["futureExtension"] == {"a": 1}
    assert [c for c in backend.calls if c[0] == "PATCH"][-1][3]["expectedDraftRevisionId"] == "r1"


@pytest.mark.parametrize("omit", [False, True])
def test_empty_array_clears_but_omission_preserves(package, omit):
    backend, manager, path = package
    doc = read_json(str(path))
    doc["content"]["glossary"] = [{"term": "x", "definition": "y"}]
    save(path, doc)
    apply(manager, path)
    manager.execute("export", name="sales", output=str(path), overwrite=True)
    doc = read_json(str(path))
    del doc["content"]["glossary"]
    save(path, doc)
    assert manager.execute("diff", name="sales", file=str(path))["updateMask"] == []
    doc["content"]["glossary"] = []
    save(path, doc)
    backend.clear_by_omission = omit
    apply(manager, path)
    assert not backend.spec["draft"]["userDraft"]["content"].get("glossary")


@pytest.mark.parametrize("change", ["file", "revision", "identity", "endpoint", "metadata"])
def test_stale_plan_cannot_write(package, change):
    backend, manager, path = package
    plan = manager.execute("diff", name="sales", file=str(path))
    if change == "file":
        doc = read_json(str(path))
        doc["content"]["glossary"] = []
        save(path, doc)
    elif change == "revision":
        backend.spec["draft"]["userDraft"] = {"revisionId": "other", "content": {"dataReferences": [{"project": "p", "table": "t"}]}}
    elif change == "identity":
        backend.spec["specId"] = "new_spec"
    elif change == "endpoint":
        backend.semantic_endpoint = lambda: "https://other-region.test/api"
    else:
        backend.spec["description"] = "concurrent change"
    with pytest.raises(SemanticError):
        manager.execute("apply", name="sales", file=str(path), plan_digest=plan["planDigest"], yes=True)
    assert not [c for c in backend.calls if c[0] != "GET"]


def test_metadata_only_plan_warns_about_no_cas(package):
    backend, manager, path = package
    apply(manager, path)
    manager.execute("export", name="sales", output=str(path), overwrite=True)
    doc = read_json(str(path))
    doc["object"]["tags"] = ["reviewed"]
    save(path, doc)
    plan, result = apply(manager, path)
    assert plan["updateMask"] == ["tags"]
    assert "expectedDraftRevisionId" not in [c for c in backend.calls if c[0] == "PATCH"][-1][3]
    assert result["revisionId"] == "r1"
    assert any("no server-side CAS" in w for w in manager.warnings)


@pytest.mark.parametrize("operation", ["create", "apply", "publish", "delete"])
def test_confirmation_required_before_mutation(package, operation):
    backend, manager, path = package
    with pytest.raises(ValidationError):
        manager.execute(operation, name="sales", file=str(path))
    assert not [c for c in backend.calls if c[0] != "GET"]


def test_publish_rejects_changed_source_and_suggestions(package):
    backend, manager, path = package
    apply(manager, path)
    with pytest.raises(SemanticError) as exc:
        manager.execute("publish", name="sales", expected_revision="stale", expected_spec_id="spec_1", yes=True)
    assert exc.value.error_code == "SEMANTIC_REVISION_CONFLICT"
    assert not [c for c in backend.calls if c[2] == ":publish"]
    doc = read_json(str(path))
    doc["source"] = "SYSTEM_SUGGESTIONS"
    save(path, doc)
    with pytest.raises(ValidationError):
        manager.execute("diff", name="sales", file=str(path))


def test_readback_failure_is_uncertain_without_repeating(package):
    backend, manager, path = package
    real = backend.semantic_request
    def fail_after_write(*args, **kwargs):
        result = real(*args, **kwargs)
        if args[1] == "PATCH":
            backend.fail_readback = True
        return result
    backend.semantic_request = fail_after_write
    with pytest.raises(SemanticError) as exc:
        apply(manager, path)
    assert exc.value.error_code == "SEMANTIC_WRITE_UNCERTAIN"
    assert exc.value.context["revisionId"] == "r1"
    assert len([c for c in backend.calls if c[0] == "PATCH"]) == 1


def test_incomplete_content_cannot_be_exported(package):
    backend, manager, _ = package
    backend.spec["draft"]["userDraft"] = {"revisionId": "r1", "sections": {"dataReferences": {"present": True}}}
    with pytest.raises(SemanticError, match="Full versioned content"):
        manager.execute("get", name="sales")


@pytest.mark.parametrize("raw", ['{"x":1,"x":2}', '{"x": NaN}', '[1]', 'null', '{'])
def test_input_json_fail_closed(tmp_path, raw):
    path = tmp_path / "bad.json"
    path.write_text(raw)
    with pytest.raises(ValidationError):
        read_json(str(path))


def test_export_rejects_links_and_unapproved_overwrite(tmp_path):
    target = tmp_path / "target"
    target.write_text("keep")
    with pytest.raises(ValidationError):
        write_json(str(target), {"new": 1})
    assert target.read_text() == "keep"
    linked = tmp_path / "link"
    linked.symlink_to(target)
    with pytest.raises(ValidationError):
        write_json(str(linked), {"new": 1}, overwrite=True)
    with pytest.raises(ValidationError):
        read_json(str(linked))
    result = write_json(str(target), {"new": 1}, overwrite=True)
    assert result["bytes"] > 0 and read_json(str(target)) == {"new": 1}
    if os.name != "nt":
        assert target.stat().st_mode & 0o077 == 0


def test_cli_envelope_manifest_and_pagination(tmp_path, monkeypatch):
    import maxc_cli.cli as cli
    backend = Catalog()
    def factory(**kwargs):
        app = object.__new__(MaxCApp)
        app.backend = backend
        app.config = SimpleNamespace(auth=SimpleNamespace(access_id="fixture", secret_access_key="fixture"))
        app.log = lambda *a, **k: None
        return app
    monkeypatch.setattr(cli, "MaxCApp", factory)
    out = StringIO()
    assert run(["semantic", "list", "--namespace", "123456", "--page-token", "page_2", "--json"], cwd=tmp_path, stdout=out, stderr=StringIO()) == 0
    result = json.loads(out.getvalue())
    assert result["command"] == "semantic list" and result["status"] == "success"
    assert backend.calls[-1][3]["pageToken"] == "page_2"
    commands = {c["command"]: c for c in _command_manifest(build_parser())["commands"]}
    for name in ("create", "apply", "publish", "delete"):
        assert commands["semantic." + name]["effect"] == "remote_write"
    assert commands["semantic.export"]["effect"] == "local_write"
    assert commands["semantic.export"]["output"]["rules"][0]["file_shape_contract"] == "semantic_document_file"
    output = tmp_path / "from-cli.json"
    out = StringIO()
    assert run(["semantic", "export", "sales", "--namespace", "123456", "--output", str(output), "--json"], cwd=tmp_path, stdout=out, stderr=StringIO()) == 0
    assert json.loads(out.getvalue())["data"]["path"] == str(output)
    assert json.loads(output.read_text())["format"] == "maxc.semantic/v1"
    assert commands["semantic.get"]["output"]["shape_contract"] == "structured"
    assert commands["meta.semantic.get"]["network"] == "none"


def test_missing_namespace_is_one_json_failure(tmp_path):
    out = StringIO()
    assert run(["semantic", "list", "--json"], cwd=tmp_path, stdout=out, stderr=StringIO()) != 0
    assert json.loads(out.getvalue())["status"] == "failure"


def test_create_readback_and_uncertain_create(package, tmp_path):
    backend, manager, _ = package
    path = tmp_path / "create.json"
    save(path, {"dataScope": {"dataReferences": [{"project": "p", "schema": "s", "table": "t"}]}})
    assert manager.execute("create", name="sales", file=str(path), yes=True)["specId"] == "spec_1"
    backend.fail_readback = True
    with pytest.raises(SemanticError, match="Create was accepted") as exc:
        manager.execute("create", name="sales", file=str(path), yes=True)
    assert exc.value.error_code == "SEMANTIC_WRITE_UNCERTAIN"
