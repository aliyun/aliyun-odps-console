"""Registered function discovery must preserve scope and avoid lazy Read requests."""

import json
from io import StringIO
from types import SimpleNamespace

import pytest
from odps import ODPS
from odps.errors import NoPermission, NoSuchObject
from requests import Response

from maxc_cli.app import MaxCApp
from maxc_cli.backend.meta import MetaMixin
from maxc_cli.cli import _command_manifest, build_parser, run
from maxc_cli.exceptions import NotFoundError, PermissionDeniedError, ValidationError
from maxc_cli.function_metadata import function_cursor

pytestmark = pytest.mark.unit


def _backend(monkeypatch, responses):
    backend = MetaMixin()
    backend.project = "p"
    backend.client = ODPS("test-id", "test-key", "p", endpoint="http://example.test")
    monkeypatch.setattr(backend.client, "is_schema_namespace_enabled", lambda **kwargs: False)
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        if not responses:
            raise AssertionError("Unexpected metadata or resource GET")
        body = responses.pop(0)
        if isinstance(body, Exception):
            raise body
        response = Response()
        response.status_code = 200
        response._content = body.encode()
        return response

    monkeypatch.setattr(backend.client.rest, "get", get)
    return backend, calls


def _app(backend, schema=None):
    app = MaxCApp.__new__(MaxCApp)
    app.backend = backend
    app.config = SimpleNamespace(default_project="p", default_schema=schema)
    app.log = lambda *args: None
    return app


def test_list_uses_real_sdk_collection_without_per_function_read(monkeypatch):
    body = "<Functions><Marker></Marker>" + "".join(
        f"<Function><Alias>f{i}</Alias></Function>" for i in range(3)
    ) + "</Functions>"
    backend, calls = _backend(monkeypatch, [body])
    rows, more = backend.list_functions(project="other", schema="s", prefix="f", limit=2)
    assert [row["function_name"] for row in rows] == ["f0", "f1"]
    assert rows[0]["class_type"] is None
    assert rows[0]["owner"] is None
    assert more is True
    assert len(calls) == 1
    assert "/projects/other/" in calls[0][0]
    assert calls[0][0].endswith("functions")
    assert calls[0][1]["params"]["curr_schema"] == "s"
    assert calls[0][1]["params"]["name"] == "f"


def test_describe_explicitly_reads_once_and_preserves_resource_references(monkeypatch):
    backend, calls = _backend(monkeypatch, [
        "<Function><Alias>normalize_phone</Alias><ClassType>mod.Normalize</ClassType>"
        "<Owner>owner</Owner><CreationTime>Sat, 10 Oct 2026 00:00:00 GMT</CreationTime>"
        "<Resources><ResourceName>other/schemas/lib/resources/code.py</ResourceName>"
        "<ResourceName>lookup.txt</ResourceName></Resources>"
        "<ProgramLanguage>PYTHON</ProgramLanguage><IsSqlFunction>false</IsSqlFunction>"
        "<Code>private implementation</Code></Function>",
    ])
    detail = backend.describe_function("normalize_phone", project="p", schema="s")
    assert len(calls) == 1
    assert calls[0][1]["curr_schema"] == "s"
    assert detail["resource_names"] == ["other/schemas/lib/resources/code.py", "lookup.txt"]
    assert detail["class_type"] == "mod.Normalize"
    assert detail["creation_time"] is not None
    assert detail["is_sql_function"] is False
    assert detail["signature"] is None
    assert detail["runtime_version"] is None
    assert "private implementation" not in json.dumps(detail)


@pytest.mark.parametrize("error, expected", [
    (NoPermission("Function Read denied"), PermissionDeniedError),
    (NoSuchObject("Function does not exist"), NotFoundError),
])
def test_metadata_permission_and_missing_function_are_distinct(monkeypatch, error, expected):
    backend, calls = _backend(monkeypatch, [error])
    with pytest.raises(expected) as raised:
        backend.describe_function("missing", project="p")
    assert len(calls) == 1
    if expected is PermissionDeniedError:
        assert "Function Read" in raised.value.suggestion
        assert "Function Execute" in raised.value.suggestion
        assert all("--table" not in step for step in raised.value.to_payload().recovery_steps)
    else:
        assert "list-functions" in raised.value.suggestion


def test_function_pages_are_bounded_and_cursor_scope_is_checked_before_remote_work():
    calls = []

    def list_functions(**kwargs):
        calls.append(kwargs)
        return [{"function_name": "f"}], kwargs["offset"] == 0

    app = _app(SimpleNamespace(list_functions=list_functions), schema="default")
    first = app.meta_list_functions(project="other", prefix="f", limit=1).to_dict()
    cursor = first["data"]["pagination"]["next_cursor"]
    second = app.meta_list_functions(project="other", prefix="f", limit=1, cursor=cursor).to_dict()
    assert second["data"]["pagination"]["offset"] == 1
    assert second["data"]["pagination"]["has_more"] is False
    assert second["data"]["functions"][0]["schema_name"] == "default"
    for overrides in ({"project": "changed"}, {"schema": "changed"}, {"prefix": "changed"}):
        args = dict(project="other", prefix="f", limit=1, cursor=cursor)
        args.update(overrides)
        with pytest.raises(ValidationError, match="scope mismatch"):
            app.meta_list_functions(**args)
    assert [call["offset"] for call in calls] == [0, 1]


@pytest.mark.parametrize("cursor", ["bad", "W10=", "e30=", "a" * 8193])
def test_malformed_cursors_are_structured_validation_errors(cursor):
    with pytest.raises(ValidationError):
        _app(None).meta_list_functions(cursor=cursor)


@pytest.mark.parametrize("limit", [0, -1, 1001, True])
def test_list_limit_is_validated_before_backend_creation(limit):
    with pytest.raises(ValidationError):
        _app(None).meta_list_functions(limit=limit)


def test_cursor_bool_offset_is_rejected():
    with pytest.raises(ValidationError):
        _app(None).meta_list_functions(cursor=function_cursor(True, project="p", schema=None, prefix=None))


@pytest.mark.parametrize("alias", ["", "schema.f", "p:f", "f/x", "f x"])
def test_describe_requires_bare_alias_with_separate_scope(alias):
    with pytest.raises(ValidationError, match="bare registered"):
        _app(None).meta_describe_function(alias)


def test_describe_envelope_preserves_selected_scope_and_unknown_contract():
    calls = []

    def describe(name, **kwargs):
        calls.append((name, kwargs))
        return {"function_name": name, "signature": None, "runtime_version": None}

    app = _app(SimpleNamespace(describe_function=describe), schema="default")
    payload = app.meta_describe_function("f", project="other", schema="lib").to_dict()
    assert calls == [("f", {"project": "other", "schema": "lib"})]
    assert payload["data"]["function"]["schema_name"] == "lib"
    assert payload["data"]["function"]["signature"] is None
    assert any("separate permissions" in warning for warning in payload["agent_hints"]["warnings"])


def test_describe_accepts_older_sdk_without_optional_function_fields():
    function = SimpleNamespace(name="f", class_type="pkg.F", _owner="owner", _resources=[], creation_time=None, reload=lambda: None)
    backend = MetaMixin()
    backend.project = "p"
    backend.client = SimpleNamespace(get_function=lambda *args, **kwargs: function)
    detail = backend.describe_function("f")
    assert detail["class_type"] == "pkg.F"
    assert detail["program_language"] is None
    assert detail["is_embedded_function"] is None


def test_parser_and_manifest_expose_read_only_function_contracts():
    parser = build_parser()
    args = parser.parse_args(["meta", "list-functions", "--project", "p", "--schema", "s", "--prefix", "f", "--limit", "2", "--json"])
    assert args.prefix == "f"
    commands = {item["command"]: item for item in _command_manifest(parser)["commands"]}
    for command in ("meta.list-functions", "meta.describe-function"):
        effects = commands[command]["effects"]
        assert any(effect["scope"] == "remote" and effect["kind"] == "read" for effect in effects)
        assert not any(effect["target"] == "metadata_cache" for effect in effects)
        assert commands[command]["requirements"]["credentials"]["mode"] == "required"
    assert "csv" in commands["meta.list-functions"]["output"]["formats"]


@pytest.mark.parametrize("output_format", ["json", "table", "csv", "ndjson"])
def test_cli_function_list_output_formats(monkeypatch, tmp_path, output_format):
    import maxc_cli.app as app_module
    from tests.test_cli_mock import clear_odps_env

    clear_odps_env(monkeypatch)
    monkeypatch.setattr(app_module, "OdpsBackend", lambda *args, **kwargs: SimpleNamespace(
        list_functions=lambda **kwargs: ([{"function_name": "normalize_phone", "class_type": None, "owner": None}], False),
    ))
    config = tmp_path / "config.yaml"
    config.write_text(f"default_project: p\nstate_file: {tmp_path / 'state.json'}\ncache_dir: {tmp_path / 'cache'}\naudit_log: {tmp_path / 'audit.jsonl'}\n")
    stdout, stderr = StringIO(), StringIO()
    code = run(["--config", str(config), "--format", output_format, "meta", "list-functions", "--project", "p"], cwd=tmp_path, stdout=stdout, stderr=stderr)
    assert code == 0, stderr.getvalue()
    assert "normalize_phone" in stdout.getvalue()
    if output_format == "json":
        assert json.loads(stdout.getvalue())["data"]["functions"][0]["function_name"] == "normalize_phone"
