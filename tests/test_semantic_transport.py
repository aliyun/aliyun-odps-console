"""Real HTTP boundary tests with a local Catalog fixture and PyODPS signing."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from odps.accounts import AliyunAccount, StsAccount
from odps.rest import RestClient

from maxc_cli.backend.semantic import SemanticMixin
from maxc_cli.semantic import SemanticError

pytestmark = pytest.mark.unit


@pytest.fixture
def http_catalog(monkeypatch):
    # Local-only credentials. Clear proxy influence, never use a real identity.
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    state = {"status": 200, "response": {"semanticSpecs": [], "nextPageToken": "opaque + / ="}, "calls": []}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def handle_request(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            state["calls"].append((self.command, self.path, dict(self.headers), body))
            raw = state.get("raw", json.dumps(state["response"]).encode())
            self.send_response(state["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            if "location" in state:
                self.send_header("Location", state["location"])
            self.end_headers()
            self.wfile.write(raw)
        do_GET = handle_request
        do_POST = handle_request
        do_PATCH = handle_request
        do_DELETE = handle_request
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}/api"
    rest = RestClient(AliyunAccount("fixture-id", "fixture-secret"), endpoint, user_agent="fixture-agent", region_name="cn-shanghai")
    backend = SemanticMixin()
    backend.client = SimpleNamespace(catalog_rest=rest)
    yield backend, state
    server.shutdown()
    thread.join()
    server.server_close()


def test_signed_pagination_and_path_are_encoded_once(http_catalog):
    backend, state = http_catalog
    result = backend.semantic_request("123456", "GET", params={"pageToken": "opaque + / =", "byTag": "中文"})
    method, url, headers, _ = state["calls"][0]
    assert method == "GET" and urlsplit(url).path == "/api/catalog/v1alpha/namespaces/123456/semanticSpecs"
    assert parse_qs(urlsplit(url).query)["pageToken"] == ["opaque + / ="]
    assert headers["Authorization"]
    assert headers["x-odps-user-agent"] == "fixture-agent"
    assert result["nextPageToken"] == "opaque + / ="


def test_sts_signing_preserves_token_without_exposing_it(http_catalog):
    backend, state = http_catalog
    rest = backend.client.catalog_rest
    backend.client.catalog_rest = RestClient(StsAccount("fixture-id", "fixture-secret", "fixture-sts-token"), rest.endpoint)
    result = backend.semantic_request("123456", "GET")
    headers = {k.lower(): v for k, v in state["calls"][0][2].items()}
    assert headers["authorization-sts-token"] == "fixture-sts-token"
    assert "fixture-sts-token" not in json.dumps(result)


def test_patch_keeps_empty_arrays_and_exact_cas(http_catalog):
    backend, state = http_catalog
    state["response"] = {"specName": "sales", "specId": "spec_1"}
    backend.semantic_request("123456", "PATCH", "sales", params={"expectedDraftRevisionId": "r1", "updateMask": "draft.userDraft.content.glossary"}, body={"draft": {"userDraft": {"content": {"glossary": []}}}})
    method, url, _, raw = state["calls"][0]
    assert method == "PATCH"
    assert parse_qs(urlsplit(url).query)["expectedDraftRevisionId"] == ["r1"]
    assert json.loads(raw)["draft"]["userDraft"]["content"]["glossary"] == []


@pytest.mark.parametrize("method", ["POST", "PATCH", "DELETE"])
def test_uncertain_writes_are_sent_once_and_errors_redacted(http_catalog, method):
    backend, state = http_catalog
    state.update(status=503, response={"Code": "InternalServerError", "Message": "fixture-secret signed headers and customer data"})
    with pytest.raises(SemanticError) as exc:
        backend.semantic_request("123456", method, "sales", suffix=":publish" if method == "POST" else "")
    assert len(state["calls"]) == 1
    assert exc.value.error_code == "SEMANTIC_WRITE_UNCERTAIN"
    assert "fixture-secret" not in str(exc.value.to_payload().to_dict())


def test_redirect_does_not_forward_signed_request(http_catalog):
    backend, state = http_catalog
    state.update(status=307, location=backend.semantic_endpoint() + "/other")
    with pytest.raises(SemanticError):
        backend.semantic_request("123456", "POST", "sales", suffix=":publish")
    assert len(state["calls"]) == 1


@pytest.mark.parametrize("status,code,expected", [(401, "Unauthorized", "SEMANTIC_AUTHENTICATION_FAILED"), (403, "SignatureNotMatch", "SEMANTIC_AUTHENTICATION_FAILED"), (409, "RevisionConflict", "SEMANTIC_REVISION_CONFLICT"), (409, "AlreadyExists", "SEMANTIC_ALREADY_EXISTS"), (403, "NoPermission", "SEMANTIC_PERMISSION_DENIED"), (404, "NotFound", "SEMANTIC_NOT_FOUND"), (400, "InvalidArgument", "VALIDATION_ERROR")])
def test_business_failures_have_stable_codes(http_catalog, status, code, expected):
    backend, state = http_catalog
    state.update(status=status, response={"Code": code, "Message": "do not echo"})
    with pytest.raises(SemanticError) as exc:
        backend.semantic_request("123456", "GET", "sales")
    assert exc.value.error_code == expected


def test_malformed_write_receipt_requires_reconciliation(http_catalog):
    backend, state = http_catalog
    state["raw"] = b"<html>not JSON</html>"
    with pytest.raises(SemanticError) as exc:
        backend.semantic_request("123456", "POST", "sales", suffix=":publish")
    assert exc.value.error_code == "SEMANTIC_WRITE_UNCERTAIN"
    assert len(state["calls"]) == 1


@pytest.mark.parametrize("token", ["opaque + / =", "escaped%2B&key=value", "中文 + token"])
@pytest.mark.parametrize("sts", [False, True])
def test_signature_uses_once_decoded_query_values(http_catalog, token, sts):
    import requests
    from odps import options
    backend, state = http_catalog
    rest = backend.client.catalog_rest
    if sts:
        rest = RestClient(StsAccount("fixture-id", "fixture-secret", "fixture-sts-token"), rest.endpoint)
        backend.client.catalog_rest = rest
    backend.semantic_request("123456", "GET", params={"pageToken": token, "pageSize": 1})
    method, url, headers, _ = state["calls"][0]
    assert parse_qs(urlsplit(url).query)["pageToken"] == [token]
    request = requests.Request(method, rest.endpoint[:-4] + url, headers=headers).prepare()
    # Server canonicalization decodes each query value once, unlike PyODPS's
    # additional URL-level unquote. Verify HMAC, not merely header presence.
    canonical = rest.account._build_canonical_str(urlsplit(url), request)
    region = rest.region_name if options.enable_v4_sign else None
    assert headers["Authorization"] == rest.account.calc_auth_str(canonical, region)
