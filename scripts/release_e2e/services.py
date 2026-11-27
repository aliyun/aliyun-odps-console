"""Loopback service fixtures for the installed CLI release test."""

from __future__ import annotations

import http.client
import json
import ssl
import subprocess
import threading
import time
import urllib.request
from contextlib import ExitStack, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit
from xml.etree import ElementTree
from xml.sax.saxutils import escape

from .upgrade import require

EMULATOR = "maxcompute/maxcompute-emulator:1.2.0@sha256:c4f708449e309cdb6900641b7d90097e6c9b2633a943dac382c27683fc3daf5f"
PROJECT = "test_project"
MCP_TOKEN = "mcpc_release-e2e-token"
SEMANTIC_COLLECTION = "/api/catalog/v1alpha/namespaces/123456/semanticSpecs"


def normalize_extended_table_metadata(body):
    # Emulator 1.2.0 emits a JSON boolean here. PyODPS reads the service
    # format, which stores Transactional as a string in the Reserved object.
    table = ElementTree.fromstring(body)
    schema_node = table.find("Schema")
    if schema_node is None or not schema_node.text:
        return body
    schema = json.loads(schema_node.text)
    reserved = json.loads(schema.get("Reserved") or "{}")
    transactional = reserved.get("Transactional")
    if not isinstance(transactional, bool):
        return body
    reserved["Transactional"] = "true" if transactional else "false"
    schema["Reserved"] = json.dumps(reserved)
    schema_node.text = json.dumps(schema)
    return ElementTree.tostring(table)


@contextmanager
def emulator():
    container = subprocess.check_output([
        "docker", "run", "--detach", "--rm", "--platform", "linux/amd64", "--memory", "1024m",
        "-p", "127.0.0.1::8080", EMULATOR, "--listen", "0.0.0.0:8080", "--seed", "/opt/emulator/seed.sql",
    ], text=True, timeout=240).strip()
    try:
        address = subprocess.check_output(["docker", "port", container, "8080/tcp"], text=True, timeout=15).strip()
        endpoint = urlsplit(f"http://{address}")
        for _ in range(60):
            try:
                with urllib.request.urlopen(endpoint.geturl() + "/readyz", timeout=2) as reply:
                    if json.load(reply)["status"] == "ready":
                        break
            except OSError:
                time.sleep(1)
        else:
            raise RuntimeError("The isolated MaxCompute Emulator did not become ready")
        yield endpoint
    finally:
        subprocess.run(["docker", "stop", container], check=True, stdout=subprocess.DEVNULL, timeout=25)


@contextmanager
def server(handler, tls=None):
    with ThreadingHTTPServer(("127.0.0.1", 0), handler) as instance:
        if tls:
            instance.socket = tls.wrap_socket(instance.socket, server_side=True)
        thread = threading.Thread(target=instance.serve_forever, daemon=True)
        thread.start()
        try:
            scheme = "https" if tls else "http"
            yield f"{scheme}://127.0.0.1:{instance.server_port}"
        finally:
            instance.shutdown()
            thread.join()


class WireHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def read_body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def respond(self, body, status=200, content_type="application/json", headers=()):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in headers:
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def xml(self, body):
        self.respond(body, content_type="application/xml")


class Catalog:
    """A single disposable Catalog spec with a draft and publication history."""

    def __init__(self):
        self.spec = None
        self.revision = 0

    def handle(self, handler, path, params, raw):
        method = handler.command
        suffix = path.removeprefix(SEMANTIC_COLLECTION)
        if method == "POST" and not suffix:
            self.spec = {**json.loads(raw), "specId": "fixture_spec", "draft": {}}
        elif method == "GET" and not suffix:
            handler.respond({"semanticSpecs": [self.spec] if self.spec else []})
            return
        elif self.spec is None:
            handler.respond({"Code": "NotFound"}, status=404)
            return
        elif method == "GET" and suffix.endswith("/publishedRevisions"):
            handler.respond({"publishedRevisions": [{"revisionId": "published_1"}]})
            return
        elif method == "PATCH":
            update = json.loads(raw)
            self.revision += 1
            draft = update.get("draft", {}).get("userDraft", {})
            draft["revisionId"] = f"draft_{self.revision}"
            self.spec.update(update)
        elif method == "POST" and suffix.endswith(":publish"):
            draft = self.spec["draft"]["userDraft"]
            require(json.loads(raw)["expectedRevisionId"] == draft["revisionId"], "Wrong semantic revision selected")
            self.spec["published"] = {
                "revisionId": "published_1", "sourceRevisionId": draft["revisionId"],
                "source": "USER_DRAFT", "content": draft["content"],
            }
        elif method == "DELETE":
            require(params["expectedSemanticSpecId"] == [self.spec["specId"]], "Wrong semantic object selected")
            self.spec = None
            handler.respond(b"", status=204)
            return
        handler.respond(self.spec)


class McpHandler(WireHandler):
    tools = [
        {"name": "maxcompute_kb_search", "description": "Fixture search", "inputSchema": {"type": "object"}},
        {"name": "maxcompute_kb_ask", "description": "Fixture question", "inputSchema": {"type": "object"}},
    ]

    def do_POST(self):
        require(self.headers.get("Authorization") == "Bearer " + MCP_TOKEN, "MCP client did not use the minted bearer")
        message = json.loads(self.read_body())
        method = message["method"]
        if method == "initialize":
            result = {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}, "serverInfo": {"name": "fixture", "version": "1"}}
        elif method == "tools/list":
            result = {"tools": self.tools}
        elif method == "tools/call":
            name = message["params"]["name"]
            if name == "maxcompute_kb_ask":
                result = {"structuredContent": {"ok": True, "data": {"answer": "A fixture answer"},
                                                "citations": [{"uri": "https://example.invalid/tables"}], "request_id": "e2e-ask"}}
            elif name == "maxcompute_kb_search":
                result = {"structuredContent": {"ok": True, "data": {"results": [
                    {"source": {"uri": "https://example.invalid/tables"}, "score": 0.95},
                ]}, "request_id": "e2e-search"}}
            else:
                raise RuntimeError(f"Unexpected MCP tool {name}")
        elif method == "notifications/initialized":
            self.respond(b"", status=202)
            return
        else:
            raise RuntimeError(f"Unexpected MCP method {method}")
        self.respond({"jsonrpc": "2.0", "id": message["id"], "result": result})


@contextmanager
def fixtures(root, upstream):
    catalog = Catalog()
    traffic = []
    submitted = []
    cancelled = set()
    analysis_tasks = {
        "fixture_cost": ("AnonymousSQLCostTask", json.dumps({"Cost": {"SQL": {"Input": 128, "Complexity": 1, "UDF": 0}}})),
        "fixture_explain": ("AnonymousSQLTask", "fixture execution plan"),
    }

    class Handler(WireHandler):
        def dispatch(self):
            request = urlsplit(self.path)
            path = unquote(request.path)
            if not path.startswith("/api/catalog/"):
                path = path.removeprefix("/api")
            params = parse_qs(request.query, keep_blank_values=True)
            body = self.read_body()
            traffic.append((self.command, path, sorted(params)))
            if path == "/api/catalog/v1alpha/mcpAccessToken":
                self.respond({"accessToken": MCP_TOKEN, "expiresIn": 300, "tokenType": "Bearer", "scope": ["maxcompute:read", "maxcompute:sql"]})
            elif path.startswith(SEMANTIC_COLLECTION):
                catalog.handle(self, path, params, body)
            elif self.command == "POST" and path.endswith("/authorization"):
                identity = {"DisplayName": "fixture@example.invalid", "ID": "123456", "AccountId": "123456"}
                self.xml(f"<Authorization><Result>{escape(json.dumps(identity))}</Result></Authorization>")
            elif self.command == "POST" and path.endswith("/auth/"):
                self.xml("<Authorization><Result>ALLOW</Result></Authorization>")
            elif self.command == "GET" and path == "/projects":
                self.xml(f"<Projects><Project><Name>{PROJECT}</Name></Project><Marker/></Projects>")
            elif self.command == "GET" and path.endswith("/schemas"):
                self.xml("<Schemas><Schema><Name>default</Name></Schema><Marker/></Schemas>")
            elif self.command == "GET" and path.endswith("/registration/functions"):
                self.xml("<Functions><Function><Alias>fixture_fn</Alias><ClassType>fixture.Udf</ClassType></Function><Marker/></Functions>")
            elif self.command == "GET" and path.endswith("/registration/functions/fixture_fn"):
                self.xml("<Function><Alias>fixture_fn</Alias><ClassType>fixture.Udf</ClassType><Owner>fixture</Owner><Resources/></Function>")
            elif self.command == "GET" and path.endswith("/tables/demo") and "data" in params:
                self.respond("id,name,amount,active\n1,alpha,10.50,true\n2,beta,20.00,false\n", content_type="text/csv")
            elif self.command == "GET" and path.endswith("/tables/events") and "data" in params:
                self.respond("id,value\n3,next day\n", content_type="text/csv")
            elif self.command == "POST" and path.endswith("/instances") and (b"<SQLCost>" in body or b"EXPLAIN SELECT" in body.upper()):
                job = "fixture_cost" if b"<SQLCost>" in body else "fixture_explain"
                self.respond(b"", status=201, headers=(("Location", f"{path}/{job}"),))
            elif self.command == "GET" and path.rsplit("/", 1)[1] in analysis_tasks:
                job = path.rsplit("/", 1)[1]
                name, result = analysis_tasks[job]
                if "result" in params or "taskname" in params:
                    self.xml(f"<Instance><Tasks><Task><Name>{name}</Name><Result>{escape(result)}</Result></Task></Tasks></Instance>")
                elif "taskstatus" in params:
                    self.xml(f"<Instance><Tasks><Task Type='SQL'><Name>{name}</Name><Status>Success</Status></Task></Tasks></Instance>")
                else:
                    self.xml(f"<Instance><Name>{job}</Name><Status>Terminated</Status></Instance>")
            elif self.command == "GET" and "instancesummary" in params:
                self.respond({"Instance": {"JsonSummary": '{"rows":1}', "Summary": "fixture task"}})
            elif self.command == "GET" and "detail" in params:
                self.respond({"jobs": [{"name": "fixture stage", "instances": [{"id": "fixture_worker", "logId": "fixture_worker_log"}]}]})
            elif self.command == "GET" and "log" in params:
                self.respond("fixture worker log", content_type="text/plain")
            elif self.command == "GET" and path.endswith("/instances"):
                entries = "".join(f"<Instance><Name>{escape(job)}</Name><Status>Terminated</Status></Instance>" for job in submitted)
                self.xml(f"<Instances>{entries}<Marker/></Instances>")
            elif self.command == "PUT" and path.rsplit("/", 1)[1] in submitted:
                cancelled.add(path.rsplit("/", 1)[1])
                self.respond(b"")
            elif self.command == "GET" and path.rsplit("/", 1)[1] in cancelled and "taskstatus" in params:
                self.xml("<Instance><Tasks><Task Type='SQL'><Name>AnonymousSQLTask</Name><Status>Cancelled</Status></Task></Tasks></Instance>")
            elif self.command == "GET" and path.rsplit("/", 1)[1] in cancelled and "result" in params:
                self.xml("<Instance><Tasks><Task><Name>AnonymousSQLTask</Name><Result>Cancelled by fixture request</Result></Task></Tasks></Instance>")
            else:
                self.forward(path, request.query, body)

        def forward(self, path, query, body):
            conn = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=10)
            try:
                headers = {name: value for name, value in self.headers.items() if name.lower() not in {"host", "accept-encoding", "connection"}}
                conn.request(self.command, path + ("?" + query if query else ""), body=body, headers=headers)
                response = conn.getresponse()
                extra = [(key, val) for key, val in response.getheaders() if key.lower() in {"location", "x-odps-request-id"}]
                location = response.getheader("Location")
                if self.command == "POST" and path.endswith("/instances") and location:
                    submitted.append(location.rsplit("/", 1)[1])
                payload = response.read()
                if (response.status == 200 and self.command == "GET" and "/tables/" in path
                        and "extended" in parse_qs(query, keep_blank_values=True)):
                    payload = normalize_extended_table_metadata(payload)
                self.respond(payload, status=response.status, content_type=response.getheader("Content-Type", "application/xml"), headers=extra)
            finally:
                conn.close()

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = dispatch

    cert, key = root / "fixture-cert.pem", root / "fixture-key.pem"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        "-out", str(cert), "-keyout", str(key),
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    with ExitStack() as stack:
        odps = stack.enter_context(server(Handler))
        mcp = stack.enter_context(server(McpHandler, tls))
        yield odps, mcp + "/mcp", cert, traffic
