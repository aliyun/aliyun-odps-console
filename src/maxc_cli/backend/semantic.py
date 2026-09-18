"""Catalog SemanticSpec transport, reusing the active PyODPS account and routing.

A dedicated HTTP session avoids inherited SDK retry/redirect policies for writes.
Only request signing and endpoint discovery are delegated to PyODPS; credentials
are never copied into a new profile or returned to the caller.
"""

import json
from urllib.parse import quote, urlsplit, urlunsplit

import requests
from odps import options

from ..exceptions import MaxCError, ValidationError
from ..semantic import MAX_FILE_BYTES, SemanticError, identifier, namespace_id


def _failure(status, code, *, mutation):
    known = {
        "Unauthorized": "SEMANTIC_AUTHENTICATION_FAILED",
        "SignatureNotMatch": "SEMANTIC_AUTHENTICATION_FAILED",
        "SignatureDoesNotMatch": "SEMANTIC_AUTHENTICATION_FAILED",
        "RevisionConflict": "SEMANTIC_REVISION_CONFLICT",
        "AlreadyExists": "SEMANTIC_ALREADY_EXISTS",
        "NotFound": "SEMANTIC_NOT_FOUND",
        "NoSuchObject": "SEMANTIC_NOT_FOUND",
        "SemanticSpecSystemSuggestionsNotReady": "SEMANTIC_NOT_READY",
        "NoPermission": "SEMANTIC_PERMISSION_DENIED",
        "AccessDenied": "SEMANTIC_PERMISSION_DENIED",
        "Forbidden": "SEMANTIC_PERMISSION_DENIED",
        "InvalidArgument": "VALIDATION_ERROR",
    }
    mapped = known.get(code) or {
        400: "VALIDATION_ERROR", 401: "SEMANTIC_AUTHENTICATION_FAILED",
        403: "SEMANTIC_PERMISSION_DENIED", 404: "SEMANTIC_NOT_FOUND",
        409: "SEMANTIC_REVISION_CONFLICT",
    }.get(status)
    mapped = mapped or ("SEMANTIC_WRITE_UNCERTAIN" if mutation else "SEMANTIC_CONNECTION_ERROR")
    return SemanticError(mapped, "Catalog semantic operation failed; inspect the object before retrying.", context={"httpStatus": status})


class SemanticMixin:
    def semantic_endpoint(self):
        # Avoid the project-keyed SQLite tenant/endpoint cache for account assets.
        if self.client is None:
            raise ValidationError("Configure an authenticated MaxCompute connection first.")
        try:
            endpoint = self.client.catalog_rest.endpoint.rstrip("/")
        except Exception as exc:
            raise SemanticError("SEMANTIC_CONNECTION_ERROR", "Catalog endpoint discovery failed.") from exc
        parsed = urlsplit(endpoint)
        if parsed.scheme not in ("https", "http") or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValidationError("Catalog endpoint must be an HTTP(S) service URL without credentials, query, or fragment.")
        return endpoint

    def semantic_request(self, namespace, method, name=None, *, suffix="", params=None, body=None):
        namespace_id(namespace)
        if name is not None:
            identifier(name, name=True)
        endpoint = self.semantic_endpoint()
        base = endpoint[:-4] if endpoint.endswith("/api") else endpoint
        path = f"/api/catalog/v1alpha/namespaces/{namespace}/semanticSpecs"
        if name is not None:
            path += "/" + quote(name, safe="")
        path += suffix
        rest = self.client.catalog_rest
        mutation = method != "GET"
        try:
            payload = None
            if body is not None:
                payload = json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
                if len(payload) > MAX_FILE_BYTES:
                    raise ValidationError("Semantic request exceeds the 16 MiB limit.")
            # Reuse the account (including OAuth/STS/proxy behavior), region and
            # user-agent from the active REST client, without persisting secrets.
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            user_agent = getattr(rest, "_user_agent", "maxc-cli")
            headers.update({"User-Agent": user_agent, "x-odps-user-agent": user_agent})
            request = requests.Request(method, base + path, params=params or {}, data=payload, headers=headers).prepare()
            region = rest.region_name if options.enable_v4_sign else None
            # PyODPS sign_request unquotes the URL before parse_qsl unquotes
            # each query value again. Protect percent escapes for signing only,
            # so an opaque token's literal '+' cannot become a space. The wire
            # URL remains encoded exactly once (including for STS/external auth).
            wire_url = request.url
            parts = urlsplit(wire_url)
            request.url = urlunsplit(parts._replace(query=quote(parts.query, safe="=&+")))
            try:
                rest.account.sign_request(request, base, region_name=region)
                if getattr(rest, "app_account", None) is not None:
                    rest.app_account.sign_request(request, base, region_name=region)
            finally:
                request.url = wire_url
            # requests' default adapters have zero retries. No redirect can
            # forward credentials or silently change a signed target/method.
            with requests.Session() as session:
                settings = session.merge_environment_settings(request.url, dict(getattr(rest, "_proxy", None) or {}), True, options.verify_ssl, None)
                with session.send(request, timeout=(options.connect_timeout, options.read_timeout), allow_redirects=False, **settings) as response:
                    status = response.status_code
                    raw = bytearray()
                    for chunk in response.iter_content(64 * 1024):
                        raw.extend(chunk)
                        if len(raw) > MAX_FILE_BYTES:
                            raise SemanticError("SEMANTIC_WRITE_UNCERTAIN" if mutation else "SEMANTIC_INVALID_RESPONSE", "Catalog response exceeds the 16 MiB limit.")
                    if not 200 <= status < 300:
                        code = ""
                        try:
                            error = json.loads(raw)
                            if isinstance(error, dict):
                                code = error.get("Code") or error.get("code") or ""
                        except (ValueError, UnicodeError):
                            pass
                        raise _failure(status, code, mutation=mutation)
                    if method == "DELETE":
                        return {"deleted": True, "specName": name, "expectedSemanticSpecId": (params or {}).get("expectedSemanticSpecId")}
                    result = json.loads(raw)
                    if not isinstance(result, dict):
                        raise ValueError("expected object")
                    return result
        except MaxCError:
            raise
        except Exception as exc:
            # Raw exceptions can contain signed headers, SQL, or user content.
            raise SemanticError("SEMANTIC_WRITE_UNCERTAIN" if mutation else "SEMANTIC_CONNECTION_ERROR", "Catalog semantic request did not produce a verifiable result. Reconcile before retrying a write.") from exc
