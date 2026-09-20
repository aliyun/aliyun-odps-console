"""Enterprise TLS interception support for HTTPS egresses.

Corporate security agents (e.g. AliLang) intercept TLS with chains rooted at
an enterprise CA that exists only in the OS trust store. Python's ``ssl``
module never consults the macOS keychain, and PyInstaller bundles ship their
own certifi file, so stdlib OAuth calls and the pyodps/requests data plane
can fail with ``CERTIFICATE_VERIFY_FAILED: unable to get local issuer
certificate`` while curl and the Go wrapper succeed on the same machine.

These helpers merge system trust anchors into every HTTPS path without ever
relaxing certificate or hostname verification.
"""

from __future__ import annotations

import os
import ssl
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path

_SYSTEM_CA_FILES = (
    "/etc/ssl/cert.pem",  # macOS anchor; also present on some Linux distros
    "/etc/pki/tls/certs/ca-bundle.crt",  # RHEL family
    "/etc/ssl/certs/ca-certificates.crt",  # Debian family
)


def _stock_ca_pem() -> bytes:
    try:
        import certifi

        return Path(certifi.where()).read_bytes()
    except Exception:
        return b""


def _export_macos_keychain_roots() -> str | None:
    if not sys.platform.startswith("darwin"):
        return None
    try:
        proc = subprocess.run(
            [
                "security", "find-certificate", "-a", "-p",
                "/Library/Keychains/System.keychain",
            ],
            capture_output=True,
            timeout=5,
        )
    except Exception:
        return None
    pem = proc.stdout.decode("utf-8", "replace") if proc.stdout else ""
    return pem if "BEGIN CERTIFICATE" in pem else None


_MERGED_BUNDLE: str | None = None
_MERGE_ATTEMPTED = False


def _ca_source_texts() -> list[str]:
    """CA PEM sources beyond the stock certifi bundle, in merge order.

    A user-specified SSL_CERT_FILE wins over auto-detected system anchors;
    it is always included rather than merely merged on top of them.
    """
    parts: list[str] = []
    user_ca = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    if user_ca and os.path.isfile(user_ca):
        try:
            text = Path(user_ca).read_text(encoding="utf-8", errors="replace")
            if "BEGIN CERTIFICATE" in text:
                parts.append(text)
        except Exception:
            pass
    for path in _SYSTEM_CA_FILES:
        try:
            if os.path.isfile(path):
                text = Path(path).read_text(encoding="utf-8", errors="replace")
                if "BEGIN CERTIFICATE" in text:
                    parts.append(text)
        except Exception:
            continue
    keychain_pem = _export_macos_keychain_roots()
    if keychain_pem:
        parts.append(keychain_pem)
    return parts


def merged_ca_bundle_path() -> str | None:
    """PEM file combining certifi, user SSL_CERT_FILE, distro anchors, and
    macOS System keychain roots.

    Returns None when there is nothing to add beyond the stock bundle. Built
    once per process from a single snapshot, so repeated configure calls
    never duplicate entries; written owner-only into the temp directory.
    """
    global _MERGED_BUNDLE, _MERGE_ATTEMPTED
    if _MERGE_ATTEMPTED:
        return _MERGED_BUNDLE
    _MERGE_ATTEMPTED = True
    parts = _ca_source_texts()
    stock = _stock_ca_pem().decode("utf-8", "replace")
    if not parts and "BEGIN CERTIFICATE" not in stock:
        return None
    try:
        handle = tempfile.NamedTemporaryFile(
            prefix="maxc-merged-ca-", suffix=".pem", delete=False
        )
        with handle:
            handle.write(stock.encode("utf-8", "replace"))
            for part in parts:
                handle.write(part.encode("utf-8", "replace"))
            name = handle.name
        os.chmod(name, 0o600)
        _MERGED_BUNDLE = name
    except Exception:
        _MERGED_BUNDLE = None
    return _MERGED_BUNDLE


def requests_verify_path() -> str | bool:
    """CA file to hand to requests-based clients, or True for their default."""
    return merged_ca_bundle_path() or True


def configure_enterprise_tls_env() -> None:
    """Point env-honoring HTTPS stacks (requests/pyodps) at merged anchors.

    A user-provided SSL_CERT_FILE is included first inside the bundle rather
    than dropped. Only called from CLI entry points, never at import time of
    library modules.
    """
    bundle = merged_ca_bundle_path()
    if bundle is None:
        return
    os.environ["SSL_CERT_FILE"] = bundle
    os.environ.setdefault("REQUESTS_CA_BUNDLE", bundle)


_HTTPS_CONTEXT: ssl.SSLContext | None = None


def https_context() -> ssl.SSLContext:
    global _HTTPS_CONTEXT
    if _HTTPS_CONTEXT is None:
        context = ssl.create_default_context()
        try:
            context.load_default_certs()
        except Exception:
            pass
        for path in _SYSTEM_CA_FILES:
            try:
                if os.path.isfile(path):
                    context.load_verify_locations(cafile=path)
            except Exception:
                continue
        _HTTPS_CONTEXT = context
    return _HTTPS_CONTEXT


def urlopen_https(req: urllib.request.Request, timeout: float):
    scheme = urllib.parse.urlsplit(req.full_url).scheme.lower()
    if scheme == "https":
        return urllib.request.urlopen(req, timeout=timeout, context=https_context())
    return urllib.request.urlopen(req, timeout=timeout)
