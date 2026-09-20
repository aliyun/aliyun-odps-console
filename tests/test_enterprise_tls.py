"""Offline unit tests for enterprise TLS trust merging."""

from __future__ import annotations

import os
import ssl
import urllib.error
import urllib.request

import pytest

from maxc_cli import enterprise_tls

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch):
    monkeypatch.setattr(enterprise_tls, "_MERGED_BUNDLE", None)
    monkeypatch.setattr(enterprise_tls, "_MERGE_ATTEMPTED", False)
    monkeypatch.setattr(enterprise_tls, "_HTTPS_CONTEXT", None)


def test_merged_bundle_includes_keychain_and_anchor_sources(
    tmp_path, monkeypatch
) -> None:
    anchor = tmp_path / "anchor.pem"
    anchor.write_text("-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----\n")
    monkeypatch.setattr(
        enterprise_tls, "_SYSTEM_CA_FILES", (str(anchor), "/nope/missing.pem")
    )
    monkeypatch.setattr(
        enterprise_tls,
        "_export_macos_keychain_roots",
        lambda: "-----BEGIN CERTIFICATE-----\nYmFy\n-----END CERTIFICATE-----\n",
    )

    bundle = enterprise_tls.merged_ca_bundle_path()
    assert bundle is not None
    text = open(bundle).read()
    # stock certifi base plus both injected system sources
    assert "Zm9v" in text and "YmFy" in text
    assert text.count("BEGIN CERTIFICATE") >= 2
    assert oct(os.stat(bundle).st_mode & 0o777) == "0o600"
    # cached across calls
    assert enterprise_tls.merged_ca_bundle_path() == bundle


def test_configure_env_merges_existing_ssl_cert_file(
    tmp_path, monkeypatch
) -> None:
    user_ca = tmp_path / "user-ca.pem"
    user_ca.write_text(
        "-----BEGIN CERTIFICATE-----\nVXNlcg==\n-----END CERTIFICATE-----\n"
    )
    anchor = tmp_path / "anchor.pem"
    anchor.write_text("-----BEGIN CERTIFICATE-----\nWm9v\n-----END CERTIFICATE-----\n")
    monkeypatch.setattr(enterprise_tls, "_SYSTEM_CA_FILES", (str(anchor),))
    monkeypatch.setattr(enterprise_tls, "_export_macos_keychain_roots", lambda: None)
    monkeypatch.setenv("SSL_CERT_FILE", str(user_ca))
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)

    enterprise_tls.configure_enterprise_tls_env()

    bundle = os.environ["SSL_CERT_FILE"]
    assert bundle != str(user_ca)
    text = open(bundle).read()
    assert "Wm9v" in text and "VXNlcg==" in text
    assert os.environ["REQUESTS_CA_BUNDLE"] == bundle


def test_urlopen_https_passes_context_only_for_https(
    monkeypatch,
) -> None:
    seen: dict[str, object] = {}

    def fake_urlopen(req, *args, **kwargs):
        seen[str(req.full_url)] = kwargs.get("context")
        raise urllib.error.URLError("stop-after-capture")

    monkeypatch.setattr(enterprise_tls.urllib.request, "urlopen", fake_urlopen)

    for url in ("https://example.invalid/t", "http://127.0.0.1:1/t"):
        with pytest.raises(urllib.error.URLError):
            enterprise_tls.urlopen_https(
                urllib.request.Request(url), timeout=1
            )

    assert isinstance(seen["https://example.invalid/t"], ssl.SSLContext)
    assert seen["http://127.0.0.1:1/t"] is None


def test_https_context_keeps_verification_strict() -> None:
    context = enterprise_tls.https_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
