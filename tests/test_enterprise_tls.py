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
    if os.name == "posix":
        assert oct(os.stat(bundle).st_mode & 0o777) == "0o600"
    # cached across calls
    assert enterprise_tls.merged_ca_bundle_path() == bundle


def test_user_ssl_cert_file_wins_and_is_not_duplicated(
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

    bundle = enterprise_tls.merged_ca_bundle_path()
    assert bundle is not None
    text = open(bundle).read()
    # user store included exactly once, ahead of system anchors
    assert text.count("VXNlcg==") == 1
    assert text.index("VXNlcg==") < text.index("Wm9v")

    # repeated configure must not append duplicates into the cached bundle
    for _ in range(3):
        enterprise_tls.configure_enterprise_tls_env()
    assert open(bundle).read().count("VXNlcg==") == 1


def test_requests_ca_bundle_used_when_ssl_cert_file_absent(
    tmp_path, monkeypatch
) -> None:
    user_ca = tmp_path / "rca.pem"
    user_ca.write_text(
        "-----BEGIN CERTIFICATE-----\nRHJpcw==\n-----END CERTIFICATE-----\n"
    )
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(user_ca))
    monkeypatch.setattr(enterprise_tls, "_SYSTEM_CA_FILES", ())
    monkeypatch.setattr(enterprise_tls, "_export_macos_keychain_roots", lambda: None)

    bundle = enterprise_tls.merged_ca_bundle_path()
    assert bundle is not None
    assert "RHJpcw==" in open(bundle).read()


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


def _fake_stock(monkeypatch) -> None:
    monkeypatch.setattr(
        enterprise_tls,
        "_stock_ca_pem",
        lambda: b"-----BEGIN CERTIFICATE-----\nU3RvY2s=\n-----END CERTIFICATE-----\n",
    )


def _write_ca(path, marker: str) -> None:
    path.write_text(f"-----BEGIN CERTIFICATE-----\n{marker}\n-----END CERTIFICATE-----\n")


def test_injected_requests_bundle_is_repointed_to_the_superset(
    tmp_path, monkeypatch
) -> None:
    """Corporate proxies that inject only REQUESTS_CA_BUNDLE must not lose public roots.

    requests resolves REQUESTS_CA_BUNDLE ahead of SSL_CERT_FILE, so leaving the
    injected path in place keeps the data plane trusting a corporate-only store.
    """
    corporate = tmp_path / "corp-only.pem"
    _write_ca(corporate, "Q29ycA==")
    _fake_stock(monkeypatch)
    monkeypatch.setattr(enterprise_tls, "_SYSTEM_CA_FILES", ())
    monkeypatch.setattr(enterprise_tls, "_export_macos_keychain_roots", lambda: None)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(corporate))

    enterprise_tls.configure_enterprise_tls_env()

    bundle = os.environ["REQUESTS_CA_BUNDLE"]
    assert bundle != str(corporate), "requests still trusts only the injected store"
    text = open(bundle).read()
    assert "Q29ycA==" in text and "U3RvY2s=" in text
    assert os.environ["SSL_CERT_FILE"] == bundle


def test_injected_curl_bundle_is_merged_and_repointed(tmp_path, monkeypatch) -> None:
    """CURL_CA_BUNDLE is a third store requests honours; it used to be ignored."""
    corporate = tmp_path / "curl-only.pem"
    _write_ca(corporate, "Q3VybA==")
    _fake_stock(monkeypatch)
    monkeypatch.setattr(enterprise_tls, "_SYSTEM_CA_FILES", ())
    monkeypatch.setattr(enterprise_tls, "_export_macos_keychain_roots", lambda: None)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
    monkeypatch.setenv("CURL_CA_BUNDLE", str(corporate))

    enterprise_tls.configure_enterprise_tls_env()

    bundle = os.environ["CURL_CA_BUNDLE"]
    assert bundle != str(corporate)
    text = open(bundle).read()
    assert "Q3VybA==" in text and "U3RvY2s=" in text


def test_distinct_ssl_and_requests_stores_are_both_merged(
    tmp_path, monkeypatch
) -> None:
    ssl_ca = tmp_path / "ssl.pem"
    _write_ca(ssl_ca, "VMxMQ==")
    requests_ca = tmp_path / "requests.pem"
    _write_ca(requests_ca, "UmVxMQ==")
    _fake_stock(monkeypatch)
    monkeypatch.setattr(enterprise_tls, "_SYSTEM_CA_FILES", ())
    monkeypatch.setattr(enterprise_tls, "_export_macos_keychain_roots", lambda: None)
    monkeypatch.setenv("SSL_CERT_FILE", str(ssl_ca))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(requests_ca))

    bundle = enterprise_tls.merged_ca_bundle_path()
    assert bundle is not None
    text = open(bundle).read()
    assert text.count("VMxMQ==") == 1
    assert text.count("UmVxMQ==") == 1, "second injected store was dropped"

    # the same file referenced by two variables must not be duplicated
    monkeypatch.setenv("CURL_CA_BUNDLE", str(requests_ca))
    enterprise_tls._MERGED_BUNDLE = None
    enterprise_tls._MERGE_ATTEMPTED = False
    bundle2 = enterprise_tls.merged_ca_bundle_path()
    assert open(bundle2).read().count("UmVxMQ==") == 1


def test_configure_leaves_env_untouched_when_nothing_to_merge(
    tmp_path, monkeypatch
) -> None:
    """Without extra anchors the stock resolution stays in charge; no bogus files."""
    _monkey_no_extra_anchors(monkeypatch)
    monkeypatch.setattr(enterprise_tls, "_stock_ca_pem", lambda: b"")
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)

    enterprise_tls.configure_enterprise_tls_env()

    for name in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
        assert name not in os.environ


def _monkey_no_extra_anchors(monkeypatch) -> None:
    monkeypatch.setattr(enterprise_tls, "_SYSTEM_CA_FILES", ())
    monkeypatch.setattr(enterprise_tls, "_export_macos_keychain_roots", lambda: None)
