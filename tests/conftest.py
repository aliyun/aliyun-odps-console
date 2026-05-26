"""Test configuration for maxc-cli."""

import pytest


@pytest.fixture
def fake_odps(monkeypatch):
    """Fixture to mock ODPS client for testing."""
    import maxc_cli.backend as backend_module
    from tests.test_cli_mock import FakeODPS, clear_odps_env

    clear_odps_env(monkeypatch)
    monkeypatch.setattr(backend_module, "ODPS", FakeODPS)
    return FakeODPS
