"""Test configuration for maxc-cli."""

import pytest


@pytest.fixture
def fake_odps(monkeypatch):
    """Fixture to mock ODPS client for testing."""
    from tests.test_cli_mock import FakeODPS, clear_odps_env
    import maxc_cli.backend as backend_module

    clear_odps_env(monkeypatch)
    monkeypatch.setattr(backend_module, "ODPS", FakeODPS)
    return FakeODPS
