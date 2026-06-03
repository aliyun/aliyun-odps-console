"""Test configuration for maxc-cli."""

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--eval-model",
        action="store",
        default=None,
        help="Model to use for skill eval tests (default: OPENAI_MODEL env var or gpt-4o)",
    )


@pytest.fixture
def fake_odps(monkeypatch):
    """Fixture to mock ODPS client for testing."""
    import maxc_cli.backend as backend_module
    from tests.test_cli_mock import FakeODPS, clear_odps_env

    clear_odps_env(monkeypatch)
    monkeypatch.setattr(backend_module, "ODPS", FakeODPS)
    return FakeODPS
