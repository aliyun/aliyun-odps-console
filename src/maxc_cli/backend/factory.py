"""Backend factory functions."""

from ..auth_providers import auth_settings_available
from ..config import MaxCConfig
from ..exceptions import FeatureUnavailableError, ValidationError
from .odps import OdpsBackend


def create_backend(config: MaxCConfig) -> OdpsBackend:
    """Create a backend instance based on configuration.

    Note: MockBackend has been removed. Only OdpsBackend is supported.
    """
    backend_type = config.backend.type.lower()

    if backend_type in {"auto", "odps", "maxcompute"}:
        if not odps_settings_available(config):
            raise ValidationError(
                "No MaxCompute authentication configuration was detected.",
                suggestion=(
                    "Run `maxc auth login`, `maxc auth login-ncs`, or set the required environment variables."
                ),
            )
        return OdpsBackend(config)

    raise FeatureUnavailableError(f"Unsupported backend type: {config.backend.type}")


def odps_settings_available(config: MaxCConfig) -> bool:
    """Check if ODPS auth settings are available for any supported provider."""
    return auth_settings_available(config)
