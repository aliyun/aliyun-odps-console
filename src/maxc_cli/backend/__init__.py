"""Backend module initialization."""

from .base import BaseBackend
from .odps import OdpsBackend
from .factory import create_backend, odps_settings_available

# Re-export ODPS_ENV_ALIASES for backward compatibility
from ..helpers import ODPS_ENV_ALIASES

__all__ = [
    "BaseBackend",
    "OdpsBackend",
    "create_backend",
    "odps_settings_available",
    "ODPS_ENV_ALIASES",
]
