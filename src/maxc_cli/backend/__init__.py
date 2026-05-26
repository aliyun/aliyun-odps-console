"""Backend module - ODPS implementation."""

# Re-export ODPS_ENV_ALIASES for backward compatibility
from ..helpers import ODPS_ENV_ALIASES
from .odps import OdpsBackend

__all__ = ["OdpsBackend", "ODPS_ENV_ALIASES"]
