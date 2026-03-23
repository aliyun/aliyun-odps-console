"""Backend factory functions."""

from ..config import MaxCConfig
from ..exceptions import FeatureUnavailableError, ValidationError
from ..helpers import missing_odps_settings, resolve_odps_settings
from .odps import OdpsBackend


def create_backend(config: MaxCConfig) -> OdpsBackend:
    """Create a backend instance based on configuration.

    Note: MockBackend has been removed. Only OdpsBackend is supported.
    """
    backend_type = config.backend.type.lower()

    if backend_type in {"auto", "odps", "maxcompute"}:
        if not odps_settings_available(config):
            raise ValidationError(
                "未检测到 MaxCompute 连接配置。",
                suggestion=(
                    "请配置 MaxCompute 认证信息（执行 maxc auth login），"
                    "或设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID, "
                    "ALIBABA_CLOUD_ACCESS_KEY_SECRET, MAXCOMPUTE_PROJECT, "
                    "MAXCOMPUTE_ENDPOINT。"
                ),
            )
        return OdpsBackend(config)

    raise FeatureUnavailableError(f"不支持的 backend 类型: {config.backend.type}")


def odps_settings_available(config: MaxCConfig) -> bool:
    """Check if ODPS settings are available."""
    settings, _ = resolve_odps_settings(config)
    return bool(
        settings.get("access_id")
        and settings.get("secret_access_key")
        and settings.get("project")
        and settings.get("endpoint")
    )
