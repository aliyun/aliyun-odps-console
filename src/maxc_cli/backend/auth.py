"""Auth-related mixin for OdpsBackend."""

from typing import Any

from ..exceptions import BackendConnectionError, MaxCError
from ..helpers import (
    build_odps_identity_payload,
    odps_identity_source,
    quote_table_name,
    translate_odps_error,
)


class AuthMixin:
    """Mixin providing authentication and authorization methods."""

    def whoami_info(self, *, project: str | None = None) -> tuple[dict[str, Any], list[str]]:
        """Get current identity info."""
        owner_display_name = self._get_owner_display_name()
        return build_odps_identity_payload(
            client=self.client,
            settings=self.settings,
            allowed_operations=self.config.allowed_operations,
            identity_source=odps_identity_source(self.setting_sources),
            project=project or self.project,
            owner_display_name=owner_display_name,
        )

    def can_i_info(
        self,
        *,
        table_name: str,
        operation: str,
        project: str | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        """Check if operation is allowed on table."""
        normalized_operation = operation.upper().strip()
        target_project = project or self.project
        if normalized_operation not in self.config.allowed_operations:
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "config_allowed_operations",
                    "reason": f"当前配置仅允许 {', '.join(self.config.allowed_operations)}。",
                    "check_error_code": "PERMISSION_DENIED",
                },
                [],
            )
        if normalized_operation != "SELECT":
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "cli_supported_operations",
                    "reason": "当前版本只支持 SELECT 读路径权限探测。",
                    "check_error_code": "FEATURE_UNAVAILABLE",
                },
                [],
            )

        safe_table_name = quote_table_name(table_name)
        sql = f"SELECT * FROM {safe_table_name} LIMIT 0"
        try:
            self._get_table(table_name, project=target_project)
            self.client.execute_sql_cost(sql, project=target_project)
        except MaxCError as exc:
            if isinstance(exc, BackendConnectionError):
                raise
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "odps_sql_cost_limit_0",
                    "reason": exc.message,
                    "check_error_code": exc.error_code,
                },
                [],
            )
        except Exception as exc:
            translated = translate_odps_error(exc)
            if isinstance(translated, BackendConnectionError):
                raise translated
            return (
                {
                    "resource_type": "table",
                    "table_name": table_name,
                    "project": target_project,
                    "operation": normalized_operation,
                    "allowed": False,
                    "check_mode": "odps_sql_cost_limit_0",
                    "reason": translated.message,
                    "check_error_code": translated.error_code,
                },
                [],
            )

        return (
            {
                "resource_type": "table",
                "table_name": table_name,
                "project": target_project,
                "operation": normalized_operation,
                "allowed": True,
                "check_mode": "odps_sql_cost_limit_0",
                "reason": "已通过元数据访问和 LIMIT 0 读路径预检。",
                "check_error_code": None,
            },
            [],
        )

    def _get_owner_display_name(self) -> str | None:
        """Get the current user's display name (e.g., 'ALIYUN$xxx' or 'RAM$xxx')."""
        if self._owner_display_name is not None:
            return self._owner_display_name

        # 方法1: 从项目 owner 获取
        try:
            project = self.client.get_project(self.project)
            owner = getattr(project, "owner", None)
            if owner:
                self._owner_display_name = owner
                return owner
        except Exception:
            pass

        # 方法2: 使用 execute_security_query
        try:
            result = self.client.execute_security_query("whoami", project=self.project)
            # result 可能是 dict 或有 raw 属性的对象
            if isinstance(result, dict):
                display_name = result.get("DisplayName")
            elif hasattr(result, "raw"):
                import json as json_module
                data = json_module.loads(result.raw)
                display_name = data.get("DisplayName")
            else:
                display_name = None
            if display_name:
                self._owner_display_name = display_name
                return display_name
        except Exception:
            pass

        return None
