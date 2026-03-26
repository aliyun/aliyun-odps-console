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
        target_project = project or self.project
        try:
            result = self.client.execute_security_query("whoami", project=target_project)
        except Exception as exc:
            raise translate_odps_error(exc, "whoami") from exc

        owner_display_name = result.get("DisplayName") if isinstance(result, dict) else None
        if owner_display_name:
            self._owner_display_name = owner_display_name
        return build_odps_identity_payload(
            client=self.client,
            settings=self.settings,
            allowed_operations=self.config.allowed_operations,
            identity_source=odps_identity_source(self.setting_sources),
            auth_type=getattr(self.resolved_auth, "auth_type", "access_key"),
            token_expires_at=getattr(self.resolved_auth, "token_expires_at", None),
            project=target_project,
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
                    "reason": f"Configured allowed operations are limited to {', '.join(self.config.allowed_operations)}.",
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
                    "reason": "This CLI currently supports only SELECT read-path permission checks.",
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
                    "reason": "Metadata access and LIMIT 0 read-path preflight both succeeded.",
                    "check_error_code": None,
                },
                [],
        )

    def _get_owner_display_name(self) -> str | None:
        """Get the current user's display name (e.g., 'ALIYUN$xxx' or 'RAM$xxx')."""
        if self._owner_display_name is not None:
            return self._owner_display_name
        try:
            result = self.client.execute_security_query("whoami", project=self.project)
            display_name = result.get("DisplayName") if isinstance(result, dict) else None
            if display_name:
                self._owner_display_name = display_name
                return display_name
        except Exception:
            pass

        return None
