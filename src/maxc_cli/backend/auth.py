"""Auth-related mixin for OdpsBackend."""

from typing import Any
from xml.etree import ElementTree

from ..exceptions import BackendConnectionError, MaxCError
from ..helpers import (
    build_odps_identity_payload,
    odps_identity_source,
    quote_table_name,
    translate_odps_error,
)


class AuthMixin:
    """Mixin providing authentication and authorization methods."""

    def whoami_info(self, *, project: 'str | None' = None) -> 'tuple[dict[str, Any], list[str]]':
        """Get current identity info by executing ``whoami`` security query.

        Calls ``client.execute_security_query("whoami")`` to verify the
        connection and return desensitized identity information.

        Args:
            project: Optional project override.

        Returns:
            Tuple of (identity payload dict, warnings list).
        """
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

    def _check_permission(
        self,
        *,
        object_name: 'str',
        object_type: 'str',
        action: 'str',
        project: 'str',
    ) -> 'tuple[bool, str]':
        """Call the MaxCompute checkPermission REST API.

        Uses GET /projects/{project}/auth/?type={type}&name={name}&grantee={action}
        (same endpoint as Java SDK's SecurityManager#checkPermission).

        Returns:
            Tuple of (allowed: bool, message: str).
        """
        import json as _json

        rest = self.client.rest
        endpoint = rest.endpoint
        url = f"{endpoint}/projects/{project}/auth/"
        params = {
            "type": object_type,
            "name": object_name,
            "grantee": action,
        }
        resp = rest.get(url, params=params)
        root = ElementTree.fromstring(resp.content)
        result = (root.findtext("Result") or "").strip()
        raw_message = (root.findtext("Message") or "").strip()
        try:
            parsed = _json.loads(raw_message)
            message = parsed.get("message", "") if isinstance(parsed, dict) else raw_message
        except (ValueError, TypeError):
            message = raw_message
        return result.upper() == "ALLOW", message

    def can_i_info(
        self,
        *,
        object_name: 'str',
        object_type: 'str' = "Table",
        operation: 'str',
        project: 'str | None' = None,
    ) -> 'tuple[dict[str, Any], list[str]]':
        """Check if a specific operation is allowed on an object.

        Calls the MaxCompute checkPermission REST API directly.

        Args:
            object_name: Object name to check (table name, function name, etc.).
            object_type: Object type (Table, Project, Function, Resource, Instance).
            operation: ODPS ActionType (e.g. "Select", "CreateInstance").
            project: Optional project override.

        Returns:
            Tuple of (permission payload dict, warnings list).
        """
        target_project = project or self.project
        if object_type == "Table":
            quote_table_name(object_name)

        try:
            allowed, message = self._check_permission(
                object_name=object_name,
                object_type=object_type,
                action=operation,
                project=target_project,
            )
        except Exception as exc:
            translated = translate_odps_error(exc)
            if isinstance(translated, BackendConnectionError):
                raise translated
            return (
                {
                    "object_type": object_type,
                    "object_name": object_name,
                    "project": target_project,
                    "operation": operation,
                    "allowed": False,
                    "check_mode": "odps_check_permission_api",
                    "reason": translated.message,
                    "check_error_code": translated.error_code,
                },
                [],
            )

        return (
            {
                "object_type": object_type,
                "object_name": object_name,
                "project": target_project,
                "operation": operation,
                "allowed": allowed,
                "check_mode": "odps_check_permission_api",
                "reason": message if message else ("Allowed." if allowed else "Denied."),
                "check_error_code": None if allowed else "PERMISSION_DENIED",
            },
            [],
        )

    def _get_owner_display_name(self) -> 'str | None':
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
