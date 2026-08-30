"""Auth-related mixin for OdpsBackend."""

import json
from typing import Any
from urllib.parse import quote
from xml.etree import ElementTree

from ..exceptions import ValidationError
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
        schema: 'str | None' = None,
    ) -> 'tuple[bool, str]':
        """Call the schema-aware MaxCompute checkPermission REST API.

        This mirrors the current Java SDK ``SecurityManager#checkPermission``
        contract: POST a resource descriptor and pass ``curr_schema`` for
        three-tier namespaces. The deprecated GET form treats
        ``schema.table`` as ``project.table`` and can report a false denial.

        Args:
            object_name: Unqualified object name within the resolved project/schema.
            object_type: MaxCompute object type.
            action: Permission action to evaluate.
            project: Resolved project containing the object.
            schema: Optional schema for a three-tier project.

        Returns:
            Tuple of (allowed: bool, message: str).
        """
        rest = self.client.rest
        endpoint = rest.endpoint
        url = f"{endpoint}/projects/{quote(project, safe='')}/auth/"
        segment_by_type = {
            "Project": None,
            "Schema": "schemas",
            "Table": "tables",
            "Function": "functions",
            "Resource": "resources",
            "Instance": "instances",
        }
        try:
            segment = segment_by_type[object_type]
        except KeyError as exc:
            raise ValidationError(
                f"Unsupported permission object type: {object_type}",
                suggestion="Run `maxc auth can-i --help` to inspect supported object types.",
            ) from exc

        if segment is None:
            resource = f"/projects/{project}"
        elif object_type == "Schema":
            resource = f"/projects/{project}/schemas/{object_name}"
        else:
            resource = f"/projects/{project}/{segment}/{object_name}"

        params = {"curr_schema": schema} if schema else {}
        # The Java SDK removes the leading slash when curr_schema is present.
        if schema:
            resource = resource.lstrip("/")
        body = json.dumps(
            [{"Action": action, "Resource": resource}],
            separators=(",", ":"),
        )
        resp = rest.post(
            url,
            data=body,
            params=params,
            headers={"Content-Type": "application/json"},
        )
        root = ElementTree.fromstring(resp.content)
        result = (root.findtext("Result") or "").strip()
        raw_message = (root.findtext("Message") or "").strip()
        try:
            parsed = json.loads(raw_message)
            message = parsed.get("message", "") if isinstance(parsed, dict) else raw_message
        except (ValueError, TypeError):
            message = raw_message
        return result.upper() == "ALLOW", message

    def _resolve_permission_target(
        self,
        *,
        object_name: 'str',
        object_type: 'str',
        project: 'str | None',
        schema: 'str | None',
    ) -> 'tuple[str, str, str | None, str]':
        """Resolve a permission target without relying on server error guesses.

        Table-like names accept ``table``, ``schema.table`` and
        ``project.schema.table``. A two-part name whose prefix equals the
        selected project is treated as ``project.table``; other two-part names
        are treated as ``schema.table``. Cross-project checks should use the
        explicit ``--project`` flag to avoid two-tier/three-tier ambiguity.

        Args:
            object_name: User-supplied object identifier.
            object_type: MaxCompute object type.
            project: Explicit project override, if any.
            schema: Explicit schema override, if any.

        Returns:
            Tuple of (project, unqualified object, schema, qualified name).
        """
        target_project = project or self.project
        target_schema = schema
        resolved_name = object_name

        if object_type == "Project":
            if schema:
                raise ValidationError("--schema cannot be used when --type=Project.")
            if project and object_name != project:
                raise ValidationError(
                    f"Conflicting project names: --object={object_name!r} and --project={project!r}.",
                    suggestion="Pass the same project to both flags, or omit --project.",
                )
            target_project = object_name
            return target_project, object_name, None, target_project

        if object_type == "Schema":
            if schema and schema != object_name:
                raise ValidationError(
                    f"Conflicting schema names: --object={object_name!r} and --schema={schema!r}.",
                    suggestion="Pass the schema as --object and omit --schema.",
                )
            if "." in object_name:
                parts = object_name.split(".")
                if len(parts) != 2:
                    raise ValidationError(
                        f"Invalid schema name: {object_name}",
                        suggestion="Use `schema` or `project.schema`.",
                    )
                qualified_project, resolved_name = parts
                if project and qualified_project != project:
                    raise ValidationError(
                        f"Conflicting project names: {qualified_project!r} and {project!r}.",
                        suggestion="Use one project name consistently.",
                    )
                target_project = qualified_project
            return target_project, resolved_name, None, f"{target_project}.{resolved_name}"

        # Resource names commonly contain dots (for example ``udf.jar``), so
        # only table/function identifiers use dot qualification here. Other
        # object types can still select a schema explicitly with --schema.
        if object_type in {"Table", "Function"}:
            if object_type == "Table":
                quote_table_name(object_name)
            parts = object_name.split(".")
            if len(parts) == 3:
                qualified_project, qualified_schema, resolved_name = parts
                if project and qualified_project != project:
                    raise ValidationError(
                        f"Conflicting project names: {qualified_project!r} and {project!r}.",
                        suggestion="Use one project name consistently.",
                    )
                if schema and qualified_schema != schema:
                    raise ValidationError(
                        f"Conflicting schema names: {qualified_schema!r} and {schema!r}.",
                        suggestion="Use one schema name consistently.",
                    )
                target_project = qualified_project
                target_schema = qualified_schema
            elif len(parts) == 2:
                qualifier, resolved_name = parts
                if qualifier == target_project and not schema:
                    target_schema = None
                else:
                    if schema and qualifier != schema:
                        raise ValidationError(
                            f"Conflicting schema names: {qualifier!r} and {schema!r}.",
                            suggestion="Use one schema name consistently.",
                        )
                    target_schema = qualifier
            elif len(parts) != 1:
                raise ValidationError(f"Invalid {object_type.lower()} name: {object_name}")

        if target_schema:
            qualified_name = f"{target_schema}.{resolved_name}"
        elif target_project and object_type in {"Table", "Function"} and "." in object_name:
            qualified_name = f"{target_project}.{resolved_name}"
        else:
            qualified_name = resolved_name
        return target_project, resolved_name, target_schema, qualified_name

    def can_i_info(
        self,
        *,
        object_name: 'str',
        object_type: 'str' = "Table",
        operation: 'str',
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'tuple[dict[str, Any], list[str]]':
        """Check if a specific operation is allowed on an object.

        Calls the MaxCompute checkPermission REST API directly.

        Args:
            object_name: Object name to check (table name, function name, etc.).
            object_type: Object type (Table, Project, Schema, Function, Resource, Instance).
            operation: ODPS ActionType (e.g. "Select", "CreateInstance").
            project: Optional project override.
            schema: Optional schema override for three-tier projects.

        Returns:
            Tuple of (permission payload dict, warnings list).
        """
        target_project, resolved_name, target_schema, qualified_name = (
            self._resolve_permission_target(
                object_name=object_name,
                object_type=object_type,
                project=project,
                schema=schema,
            )
        )

        try:
            allowed, message = self._check_permission(
                object_name=resolved_name,
                object_type=object_type,
                action=operation,
                project=target_project,
                schema=target_schema,
            )
        except Exception as exc:
            if isinstance(exc, ValidationError):
                raise
            raise translate_odps_error(exc, "check_permission") from exc

        return (
            {
                "object_type": object_type,
                "object_name": object_name,
                "resolved_object_name": resolved_name,
                "qualified_name": qualified_name,
                "project": target_project,
                "schema": target_schema,
                "operation": operation,
                "allowed": allowed,
                "check_mode": "odps_check_permission_api_v2",
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
