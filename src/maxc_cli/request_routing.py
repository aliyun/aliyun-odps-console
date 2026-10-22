"""Per-invocation routing and durable bearer job context (no credential storage)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

from .bearer_auth import validate_bearer_endpoints
from .catalog_bootstrap import region_to_endpoint
from .exceptions import ValidationError
from .store import JobStore


def credential_source(config):
    bearer = config.auth.bearer
    # Rotation does not change identity. Switching profile/file does.
    path = (
        Path(bearer.config_path) if bearer.config_path else Path.home() / ".aliyun" / "config.json"
    )
    identity = [bearer.profile, str(path.resolve())]
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def resolve_request(config, *, region=None, project=None, endpoint=None, job_id=None):
    is_bearer = (config.auth.provider or "").strip().lower() == "bearer"
    saved = None
    if is_bearer and job_id:
        # Do not create state for an unknown legacy job merely by looking it up.
        if (config.state_dir / "jobs.json").exists():
            record = JobStore(config.state_dir).get_remote_job_context(job_id)
            saved = (record or {}).get("connection")
        if saved:
            if saved.get("credential_source") != credential_source(config):
                raise ValidationError(
                    "Job belongs to a different credential source; use the original CLI profile."
                )
            for name, value in [("region", region), ("project", project), ("endpoint", endpoint)]:
                if value is not None and value != saved.get(name):
                    raise ValidationError(
                        f"Job {name} conflicts with its saved submission context."
                    )
            region, project, endpoint = saved["region"], saved["project"], saved["endpoint"]
        elif not (region and project):
            raise ValidationError(
                "This job has no saved regional context; specify its original --region and --project."
            )
    if not (region or endpoint or is_bearer):
        return None
    selected_region = region or config.auth.region_name or config.default_region
    selected_project = project or config.default_project or config.auth.project
    # An explicit region replaces all endpoint defaults from another region.
    selected_endpoint = endpoint or (
        region_to_endpoint(region)
        if region
        else config.auth.endpoint or region_to_endpoint(selected_region)
    )
    if not selected_project or not selected_endpoint:
        raise ValidationError(
            "Request requires a project and a supported region or explicit endpoint."
        )
    if is_bearer:
        validate_bearer_endpoints(selected_endpoint)
    config.auth = replace(
        config.auth,
        project=selected_project,
        region_name=selected_region,
        endpoint=selected_endpoint,
        tunnel_endpoint=(
            saved.get("tunnel_endpoint")
            if saved
            else None
            if region
            else config.auth.tunnel_endpoint
        ),
        catalog_endpoint=(
            saved.get("catalog_endpoint")
            if saved
            else None
            if region
            else config.auth.catalog_endpoint
        ),
    )
    config.command_connection = {
        "region_name": selected_region,
        "endpoint": selected_endpoint,
        "tunnel_endpoint": config.auth.tunnel_endpoint,
        "catalog_endpoint": config.auth.catalog_endpoint,
    }
    config.default_region = selected_region
    config.default_project = selected_project
    config.command_project = selected_project
    route = dict(
        region=selected_region,
        project=selected_project,
        endpoint=selected_endpoint,
        tunnel_endpoint=config.auth.tunnel_endpoint,
        catalog_endpoint=config.auth.catalog_endpoint,
    )
    if is_bearer:
        route["credential_source"] = credential_source(config)
        # Metadata/idempotency caches must not mix regions or CLI identities.
        scope = hashlib.sha256(json.dumps(route, sort_keys=True).encode()).hexdigest()[:24]
        config.cache_dir = config.cache_dir / "bearer" / scope
        return route
    return None


class RoutedJobStore(JobStore):
    def __init__(self, state_dir, connection):
        super().__init__(state_dir)
        self.connection = dict(connection)

    def save_remote_job_context(self, job_id, context):
        super().save_remote_job_context(job_id, {**context, "connection": self.connection})

    def get_remote_job_context(self, job_id):
        record = super().get_remote_job_context(job_id)
        if record and record.get("connection") and record["connection"] != self.connection:
            raise ValidationError(
                "Job context differs from the active region/project/credential source."
            )
        return record
