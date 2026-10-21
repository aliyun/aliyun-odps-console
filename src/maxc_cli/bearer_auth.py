"""Native ODPS bearer signing backed by a CLI-owned OAuth credential process."""

from __future__ import annotations

import copy
import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from .catalog_bootstrap import region_to_endpoint
from .exceptions import ValidationError


@dataclass
class BearerAuthConfig:
    cli: str = "aliyun"
    profile: str = ""
    config_path: str | None = None
    issuer_region: str = ""
    issuer_project: str = ""
    policy: dict[str, Any] = field(default_factory=dict)
    lifetime_hours: int = 1
    timeout_seconds: int = 90

    @classmethod
    def from_mapping(cls, value):
        if value is None:
            return cls()
        if not isinstance(value, dict):
            raise ValidationError("auth.bearer must be an object.")
        if set(value) - set(cls.__dataclass_fields__):
            raise ValidationError("Unknown auth.bearer field.")
        obj = cls(**value)
        obj.validate()
        return obj

    def validate(self):
        for name in ("cli", "profile", "issuer_region", "issuer_project"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or value.startswith("-"):
                raise ValidationError(f"auth.bearer.{name} must be a nonempty value.")
        if self.config_path is not None and (
            not isinstance(self.config_path, str) or not os.path.isabs(self.config_path)
        ):
            raise ValidationError("auth.bearer.config_path must be absolute.")
        if region_to_endpoint(self.issuer_region) is None:
            raise ValidationError("Unsupported bearer issuer_region.")
        if type(self.lifetime_hours) is not int or not 1 <= self.lifetime_hours <= 24:
            raise ValidationError("Bearer lifetime_hours must be between 1 and 24.")
        if type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 300:
            raise ValidationError("Bearer timeout_seconds must be between 1 and 300.")
        if (
            not isinstance(self.policy, dict)
            or self.policy.get("Version") != "1"
            or not isinstance(self.policy.get("Statement"), list)
            or not self.policy["Statement"]
        ):
            raise ValidationError(
                "An explicit ODPS bearer policy with Version 1 and Statement is required."
            )
        # Policies are explicit configuration, never inferred or silently widened.
        try:
            json.dumps(self.policy, allow_nan=False)
        except (ValueError, TypeError):
            raise ValidationError("Bearer policy must be JSON serializable.") from None

    def to_mapping(self):
        return {k: v for k, v in vars(self).items() if v is not None}


def validate_bearer_endpoints(*endpoints, allow_query=False):
    """Bearer capabilities must never travel over a plaintext custom endpoint."""
    for endpoint in endpoints:
        if endpoint is None:
            continue
        try:
            parsed = urlparse(endpoint)
            valid = (
                parsed.scheme == "https"
                and parsed.hostname
                and not parsed.username
                and not parsed.password
                and (allow_query or not parsed.query)
                and not parsed.fragment
            )
            # Access port to validate malformed / out-of-range port syntax too.
            parsed.port
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise ValidationError(
                "Bearer data, Tunnel and Catalog endpoints must use HTTPS without embedded credentials or query."
            )


class _SecretLogFilter(logging.Filter):
    def filter(self, record):
        # Upstream accounts logs tokens at INFO and signed headers at DEBUG.
        if record.name in {"odps.rest", "odps.accounts"} and record.levelno <= logging.DEBUG:
            return False
        return not any(
            marker in str(record.msg).lower()
            for marker in ("token reloaded", "headers after signing")
        )


_SECRET_FILTER = _SecretLogFilter()


def oauth_sts(config: BearerAuthConfig):
    command = [config.cli, "configure", "oauth-credential", "--profile", config.profile]
    if config.config_path:
        command += ["--config-path", config.config_path]
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ALIBABA_", "ALIBABACLOUD_", "ALIYUN_", "ODPS_", "MAXCOMPUTE_"))
    }
    try:
        proc = subprocess.run(
            command, capture_output=True, timeout=config.timeout_seconds, env=env, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ValidationError(
            "OAuth credential process failed or timed out; check the configured CLI and profile."
        ) from None
    if proc.returncode:
        raise ValidationError(
            "OAuth credential process failed; a CLI with configure oauth-credential is required. Reauthorize the selected profile if needed."
        )
    try:
        data = json.loads(proc.stdout)
        if data.get("Version") != 1 or data.get("Type") != "sts":
            raise ValueError()
        if not all(
            isinstance(data.get(k), str) and data[k]
            for k in ("AccessKeyId", "AccessKeySecret", "SecurityToken", "Expiration")
        ):
            raise ValueError()
        expiry = datetime.fromisoformat(data["Expiration"].replace("Z", "+00:00"))
        if (expiry - datetime.now(timezone.utc)).total_seconds() <= 60:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise ValidationError(
            "OAuth credential process returned an invalid or expired STS response."
        ) from None
    return data


def create_bearer_account(config: BearerAuthConfig):
    config = BearerAuthConfig.from_mapping(copy.deepcopy(config.to_mapping()))
    from .enterprise_tls import configure_enterprise_tls_env

    configure_enterprise_tls_env()
    import re

    from odps import ODPS, __version__

    version = re.match(r"^(\d+)\.(\d+)\.(\d+)", __version__)
    if not version or tuple(map(int, version.groups())) < (0, 13, 2):
        raise ValidationError("Bearer provider requires PyODPS >= 0.13.2.")
    from odps.accounts import BearerTokenAccount, StsAccount

    logging.getLogger("odps.accounts").addFilter(_SECRET_FILTER)
    logging.getLogger("odps.rest").addFilter(_SECRET_FILTER)

    class CLISTS(StsAccount):
        def _reload_account(self):
            # REST can force reload after server rejection. Never let that
            # switch an explicit CLI snapshot to ODPS_STS_* environment values.
            return None

    def issue():
        data = oauth_sts(config)
        # Immutable per-issuance snapshot. No environment-driven STS reload.
        account = CLISTS(
            data["AccessKeyId"], data["AccessKeySecret"], data["SecurityToken"], expired_hours=None
        )
        client = ODPS(
            account=account,
            project=config.issuer_project,
            endpoint=region_to_endpoint(config.issuer_region),
            region_name=config.issuer_region,
        )
        try:
            token = client.get_project().generate_auth_token(
                config.policy, "bearer", config.lifetime_hours
            )
            if not isinstance(token, str) or not token:
                raise ValueError("empty token")
            return token
        except Exception:
            # SDK/server diagnostics can include signed requests and identities.
            raise ValidationError(
                "ODPS bearer issuance failed; check issuer access and the configured policy. No alternative identity was used."
            ) from None

    # The official account owns the in-process double-checked refresh lock.
    # Every process has its own bearer; the Go process owns cross-process OAuth.
    class RefreshableBearer(BearerTokenAccount):
        def sign_request(self, req, endpoint, region_name=None):
            # SDK-discovered endpoints and global SDK options bypass config
            # validation, so enforce transport again before attaching a token.
            validate_bearer_endpoints(endpoint)
            validate_bearer_endpoints(req.url, allow_query=True)
            return super().sign_request(req, endpoint, region_name=region_name)

        def reload(self, force=False):
            # PyODPS advances its timestamp before calling the provider. A failed
            # early refresh must not make a stale token appear fresh next time.
            with self._refresh_lock:
                previous = self._last_refresh_time
                try:
                    return super().reload(force=force)
                except Exception:
                    self._last_refresh_time = previous
                    raise

    return RefreshableBearer(get_bearer_token_fun=issue, expired_hours=config.lifetime_hours * 0.75)
