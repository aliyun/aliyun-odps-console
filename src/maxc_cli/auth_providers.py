from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shlex
import shutil
import subprocess
from typing import Any

from .config import AuthConfig, MaxCConfig, NcsAuthConfig
from .exceptions import FeatureUnavailableError, ValidationError
from .helpers import missing_odps_settings, odps_identity_source, resolve_odps_settings


@dataclass(slots=True)
class ResolvedAuthConnection:
    auth_type: str
    provider: str
    project: str
    endpoint: str
    region_name: str | None
    tunnel_endpoint: str | None
    access_id: str | None
    secret_access_key: str | None
    security_token: str | None
    token_expires_at: str | None
    identity_source: str
    settings: dict[str, str | None]
    setting_sources: dict[str, str]
    account: Any | None = None

    def create_client(self):
        try:
            from odps import ODPS
        except ImportError as exc:
            raise FeatureUnavailableError("pyodps is not installed in the current environment.") from exc

        kwargs = {
            "project": self.project,
            "endpoint": self.endpoint,
            "region_name": self.region_name or None,
            "tunnel_endpoint": self.tunnel_endpoint or None,
        }
        if self.account is not None:
            return ODPS(self.account, **kwargs)
        return ODPS(
            access_id=self.access_id,
            secret_access_key=self.secret_access_key,
            **kwargs,
        )


def auth_settings_available(config: MaxCConfig) -> bool:
    settings, _ = resolve_odps_settings(config)
    provider = infer_auth_provider(config, settings)
    try:
        if provider == "ncs":
            return not missing_odps_settings(settings, auth_type="ncs")
        if provider == "sts_token":
            return not missing_odps_settings(settings, auth_type="sts_token")
        return not missing_odps_settings(settings, auth_type="access_key")
    except ValidationError:
        return False


def resolve_auth_connection(
    config: MaxCConfig,
    *,
    auth_override: AuthConfig | None = None,
) -> ResolvedAuthConnection:
    settings, sources = resolve_odps_settings(config, auth_override=auth_override)
    provider = infer_auth_provider(config, settings, auth_override=auth_override)

    if provider == "ncs":
        missing = missing_odps_settings(settings, auth_type="ncs")
        if missing:
            raise ValidationError(
                f"ncs authentication is missing required fields: {', '.join(missing)}.",
                suggestion="Provide project, endpoint, and ncs account configuration before using the ncs provider.",
            )
        account = build_ncs_account(settings)
        return ResolvedAuthConnection(
            auth_type="ncs",
            provider="ncs",
            project=settings["project"] or config.default_project,
            endpoint=settings["endpoint"] or "",
            region_name=settings.get("region_name"),
            tunnel_endpoint=settings.get("tunnel_endpoint"),
            access_id=None,
            secret_access_key=None,
            security_token=None,
            token_expires_at=settings.get("token_expires_at"),
            identity_source=odps_identity_source(sources),
            settings=settings,
            setting_sources=sources,
            account=account,
        )

    if provider == "sts_token":
        missing = missing_odps_settings(settings, auth_type="sts_token")
        if missing:
            raise ValidationError(
                f"STS authentication is missing required fields: {', '.join(missing)}.",
                suggestion="Provide access_id, secret_access_key, security_token, project, and endpoint.",
            )
        try:
            from odps.accounts import StsAccount
        except ImportError as exc:
            raise FeatureUnavailableError("pyodps is not installed in the current environment.") from exc

        account = StsAccount(
            settings["access_id"],
            settings["secret_access_key"],
            settings["security_token"],
        )
        return ResolvedAuthConnection(
            auth_type="sts_token",
            provider="sts_token",
            project=settings["project"] or config.default_project,
            endpoint=settings["endpoint"] or "",
            region_name=settings.get("region_name"),
            tunnel_endpoint=settings.get("tunnel_endpoint"),
            access_id=settings.get("access_id"),
            secret_access_key=settings.get("secret_access_key"),
            security_token=settings.get("security_token"),
            token_expires_at=settings.get("token_expires_at"),
            identity_source=odps_identity_source(sources),
            settings=settings,
            setting_sources=sources,
            account=account,
        )

    missing = missing_odps_settings(settings, auth_type="access_key")
    if missing:
        raise ValidationError(
            f"MaxCompute connection settings are incomplete: {', '.join(missing)}.",
            suggestion="Run `maxc auth login` or set the required environment variables.",
        )
    return ResolvedAuthConnection(
        auth_type="access_key",
        provider="access_key",
        project=settings["project"] or config.default_project,
        endpoint=settings["endpoint"] or "",
        region_name=settings.get("region_name"),
        tunnel_endpoint=settings.get("tunnel_endpoint"),
        access_id=settings.get("access_id"),
        secret_access_key=settings.get("secret_access_key"),
        security_token=None,
        token_expires_at=settings.get("token_expires_at"),
        identity_source=odps_identity_source(sources),
        settings=settings,
        setting_sources=sources,
    )


def infer_auth_provider(
    config: MaxCConfig,
    settings: dict[str, str | None],
    *,
    auth_override: AuthConfig | None = None,
) -> str:
    auth = auth_override or config.auth
    explicit = (auth.provider or settings.get("provider") or "").strip().lower()
    if explicit in {"access_key", "sts_token", "sts", "ncs"}:
        return "sts_token" if explicit == "sts" else explicit
    if settings.get("security_token"):
        return "sts_token"
    if auth.ncs.is_configured() or settings.get("ncs_process_command"):
        return "ncs"
    return "access_key"


def build_ncs_auth_config(
    *,
    account_type: str,
    employee_id: str | None,
    account_name: str | None,
    app_name: str | None,
    process_timeout: int = 20,
) -> NcsAuthConfig:
    normalized = account_type.strip().lower()
    ncs = NcsAuthConfig(
        account_type=normalized,
        employee_id=employee_id,
        account_name=account_name,
        app_name=app_name,
        process_timeout=process_timeout,
    )
    ncs.process_command = build_ncs_process_command_from_config(ncs)
    return ncs


def build_ncs_process_command_from_config(ncs: NcsAuthConfig) -> str:
    account_type = (ncs.account_type or "").strip().lower()
    if account_type == "user":
        if not ncs.employee_id:
            raise ValidationError("ncs account type `user` requires `employee_id`.")
        return "ncs create credential odpsuser --employee-id {id} -o template -t odpscmd".format(
            id=shlex.quote(ncs.employee_id)
        )
    if account_type == "account":
        if not ncs.account_name:
            raise ValidationError("ncs account type `account` requires `account_name`.")
        return "ncs create credential odpsaccount --account-name {name} -o template -t odpscmd".format(
            name=shlex.quote(ncs.account_name)
        )
    if account_type == "app":
        if not ncs.app_name:
            raise ValidationError("ncs account type `app` requires `app_name`.")
        return "ncs create credential odpsapp --app-name {name} -o template -t odpscmd".format(
            name=shlex.quote(ncs.app_name)
        )
    raise ValidationError("ncs account type must be one of: user, account, app.")


def build_ncs_account(settings: dict[str, str | None]):
    if shutil.which("ncs") is None:
        raise FeatureUnavailableError(
            "ncs CLI is not installed or not available on PATH.",
            suggestion="Install ncs, or switch to access key / STS authentication.",
        )
    try:
        from odps.accounts import CredentialProviderAccount
    except ImportError as exc:
        raise FeatureUnavailableError("pyodps is not installed in the current environment.") from exc

    command = settings.get("ncs_process_command")
    if not command:
        command = build_ncs_process_command_from_config(
            NcsAuthConfig(
                account_type=settings.get("ncs_account_type"),
                employee_id=settings.get("ncs_employee_id"),
                account_name=settings.get("ncs_account_name"),
                app_name=settings.get("ncs_app_name"),
                process_timeout=int(settings.get("ncs_process_timeout") or 20),
            )
        )

    timeout = int(settings.get("ncs_process_timeout") or 20)
    return CredentialProviderAccount(NcsCredentialProvider(command=command, timeout=timeout))


class NcsCredentialProvider:
    def __init__(self, *, command: str, timeout: int) -> None:
        self.command = command
        self.timeout = timeout

    def get_credentials(self):
        result = subprocess.run(
            self.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=self.timeout,
        )
        if result.returncode != 0:
            raise FeatureUnavailableError(
                "ncs failed to issue MaxCompute credentials.",
                suggestion=(result.stderr or "Check the ncs configuration and selected account.").strip(),
            )
        payload = parse_ncs_credential_output(result.stdout)
        return SimpleTempCredential(
            access_key_id=payload["access_key_id"],
            access_key_secret=payload["access_key_secret"],
            security_token=payload["security_token"],
        )

    get_credential = get_credentials


@dataclass(slots=True)
class SimpleTempCredential:
    access_key_id: str
    access_key_secret: str
    security_token: str

    def get_access_key_id(self) -> str:
        return self.access_key_id

    def get_access_key_secret(self) -> str:
        return self.access_key_secret

    def get_security_token(self) -> str:
        return self.security_token


def parse_ncs_credential_output(stdout: str) -> dict[str, str]:
    text = stdout.strip()
    if not text:
        raise ValidationError("ncs returned an empty credential payload.")

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        normalized = _normalize_credential_mapping(payload)
        if normalized:
            return normalized

    mapping: dict[str, str] = {}
    for line in text.splitlines():
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        if candidate.startswith("export "):
            candidate = candidate[len("export ") :]
        if "=" not in candidate:
            continue
        key, value = candidate.split("=", 1)
        mapping[key.strip()] = value.strip().strip('"').strip("'")

    normalized = _normalize_credential_mapping(mapping)
    if normalized:
        return normalized

    raise ValidationError(
        "Unable to parse credentials returned by ncs.",
        suggestion="Ensure the ncs process command prints access key id, secret, and security token values.",
    )


def _normalize_credential_mapping(payload: dict[str, Any]) -> dict[str, str] | None:
    candidates = {
        "access_key_id": [
            "accessKeyId",
            "access_key_id",
            "ACCESS_KEY_ID",
            "ODPS_STS_ACCESS_KEY_ID",
            "ALIBABA_CLOUD_ACCESS_KEY_ID",
        ],
        "access_key_secret": [
            "accessKeySecret",
            "access_key_secret",
            "ACCESS_KEY_SECRET",
            "ODPS_STS_ACCESS_KEY_SECRET",
            "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        ],
        "security_token": [
            "securityToken",
            "security_token",
            "SECURITY_TOKEN",
            "ODPS_STS_TOKEN",
            "ALIBABA_CLOUD_SECURITY_TOKEN",
        ],
    }

    normalized: dict[str, str] = {}
    for target, keys in candidates.items():
        for key in keys:
            value = payload.get(key)
            if value:
                normalized[target] = str(value).strip()
                break

    if {"access_key_id", "access_key_secret", "security_token"} <= set(normalized):
        return normalized
    return None


def list_ncs_accounts(account_type: str) -> dict[str, Any]:
    normalized = account_type.strip().lower()
    if shutil.which("ncs") is None:
        raise FeatureUnavailableError(
            "ncs CLI is not installed or not available on PATH.",
            suggestion="Install ncs before listing ncs-backed MaxCompute accounts.",
        )

    commands = {
        "user": "ncs list authorizations odpsuser -o custom-columns=BUC_USER_ID:.extension.bucUserId,BUC_USER_TYPE:.extension.bucUserType,BUC_ACCOUNT_NAME:.extension.bucDomainAccount",
        "account": "ncs list authorizations odpsaccount --scenario app -o custom-columns=accountName:.extension.accountName",
        "app": "ncs list authorizations odpsapp -o custom-columns=AppName:.extension.appName",
    }
    if normalized not in commands:
        raise ValidationError("ncs account type must be one of: user, account, app.")

    result = subprocess.run(
        commands[normalized],
        shell=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if result.returncode != 0:
        raise FeatureUnavailableError(
            "ncs failed while listing available accounts.",
            suggestion=(result.stderr or "Check the ncs CLI setup.").strip(),
        )

    lines = [line.rstrip() for line in result.stdout.splitlines() if line.strip()]
    return {
        "account_type": normalized,
        "raw_lines": lines,
        "raw_output": result.stdout,
    }


def build_auth_options(config_path: Path | None = None) -> list[dict[str, Any]]:
    options = [
        {
            "type": "access_key",
            "description": "Authenticate with a long-lived access key.",
            "command": "auth login --from-env",
        },
        {
            "type": "sts_token",
            "description": "Authenticate with a temporary STS token.",
            "command": "auth login --security-token <token> --access-id <id> --secret-access-key <secret> --project <project> --endpoint <endpoint>",
        },
        {
            "type": "ncs",
            "description": "Authenticate through ncs-issued temporary credentials.",
            "command": "auth login-ncs --interactive",
            "requirements": ["ncs CLI"],
        },
    ]
    if config_path is not None:
        for item in options:
            item["config_path"] = str(config_path)
    return options
