"""Owner-only, short-lived state for multi-step authentication flows."""

from __future__ import annotations

import json
import os
import re
import secrets
import time
from pathlib import Path
from typing import Any

from .exceptions import ValidationError
from .state_permissions import ensure_private_directory, open_private_file

AUTH_CONTINUATION_TTL_SECONDS = 600
_CONTINUATION_ID_RE = re.compile(r"[0-9a-f]{64}")
_CONTINUATION_KIND_RE = re.compile(r"[a-z][a-z0-9_-]{0,31}")


def _directory(state_dir: Path) -> Path:
    directory = state_dir / "auth-continuations"
    if directory.is_symlink():
        raise ValidationError(
            "Refusing to use a symbolic link as the auth continuation directory."
        )
    ensure_private_directory(directory, secure_existing=True)
    return directory


def _path(
    state_dir: Path,
    continuation_id: str,
    *,
    suffix: str = ".json",
) -> Path:
    if not _CONTINUATION_ID_RE.fullmatch(continuation_id or ""):
        raise ValidationError(
            "The auth continuation identifier is invalid or incomplete.",
            suggestion="Restart the authentication command.",
        )
    return _directory(state_dir) / f"{continuation_id}{suffix}"


def save_auth_continuation(
    state_dir: Path,
    *,
    kind: str,
    target_config_path: Path,
    secret_payload: dict[str, Any],
    now: float | None = None,
) -> tuple[str, int]:
    """Create a mode-0600 continuation and return its random bearer ID."""
    if not _CONTINUATION_KIND_RE.fullmatch(kind or ""):
        raise ValidationError("Invalid auth continuation kind.")
    created_at = int(time.time() if now is None else now)
    expires_at = created_at + AUTH_CONTINUATION_TTL_SECONDS
    payload = {
        "version": 1,
        "kind": kind,
        "created_at": created_at,
        "expires_at": expires_at,
        "target_config_path": str(target_config_path.expanduser().resolve()),
        "secret_payload": secret_payload,
    }
    directory = _directory(state_dir)
    for _ in range(4):
        continuation_id = secrets.token_hex(32)
        continuation_path = directory / f"{continuation_id}.json"
        try:
            descriptor = open_private_file(
                continuation_path,
                os.O_WRONLY | os.O_EXCL,
                create=True,
            )
        except FileExistsError:
            continue
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=True, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
        except Exception:
            continuation_path.unlink(missing_ok=True)
            raise
        return continuation_id, expires_at
    raise ValidationError("Could not allocate a unique auth continuation identifier.")


def load_auth_continuation(
    state_dir: Path,
    continuation_id: str,
    *,
    kind: str,
    target_config_path: Path,
    now: float | None = None,
) -> dict[str, Any]:
    """Load a continuation bound to its kind, config path, and expiry."""
    continuation_path = _path(state_dir, continuation_id)
    claimed_path = _path(state_dir, continuation_id, suffix=".claimed")
    try:
        # A fixed-name hard-link is an atomic no-clobber claim. Concurrent
        # actions may all possess the same bearer ID, but only one can create
        # `.claimed`; the rest fail without reading credential material.
        os.link(continuation_path, claimed_path, follow_symlinks=False)
        continuation_path.unlink()
    except (FileExistsError, FileNotFoundError) as exc:
        raise ValidationError(
            "The auth continuation was not found or has already been used.",
            suggestion="Restart the authentication command.",
        ) from exc
    try:
        descriptor = open_private_file(claimed_path, os.O_RDONLY)
    except OSError as exc:
        raise ValidationError(
            "The auth continuation state could not be claimed safely.",
            suggestion="Restart the authentication command.",
        ) from exc
    try:
        with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, ValueError, TypeError) as exc:
        raise ValidationError(
            "The auth continuation state is unreadable or invalid.",
            suggestion="Restart the authentication command.",
        ) from exc

    current = int(time.time() if now is None else now)
    try:
        expires_at = int(payload["expires_at"])
        recorded_kind = str(payload["kind"])
        recorded_config = Path(str(payload["target_config_path"])).expanduser().resolve()
        secret_payload = payload["secret_payload"]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError(
            "The auth continuation state is incomplete.",
            suggestion="Restart the authentication command.",
        ) from exc
    if not isinstance(secret_payload, dict):
        raise ValidationError(
            "The auth continuation state is incomplete.",
            suggestion="Restart the authentication command.",
        )
    if current >= expires_at:
        claimed_path.unlink(missing_ok=True)
        raise ValidationError(
            "The auth continuation expired before authentication completed.",
            suggestion="Restart the authentication command.",
        )
    if recorded_kind != kind:
        raise ValidationError(
            "The auth continuation belongs to a different authentication flow.",
            suggestion="Use the exact suggested action or restart authentication.",
        )
    if recorded_config != target_config_path.expanduser().resolve():
        raise ValidationError(
            "The auth continuation belongs to a different --config path.",
            suggestion="Use the --config path from the suggested action.",
        )
    return secret_payload


def delete_auth_continuation(state_dir: Path, continuation_id: str) -> None:
    """Delete a successfully consumed continuation without following links."""
    _path(state_dir, continuation_id).unlink(missing_ok=True)
    _path(state_dir, continuation_id, suffix=".claimed").unlink(missing_ok=True)


def clear_auth_continuations(
    state_dir: Path,
    *,
    target_config_path: Path,
    now: float | None = None,
) -> tuple[int, int, int]:
    """Remove continuations for one config plus globally expired entries.

    The directory is intentionally not created by logout when no continuation
    state exists. Invalid or unreadable entries are left untouched and counted
    as failures so logout can remain successful without pretending cleanup was
    complete.
    """
    directory = state_dir / "auth-continuations"
    if not os.path.lexists(directory):
        return 0, 0, 0
    if directory.is_symlink():
        raise ValidationError(
            "Refusing to use a symbolic link as the auth continuation directory."
        )
    ensure_private_directory(directory, secure_existing=True)

    target_config = target_config_path.expanduser().resolve()
    current = int(time.time() if now is None else now)
    removed_for_config = 0
    removed_expired = 0
    failures = 0
    candidates = [*directory.glob("*.json"), *directory.glob("*.claimed")]
    for continuation_path in candidates:
        if not _CONTINUATION_ID_RE.fullmatch(continuation_path.stem):
            continue
        try:
            descriptor = open_private_file(continuation_path, os.O_RDONLY)
            with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
                payload = json.load(stream)
            recorded_config = Path(
                str(payload["target_config_path"])
            ).expanduser().resolve()
            expired = current >= int(payload["expires_at"])
            matches_target = recorded_config == target_config
            if not (expired or matches_target):
                continue
            continuation_path.unlink(missing_ok=True)
            if matches_target:
                removed_for_config += 1
            else:
                removed_expired += 1
        except (OSError, ValueError, TypeError, KeyError):
            failures += 1
    return removed_for_config, removed_expired, failures
