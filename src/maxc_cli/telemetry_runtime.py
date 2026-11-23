"""Append command usage locally and start an upload when it is due."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import timedelta
from pathlib import Path
from time import monotonic, time
from urllib.parse import quote
from uuid import uuid4

from .config import default_global_config_path, load_config, load_config_mapping
from .exceptions import MaxCError
from .state_permissions import (
    PrivateDirectoryHandle,
    close_private_directory,
    descriptor_matches_path,
    lock_file_descriptor,
    open_private_directory,
    open_private_file_at,
    replace_private_file_at,
    unlink_private_file_at,
)
from .telemetry import (
    RETENTION_DAYS,
    UPLOAD_ARG,
    decode_batch,
    encode_batch,
    host_opted_out,
    report_day,
)

DEFAULT_UPLOAD_INTERVAL = 1800
UPLOAD_WINDOW_SECONDS = 10
_MAX_UPLOAD_BATCHES = 3
_MAX_USAGE_FILE_BYTES = 256 * 1024
_USAGE_LOCK = "usage.lock"
_UPLOAD_LOCK = "upload.lock"
_USAGE_NAME = re.compile(
    r"(standalone|aliyun)\.(\d{4}-\d{2}-\d{2})(?:\.([0-9a-f-]{36}))?\.usage\Z", re.ASCII,
)


def _maxc_switch() -> bool | None:
    path = default_global_config_path()
    try:
        if path.is_dir():
            return None
        settings = load_config_mapping(path).get("telemetry", {})
        enabled = settings.get("enabled", True) if isinstance(settings, dict) else None
        return enabled if isinstance(enabled, bool) else None
    except (MaxCError, OSError, UnicodeError):
        return None


def _aliyun_switch() -> bool | None:
    path = Path.home() / ".aliyun" / "telemetry.json"
    try:
        if not stat.S_ISREG(path.lstat().st_mode):
            return None
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        try:
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode) or details.st_size > 4096:
                return None
            settings = json.loads(os.read(descriptor, 4096))
        finally:
            os.close(descriptor)
    except FileNotFoundError:
        return True
    except (OSError, ValueError):
        return None
    enabled = settings.get("enabled", True) if isinstance(settings, dict) else None
    return enabled if isinstance(enabled, bool) else None


def read_switches() -> dict[str, bool | None]:
    maxc = _maxc_switch()
    host = _aliyun_switch()
    aliyun = False if maxc is False or host is False else None
    if maxc is True and host is True:
        aliyun = True
    return {"standalone": maxc, "aliyun": aliyun}


@contextmanager
def _usage_directory(*, create: bool) -> Iterator[PrivateDirectoryHandle]:
    state = default_global_config_path().parent / "state" / "telemetry"
    directory = open_private_directory(state, create=create, secure_existing=True)
    try:
        yield directory
    finally:
        close_private_directory(directory)


@contextmanager
def _open_file(
    directory: PrivateDirectoryHandle, name: str, flags: int, *, create: bool = False,
) -> Iterator[int]:
    descriptor = open_private_file_at(directory, name, flags | getattr(os, "O_BINARY", 0), create=create)
    try:
        if directory.descriptor is None and not descriptor_matches_path(directory.path / name, descriptor):
            raise OSError("Telemetry state file was replaced or linked.")
        yield descriptor
    finally:
        os.close(descriptor)


def _launch_upload() -> None:
    import subprocess

    frozen = getattr(sys, "frozen", False)
    executable = [sys.executable] if frozen else [sys.executable, "-m", "maxc_cli"]
    working_dir = Path(sys.executable).parent if frozen else Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    if frozen:
        environment["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
    windows = os.name == "nt"
    subprocess.Popen(
        [*executable, UPLOAD_ARG],
        cwd=working_dir,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=not windows,
        creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP if windows else 0,
    )


def _launch_if_due(directory: PrivateDirectoryHandle) -> None:
    with _open_file(directory, _UPLOAD_LOCK, os.O_RDWR, create=True) as marker:
        state = os.fstat(marker)
        if state.st_size == 0:
            # Start the first interval with a byte in place, so Windows' byte
            # lock does not initialize the file and reset its timestamp when due.
            os.write(marker, b"1")
            return
        if time() - state.st_mtime < DEFAULT_UPLOAD_INTERVAL:
            return
        with lock_file_descriptor(marker, blocking=False):
            if time() - os.fstat(marker).st_mtime >= DEFAULT_UPLOAD_INTERVAL:
                os.lseek(marker, 0, os.SEEK_SET)
                os.write(marker, b"1")
                _launch_upload()


def _prune_usage(
    directory: PrivateDirectoryHandle, switches: dict[str, bool | None], cutoff: str,
) -> list[tuple[str, str, str | None, str]]:
    files = []
    for path in directory.path.iterdir():
        match = _USAGE_NAME.fullmatch(path.name)
        if match is None:
            continue
        scope, day, batch_id = match.groups()
        if switches[scope] is False or day < cutoff:
            with suppress(FileNotFoundError):
                unlink_private_file_at(directory, path.name)
        elif switches[scope] is True:
            files.append((day, scope, batch_id, path.name))
    return files


def emit_automatic_invocation(scope: str, command: str) -> None:
    """Make a best-effort append without waiting on a lock or the network."""
    if host_opted_out(scope):
        return
    try:
        day = report_day()
        switches = read_switches()
        enabled = switches[scope] is True
        prune = False in switches.values()
        if not enabled and not prune:
            return
        with _usage_directory(create=enabled) as directory:
            with _open_file(directory, _USAGE_LOCK, os.O_RDWR, create=True) as lock:
                # Shared locks let concurrent POSIX appends proceed. Rotation
                # and opt-out cleanup take the exclusive lock for local I/O only.
                with lock_file_descriptor(lock, exclusive=prune, blocking=False):
                    if prune:
                        cutoff = (day - timedelta(days=RETENTION_DAYS)).isoformat()
                        _prune_usage(directory, switches, cutoff)
                    if not enabled:
                        return
                    record = f"{command}\n".encode("ascii")
                    name = f"{scope}.{day.isoformat()}.usage"
                    with _open_file(directory, name, os.O_WRONLY | os.O_APPEND, create=True) as log:
                        if os.fstat(log).st_size + len(record) <= _MAX_USAGE_FILE_BYTES:
                            os.write(log, record)
            _launch_if_due(directory)
    except (MaxCError, OSError, ValueError, RuntimeError):
        return


def _freeze_usage(directory: PrivateDirectoryHandle) -> list[tuple[str, str, str, str]]:
    today = report_day()
    latest = today.isoformat()
    cutoff = (today - timedelta(days=RETENTION_DAYS)).isoformat()
    switches = read_switches()
    candidates = {}
    frozen = []
    with _open_file(directory, _USAGE_LOCK, os.O_RDWR, create=True) as lock, lock_file_descriptor(lock):
        for day, scope, batch_id, name in _prune_usage(directory, switches, cutoff):
            if day <= latest and (batch_id or (day, scope) not in candidates):
                candidates[day, scope] = (batch_id, name)
        for (day, scope), (batch_id, name) in sorted(candidates.items())[:_MAX_UPLOAD_BATCHES]:
            if batch_id is None:
                batch_id = str(uuid4())
                frozen_name = f"{scope}.{day}.{batch_id}.usage"
                replace_private_file_at(directory, name, frozen_name)
                name = frozen_name
            frozen.append((day, scope, batch_id, name))
    return frozen


def _send_batch(scope: str, payload: bytes, timeout: float) -> str:
    """Deliver a batch over the user's existing noninteractive ODPS connection."""
    try:
        decode_batch(payload)
    except (ValueError, RecursionError):
        return "invalid"
    try:
        if read_switches()[scope] is not True:
            return "paused"
        from odps.utils import clear_survey_calls

        from .auth_providers import resolve_auth_connection
        from .backend.odps import _request_options

        # Read only trusted user settings and the launch environment.
        config = load_config(Path.cwd(), user_only=True)
        config.command_project = config.default_project or None
        connection = resolve_auth_connection(config, background_timeout=timeout / 2)
        with _request_options(timeout=timeout):
            client = connection.create_client()
            # PyODPS can flush its own survey calls before a REST request.
            clear_survey_calls()
            if read_switches()[scope] is not True:
                return "paused"
            rest = client.rest
            project_path = quote(connection.project, safe="")
            url = f"{rest.endpoint.rstrip('/')}/projects/{project_path}/logs"
            with rest.put(
                url, data=payload, headers={"Content-Type": "application/json"},
                timeout=(timeout / 4, timeout / 4),
            ) as response:
                status = response.status_code
    except Exception as exc:
        # Credentials or ODPS exceptions may contain sensitive strings.
        status = getattr(exc, "status_code", None)
    if type(status) is int:
        if status == 200:
            return "accepted"
        if status in {413, 415, 422}:
            return "invalid"
    return "retry"


def upload_usage(commands: frozenset[str]) -> None:
    """Flush a few immutable files in a process that exits after the attempt."""
    deadline = monotonic() + UPLOAD_WINDOW_SECONDS
    allowed = {command.encode("ascii"): command for command in commands}
    with (
        _usage_directory(create=False) as directory,
        _open_file(directory, _UPLOAD_LOCK, os.O_RDWR, create=True) as marker,
        lock_file_descriptor(marker),
    ):
        for day, scope, batch_id, name in _freeze_usage(directory):
            timeout = min(4, deadline - monotonic())
            if timeout < 0.5:
                break
            try:
                with _open_file(directory, name, os.O_RDONLY) as log, open(log, "rb", closefd=False) as stream:
                    contents = stream.read(_MAX_USAGE_FILE_BYTES)
            except FileNotFoundError:
                continue
            # Ignore a partial trailing write if the append was interrupted.
            records = contents.rpartition(b"\n")[0].splitlines()
            counts = Counter(allowed[record] for record in records if record in allowed)
            if counts:
                payload = encode_batch(batch_id, day, sorted(counts.items()))
                result = _send_batch(scope, payload, timeout)
                if result == "retry":
                    break
                if result == "paused":
                    continue
            with suppress(FileNotFoundError):
                unlink_private_file_at(directory, name)
