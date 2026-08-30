
import json
import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from .exceptions import NotFoundError, ValidationError
from .state_permissions import (
    PrivateDirectoryHandle,
    close_private_directory,
    fsync_private_directory,
    lock_file_descriptor,
    open_private_directory,
    open_private_file_at,
    replace_private_file_at,
    unlink_private_file_at,
)
from .utils import now_utc_iso

_EMPTY_STORE = {
    "jobs": {},
    "idempotency": {},
    "remote_job_contexts": {},
}


class JobStore:
    def __init__(self, state_dir: 'Path') -> 'None':
        self.state_dir = state_dir
        self.path = self.state_dir / "jobs.json"
        self.lock_path = self.state_dir / "jobs.lock"
        directory = open_private_directory(self.state_dir)
        try:
            # jobs.json contains SQL and remote job context. Repair files left
            # world-readable by earlier releases, but do not create an empty
            # JSON file: an interrupted first write must not look valid.
            try:
                descriptor = open_private_file_at(
                    directory,
                    self.path.name,
                    os.O_RDONLY,
                    display_path=self.path,
                )
            except FileNotFoundError:
                pass
            else:
                os.close(descriptor)
        finally:
            close_private_directory(directory)

    @contextmanager
    def _locked(
        self,
        exclusive: bool = False,
    ) -> 'Generator[PrivateDirectoryHandle, None, None]':
        """Acquire an advisory file lock (shared for reads, exclusive for writes).

        POSIX uses ``flock``. Windows uses the standard-library ``msvcrt``
        byte-range lock and conservatively serializes reads as well as writes.
        Silently proceeding without a cross-process lock would make a
        read-modify-write lose updates, so unsupported platforms fail closed.
        """
        directory = open_private_directory(
            self.state_dir,
            create=False,
        )
        try:
            descriptor = open_private_file_at(
                directory,
                self.lock_path.name,
                os.O_RDWR | os.O_APPEND,
                create=True,
                display_path=self.lock_path,
            )
        except Exception:
            close_private_directory(directory)
            raise
        try:
            with lock_file_descriptor(descriptor, exclusive=exclusive):
                yield directory
        finally:
            os.close(descriptor)
            close_private_directory(directory)

    def create_job(
        self,
        *,
        sql: 'str',
        project: 'str',
        result: 'dict[str, Any]',
        idempotency_key: 'str | None' = None,
    ) -> 'dict[str, Any]':
        with self._locked(exclusive=True) as directory_descriptor:
            payload = self._load(directory_descriptor)
            if idempotency_key:
                existing_id = payload["idempotency"].get(idempotency_key)
                if existing_id:
                    return payload["jobs"][existing_id]

            job_id = f"job_{uuid4().hex[:10]}"
            now = now_utc_iso()
            job = {
                "job_id": job_id,
                "status": "pending",
                "sql": sql,
                "project": project,
                "progress": 0,
                "submitted_at": now,
                "updated_at": now,
                "result": result,
                "idempotency_key": idempotency_key,
            }
            payload["jobs"][job_id] = job
            if idempotency_key:
                payload["idempotency"][idempotency_key] = job_id
            self._save(payload, directory_descriptor)
            return job

    def get_job(self, job_id: 'str') -> 'dict[str, Any]':
        with self._locked() as directory_descriptor:
            payload = self._load(directory_descriptor)
            try:
                return payload["jobs"][job_id]
            except KeyError as exc:
                raise NotFoundError(
                    f"Job not found: {job_id}",
                    suggestion="Run `maxc job list` to inspect available jobs.",
                ) from exc

    def list_jobs(self) -> 'list[dict[str, Any]]':
        with self._locked() as directory_descriptor:
            payload = self._load(directory_descriptor)
            jobs = list(payload["jobs"].values())
            jobs.sort(key=lambda item: item["submitted_at"], reverse=True)
            return jobs

    def update_job(self, job_id: 'str', **changes: 'Any') -> 'dict[str, Any]':
        with self._locked(exclusive=True) as directory_descriptor:
            payload = self._load(directory_descriptor)
            try:
                job = payload["jobs"][job_id]
            except KeyError as exc:
                raise NotFoundError(
                    f"Job not found: {job_id}",
                    suggestion="Run `maxc job list` to inspect available jobs.",
                ) from exc
            job.update(changes)
            job["updated_at"] = now_utc_iso()
            payload["jobs"][job_id] = job
            self._save(payload, directory_descriptor)
            return job

    def save_remote_job_context(self, job_id: 'str', context: 'dict[str, Any]') -> 'None':
        with self._locked(exclusive=True) as directory_descriptor:
            payload = self._load(directory_descriptor)
            payload["remote_job_contexts"][job_id] = {
                **context,
                "updated_at": now_utc_iso(),
            }
            self._save(payload, directory_descriptor)

    def get_remote_job_context(self, job_id: 'str') -> 'dict[str, Any] | None':
        with self._locked() as directory_descriptor:
            payload = self._load(directory_descriptor)
            context = payload["remote_job_contexts"].get(job_id)
            if context is None:
                return None
            cleaned = dict(context)
            cleaned.pop("updated_at", None)
            return cleaned

    def _load(self, directory: 'PrivateDirectoryHandle') -> 'dict[str, Any]':
        try:
            descriptor = open_private_file_at(
                directory,
                self.path.name,
                os.O_RDONLY,
                display_path=self.path,
            )
        except FileNotFoundError:
            return {key: dict(value) for key, value in _EMPTY_STORE.items()}
        try:
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            raise self._corruption_error() from exc
        if not isinstance(payload, dict):
            raise self._corruption_error()
        payload.setdefault("jobs", {})
        payload.setdefault("idempotency", {})
        payload.setdefault("remote_job_contexts", {})
        if any(
            not isinstance(payload[key], dict)
            for key in ("jobs", "idempotency", "remote_job_contexts")
        ):
            raise self._corruption_error()
        return payload

    def _save(
        self,
        payload: 'dict[str, Any]',
        directory: 'PrivateDirectoryHandle',
    ) -> 'None':
        serialized = json.dumps(payload, ensure_ascii=False, indent=2)
        temporary_name = f".{self.path.name}.{uuid4().hex}.tmp"
        temporary_exists = False
        try:
            descriptor = open_private_file_at(
                directory,
                temporary_name,
                os.O_WRONLY | os.O_EXCL,
                create=True,
                display_path=self.state_dir / temporary_name,
            )
            temporary_exists = True
            with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
                temporary.write(serialized)
                temporary.flush()
                os.fsync(temporary.fileno())

            replace_private_file_at(
                directory,
                temporary_name,
                self.path.name,
            )
            temporary_exists = False
            replacement_descriptor = open_private_file_at(
                directory,
                self.path.name,
                os.O_RDONLY,
                display_path=self.path,
            )
            os.close(replacement_descriptor)
            fsync_private_directory(directory)
        finally:
            if temporary_exists:
                try:
                    unlink_private_file_at(directory, temporary_name)
                except FileNotFoundError:
                    pass

    def _corruption_error(self) -> 'ValidationError':
        return ValidationError(
            f"Local job store is corrupted and was not modified: {self.path}",
            suggestion=(
                "Move the damaged jobs.json aside for inspection, then retry. "
                "Do not replace it with an empty file unless losing local job history is acceptable."
            ),
        )
