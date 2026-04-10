
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator
from uuid import uuid4

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False

from .exceptions import NotFoundError
from .utils import now_utc_iso


class JobStore:
    def __init__(self, state_dir: 'Path') -> 'None':
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.state_dir / "jobs.json"

    @contextmanager
    def _locked(self, exclusive: bool = False) -> 'Generator[None, None, None]':
        """Acquire an advisory file lock (shared for reads, exclusive for writes).

        Falls back to a no-op on platforms without fcntl (e.g. Windows).
        """
        if not _HAS_FCNTL:
            yield
            return
        lock_path = self.path.with_suffix(".lock")
        lock_path.touch(exist_ok=True)
        fd = lock_path.open("r")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()

    def create_job(
        self,
        *,
        sql: 'str',
        project: 'str',
        result: 'dict[str, Any]',
        idempotency_key: 'str | None' = None,
    ) -> 'dict[str, Any]':
        with self._locked(exclusive=True):
            payload = self._load()
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
            self._save(payload)
            return job

    def get_job(self, job_id: 'str') -> 'dict[str, Any]':
        with self._locked():
            payload = self._load()
            try:
                return payload["jobs"][job_id]
            except KeyError as exc:
                raise NotFoundError(
                    f"Job not found: {job_id}",
                    suggestion="Run `maxc job list` to inspect available jobs.",
                ) from exc

    def list_jobs(self) -> 'list[dict[str, Any]]':
        with self._locked():
            payload = self._load()
            jobs = list(payload["jobs"].values())
            jobs.sort(key=lambda item: item["submitted_at"], reverse=True)
            return jobs

    def update_job(self, job_id: 'str', **changes: 'Any') -> 'dict[str, Any]':
        with self._locked(exclusive=True):
            payload = self._load()
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
            self._save(payload)
            return job

    def _load(self) -> 'dict[str, Any]':
        if not self.path.exists():
            return {"jobs": {}, "idempotency": {}}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            return {"jobs": {}, "idempotency": {}}

    def _save(self, payload: 'dict[str, Any]') -> 'None':
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
