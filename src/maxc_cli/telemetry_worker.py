"""Managed local aggregation and authenticated ODPS SDK LOG delivery."""

from __future__ import annotations

import json
import os
import random
import re
import signal
import socket
import sqlite3
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from threading import Event
from time import monotonic
from urllib.parse import quote
from uuid import uuid4

from .auth_providers import resolve_auth_connection
from .config import default_global_config_path, load_config, load_config_mapping
from .exceptions import MaxCError, ValidationError
from .state_permissions import (
    close_private_directory,
    descriptor_matches_path,
    lock_file_descriptor,
    open_private_directory,
    open_private_file_at,
)
from .telemetry import (
    MAX_BATCH_COMMANDS,
    MAX_PACKET_BYTES,
    RETENTION_DAYS,
    SCOPES,
    decode_batch,
    decode_invocation,
    encode_batch,
    receiver_settings,
    report_day,
)

_STORAGE_BYTES = 2 * 1024 * 1024
_PERSIST_SECONDS = 5
_PERSIST_EVENTS = 256
_POLL_SECONDS = 0.2
_POLICY_SECONDS = 1
_CYCLE_SECONDS = 10
_MAX_SENDS = 3
_REQUEST_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")


@dataclass(frozen=True)
class Batch:
    scope: str
    day: str
    batch_id: str
    payload: bytes


@dataclass(frozen=True)
class Delivery:
    result: str
    http_status: int | None = None
    request_id: str | None = None


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
        settings = json.loads(path.read_text(encoding="utf-8"))
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
def _open_state(state: Path) -> Iterator[sqlite3.Connection]:
    with ExitStack() as stack:
        directory = open_private_directory(state, secure_existing=True)
        stack.callback(close_private_directory, directory)
        lock = open_private_file_at(directory, "worker.lock", os.O_RDWR, create=True)
        stack.callback(os.close, lock)
        stack.enter_context(lock_file_descriptor(lock, blocking=False))

        path = directory.path / "usage.sqlite3"
        for suffix in ("-journal", "-shm", "-wal"):
            name = path.name + suffix
            try:
                companion = open_private_file_at(directory, name, os.O_RDWR)
            except FileNotFoundError:
                continue
            stack.callback(os.close, companion)
        descriptor = open_private_file_at(directory, path.name, os.O_RDWR, create=True)
        stack.callback(os.close, descriptor)
        def verify_queue_path() -> None:
            if not descriptor_matches_path(directory.path, directory, directory=True):
                raise OSError("Telemetry state directory was replaced.")
            if not descriptor_matches_path(path, descriptor):
                raise OSError("Telemetry database was replaced.")

        verify_queue_path()
        db = sqlite3.connect(str(path), timeout=0.2)
        stack.callback(db.close)
        verify_queue_path()
        db.execute("PRAGMA journal_mode = DELETE")
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        db.execute(f"PRAGMA max_page_count = {_STORAGE_BYTES // page_size}")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS counts (
                scope TEXT NOT NULL, day TEXT NOT NULL, command TEXT NOT NULL,
                n INTEGER NOT NULL, PRIMARY KEY (scope, day, command)
            );
            CREATE TABLE IF NOT EXISTS pending (
                scope TEXT PRIMARY KEY NOT NULL, day TEXT NOT NULL,
                batch_id TEXT NOT NULL, payload BLOB NOT NULL
            );
        """)
        yield db


class UsageQueue:
    """All operations run on the receiver thread; only it mutates the queue."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def discard(self, switches: dict[str, bool | None], cutoff: str) -> None:
        scopes = [(scope,) for scope, enabled in switches.items() if enabled is False]
        with self.db:
            self.db.execute("DELETE FROM counts WHERE day < ?", (cutoff,))
            self.db.execute("DELETE FROM pending WHERE day < ?", (cutoff,))
            self.db.executemany("DELETE FROM counts WHERE scope = ?", scopes)
            self.db.executemany("DELETE FROM pending WHERE scope = ?", scopes)

    def persist(self, counts: Counter[tuple[str, str, str]]) -> None:
        with self.db:
            self.db.executemany(
                "INSERT INTO counts VALUES (?, ?, ?, ?) "
                "ON CONFLICT(scope, day, command) DO UPDATE SET n = n + excluded.n",
                ((*key, count) for key, count in counts.items()),
            )

    def discard_oldest_day(self) -> bool:
        oldest = self.db.execute(
            "SELECT MIN(day) FROM (SELECT day FROM counts UNION ALL SELECT day FROM pending)"
        ).fetchone()[0]
        if oldest is None:
            return False
        with self.db:
            self.db.execute("DELETE FROM counts WHERE day = ?", (oldest,))
            self.db.execute("DELETE FROM pending WHERE day = ?", (oldest,))
        return True

    def next_batch(self, scopes: set[str]) -> Batch | None:
        for row in self.db.execute("SELECT scope, day, batch_id, payload FROM pending ORDER BY day, scope"):
            if row[0] in scopes:
                return Batch(*row)
        queued = self.db.execute("SELECT DISTINCT scope, day FROM counts ORDER BY day, scope")
        candidate = next((row for row in queued if row[0] in scopes), None)
        queued.close()
        if candidate is None:
            return None
        scope, day = candidate
        rows = self.db.execute(
            "SELECT command, n FROM counts WHERE scope = ? AND day = ? "
            "ORDER BY command LIMIT ?", (scope, day, MAX_BATCH_COMMANDS),
        ).fetchall()
        batch_id = str(uuid4())
        payload = encode_batch(batch_id, day, rows)
        with self.db:
            self.db.execute("INSERT INTO pending VALUES (?, ?, ?, ?)", (scope, day, batch_id, payload))
            self.db.executemany(
                "DELETE FROM counts WHERE scope = ? AND day = ? AND command = ?",
                ((scope, day, command) for command, _ in rows),
            )
        return Batch(scope, day, batch_id, payload)

    def finish(self, batch: Batch) -> None:
        with self.db:
            self.db.execute(
                "DELETE FROM pending WHERE scope = ? AND batch_id = ?",
                (batch.scope, batch.batch_id),
            )


def _safe_delivery(status: object, request_id: object) -> Delivery:
    code = status if type(status) is int and 100 <= status <= 599 else None
    request = request_id if isinstance(request_id, str) and _REQUEST_ID.fullmatch(request_id) else None
    if code == 200:
        return Delivery("accepted", code, request)
    if code in {413, 415, 422}:
        return Delivery("invalid", code, request)
    return Delivery("retry", code, request)


def send_batch(
    batch: Batch,
    commands: frozenset[str],
    config_path: Path | None,
    timeout: float,
) -> Delivery:
    """Use an existing noninteractive ODPS connection and keep its details local."""
    try:
        batch_id, day, counts = decode_batch(batch.payload)
        if batch_id != batch.batch_id or day != batch.day or any(command not in commands for command, _ in counts):
            return Delivery("invalid")
    except (ValueError, RecursionError):
        return Delivery("invalid")
    try:
        if read_switches()[batch.scope] is not True:
            return Delivery("paused")
        config = load_config(Path.cwd(), config_path, user_only=True)
        # The worker selects its own trusted default project. Foreground
        # command-line and workspace overrides never reach this connection.
        config.command_project = config.default_project or None
        connection = resolve_auth_connection(config, background_timeout=timeout / 2)
        from odps.utils import clear_survey_calls

        from .backend.odps import _request_options

        with _request_options(timeout=timeout):
            client = connection.create_client()
            # PyODPS can flush its own method counters before a REST request.
            # This dedicated thread sends only the MaxC batch.
            clear_survey_calls()
            if read_switches()[batch.scope] is not True:
                return Delivery("paused")
            rest = client.rest
            project_path = quote(connection.project, safe="")
            url = f"{rest.endpoint.rstrip('/')}/projects/{project_path}/logs"
            with rest.put(
                url, data=batch.payload, headers={"Content-Type": "application/json"},
                timeout=(timeout / 4, timeout / 4),
            ) as response:
                return _safe_delivery(response.status_code, response.headers.get("x-odps-request-id"))
    except Exception as exc:
        # Credential helpers and ODPS exceptions may include sensitive strings.
        return _safe_delivery(getattr(exc, "status_code", None), getattr(exc, "request_id", None))


@contextmanager
def _shutdown_signals() -> Iterator[Event]:
    stop = Event()
    watched = (signal.SIGINT, signal.SIGTERM)
    previous = [signal.getsignal(sig) for sig in watched]
    try:
        for sig in watched:
            signal.signal(sig, lambda _signum, _frame: stop.set())
        yield stop
    finally:
        for sig, handler in zip(watched, previous):
            signal.signal(sig, handler)


class TelemetryWorker:
    def __init__(
        self, queue: UsageQueue, commands: frozenset[str], config: Path | None, interval: int,
    ):
        self.queue = queue
        self.commands = commands
        self.config = config
        self.interval = interval
        self.counts: Counter[tuple[str, str, str]] = Counter()
        self.unflushed = 0
        self.stats: Counter[str] = Counter()
        self.switches: dict[str, bool | None] = {}
        self.today = report_day()
        self.last_delivery: Delivery | None = None

    def _refresh_policy(self) -> None:
        today = report_day()
        switches = read_switches()
        cutoff = (today - timedelta(days=RETENTION_DAYS)).isoformat()
        if switches != self.switches or today != self.today:
            self.queue.discard(switches, cutoff)
            self.counts = Counter({
                key: n for key, n in self.counts.items()
                if switches[key[0]] is not False and cutoff <= key[1] <= today.isoformat()
            })
            self.unflushed = sum(self.counts.values())
            self.switches, self.today = switches, today

    def _flush(self) -> None:
        if not self.counts:
            return
        try:
            try:
                self.queue.persist(self.counts)
            except sqlite3.OperationalError as exc:
                if "full" not in str(exc).lower() or not self.queue.discard_oldest_day():
                    raise
                self.stats["evicted_days"] += 1
                self.queue.persist(self.counts)
        except sqlite3.Error:
            self.stats["dropped_for_storage"] += self.unflushed
        self.counts.clear()
        self.unflushed = 0

    def _next_interval(self) -> float:
        return self.interval + random.uniform(0, min(300, self.interval / 6))

    def _receive(self, listener: socket.socket, token: str) -> None:
        try:
            packet, _ = listener.recvfrom(MAX_PACKET_BYTES + 1)
        except socket.timeout:
            return
        invocation = decode_invocation(packet, token, self.commands, report_day())
        if invocation is not None and self.switches[invocation[0]] is True:
            self.counts[invocation] += 1
            self.unflushed += 1
            self.stats["collected"] += 1

    def run(self, listener: socket.socket, token: str, stop: Event) -> dict[str, object]:
        self._refresh_policy()
        now = monotonic()
        poll_at = now + _POLICY_SECONDS
        flush_at = now + _PERSIST_SECONDS
        cycle_at = now + self._next_interval()
        deadline = now
        remaining = 0
        active: tuple[Batch, Future[Delivery]] | None = None

        with ThreadPoolExecutor(max_workers=1) as sender:
            while not stop.is_set():
                self._receive(listener, token)
                now = monotonic()
                if now >= poll_at:
                    self._refresh_policy()
                    poll_at = now + _POLICY_SECONDS
                if now >= cycle_at:
                    self._refresh_policy()
                    cycle_at = now + self._next_interval()
                    deadline = now + _CYCLE_SECONDS
                    remaining = _MAX_SENDS
                    self._flush()
                    flush_at = now + _PERSIST_SECONDS
                if now >= flush_at or self.unflushed >= _PERSIST_EVENTS:
                    self._flush()
                    flush_at = now + _PERSIST_SECONDS
                if active is not None and active[1].done():
                    self._finish(*active)
                    if self.last_delivery and self.last_delivery.result in {"retry", "paused"}:
                        remaining = 0
                    active = None
                if active is None and remaining and now + 0.5 < deadline and not stop.is_set():
                    enabled = {scope for scope in SCOPES if self.switches[scope] is True}
                    batch = self.queue.next_batch(enabled)
                    if batch is None:
                        remaining = 0
                        continue
                    future = sender.submit(send_batch, batch, self.commands, self.config, min(4, deadline - now))
                    active = (batch, future)
                    remaining -= 1
            self._refresh_policy()
            self._flush()
            if active is not None:
                self._finish(*active)
        return {
            "collected": self.stats["collected"],
            "accepted_batches": self.stats["accepted"],
            "retry_batches": self.stats["retry"],
            "invalid_batches": self.stats["invalid"],
            "dropped_for_storage": self.stats["dropped_for_storage"],
            "evicted_days": self.stats["evicted_days"],
            "last_http_status": self.last_delivery.http_status if self.last_delivery else None,
            "last_request_id": self.last_delivery.request_id if self.last_delivery else None,
        }

    def _finish(self, batch: Batch, future: Future[Delivery]) -> None:
        self.last_delivery = future.result()
        result = self.last_delivery.result
        self.stats[result] += 1
        if result in {"accepted", "invalid"}:
            self.queue.finish(batch)


def run_worker(
    receiver: str,
    *,
    state_parent: Path,
    config_path: Path | None,
    commands: frozenset[str],
    interval: int,
    ready: Callable[[], None],
) -> dict[str, object]:
    try:
        port, token = receiver_settings(receiver)
    except ValueError as exc:
        raise ValidationError(
            "Invalid or missing MAXC_TELEMETRY_RECEIVER.",
            suggestion="Have the launcher provide a local port and a 128-bit lowercase hex token.",
        ) from exc
    try:
        with _shutdown_signals() as stop, _open_state(state_parent / "telemetry") as db:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener:
                listener.bind(("127.0.0.1", port))
                listener.settimeout(_POLL_SECONDS)
                ready()
                worker = TelemetryWorker(UsageQueue(db), commands, config_path, interval)
                return worker.run(listener, token, stop)
    except (OSError, sqlite3.Error):
        raise ValidationError(
            "Unable to use the MaxC telemetry port or private queue.",
            suggestion="Check the configured local port and state directory, and run one worker per user.",
        ) from None
