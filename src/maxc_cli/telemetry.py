"""Local invocation notifications and the MaxC usage batch format."""

from __future__ import annotations

import json
import os
import socket
from datetime import date, datetime, timedelta, timezone

RECEIVER_ENV = "MAXC_TELEMETRY_RECEIVER"
HOST_DISABLE_ENV = "ALIBABA_CLOUD_CLI_TELEMETRY_DISABLE"
SCOPES = ("standalone", "aliyun")
EVENT_TYPE = "maxc.command_usage"
MAX_PACKET_BYTES = 256
MAX_BATCH_BYTES = 32 * 1024
MAX_BATCH_COMMANDS = 128
RETENTION_DAYS = 7

_BEIJING_TIME = timezone(timedelta(hours=8))
_HEX_DIGITS = frozenset("0123456789abcdef")
_COMMAND = r"[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)*"


def report_day() -> date:
    return datetime.now(_BEIJING_TIME).date()


def receiver_settings(value: str) -> tuple[int, str]:
    port_text, separator, token = value.partition(":")
    if (
        not separator
        or not port_text.isascii()
        or not port_text.isdecimal()
        or len(port_text) > 5
        or len(token) != 32
        or not set(token).issubset(_HEX_DIGITS)
    ):
        raise ValueError("Expected a UDP port and a 32-digit lowercase hex token.")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError("Telemetry receiver port must be in the range 1–65535.")
    return port, token


def emit_invocation(receiver: str, scope: str, command: str) -> None:
    """Attempt one datagram to the managed worker without delaying the command."""
    if scope == "aliyun" and os.environ.get(HOST_DISABLE_ENV, "").strip().lower() in {
        "1", "true", "yes",
    }:
        return
    try:
        port, token = receiver_settings(receiver)
        packet = f"{token} {scope} {report_day().isoformat()} {command}".encode("ascii")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.setblocking(False)
            sender.sendto(packet, ("127.0.0.1", port))
    except (OSError, ValueError):
        return


def decode_invocation(
    packet: bytes, token: str, commands: frozenset[str], today: date
) -> tuple[str, str, str] | None:
    from hmac import compare_digest

    if len(packet) > MAX_PACKET_BYTES:
        return None
    try:
        supplied_token, scope, day, command = packet.decode("ascii").split(" ")
        parsed_day = date.fromisoformat(day)
    except ValueError:
        return None
    if (
        not compare_digest(supplied_token, token)
        or scope not in SCOPES
        or command not in commands
        or parsed_day.isoformat() != day
        or not today - timedelta(days=RETENTION_DAYS) <= parsed_day <= today
    ):
        return None
    return scope, day, command


def encode_batch(batch_id: str, day: str, counts: list[tuple[str, int]]) -> bytes:
    return json.dumps(
        {
            "event_type": EVENT_TYPE,
            "schema_version": 1,
            "batch_id": batch_id,
            "date": day,
            "counts": [{"command": command, "count": count} for command, count in counts],
        },
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def decode_batch(raw: bytes | str) -> tuple[str, str, list[tuple[str, int]]]:
    """Validate a MaxC batch from the local queue or an ODPS SDK LOG line."""
    from re import fullmatch
    from uuid import UUID

    if len(raw) > MAX_BATCH_BYTES:
        raise ValueError("Telemetry batch is too large.")
    batch = json.loads(raw)
    fields = {"event_type", "schema_version", "batch_id", "date", "counts"}
    if (
        not isinstance(batch, dict)
        or batch.keys() != fields
        or batch["event_type"] != EVENT_TYPE
        or type(batch["schema_version"]) is not int
        or batch["schema_version"] != 1
    ):
        raise ValueError("Invalid telemetry batch.")

    batch_id, day, rows = batch["batch_id"], batch["date"], batch["counts"]
    if not isinstance(batch_id, str) or not isinstance(day, str):
        raise ValueError("Invalid telemetry batch identity.")
    identifier = UUID(batch_id)
    if identifier.version != 4 or str(identifier) != batch_id:
        raise ValueError("Invalid telemetry batch identifier.")
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError("Invalid telemetry batch date.")
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_BATCH_COMMANDS:
        raise ValueError("Invalid telemetry counts.")

    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict) or row.keys() != {"command", "count"}:
            raise ValueError("Invalid telemetry count entry.")
        command, count = row["command"], row["count"]
        if (
            not isinstance(command, str)
            or len(command) > 80
            or not fullmatch(_COMMAND, command)
            or command in counts
            or type(count) is not int
            or not 1 <= count <= 2**63 - 1
        ):
            raise ValueError("Invalid telemetry command or count.")
        counts[command] = count
    return batch_id, day, sorted(counts.items())
