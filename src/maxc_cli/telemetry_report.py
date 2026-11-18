"""Roll up a complete SDK LOG export for the reporting dates being rebuilt."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from collections.abc import Iterable

from .telemetry import EVENT_TYPE, MAX_BATCH_BYTES, decode_batch


def daily_counts(
    lines: Iterable[str], commands: frozenset[str],
) -> tuple[list[tuple[str, str, int]], dict[str, int]]:
    batches: dict[str, tuple[str, list[tuple[str, int]]]] = {}
    conflicted: set[str] = set()
    quality = {"accepted": 0, "duplicates": 0, "conflicts": 0, "rejected": 0}
    for line in lines:
        # SDK LOG exports may contain other clients' records and non-JSON lines.
        if EVENT_TYPE not in line:
            continue
        try:
            if len(line) > 2 * MAX_BATCH_BYTES:
                raise ValueError("SDK LOG record too large.")
            raw = json.loads(line)
            payload = raw.get("SDK LOG", line) if isinstance(raw, dict) else line
            if not isinstance(payload, str):
                raise ValueError("SDK LOG must contain a JSON string.")
            batch_id, day, rows = decode_batch(payload)
            if any(command not in commands for command, _ in rows):
                raise ValueError("Command not in the reporting dictionary.")
        except (ValueError, RecursionError):
            quality["rejected"] += 1
            continue
        if batch_id in conflicted:
            continue
        batch = (day, rows)
        previous = batches.get(batch_id)
        if previous is None:
            batches[batch_id] = batch
        elif previous == batch:
            quality["duplicates"] += 1
        else:
            del batches[batch_id]
            conflicted.add(batch_id)

    quality["accepted"] = len(batches)
    quality["conflicts"] = len(conflicted)
    totals: Counter[tuple[str, str]] = Counter()
    for day, rows in batches.values():
        for command, count in rows:
            totals[(day, command)] += count
    return [(day, command, count) for (day, command), count in sorted(totals.items())], quality


def main() -> None:
    from .cli import telemetry_command_ids

    parser = argparse.ArgumentParser(
        description="Aggregate exported MaxC SDK LOG lines from stdin into daily CSV rows.",
        allow_abbrev=False,
    )
    parser.parse_args()
    rows, quality = daily_counts(sys.stdin, telemetry_command_ids())
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(("stat_date", "command", "invocation_count"))
    writer.writerows(rows)
    print(json.dumps(quality), file=sys.stderr)


if __name__ == "__main__":
    main()
