
import json
from pathlib import Path
from typing import Any

from .utils import now_utc_iso


class AuditLogger:
    def __init__(self, path: 'Path') -> 'None':
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, payload: 'dict[str, Any]') -> 'None':
        record = dict(payload)
        record.setdefault("ts", now_utc_iso())
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
