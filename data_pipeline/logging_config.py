"""Centralised logging configuration.

Supports two output formats, selectable via settings:

- ``text`` — the original human-readable single-line format, for local dev.
- ``json`` — one JSON object per line, for log aggregation (CloudWatch,
  Loki, Datadog, etc.) where structured fields are searchable.

Both honour any ``extra={...}`` passed to a log call, so callers can attach
structured context (e.g. ``logger.info("done", extra={"run_id": rid})``)
without changing the format string.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

TEXT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# LogRecord attributes that are built-in; anything else in __dict__ is treated
# as caller-supplied structured context and included in JSON output.
_RESERVED_ATTRS = frozenset(logging.makeLogRecord({}).__dict__.keys()) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Attach any structured context passed via extra={...}.
        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRS and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    """Configure the root logger.

    Idempotent: replaces existing handlers so repeated calls (e.g. pipeline
    import + CLI entry) don't duplicate log lines.

    Args:
        level: Logging level name (e.g. "INFO", "DEBUG").
        fmt: "text" or "json".
    """
    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(TEXT_FORMAT))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
