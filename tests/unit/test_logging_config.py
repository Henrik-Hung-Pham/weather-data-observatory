"""Unit tests for structured logging configuration."""

import json
import logging

import pytest

from data_pipeline.logging_config import JsonFormatter, configure_logging


@pytest.mark.unit
def test_json_formatter_emits_valid_json() -> None:
    formatter = JsonFormatter()
    record = logging.makeLogRecord(
        {
            "name": "data_pipeline.test",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "hello %s",
            "args": ("world",),
        }
    )
    payload = json.loads(formatter.format(record))

    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "data_pipeline.test"
    assert "timestamp" in payload


@pytest.mark.unit
def test_json_formatter_includes_extra_context() -> None:
    formatter = JsonFormatter()
    record = logging.makeLogRecord(
        {"name": "x", "levelname": "INFO", "msg": "run", "run_id": "abc-123"}
    )
    payload = json.loads(formatter.format(record))
    assert payload["run_id"] == "abc-123"


@pytest.mark.unit
def test_json_formatter_includes_exception() -> None:
    formatter = JsonFormatter()
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.makeLogRecord(
            {"name": "x", "levelname": "ERROR", "msg": "failed", "exc_info": sys.exc_info()}
        )
    payload = json.loads(formatter.format(record))
    assert "boom" in payload["exception"]


@pytest.mark.unit
def test_configure_logging_json_is_idempotent() -> None:
    configure_logging("DEBUG", "json")
    configure_logging("DEBUG", "json")  # second call must not duplicate handlers

    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)
    assert root.level == logging.DEBUG


@pytest.mark.unit
def test_configure_logging_text_format() -> None:
    configure_logging("WARNING", "text")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert not isinstance(root.handlers[0].formatter, JsonFormatter)
    assert root.level == logging.WARNING
    # restore a sane default for any later tests
    configure_logging("INFO", "text")
