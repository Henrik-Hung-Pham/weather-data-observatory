"""Unit tests for the CLI, focused on the replay command.

``cli.py`` had no test coverage at all. These cover the argument contract
(date parsing, range validation) and the exit-code mapping, which is what a
scheduler or a human at 2am actually depends on.
"""

import argparse
from datetime import datetime, timezone

import pytest

import data_pipeline.cli as cli


class _StubResult:
    def __init__(self, status: str, **kwargs) -> None:
        self.status = status
        self.records_ingested = kwargs.get("records_ingested", 0)
        self.records_loaded = kwargs.get("records_loaded", 0)
        self.quality_gate_reason = kwargs.get("quality_gate_reason", "")
        self.error_message = kwargs.get("error_message", "")


# ---------------------------------------------------------------------------
# Date argument parsing
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_utc_date_parses_an_iso_date():
    assert cli._utc_date("2024-01-30") == datetime(2024, 1, 30, tzinfo=timezone.utc)


@pytest.mark.unit
@pytest.mark.parametrize("value", ["30-01-2024", "2024-13-01", "yesterday", ""])
def test_utc_date_rejects_anything_else(value):
    """A bad date is a usage error, not a traceback."""
    with pytest.raises(argparse.ArgumentTypeError):
        cli._utc_date(value)


# ---------------------------------------------------------------------------
# Range validation and exit codes
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_backwards_range_is_rejected_before_touching_the_lake(monkeypatch):
    """--to earlier than --from exits 1 without constructing a pipeline."""

    def _explode(*args, **kwargs):
        raise AssertionError("must not build a pipeline for an invalid range")

    monkeypatch.setattr("data_pipeline.pipeline.DataPipeline", _explode)

    args = argparse.Namespace(
        from_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
        to_date=datetime(2024, 1, 30, tzinfo=timezone.utc),
    )
    assert cli.replay_pipeline(args) == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "status,expected_code",
    [("success", 0), ("blocked", 2), ("failed", 1)],
)
def test_replay_exit_codes(monkeypatch, status, expected_code):
    """The replay command maps status to the same exit codes as `run`."""
    captured = {}

    class _StubPipeline:
        def replay(self, start_date, end_date):
            captured["range"] = (start_date, end_date)
            return _StubResult(status, error_message="boom", quality_gate_reason="gate")

    monkeypatch.setattr("data_pipeline.pipeline.DataPipeline", lambda: _StubPipeline())

    args = argparse.Namespace(
        from_date=datetime(2024, 1, 30, tzinfo=timezone.utc),
        to_date=None,
    )
    assert cli.replay_pipeline(args) == expected_code
    assert captured["range"] == (datetime(2024, 1, 30, tzinfo=timezone.utc), None)


@pytest.mark.unit
def test_replay_reports_an_unexpected_failure_as_exit_1(monkeypatch):
    """An exception from the pipeline is logged and mapped, not propagated."""

    class _StubPipeline:
        def replay(self, start_date, end_date):
            raise RuntimeError("lake unreachable")

    monkeypatch.setattr("data_pipeline.pipeline.DataPipeline", lambda: _StubPipeline())

    args = argparse.Namespace(
        from_date=datetime(2024, 1, 30, tzinfo=timezone.utc),
        to_date=None,
    )
    assert cli.replay_pipeline(args) == 1


# ---------------------------------------------------------------------------
# End-to-end argument wiring
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_main_routes_replay_with_parsed_dates(monkeypatch):
    """`observatory replay --from X --to Y` reaches DataPipeline.replay."""
    captured = {}

    class _StubPipeline:
        def replay(self, start_date, end_date):
            captured["range"] = (start_date, end_date)
            return _StubResult("success")

    monkeypatch.setattr("data_pipeline.pipeline.DataPipeline", lambda: _StubPipeline())
    monkeypatch.setattr(
        "sys.argv",
        ["observatory", "replay", "--from", "2024-01-30", "--to", "2024-02-02"],
    )

    assert cli.main() == 0
    assert captured["range"] == (
        datetime(2024, 1, 30, tzinfo=timezone.utc),
        datetime(2024, 2, 2, tzinfo=timezone.utc),
    )


@pytest.mark.unit
def test_main_requires_a_from_date(monkeypatch):
    """--from is mandatory; argparse exits 2 without it."""
    monkeypatch.setattr("sys.argv", ["observatory", "replay"])

    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2
