"""Tests for the CLI entry point.

``data_pipeline/cli.py`` is what the container runs and what a human types,
and it was at 0% coverage: every exit code and every dispatch branch was
unverified. Exit codes are the contract here -- an orchestrator reads them to
decide whether to retry -- so they are asserted explicitly, especially the
distinction between 2 (blocked by a quality gate: the data was bad, retrying
changes nothing) and 1 (failed: the run broke, retrying might work).
"""

import subprocess
import sys
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest

from data_pipeline import cli
from data_pipeline.pipeline import PipelineRunResult


def _result(status: str, **kwargs: Any) -> PipelineRunResult:
    return PipelineRunResult(
        run_id=uuid4(),
        status=status,
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        **kwargs,
    )


class _FakePipeline:
    """Captures the cities it was handed so argument parsing can be asserted."""

    last_cities: list[str] | None = None

    def __init__(self, result: PipelineRunResult | None = None) -> None:
        self.result = result or _result("success")

    def run(self, cities: list[str] | None = None) -> PipelineRunResult:
        type(self).last_cities = cities
        return self.result


def _patch_pipeline(monkeypatch, result: PipelineRunResult | None = None, raises: bool = False):
    import data_pipeline.pipeline as pipeline_module

    _FakePipeline.last_cities = None

    def factory(*args: Any, **kwargs: Any):
        if raises:
            raise RuntimeError("boom")
        return _FakePipeline(result)

    monkeypatch.setattr(pipeline_module, "DataPipeline", factory)


def _argv(monkeypatch, *args: str) -> None:
    monkeypatch.setattr(sys, "argv", ["data-observatory", *args])


@pytest.mark.unit
def test_no_command_prints_help_and_succeeds(monkeypatch, capsys):
    _argv(monkeypatch)

    assert cli.main() == 0
    assert "Available commands" in capsys.readouterr().out


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status", "exit_code"),
    [
        ("success", 0),
        ("failed", 1),
        # 2, not 1: the gate rejected the data. A retry would reject it again.
        ("blocked", 2),
    ],
)
def test_run_maps_pipeline_status_to_exit_code(monkeypatch, status: str, exit_code: int):
    _patch_pipeline(monkeypatch, _result(status))
    _argv(monkeypatch, "run")

    assert cli.main() == exit_code


@pytest.mark.unit
def test_run_reports_failure_instead_of_raising(monkeypatch):
    """A crash must become exit 1, not a traceback out of the container."""
    _patch_pipeline(monkeypatch, raises=True)
    _argv(monkeypatch, "run")

    assert cli.main() == 1


@pytest.mark.unit
def test_run_splits_and_strips_the_cities_argument(monkeypatch):
    _patch_pipeline(monkeypatch)
    _argv(monkeypatch, "run", "--cities", "London, Paris ,Helsinki")

    assert cli.main() == 0
    assert _FakePipeline.last_cities == ["London", "Paris", "Helsinki"]


@pytest.mark.unit
def test_run_without_cities_passes_none_so_settings_decide(monkeypatch):
    _patch_pipeline(monkeypatch)
    _argv(monkeypatch, "run")

    assert cli.main() == 0
    assert _FakePipeline.last_cities is None


@pytest.mark.unit
def test_an_unknown_quality_mode_is_rejected_by_the_parser(monkeypatch):
    _argv(monkeypatch, "run", "--quality-mode", "nonsense")

    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


@pytest.mark.unit
def test_validate_requires_a_layer(monkeypatch):
    _argv(monkeypatch, "validate")

    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


@pytest.mark.unit
def test_validate_returns_zero_when_the_layer_is_empty(monkeypatch):
    """Nothing to validate is not a validation failure."""
    import data_pipeline.storage as storage

    class _EmptyStorage:
        def list_objects(self, layer: str) -> list[str]:
            return []

    monkeypatch.setattr(storage, "DataLakeStorage", lambda *a, **k: _EmptyStorage())
    _argv(monkeypatch, "validate", "--layer", "bronze")

    assert cli.main() == 0


@pytest.mark.unit
@pytest.mark.parametrize(("passed", "exit_code"), [(True, 0), (False, 1)])
def test_validate_reports_the_gate_verdict(monkeypatch, passed: bool, exit_code: int):
    import data_pipeline.quality.gates as gates
    import data_pipeline.storage as storage

    class _Storage:
        def list_objects(self, layer: str) -> list[str]:
            return ["bronze/2026-01-01/a.json", "bronze/2026-01-02/b.json"]

        def read_json(self, key: str) -> dict[str, Any]:
            assert key == "bronze/2026-01-02/b.json", "must validate the newest object"
            return {"city": "London"}

    class _Gate:
        def evaluate(self, data: list[dict[str, Any]], layer: str):
            assert isinstance(data, list), "a single object must be wrapped in a list"
            return type("R", (), {"passed": passed, "metrics": {}})()

    monkeypatch.setattr(storage, "DataLakeStorage", lambda *a, **k: _Storage())
    monkeypatch.setattr(gates, "build_gate_for_layer", lambda layer: _Gate())
    _argv(monkeypatch, "validate", "--layer", "bronze")

    assert cli.main() == exit_code


@pytest.mark.unit
def test_init_returns_zero_when_the_schema_is_created(monkeypatch):
    import data_pipeline.storage as storage

    class _Database:
        def initialize_schema(self) -> bool:
            return True

    monkeypatch.setattr(storage, "DatabaseManager", lambda *a, **k: _Database())
    _argv(monkeypatch, "init")

    assert cli.main() == 0


@pytest.mark.unit
def test_init_reports_failure_instead_of_raising(monkeypatch):
    import data_pipeline.storage as storage

    def explode(*args: Any, **kwargs: Any):
        raise RuntimeError("no database")

    monkeypatch.setattr(storage, "DatabaseManager", explode)
    _argv(monkeypatch, "init")

    assert cli.main() == 1


@pytest.mark.unit
def test_dashboard_launches_streamlit(monkeypatch):
    called: dict[str, Any] = {}

    def fake_run(command: list[str], **kwargs: Any):
        called["command"] = command
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    _argv(monkeypatch, "dashboard")

    assert cli.main() == 0
    assert called["command"][:2] == ["streamlit", "run"]


@pytest.mark.unit
def test_dashboard_stopped_with_ctrl_c_is_not_an_error(monkeypatch):
    def fake_run(command: list[str], **kwargs: Any):
        raise KeyboardInterrupt

    monkeypatch.setattr(subprocess, "run", fake_run)
    _argv(monkeypatch, "dashboard")

    assert cli.main() == 0
