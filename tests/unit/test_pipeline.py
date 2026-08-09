"""Unit tests for the DataPipeline orchestrator.

The orchestrator (``DataPipeline.run``) owns the medallion control flow:
phase ordering, quality-gate blocking, exception-to-status mapping, and
best-effort persistence/alerting. Previously this was exercised only by the
service-dependent integration tests. These tests inject lightweight fakes via
the constructor's dependency-injection seams so the whole flow runs fast and
offline, asserting the status/exit-code contract directly.
"""

from types import SimpleNamespace

import pytest

from data_pipeline.pipeline import DataPipeline


# ---------------------------------------------------------------------------
# Test doubles (constructor DI seams: api_client / storage / database /
# alerter)
# ---------------------------------------------------------------------------
class _FakeWeather:
    """Stand-in for WeatherData — only ``to_dict`` is used by ingestion."""

    def __init__(self, record: dict) -> None:
        self._record = record

    def to_dict(self) -> dict:
        return dict(self._record)


class FakeAPIClient:
    def __init__(self, records: list[dict]) -> None:
        self._records = records

    def fetch_multiple_cities(self, cities: list[str]) -> list[_FakeWeather]:
        return [_FakeWeather(r) for r in self._records]


class FakeStorage:
    """Records writes; never touches S3."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []

    def write_json(self, data, layer, filename, timestamp=None) -> str:
        self.writes.append((layer, filename))
        return f"{layer}/{filename}.json"


class FakeDatabase:
    def __init__(self) -> None:
        self.weather_rows: list[dict] = []
        self.runs: list[dict] = []
        self.metrics: list[dict] = []

    def insert_weather_data(self, records) -> int:
        self.weather_rows.extend(records)
        return len(records)

    def insert_pipeline_run(self, run_result) -> None:
        self.runs.append(run_result)

    def insert_quality_metrics(self, run_id, gate_result) -> None:
        self.metrics.append({"run_id": run_id, **gate_result})


class FakeAlerter:
    def __init__(self) -> None:
        self.alerts: list[dict] = []

    def alert_pipeline_result(self, **kwargs) -> bool:
        self.alerts.append(kwargs)
        return False


def _make_pipeline(records, storage=None, database=None, alerter=None) -> DataPipeline:
    return DataPipeline(
        api_client=FakeAPIClient(records),
        storage=storage or FakeStorage(),
        database=database or FakeDatabase(),
        alerter=alerter or FakeAlerter(),
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_run_success_flows_through_all_layers(sample_bronze_data):
    storage = FakeStorage()
    database = FakeDatabase()
    pipeline = _make_pipeline(sample_bronze_data, storage=storage, database=database)

    result = pipeline.run(cities=["London", "Paris"])

    assert result.status == "success"
    assert result.quality_gate_passed is True
    assert result.records_ingested == 2
    assert result.records_transformed == 2
    assert result.records_loaded == 2
    # Bronze + Silver + Gold gates all ran.
    assert [r.layer for r in result.quality_results] == ["bronze", "silver", "gold"]
    # Data was actually written to the (fake) serving layer.
    assert len(database.weather_rows) == 2
    # Bronze/Silver/Gold objects were written to the (fake) lake.
    assert {layer for layer, _ in storage.writes} >= {"bronze", "silver", "gold"}


# ---------------------------------------------------------------------------
# Blocked path — a critical issue at Bronze stops the pipeline
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_run_blocked_when_bronze_gate_critical(sample_bronze_data):
    # Drop a required column from every record -> schema-drift CRITICAL.
    bad = [{k: v for k, v in rec.items() if k != "timestamp"} for rec in sample_bronze_data]
    alerter = FakeAlerter()
    pipeline = _make_pipeline(bad, alerter=alerter)

    result = pipeline.run(cities=["London", "Paris"])

    assert result.status == "blocked"
    assert result.quality_gate_passed is False
    assert result.quality_gate_reason  # non-empty reason
    # Blocked at Bronze, so Silver/Gold never validated.
    assert [r.layer for r in result.quality_results] == ["bronze"]
    # A block triggers an alert.
    assert alerter.alerts and alerter.alerts[0]["status"] == "blocked"


# ---------------------------------------------------------------------------
# Lineage — every gate result must be attributable to its pipeline run
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_gate_results_carry_the_pipeline_run_id(sample_bronze_data):
    """Quality metrics must join back to the pipeline run that produced them.

    Regression test: ``QualityGate.__init__`` minted its own ``uuid4()`` and
    the pipeline never passed its own id in, so each run wrote three
    ``data_quality_metrics`` rows under three unrelated random UUIDs, none
    matching ``pipeline_runs.run_id``. "Which gate failed on run X?" was
    unanswerable.
    """
    database = FakeDatabase()
    pipeline = _make_pipeline(sample_bronze_data, database=database)

    result = pipeline.run(cities=["London", "Paris"])

    assert result.status == "success"
    # Every gate result is stamped with the owning run.
    assert {r.run_id for r in result.quality_results} == {result.run_id}
    # ...and so is every row handed to the database.
    assert [m["run_id"] for m in database.metrics] == [str(result.run_id)] * 3
    # The run row uses the same id, so the two tables join.
    assert database.runs[0]["run_id"] == str(result.run_id)


@pytest.mark.unit
def test_standalone_gate_still_gets_its_own_run_id():
    """A gate built outside a pipeline run (the CLI) still has an id."""
    from data_pipeline.quality.gates import build_gate_for_layer

    gate = build_gate_for_layer("bronze", mode="warn")

    assert gate.run_id is not None
    assert build_gate_for_layer("bronze", mode="warn").run_id != gate.run_id


# ---------------------------------------------------------------------------
# Failed path — an empty ingestion raises and maps to "failed"
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_run_failed_when_no_data_ingested():
    alerter = FakeAlerter()
    pipeline = _make_pipeline([], alerter=alerter)

    result = pipeline.run(cities=["Nowhere"])

    assert result.status == "failed"
    assert "No data ingested" in result.error_message
    assert alerter.alerts and alerter.alerts[0]["status"] == "failed"


# ---------------------------------------------------------------------------
# Persistence is best-effort — a DB failure must not crash the run
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_persistence_failure_does_not_break_run(sample_bronze_data):
    class ExplodingDatabase(FakeDatabase):
        def insert_pipeline_run(self, run_result):
            raise RuntimeError("db down")

        def insert_quality_metrics(self, run_id, gate_result):
            raise RuntimeError("db down")

    pipeline = _make_pipeline(sample_bronze_data, database=ExplodingDatabase())

    result = pipeline.run(cities=["London", "Paris"])

    # Run still succeeds despite the metrics/run-row persistence failing.
    assert result.status == "success"


# ---------------------------------------------------------------------------
# Exit-code contract of the module entry point
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.parametrize("status,expected_code", [("success", 0), ("blocked", 2), ("failed", 1)])
def test_main_exit_codes(monkeypatch, status, expected_code):
    import data_pipeline.pipeline as pipeline_module

    class StubPipeline:
        def __init__(self, *args, **kwargs):
            pass

        def run(self, *args, **kwargs):
            return SimpleNamespace(status=status)

    monkeypatch.setattr(pipeline_module, "DataPipeline", StubPipeline)

    with pytest.raises(SystemExit) as exc_info:
        pipeline_module.main()

    assert exc_info.value.code == expected_code
