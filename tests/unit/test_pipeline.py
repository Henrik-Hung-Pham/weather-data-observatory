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


def _data_writes(storage: "FakeStorage", layer: str, filename_prefix: str) -> list[str]:
    """Filenames of *data* objects written to ``layer``.

    ``_persist_run_result`` files the run receipt under the gold prefix too, so
    asserting on the layer alone would conflate a receipt with real Gold data.
    """
    return [
        name
        for written_layer, name in storage.writes
        if written_layer == layer and name.startswith(filename_prefix)
    ]


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
# Gates run *before* their writes — a block must leave nothing persisted
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_gold_gate_block_keeps_records_out_of_serving_layer(sample_bronze_data):
    """A Gold-gate block must not leave rows in the serving layer.

    Regression test: the Gold gate used to run *after* ``_transform_to_gold``
    had already written to S3 and inserted into PostgreSQL, so a duplicate key
    was reported as "blocked" while the duplicate rows sat in the table the
    dashboard reads.
    """
    # Two identical records -> duplicate (city, timestamp) -> unique_check
    # raises a CRITICAL issue at the Gold gate.
    duplicated = [dict(sample_bronze_data[0]), dict(sample_bronze_data[0])]
    storage = FakeStorage()
    database = FakeDatabase()
    pipeline = _make_pipeline(duplicated, storage=storage, database=database)

    result = pipeline.run(cities=["London"])

    assert result.status == "blocked"
    # Nothing reached the serving layer.
    assert database.weather_rows == []
    # No Gold *data* object was written. (The run receipt is also filed under
    # the gold prefix, so match on the data filename rather than the layer.)
    assert not _data_writes(storage, "gold", "weather_gold_")
    # The gate still ran and was recorded.
    assert [r.layer for r in result.quality_results] == ["bronze", "silver", "gold"]
    assert result.records_loaded == 0


@pytest.mark.unit
def test_silver_is_written_only_after_its_gate_passes(sample_bronze_data):
    """Silver must not be persisted until the Silver gate has passed."""
    # A Bronze-gate block happens before Silver is even transformed.
    bad = [{k: v for k, v in rec.items() if k != "timestamp"} for rec in sample_bronze_data]
    storage = FakeStorage()
    pipeline = _make_pipeline(bad, storage=storage)

    result = pipeline.run(cities=["London", "Paris"])

    assert result.status == "blocked"
    # Bronze is the immutable raw landing zone and is always kept...
    assert _data_writes(storage, "bronze", "weather_batch_")
    # ...but nothing downstream of the failed gate was persisted.
    assert not _data_writes(storage, "silver", "weather_cleaned_")
    assert not _data_writes(storage, "gold", "weather_gold_")


@pytest.mark.unit
def test_success_persists_every_layer_after_its_gate(sample_bronze_data):
    """On a clean run each layer is still written, just after its gate."""
    storage = FakeStorage()
    database = FakeDatabase()
    pipeline = _make_pipeline(sample_bronze_data, storage=storage, database=database)

    result = pipeline.run(cities=["London", "Paris"])

    assert result.status == "success"
    assert {layer for layer, _ in storage.writes} >= {"bronze", "silver", "gold"}
    assert len(database.weather_rows) == 2


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
