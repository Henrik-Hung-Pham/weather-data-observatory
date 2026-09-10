"""Unit tests for Bronze replay.

Bronze is described as the immutable raw landing zone, but nothing ever read
it back -- so a bug fixed in the Silver or Gold transformation could not be
applied to data already collected. ``DataPipeline.replay`` is that path.

These tests use the constructor's DI seams, so nothing here touches S3, the
API or Postgres.
"""

from datetime import datetime, timezone

import pytest

from data_pipeline.pipeline import DataPipeline


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------
class ExplodingAPIClient:
    """A replay must never call the weather API."""

    def fetch_multiple_cities(self, cities):
        raise AssertionError("replay must not call the API")


class FakeLakeStorage:
    """A data lake pre-populated with Bronze objects, keyed by partition date."""

    def __init__(self, objects: dict[str, list[dict]]) -> None:
        self._objects = objects
        self.writes: list[tuple[str, str]] = []
        self.listed: list[tuple[datetime | None, datetime | None]] = []

    def list_objects(self, layer, prefix="", start_date=None, end_date=None) -> list[str]:
        self.listed.append((start_date, end_date))
        if layer != "bronze":
            return []
        keys = []
        for key in self._objects:
            obj_date = datetime.strptime(key.split("/")[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if start_date and obj_date.date() < start_date.date():
                continue
            if end_date and obj_date.date() > end_date.date():
                continue
            keys.append(key)
        return keys

    def read_json(self, key):
        return self._objects[key]

    def write_json(self, data, layer, filename, timestamp=None) -> str:
        self.writes.append((layer, filename))
        return f"{layer}/{filename}.json"


class FakeDatabase:
    def __init__(self) -> None:
        self.weather_rows: list[dict] = []

    def insert_weather_data(self, records) -> int:
        self.weather_rows.extend(records)
        return len(records)

    def insert_pipeline_run(self, run_result) -> None:
        pass

    def insert_quality_metrics(self, run_id, gate_result) -> None:
        pass


class FakeAlerter:
    def __init__(self) -> None:
        self.alerts: list[dict] = []

    def alert_pipeline_result(self, **kwargs) -> bool:
        self.alerts.append(kwargs)
        return False


def _bronze_lake(sample_bronze_data) -> FakeLakeStorage:
    """Two days of Bronze, one object each."""
    return FakeLakeStorage(
        {
            "bronze/2024-01-30/batch.json": [sample_bronze_data[0]],
            "bronze/2024-01-31/batch.json": [sample_bronze_data[1]],
        }
    )


def _make_pipeline(storage, database=None, alerter=None) -> DataPipeline:
    return DataPipeline(
        api_client=ExplodingAPIClient(),
        storage=storage,
        database=database or FakeDatabase(),
        alerter=alerter or FakeAlerter(),
    )


# ---------------------------------------------------------------------------
# The happy path
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_replay_loads_bronze_history_without_calling_the_api(sample_bronze_data):
    """Bronze is read back and driven through Silver and Gold to the serving layer."""
    storage = _bronze_lake(sample_bronze_data)
    database = FakeDatabase()
    pipeline = _make_pipeline(storage, database=database)

    result = pipeline.replay(
        start_date=datetime(2024, 1, 30, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
    )

    assert result.status == "success"
    assert result.records_ingested == 2
    assert result.records_loaded == 2
    assert len(database.weather_rows) == 2
    # ExplodingAPIClient would have raised if the API had been touched.


@pytest.mark.unit
def test_replay_writes_nothing_back_to_bronze(sample_bronze_data):
    """Bronze is the record of truth; a replay derives from it, it does not rewrite it."""
    storage = _bronze_lake(sample_bronze_data)
    pipeline = _make_pipeline(storage)

    pipeline.replay(start_date=datetime(2024, 1, 30, tzinfo=timezone.utc))

    written_layers = {layer for layer, _ in storage.writes}
    assert "bronze" not in written_layers
    assert "silver" in written_layers


@pytest.mark.unit
def test_to_defaults_to_a_single_day(sample_bronze_data):
    """Omitting the end date replays exactly the start date's partition."""
    storage = _bronze_lake(sample_bronze_data)
    pipeline = _make_pipeline(storage)

    result = pipeline.replay(start_date=datetime(2024, 1, 30, tzinfo=timezone.utc))

    assert result.records_ingested == 1
    start, end = storage.listed[0]
    assert start == end == datetime(2024, 1, 30, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# A replay is not a way around the gates
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_replay_is_still_subject_to_the_quality_gates(sample_bronze_data):
    """A batch that a live run would block, a replay blocks too.

    The gates are the point of the platform; a backfill path that skipped them
    would be a hole straight through to the serving layer.
    """
    broken = dict(sample_bronze_data[0])
    broken.pop("city")
    storage = FakeLakeStorage({"bronze/2024-01-30/batch.json": [broken]})
    database = FakeDatabase()
    pipeline = _make_pipeline(storage, database=database)

    result = pipeline.replay(start_date=datetime(2024, 1, 30, tzinfo=timezone.utc))

    assert result.status == "blocked"
    assert result.quality_gate_passed is False
    assert database.weather_rows == [], "blocked replay must not reach the serving layer"


@pytest.mark.unit
def test_empty_range_fails_with_a_clear_reason(sample_bronze_data):
    """A range with no Bronze objects is a failed replay, not a silent success."""
    storage = _bronze_lake(sample_bronze_data)
    alerter = FakeAlerter()
    pipeline = _make_pipeline(storage, alerter=alerter)

    result = pipeline.replay(start_date=datetime(2020, 5, 1, tzinfo=timezone.utc))

    assert result.status == "failed"
    assert "No Bronze objects found" in result.error_message
    assert alerter.alerts and alerter.alerts[0]["status"] == "failed"


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_replay_records_its_source_objects_in_lineage(sample_bronze_data):
    """The manifest says which Bronze artifacts the replay was derived from."""
    storage = _bronze_lake(sample_bronze_data)
    pipeline = _make_pipeline(storage)

    pipeline.replay(
        start_date=datetime(2024, 1, 30, tzinfo=timezone.utc),
        end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
    )

    manifest = pipeline._manifest
    assert manifest is not None
    sources = [a.key for a in manifest.artifacts if a.layer == "bronze_replayed"]
    assert sources == [
        "bronze/2024-01-30/batch.json",
        "bronze/2024-01-31/batch.json",
    ]
    # Cities are unknown until Bronze is read, so the manifest is filled in then.
    assert manifest.cities == sorted({r["city"] for r in sample_bronze_data})


# ---------------------------------------------------------------------------
# run() and replay() share one path
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_run_and_replay_share_the_same_gated_path():
    """Both entry points go through _execute, so gates cannot drift apart."""
    import inspect

    for method in (DataPipeline.run, DataPipeline.replay):
        assert "self._execute(" in inspect.getsource(method)
